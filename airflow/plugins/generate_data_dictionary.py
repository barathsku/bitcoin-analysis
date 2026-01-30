import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any

from dbt_artifacts_parser.parser import parse_catalog, parse_manifest
from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogV1
from dbt_artifacts_parser.parsers.manifest.manifest_v2 import ManifestV2

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = (
    Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "dbt" / "analysis"
)
TARGET_DIR = DBT_PROJECT_DIR / "target"
DOCS_DIR = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow")) / "docs"
MANIFEST_PATH = TARGET_DIR / "manifest.json"
CATALOG_PATH = TARGET_DIR / "catalog.json"
OUTPUT_FILE = DOCS_DIR / "DATA_DICTIONARY.md"


def load_artifacts() -> tuple[ManifestV2, CatalogV1]:
    """Load and parse dbt manifest and catalog artifacts."""
    logger.info(f"Loading artifacts from {TARGET_DIR}")

    if not MANIFEST_PATH.exists() or not CATALOG_PATH.exists():
        raise FileNotFoundError(
            f"Artifacts not found in {TARGET_DIR}. Run 'dbt docs generate' first."
        )

    with open(MANIFEST_PATH, "r") as f:
        manifest = parse_manifest(json.load(f))

    with open(CATALOG_PATH, "r") as f:
        catalog = parse_catalog(json.load(f))

    return manifest, catalog


def format_table_header(name: str, description: str) -> str:
    """Format the header for a table section."""
    return f"### Table: `{name}`\n**Description**: {description}\n\n"


def merge_column_info(manifest_node: Any, catalog_node: Any) -> Dict[str, Dict]:
    """
    Merge column information from Manifest (descriptions) and Catalog (types).
    Prioritize Manifest for descriptions and Catalog for types/order.
    """
    cols = {}

    if manifest_node and manifest_node.columns:
        for name, col in manifest_node.columns.items():
            cols[name] = {
                "name": name,
                "type": col.data_type or "Unknown",
                "description": col.description or "",
                "index": 9999,
            }

    if catalog_node and catalog_node.columns:
        for name, col in catalog_node.columns.items():
            if name not in cols:
                cols[name] = {
                    "name": name,
                    "type": col.type,
                    "description": col.comment or "",
                    "index": col.index,
                }
            else:
                cols[name]["type"] = col.type
                cols[name]["index"] = col.index

                if not cols[name]["description"] and col.comment:
                    cols[name]["description"] = col.comment

    return cols


def format_columns(columns: Dict[str, Dict]) -> str:
    """Format columns into a Markdown table."""
    if not columns:
        return "_No column information available._\n\n---\n\n"

    table = "| Column | Type | Description |\n|--------|------|-------------|\n"

    sorted_cols = sorted(columns.values(), key=lambda x: x["index"])

    for col in sorted_cols:
        col_type = col["type"]
        description = col["description"]
        description = description.replace("|", "\\|").replace("\n", " ")
        table += f"| `{col['name']}` | {col_type} | {description} |\n"

    return table + "\n---\n\n"


def generate_dictionary_content(manifest: ManifestV2, catalog: CatalogV1) -> str:
    """Generate the Markdown content for the data dictionary."""
    content = "# Data Dictionary\n\n"
    content += f"**Last Updated**: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
    content += "This document is automatically generated from dbt artifacts.\n\n"

    sources = []
    models = []

    for key, node in manifest.nodes.items():
        if node.resource_type == "model" and node.package_name == "analysis":
            models.append(node)

    for key, source in manifest.sources.items():
        if source.package_name == "analysis":
            sources.append(source)

    models.sort(key=lambda x: x.name)
    sources.sort(key=lambda x: x.source_name + x.name)

    if sources:
        content += "## Sources\n\n"
        for source in sources:
            description = source.description or "No description provided."
            table_name = f"{source.source_name}.{source.name}"

            content += format_table_header(table_name, description)

            catalog_node = catalog.sources.get(source.unique_id)

            merged_cols = merge_column_info(source, catalog_node)
            content += format_columns(merged_cols)

    if models:
        content += "## Models\n\n"

        staging = [m for m in models if m.path.startswith("staging")]
        marts = [m for m in models if m.path.startswith("marts")]
        other = [m for m in models if m not in staging and m not in marts]

        for layer_name, layer_models in [
            ("Marts", marts),
            ("Staging", staging),
            ("Other", other),
        ]:
            if not layer_models:
                continue

            content += f"### {layer_name}\n\n"

            for model in layer_models:
                description = model.description or "No description provided."
                content += format_table_header(model.name, description)

                catalog_node = catalog.nodes.get(model.unique_id)

                merged_cols = merge_column_info(model, catalog_node)
                content += format_columns(merged_cols)

    return content


def main():
    try:
        if not DOCS_DIR.exists():
            DOCS_DIR.mkdir(parents=True, exist_ok=True)

        manifest, catalog = load_artifacts()
        content = generate_dictionary_content(manifest, catalog)

        with open(OUTPUT_FILE, "w") as f:
            f.write(content)

        logger.info(f"Data dictionary generated successfully at {OUTPUT_FILE}")

    except Exception as e:
        logger.error(f"Failed to generate data dictionary: {e}")
        raise


if __name__ == "__main__":
    main()
