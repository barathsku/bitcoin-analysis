"""
Dynamic DAG generation for ingestion pipelines.

This file auto-discovers and generates DAGs based on the asset catalog:
- Automatically detects all (source, resource) combinations from assets.yaml
- Creates one DAG per combination (e.g., massive_stocks, coingecko_market_chart)
- Each DAG contains task groups for all enabled assets of that type
"""

from datetime import datetime, timedelta
from typing import Dict, List

from common.core.dag_generator import generate_ingestion_dag
from common.core.asset_catalog import AssetCatalog
from common.contracts.loader import ContractLoader

# Default arguments for all generated DAGs
DEFAULT_ARGS = {
    "owner": "barath",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
}

# DAG generation defaults
DAG_DEFAULTS = {
    "schedule": "@daily",
    "start_date": datetime(2026, 1, 26),
    "catchup": False,
    "max_active_runs": 1,
}


def auto_discover_dag_configs() -> List[Dict]:
    """
    Auto-discover DAG configurations from the asset catalog.

    Scans all enabled assets and creates a DAG config for each unique
    (source, resource) combination. Metadata is loaded from contract YAML files.

    Returns:
        List of DAG configuration dictionaries
    """
    catalog = AssetCatalog()
    assets = catalog.get_assets(enabled_only=True)

    # Load contract metadata
    contract_loader = ContractLoader()

    # Find all unique (source, resource) combinations
    source_resource_pairs = set((asset.source, asset.resource) for asset in assets)

    dag_configs = []
    for source, resource in sorted(source_resource_pairs):
        # Generate DAG ID: {source}_{resource}_ingestion
        dag_id = f"{source}_{resource}_ingestion"

        # Load metadata from contracts
        try:
            contract = contract_loader.load(source, resource)
            source_display = contract["source"].get("display_name", source.title())
            resource_display = contract["resource"].get(
                "display_name", resource.replace("_", " ").title()
            )
            resource_tags = contract["resource"].get("tags", [])
            schedule = contract["resource"].get("schedule", DAG_DEFAULTS["schedule"])
        except Exception as _:
            source_display = source.title()
            resource_display = resource.replace("_", " ").title()
            resource_tags = []
            schedule = DAG_DEFAULTS["schedule"]

        # Build tags: base tags + resource-specific tags
        tags = ["ingestion", source, resource] + resource_tags

        # Build description
        description = (
            f"Daily ingestion of {resource_display} data from {source_display} API"
        )

        config = {
            "dag_id": dag_id,
            "source": source,
            "resource": resource,
            "schedule": schedule,
            "start_date": DAG_DEFAULTS["start_date"],
            "dag_tags": tags,
            "description": description,
        }

        dag_configs.append(config)

    return dag_configs


# Auto-discover DAG configurations from asset catalog
DAG_CONFIGS = auto_discover_dag_configs()

# Each iteration creates a separate DAG that Airflow will auto-register
for config in DAG_CONFIGS:
    dag_id = config["dag_id"]

    # Generate the DAG
    generated_dag = generate_ingestion_dag(
        dag_id=dag_id,
        source=config["source"],
        resource=config["resource"],
        schedule=config["schedule"],
        start_date=config["start_date"],
        catchup=DAG_DEFAULTS["catchup"],
        max_active_runs=DAG_DEFAULTS["max_active_runs"],
        dag_tags=config["dag_tags"],
        default_args=DEFAULT_ARGS,
    )

    globals()[dag_id] = generated_dag
