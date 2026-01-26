"""
Contract loader for reading source and resource metadata.

Loads metadata from YAML contract files for use in DAG generation.
"""

import logging
from pathlib import Path
from typing import Dict, List, Optional
import yaml

logger = logging.getLogger(__name__)


class ContractLoader:
    """Load metadata from contract YAML files."""

    def __init__(self, contracts_dir: Optional[Path] = None):
        """
        Initialize contract loader.

        Args:
            contracts_dir: Path to contracts directory
        """
        if contracts_dir is None:
            # Default to contracts directory relative to this file
            contracts_dir = Path(__file__).parent.parent / "contracts"

        self.contracts_dir = Path(contracts_dir)
        self._sources: Optional[Dict] = None
        self._resources: Optional[Dict] = None

    def _load_sources(self) -> Dict:
        """Load source metadata from sources.yaml."""
        sources_file = self.contracts_dir / "sources.yaml"

        if not sources_file.exists():
            logger.warning(f"Sources file not found: {sources_file}")
            return {}

        with open(sources_file, "r") as f:
            data = yaml.safe_load(f)

        return data.get("sources", {})

    def _load_resources(self) -> Dict:
        """
        Load all resource metadata from resource YAML files.

        Returns:
            Dict mapping (source, resource_name) -> resource metadata
        """
        resources = {}
        resources_dir = self.contracts_dir / "resources"

        if not resources_dir.exists():
            logger.warning(f"Resources directory not found: {resources_dir}")
            return {}

        # Scan for all resource YAML files
        for source_dir in resources_dir.iterdir():
            if not source_dir.is_dir():
                continue

            source_name = source_dir.name

            for resource_file in source_dir.glob("*.yaml"):
                try:
                    with open(resource_file, "r") as f:
                        data = yaml.safe_load(f)

                    resource_data = data.get("resource", {})
                    resource_name = resource_data.get("name")

                    if resource_name:
                        key = (source_name, resource_name)
                        resources[key] = resource_data
                except Exception as e:
                    logger.warning(f"Failed to load resource file {resource_file}: {e}")

        return resources

    def get_source_metadata(self, source_name: str) -> Dict:
        """
        Get metadata for a source.

        Args:
            source_name: Name of the source

        Returns:
            Source metadata dict with display_name, etc.
        """
        if self._sources is None:
            self._sources = self._load_sources()

        return self._sources.get(source_name, {})

    def get_resource_metadata(self, source_name: str, resource_name: str) -> Dict:
        """
        Get metadata for a resource.

        Args:
            source_name: Name of the source
            resource_name: Name of the resource

        Returns:
            Resource metadata dict with display_name, tags, etc.
        """
        if self._resources is None:
            self._resources = self._load_resources()

        key = (source_name, resource_name)
        return self._resources.get(key, {})

    def get_source_display_name(self, source_name: str) -> str:
        """Get display name for a source, falling back to title case."""
        metadata = self.get_source_metadata(source_name)
        return metadata.get("display_name", source_name.title())

    def get_resource_display_name(self, source_name: str, resource_name: str) -> str:
        """Get display name for a resource, falling back to title case."""
        metadata = self.get_resource_metadata(source_name, resource_name)
        return metadata.get("display_name", resource_name.replace("_", " ").title())

    def get_resource_tags(self, source_name: str, resource_name: str) -> List[str]:
        """Get tags for a resource."""
        metadata = self.get_resource_metadata(source_name, resource_name)
        return metadata.get("tags", [])
