"""
Contract loader to read and parse YAML data contracts.

Supports three-layer architecture:
  1. sources.yaml - API credentials and base configuration
  2. resources/{source}/{resource}.yaml - Endpoint definitions
  3. Merged contract combining both layers
"""

import logging
import os
import re
from typing import Dict, Any
from pathlib import Path
import yaml

logger = logging.getLogger(__name__)


class ContractLoader:
    """Load and cache data contracts from YAML files with layered architecture."""

    def __init__(self, contracts_dir: str = None):
        """
        Initialize contract loader.

        Args:
            contracts_dir: Directory containing contract YAML files
        """
        if contracts_dir is None:
            current_dir = Path(__file__).parent
            contracts_dir = current_dir

        self.contracts_dir = Path(contracts_dir)
        self._cache = {}
        self._sources_cache = None

    def _read_yaml_with_env(self, file_path: Path) -> Dict[str, Any]:
        """
        Read YAML file with environment variable substitution.
        Supports ${VAR_NAME} syntax.

        Args:
            file_path: Path to YAML file

        Returns:
            Parsed YAML data
        """
        pattern = re.compile(r"\$\{(\w+)\}")

        def replace_env(match):
            env_var = match.group(1)
            value = os.environ.get(env_var)
            if value is None:
                raise ValueError(
                    f"Missing environment variable: {env_var} required for {file_path}"
                )
            return value

        with open(file_path, "r") as f:
            content = f.read()

        # Substitute environment variables
        content_with_env = pattern.sub(replace_env, content)

        return yaml.safe_load(content_with_env)

    def _load_sources(self) -> Dict[str, Any]:
        """
        Load sources configuration from sources.yaml.

        Returns:
            Dict of source configurations keyed by source name
        """
        if self._sources_cache is not None:
            return self._sources_cache

        sources_path = self.contracts_dir / "sources.yaml"

        if not sources_path.exists():
            raise FileNotFoundError(f"Sources config not found: {sources_path}")

        logger.info(f"Loading sources configuration from {sources_path}")

        data = self._read_yaml_with_env(sources_path)

        sources = data.get("sources", {})
        self._sources_cache = sources

        logger.debug(f"Loaded {len(sources)} source configurations")
        return sources

    def _load_resource(self, source: str, resource: str) -> Dict[str, Any]:
        """
        Load resource configuration from resources/{source}/{resource}.yaml.

        Args:
            source: Source name
            resource: Resource name

        Returns:
            Resource configuration dict
        """
        resource_path = self.contracts_dir / "resources" / source / f"{resource}.yaml"

        if not resource_path.exists():
            raise FileNotFoundError(f"Resource config not found: {resource_path}")

        logger.info(f"Loading resource configuration from {resource_path}")

        data = self._read_yaml_with_env(resource_path)

        return data.get("resource", {})

    def load(self, source: str, resource: str) -> Dict[str, Any]:
        """
        Load contract for a specific source and resource.

        Merges source config from sources.yaml with resource config from
        resources/{source}/{resource}.yaml to create a unified contract.

        Args:
            source: Source name (e.g., 'coingecko', 'massive')
            resource: Resource name (e.g., 'stocks', 'forex', 'market_chart')

        Returns:
            Merged contract as dict with 'source' and 'resource' keys

        Raises:
            FileNotFoundError: If source or resource config doesn't exist
            ValueError: If configuration is invalid
        """
        cache_key = f"{source}/{resource}"

        if cache_key in self._cache:
            logger.debug(f"Loading contract from cache: {cache_key}")
            return self._cache[cache_key]

        # Load source configuration
        sources = self._load_sources()
        if source not in sources:
            raise ValueError(f"Source '{source}' not found in sources.yaml")

        source_config = sources[source]

        # Load resource configuration
        resource_config = self._load_resource(source, resource)

        # Verify resource references correct source
        if resource_config.get("source") != source:
            logger.warning(
                f"Resource {resource} references source '{resource_config.get('source')}' "
                f"but was loaded from {source} directory"
            )

        # Merge into unified contract
        contract = {"source": source_config, "resource": resource_config}

        # Validate merged contract
        self._validate_contract(contract, cache_key)

        # Cache result
        self._cache[cache_key] = contract

        logger.info(f"Successfully loaded and merged contract: {cache_key}")
        return contract

    def _validate_contract(self, contract: Dict[str, Any], cache_key: str) -> None:
        """
        Basic validation of contract structure.

        Args:
            contract: Parsed contract
            cache_key: Contract identifier for error messages

        Raises:
            ValueError: If contract is invalid
        """
        required_keys = ["source", "resource"]
        for key in required_keys:
            if key not in contract:
                raise ValueError(f"Contract {cache_key} missing required key: {key}")

        source = contract["source"]
        if "name" not in source:
            raise ValueError(f"Contract {cache_key} missing source.name")
        if "base_url" not in source:
            raise ValueError(f"Contract {cache_key} missing source.base_url")

        resource = contract["resource"]
        if "name" not in resource:
            raise ValueError(f"Contract {cache_key} missing resource.name")

        if "extract" not in resource:
            raise ValueError(f"Contract {cache_key} missing resource.extract")

        logger.debug(f"Contract {cache_key} validated successfully")

    def reload(self):
        """Clear all caches to force reload on next access."""
        self._cache.clear()
        self._sources_cache = None
        logger.info("Contract loader cache cleared")
