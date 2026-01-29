"""
Asset catalog loader for centralized asset management.

Provides filtering and querying capabilities for assets defined in assets.yaml.
"""

import logging
from typing import List, Optional, Dict, Any
from pathlib import Path
from dataclasses import dataclass, field
import yaml

logger = logging.getLogger(__name__)


@dataclass
class Asset:
    """Represents a single asset from the catalog."""

    ticker: str
    name: str
    asset_type: str
    source: str
    resource: str
    enabled: bool = True
    tags: List[str] = field(default_factory=list)
    params: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Asset":
        """Create Asset from dictionary."""
        return cls(
            ticker=data["ticker"],
            name=data["name"],
            asset_type=data["asset_type"],
            source=data["source"],
            resource=data["resource"],
            enabled=data.get("enabled", True),
            tags=data.get("tags", []),
            params=data.get("params", {}),
        )


class AssetCatalog:
    """Load and filter assets from centralized catalog."""

    def __init__(self, catalog_path: str = None):
        """
        Initialize asset catalog loader.

        Args:
            catalog_path: Path to assets.yaml file
        """
        if catalog_path is None:
            current_dir = Path(__file__).parent.parent / "contracts"
            catalog_path = current_dir / "assets.yaml"

        self.catalog_path = Path(catalog_path)
        self._assets: Optional[List[Asset]] = None

    def _load_catalog(self) -> List[Asset]:
        """Load and parse assets.yaml."""
        if not self.catalog_path.exists():
            raise FileNotFoundError(f"Asset catalog not found: {self.catalog_path}")

        logger.info(f"Loading asset catalog from {self.catalog_path}")

        with open(self.catalog_path, "r") as f:
            data = yaml.safe_load(f)

        assets = []
        for asset_data in data.get("assets", []):
            try:
                asset = Asset.from_dict(asset_data)
                assets.append(asset)
            except Exception as e:
                logger.warning(f"Failed to parse asset: {asset_data}. Error: {e}")

        logger.info(f"Loaded {len(assets)} assets from catalog")
        return assets

    def get_assets(
        self,
        source: Optional[str] = None,
        resource: Optional[str] = None,
        asset_type: Optional[str] = None,
        tags: Optional[List[str]] = None,
        enabled_only: bool = True,
    ) -> List[Asset]:
        """
        Filter and return assets from catalog.

        Args:
            source: Filter by source name (e.g., 'massive', 'coingecko')
            resource: Filter by resource name (e.g., 'stocks', 'forex')
            asset_type: Filter by asset type (e.g., 'stock', 'forex', 'crypto')
            tags: Filter by tags (asset must have ALL specified tags)
            enabled_only: If True, return only enabled assets

        Returns:
            List of filtered assets
        """
        # Load catalog if not already loaded
        if self._assets is None:
            self._assets = self._load_catalog()

        filtered = self._assets

        # Apply filters
        if enabled_only:
            filtered = [a for a in filtered if a.enabled]

        if source:
            filtered = [a for a in filtered if a.source == source]

        if resource:
            filtered = [a for a in filtered if a.resource == resource]

        if asset_type:
            filtered = [a for a in filtered if a.asset_type == asset_type]

        if tags:
            filtered = [a for a in filtered if all(tag in a.tags for tag in tags)]

        logger.debug(
            f"Filtered assets: source={source}, resource={resource}, "
            f"asset_type={asset_type}, tags={tags}, enabled_only={enabled_only} "
            f"-> {len(filtered)} assets"
        )

        return filtered

    def get_asset_by_ticker(self, ticker: str) -> Optional[Asset]:
        """
        Get a specific asset by ticker.

        Args:
            ticker: Ticker symbol

        Returns:
            Asset if found, None otherwise
        """
        if self._assets is None:
            self._assets = self._load_catalog()

        for asset in self._assets:
            if asset.ticker == ticker:
                return asset

        return None

    def reload(self):
        """Force reload of asset catalog."""
        self._assets = None
        self._load_catalog()
