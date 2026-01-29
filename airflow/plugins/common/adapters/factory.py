"""
Adapter factory to instantiate appropriate adapter based on source and resource.
"""

from typing import Dict, Any

from common.adapters.coingecko import CoinGeckoAdapter
from common.adapters.massive import MassiveStocksAdapter, MassiveForexAdapter


def get_adapter(source: str, resource: str, contract: Dict[str, Any]):
    """
    Factory function to get the appropriate adapter.

    Args:
        source: Source name
        resource: Resource name
        contract: Data contract

    Returns:
        Adapter instance
    """
    if source == "coingecko":
        return CoinGeckoAdapter(contract)
    elif source == "massive":
        if "stocks" in resource:
            return MassiveStocksAdapter(contract)
        elif "forex" in resource:
            return MassiveForexAdapter(contract)
        else:
            raise ValueError(f"Unknown Massive resource: {resource}")
    else:
        raise ValueError(f"Unknown source: {source}")
