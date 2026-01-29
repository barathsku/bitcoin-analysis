"""
Filesystem helper functions for checking data availability.
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def get_existing_dates(
    source: str,
    resource: str,
    ticker: Optional[str] = None,
    base_path: Optional[str] = None,
) -> set:
    """
    Scan bronze partitions to find already-ingested dates.

    Checks the bronze directory structure to determine which dates
    already have data, enabling gap-aware fetching.

    Args:
        source: Source name (e.g., 'coingecko', 'massive')
        resource: Resource name (e.g., 'market_chart', 'stocks')
        ticker: Optional ticker symbol for partitioned resources
        base_path: Optional base path (defaults to AIRFLOW_HOME/data)

    Returns:
        Set of date objects for which data already exists
    """
    if base_path is None:
        airflow_home = os.getenv("AIRFLOW_HOME", "/opt/airflow")
        base_path = os.path.join(airflow_home, "data")

    bronze_path = (
        Path(base_path) / "bronze" / f"source={source}" / f"resource={resource}"
    )

    if not bronze_path.exists():
        logger.info(f"No existing bronze data found at {bronze_path}")
        return set()

    existing_dates = set()

    # Handle different partition structures
    if ticker:
        # Multi-partition: ticker + data_date (stocks, forex)
        ticker_path = bronze_path / f"ticker={ticker}"
        if ticker_path.exists():
            for date_partition in ticker_path.iterdir():
                if date_partition.is_dir() and date_partition.name.startswith(
                    "data_date="
                ):
                    date_str = date_partition.name.split("=")[1]
                    try:
                        existing_dates.add(
                            datetime.strptime(date_str, "%Y-%m-%d").date()
                        )
                    except ValueError:
                        logger.warning(f"Invalid date partition: {date_partition.name}")
    else:
        # Single partition: data_date only (crypto)
        for date_partition in bronze_path.iterdir():
            if date_partition.is_dir() and date_partition.name.startswith("data_date="):
                date_str = date_partition.name.split("=")[1]
                try:
                    existing_dates.add(datetime.strptime(date_str, "%Y-%m-%d").date())
                except ValueError:
                    logger.warning(f"Invalid date partition: {date_partition.name}")

    if existing_dates:
        logger.info(
            f"Found {len(existing_dates)} existing dates in bronze for {source}/{resource}"
        )

    return existing_dates
