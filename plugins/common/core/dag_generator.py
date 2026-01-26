"""
DAG generator for auto-generating ingestion DAGs from asset catalog.

Reads assets from the centralized catalog and dynamically creates task groups,
eliminating the need to maintain hardcoded ticker lists in DAG files.
"""

import logging
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from common.core.asset_catalog import AssetCatalog, Asset
from common.core.components import ingest

logger = logging.getLogger(__name__)


def sanitize_task_id(value: str) -> str:
    """
    Sanitize a value for use in Airflow task IDs.

    Replaces special characters with underscores to create valid task IDs.

    Args:
        value: Raw value (e.g., ticker like 'C:EURUSD')

    Returns:
        Sanitized task ID safe string
    """
    # Replace colons and other special chars with underscores
    return re.sub(r"[^a-zA-Z0-9_]", "_", value)


def generate_task_group_id(asset: Asset) -> str:
    """
    Generate semantic task group ID from asset metadata.

    Format: {source}_{resource}_{sanitized_ticker}
    Examples:
        - massive_stocks_AAPL
        - massive_forex_C_EURUSD
        - coingecko_market_chart_bitcoin

    Args:
        asset: Asset instance

    Returns:
        Task group ID string
    """
    sanitized_ticker = sanitize_task_id(asset.ticker)
    return f"{asset.source}_{asset.resource}_{sanitized_ticker}"


def generate_ingestion_dag(
    dag_id: str,
    source: Optional[str] = None,
    resource: Optional[str] = None,
    asset_type: Optional[str] = None,
    tags: Optional[List[str]] = None,
    schedule: str = "@daily",
    start_date: datetime = datetime(2025, 1, 18),
    catchup: bool = False,
    max_active_runs: int = 1,
    dag_tags: Optional[List[str]] = None,
    default_args: Optional[Dict[str, Any]] = None,
) -> DAG:
    """
    Generate an ingestion DAG for assets matching the specified filters.

    Reads assets from the catalog and creates task groups dynamically.

    Args:
        dag_id: DAG identifier
        source: Filter assets by source (e.g., 'massive', 'coingecko')
        resource: Filter assets by resource (e.g., 'stocks', 'forex')
        asset_type: Filter assets by type (e.g., 'stock', 'forex', 'crypto')
        tags: Filter assets by tags (must have ALL specified tags)
        schedule: Cron schedule or preset
        start_date: DAG start date
        catchup: Whether to backfill
        max_active_runs: Max concurrent DAG runs
        dag_tags: Tags for the DAG itself
        default_args: Default args for tasks

    Returns:
        Generated DAG instance
    """
    # Default args
    if default_args is None:
        default_args = {
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

    # Default DAG tags
    if dag_tags is None:
        dag_tags = ["ingestion", "auto_generated"]
        if source:
            dag_tags.append(source)
        if resource:
            dag_tags.append(resource)

    # Load assets from catalog
    catalog = AssetCatalog()
    assets = catalog.get_assets(
        source=source,
        resource=resource,
        asset_type=asset_type,
        tags=tags,
        enabled_only=True,
    )

    logger.info(
        f"Generating DAG '{dag_id}' with {len(assets)} assets "
        f"(source={source}, resource={resource}, asset_type={asset_type})"
    )

    # Generate description
    description_parts = []
    if source:
        description_parts.append(f"from {source}")
    if resource:
        description_parts.append(f"({resource} data)")
    if asset_type:
        description_parts.append(f"[{asset_type}]")

    description = f"Auto-generated ingestion DAG {' '.join(description_parts)}"

    # Create DAG
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        max_active_runs=max_active_runs,
        tags=dag_tags,
    ) as dag:
        begin = EmptyOperator(task_id="begin")
        end = EmptyOperator(task_id="end")

        task_groups = []

        for asset in assets:
            # Generate semantic task group ID
            group_id = generate_task_group_id(asset)

            # Determine pool from source
            pool = f"{asset.source}_api_pool"

            # Build kwargs from asset params
            kwargs = {"ticker": asset.ticker, **asset.params}

            logger.debug(
                f"Creating task group '{group_id}' for {asset.name} "
                f"(ticker={asset.ticker}, source={asset.source}, resource={asset.resource})"
            )

            # Create ingestion task group
            task_group = ingest(
                group_id=group_id,
                source=asset.source,
                resource=asset.resource,
                pool=pool,
                **kwargs,
            )

            task_groups.append(task_group)

        # Set up dependencies
        if task_groups:
            begin >> task_groups >> end
        else:
            logger.warning(f"No assets found for DAG '{dag_id}' - creating empty DAG")
            begin >> end

    logger.info(
        f"DAG '{dag_id}' generated successfully with {len(task_groups)} task groups"
    )

    return dag
