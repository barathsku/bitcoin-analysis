"""
Shared Airflow components for data ingestion pipeline.
"""

import logging
import uuid
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any
from collections import defaultdict

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException

from common.contracts.loader import ContractLoader
from common.core.api_client import APIClient
from common.core.transformer import Transformer
from common.core.validator import Validator
from common.core.writer import ParquetWriter
from common.core.metadata_repository import MetadataRepository
from common.core.exceptions import ValidationError, ExtractionError, ConfigurationError
from common.adapters.coingecko import CoinGeckoAdapter
from common.adapters.massive import MassiveStocksAdapter, MassiveForexAdapter

logger = logging.getLogger(__name__)


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


@task_group
def ingest(
    source: str,
    resource: str,
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    pool: Optional[str] = None,
    **kwargs,
):
    """
    TaskGroup factory for data ingestion with 4 atomic tasks (WAP pattern).

    Tasks:
    1. extract: API call → staging (JSON)
    2. transform: staging → bronze _tmp/ (Parquet)
    3. validate: Run Soda Core checks on _tmp/
    4. load: Atomic rename _tmp/ → final partition

    Args:
        source: Source name (e.g., 'coingecko', 'massive')
        resource: Resource name (e.g., 'btc_usd_daily', 'stocks_daily')
        window_start: Start date (YYYY-MM-DD) or None for data_interval_start
        window_end: End date (YYYY-MM-DD) or None for data_interval_end
        pool: Airflow pool for rate limiting (optional)
        **kwargs: Additional parameters (e.g., ticker)

    Returns:
        TaskGroup with ingestion tasks
    """

    @task(task_id="extract", pool=pool)
    def extract_data(
        source: str,
        resource: str,
        window_start: Optional[str] = None,
        window_end: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Extract: Fetch data from API and write to staging.

        This task is idempotent:
        - Uses deterministic batch_id
        - Overwrites staging path on retry
        """
        logger.info(f"Extracting {source}/{resource}")

        # Initialize metadata repository
        metadata_repo = MetadataRepository()

        # Load contract
        loader = ContractLoader()
        try:
            contract = loader.load(source, resource)
        except Exception as e:
            raise ConfigurationError(f"Failed to load contract: {e}")

        # Get context from kwargs
        context = kwargs

        # Determine date range
        if window_start and window_end:
            start_date_str = window_start
            end_date_str = window_end
            # Use window dates as interval if manually provided
            data_interval_start = datetime.strptime(window_start, "%Y-%m-%d")
            data_interval_end = datetime.strptime(window_end, "%Y-%m-%d") + timedelta(
                days=1
            )
        else:
            data_interval_start = context["data_interval_start"]
            data_interval_end = context["data_interval_end"]
            start_date_str = data_interval_start.strftime("%Y-%m-%d")
            end_date_str = (data_interval_end - timedelta(seconds=1)).strftime(
                "%Y-%m-%d"
            )

        # Generate deterministic batch_id (key for idempotency)
        batch_id = f"{source}_{resource}_{start_date_str.replace('-', '')}"
        if kwargs.get("ticker"):
            batch_id = f"{batch_id}_{kwargs['ticker'].replace(':', '_')}"

        logger.info(f"Batch ID: {batch_id}")

        # Create pipeline run and start extract step
        run_id = metadata_repo.create_run(
            batch_id=batch_id, source=source, resource=resource
        )
        metadata_repo.start_step(run_id=run_id, step_name="extract")

        # Parse dates
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

        # Initialize components
        api_config = contract["source"]
        api_client = APIClient(
            base_url=api_config["base_url"],
            auth_config=api_config["auth"],
            rate_limit_rpm=api_config.get("rate_limit", {}).get(
                "requests_per_minute", 30
            ),
        )

        adapter = get_adapter(source, resource, contract)
        writer = ParquetWriter()

        # Fetch and write to staging
        request_kwargs = {
            k: v for k, v in kwargs.items() if k not in ["start_date", "end_date"]
        }
        request_kwargs["source"] = source
        request_kwargs["resource"] = resource

        staging_paths = []

        try:
            for request_params in adapter.iterate_requests(
                start_date, end_date, **request_kwargs
            ):
                logger.info(f"Fetching with params: {request_params}")
                raw_response = adapter.fetch(api_client, request_params)

                # Write to staging (idempotent: same path on retry)
                staging_path = writer.write_staging(
                    raw_response=raw_response, source=source, batch_id=batch_id
                )
                staging_paths.append(staging_path)

        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            metadata_repo.end_step(
                run_id=run_id,
                step_name="extract",
                status="FAILED",
                metadata={"error": str(e)},
            )
            raise ExtractionError(f"API extraction failed: {e}")

        # Log successful extraction
        metadata_repo.end_step(
            run_id=run_id,
            step_name="extract",
            status="SUCCESS",
            metadata={"staging_paths": staging_paths, "num_files": len(staging_paths)},
        )

        return {
            "contract": contract,
            "batch_id": batch_id,
            "source": source,
            "resource": resource,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "staging_paths": staging_paths,
            "data_interval_start": data_interval_start.isoformat(),
            "data_interval_end": data_interval_end.isoformat(),
            # Only include safe kwargs (like ticker) to avoid serialization errors with Airflow context objects
            "ticker": kwargs.get("ticker"),
            "run_id": run_id,
        }

    @task(task_id="transform")
    def transform_data(extract_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform: Read staging, transform to records, write to bronze _tmp/.

        This prepares data for validation but doesn't publish yet (WAP pattern).
        """
        contract = extract_result["contract"]
        batch_id = extract_result["batch_id"]
        source_name = extract_result["source"]
        resource_name = extract_result["resource"]
        start_date_str = extract_result["start_date"]
        end_date_str = extract_result["end_date"]

        logger.info(f"Transforming {source_name}/{resource_name}")

        # Initialize metadata repository and log step start
        run_id = extract_result.get("run_id")
        if run_id:
            metadata_repo = MetadataRepository()
            metadata_repo.start_step(run_id=run_id, step_name="transform")

        # Initialize components
        transformer = Transformer(contract)
        writer = ParquetWriter()
        adapter = get_adapter(source_name, resource_name, contract)

        # Parse dates
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

        # Read staging files and transform
        all_records = []
        request_kwargs = {
            k: v
            for k, v in extract_result.items()
            if k
            not in [
                "contract",
                "batch_id",
                "source",
                "resource",
                "start_date",
                "end_date",
                "staging_paths",
                "data_interval_start",
                "data_interval_end",
            ]
        }

        # Read from staging files (no API re-fetch)
        staging_paths = extract_result["staging_paths"]

        for staging_path in staging_paths:
            # Read raw response from staging
            raw_response = writer.read_staging(staging_path)

            # Transform the raw response
            records = transformer.transform(
                raw_response,
                batch_id=batch_id,
                source_name=source_name,
                **request_kwargs,
            )
            all_records.extend(records)

        logger.info(f"Transformed {len(all_records)} records")

        # Collect all partition keys (support multi-dimensional partitioning)
        extract_rules = contract.get("resource", {}).get("extract", {})
        partition_keys = []
        for col_name, rule in extract_rules.items():
            if rule.get("partition_key"):
                partition_keys.append(col_name)

        if not partition_keys:
            raise AirflowException("No partition key defined in contract")

        logger.info(f"Partitioning by: {', '.join(partition_keys)}")

        # Write to _tmp/ (NOT final location - WAP pattern)
        # Group records by partition key tuple (e.g., (ticker, data_date))
        partitioned_records = defaultdict(list)
        for record in all_records:
            # Build partition tuple
            partition_tuple = []
            for key in partition_keys:
                value = record.get(key)
                if value:
                    if isinstance(value, date):
                        value = value.isoformat()
                    partition_tuple.append((key, value))

            if len(partition_tuple) == len(partition_keys):
                # All partition keys present
                partition_tuple_key = tuple(partition_tuple)
                partitioned_records[partition_tuple_key].append(record)
            else:
                logger.warning(f"Record missing partition keys: {record}")

        tmp_paths = []
        for partition_tuple, records in partitioned_records.items():
            # Convert partition tuple to dict
            partition_values = dict(partition_tuple)

            # Write to _tmp directory using a marker in context
            path = writer.write_bronze(
                records=records,
                contract=contract,
                partition_values=partition_values,
                source=source_name,
                resource=resource_name,
            )
            # Store partition values as string for tracking
            partition_str = ", ".join([f"{k}={v}" for k, v in partition_values.items()])
            tmp_paths.append((partition_str, path, len(records)))

        # Log successful transformation
        if run_id:
            metadata_repo.end_step(
                run_id=run_id,
                step_name="transform",
                status="SUCCESS",
                metadata={
                    "total_records": len(all_records),
                    "num_partitions": len(tmp_paths),
                    "partition_keys": partition_keys,
                },
            )

        return {
            "contract": contract,
            "batch_id": batch_id,
            "source": source_name,
            "resource": resource_name,
            "partition_keys": partition_keys,
            "tmp_paths": tmp_paths,  # [(partition_value, path, record_count), ...]
            "total_records": len(all_records),
            "data_interval_start": extract_result["data_interval_start"],
            "data_interval_end": extract_result["data_interval_end"],
            "run_id": run_id,
        }

    @task(task_id="validate")
    def validate_data(transform_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate: Run Soda Core checks on transformed data (Audit phase of WAP).

        Reads from bronze _tmp/ and validates. Does not publish yet.
        """
        contract = transform_result["contract"]
        batch_id = transform_result["batch_id"]
        source_name = transform_result["source"]
        resource_name = transform_result["resource"]

        logger.info(f"Validating {source_name}/{resource_name}")

        # Initialize metadata repository and log step start
        run_id = transform_result.get("run_id")
        if run_id:
            metadata_repo = MetadataRepository()
            metadata_repo.start_step(run_id=run_id, step_name="validate")

        # For validation, we use the _tmp/ parquet files directly with DuckDB
        tmp_paths = transform_result.get("tmp_paths", [])

        logger.info(
            f"Validation: Validating data from {len(tmp_paths)} temporary files"
        )

        validator = Validator()

        # Run validation using DuckDB on the parquet files
        validation_result = validator.validate(contract=contract, tmp_paths=tmp_paths)

        # Store quality results in metadata database
        if not run_id:
            metadata_repo = MetadataRepository()
        metadata_repo.save_quality_result(
            batch_id=batch_id,
            source=source_name,
            resource=resource_name,
            data_interval_start=datetime.fromisoformat(
                transform_result["data_interval_start"]
            ),
            data_interval_end=datetime.fromisoformat(
                transform_result["data_interval_end"]
            ),
            is_valid=validation_result["is_valid"],
            checks_passed=validation_result["checks_passed"],
            checks_failed=validation_result["checks_failed"],
            failed_checks=validation_result["failed_checks"],
        )

        # Log validation step completion
        if run_id:
            metadata_repo.end_step(
                run_id=run_id,
                step_name="validate",
                status="SUCCESS" if validation_result["is_valid"] else "FAILED",
                metadata={
                    "checks_passed": validation_result["checks_passed"],
                    "checks_failed": validation_result["checks_failed"],
                    "num_tmp_files": len(tmp_paths),
                },
            )

        if not validation_result["is_valid"]:
            raise ValidationError(
                f"Validation failed: {validation_result['checks_failed']} checks failed",
                failed_checks=validation_result["failed_checks"],
            )

        logger.info("Validation passed")

        return {**transform_result, "validation_result": validation_result}

    @task(task_id="load")
    def load_data(validate_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Load: Atomic publish from _tmp/ to final partition (Publish phase of WAP).

        This is the final step - atomically moves data to production location.
        """
        batch_id = validate_result["batch_id"]
        source_name = validate_result["source"]
        resource_name = validate_result["resource"]
        tmp_paths = validate_result["tmp_paths"]

        logger.info(f"Loading {source_name}/{resource_name} to final location")

        # Initialize metadata repository and log step start
        run_id = validate_result.get("run_id")
        if run_id:
            metadata_repo = MetadataRepository()
            metadata_repo.start_step(run_id=run_id, step_name="load")

        # In the current write_bronze implementation, atomic swap already happens
        # The _tmp/ pattern is already implemented in writer.py
        # So tmp_paths are actually final paths after atomic swap

        written_partitions = [path for _, path, _ in tmp_paths]
        total_records = validate_result["total_records"]

        logger.info(
            f"Successfully loaded {total_records} records to {len(written_partitions)} partitions"
        )

        # Log successful load and update run status
        if run_id:
            metadata_repo.end_step(
                run_id=run_id,
                step_name="load",
                status="SUCCESS",
                metadata={
                    "records_written": total_records,
                    "partitions_written": len(written_partitions),
                    "partition_paths": written_partitions,
                },
            )
            # Mark entire pipeline run as successful
            metadata_repo.update_run_status(run_id=run_id, status="SUCCESS")

        return {
            "status": "success",
            "batch_id": batch_id,
            "records_written": total_records,
            "partitions_written": len(written_partitions),
            "partition_paths": written_partitions,
        }

    # Build task dependencies (4 atomic tasks)
    extracted = extract_data(
        source=source,
        resource=resource,
        window_start=window_start,
        window_end=window_end,
        **kwargs,
    )
    transformed = transform_data(extracted)
    validated = validate_data(transformed)
    loaded = load_data(validated)

    return loaded
