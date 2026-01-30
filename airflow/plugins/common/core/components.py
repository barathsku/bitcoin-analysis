"""
Shared Airflow components for data ingestion pipeline
"""

import logging
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any
from collections import defaultdict
import redis

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException

from common.contracts.loader import ContractLoader
from common.core.api_client import APIClient
from common.core.transformer import Transformer
from common.core.validator import Validator
from common.core.writer import ParquetWriter
from common.core.metadata_repository import MetadataRepository
from common.core.exceptions import ValidationError, ExtractionError, ConfigurationError
from common.adapters.factory import get_adapter
from common.core.filesystem import get_existing_dates

logger = logging.getLogger(__name__)


@task_group
def _ingest_data(
    source: str,
    resource: str,
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    pool: Optional[str] = None,
    **kwargs,
):
    """
    TaskGroup factory for data ingestion with 4 atomic tasks

    Tasks:
    1. extract: API call --> staging (JSON)
    2. transform: staging --> bronze _tmp/ (Parquet)
    3. validate: Run Soda Core checks on _tmp/
    4. load: Atomic rename _tmp/ --> final partition


    Args:
        source: Source name (e.g., 'coingecko', 'massive')
        resource: Resource name (e.g., 'market_chart', 'stocks', 'forex)
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
        Extract: Fetch data from API and write to staging

        This task is idempotent:
        - Uses deterministic batch_id
        - Overwrites staging path on retry
        """
        logger.info(f"Extracting {source}/{resource}")

        metadata_repo = MetadataRepository()

        loader = ContractLoader()
        try:
            contract = loader.load(source, resource)
        except Exception as e:
            raise ConfigurationError(f"Failed to load contract: {e}")

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

        # Initialize Redis client for rate limiting
        try:
            redis_client = redis.Redis(host="redis", port=6379, db=0)
            logger.info("Successfully connected to Redis for rate limiting")
        except Exception as e:
            logger.warning(
                f"Failed to connect to Redis, falling back to local rate limiting: {e}"
            )
            redis_client = None

        api_client = APIClient(
            base_url=api_config["base_url"],
            auth_config=api_config["auth"],
            rate_limit_rpm=api_config.get("rate_limit", {}).get(
                "requests_per_minute", 30
            ),
            redis_client=redis_client,
            rate_limit_key=source,
        )

        adapter = get_adapter(source, resource, contract)
        writer = ParquetWriter()

        # Gap-aware fetching: check if data already exists
        force_refetch = kwargs.get("force_refetch", False)
        ticker = kwargs.get("ticker")

        if not force_refetch:
            existing_dates = get_existing_dates(
                source=source, resource=resource, ticker=ticker
            )

            # Generate set of requested dates
            requested_dates = set()
            current = start_date
            while current <= end_date:
                requested_dates.add(current)
                current += timedelta(days=1)

            missing_dates = requested_dates - existing_dates

            if not missing_dates:
                logger.info(
                    f"All {len(requested_dates)} requested dates already exist in bronze. "
                    f"Skipping fetch (use force_refetch=true to override)"
                )
                metadata_repo.end_step(
                    run_id=run_id,
                    step_name="extract",
                    status="SUCCESS",
                    metadata={
                        "staging_paths": [],
                        "num_files": 0,
                        "skipped_reason": "all_dates_exist",
                    },
                )
                return {
                    "contract": contract,
                    "batch_id": batch_id,
                    "source": source,
                    "resource": resource,
                    "start_date": start_date_str,
                    "end_date": end_date_str,
                    "staging_paths": [],
                    "data_interval_start": data_interval_start.isoformat(),
                    "data_interval_end": data_interval_end.isoformat(),
                    "ticker": ticker,
                    "run_id": run_id,
                    "skipped": True,
                }
            else:
                logger.info(
                    f"Gap-aware fetch: {len(missing_dates)} of {len(requested_dates)} dates missing. "
                    f"Fetching entire range (API limitation: range queries only)"
                )
        else:
            logger.info(
                "force_refetch=true, fetching all dates regardless of existing data"
            )

        # Fetch and write to staging
        request_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ["start_date", "end_date", "force_refetch"]
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
        Transform: Read staging, transform to records, write to bronze _tmp/

        This prepares data for validation but doesn't publish yet
        """
        # Handle skipped extraction (gap-aware logic)
        if extract_result.get("skipped", False):
            logger.info("Extraction was skipped (all dates exist), skipping transform")
            return {
                **extract_result,
                "tmp_paths": [],
                "total_records": 0,
            }

        contract = extract_result["contract"]
        batch_id = extract_result["batch_id"]
        source_name = extract_result["source"]
        resource_name = extract_result["resource"]

        logger.info(f"Transforming {source_name}/{resource_name}")

        run_id = extract_result.get("run_id")
        if run_id:
            metadata_repo = MetadataRepository()
            metadata_repo.start_step(run_id=run_id, step_name="transform")

        transformer = Transformer(contract)
        writer = ParquetWriter()

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

        # Write to _tmp/ (NOT final location)
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

            # Write to _tmp directory
            path = writer.write_temporary(
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
        Validate: Run Soda Core checks on transformed data (Audit phase of WAP)

        Reads from bronze _tmp/ and validates. Does not publish yet
        """
        # Handle skipped extraction
        if transform_result.get("skipped", False):
            logger.info("Extraction was skipped (all dates exist), skipping validation")
            return {
                **transform_result,
                "validation_result": {
                    "is_valid": True,
                    "checks_passed": 0,
                    "checks_failed": 0,
                    "failed_checks": [],
                },
            }

        contract = transform_result["contract"]
        batch_id = transform_result["batch_id"]
        source_name = transform_result["source"]
        resource_name = transform_result["resource"]

        logger.info(f"Validating {source_name}/{resource_name}")

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
        if validate_result.get("skipped", False):
            logger.info("Extraction was skipped (all dates exist), skipping load")
            return {
                "status": "skipped",
                "batch_id": validate_result.get("batch_id"),
                "records_written": 0,
                "partitions_written": 0,
                "partition_paths": [],
            }

        batch_id = validate_result["batch_id"]
        source_name = validate_result["source"]
        resource_name = validate_result["resource"]
        tmp_paths = validate_result["tmp_paths"]

        logger.info(f"Loading {source_name}/{resource_name} to final location")

        run_id = validate_result.get("run_id")
        if run_id:
            metadata_repo = MetadataRepository()
            metadata_repo.start_step(run_id=run_id, step_name="load")

        contract = validate_result["contract"]

        writer = ParquetWriter()

        written_partitions = []
        for partition_str, tmp_path, _ in tmp_paths:
            # Reconstruct partition values from string
            # Format: key1=val1, key2=val2
            partition_values = {}
            for item in partition_str.split(", "):
                k, v = item.split("=")
                partition_values[k] = v

            final_path = writer.publish(
                tmp_path=tmp_path,
                contract=contract,
                partition_values=partition_values,
                source=source_name,
                resource=resource_name,
            )
            written_partitions.append(final_path)
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


def ingest(
    source: str,
    resource: str,
    window_start: Optional[str] = None,
    window_end: Optional[str] = None,
    pool: Optional[str] = None,
    **kwargs,
):
    """
    Wrapper for _ingest_data task group to allow dynamic group_id injection

    Args:
        source: Source name
        resource: Resource name
        group_id: Optional custom group_id (popped from kwargs)
        **kwargs: Other args passed to underlying task group
    """
    group_id = kwargs.pop("group_id", None)

    if group_id:
        return _ingest_data.override(group_id=group_id)(
            source=source,
            resource=resource,
            window_start=window_start,
            window_end=window_end,
            pool=pool,
            **kwargs,
        )
    else:
        return _ingest_data(
            source=source,
            resource=resource,
            window_start=window_start,
            window_end=window_end,
            pool=pool,
            **kwargs,
        )
