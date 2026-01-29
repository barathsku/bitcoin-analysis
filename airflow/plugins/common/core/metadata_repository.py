"""
Repository for storing pipeline metadata in PostgreSQL.
"""

import logging
import uuid
from typing import Dict, Any, Optional
from datetime import datetime
from sqlalchemy import (
    create_engine,
    Table,
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    JSON,
    MetaData,
    Index,
)
from sqlalchemy.dialects.postgresql import insert

logger = logging.getLogger(__name__)


class MetadataRepository:
    """
    Repository for pipeline metadata storage.

    Stores quality validation results in PostgreSQL.
    """

    def __init__(self, connection_string: str = None):
        """
        Initialize repository with database connection.

        Args:
            connection_string: PostgreSQL connection string
                              (default: uses Airflow's Postgres with pipeline_metadata db)
        """
        if connection_string is None:
            # Default to Airflow's PostgreSQL instance, different database
            connection_string = (
                "postgresql://airflow:airflow@postgres:5432/pipeline_metadata"
            )

        self.engine = create_engine(connection_string)
        self.metadata = MetaData()

        # Define quality_results table
        self.quality_results = Table(
            "quality_results",
            self.metadata,
            Column("batch_id", String, primary_key=True),
            Column("source", String, nullable=False),
            Column("resource", String, nullable=False),
            Column("data_interval_start", DateTime),
            Column("data_interval_end", DateTime),
            Column("is_valid", Boolean, nullable=False),
            Column("checks_passed", Integer),
            Column("checks_failed", Integer),
            Column("failed_checks", JSON),
            Column("created_at", DateTime, default=datetime.utcnow),
            Index("idx_quality_source_resource", "source", "resource"),
            Index("idx_quality_data_interval", "data_interval_start"),
        )

        # Define pipeline_runs table
        self.pipeline_runs = Table(
            "pipeline_runs",
            self.metadata,
            Column("run_id", String, primary_key=True),
            Column("batch_id", String, nullable=False),
            Column("source", String, nullable=False),
            Column("resource", String, nullable=False),
            Column("status", String, nullable=False),  # RUNNING, SUCCESS, FAILED
            Column("start_time", DateTime, nullable=False),
            Column("end_time", DateTime),
            Column("created_at", DateTime, default=datetime.utcnow),
            Index("idx_runs_batch_id", "batch_id"),
            Index("idx_runs_source_resource", "source", "resource"),
            Index("idx_runs_status", "status"),
        )

        # Define pipeline_steps table
        self.pipeline_steps = Table(
            "pipeline_steps",
            self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("run_id", String, nullable=False),
            Column("step_name", String, nullable=False),
            Column("status", String, nullable=False),  # RUNNING, SUCCESS, FAILED
            Column("start_time", DateTime, nullable=False),
            Column("end_time", DateTime),
            Column("metadata", JSON),  # Additional step metadata
            Column("created_at", DateTime, default=datetime.utcnow),
            Index("idx_steps_run_id", "run_id"),
            Index("idx_steps_step_name", "step_name"),
        )

        # Create tables if they don't exist
        self.metadata.create_all(self.engine)
        logger.info("Metadata repository initialized")

    def save_quality_result(
        self,
        batch_id: str,
        source: str,
        resource: str,
        data_interval_start: datetime,
        data_interval_end: datetime,
        is_valid: bool,
        checks_passed: int,
        checks_failed: int,
        failed_checks: list,
    ) -> None:
        """
        Save quality validation result.

        Uses INSERT ... ON CONFLICT UPDATE (upsert) for idempotency.

        Args:
            batch_id: Unique batch identifier
            source: Source name
            resource: Resource name
            data_interval_start: Start of data interval
            data_interval_end: End of data interval
            is_valid: Whether validation passed
            checks_passed: Number of checks that passed
            checks_failed: Number of checks that failed
            failed_checks: List of failed check details
        """
        insert_stmt = insert(self.quality_results).values(
            batch_id=batch_id,
            source=source,
            resource=resource,
            data_interval_start=data_interval_start,
            data_interval_end=data_interval_end,
            is_valid=is_valid,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            failed_checks=failed_checks,
            created_at=datetime.utcnow(),
        )

        # Upsert: if batch_id exists, update the record
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["batch_id"],
            set_={
                "is_valid": insert_stmt.excluded.is_valid,
                "checks_passed": insert_stmt.excluded.checks_passed,
                "checks_failed": insert_stmt.excluded.checks_failed,
                "failed_checks": insert_stmt.excluded.failed_checks,
                "created_at": insert_stmt.excluded.created_at,
            },
        )

        with self.engine.begin() as conn:
            conn.execute(upsert_stmt)

        logger.info(f"Saved quality result for batch {batch_id}: valid={is_valid}")

    def get_quality_result(self, batch_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve quality result by batch_id.

        Args:
            batch_id: Batch identifier

        Returns:
            Dict with quality result or None if not found
        """
        with self.engine.connect() as conn:
            result = conn.execute(
                self.quality_results.select().where(
                    self.quality_results.c.batch_id == batch_id
                )
            ).fetchone()

            if result:
                return dict(result._mapping)
            return None

    def create_run(self, batch_id: str, source: str, resource: str, **kwargs) -> str:
        """
        Create a new pipeline run record.

        Args:
            batch_id: Unique batch identifier
            source: Source name
            resource: Resource name
            **kwargs: Additional metadata to store

        Returns:
            run_id: Unique run identifier (UUID)
        """
        run_id = str(uuid.uuid4())
        start_time = datetime.utcnow()

        insert_stmt = insert(self.pipeline_runs).values(
            run_id=run_id,
            batch_id=batch_id,
            source=source,
            resource=resource,
            status="RUNNING",
            start_time=start_time,
            end_time=None,
            created_at=start_time,
        )

        with self.engine.begin() as conn:
            conn.execute(insert_stmt)

        logger.info(f"Created pipeline run {run_id} for batch {batch_id}")
        return run_id

    def update_run_status(
        self, run_id: str, status: str, end_time: Optional[datetime] = None
    ) -> None:
        """
        Update the status of a pipeline run.

        Args:
            run_id: Run identifier
            status: New status (SUCCESS, FAILED)
            end_time: End time (defaults to now)
        """
        if end_time is None:
            end_time = datetime.utcnow()

        with self.engine.begin() as conn:
            conn.execute(
                self.pipeline_runs.update()
                .where(self.pipeline_runs.c.run_id == run_id)
                .values(status=status, end_time=end_time)
            )

        logger.info(f"Updated run {run_id} status to {status}")

    def start_step(self, run_id: str, step_name: str) -> None:
        """
        Log the start of a pipeline step.

        Args:
            run_id: Run identifier
            step_name: Name of the step
        """
        start_time = datetime.utcnow()

        insert_stmt = insert(self.pipeline_steps).values(
            run_id=run_id,
            step_name=step_name,
            status="RUNNING",
            start_time=start_time,
            end_time=None,
            metadata=None,
            created_at=start_time,
        )

        with self.engine.begin() as conn:
            conn.execute(insert_stmt)

        logger.info(f"Started step '{step_name}' for run {run_id}")

    def end_step(
        self,
        run_id: str,
        step_name: str,
        status: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Log the end of a pipeline step.

        Args:
            run_id: Run identifier
            step_name: Name of the step
            status: Final status (SUCCESS, FAILED)
            metadata: Additional step metadata (record counts, paths, etc.)
        """
        end_time = datetime.utcnow()

        # Find the step record to update
        with self.engine.begin() as conn:
            # Get the step ID
            result = conn.execute(
                self.pipeline_steps.select().where(
                    (self.pipeline_steps.c.run_id == run_id)
                    & (self.pipeline_steps.c.step_name == step_name)
                    & (self.pipeline_steps.c.status == "RUNNING")
                )
            ).fetchone()

            if result:
                step_id = result.id
                conn.execute(
                    self.pipeline_steps.update()
                    .where(self.pipeline_steps.c.id == step_id)
                    .values(status=status, end_time=end_time, metadata=metadata)
                )
                logger.info(
                    f"Ended step '{step_name}' for run {run_id} with status {status}"
                )
            else:
                logger.warning(
                    f"Could not find running step '{step_name}' for run {run_id}"
                )
