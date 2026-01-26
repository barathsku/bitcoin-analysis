"""
Parquet writer with Write-Audit-Publish (WAP) pattern.
"""
import os
import json
import logging
import shutil
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class ParquetWriter:
    """Writer for staging (JSON) and bronze (Parquet) layers with WAP pattern."""
    
    def __init__(self, base_path: Optional[str] = None):
        """
        Initialize writer.
        
        Args:
            base_path: Base directory for data storage
        """
        if base_path is None:
            # Default to AIRFLOW_HOME/data
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            base_path = os.path.join(airflow_home, "data")
            
        self.base_path = Path(base_path)
        self.staging_path = self.base_path / "staging"
        self.bronze_path = self.base_path / "bronze"
        
        logger.info(f"Initializing ParquetWriter with base_path: {self.base_path}")
        
        # Ensure base directories exist
        try:
            self.staging_path.mkdir(parents=True, exist_ok=True)
            self.bronze_path.mkdir(parents=True, exist_ok=True)
        except PermissionError as e:
            logger.error(f"Permission denied creating directories at {self.base_path}. Check AIRFLOW_HOME permissions.")
            raise e
    
    def write_staging(
        self,
        raw_response: Dict[str, Any],
        source: str,
        batch_id: str,
        ingestion_date: Optional[str] = None
    ) -> str:
        """
        Write raw JSON response to staging area.
        
        Staging is append-only - each ingestion creates a new batch.
        
        Args:
            raw_response: Raw API response
            source: Source name
            batch_id: Unique batch identifier
            ingestion_date: Ingestion date (defaults to today)
            
        Returns:
            Path to written file
        """
        if ingestion_date is None:
            ingestion_date = datetime.utcnow().date().isoformat()
        
        # Build path: staging/source={source}/ingestion_date={date}/batch_id={uuid}/
        staging_dir = (
            self.staging_path 
            / f"source={source}" 
            / f"ingestion_date={ingestion_date}" 
            / f"batch_id={batch_id}"
        )
        staging_dir.mkdir(parents=True, exist_ok=True)
        
        # Write JSON
        file_path = staging_dir / "raw.json"
        with open(file_path, 'w') as f:
            json.dump(raw_response, f, indent=2)
        
        logger.info(f"Wrote staging data to {file_path}")
        return str(file_path)
    
    def read_staging(self, staging_path: str) -> Dict[str, Any]:
        """
        Read raw JSON from staging path.
        
        Args:
            staging_path: Path to staging JSON file
            
        Returns:
            Raw API response as dict
        """
        with open(staging_path, 'r') as f:
            data = json.load(f)
        
        logger.info(f"Read staging data from {staging_path}")
        return data
    
    def write_bronze(
        self,
        records: List[Dict[str, Any]],
        contract: Dict[str, Any],
        partition_values: Dict[str, str],
        **context
    ) -> str:
        """
        Write records to bronze layer using Write-Audit-Publish pattern.
        
        Steps:
        1. Write Parquet to _tmp/ directory
        2. (Future: Run validation)
        3. Atomically rename _tmp/ to final partition
        
        Args:
            records: List of flat dicts to write
            contract: Data contract with schema info
            partition_values: Dict of partition key/values
            **context: Additional context (source, resource names)
            
        Returns:
            Path to final bronze partition
        """
        source = context.get('source', contract.get('source', {}).get('name', 'unknown'))
        resource = contract.get('resource', {}).get('name', 'unknown')
        
        # Build partition path
        base_dir = self.bronze_path / f"source={source}" / f"resource={resource}"
        
        # Add partition subdirectories
        for key, value in partition_values.items():
            base_dir = base_dir / f"{key}={value}"
        
        # Temporary directory for WAP
        tmp_dir = base_dir / "_tmp"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            # Convert to PyArrow table with schema
            table = self._records_to_table(records, contract)
            
            # Write Parquet
            parquet_file = tmp_dir / "data.parquet"
            pq.write_table(table, parquet_file, compression='snappy')
            
            logger.info(f"Wrote temporary Parquet to {tmp_dir}")
            
            # Atomic publish: remove old partition, rename tmp
            final_dir = base_dir
            if final_dir.exists() and final_dir != tmp_dir:
                # Remove old partition
                for item in final_dir.iterdir():
                    if item.name != "_tmp":
                        if item.is_file():
                            item.unlink()
                        elif item.is_dir():
                            shutil.rmtree(item)
            
            # Move parquet file from _tmp to final location
            final_file = final_dir / "data.parquet"
            shutil.move(str(parquet_file), str(final_file))
            
            # Remove _tmp directory
            shutil.rmtree(tmp_dir)
            
            logger.info(f"Published bronze partition to {final_dir}")
            return str(final_dir)
            
        except Exception as e:
            # Cleanup on failure
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir)
            logger.error(f"Failed to write bronze data: {e}")
            raise
    
    def _records_to_table(
        self, 
        records: List[Dict[str, Any]], 
        contract: Dict[str, Any]
    ) -> pa.Table:
        """
        Convert records to PyArrow table with explicit schema.
        
        Type conversions happen here based on contract's 'type' hints.
        
        Args:
            records: List of dicts
            contract: Data contract with extract rules
            
        Returns:
            PyArrow table
        """
        if not records:
            raise ValueError("No records to write")
        
        # Build PyArrow schema from contract
        extract_rules = contract.get('resource', {}).get('extract', {})
        fields = []
        
        for column_name in records[0].keys():
            # Get type from contract or infer
            rule = extract_rules.get(column_name, {})
            type_hint = rule.get('type')
            
            pa_type = self._get_pyarrow_type(type_hint, records, column_name)
            fields.append(pa.field(column_name, pa_type))
        
        schema = pa.schema(fields)
        
        # Convert records to PyArrow table
        # Handle type conversions
        converted_records = []
        for record in records:
            converted_record = {}
            for column_name, value in record.items():
                rule = extract_rules.get(column_name, {})
                type_hint = rule.get('type')
                converted_record[column_name] = self._convert_value(value, type_hint)
            converted_records.append(converted_record)
        
        # Create table
        table = pa.Table.from_pylist(converted_records, schema=schema)
        return table
    
    def _get_pyarrow_type(
        self, 
        type_hint: Optional[str], 
        records: List[Dict], 
        column_name: str
    ) -> pa.DataType:
        """
        Get PyArrow type from type hint or infer from data.
        
        Args:
            type_hint: Type hint from contract
            records: Sample records for inference
            column_name: Column name
            
        Returns:
            PyArrow data type
        """
        if type_hint == 'timestamp_ms_to_date':
            return pa.date32()
        elif type_hint == 'string_to_date':
            return pa.date32()
        elif type_hint == 'string':
            return pa.string()
        elif type_hint == 'int':
            return pa.int64()
        elif type_hint == 'float':
            return pa.float64()
        elif type_hint == 'bool':
            return pa.bool_()
        else:
            # Auto-infer from first record
            sample_value = records[0].get(column_name)
            if isinstance(sample_value, bool):
                return pa.bool_()
            elif isinstance(sample_value, int):
                return pa.int64()
            elif isinstance(sample_value, float):
                return pa.float64()
            elif isinstance(sample_value, str):
                return pa.string()
            elif isinstance(sample_value, datetime):
                return pa.timestamp('us')
            elif isinstance(sample_value, date):
                return pa.date32()
            else:
                # Default to string
                return pa.string()
    
    def _convert_value(self, value: Any, type_hint: Optional[str]) -> Any:
        """
        Convert value based on type hint.
        
        Args:
            value: Original value
            type_hint: Type hint from contract
            
        Returns:
            Converted value
        """
        if value is None:
            return None
        
        if type_hint == 'timestamp_ms_to_date':
            # Convert millisecond timestamp to date
            if isinstance(value, (int, float)):
                dt = datetime.fromtimestamp(value / 1000.0)
                return dt.date()
        elif type_hint == 'string_to_date':
            # Convert string to date
            if isinstance(value, str):
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y']:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
        
        # No conversion needed
        return value
