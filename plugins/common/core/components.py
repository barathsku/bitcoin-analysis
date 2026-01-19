"""
Shared Airflow components for data ingestion pipeline.
"""
import logging
import uuid
from datetime import datetime, timedelta, date
from typing import Optional, Dict, Any

from airflow.decorators import task, task_group
from airflow.exceptions import AirflowException

from common.contracts.loader import ContractLoader
from common.core.api_client import APIClient
from common.core.transformer import Transformer
from common.core.validator import Validator
from common.core.writer import ParquetWriter
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
    if source == 'coingecko':
        return CoinGeckoAdapter(contract)
    elif source == 'massive':
        if 'stocks' in resource:
            return MassiveStocksAdapter(contract)
        elif 'forex' in resource:
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
    **kwargs
):
    """
    TaskGroup factory for data ingestion.
    
    Used by both scheduled and manual DAGs.
    
    Args:
        source: Source name (e.g., 'coingecko', 'massive')
        resource: Resource name (e.g., 'btc_usd_daily', 'stocks_daily')
        window_start: Start date (YYYY-MM-DD) or None for data_interval_start
        window_end: End date (YYYY-MM-DD) or None for data_interval_end
        **kwargs: Additional parameters (e.g., ticker)
        
    Returns:
        TaskGroup with ingestion tasks
    """
    
    @task
    def validate_params(**context) -> Dict[str, Any]:
        """Validate parameters and load contract."""
        logger.info(f"Validating params: source={source}, resource={resource}")
        
        # Load contract
        loader = ContractLoader()
        try:
            contract = loader.load(source, resource)
            logger.info(f"Contract loaded successfully for {source}/{resource}")
        except Exception as e:
            raise AirflowException(f"Failed to load contract: {e}")
        
        # Determine date range
        if window_start and window_end:
            start_date_str = window_start
            end_date_str = window_end
        else:
            # Use Airflow's data_interval
            data_interval_start = context['data_interval_start']
            data_interval_end = context['data_interval_end']
            
            start_date_str = data_interval_start.strftime('%Y-%m-%d')
            end_date_str = (data_interval_end - timedelta(seconds=1)).strftime('%Y-%m-%d')
        
        # Generate batch ID
        batch_id = str(uuid.uuid4())
        
        return {
            'contract': contract,
            'start_date': start_date_str,
            'end_date': end_date_str,
            'batch_id': batch_id,
            'source': source,
            'resource': resource,
            **kwargs
        }
    
    @task
    def fetch_and_process(task_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from API and process through the pipeline.
        
        This task handles:
        1. API fetching with retry/rate limiting
        2. Writing raw JSON to staging
        3. Transforming to flat records
        4. Validating records
        5. Writing Parquet to bronze with WAP
        """
        contract = task_params['contract']
        start_date = datetime.strptime(task_params['start_date'], '%Y-%m-%d').date()
        end_date = datetime.strptime(task_params['end_date'], '%Y-%m-%d').date()
        batch_id = task_params['batch_id']
        source_name = task_params['source']
        resource_name = task_params['resource']
        
        logger.info(f"Processing {source_name}/{resource_name} from {start_date} to {end_date}")
        
        # Initialize components
        api_config = contract['source']
        api_client = APIClient(
            base_url=api_config['base_url'],
            auth_config=api_config['auth'],
            rate_limit_rpm=api_config.get('rate_limit', {}).get('requests_per_minute', 30)
        )
        
        adapter = get_adapter(source_name, resource_name, contract)
        transformer = Transformer(contract)
        validator = Validator()
        writer = ParquetWriter()
        
        # Iterate through date range
        all_records = []
        partition_values = {}
        
        # Filter out start_date and end_date from task_params to avoid argument collision
        request_kwargs = {k: v for k, v in task_params.items() if k not in ['start_date', 'end_date']}
        
        for request_params in adapter.iterate_requests(start_date, end_date, **request_kwargs):
            logger.info(f"Fetching with params: {request_params}")
            
            # Fetch from API
            raw_response = adapter.fetch(api_client, request_params)
            
            # Write to staging
            writer.write_staging(
                raw_response=raw_response,
                source=source_name,
                batch_id=batch_id
            )
            
            # Transform
            # Filter out explicit args from request_params to avoid collisions
            transform_kwargs = {k: v for k, v in request_params.items() if k not in ['batch_id', 'source_name']}
            
            records = transformer.transform(
                raw_response,
                batch_id=batch_id,
                source_name=source_name,
                **transform_kwargs
            )
            
            # Validate
            if not validator.validate(records, contract):
                raise AirflowException("Validation failed")
            
            all_records.extend(records)
        
        logger.info(f"Total records fetched: {len(all_records)}")
        
        # Group records by partition key
        extract_rules = contract.get('resource', {}).get('extract', {})
        partition_key = None
        for col_name, rule in extract_rules.items():
            if rule.get('partition_key'):
                partition_key = col_name
                break
        
        if not partition_key:
            raise AirflowException("No partition key defined in contract")
        
        # Group records by partition value
        from collections import defaultdict
        partitioned_records = defaultdict(list)
        
        for record in all_records:
            partition_value = record.get(partition_key)
            if partition_value:
                # Convert date objects to strings
                if isinstance(partition_value, date):
                    partition_value = partition_value.isoformat()
                partitioned_records[partition_value].append(record)
        
        # Write each partition
        written_partitions = []
        for partition_value, records in partitioned_records.items():
            logger.info(f"Writing partition {partition_key}={partition_value} with {len(records)} records")
            
            path = writer.write_bronze(
                records=records,
                contract=contract,
                partition_values={partition_key: partition_value},
                source=source_name,
                resource=resource_name
            )
            written_partitions.append(path)
        
        return {
            'status': 'success',
            'records_written': len(all_records),
            'partitions_written': len(written_partitions),
            'partition_paths': written_partitions
        }
    
    # Build task dependencies
    params = validate_params()
    result = fetch_and_process(params)
    
    return result
