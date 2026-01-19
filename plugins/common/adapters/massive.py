"""
Massive (Polygon) adapters for stocks and forex data.
"""
import logging
from typing import Dict, Any, Iterator
from datetime import date, timedelta
from common.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class MassiveStocksAdapter(BaseAdapter):
    """
    Adapter for Massive Stocks API (single-day endpoint).
    
    The stocks endpoint returns data for a single day, so we need to iterate
    through the date range with rate limiting.
    """
    
    def fetch(
        self, 
        api_client: Any,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetch data from Massive Stocks API.
        
        Args:
            api_client: Configured APIClient instance
            params: Request parameters (must include ticker and data_date)
            
        Returns:
            Raw API response
        """
        endpoint = self.build_url(params)
        query_params = self.build_query_params(params)
        
        ticker = params.get('ticker', 'UNKNOWN')
        data_date = params.get('data_date', 'UNKNOWN')
        logger.info(f"Fetching Massive Stocks: {ticker} for {data_date}")
        
        response = api_client.request(
            method=self.resource_config.get('method', 'GET'),
            endpoint=endpoint,
            params=query_params
        )
        
        return response
    
    def iterate_requests(
        self, 
        start_date: date, 
        end_date: date,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Generate request parameters for each day in the range.
        
        Since Massive Stocks is a single-day API, we yield one request per day.
        The API client's rate limiter will handle spacing.
        
        Args:
            start_date: Start date
            end_date: End date
            **kwargs: Must include 'ticker'
            
        Yields:
            One request dict per day
        """
        ticker = kwargs.get('ticker')
        if not ticker:
            raise ValueError("ticker is required for Massive Stocks adapter")
        
        current_date = start_date
        request_count = 0
        
        while current_date <= end_date:
            # Format date as YYYY-MM-DD
            date_str = current_date.strftime('%Y-%m-%d')
            
            yield {
                'ticker': ticker,
                'data_date': date_str,
                **kwargs
            }
            
            current_date += timedelta(days=1)
            request_count += 1
        
        logger.info(f"Massive Stocks: Generated {request_count} requests for {ticker}")


class MassiveForexAdapter(BaseAdapter):
    """
    Adapter for Massive Forex API (range endpoint).
    
    The forex endpoint supports date ranges, so we can fetch in a single request.
    """
    
    def fetch(
        self, 
        api_client: Any,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetch data from Massive Forex API.
        
        Args:
            api_client: Configured APIClient instance
            params: Request parameters
            
        Returns:
            Raw API response
        """
        endpoint = self.build_url(params)
        query_params = self.build_query_params(params)
        
        ticker = params.get('ticker', 'UNKNOWN')
        logger.info(f"Fetching Massive Forex: {ticker}")
        
        response = api_client.request(
            method=self.resource_config.get('method', 'GET'),
            endpoint=endpoint,
            params=query_params
        )
        
        return response
    
    def iterate_requests(
        self, 
        start_date: date, 
        end_date: date,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Generate request parameters for Massive Forex.
        
        Forex supports range queries, so we can fetch the entire range
        in a single request.
        
        Args:
            start_date: Start date
            end_date: End date
            **kwargs: Must include 'ticker'
            
        Yields:
            Single request covering the entire date range
        """
        ticker = kwargs.get('ticker')
        if not ticker:
            raise ValueError("ticker is required for Massive Forex adapter")
        
        # Format dates as YYYY-MM-DD
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')
        
        logger.info(f"Massive Forex: Fetching {ticker} from {start_str} to {end_str}")
        
        yield {
            'ticker': ticker,
            'start_date': start_str,
            'end_date': end_str,
            **kwargs
        }
