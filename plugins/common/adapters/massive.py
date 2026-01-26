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
    Adapter for Massive Stocks Aggregates API (range endpoint with pagination).
    
    Uses /v2/aggs endpoint to fetch date ranges. Handles cursor-based pagination
    (120 items per page) to retrieve complete dataset.
    """
    
    def fetch(
        self, 
        api_client: Any,
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Fetch data from Massive Stocks Aggregates API with pagination.
        
        Follows next_url cursor until all pages are retrieved.
        
        Args:
            api_client: Configured APIClient instance
            params: Request parameters (must include ticker, start_date, end_date)
            
        Returns:
            Merged API response with all results
        """
        endpoint = self.build_url(params)
        query_params = self.build_query_params(params)
        
        ticker = params.get('ticker', 'UNKNOWN')
        logger.info(f"Fetching Massive Stocks: {ticker} from {params.get('start_date')} to {params.get('end_date')}")
        
        all_results = []
        next_url = None
        page_count = 0
        
        while True:
            page_count += 1
            
            if next_url:
                # Use full next_url for pagination
                # Massive requires apiKey to be appended to the cursor URL
                if 'apiKey' not in next_url and 'api_key' in api_client.auth_config:
                    separator = '&' if '?' in next_url else '?'
                    next_url = f"{next_url}{separator}apiKey={api_client.auth_config['api_key']}"

                response = api_client.request(
                    method='GET',
                    endpoint=next_url
                )
            else:
                # First request
                response = api_client.request(
                    method=self.resource_config.get('method', 'GET'),
                    endpoint=endpoint,
                    params=query_params
                )
            
            results = response.get('results', [])
            all_results.extend(results)
            
            next_url = response.get('next_url')
            logger.info(f"Page {page_count}: {len(results)} records ({len(all_results)} total)")
            
            if not next_url:
                break
        
        logger.info(f"Completed: {ticker} - {len(all_results)} records across {page_count} pages")
        
        # Return in same format as original response
        return {
            'results': all_results,
            'ticker': ticker,
            'queryCount': len(all_results),
            'resultsCount': len(all_results)
        }
    
    def iterate_requests(
        self, 
        start_date: date, 
        end_date: date,
        **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Generate request parameters for date range.
        
        With range endpoint, yields single request (pagination handled in fetch).
        
        Args:
            start_date: Start date
            end_date: End date
            **kwargs: Must include 'ticker'
            
        Yields:
            Single request dict for entire date range
        """
        ticker = kwargs.get('ticker')
        if not ticker:
            raise ValueError("ticker is required for Massive Stocks adapter")
        
        logger.info(f"Massive Stocks: Requesting {ticker} from {start_date} to {end_date}")
        
        yield {
            'ticker': ticker,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            **kwargs
        }


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
