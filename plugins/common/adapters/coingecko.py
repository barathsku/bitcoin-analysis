"""
CoinGecko adapter for Bitcoin price data.
"""

import logging
from typing import Dict, Any, Iterator
from datetime import date
from common.adapters.base import BaseAdapter

logger = logging.getLogger(__name__)


class CoinGeckoAdapter(BaseAdapter):
    """Adapter for CoinGecko API."""

    def fetch(self, api_client: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from CoinGecko API.

        Args:
            api_client: Configured APIClient instance
            params: Request parameters

        Returns:
            Raw API response
        """
        endpoint = self.build_url(params)
        query_params = self.build_query_params(params)

        logger.info(f"Fetching from CoinGecko: {endpoint}")

        response = api_client.request(
            method=self.resource_config.get("method", "GET"),
            endpoint=endpoint,
            params=query_params,
        )

        return response

    def iterate_requests(
        self, start_date: date, end_date: date, **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Generate request parameters for CoinGecko.

        CoinGecko uses a 'days' parameter, so we can fetch the entire range
        in a single request.

        Args:
            start_date: Start date
            end_date: End date
            **kwargs: Additional parameters

        Yields:
            Single request covering the entire date range
        """
        # Calculate number of days
        delta = end_date - start_date
        days = delta.days + 1  # Inclusive

        # CoinGecko free tier API has a maximum of 365 days
        # Cap the request to avoid 401 errors
        if days > 365:
            logger.warning(
                f"Requested {days} days exceeds CoinGecko free tier limit (365 days). Capping to 365."
            )
            days = 365

        logger.info(f"CoinGecko: Fetching {days} days of data in single request")

        yield {"days": str(days), **kwargs}
