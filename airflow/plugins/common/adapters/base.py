"""
Base adapter interface for API sources.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Iterator
from datetime import date
import logging

logger = logging.getLogger(__name__)


class BaseAdapter(ABC):
    """Abstract base class for API adapters."""

    def __init__(self, contract: Dict[str, Any]):
        """
        Initialize adapter with a data contract.

        Args:
            contract: Data contract containing API configuration
        """
        self.contract = contract
        self.source_config = contract.get("source", {})
        self.resource_config = contract.get("resource", {})

    @abstractmethod
    def fetch(self, api_client: Any, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Fetch data from API using the provided client.

        Args:
            api_client: Configured APIClient instance
            params: Request parameters (e.g., date ranges, tickers)

        Returns:
            Raw API response as dict
        """
        pass

    @abstractmethod
    def iterate_requests(
        self, start_date: date, end_date: date, **kwargs
    ) -> Iterator[Dict[str, Any]]:
        """
        Generate request parameters for a date range.

        Handles differences between single-day and range APIs.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            **kwargs: Additional parameters (e.g., ticker)

        Yields:
            Dict of request parameters for each API call
        """
        pass

    def build_url(self, params: Dict[str, Any]) -> str:
        """
        Build URL from endpoint template and path params.

        Args:
            params: Parameters to substitute into endpoint

        Returns:
            Formatted endpoint URL
        """
        endpoint = self.resource_config.get("endpoint", "")

        path_params = self.resource_config.get("path_params", {})
        for param_name, param_value in path_params.items():
            if (
                isinstance(param_value, str)
                and param_value.startswith("{")
                and param_value.endswith("}")
            ):
                var_name = param_value[1:-1]
                actual_value = params.get(var_name, param_value)
                endpoint = endpoint.replace(f"{{{param_name}}}", str(actual_value))
            else:
                endpoint = endpoint.replace(f"{{{param_name}}}", str(param_value))

        return endpoint

    def build_query_params(self, params: Dict[str, Any]) -> Dict[str, str]:
        """
        Build query parameters from contract and runtime params.

        Args:
            params: Runtime parameters

        Returns:
            Dict of query parameters
        """
        query_params = {}
        contract_query_params = self.resource_config.get("query_params", {})

        for param_name, param_value in contract_query_params.items():
            if (
                isinstance(param_value, str)
                and param_value.startswith("{")
                and param_value.endswith("}")
            ):
                var_name = param_value[1:-1]
                actual_value = params.get(var_name, param_value)
                query_params[param_name] = str(actual_value)
            else:
                query_params[param_name] = str(param_value)

        return query_params
