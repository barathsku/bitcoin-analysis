"""
Unit tests for CoinGecko adapter.
"""

from datetime import date
from common.adapters.coingecko import CoinGeckoAdapter


# Adding this since we're under the free API rate limits
def test_iterate_requests_capping():
    """Test that requests are capped at 365 days."""
    contract = {"source": {"name": "coingecko"}, "resource": {"endpoint": "/test"}}
    adapter = CoinGeckoAdapter(contract)

    start = date(2020, 1, 1)
    end = date(2022, 1, 1)  # 2 years

    reqs = list(adapter.iterate_requests(start, end))

    assert len(reqs) == 1
    # Check days param is capped
    assert reqs[0]["days"] == "365"


def test_build_url():
    """Test URL construction with params."""
    contract = {
        "source": {"name": "coingecko"},
        "resource": {
            "endpoint": "/coins/{id}/history",
            "path_params": {"id": "{ticker}"},
        },
    }
    adapter = CoinGeckoAdapter(contract)
    params = {"ticker": "bitcoin"}

    url = adapter.build_url(params)
    assert url == "/coins/bitcoin/history"
