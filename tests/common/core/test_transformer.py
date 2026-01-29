"""
Unit tests for Transformer class.
"""

from datetime import datetime, date
from common.core.transformer import Transformer


def test_extract_value_dot_notation(sample_contract):
    """Test extracting values using dot notation."""
    transformer = Transformer(sample_contract)
    data = {"prices": {"current": 100.5}}

    val = transformer._extract_value(data, "prices.current")
    assert val == 100.5


def test_extract_value_array_wildcard(sample_contract):
    """Test extracting list of values using [*]."""
    transformer = Transformer(sample_contract)
    data = {"items": [{"id": 1}, {"id": 2}]}

    val = transformer._extract_value(data, "items[*].id")
    assert val == [1, 2]


def test_convert_type_timestamp():
    """Test timestamp conversion."""
    transformer = Transformer({})

    # ms to datetime
    ts_ms = 1609459200000  # 2021-01-01 00:00:00 UTC
    dt = transformer._convert_type(ts_ms, "timestamp_ms_to_datetime")
    assert isinstance(dt, datetime)
    assert dt.year == 2021

    # ms to date
    d = transformer._convert_type(ts_ms, "timestamp_ms_to_date")
    assert isinstance(d, date)
    assert d.year == 2021


def test_transform(sample_contract):
    """Test full transformation flow."""
    transformer = Transformer(sample_contract)
    raw_data = {
        "prices": {"current": 50000.0},
        "meta": {"ts": 1609459200000},
        "id": "btc",
    }

    records = transformer.transform(raw_data)

    assert len(records) == 1
    record = records[0]
    assert record["price"] == 50000.0
    assert record["id"] == "btc"
    assert "timestamp" in record
    assert record["__batch_id"] is not None
