"""
Unit tests for Validator fail_on_empty_data configuration.
"""

import pytest
from unittest.mock import patch
from common.core.validator import Validator


@pytest.fixture
def mock_soda_scan():
    with patch("common.core.validator.Scan") as mock_scan_cls:
        scan_instance = mock_scan_cls.return_value
        yield scan_instance


@patch("common.core.validator.duckdb")
def test_validator_empty_default_success(mock_duckdb, mock_soda_scan):
    """Test validity when no data is provided (default behavior)."""
    validator = Validator()
    contract = {"resource": {"validation": {}}}

    # Empty tmp_paths
    result = validator.validate(contract=contract, tmp_paths=[])

    assert result["is_valid"] is True
    assert result["checks_failed"] == 0
    assert result["checks_passed"] == 0


@patch("common.core.validator.duckdb")
def test_validator_empty_with_fail_flag(mock_duckdb, mock_soda_scan):
    """Test validity when no data is provided and fail_on_empty_data is True."""
    validator = Validator()
    contract = {"resource": {"validation": {"fail_on_empty_data": True}}}

    # Empty tmp_paths
    result = validator.validate(contract=contract, tmp_paths=[])

    assert result["is_valid"] is False
    assert result["checks_failed"] == 1
    assert result["failed_checks"][0]["name"] == "fail_on_empty_data"


@patch("common.core.validator.duckdb")
def test_validator_empty_files_with_fail_flag(mock_duckdb, mock_soda_scan):
    """Test validity when paths are provided but contain no parquet files."""
    validator = Validator()
    contract = {"resource": {"validation": {"fail_on_empty_data": True}}}

    result = validator.validate(contract=contract, tmp_paths=[123])  # Invalid item type

    assert result["is_valid"] is False
    assert result["checks_failed"] == 1
    assert result["failed_checks"][0]["name"] == "fail_on_empty_data"
