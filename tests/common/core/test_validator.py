"""
Unit tests for Validator with Soda Core (Mocked).
"""

import pytest
from unittest.mock import MagicMock, patch
from common.core.validator import Validator


@pytest.fixture
def mock_soda_scan():
    with patch("common.core.validator.Scan") as mock_scan_cls:
        scan_instance = mock_scan_cls.return_value
        yield scan_instance


@patch("common.core.validator.duckdb")
def test_validator_success(mock_duckdb, mock_soda_scan, sample_contract):
    """Test successful validation."""
    validator = Validator()
    mock_con = mock_duckdb.connect.return_value

    # Mock Soda results
    check_mock = MagicMock()
    check_mock.name = "row_count > 0"
    check_mock.outcome = "pass"
    mock_soda_scan._checks = [check_mock]

    # Add soda checks to contract
    sample_contract.setdefault("resource", {}).setdefault("validation", {})[
        "soda_checks"
    ] = ["row_count > 0"]

    result = validator.validate(
        contract=sample_contract, tmp_paths=[("p1", "/tmp/path", 10)]
    )

    assert result["is_valid"] is True
    assert result["checks_passed"] == 1
    assert result["checks_failed"] == 0


@patch("common.core.validator.duckdb")
def test_validator_failure(mock_duckdb, mock_soda_scan, sample_contract):
    """Test validation failure."""
    validator = Validator()
    mock_con = mock_duckdb.connect.return_value

    # Mock Soda results
    check_mock = MagicMock()
    check_mock.name = "row_count > 0"
    check_mock.outcome = "fail"
    mock_soda_scan._checks = [check_mock]

    # Add soda checks to contract
    sample_contract.setdefault("resource", {}).setdefault("validation", {})[
        "soda_checks"
    ] = ["row_count > 0"]

    result = validator.validate(
        contract=sample_contract, tmp_paths=[("p1", "/tmp/path", 10)]
    )

    assert result["is_valid"] is False
    assert result["checks_failed"] == 1
    assert len(result["failed_checks"]) == 1
