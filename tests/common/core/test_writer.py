"""
Unit tests for ParquetWriter.
"""

import os
from common.core.writer import ParquetWriter


def test_staging_write_read(temp_dirs):
    """Test writing and reading to staging."""
    writer = ParquetWriter(base_path=temp_dirs["base"])
    data = {"test": "data"}

    path = writer.write_staging(data, "test_source", "batch_123")
    assert os.path.exists(path)

    loaded = writer.read_staging(path)
    assert loaded == data


def test_publish_wap_pattern(temp_dirs, sample_contract):
    """Test full Write-Audit-Publish flow."""
    writer = ParquetWriter(base_path=temp_dirs["base"])
    records = [{"price": 100.0, "id": "1"}]
    partition = {"date": "2026-01-01"}

    # 1. Write Temporary
    tmp_path = writer.write_temporary(
        records,
        sample_contract,
        partition,
        source="test_source",
        resource="test_resource",
    )
    assert "_tmp_" in tmp_path
    assert os.path.exists(tmp_path)

    # 2. Publish
    final_path = writer.publish(
        tmp_path,
        sample_contract,
        partition,
        source="test_source",
        resource="test_resource",
    )

    # Verify temp is gone and final exists
    assert not os.path.exists(tmp_path)
    assert os.path.exists(final_path)
    assert (
        "bronze/source=test_source/resource=test_resource/date=2026-01-01" in final_path
    )
