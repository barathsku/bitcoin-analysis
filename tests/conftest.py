"""
Shared fixtures for unit tests.
"""

import os
import shutil
import pytest
import tempfile


@pytest.fixture
def temp_dirs():
    """Create temporary directories for testing."""
    base_dir = tempfile.mkdtemp()
    staging_dir = os.path.join(base_dir, "staging")
    bronze_dir = os.path.join(base_dir, "bronze")

    os.makedirs(staging_dir)
    os.makedirs(bronze_dir)

    yield {"base": base_dir, "staging": staging_dir, "bronze": bronze_dir}

    shutil.rmtree(base_dir)


@pytest.fixture
def sample_contract():
    """Return a sample data contract for testing."""
    return {
        "source": {
            "name": "test_source",
            "base_url": "https://api.test.com",
            "auth": {"type": "none"},
        },
        "resource": {
            "name": "test_resource",
            "extract": {
                "price": {"path": "prices.current", "type": "float"},
                "timestamp": {
                    "path": "meta.ts",
                    "type": "timestamp_ms_to_datetime",
                    "partition_key": True,
                },
                "id": {"path": "id"},
            },
        },
    }
