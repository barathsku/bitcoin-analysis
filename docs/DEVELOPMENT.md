# Development & Testing Guide

## Local Environment Setup

We use `pyproject.toml` for Python dependency management. It is recommended to use a virtual environment.

### Prerequisites

*   Python 3.9+
*   `pip`
*   `virtualenv` (or any other env manager like Anaconda/Miniconda, etc.)

### Installation

1.  **Create a Virtual Environment (example)**:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2.  **Install Dependencies**:
    Install the project in editable mode:
    ```bash
    pip install -e .
    ```

## 2. Running Unit Tests

We use `pytest` for unit testing the Python code (Airflow plugins, custom operators, and utility functions). The tests are located in the `tests/` directory.

### Running All Tests

To run the full test suite:

```bash
pytest
```

### Running Specific Tests

To run a specific test file:

```bash
pytest tests/common/core/test_transformer.py
```

### Test Structure

*   `tests/`: Root directory for all tests.
*   `tests/common/`: Tests for shared libraries and plugins.
*   `tests/conftest.py`: Shared pytest fixtures (e.g., temporary directories, sample contracts).

## 3. Running dbt Models Locally

You can develop and test dbt models locally using the `dbt` CLI, provided you have the necessary data.

### Prerequisites

*   Ensure you are in the dbt project directory:
    ```bash
    cd dbt/analysis
    ```

### Common Commands

*   **Debug Connection**:
    ```bash
    dbt debug
    ```

*   **Run Models**:
    ```bash
    dbt run
    ```

*   **Test Models**:
    Run schema tests defined in `schema.yml` files:
    ```bash
    dbt test
    ```

*   **Build (Run + Test)**:
    ```bash
    dbt build
    ```

*   **Generate Documentation**:
    ```bash
    dbt docs generate
    dbt docs serve
    ```
