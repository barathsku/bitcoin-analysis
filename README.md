# Bitcoin vs. Traditional Assets Analysis


## Overview

This project implements an end-to-end data pipeline using Airflow, dbt and DuckDB to answer key assessment questions relating to the performance of Bitcoin against traditional assets (fiat currencies, stocks, and market indices), e.g.,
*   How does Bitcoin compare to traditional assets in terms of returns?
*   What is the volatility difference?
*   What are the effects of Dollar Cost Averaging (DCA)?

## Prerequisites

*   **Docker** (and Docker Compose)
*   **Git**

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/barathsku/bitcoin-analysis.git
cd bitcoin-analysis
```

### 2. Configure Environment

Create a `.env` file in the root directory to configure the environment.

**For Mac/Linux Users:**
To prevent permission issues, you must set the `AIRFLOW_UID` to your current user ID. Run the following command:

```bash
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

**For Windows Users:**
You can manually create the `.env` file with a default UID:

```bash
echo "AIRFLOW_UID=50000" > .env
```

**Add API Keys & Project Config:**
Append the following configurations to your `.env` file:

```bash
COINGECKO_API_KEY=your_api_key_here
MASSIVE_API_KEY=your_api_key_here
AIRFLOW_PROJ_DIR=.
```

The CoinGecko/Massive API keys in the `.env` file are required for the ingestion DAGs to work as expected.

### 3. Run the Platform

We use Docker to orchestrate the Airflow services and resources.

**First-time Setup (Database Initialization):**
On the very first run, you need to initialize the database and create the default user.

```bash
docker compose up airflow-init
```

Wait until you see a message indicating "User \"airflow\" created" or "exited with code 0".

**Start Services:**
Once initialized, start the platform:

```bash
docker compose up -d
```

You will have to wait for 3-4 minutes for the services during first-time startups, and 2-3 minutes for subsequent startups (due to the custom Python libraries being installed across all Airflow containers). Once the services are running, access the Airflow UI at:
*   **URL**: [http://localhost:8089](http://localhost:8089)
*   **Username**: `airflow`
*   **Password**: `airflow`

### 4. Execute & Reproduce Results

Once the services are running, you can orchestrate the data ingestion and transformation via the Airflow UI.

*   **Standard Operations**: For daily runs and general management, see the **[Operations Guide](docs/OPERATIONS.md)**.
*   **Reproduce Analysis Results**: To reproduce the specific outputs submitted for this assessment (covering `2025-01-28` to `2026-01-28`), please follow the step-by-step **[Reproduction Instructions](docs/OPERATIONS.md#reproducing-assessment-results)**.

## Documentation & Analysis

The project includes detailed documentation and the final analysis report:

*   **[Architecture & Design](docs/ARCHITECTURE.md)**: A deep dive into the system design, including the WAP pattern, data layers (bronze, silver, gold), and design trade-offs.
*   **[Data Quality](docs/DATA_QUALITY.md)**: An introduction to how data quality is implemented/observed throughout the pipeline.
*   **[Data Dictionary](docs/DATA_DICTIONARY.md)**: Detailed schema definitions for the datasets.
*   **[Analysis Report](reports/ANALYSIS.md)**: The written analysis answering the assessment questions (e.g., Bitcoin vs. Fiat volatility, DCA vs. Lump Sum).
*   **[Extra Credit](reports/EXTRA_CREDIT.md)**: Solutions for the extra credit questions (Etherscan & Dune Analytics).
*   **[Operations Guide](docs/OPERATIONS.md)**: Instructions for accessing the UI, triggering DAGs, and performing manual backfills.

## Testing & Development

For instructions on setting up a local development environment, running unit tests with `pytest`, and executing dbt models locally, please refer to the **[Development & Testing Guide](docs/DEVELOPMENT.md)**. 

When running the Docker containers locally, they can be accessed internally like:

```
docker exec -it bitcoin-analysis-postgres-1 psql -U airflow -d pipeline_metadata
```

This is beneficial when troubleshooting container-specific issues, like checking if Airflow's Redis server is active or not:

```
docker exec bitcoin-analysis-redis-1 redis-cli ping
```

## Project Structure

```text
.
├── airflow/            # Airflow DAGs, plugins, and custom operators
├── data/               # Local data lake (Bronze/Silver/Gold layers)
├── dbt/                # dbt project for data transformation and modeling
├── docs/               # Project documentation (Architecture, Data Dictionary)
├── reports/            # Final analysis and extra credit reports
├── tests/              # Unit and integration tests
└── docker-compose.yaml # Container orchestration configuration
```