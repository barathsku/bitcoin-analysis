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

Create a `.env` file in the root directory to configure the environment. You can use the following template:

```bash
AIRFLOW_UID=1000
COINGECKO_API_KEY=your_api_key_here
MASSIVE_API_KEY=your_api_key_here
AIRFLOW_PROJ_DIR=.
AIRFLOW_HOME=/home/bdn/coingecko-assessment
```

### 3. Run the Platform

We use Docker to orchestrate the Airflow services and resources.

```bash
docker compose up -d
```

You will have to wait for 3-4 minutes for the services during first-time startups, and 2-3 minutes for subsequent startups (due to the custom Python libraries being installed across all Airflow containers). Once the services are running, access the Airflow UI at:
*   **URL**: [http://localhost:8089](http://localhost:8089)
*   **Username**: `airflow`
*   **Password**: `airflow`

### 4. Execute the Pipeline

Once the services are running, you can orchestrate the data ingestion and transformation via the Airflow UI.

For detailed instructions on enabling and running the standard pipelines or performing backfills, please refer to the **[Operations Guide](docs/OPERATIONS.md)**.

## Documentation & Analysis

The project includes detailed documentation and the final analysis report:

*   **[Architecture & Design](docs/ARCHITECTURE.md)**: A deep dive into the system design, including the WAP pattern, data layers (bronze, silver, gold), and design trade-offs.
*   **[Data Quality](docs/DATA_QUALITY.md)**: An introduction to how data quality is implemented/observed throughout the pipeline.
*   **[Data Dictionary](docs/DATA_DICTIONARY.md)**: Detailed schema definitions for the datasets.
*   **[Analysis Report](reports/ANALYSIS.md)**: The written analysis answering the assessment questions (e.g., Bitcoin vs. Fiat volatility, DCA vs. Lump Sum).
*   **[Extra Credit](reports/EXTRA_CREDIT.md)**: Solutions for the extra credit questions (Etherscan & Dune Analytics).
*   **[Operations Guide](docs/OPERATIONS.md)**: Instructions for accessing the UI, triggering DAGs, and performing manual backfills.

## Testing

To run the test suite (unit tests and integration tests) for the Airflow plugins and logic:

```bash
docker compose run --rm airflow-scheduler pytest /opt/airflow/tests
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