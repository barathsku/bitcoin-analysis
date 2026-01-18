# Data Platform Engineer Assessment



Build a data pipeline that ingests, processes, and analyzes the comparative performance of traditional assets (fiat currencies, stocks, and market indices) against Bitcoin. This assignment assesses your ability to design scalable data pipelines, implement robust data models, and ensure data quality while working with multiple external APIs.

---

## Objective

Create a data pipeline to compare the performance of traditional assets versus bitcoin in the last 30 days. Your submission should include working code and data output in CSV.

## Requirements
Massive API (https://massive.com/docs)
- Fiat Currencies: USD, EUR, GBP exchange rates
- Stocks: AAPL (Apple), GOOGL (Google/Alphabet), MSFT (Microsoft)
- Market Index: S&P 500 (SPY ETF as proxy)
- Time Range: Last 365 days of daily data

CoinGecko API (https://docs.coingecko.com/reference/introduction)
- Cryptocurrency: Bitcoin (BTC) price in USD
- Time Range: Last 365 days of daily data

Additionally, your solution should handle fetching and storing historical data for a new date range (e.g., extending from 30 days to 90 days) or specific datetime range in the past without duplicating existing records.


## Technical Requirements

**Pipeline Architecture**
- Design a modular, scalable pipeline architecture
- Implement proper error handling and retry mechanisms
- Support both batch and incremental data loading

**Data Modeling**
- Design a star schema with appropriate fact and dimension tables
- Include dimensions for: assets, dates, asset types
- Create fact table(s) for daily prices and calculated metrics

**Data Reports**
- Calculate daily returns for each asset
- Compute relative performance vs Bitcoin (percentage difference)
- Generate 7-day, 14-day, and 30-day rolling performance metrics
- Calculate volatility (standard deviation of returns)

**Data Quality**
- Implement data validation checks (completeness, accuracy, consistency)
- Create data quality metrics and monitoring
- Handle missing data appropriately
- Implement data lineage tracking

**Output Requirements**
- Store processed data in a queryable format (CSV, Parquet, or database)
- Create summary statistics showing:
    - Best/worst performing assets vs Bitcoin
    - Volatility comparison
    - Correlation analysis


## Deliverables
1. Code Repository
Your submission should include a README that includes at least a setup guide, dependencies and other relevant information for the reviewing team.

2. Documentation
- Architecture diagram showing data flow
- Data dictionary describing all tables and fields
- Setup and execution instructions
- Design decisions and trade-offs

3. Written Analysis 
- Provide a summary which answers the following question:
- Which asset outperformed Bitcoin across each time window (5Y, 1Y, YTD, 6M, 3M, 1M, 7D)?
- What is the current worth of a $1K USD vs Bitcoin investment made one year ago?
- How do returns compare for $100/month dollar-cost averaging into Bitcoin over 12 months vs an initial lump sum?
- Based on these numbers, which was more volatile: fiat currencies or Bitcoin?


## Scoring Guide
All submissions will be evaluated based on the following criteria:
- Technical Implementation including code quality, proper use of libraries and error handling. 
- Data Architecture, Modelling and Storage including scalability, schema evolution and performance 
- Tooling and Operationals including automated tests, data quality and observability. 
- Documentation including readability and clear articulation of design decision and tradeoffs. 

## Constraints
- Use Python as the primary language
- Free API tiers are sufficient (Massive: 5 requests/minute, CoinGecko: 10-50 requests/minute)



## Extra Credit
The following are extra credit questions. You may choose to complete, partially complete or skip this portion of the assessment in your application. 
1. Using Etherscan, find recent swap transactions for the USDC/WETH pool on Uniswap V2 (address: 0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc). 
- Provide a screenshot showing at least 5 recent Swap events from the Events tab.
- Choose a single swap and identify the values for: sender, amount0In, amount1In, amount0Out, amount1Out, to. 
- Identify the tokens: token0 and token1
- Convert the amount to human-readable values

2. Using Dune Analytics, query the uniswap_v2_ethereum.trades for the following datasets
- Top 10 token pairs by swap count in the last 7 days
- Total USD volume per pair
- Number of unique traders (takers) per pair

Share your query URL in your submission

---

## Submission
- GitHub repository with all code and documentation
- README with setup and execution instructions to reproduce outputs and results
- Sample output data and analysis results
- Brief presentation slides (optional) explaining your approach
