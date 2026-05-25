"""
Configuration file for the Finance data pipeline.
"""

# --- Active Tickers Check Job ---
ACTIVE_TICKERS_BATCH_SIZE = 100
ACTIVE_TICKERS_THREADS = 30

# --- Financial Data ETL Jobs ---
# Number of tickers to fetch per job run
ETL_BATCH_SIZE = 50
# Concurrent threads for fetching financial data from Yahoo
ETL_THREADS = 1

# --- General ---
SCHEMA = "finance"
LOG_LEVEL = 20  # INFO

# Table name -> stockdex method mapping
FINANCIAL_TABLES = {
    "income_stmt": "yahoo_api_income_statement",
    "cash_flow": "yahoo_api_cash_flow",
    "balance_sheet": "yahoo_api_balance_sheet",
    "financials": "yahoo_api_financials",
}
