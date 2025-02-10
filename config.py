# Description: Configuration file for the application

# Number of tickers to be processed in a batch for jobs that fill the database
BATCH_SIZE = 100

# Only stocks that have earnings released in the following currencies will be processed
CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]

# warning log level
LOG_LEVEL = 20

# List of tables to be backed up through backup workflow
# (read tables into parquet files and load them into s3)
BACKUP_TABLES = [
    "balance_sheet",
    "cashflow",
    "financials",
    "income_stmt",
    "valid_tickers",
]
