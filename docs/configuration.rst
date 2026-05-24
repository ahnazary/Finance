Configuration
=============

All pipeline parameters are centralized in ``config.py`` at the project root.

config.py
---------

.. code-block:: python

    # --- Active Tickers Check Job ---
    ACTIVE_TICKERS_BATCH_SIZE = 100
    ACTIVE_TICKERS_THREADS = 30

    # --- Financial Data ETL Jobs ---
    ETL_BATCH_SIZE = 50
    ETL_THREADS = 10

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

Environment Variables
---------------------

.. list-table::
   :header-rows: 1

   * - Variable
     - Required
     - Description
   * - ``PG_NEON_FINANCE_URL``
     - Yes (in CI)
     - PostgreSQL connection string for Neon database

The connection string is stored as a GitHub Secret and injected into workflows.
For local development, it falls back to a hardcoded default in ``postgres_interface.py``.

Dependencies
------------

All Python dependencies are listed in ``finance/requirements.txt``:

- **pandas** — Data manipulation and melting
- **openpyxl** — Reading Excel ticker files
- **python-dotenv** — Environment variable loading
- **SQLAlchemy** — Database ORM and connection management
- **psycopg2-binary** — PostgreSQL driver
- **stockdex** — Yahoo Finance API wrapper
