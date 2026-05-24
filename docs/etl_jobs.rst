ETL Jobs
========

The pipeline has two types of jobs:

1. Active Tickers Check
-----------------------

**Purpose**: Determine which of the 106K tickers in the Excel file are active on Yahoo Finance.

**Module**: ``finance.src.etl_job.ETLJob``

**Runner**: ``finance.src.run_active_tickers_check``

**Logic**:

1. Read all tickers from ``tickers_list.xlsx``
2. Query DB for already-checked tickers → skip them
3. For each remaining ticker (in parallel):
   - Create a ``stockdex.Ticker`` object
   - Try each of the 4 Yahoo API methods
   - Short-circuit: return ``True`` on first valid response
4. Upsert batch results into ``active_tickers`` table

**CLI Usage**:

.. code-block:: bash

    # Run with defaults (100 batch, 30 threads)
    python -m finance.src.run_active_tickers_check

    # Custom settings
    python -m finance.src.run_active_tickers_check --mode single --threads 20 --max-batches 10

    # Distributed mode (multi-process)
    python -m finance.src.run_active_tickers_check --mode distributed --threads 20

**Arguments**:

.. list-table::
   :header-rows: 1

   * - Argument
     - Default
     - Description
   * - ``--mode``
     - single
     - Execution mode: "single" or "distributed"
   * - ``--threads``
     - 20
     - Max concurrent threads per process
   * - ``--max-batches``
     - None (all)
     - Stop after N batches

2. Financial Data ETL
---------------------

**Purpose**: Fetch financial statement data for active tickers and store in long format.

**Module**: ``finance.src.financial_data_etl.FinancialDataETL``

**Runner**: ``finance.src.run_financial_etl``

**Logic**:

1. Query ``active_tickers`` for priority tickers:
   - **First priority**: Active tickers NOT yet in the target table
   - **Second priority**: Active tickers with oldest ``insert_datetime``
2. Fetch financial data in parallel using stockdex
3. Melt wide-format data into long format (handle B/M/K/T suffixes)
4. Delete + Insert (upsert) into target table

**CLI Usage**:

.. code-block:: bash

    # Run for a specific table
    python -m finance.src.run_financial_etl --table income_stmt
    python -m finance.src.run_financial_etl --table cash_flow
    python -m finance.src.run_financial_etl --table balance_sheet
    python -m finance.src.run_financial_etl --table financials

    # With custom settings
    python -m finance.src.run_financial_etl --table income_stmt --batch-size 100 --threads 15 --max-batches 5

**Arguments**:

.. list-table::
   :header-rows: 1

   * - Argument
     - Default
     - Description
   * - ``--table``
     - (required)
     - Target table: income_stmt, cash_flow, balance_sheet, financials
   * - ``--batch-size``
     - 50
     - Number of tickers per batch
   * - ``--threads``
     - 10
     - Concurrent threads for Yahoo API calls
   * - ``--max-batches``
     - None (all)
     - Stop after N batches

**Priority Query**:

.. code-block:: sql

    WITH active AS (
        SELECT ticker FROM finance.active_tickers WHERE is_active = true
    ),
    existing AS (
        SELECT ticker, MAX(insert_datetime) as last_insert
        FROM finance.<table_name>
        GROUP BY ticker
    )
    SELECT a.ticker
    FROM active a
    LEFT JOIN existing e ON a.ticker = e.ticker
    ORDER BY
        CASE WHEN e.ticker IS NULL THEN 0 ELSE 1 END,
        e.last_insert ASC NULLS FIRST
    LIMIT :batch_size

Data Transformation
-------------------

stockdex returns data in wide format:

.. code-block:: text

    Index (dates)    | annualTotalRevenue | annualNetIncome | ...
    -----------------|--------------------|-----------------|----
    2022-09-30       | 394.33B            | 99.80B          | ...
    2023-09-30       | 383.29B            | 97.00B          | ...

The pipeline melts this into long format:

.. code-block:: text

    ticker | frequency | report_date | metric               | value
    -------|-----------|-------------|-----------------------|---------------
    AAPL   | annual    | 2022-09-30  | annualTotalRevenue   | 394330000000
    AAPL   | annual    | 2022-09-30  | annualNetIncome      | 99800000000
    AAPL   | annual    | 2023-09-30  | annualTotalRevenue   | 383290000000
    ...
