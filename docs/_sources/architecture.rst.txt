Architecture
============

Overview
--------

The Finance Data Pipeline consists of two main stages:

1. **Active Tickers Check** — Validates 106K tickers against Yahoo Finance API to determine which are active
2. **Financial Data ETL** — Fetches financial statements for active tickers and stores them in PostgreSQL

.. image:: images/pipeline_flow.svg
   :width: 700
   :align: center
   :class: rounded

|

Pipeline Flow
-------------

.. code-block:: text

    ┌─────────────────────────────────────────────────────────────────────┐
    │                        STAGE 1: Ticker Validation                    │
    ├─────────────────────────────────────────────────────────────────────┤
    │                                                                     │
    │   tickers_list.xlsx ──▶ ThreadPoolExecutor ──▶ active_tickers table │
    │   (106K tickers)        (30 threads)           (is_active: bool)    │
    │                                                                     │
    │   • Skips already-checked tickers                                   │
    │   • Short-circuits on first valid API response                      │
    │   • Batches of 100 with parallel HTTP calls                         │
    │                                                                     │
    └─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────────┐
    │                     STAGE 2: Financial Data ETL                      │
    ├─────────────────────────────────────────────────────────────────────┤
    │                                                                     │
    │   active_tickers ──▶ Priority Query ──▶ ThreadPoolExecutor ──▶ DB   │
    │   (is_active=true)   (new first,        (10 threads)                │
    │                       then oldest)                                   │
    │                                                                     │
    │   4 Independent Jobs:                                               │
    │   • income_stmt         (yahoo_api_income_statement)                │
    │   • cash_flow           (yahoo_api_cash_flow)                       │
    │   • balance_sheet       (yahoo_api_balance_sheet)                   │
    │   • financials          (yahoo_api_financials)                      │
    │                                                                     │
    └─────────────────────────────────────────────────────────────────────┘

Concurrency Model
-----------------

The pipeline uses Python's ``concurrent.futures`` for parallelism:

**Single Mode** (default):
    One process with a ``ThreadPoolExecutor``. Ideal for CI runners and small machines.
    Threads are perfect here because the workload is I/O-bound (HTTP requests to Yahoo).

**Distributed Mode**:
    ``ProcessPoolExecutor`` spawns multiple processes, each with its own thread pool.
    Useful for machines with many CPU cores to maximize network throughput.

.. code-block:: text

    Single Mode:
    ┌──────────────────────────────────────┐
    │ Process                              │
    │  ├── Thread 1 ──▶ check ticker A    │
    │  ├── Thread 2 ──▶ check ticker B    │
    │  ├── Thread 3 ──▶ check ticker C    │
    │  └── ...          (up to N threads) │
    └──────────────────────────────────────┘

    Distributed Mode:
    ┌────────────────┐  ┌────────────────┐  ┌────────────────┐
    │ Process 1      │  │ Process 2      │  │ Process 3      │
    │  ├── Thread 1  │  │  ├── Thread 1  │  │  ├── Thread 1  │
    │  ├── Thread 2  │  │  ├── Thread 2  │  │  ├── Thread 2  │
    │  └── ...       │  │  └── ...       │  │  └── ...       │
    └────────────────┘  └────────────────┘  └────────────────┘

Technology Stack
----------------

- **Python 3.11+** — Core language
- **stockdex** — Yahoo Finance API wrapper (`PyPI <https://pypi.org/project/stockdex/>`_)
- **SQLAlchemy** — Database ORM and connection pooling
- **pandas** — Data transformation (wide → long format)
- **PostgreSQL (Neon)** — Cloud-hosted database with connection pooling
- **GitHub Actions** — CI/CD and scheduled job execution
