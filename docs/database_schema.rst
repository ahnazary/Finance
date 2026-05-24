Database Schema
===============

The database uses a **long (melted) format** also known as EAV (Entity-Attribute-Value).
This design was chosen because different companies report different metrics, making a
wide-column approach impractical at scale.

Schema: ``finance``
-------------------

All tables live in the ``finance`` schema on Neon PostgreSQL.

active_tickers
~~~~~~~~~~~~~~

Tracks which of the 106K tickers are valid/active on Yahoo Finance.

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Column
     - Type
     - Description
   * - ``ticker`` (PK)
     - VARCHAR(20)
     - Stock ticker symbol (e.g., "AAPL", "MSFT")
   * - ``name``
     - VARCHAR
     - Company name
   * - ``exchange``
     - VARCHAR
     - Exchange code (e.g., "NYQ", "NMS")
   * - ``category_name``
     - VARCHAR
     - Industry category
   * - ``country``
     - VARCHAR
     - Country of origin
   * - ``is_active``
     - BOOLEAN
     - Whether Yahoo API returns data for this ticker
   * - ``upsert_datetime``
     - TIMESTAMP
     - When this record was last updated

Financial Statement Tables
~~~~~~~~~~~~~~~~~~~~~~~~~~

All four tables (``income_stmt``, ``cash_flow``, ``balance_sheet``, ``financials``)
share the same schema:

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Column
     - Type
     - Description
   * - ``ticker``
     - VARCHAR
     - Stock ticker symbol
   * - ``frequency``
     - VARCHAR
     - "annual" or "quarterly"
   * - ``report_date``
     - DATE
     - Financial report date
   * - ``metric``
     - VARCHAR
     - Metric name (e.g., "annualTotalRevenue")
   * - ``value``
     - DOUBLE PRECISION
     - Numeric value (parsed from B/M/K suffixes)
   * - ``insert_datetime``
     - TIMESTAMP
     - When this row was inserted

ER Diagram
----------

.. code-block:: text

    ┌──────────────────────┐
    │   active_tickers     │
    ├──────────────────────┤        ┌──────────────────────┐
    │ ticker (PK)          │───────▶│   income_stmt        │
    │ name                 │        ├──────────────────────┤
    │ exchange             │        │ ticker               │
    │ category_name        │        │ frequency            │
    │ country              │        │ report_date          │
    │ is_active            │        │ metric               │
    │ upsert_datetime      │        │ value                │
    └──────────────────────┘        │ insert_datetime      │
              │                     └──────────────────────┘
              │
              ├────────────────────▶ cash_flow (same schema)
              │
              ├────────────────────▶ balance_sheet (same schema)
              │
              └────────────────────▶ financials (same schema)

Example Queries
---------------

**Get latest revenue for all tickers:**

.. code-block:: sql

    SELECT ticker, report_date, value
    FROM finance.income_stmt
    WHERE metric = 'annualTotalRevenue'
    ORDER BY ticker, report_date DESC;

**Find top 10 companies by net income:**

.. code-block:: sql

    SELECT DISTINCT ON (ticker) ticker, value
    FROM finance.income_stmt
    WHERE metric = 'annualNetIncome'
      AND frequency = 'annual'
    ORDER BY ticker, report_date DESC, value DESC
    LIMIT 10;

**Count tickers per exchange:**

.. code-block:: sql

    SELECT exchange, COUNT(*) as count
    FROM finance.active_tickers
    WHERE is_active = true
    GROUP BY exchange
    ORDER BY count DESC;
