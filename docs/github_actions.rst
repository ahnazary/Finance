GitHub Actions
==============

The project uses GitHub Actions for CI/CD and scheduled data collection.

Workflows
---------

check_active_tickers.yaml
~~~~~~~~~~~~~~~~~~~~~~~~~~

Validates tickers against Yahoo Finance API every 6 hours.

- **Trigger**: Schedule (every 6h) + manual dispatch
- **Environment**: ``prod`` (has access to ``PG_NEON_FINANCE_URL`` secret)
- **Action**: Runs ``finance.src.run_active_tickers_check``

financial_data_etl.yaml
~~~~~~~~~~~~~~~~~~~~~~~

Fetches financial data for all 4 statement types every 6 hours.

- **Trigger**: Schedule (every 6h) + manual dispatch
- **Environment**: ``prod``
- **Action**: Runs 4 parallel jobs for income_stmt, cash_flow, balance_sheet, financials

pre-deploy.yaml
~~~~~~~~~~~~~~~

Runs linters and format checks on every push/PR to master.

- **Trigger**: Push/PR to master (when ``finance/`` files change)
- **Jobs**: pylint, black format check

sphinx.yaml
~~~~~~~~~~~~

Builds and deploys Sphinx documentation to GitHub Pages.

- **Trigger**: Release published + manual dispatch
- **Action**: Builds docs, deploys to ``gh-pages`` branch

Secrets Required
----------------

.. list-table::
   :header-rows: 1

   * - Secret
     - Description
   * - ``PG_NEON_FINANCE_URL``
     - Full PostgreSQL connection string for Neon database
   * - ``GITHUB_TOKEN``
     - Auto-provided by GitHub for Pages deployment
