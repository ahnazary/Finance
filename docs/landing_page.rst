Finance Data Pipeline
=====================

.. image:: images/architecture_diagram.svg
   :width: 700
   :align: center
   :class: rounded

|

An automated financial data pipeline that collects stock financial statements from Yahoo Finance
for **106,000+ tickers** worldwide and stores them in a cloud-hosted PostgreSQL database.

Key Features
------------

- 🌍 **106K+ Global Tickers** validated against Yahoo Finance API
- 📊 **4 Financial Statement Types**: Income Statement, Cash Flow, Balance Sheet, Financials
- 🗄️ **Long-Format Storage** (EAV schema) for maximum flexibility
- ⚡ **Multi-threaded Execution** for high throughput (configurable concurrency)
- 🔄 **Incremental Updates**: New tickers first, then refresh stale data
- 🤖 **Fully Automated** via GitHub Actions (runs every 6 hours)

Quick Links
-----------

- `GitHub Repository <https://github.com/ahnazary/Finance>`_
- `stockdex Package <https://pypi.org/project/stockdex/>`_ (underlying data source)
