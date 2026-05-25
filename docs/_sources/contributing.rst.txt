Contributing
============

Development Setup
-----------------

1. Clone the repository:

.. code-block:: bash

    git clone https://github.com/ahnazary/Finance.git
    cd Finance

2. Install dependencies:

.. code-block:: bash

    pip install -r finance/requirements.txt
    pip install -r requirements_sphinx.txt

3. Set up environment variables:

.. code-block:: bash

    export PG_NEON_FINANCE_URL="your-connection-string"

Code Style
----------

- **Formatter**: Black (line length 88)
- **Import sorting**: isort
- **Linting**: pylint (minimum score: 1)

Run formatting:

.. code-block:: bash

    black .
    isort .

Building Docs
-------------

.. code-block:: bash

    cd docs
    sphinx-build . build
    # Open build/index.html in browser

Adding a New Financial Table
----------------------------

1. Add the table migration in Alembic (``database-version-control`` repo)
2. Add the stockdex method mapping to ``FINANCIAL_TABLES`` in ``config.py``
3. The ``FinancialDataETL`` class will automatically support it
4. Add a new workflow job in ``financial_data_etl.yaml``
