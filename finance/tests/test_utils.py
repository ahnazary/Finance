import pytest
from sqlalchemy import MetaData, Table, select


def test_tables_exist(setup_tables, test_engine):
    """
    Test to check if the tables are created correctly
    """
    # List of tables in local postgres database in stocks schema
    metadata = MetaData()
    information_schema_tables = Table(
        "tables", metadata, autoload_with=test_engine, schema="information_schema"
    )
    query = (
        select(information_schema_tables.c.table_name)
        .where(information_schema_tables.c.table_schema == "stocks")
        .order_by(information_schema_tables.c.table_name)
    )

    with test_engine.connect() as conn:
        tables = [table[0] for table in conn.execute(query).fetchall()]
    assert tables == [
        "financials",
        "income_statement",
        "balance_sheet",
        "cash_flow",
        "tickers_list",
        "valid_tickers",
    ]
