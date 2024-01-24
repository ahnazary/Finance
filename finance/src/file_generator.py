""" This is a module that generates csv files of of the data in the database."""

from src.postgres_interface import PostgresInterface
from sqlalchemy import MetaData, Table, select

pg_interface = PostgresInterface()
engine = pg_interface.create_engine()


def generate_valid_tickers_csv():
    # create a csv file containing all valid tickers in valid_tickers table
    valid_tickers = Table(
        "valid_tickers", MetaData(), autoload_with=engine, schema="stocks"
    )
    query = select(valid_tickers).where(valid_tickers.columns.validity)

    schema_table = Table(
        "columns", MetaData(), autoload_with=engine, schema="information_schema"
    )
    columns_query = select(schema_table).where(
        schema_table.columns.table_name == "valid_tickers"
    )

    with engine.connect() as conn:
        columns = [row[3] for row in conn.execute(columns_query).fetchall()]

    result = pg_interface.execute(query)
    with open("finance/src/database/valid_tickers.csv", "w") as file_handler:
        file_handler.write(f"{columns}\n")
        for row in result:
            file_handler.write(f"{row}\n")


if __name__ == "__main__":
    generate_valid_tickers_csv()
