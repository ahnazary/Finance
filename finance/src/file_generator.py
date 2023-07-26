""" This is a module that generates csv files of of the data in the database."""

from postgres_interface import PostgresInterface

pg_interface = PostgresInterface()
engine = pg_interface.create_engine()


def generate_valid_tickers_csv():
    # create a csv file containing all valid tickers in valid_tickers table
    query = """
    SELECT * FROM stocks.valid_tickers WHERE validity = True
    """

    result = pg_interface.execute(query)
    with open("finance/src/database/valid_tickers.csv", "w") as f:
        for row in result:
            f.write(f"{row}\n")


generate_valid_tickers_csv()
