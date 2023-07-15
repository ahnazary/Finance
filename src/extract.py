from typing import List, Literal, Union

import pandas as pd
import yfinance as yf
from sqlalchemy import text
from src.postgres_interface import PostgresInterface
from .utils import are_incremental


class Ticker:
    def __init__(self, countries: Union[str, List[str]] = None):
        self.postgres_interface = PostgresInterface()
    
    def update_tickers_list_table(self):
        """
        Method to update the tickers_list table in postgres
        Gets all the data in the data dir excel file and inserts it into the database
        """
        df = pd.read_excel("src/data/tickers_list.xlsx")
        # rename columns to match the database
        df.rename(
            columns={
                "Ticker": "ticker",
                "Name": "name",
                "Exchange": "exchange",
                "Category Name": "category_name",
                "Country": "country",
            },
            inplace=True,
        )

        engine = self.postgres_interface.create_engine()

        # insert the data into the database
        df.to_sql(
            name="tickers_list",
            con=engine,
            if_exists="replace",
            schema="stocks",
            index=False,
            method="multi",
            chunksize=1000,
        )