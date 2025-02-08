"""
Module that schedules jobs in the CI/CD pipeline use to update the database

This module contains Methods and classes that can be reused over all the
different jobs in the CI/CD pipeline
"""

import os
import sys

import pandas as pd

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

from datetime import datetime

import yfinance as yf
from sqlalchemy import text
from src.postgres_interface import PostgresInterface
from src.utils import custom_logger
from stockdex import Ticker

from config import BATCH_SIZE


class Jobs:
    """
    Class that schedules jobs in the CI/CD pipeline use for updating the
    database and extracting data from yfinance
    """

    def __init__(
        self,
        provider: str,
        table_name: str,
        frequency: str = "annual",
        batch_size: int = BATCH_SIZE,
    ):
        """
        Parameters
        ----------
        provider : str
            provider of the data
            As of now, only "LOCAL" and "NEON" are supported
        batch_size : int
            size of the batch to insert into the database for each table
            default: BATCH_SIZE from config.py
        """

        self.logger = custom_logger(logger_name="schedule_jobs")
        self.batch_size = batch_size
        self.provider = provider
        self.table_name = table_name
        self.frequency = frequency

        self.postgres_interface = PostgresInterface()

        # create engines to connect to the databases
        self.engine = self.postgres_interface.create_engine()

    def fill_valid_tickers_table(self):
        """
        Method that reads valid tickers from finance/src/database/valid_tickers.csv
        and inserts them into the valid_tickers table in the database
        """

        pd.read_csv("finance/src/database/valid_tickers.csv")[
            ["ticker", "currency_code", "validity"]
        ].to_sql(
            "valid_tickers",
            con=self.engine,
            if_exists="replace",
            index=False,
            schema="finance",
        )

    def tickers_to_query(
        self,
        frequency: str = "annual",
    ) -> list[tuple[str, str]]:
        """
        Parameters
        ----------
        table_name : str
            name of the table to get the tickers from
        frequency : str
            frequency of the data (either annual or quarterly)
        """

        # This query will return tickers as long as not all the tickers have been
        # processed
        query = text(
            f"""
            select vt.ticker, vt.currency_code
            from finance.valid_tickers vt left join finance.{self.table_name} t
            on vt.ticker = t.ticker
            where vt.validity = 'true' and t.insert_date is null
            limit {BATCH_SIZE}
            """
        )

        with self.engine.connect() as connection:
            result = connection.execute(query)
            tickers = [(row[0], row[1]) for row in result]
            if tickers:
                return tickers
            else:
                query = text(
                    f"""
                    select distinct vt.ticker, vt.currency_code, t.insert_date
                    from finance.valid_tickers vt left join finance.{self.table_name} t
                    on vt.ticker = t.ticker
                    where vt.validity = 'true'
                    order by t.insert_date asc
                    limit {BATCH_SIZE}
                    """
                )
                result = connection.execute(query)
                return [(row[0], row[1]) for row in result]

    def get_tickers_batch_yf_object(self, tickers_list: list) -> list[yf.Ticker]:
        """
        Method to get a batch of yfinance tickers from a list of tickers

        Parameters
        ----------
        tickers : list
            list of tickers to get the yfinance tickers from
        """
        return [yf.Ticker(ticker) for ticker in tickers_list]

    def run_pipeline(self, attribute: str, frequency: str = "annual"):
        """
        Main method that each of the jobs in the CI/CD pipeline will run
        """
        self.logger.info(
            f"""Running pipeline for {self.table_name} with {self.provider}
            provider and {self.frequency} frequency"""
        )

        insert_df = pd.DataFrame(
            columns=[
                "ticker",
                "insert_date",
                "report_date",
                "currency_code",
                "frequency",
                "data",
            ]
        )

        # getting a list[str] of old tickers with batch_size
        tickers_list = self.tickers_to_query(frequency=self.frequency)

        for ticker in tickers_list:
            data = Ticker(ticker[0]).__getattribute__(f"yahoo_api_{attribute}")()

            for row in data.iterrows():
                insert_df.loc[-1] = [
                    ticker[0],
                    datetime.now(),
                    row[0],
                    ticker[1],
                    frequency,
                    row[1].to_json(),
                ]
                insert_df.index = insert_df.index + 1
                insert_df = insert_df.sort_index()

        insert_df.to_sql(
            self.table_name,
            con=self.engine,
            if_exists="append",
            index=False,
            schema="finance",
        )
