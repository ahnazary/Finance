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

import sqlalchemy
import yfinance as yf
from sqlalchemy import asc, func, select
from src.postgres_interface import PostgresInterface
from src.utils import custom_logger

from finance.src.yahoo_ticker import Ticker


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
        batch_size: int = 500,
    ):
        """
        Parameters
        ----------
        provider : str
            provider of the data
            As of now, only "LOCAL" and "NEON" are supported
        batch_size : int
            size of the batch to insert into the database for each table
            default: 500
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
        table_name: str,
        engine: sqlalchemy.engine.Engine,
        frequency: str = "annual",
    ) -> list:
        """
        TODO: make this docs better
        TODO: make the query better

        Method to get a batch of tickers from valid_tickers table that have not
        been inserted into other main tables (financials, balance_sheet,
        cashflow, etc.).


        Parameters
        ----------
        table_name : str
            name of the table to get the tickers from
        engine : sqlalchemy.engine.Engine
            engine to connect to the database, defines if it is local or neon
        frequency : str
            frequency of the data (either annual or quarterly)

        The query is equivalent to (for cahsflow table):
            select valid_tickers.ticker, valid_tickers.cashflow_annual_available,
            subquery.max_insert_date
            from stocks.valid_tickers
            left join (
            select cashflow.ticker, max(cashflow.insert_date) as max_insert_date
            from stocks.cashflow
            group by cashflow.ticker
            ) as subquery
            on valid_tickers.ticker = subquery.ticker
            where valid_tickers.cashflow_annual_available
            order by subquery.max_insert_date asc

        Returns
        -------
        list
            list of tickers
        """

        valid_tickers_table = self.postgres_interface.create_table_object(
            "valid_tickers", engine
        )
        table = self.postgres_interface.create_table_object(table_name, engine)

        # this is the column that will be used to check if the ticker is available
        availablility_column = getattr(
            valid_tickers_table.c, f"{table_name}_{frequency}_available"
        )

        subquery = (
            select(
                table.c.ticker,
                func.max(table.c.insert_date).label("max_insert_date"),
            )
            .group_by(table.c.ticker)
            .alias()
        )

        query = (
            select(valid_tickers_table.c.ticker, availablility_column)
            .join(
                subquery,
                valid_tickers_table.c.ticker == subquery.c.ticker,
                isouter=True,
            )
            .order_by(asc(subquery.c.max_insert_date))
            .where(availablility_column)
        )

        with engine.connect() as conn:
            result = conn.execute(query).fetchmany(self.batch_size)

        return [result[0] for result in result]

    def update_validy_in_valid_tickers_table(
        self, ticker: list, validity: bool = False
    ):
        """
        Method that gets a list of tickers and updates their validity in the
        valid_tickers table
        """

        valid_tickers_table = self.postgres_interface.create_table_object(
            "valid_tickers", self.engine
        )

        query = (
            valid_tickers_table.update()
            .where(valid_tickers_table.c.ticker.in_(ticker))
            .values(validity=validity)
        )

        with self.engine.connect() as conn:
            conn.execute(query)

    def get_tickers_batch_yf_object(self, tickers_list: list) -> list[yf.Ticker]:
        """
        Method to get a batch of yfinance tickers from a list of tickers

        Parameters
        ----------
        tickers : list
            list of tickers to get the yfinance tickers from
        """
        return [yf.Ticker(ticker) for ticker in tickers_list]

    def run_pipeline(self):
        """
        Main method that each of the jobs in the CI/CD pipeline will run
        It includes steps like:

        - getting a batch of tickers to update from valid_tickers table
        - extracting data from yfinance for each ticker
        - inserting the data into the database
        - updating the validity of the tickers in valid_tickers table
        """
        self.logger.info(
            f"""Running pipeline for {self.table_name} with {self.provider}
            provider and {self.frequency} frequency"""
        )

        # getting a list[str] of old tickers with batch_size
        tickers_list = self.tickers_to_query(
            table_name=self.table_name, engine=self.engine, frequency=self.frequency
        )
        tickers_list = [x for x in tickers_list if x is not None]

        # getting a list[yf.Ticker] of old tickers with batch_size
        tickers_yf_batch = self.get_tickers_batch_yf_object(tickers_list=tickers_list)

        ticker_interface = Ticker(frequency=self.frequency)

        table_columns = ticker_interface.get_columns_names(table_name=self.table_name)

        records = []
        invalid_tickers = []
        for ticker_yf_obj in tickers_yf_batch:
            record = ticker_interface.extract_tickers_data(
                ticker=ticker_yf_obj,
                table_name=self.table_name,
                table_columns=table_columns,
            )
            if record is not None:
                records.append(record)
                self.logger.warning(
                    f"record: {record} has been added to records, records length: {len(records)}"
                )
            else:
                invalid_tickers.append(ticker_yf_obj.ticker)
                self.logger.warning(
                    f"""ticker: {ticker_yf_obj.ticker} has been added to invalid_tickers,
                    invalid_tickers length: {len(invalid_tickers)}"""
                )

        # Update availability status in valid_tickers table
        ticker_interface.update_validity_status(
            table_name=self.table_name,
            tickers=invalid_tickers,
            availability=False,
        )

        # convert list[list[dict]] to list[dict]
        flattened_records = [item for sublist in records for item in sublist]

        # flush records to the database
        ticker_interface.flush_records(
            table_name=self.table_name,
            records=flattened_records,
        )
