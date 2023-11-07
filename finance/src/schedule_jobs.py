"""
Module that schedules jobs in the CI/CD pipeline use to update the database

This module contains Methods and classes that can be reused over all the
different jobs in the CI/CD pipeline
"""

import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

import sqlalchemy
import yfinance as yf
from sqlalchemy import asc, func, select, distinct
from src.extract import Ticker
from src.postgres_interface import PostgresInterface
from src.utils import custom_logger

from config import CURRENCIES


class ScheduleJobs:
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
        self.engine = self.postgres_interface.create_engine(provider=provider)

    def get_tickers_batch(
        self,
        table_name: str,
        engine: sqlalchemy.engine.Engine,
        frequency: str = "annual",
    ) -> list:
        """
        Method to get a batch of oldest tickers from a table that have not #
        been updated for a while

        Parameters
        ----------
        table_name : str
            name of the table to get the tickers from
        engine : sqlalchemy.engine.Engine
            engine to connect to the database, defines if it is local or neon
        """

        table = self.postgres_interface.create_table_object(table_name, engine)
        query = (
            select(
                table.c.ticker,
                func.max(table.c.insert_date).label("latest_insert_date"),
            )
            .where(table.c.currency_code.in_(CURRENCIES))
            .where(table.c.frequency == frequency)
            .group_by(table.c.ticker)
            .order_by(asc("latest_insert_date"))
        )

        with engine.connect() as conn:
            result = conn.execute(query).fetchmany(self.batch_size)

        return [result[0] for result in result]

    def get_tickers_batch_backfill(
        self,
        table_name: str,
        engine: sqlalchemy.engine.Engine,
        frequency: str = "annual",
    ) -> list:
        """
        TODO: make this docs better
        TODO: make the query better

        Method to get a batch of tickers from valid_tickers table that have not
        been been inserted into other main tables (financials, balance_sheet,
        cashflow, etc.)


        Parameters
        ----------
        table_name : str
            name of the table to get the tickers from
        engine : sqlalchemy.engine.Engine
            engine to connect to the database, defines if it is local or neon
        frequency : str
            frequency of the data (either annual or quarterly)

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
        available_column = getattr(
            valid_tickers_table.c, f"{table_name}_{frequency}_available"
        )
        query = (
            # join valid_tickers table with the table on ticker column and get only one row 
            # from table with latest insert_date
            select(
                distinct(table.c.ticker),
                func.max(table.c.insert_date).label("latest_insert_date"),
            )
            .select_from(table.join(valid_tickers_table, table.c.ticker == valid_tickers_table.c.ticker))
            .where(table.c.currency_code.in_(CURRENCIES))
            .where(table.c.frequency == frequency)
            .where(available_column == True)
            .group_by(table.c.ticker)
        )

        with engine.connect() as conn:
            result = conn.execute(query).fetchmany(self.batch_size)

        if len(result) == 0:
            # sort table by insert_date and get the oldest tickers by batch_size
            query = (
                select(
                    table.c.ticker,
                    func.max(table.c.insert_date).label("latest_insert_date"),
                )
                .where(table.c.currency_code.in_(CURRENCIES))
                .where(table.c.frequency == frequency)
                .group_by(table.c.ticker)
                .order_by(asc("latest_insert_date"))
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
        tickers_list = self.get_tickers_batch_backfill(
            table_name=self.table_name, engine=self.engine, frequency=self.frequency
        )
        tickers_list = [x for x in tickers_list if x is not None]

        # getting a list[yf.Ticker] of old tickers with batch_size
        tickers_yf_batch = self.get_tickers_batch_yf_object(tickers_list=tickers_list)

        ticker_interface = Ticker(provider=self.provider, frequency=self.frequency)

        table_columns = ticker_interface.get_columns_names(table_name=self.table_name)

        records = []
        invalid_tickers = []
        for ticker_yf_obj in tickers_yf_batch:
            record = ticker_interface.update_table(
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
                    f"ticker: {ticker_yf_obj.ticker} has been added to invalid_tickers, invalid_tickers length: {len(invalid_tickers)}"
                )

        # Update availability status in valid_tickers table
        ticker_interface.update_validity_status(
            table_name=self.table_name,
            tickers=invalid_tickers,
            availability=False,
        )

        # convert list[list[dict]] to list[dict]
        flattened_records = [item for sublist in records for item in sublist]

        # flush records to database all at once
        ticker_interface.flush_records(
            table_name=self.table_name, records=flattened_records
        )
