import os
import sys
from logging import getLogger

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

import sqlalchemy
import yfinance as yf
from sqlalchemy import asc, func, select
from src.postgres_interface import PostgresInterface

from config import CURRENCIES


class ScheduleJobs:
    """
    Class that schedules jobs in the CI/CD pipeline use to update the database
    """

    def __init__(self, provider: str, batch_size: int = 500):
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

        self.logger = getLogger(__name__)
        self.postgres_interface = PostgresInterface()
        self.batch_size = batch_size
        self.provider = provider

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

        Method to get a batch of tickers from valid_tickers table that have not
        been been inserted into other main tables (financials, balance_sheet,
        cash_flow, etc.)


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
        query = select(valid_tickers_table.c.ticker).where(
            valid_tickers_table.c.ticker.notin_(
                select(table.c.ticker).where(table.c.frequency == frequency)
            )
        )

        with engine.connect() as conn:
            result = conn.execute(query).fetchmany(self.batch_size)

        return [result[0] for result in result]

    def get_tickers_batch_yf_object(self, tickers_list: list) -> list[yf.Ticker]:
        """
        Method to get a batch of yfinance tickers from a list of tickers

        Parameters
        ----------
        tickers : list
            list of tickers to get the yfinance tickers from
        """
        return [yf.Ticker(ticker) for ticker in tickers_list]
