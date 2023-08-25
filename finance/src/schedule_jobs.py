from logging import getLogger

import sqlalchemy
import yfinance as yf
from sqlalchemy import asc, func, select
from src.constants import CURRENCIES
from src.extract import Ticker
from src.postgres_interface import PostgresInterface


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
        self, table_name: str, engine: sqlalchemy.engine.Engine
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
            .group_by(table.c.ticker)
            .order_by(asc("latest_insert_date"))
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

    # def update_table_batch(self, table_name: str, engine: sqlalchemy.engine.Engine):
    #     """
    #     Method to update a table
    #     """
    #     tickers = self.get_tickers_batch(table_name=table_name, engine=engine)

    #     ticker_interface = Ticker(provider=self.provider)
    #     ticker_interface.update_cash_flow(engine=engine, tickers=tickers)
