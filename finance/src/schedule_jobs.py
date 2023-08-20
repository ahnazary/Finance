from logging import getLogger

import sqlalchemy
from sqlalchemy import asc, func, select
from src.constants import CURRENCIES
from src.extract import Ticker
from src.postgres_interface import PostgresInterface


class ScheduleJobs:
    def __init__(self, provider: str, batch_size: int = 500):
        self.logger = getLogger(__name__)
        self.postgres_interface = PostgresInterface()
        self.batch_size = batch_size
        self.provider = provider

        # create engines to connect to the databases
        self.engine = self.postgres_interface.create_engine(provider=provider)

    def get_tickers_batch(self, table_name: str, engine: sqlalchemy.engine.Engine):
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

    def update_table_batch(self, table_name: str, engine: sqlalchemy.engine.Engine):
        tickers = self.get_tickers_batch(table_name=table_name, engine=engine)

        ticker_interface = Ticker(provider=self.provider)
        ticker_interface.update_cash_flow(engine=engine, tickers=tickers)
