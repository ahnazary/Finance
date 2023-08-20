from logging import getLogger
from src.postgres_interface import PostgresInterface
import sqlalchemy
from sqlalchemy import func, select, asc
from src.extract import Ticker
from src.constants import CURRENCIES


class ScheduleJobs:
    def __init__(self, batch_size: int = 500):
        self.logger = getLogger(__name__)
        self.postgres_interface = PostgresInterface()
        self.batch_size = batch_size

        # create engines to connect to the databases
        engines = self.postgres_interface.create_engine()
        self.engine_local = engines["local"]
        self.engine_neon = engines["neon"]

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

    def update_table_batch(
        self, table_name: str, engine: sqlalchemy.engine.Engine
    ):
        tickers = self.get_tickers_batch(table_name=table_name, engine=engine)
        
        ticker_interface = Ticker()
        ticker_interface.update_cash_flow(engine=engine, tickers=tickers)
