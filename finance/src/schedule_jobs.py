from logging import getLogger
from src.postgres_interface import PostgresInterface
import sqlalchemy
from sqlalchemy import func, desc, select, MetaData, Table, asc


class ScheduleJobs:
    def __init__(self, batch_size: int = 500):
        self.logger = getLogger(__name__)
        self.postgres_interface = PostgresInterface()
        self.batch_size = batch_size

        # create engines to connect to the databases
        engines = self.postgres_interface.create_engine()
        self.engine_local = engines["local"]
        self.engine_neon = engines["neon"]

    def get_tickers_batch(self, table: str, engine: sqlalchemy.engine.Engine):
        table = self.postgres_interface.create_table_object(table, engine)
        query = (
            select(
                table.c.ticker,
                func.max(table.c.insert_date).label("latest_insert_date"),
            )
            .group_by(table.c.ticker)
            .order_by(asc("latest_insert_date"))
        )

        with engine.connect() as conn:
            result = conn.execute(query).fetchmany(self.batch_size)

        return result
