"""
Module to interact with postgres databases

It contains generic methods to interact with postgres databases regardless of
the data they contain
"""

import os
from typing import Literal

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, select
from sqlalchemy.dialects.postgresql import insert
from src.utils import custom_logger


class PostgresInterface:
    """
    Class to interact with postgres databases
    """

    def __init__(self):
        load_dotenv()
        self.logger = custom_logger(logger_name="postgres_interface")

    def create_engine(
        self, provider: Literal["LOCAL", "NEON"] = "LOCAL"
    ) -> sqlalchemy.engine.Engine:
        """
        function that creates engines to connect to postgres databases

        Returns
        -------
        dict
            dictionary with the engines to connect to the databases
        """
        user = os.environ.get(f"{provider}_POSTGRES_USER")
        password = os.environ.get(f"{provider}_POSTGRES_PASSWORD")
        host = os.environ.get(f"{provider}_POSTGRES_HOST")
        port = os.environ.get(f"{provider}_POSTGRES_PORT")
        db = os.environ.get(f"{provider}_POSTGRES_DB")

        ssl_mode = "" if provider == "LOCAL" else "?sslmode=require"

        engine = sqlalchemy.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{db}{ssl_mode}"
        )

        return engine

    def create_table_object(
        self, table_name: str, engine: sqlalchemy.engine.Engine, schema: str = "stocks"
    ):
        """
        Method to create a table object

        Parameters
        ----------
        table_name : str
            name of the table to create the object for
        engine : sqlalchemy.engine.Engine
            engine to connect to the database
        schema : str
            schema of the table
            default: stocks

        Returns
        -------
        sqlalchemy.Table
            table object
        """
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=engine, schema=schema)
        return table

    def migrate_local_to_cloud(self) -> None:
        """
        Method to migrate the local database to the neon database
        Supposed to be used only once to migrate the data from the local
        to a target neon database (for now NEON cloud database)

        Returns
        -------
        None
        """
        engines = self.create_engine()
        engine_local = engines["local"]
        engine_neon = engines["neon"]

        # List of tables in local postgres database in stocks schema
        metadata = MetaData()
        information_schema_tables = Table(
            "tables", metadata, autoload_with=engine_local, schema="information_schema"
        )
        query = (
            select(information_schema_tables.c.table_name)
            .where(information_schema_tables.c.table_schema == "stocks")
            .order_by(information_schema_tables.c.table_name)
        )

        with engine_local.connect() as conn_local:
            tables = [table[0] for table in conn_local.execute(query).fetchall()]

        # insert table's data into neon database
        for table in tables:
            self.logger.warning(f"Inserting data from {table} into neon database")
            with engine_local.connect() as conn_local:
                with engine_neon.connect() as conn_neon:
                    table_local = self.create_table_object(table, engine_local)
                    query = select(table_local)
                    data = [tuple(row) for row in conn_local.execute(query).fetchall()]

                    # cast data into batches of 1000 rows
                    data = [
                        data[i : i + 1000]  # noqa: E203
                        for i in range(0, len(data), 1000)
                    ]

                    table_neon = self.create_table_object(table, engine_neon)
                    inserted_batches = 0
                    for batch in data:
                        # statement to insert data into neon database
                        self.logger.warning(f"Inserting batch of {len(batch)} rows")
                        self.insert_batch(table=table_neon, batch=batch, conn=conn_neon)
                        self.logger.warning(f"Inserted {inserted_batches} batches")

    def insert_batch(
        self, table: sqlalchemy.Table, batch: list, conn: sqlalchemy.engine.Connection
    ) -> None:
        """
        Method to insert a batch of data into a table

        Parameters
        ----------
        table : str
            table to insert data into
        data : list
            list of tuples with the data to insert into the table

        Returns
        -------
        None
        """

        # statement to insert data into neon database
        self.logger.warning(f"Inserting batch of {len(batch)} rows")
        insert_statement = insert(table).values(batch).on_conflict_do_nothing()
        conn.execute(insert_statement)
        conn.commit()

    def read_table_to_df(self, table: str, schema: str = "stocks") -> pd.DataFrame:
        """
        Method to read a table into a dataframe

        Parameters
        ----------
        table : tabble name to read

        Returns
        -------
        pd.DataFrame
            dataframe with the data from the table
        """
        engine = self.create_engine()
        table = self.create_table_object(table, engine, schema)
        query = select(table)
        with engine.connect() as conn:
            result = conn.execute(query).fetchall()
        df = pd.DataFrame(result, columns=table.columns.keys())
        return df
