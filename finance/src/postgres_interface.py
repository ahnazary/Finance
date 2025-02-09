"""
Module to interact with postgres databases

It contains generic methods to interact with postgres databases regardless of
the data they contain
"""

import os

import pandas as pd
import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, select, text
from sqlalchemy.dialects.postgresql import insert
from src.utils import emit_log

from config import LOG_LEVEL


class PostgresInterface:
    """
    Class to interact with postgres databases
    """

    def __init__(self):
        load_dotenv()
        self.provider = os.environ.get("PROVIDER")

    def create_engine(self) -> sqlalchemy.engine.Engine:
        """
        function that creates engines to connect to postgres databases

        Returns
        -------
        dict
            dictionary with the engines to connect to the databases
        """
        conn_str = os.environ.get(f"{self.provider}_POSTGRES_CONNECTION_STRING")
        user = os.environ.get(f"{self.provider}_POSTGRES_USER")
        password = os.environ.get(f"{self.provider}_POSTGRES_PASSWORD")
        host = os.environ.get(f"{self.provider}_POSTGRES_HOST")
        port = os.environ.get(f"{self.provider}_POSTGRES_PORT")
        db = os.environ.get(f"{self.provider}_POSTGRES_DB")
        ssl_mode = "?sslmode=" + os.environ.get(f"{self.provider}_SSL_MODE") or ""

        engine = (
            sqlalchemy.create_engine(
                f"postgresql://{user}:{password}@{host}:{port}/{db}{ssl_mode}"
            )
            if not conn_str
            else sqlalchemy.create_engine(conn_str)
        )

        return engine

    def create_table_object(
        self, table_name: str, engine: sqlalchemy.engine.Engine, schema: str = "stocks"
    ) -> sqlalchemy.Table:
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
        emit_log(f"Inserting batch of {len(batch)} rows", log_level=LOG_LEVEL)
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
