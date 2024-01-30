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
from src.utils import custom_logger


class PostgresInterface:
    """
    Class to interact with postgres databases
    """

    def __init__(self):
        load_dotenv()
        self.logger = custom_logger(logger_name="postgres_interface")
        self.provider = os.environ.get("PROVIDER")

    def create_engine(self) -> sqlalchemy.engine.Engine:
        """
        function that creates engines to connect to postgres databases

        Returns
        -------
        dict
            dictionary with the engines to connect to the databases
        """
        user = os.environ.get(f"{self.provider}_POSTGRES_USER")
        password = os.environ.get(f"{self.provider}_POSTGRES_PASSWORD")
        host = os.environ.get(f"{self.provider}_POSTGRES_HOST")
        port = os.environ.get(f"{self.provider}_POSTGRES_PORT")
        db = os.environ.get(f"{self.provider}_POSTGRES_DB")
        ssl_mode = "?sslmode=" + os.environ.get(f"{self.provider}_SSL_MODE") or ""

        engine = sqlalchemy.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{db}{ssl_mode}"
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

    def migrate_dbs(
        self,
        batch_size: int = 5000,
        tap_cloud_provider: str = "NEON",
        target_cloud_provider: str = "AVN",
    ) -> None:
        """
        Method to migrate a database to another one
        Supposed to be used only once to migrate data
        to a target database

        Parameters
        ----------
        batch_size : int
            number of rows to insert in each batch
            default: 5000

        tap_cloud_provider : str
            cloud provider of the tap database
            default: NEON

        target_cloud_provider : str
            cloud provider of the target database
            default: AVN

        Returns
        -------
        None
        """
        tap_user = os.environ.get(f"{tap_cloud_provider}_POSTGRES_USER")
        tap_password = os.environ.get(f"{tap_cloud_provider}_POSTGRES_PASSWORD")
        tap_host = os.environ.get(f"{tap_cloud_provider}_POSTGRES_HOST")
        tap_port = os.environ.get(f"{tap_cloud_provider}_POSTGRES_PORT")
        tap_db = os.environ.get(f"{tap_cloud_provider}_POSTGRES_DB")
        engine_tap = sqlalchemy.create_engine(
            f"postgresql://{tap_user}:{tap_password}@{tap_host}:{tap_port}/{tap_db}"
        )

        target_user = os.environ.get(f"{target_cloud_provider}_POSTGRES_USER")
        target_password = os.environ.get(f"{target_cloud_provider}_POSTGRES_PASSWORD")
        target_host = os.environ.get(f"{target_cloud_provider}_POSTGRES_HOST")
        target_port = os.environ.get(f"{target_cloud_provider}_POSTGRES_PORT")
        target_db = os.environ.get(f"{target_cloud_provider}_POSTGRES_DB")
        engine_target = sqlalchemy.create_engine(
            f"postgresql://{target_user}:{target_password}@{target_host}:{target_port}/{target_db}"
        )

        # List of tables in local postgres database in stocks schema
        metadata = MetaData()
        information_schema_tables = Table(
            "tables", metadata, autoload_with=engine_tap, schema="information_schema"
        )
        query = (
            select(information_schema_tables.c.table_name)
            .where(information_schema_tables.c.table_schema == "stocks")
            .order_by(information_schema_tables.c.table_name)
        )

        with engine_tap.connect() as conn_local:
            tables = [table[0] for table in conn_local.execute(query).fetchall()]

        # if "alembic" of "dbt" are in table's name, remove them
        blacklist = ["alembic", "dbt"]
        tables = [table for table in tables if not any(x in table for x in blacklist)]

        # insert table's data into neon database
        for table in tables:
            self.logger.warning(f"Inserting data from {table} into target database")
            with engine_tap.connect() as conn_tap:
                with engine_target.connect() as conn_target:
                    table_tap = self.create_table_object(table, engine_tap)
                    total_rows = conn_tap.execute(
                        text("SELECT COUNT(*) FROM stocks." + table)
                    ).scalar()
                    self.logger.warning(
                        f"Total rows in {table_tap} from tap database: {total_rows}"
                    )

                    # Calculate how many iterations you will need
                    iterations = total_rows // batch_size + (
                        1 if total_rows % batch_size else 0
                    )

                    for i in range(iterations):
                        offset = i * batch_size
                        # Create a SELECT statement with LIMIT and OFFSET
                        select_stmt = conn_tap.execute(
                            text(
                                "SELECT * FROM stocks."
                                + table
                                + f" LIMIT {batch_size} OFFSET {offset}"
                            )
                        )
                        result_set = select_stmt.fetchall()
                        # convert result set to list of dicts
                        result_set = [tuple(row) for row in result_set]
                        self.logger.warning(
                            f"""Selected {len(result_set)} rows from {table} from tap database
                             with offset {offset}"""
                        )

                        table_target = self.create_table_object(table, engine_target)

                        # statement to insert data into neon database
                        self.logger.warning(
                            f"Inserting batch of {len(result_set)} rows"
                        )
                        self.insert_batch(table_target, result_set, conn_target)
                        self.logger.warning(
                            f"Inserted {len(result_set)} rows from {table} into target database"
                        )

                        del result_set
