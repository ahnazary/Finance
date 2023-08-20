import os
from logging import getLogger

import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import MetaData, Table, select
from sqlalchemy.dialects.postgresql import insert


class PostgresInterface:
    def __init__(self):
        load_dotenv()
        self.logger = getLogger(__name__)

    def create_engine(self) -> dict:
        """
        function that creates engines to connect to postgres databases

        Returns
        -------
        dict
            dictionary with the engines to connect to the databases
        """
        local_user = os.environ.get("POSTGRES_USER")
        local_password = os.environ.get("POSTGRES_PASSWORD")
        local_host = os.environ.get("POSTGRES_HOST")
        local_port = os.environ.get("POSTGRES_PORT")

        engine_local = sqlalchemy.create_engine(
            f"postgresql://{local_user}:{local_password}@{local_host}:{local_port}/"
        )

        neon_user = os.environ.get("NEON_POSTGRES_USER")
        neon_password = os.environ.get("NEON_POSTGRES_PASSWORD")
        neon_host = os.environ.get("NEON_POSTGRES_HOST")
        neon_port = os.environ.get("NEON_POSTGRES_PORT")
        neon_db = os.environ.get("NEON_POSTGRES_DB")

        # use sslmode=require to connect to the database
        engine_neon = sqlalchemy.create_engine(
            f"postgresql://{neon_user}:{neon_password}@{neon_host}:{neon_port}/{neon_db}?sslmode=require"
        )

        engine_dict = {"local": engine_local, "neon": engine_neon}

        return engine_dict

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

    def migrate_local_to_neon(self):
        """
        Method to migrate the local database to the neon database
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
                    data = [data[i : i + 1000] for i in range(0, len(data), 1000)]

                    table_neon = self.create_table_object(table, engine_neon)
                    inserted_batches = 0
                    for batch in data:
                        # statement to insert data into neon database
                        self.logger.warning(f"Inserting batch of {len(batch)} rows")
                        self.insert_batch(table=table_neon, batch=batch, conn=conn_neon)
                        self.logger.warning(f"Inserted {inserted_batches} batches")

    def insert_batch(
        self, table: sqlalchemy.Table, batch: list, conn: sqlalchemy.engine.Connection
    ):
        """
        Method to insert a batch of data into a table

        Parameters
        ----------
        table : str
            table to insert data into
        data : list
            list of tuples with the data to insert into the table
        """

        # statement to insert data into neon database
        self.logger.warning(f"Inserting batch of {len(batch)} rows")
        insert_statement = insert(table).values(batch).on_conflict_do_nothing()
        conn.execute(insert_statement)
        conn.commit()
