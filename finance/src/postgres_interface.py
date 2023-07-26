import os

import sqlalchemy
from dotenv import load_dotenv
from sqlalchemy import text


class PostgresInterface:
    def __init__(self):
        load_dotenv()

    def create_engine(self):
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")
        host = os.environ.get("POSTGRES_HOST")
        port = os.environ.get("POSTGRES_PORT")

        engine = sqlalchemy.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/postgres"
        )
        return engine

    def execute_query(self, query):
        engine = self.create_engine()
        with engine.connect() as conn:
            result = conn.execute(text(query)).fetchall()
        result = [r[0] for r in result]
        return result

    def execute(self, sql_sttm):
        """
        Method to execute a sql statement

        Parameters
        ----------
        sql_sttm : str
            sql statement to execute

        Returns
        -------
        list
            list of tuples with the results of the query
        """

        engine = self.create_engine()
        with engine.connect() as conn:
            if isinstance(sql_sttm, str):
                result = conn.execute(text(sql_sttm))
            else:
                result = conn.execute(sql_sttm)

        return result.fetchall()
