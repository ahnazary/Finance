"""
PostgreSQL interface for connecting to the Neon Postgres finance database.
"""

import os

import sqlalchemy
from dotenv import load_dotenv

load_dotenv()

NEON_CONNECTION_STRING = os.environ.get("PG_NEON_FINANCE_URL")

if not NEON_CONNECTION_STRING:
    raise EnvironmentError(
        "PG_NEON_FINANCE_URL environment variable is not set. "
        "Please set it in .env or as a system environment variable."
    )


class PostgresInterface:
    """Interface for connecting to the Neon Postgres finance database."""

    def __init__(self, connection_string: str = NEON_CONNECTION_STRING):
        self.connection_string = connection_string
        self.engine = sqlalchemy.create_engine(
            self.connection_string,
            pool_pre_ping=True,
            pool_recycle=300,
        )

    def get_engine(self) -> sqlalchemy.engine.Engine:
        return self.engine
