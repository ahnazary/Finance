import os
import sqlalchemy


class PostgresInterface:
    def __init__(self):
        self.create_engine()

    def create_engine(self):
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")
        host = os.environ.get("POSTGRES_HOST")
        port = os.environ.get("POSTGRES_PORT")
        self.engine = sqlalchemy.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/postgres"
        )
    
    
