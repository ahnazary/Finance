import os
import sqlalchemy
from dotenv import load_dotenv


class PostgresInterface:
    def __init__(self):
        load_dotenv()

    def create_engine(self):
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")
        host = os.environ.get("POSTGRES_HOST")
        port = os.environ.get("POSTGRES_PORT")
        print(user, password, host, port)
        
        engine = sqlalchemy.create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/postgres"
        )
        return engine

