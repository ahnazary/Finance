import os
import re
import sqlite3


class TickersDatabaseInterface:
    def __init__(self):
        PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))

        self.conn = sqlite3.connect(
            PROJECT_PATH + "/database/Tickers.sqlite", check_same_thread=False
        )
        self.cur = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tickers (
                ticker TEXT PRIMARY KEY UNIQUE,
                name TEXT,
                exchange TEXT,
                category TEXT,
                country TEXT,
                active TEXT
            )
            """
        )
        self.conn.commit()

    def insert_tickers(
        self,
        ticker: str,
        name: str,
        exchange: str,
        category: str,
        country: str,
        active: str = None,
    ):
        # remove ' from name
        name = re.sub("[\"']", "", name)

        sql_command = f"""
            INSERT OR REPLACE INTO tickers (ticker, name, exchange, category, country, active)
            VALUES ("{ticker}", "{name}", "{exchange}", "{category}", "{country}", "{active}")
            """

        self.cur.executescript(sql_command)
        self.conn.commit()
