import os
import re
import sqlite3
from typing import List


class TickersDatabaseInterface:
    def __init__(self):
        PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))

        self.conn = sqlite3.connect(
            PROJECT_PATH + "/database/Tickers.sqlite", check_same_thread=False
        )
        self.cur = self.conn.cursor()
        self.create_data_table()
        self.create_blance_sheet_table()

    def create_data_table(self):
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

    def create_blance_sheet_table(self):
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS balance_sheet (
                ticker TEXT PRIMARY KEY UNIQUE,
                asofDate TEXT,
                periodType TEXT,
                currencyCode TEXT,
                TotalAssets TEXT,
                UNIQUE (ticker, asofDate, periodType)
                
            )   
            """
        )
        self.conn.commit()

    def insert_into_balance_sheet(
        self,
        ticker: str,
        asofDate: str,
        periodType: str,
        currencyCode: str,
        TotalAssets: float,
    ):
        """
        Upserts a data about a ticker and whether it is increasing or decreasing in different parameters and inserts it into the database
        """

        sql_command = f"""
            INSERT OR REPLACE INTO balance_sheet (ticker, asofDate, periodType, currencyCode, TotalAssets)
            VALUES ("{ticker}", "{asofDate}", "{periodType}", "{currencyCode}", "{TotalAssets}")
            """

        self.cur.executescript(sql_command)
        self.conn.commit()

    def insert_into_tickers(
        self,
        ticker: str,
        name: str,
        exchange: str,
        category: str,
        country: str,
        active: str = None,
    ):
        """
        Upserts a ticker extracted from the file in data directory into the database

        Parameters
        ----------
        ticker: str
            ticker of the company

        name: str
            name of the company

        exchange: str
            exchange of the company

        category: str
            category of the company

        country: str
            country of the company

        active: str
            active status of the company

        Returns
        -------
        None

        """
        name = re.sub("[\"']", "", name)

        sql_command = f"""
            INSERT OR REPLACE INTO tickers (ticker, name, exchange, category, country, active)
            VALUES ("{ticker}", "{name}", "{exchange}", "{category}", "{country}", "{active}")
            """

        self.cur.executescript(sql_command)
        self.conn.commit()

    def get_tickers(self) -> List[str]:
        """
        Method that gets all the tickers from the database

        Returns
        -------
        List[str]
            list of tickers
        """
        self.cur.execute("SELECT ticker FROM tickers")
        tickers = self.cur.fetchall()
        return [ticker[0] for ticker in tickers]

    def set_active_status(self, ticker: str, active: str = "Active"):
        """
        Method that sets the active status of a ticker

        Parameters
        ----------
        active: str
            active status of the ticker
            by default it is set to Active

        ticker: str
            ticker of the company

        Returns
        -------
        None
        """
        sql_command = f"""
            UPDATE tickers
            SET active = '{active}'
            WHERE ticker = '{ticker}'
            """

        self.cur.executescript(sql_command)
        self.conn.commit()

    def get_active_tickers(self) -> List[str]:
        """
        Method that gets all the active tickers from the database

        Returns
        -------
        List[str]
            list of tickers
        """
        self.cur.execute("SELECT ticker FROM tickers WHERE active != 'Inactive'")
        tickers = self.cur.fetchall()
        return [ticker[0] for ticker in tickers]
