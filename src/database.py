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
        # combination of symbol, asofDate, periodType, currencyCode should be unique
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tickers (
                symbol TEXT PRIMARY KEY UNIQUE,
                asofDate TEXT,
                periodType TEXT,
                currencyCode TEXT,
                TotalAssets REAL,
                TotalCurrentAssets REAL,
                Inventory REAL,
                CONSTRAINJ
                
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

    def get_symbols(self) -> List[str]:
        """
        Method that gets all the symbols from the database

        Returns
        -------
        List[str]
            list of symbols
        """
        self.cur.execute("SELECT ticker FROM tickers")
        symbols = self.cur.fetchall()
        return [symbol[0] for symbol in symbols]