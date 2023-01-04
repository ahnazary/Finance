import os
import re
import sqlite3
from typing import List

import numpy as np
import yahooquery

from src.utils import Logger


class TickersDatabaseInterface:
    def __init__(self):
        PROJECT_PATH = os.path.abspath(os.path.dirname(__file__))

        self.conn = sqlite3.connect(
            PROJECT_PATH + "/database/Tickers.sqlite", check_same_thread=False
        )
        self.cur = self.conn.cursor()

        # Create tables if they don't exist
        self.create_data_table()
        self.create_blance_sheet_table()
        self.create_income_statement_table()
        self.create_cash_flow_table()

        self.logger = Logger()

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

    def create_income_statement_table(self):
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS income_statement (
                ticker TEXT PRIMARY KEY UNIQUE,
                asofDate TEXT,
                periodType TEXT,
                currencyCode TEXT,
                TotalRevenue TEXT,
                PretaxIncome TEXT,
                BasicEPS TEXT,
                EBITDA TEXT,
                EBIT TEXT,
                GrossProfit TEXT,
                NetIncome TEXT,
                NetIncomeCommonStockholders TEXT,
                OperatingIncome TEXT,
                OperatingRevenue TEXT,
                ResearchAndDevelopment TEXT,
                UNIQUE (ticker, asofDate, periodType)
            )   
            """
        )
        self.conn.commit()

    def create_cash_flow_table(self):
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cash_flow (
                ticker TEXT PRIMARY KEY UNIQUE,
                asofDate TEXT,
                periodType TEXT,
                currencyCode TEXT,
                CapitalExpenditure TEXT,
                CashDividendsPaid TEXT,
                ChangeInInventory TEXT,
                ChangesInCash TEXT,
                CommonStockDividendPaid TEXT,
                DeferredIncomeTax TEXT,
                DeferredTax TEXT,
                EndCashPosition TEXT,
                FinancingCashFlow TEXT,
                FreeCashFlow TEXT,
                InvestingCashFlow TEXT,
                NetIncome TEXT,
                NetInvestmentPurchaseAndSale TEXT,
                Unique (ticker, asofDate, periodType)
            )
            """
        )
        self.conn.commit()

    def insert_into_cash_flow(
        self,
        ticker: List[str],
        asofDate: List[str],
        periodType: List[str],
        currencyCode: List[str],
        CapitalExpenditure: List[float],
        CashDividendsPaid: List[float],
        ChangeInInventory: List[float],
        ChangesInCash: List[float],
        CommonStockDividendPaid: List[float],
        DeferredIncomeTax: List[float],
        DeferredTax: List[float],
        EndCashPosition: List[float],
        FinancingCashFlow: List[float],
        FreeCashFlow: List[float],
        InvestingCashFlow: List[float],
        NetIncome: List[float],
        NetInvestmentPurchaseAndSale: List[float],
    ):
        """
        Method to insert rows into the cash_flow table

        Returns:
        --------
        None

        """

        sql_command = f"""
            INSERT OR REPLACE INTO cash_flow (ticker, asofDate, periodType, currencyCode, CapitalExpenditure, CashDividendsPaid, ChangeInInventory, ChangesInCash, CommonStockDividendPaid, DeferredIncomeTax, DeferredTax, EndCashPosition, FinancingCashFlow, FreeCashFlow, InvestingCashFlow, NetIncome, NetInvestmentPurchaseAndSale)
            VALUES ("{ticker}", "{asofDate}", "{periodType}", "{currencyCode}", "{CapitalExpenditure}", "{CashDividendsPaid}", "{ChangeInInventory}", "{ChangesInCash}", "{CommonStockDividendPaid}", "{DeferredIncomeTax}", "{DeferredTax}", "{EndCashPosition}", "{FinancingCashFlow}", "{FreeCashFlow}", "{InvestingCashFlow}", "{NetIncome}", "{NetInvestmentPurchaseAndSale}")
            """

        self.cur.executescript(sql_command)
        self.conn.commit()

    def insert_into_income_statement(
        self,
        ticker: List[str],
        asofDate: List[str],
        periodType: List[str],
        currencyCode: List[str],
        TotalRevenue: List[float],
        PretaxIncome: List[float],
        BasicEPS: List[float],
        EBITDA: List[float],
        EBIT: List[float],
        GrossProfit: List[float],
        NetIncome: List[float],
        NetIncomeCommonStockholders: List[float],
        OperatingIncome: List[float],
        OperatingRevenue: List[float],
        ResearchAndDevelopment: List[float],
    ):
        """
        Method to insert rows into the income_statement table

        Returns:
        --------
        None

        """

        sql_command = f"""
            INSERT OR REPLACE INTO income_statement (ticker, asofDate, periodType, currencyCode, TotalRevenue, PretaxIncome, BasicEPS, EBITDA, EBIT, GrossProfit, NetIncome, NetIncomeCommonStockholders, OperatingIncome, OperatingRevenue, ResearchAndDevelopment)
            VALUES ("{ticker}", "{asofDate}", "{periodType}", "{currencyCode}", "{TotalRevenue}", "{PretaxIncome}", "{BasicEPS}", "{EBITDA}", "{EBIT}", "{GrossProfit}", "{NetIncome}", "{NetIncomeCommonStockholders}", "{OperatingIncome}", "{OperatingRevenue}", "{ResearchAndDevelopment}")
            """

        self.cur.executescript(sql_command)
        self.conn.commit()

    def insert_into_balance_sheet(
        self,
        ticker: List[str],
        asofDate: List[str],
        periodType: List[str],
        currencyCode: List[str],
        TotalAssets: List[float],
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
        Upserts a ticker extracted from the file in data directory into the tickers table

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
        Method that sets the active status of a ticker.

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

    def update_tickers_status(self):
        """
        Method that updates the active status of the tickers
        If has pervious close price then it is active, else it is inactive

        Returns
        -------
        None
        """

        active_tickers = self.get_tickers()

        for ticker in active_tickers:
            try:
                previous_close = (
                    yahooquery.Ticker(ticker)
                    .summary_detail.get(ticker)
                    .get("previousClose")
                )
                self.logger.warning(
                    f"{ticker} is active with previousClose {previous_close}"
                )
                self.set_active_status(ticker, "Active")
            except Exception as e:
                self.logger.warning(f"{ticker} is inactive, {e}")
                self.set_active_status(ticker, "Inactive")
