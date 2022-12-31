from typing import List, Literal, Union

import pandas as pd
import yahooquery

from src.database import TickersDatabaseInterface
from src.utils import Logger, are_incremental


class FilterTickers:
    def __init__(
        self,
        frequency: Literal["annual", "quarterly"] = "annual",
    ):
        self.database_interface = TickersDatabaseInterface()
        self.tickers_list = self.database_interface.get_active_tickers()
        self.frequency = frequency
        self.logger = Logger()

    def update_balance_sheet_table(self, params: List[str] = ["TotalAssets"]):

        for ticker in self.tickers_list:
            try:
                balance_sheet = yahooquery.Ticker(ticker).balance_sheet(
                    frequency=self.frequency
                )

                self.database_interface.insert_into_balance_sheet(
                    ticker=ticker,
                    asofDate=balance_sheet["asOfDate"].tolist(),
                    periodType=balance_sheet["periodType"].tolist(),
                    currencyCode=balance_sheet["currencyCode"].tolist(),
                    TotalAssets=balance_sheet["TotalAssets"].tolist(),
                )
                # if not are_incremental(balance_sheet[param].tolist()):
                #     self.data_df = self.data_df[self.data_df["Ticker"] != ticker]
                self.database_interface.set_active_status(ticker)
                self.logger.warning(
                    f"ticker {ticker} is active, balance sheet data inserted into database"
                )
            except:
                self.logger.warning(
                    f"Balance sheet {ticker} does not exist on yahoo finance"
                )

    def update_income_statement_table(self, params: List[str] = None):
        params = params or [
            "TotalRevenue",
            "PretaxIncome",
            "BasicEPS",
            "EBITDA",
            "EBIT",
            "GrossProfit",
            "NetIncome",
            "NetIncomeCommonStockholders",
            "OperatingIncome",
            "OperatingRevenue",
            "ResearchAndDevelopment",
        ]

        for ticker in self.tickers_list:
            try:
                income_statement = yahooquery.Ticker(ticker).income_statement(
                    frequency=self.frequency
                )

                self.database_interface.insert_into_income_statement(
                    ticker=ticker,
                    asofDate=income_statement["asOfDate"].tolist(),
                    periodType=income_statement["periodType"].tolist(),
                    currencyCode=income_statement["currencyCode"].tolist(),
                    TotalRevenue=income_statement["TotalRevenue"].tolist(),
                    PretaxIncome=income_statement["PretaxIncome"].tolist(),
                    BasicEPS=income_statement["BasicEPS"].tolist(),
                    EBITDA=income_statement["EBITDA"].tolist(),
                    EBIT=income_statement["EBIT"].tolist(),
                    GrossProfit=income_statement["GrossProfit"].tolist(),
                    NetIncome=income_statement["NetIncome"].tolist(),
                    NetIncomeCommonStockholders=income_statement[
                        "NetIncomeCommonStockholders"
                    ].tolist(),
                    OperatingIncome=income_statement["OperatingIncome"].tolist(),
                    OperatingRevenue=income_statement["OperatingRevenue"].tolist(),
                    ResearchAndDevelopment=income_statement[
                        "ResearchAndDevelopment"
                    ].tolist(),
                )
                # if not are_incremental(balance_sheet[param].tolist()):
                #     self.data_df = self.data_df[self.data_df["Ticker"] != ticker]
                self.database_interface.set_active_status(ticker)
                self.logger.warning(
                    f"ticker {ticker} is active, income statement data inserted into database"
                )
            except:
                self.logger.warning(
                    f"Income statement for {ticker} does not exist on yahoo finance"
                )
