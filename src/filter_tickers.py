import ast
from typing import List, Literal, Union

import pandas as pd
import yahooquery

from src.database import TickersDatabaseInterface
from src.utils import Logger, are_incremental
import re


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

    def update_cash_flow_table(self, params: List[str] = None):
        params = params or [
            "otalRevenue",
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
                cash_flow = yahooquery.Ticker(ticker).cash_flow(
                    frequency=self.frequency
                )

                self.database_interface.insert_into_cash_flow(
                    ticker=ticker,
                    asofDate=cash_flow["asOfDate"].tolist(),
                    periodType=cash_flow["periodType"].tolist(),
                    currencyCode=cash_flow["currencyCode"].tolist(),
                    CapitalExpenditure=cash_flow["CapitalExpenditure"].tolist(),
                    CashDividendsPaid=cash_flow["CashDividendsPaid"].tolist(),
                    ChangeInInventory=cash_flow["ChangeInInventory"].tolist(),
                    ChangesInCash=cash_flow["ChangesInCash"].tolist(),
                    CommonStockDividendPaid=cash_flow[
                        "CommonStockDividendPaid"
                    ].tolist(),
                    DeferredTax=cash_flow["DeferredTax"].tolist(),
                    DeferredIncomeTax=cash_flow["DeferredIncomeTax"].tolist(),
                    EndCashPosition=cash_flow["EndCashPosition"].tolist(),
                    FinancingCashFlow=cash_flow["FinancingCashFlow"].tolist(),
                    FreeCashFlow=cash_flow["FreeCashFlow"].tolist(),
                    InvestingCashFlow=cash_flow["InvestingCashFlow"].tolist(),
                    NetIncome=cash_flow["NetIncome"].tolist(),
                    NetInvestmentPurchaseAndSale=cash_flow[
                        "NetInvestmentPurchaseAndSale"
                    ].tolist(),
                )
                # if not are_incremental(balance_sheet[param].tolist()):
                #     self.data_df = self.data_df[self.data_df["Ticker"] != ticker]
                self.database_interface.set_active_status(ticker)
                self.logger.warning(
                    f"ticker {ticker} is active, cash flow data inserted into database"
                )
            except:
                self.logger.warning(
                    f"Cash flow for {ticker} does not exist on yahoo finance"
                )

    def filter_revenue_net_income(self):
        self.database_interface.cur.execute(
            "SELECT ticker, asofDate, periodType, currencyCode, TotalRevenue, NetIncomeCommonStockholders FROM income_statement"
        )
        data = self.database_interface.cur.fetchall()
        data = pd.DataFrame(
            data,
            columns=[
                "ticker",
                "asofDate",
                "periodType",
                "currencyCode",
                "TotalRevenue",
                "NetIncome",
            ],
        )
        # iterate over all revenue and net income columns
        for col in data.itertuples():
            revenue = col.TotalRevenue
            net_income = col.NetIncome
            if "nan" in col.TotalRevenue or "nan" in col.NetIncome:
                revenue = re.sub(r"nan", "0", col.TotalRevenue)
                net_income = re.sub(r"nan", "0", col.NetIncome)

            revenue_list = ast.literal_eval((revenue))
            net_income_list = ast.literal_eval((net_income))
            if are_incremental(revenue_list) and are_incremental(net_income_list):
                self.logger.warning(
                    f"ticker {col.ticker} has incremental revenue and net income"
                )
                self.database_interface.insert_into_growth_stocks(
                    ticker=col.ticker,
                    asofDate=col.asofDate,
                    periodType=col.periodType,
                    currencyCode=col.currencyCode,
                )
            else:
                self.logger.warning(
                    f"ticker {col.ticker} does not have incremental revenue and net income"
                )
