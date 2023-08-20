""" Module to extract data from the yahoo finance API and load it into the database """

from logging import getLogger
from typing import List, Literal, Union

import pandas as pd
import sqlalchemy
import yfinance as yf
from sqlalchemy import MetaData, Table, select
from sqlalchemy.sql import null
from src.columns import BALANCE_SHEET_COLUMNS, CASH_FLOW_COLUMNS, FINANCIALS_COLUMNS
from src.postgres_interface import PostgresInterface


class Ticker:
    def __init__(
        self,
        countries: Union[str, List[str]] = None,
        chunksize: int = 20,
        frequency: Literal["annual", "quarterly"] = "annual",
        schema: str = "stocks",
    ):
        self.logger = getLogger(__name__)
        self.countries = countries
        self.chunksize = chunksize
        self.frequency = frequency
        self.schema = schema

        self.postgres_interface = PostgresInterface()
        engines = self.postgres_interface.create_engine()
        self.engine_local = engines["local"]
        self.engine_neon = engines["neon"]

    def update_tickers_list_table(self):
        """
        Method to update the tickers_list table in postgres
        Gets all the data in the data dir excel file and inserts it into the database
        """
        valids_df = pd.read_excel("src/data/tickers_list.xlsx")
        # rename columns to match the database
        valids_df.rename(
            columns={
                "Ticker": "ticker",
                "Name": "name",
                "Exchange": "exchange",
                "Category Name": "category_name",
                "Country": "country",
            },
            inplace=True,
        )

        # insert the data into the database
        valids_df.to_sql(
            name="tickers_list",
            con=self.engine_local,
            if_exists="replace",
            schema="stocks",
            index=False,
            method="multi",
            chunksize=1000,
        )

    def update_valid_tickers_table(self):
        """
        Method to update the valid_tickers table in postgres
        Gets all the tickers from the tickers_list table and checks if they are valid

        validity check:
            - ticker must have market cap
            - ticker must have total revenue
            - ticker must have free cash flow
            - ticker must have total assets
            - ticker must have currency code
        """

        # query all the tickers from the tickers_list table that are not in valid_tickers table

        tickers_list = Table(
            "tickers_list", MetaData(), autoload_with=self.engine_local, schema="stocks"
        )
        valid_tickers = Table(
            "valid_tickers",
            MetaData(),
            autoload_with=self.engine_local,
            schema="stocks",
        )

        query = (
            select(tickers_list.c.ticker)
            .outerjoin(valid_tickers, tickers_list.c.ticker == valid_tickers.c.ticker)
            .where(valid_tickers.c.ticker == null())
        )

        with self.engine_local.connect() as conn:
            tickers = [result[0] for result in conn.execute(query).fetchall()]

        valids_df = pd.DataFrame(
            columns=[
                "ticker",
                "currency_code",
                "market_cap",
                "total_revenue",
                "free_cash_flow",
                "total_assets",
                "validity",
            ]
        )
        invalids_df = pd.DataFrame(columns=["ticker", "validity"])

        for ticker_symbol in tickers:
            ticker = yf.Ticker(ticker_symbol)

            if len(valids_df) > self.chunksize:
                # insert the data into the database
                valids_df.to_sql(
                    name="valid_tickers",
                    con=self.engine_local,
                    if_exists="append",
                    schema="stocks",
                    index=False,
                    method="multi",
                )
                # empty the valids_df
                valids_df = valids_df.iloc[0:0]

                self.logger.warning(f"Inserted {self.chunksize} rows for valid tickers")

            if len(invalids_df) > self.chunksize:
                # insert the data into the database
                invalids_df.to_sql(
                    name="valid_tickers",
                    con=self.engine_local,
                    if_exists="append",
                    schema="stocks",
                    index=False,
                    method="multi",
                )
                # empty the invalids_df
                invalids_df = invalids_df.iloc[0:0]

                self.logger.warning(
                    f"Inserted {self.chunksize} rows for invalid tickers"
                )

            # check if the ticker is valid
            try:
                if (
                    ticker.info["marketCap"]
                    and ticker.financials.loc["Total Revenue"].values[0]
                    and ticker.cashflow.loc["Free Cash Flow"].values[0]
                    and ticker.balance_sheet.loc["Total Assets"].values[0]
                    and ticker.info["currency"]
                ):
                    valids_df.loc[len(valids_df)] = pd.Series(
                        {
                            "ticker": ticker_symbol,
                            "currency_code": ticker.info["currency"],
                            "market_cap": ticker.info["marketCap"],
                            "total_revenue": ticker.financials.loc[
                                "Total Revenue"
                            ].values[0],
                            "free_cash_flow": ticker.cashflow.loc[
                                "Free Cash Flow"
                            ].values[0],
                            "total_assets": ticker.balance_sheet.loc[
                                "Total Assets"
                            ].values[0],
                            "validity": True,
                        }
                    )

            except:
                self.logger.warning(f"Ticker {ticker_symbol} is invalid")
                invalids_df.loc[len(invalids_df)] = pd.Series(
                    {"ticker": ticker_symbol, "validity": False}
                )

    def load_valid_tickers(self, sink_table: str) -> List[str]:
        """
        Method to load the valid tickers from the database
        """

        valid_tickers = Table(
            "valid_tickers",
            MetaData(),
            autoload_with=self.engine_local,
            schema=self.schema,
        )
        balance_sheet = Table(
            sink_table, MetaData(), autoload_with=self.engine_local, schema=self.schema
        )
        query = (
            select(valid_tickers.c.ticker)
            .outerjoin(balance_sheet, valid_tickers.c.ticker == balance_sheet.c.ticker)
            .where(balance_sheet.c.ticker == null())
            .where(valid_tickers.c.validity == True)
        )

        with self.engine_local.connect() as conn:
            valid_tickers = [result[0] for result in conn.execute(query).fetchall()]

        return valid_tickers

    def update_cash_flow(self, engine: sqlalchemy.engine.Engine, tickers: List[str]):
        for ticker in tickers:
            self.logger.warning(f"Updating cash flow for {ticker}")

            ticker = yf.Ticker(ticker)
            try:
                cash_flow_df = ticker.cashflow.T
                cash_flow_df["ticker"] = ticker.ticker
                cash_flow_df["currency_code"] = ticker.info["currency"]
                cash_flow_df["frequency"] = self.frequency
                cash_flow_df.reset_index(inplace=True)
                cash_flow_df.rename(columns={"index": "report_date"}, inplace=True)
            except:
                self.logger.warning(f"Ticker {ticker} has no cash flow")
                continue

            # if a column does not exist in the stocks.cash_flow table, drop it from the df
            for column in cash_flow_df.columns:
                if column not in CASH_FLOW_COLUMNS:
                    cash_flow_df.drop(columns=column, inplace=True)

            # insert the data into the database
            cash_flow_df.to_sql(
                name="cash_flow",
                con=engine,
                if_exists="append",
                schema="stocks",
                index=False,
                method="multi",
            )

    def insert_financials(self):
        """
        Method to populate the stocks.financials table
        """

        valid_tickers = self.load_valid_tickers(sink_table="financials")

        for ticker in valid_tickers:
            self.logger.warning(f"Updating financials for {ticker}")

            ticker = yf.Ticker(ticker)
            try:
                financials_df = ticker.financials.T
                financials_df["ticker"] = ticker.ticker
                financials_df["currency_code"] = ticker.info["currency"]
                financials_df["frequency"] = self.frequency
                financials_df.reset_index(inplace=True)
                financials_df.rename(columns={"index": "report_date"}, inplace=True)
            except:
                self.logger.warning(f"Ticker {ticker} has no financials")
                continue

            # if a column does not exist in the stocks.financials table, drop it from the df
            for column in financials_df.columns:
                if column not in FINANCIALS_COLUMNS:
                    financials_df.drop(column, axis=1, inplace=True)

            # insert the data into the database
            financials_df.to_sql(
                name="financials",
                con=self.engine_local,
                if_exists="append",
                schema="stocks",
                index=False,
                method="multi",
            )

    def update_balance_sheet(self):
        valid_tickers = self.load_valid_tickers(sink_table="balance_sheet")

        for ticker in valid_tickers:
            self.logger.warning(f"Updating balance sheet for {ticker}")

            ticker = yf.Ticker(ticker)
            try:
                balance_sheet_df = ticker.balance_sheet.T
                balance_sheet_df["ticker"] = ticker.ticker
                balance_sheet_df["currency_code"] = ticker.info["currency"]
                balance_sheet_df["frequency"] = self.frequency
                balance_sheet_df.reset_index(inplace=True)
                balance_sheet_df.rename(columns={"index": "report_date"}, inplace=True)
            except:
                self.logger.warning(f"Ticker {ticker} has no balance sheet")
                continue

            # if a column does not exist in the stocks.balance_sheet table, drop it from the df
            for column in balance_sheet_df.columns:
                if column not in BALANCE_SHEET_COLUMNS:
                    balance_sheet_df.drop(column, axis=1, inplace=True)

            # TODO: df.to_sql does not support on conflict do nothing. replace with a stored procedure
            # insert the data into the database
            balance_sheet_df.to_sql(
                name="balance_sheet",
                con=self.engine_local,
                if_exists="append",
                schema="stocks",
                index=False,
                method="multi",
            )
