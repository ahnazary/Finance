from logging import getLogger
from typing import List, Literal, Union

import pandas as pd
import yfinance as yf
from src.columns import FINANCIALS_COLUMNS
from src.postgres_interface import PostgresInterface


class Ticker:
    def __init__(
        self,
        countries: Union[str, List[str]] = None,
        chunksize: int = 20,
        frequency: Literal["annual", "quarterly"] = "annual",
    ):
        self.logger = getLogger(__name__)
        self.countries = countries
        self.chunksize = chunksize
        self.frequency = frequency

        self.postgres_interface = PostgresInterface()
        self.engine = self.postgres_interface.create_engine()

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
            con=self.engine,
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
        query = """ 
            SELECT tickers_list.ticker
            FROM stocks.tickers_list 
            LEFT JOIN stocks.valid_tickers 
            ON tickers_list.ticker = valid_tickers.ticker
            WHERE valid_tickers.ticker IS NULL;
        """
        tickers = self.postgres_interface.execute_query(query)
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
                    con=self.engine,
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
                    con=self.engine,
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

    def load_valid_tickers(self):
        """
        Method to load the valid tickers from the database
        """
        query = """
            SELECT stocks.valid_tickers.ticker
            FROM stocks.valid_tickers
            LEFT JOIN stocks.financials
            ON valid_tickers.ticker = financials.ticker
            WHERE financials.ticker IS NULL AND validity = True;
        """
        valid_tickers = self.postgres_interface.execute_query(query)
        return valid_tickers

    def insert_financials(self):
        """
        Method to populate the stocks.financials table
        """

        valid_tickers = self.load_valid_tickers()

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
                con=self.engine,
                if_exists="append",
                schema="stocks",
                index=False,
                method="multi",
            )
