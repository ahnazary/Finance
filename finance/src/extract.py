""" Module to extract data from the yahoo finance API and load it into the database """

from logging import getLogger
from typing import List, Literal, Union

import pandas as pd
import yfinance as yf
from sqlalchemy import MetaData, Table, func, select, update
from sqlalchemy.dialects.postgresql import insert
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
        provider: Literal["LOCAL", "NEON"] = "LOCAL",
    ):
        self.logger = getLogger(__name__)
        self.countries = countries
        self.chunksize = chunksize
        self.frequency = frequency
        self.schema = schema
        self.provider = provider

        self.postgres_interface = PostgresInterface()
        self.engine = self.postgres_interface.create_engine(provider=provider)

    def _create_yf_ticker(self, ticker_symbol: str) -> yf.Ticker:
        """
        Method to create a yfinance.Ticker object

        Parameters
        ----------
        ticker_symbol : str
            ticker symbol of the stock

        Returns
        -------
        yf.Ticker
            yfinance.Ticker object
        """

        ticker = yf.Ticker(ticker_symbol)
        return ticker

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

        tickers_list = Table(
            "tickers_list", MetaData(), autoload_with=self.engine, schema="stocks"
        )
        valid_tickers = Table(
            "valid_tickers",
            MetaData(),
            autoload_with=self.engine,
            schema="stocks",
        )

        query = (
            select(tickers_list.c.ticker)
            .outerjoin(valid_tickers, tickers_list.c.ticker == valid_tickers.c.ticker)
            .where(valid_tickers.c.ticker == null())
        )

        with self.engine.connect() as conn:
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
                    con=self.engine,
                    if_exists="append",
                    schema="stocks",
                    index=False,
                    method="multi",
                )
                # empty the valids_df
                valids_df = valids_df.iloc[0:0]

                self.logger.warning(
                    f"Inserted {self.chunksize} rows for valid tickers to valid_tickers table"
                )

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

    def load_valid_tickers(self, sink_table: str) -> List[str]:
        """
        Method to load the valid tickers from the database
        """

        valid_tickers = Table(
            "valid_tickers",
            MetaData(),
            autoload_with=self.engine,
            schema=self.schema,
        )
        balance_sheet = Table(
            sink_table, MetaData(), autoload_with=self.engine, schema=self.schema
        )
        query = (
            select(valid_tickers.c.ticker)
            .outerjoin(balance_sheet, valid_tickers.c.ticker == balance_sheet.c.ticker)
            .where(balance_sheet.c.ticker == null())
            .where(valid_tickers.c.validity == True)
        )

        with self.engine.connect() as conn:
            valid_tickers = [result[0] for result in conn.execute(query).fetchall()]

        return valid_tickers

    def update_cash_flow(self, ticker: yf.Ticker):
        """
        Method to update the cash flow table in postgres based on the tickers provided

        Parameters
        ----------
        engine: sqlalchemy.engine.Engine
            The engine to connect to the database
        tickers: List[str]
            The list of tickers to update

        Returns
        -------
        None
        """

        self.logger.warning(f"Updating cash flow for {ticker}")
        try:
            cash_flow_df = (
                ticker.cashflow.T
                if self.frequency == "annual"
                else ticker.quarterly_cashflow.T
            )
            cash_flow_df["ticker"] = ticker.ticker
            cash_flow_df["currency_code"] = ticker.info["currency"]
            cash_flow_df["insert_date"] = func.current_date()
            cash_flow_df["frequency"] = self.frequency
            cash_flow_df.reset_index(inplace=True)
            cash_flow_df.rename(columns={"index": "report_date"}, inplace=True)
            self.logger.warning(f"Data extracted for {ticker}")
        except:
            self.logger.warning(f"Ticker {ticker} has no cash flow")
            return

        # if a column does not exist in the stocks.cash_flow table, drop it from the df
        for column in cash_flow_df.columns:
            if column not in CASH_FLOW_COLUMNS:
                cash_flow_df.drop(columns=column, inplace=True)
        # if a column does not exist in the df, It will be added with null values
        for column in CASH_FLOW_COLUMNS:
            if column not in cash_flow_df.columns:
                cash_flow_df[column] = None

        # convert pd.dataframe to list of tuples
        cash_flow_list = cash_flow_df.to_dict("records")

        self.logger.warning(f"Data transformed for {ticker} cash flow")
        return cash_flow_list

    def flush_records(self, table_name: str, records: list):
        """
        Method to flush records from a table
        """
        table = self.postgres_interface.create_table_object(
            table_name=table_name, engine=self.engine, schema=self.schema
        )
        with self.engine.connect() as conn:
            # insert the data into the database on conflict update
            conn.execute(
                insert(table)
                .values(records)
                .on_conflict_do_update(
                    index_elements=["ticker", "report_date", "frequency"],
                    set_={
                        "insert_date": func.current_date(),
                    },
                )
            )
            conn.commit()

        self.logger.warning(
            f"Data flushed with {len(records)} records inserted into {table_name}"
        )

    def update_financials(self, ticker: yf.Ticker):
        """
        Method to update the financials table in postgres based on the tickers provided

        Parameters
        ----------
        engine: sqlalchemy.engine.Engine
            The engine to connect to the database
        tickers: List[str]
            The list of tickers to update

        Returns
        -------
        None
        """

        self.logger.warning(f"Updating financials for {ticker}")
        try:
            financials_df = (
                ticker.financials.T
                if self.frequency == "annual"
                else ticker.quarterly_financials.T
            )
            financials_df["ticker"] = ticker.ticker
            financials_df["currency_code"] = ticker.info["currency"]
            financials_df["insert_date"] = func.current_date()
            financials_df["frequency"] = self.frequency
            financials_df.reset_index(inplace=True)
            financials_df.rename(columns={"index": "report_date"}, inplace=True)
            self.logger.warning(f"Data extracted for {ticker}")
        except:
            self.logger.warning(f"Ticker {ticker} has no financials")
            return

        # if a column does not exist in the stocks.cash_flow table, drop it from the df
        for column in financials_df.columns:
            if column not in CASH_FLOW_COLUMNS:
                financials_df.drop(columns=column, inplace=True)
        # if a column does not exist in the df, It will be added with null values
        for column in CASH_FLOW_COLUMNS:
            if column not in financials_df.columns:
                financials_df[column] = None

        # convert pd.dataframe to list of tuples
        cash_flow_list = financials_df.to_dict("records")

        self.logger.warning(f"Data transformed for {ticker} cash flow")
        return cash_flow_list

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
                con=self.engine,
                if_exists="append",
                schema="stocks",
                index=False,
                method="multi",
            )
