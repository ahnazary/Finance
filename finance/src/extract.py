""" Module to extract data from the yahoo finance API and load it into the database """

from typing import List, Literal, Union

import pandas as pd
import yfinance as yf
from sqlalchemy import MetaData, Table, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import null
from src.postgres_interface import PostgresInterface
from src.utils import custom_logger


class Ticker:
    def __init__(
        self,
        countries: Union[str, List[str]] = None,
        chunksize: int = 20,
        frequency: Literal["annual", "quarterly"] = "annual",
        schema: str = "stocks",
        provider: Literal["LOCAL", "NEON"] = "LOCAL",
    ):
        self.logger = custom_logger(logger_name="ticker")
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

    def flush_records(self, table_name: str, records: list):
        """
        Method to flush records from a table
        """
        if not records:
            return
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

    def get_data_df(self, table_name: str, frequency: str, ticker: yf.Ticker):
        """
        Method that returns a df based on the name of the table and frequency

        Parameters
        ----------
        table_name: str
            The name of the table that is going to be filled
        frequency: str
            The frequency of the data to be extracted
            Either annual or quarterly

        Returns
        -------
        pd.DataFrame
            The dataframe with the data
        """
        property_dict = {
            ("income_stmt", "annual"): "income_stmt",
            ("income_stmt", "quarterly"): "quarterly_income_stmt",
            ("balance_sheet", "annual"): "balance_sheet",
            ("balance_sheet", "quarterly"): "quarterly_balance_sheet",
            ("cashflow", "annual"): "cashflow",
            ("cashflow", "quarterly"): "quarterly_cashflow",
            ("financials", "annual"): "financials",
            ("financials", "quarterly"): "quarterly_financials",
        }

        property = property_dict[(table_name, frequency)]
        df = getattr(ticker, property).T
        return df

    def update_table(self, ticker: yf.Ticker, table_name: str, table_columns: list):
        """
        Method to update a table in postgres based on the tickers provided

        Parameters
        ----------
        ticker: yf.Ticker
            The ticker or stock to update

        Returns
        -------
        None
        """

        self.logger.warning(f"Updating {table_name} for {ticker}")
        try:
            df = self.get_data_df(
                table_name=table_name, frequency=self.frequency, ticker=ticker
            )
            df["ticker"] = ticker.ticker
            df["currency_code"] = ticker.info["currency"]
            df["insert_date"] = func.current_date()
            df["frequency"] = self.frequency
            df.reset_index(inplace=True)
            df.rename(columns={"index": "report_date"}, inplace=True)
            self.logger.warning(f"Data extracted for {ticker}")
        except:
            self.logger.warning(f"Ticker {ticker} has no {table_name} data")
            self.update_validity_status(
                ticker=ticker,
                table_name=table_name,
                validity=False,
            )
            self.logger.warning(f"Validity status updated for {ticker}")
            return None

        # make column names all lower case and replace spaces with underscores
        df.columns = [i.replace(" ", "_").lower() for i in list(df.columns)]

        missed_columns = []

        # if a column does not exist in the stocks.table_name table, drop it from the df
        for column in [i.replace(" ", "_") for i in list(df.columns)]:
            if column not in table_columns:
                self.logger.warning(f"Column {column} not in {table_name} columns")
                missed_columns.append(column)
                df.drop(columns=column, inplace=True)
        # if a column does not exist in the df, It will be added with null values
        for column in table_columns:
            if column not in df.columns:
                df[column] = None

        # convert pd.dataframe to list of tuples
        result = df.to_dict("records")

        self.logger.warning(f"Data transformed for {ticker} {table_name}")
        return result

    def get_columns_names(self, table_name: str):
        """
        Method that returns the columns names of a table
        """

        table = self.postgres_interface.create_table_object(
            table_name=table_name, engine=self.engine
        )
        columns = [column.name for column in table.columns]
        return columns

    def update_validity_status(
        self, table_name: str, ticker: yf.Ticker, validity: bool = False
    ):
        """
        Method That gets a ticker and updates the validity status of the ticker
        for a specific criteria (e.g. balance_sheet_annual_availabile) in the
        valid_tickers table, e.g. if the ticker has not balance sheet data for
        the quarterly frequency, the balance_sheet_quarterly_available column
        in the valid_tickers table will be updated to False

        Parameters
        ----------
        table_name: str
            The name of the table which the ticker was supposed to be updated
        ticker: yf.Ticker
            The ticker or stock to update
        validity: bool
            The validity status of the ticker for the specific criteria
            default: False

        Returns
        -------
        None
        """

        # get the table object
        valid_tickers = Table(
            "valid_tickers",
            MetaData(),
            autoload_with=self.engine,
            schema=self.schema,
        )

        # update the validity status
        with self.engine.connect() as conn:
            conn.execute(
                valid_tickers.update()
                .where(valid_tickers.c.ticker == ticker.ticker)
                .values({f"{table_name}_{self.frequency}_available": validity})
            )
            conn.commit()
