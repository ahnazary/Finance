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
        Gets all the data in the data dir excel file (all available tickers) and
        inserts them into the database
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

    def load_valid_tickers(self, sink_table: str) -> List[str]:
        """
        Method to load the valid tickers from the database based
        on the validity status of the tickers in the valid_tickers table

        Parameters
        ----------
        sink_table : str
            The name of the table to load the tickers from
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
            .where(valid_tickers.c.validity)
        )

        with self.engine.connect() as conn:
            valid_tickers = [result[0] for result in conn.execute(query).fetchall()]

        return valid_tickers

    def flush_records(self, table_name: str, records: list):
        """
        Method to flush records to a table

        Parameters
        ----------
        table_name: str
            The name of the table to flush the records to
        records: list
            The records to flush to the table
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

    def extract_tickers_data(
        self, ticker: yf.Ticker, table_name: str, table_columns: list
    ) -> pd.DataFrame:
        """
        Method that gets the data from the yfinance API and transforms it into
        a list of tuples

        Parameters
        ----------
        ticker: yf.Ticker
            The ticker or stock to update
        table_name: str
            The name of the table that the ticker data is extracted for
        table_columns: list
            The columns of the table

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

            # TODO: Since yfinance does not return and Ticker.info["currency"] for the tickers,
            # the currency_code column will be set to "Unavailable" for now if the currency is
            # not available in the database
            # Visit this issue for more details: https://github.com/ranaroussi/yfinance/issues/1729
            currency_code = self.get_currency_code(ticker=ticker.ticker)
            df["currency_code"] = currency_code if currency_code else "Unavailable"

            df["insert_date"] = func.current_date()
            df["frequency"] = self.frequency
            df.reset_index(inplace=True)
            df.rename(columns={"index": "report_date"}, inplace=True)
            self.logger.warning(f"Data extracted for {ticker}")

            # if df is empty, return None
            if df.empty:
                self.logger.warning(f"Data is empty for {ticker}, returning None")
                return None
        except Exception as e:
            self.logger.warning(f"Error extracting data for {ticker}: {e}")
            self.logger.warning(
                f"Ticker {ticker} has no {table_name} data, returning None"
            )
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
        self, table_name: str, tickers: list[str], availability: bool = False
    ):
        """
        Method That gets a list of tickers and updates the validity status of the tickers
        for a specific criteria (e.g. balance_sheet_annual_availabile) in the
        valid_tickers table, e.g. if the ticker has not balance sheet data for
        the quarterly frequency, the balance_sheet_quarterly_available column
        in the valid_tickers table will be updated to False

        Parameters
        ----------
        table_name: str
            The name of the table which the ticker was supposed to be updated
        ticker: list[str]
            The tickers that was supposed to be updated
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

        # update the validity status of all the tickers at once
        query = (
            valid_tickers.update()
            .where(valid_tickers.c.ticker.in_(tickers))
            .values(
                {
                    f"{table_name}_{self.frequency}_available": availability,
                }
            )
        )

        with self.engine.connect() as conn:
            conn.execute(query)
            conn.commit()

        self.logger.warning(
            f"Validity status updated to {availability} for {len(tickers)} tickers"
        )

    def get_currency_code(self, ticker: str) -> str:
        """
        Method that gets the currency code of a ticker from valid_tickers table
        in the database

        Parameters
        ----------
        ticker: str
            The ticker symbol

        Returns
        -------
        str
            The currency code of the ticker
        """
        table = Table(
            "valid_tickers",
            MetaData(),
            autoload_with=self.engine,
            schema=self.schema,
        )

        query = select(table.c.currency_code).where(table.c.ticker == ticker)

        with self.engine.connect() as conn:
            currency_code = conn.execute(query).fetchone()[0]

        return currency_code
