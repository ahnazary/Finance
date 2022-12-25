from typing import List, Literal, Union

import pandas as pd
import yahooquery

from .database import TickersDatabaseInterface
from .utils import Logger, are_incremental


class Ticker:
    def __init__(self, countries: Union[str, List[str]]):
        self.countries = countries
        self.data_df = pd.read_excel("src/data/Yahoo Ticker Symbols - September 2017.xlsx")
        self.logger = Logger()

    def extract_tickers_into_db(self) -> pd.DataFrame:
        """
        Method that extracts teickers that are from the given countries
        and inserts them into the database
        """

        self.filtered_df = self.data_df[self.data_df["Country"].isin(self.countries)]

        my_db = TickersDatabaseInterface()
        for _, row in self.filtered_df.iterrows():
            my_db.insert_tickers(
                ticker=row["Ticker"],
                name=row["Name"],
                exchange=row["Exchange"],
                category=row["Category Name"],
                country=row["Country"],
            )
            self.logger.info(
                f"inserted ticker {row['Ticker']} and country {row['Country']} into database"
            )
        return self.filtered_df

    def filter_by_balance_sheet(
        self,
        tickers: pd.DataFrame,
        params: List[str] = None,
        frequency: Literal["annual", "quarterly"] = "annual",
    ) -> pd.DataFrame:
        """
        method that filters the tickers if their passed parameters are increasing

        parameters:
        ----------
        params: List[str]
            list of parameters to filter by

        tickers: list[str]
            list of tickers to filter by

        frequency: Literal["annual", "quarterly"]
            frequency of the balance sheet


        """

        if params is None:
            params = ["TotalAssets"]

        tickers = tickers["Ticker"].tolist()

        for ticker in tickers:
            try:
                balance_sheet = yahooquery.Ticker(ticker).balance_sheet(frequency=frequency)
                for param in params:
                    if not are_incremental(balance_sheet[param].tolist()):
                        self.data_df = self.data_df[self.data_df["Ticker"] != ticker]
            except:
                self.logger.warning(f"ticker {ticker} does not exist on yahoo finance")
                continue
