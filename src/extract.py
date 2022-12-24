import pandas as pd
import yahooquery
from typing import Literal, Union, List


class Ticker:
    def __init__(self, countries: Union[str, List[str]]):
        self.countries = countries
        self.data_df = pd.read_excel("src/data/Yahoo Ticker Symbols - September 2017.xlsx")

    def extract_tickers(self) -> pd.DataFrame:
        """
        method that extracts teickers that are from the given countries
        """

        filtered_df = self.data_df[self.data_df["Country"].isin(self.countries)]

        return filtered_df
