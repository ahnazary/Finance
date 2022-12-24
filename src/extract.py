import pandas as pd
import yahooquery
from typing import Literal, Union, List
from utils import are_incremental


class Ticker:
    def __init__(self, ticker: Union[str, List[str]]):
        self.ticker = ticker
        self.data_df = pd.read_excel("data/Yahoo Ticker Symbols - September 2017.xlsx")



