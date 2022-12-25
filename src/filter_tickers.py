from .database import TickersDatabaseInterface
from typing import List, Literal, Union
from .utils import are_incremental

import pandas as pd
import yahooquery

class FilterTickers:
    def __init__(self, params: List[str] = None, frequency: Literal["annual", "quarterly"] = "annual"):
        self.database_interface = TickersDatabaseInterface()
        self.symbol_list = self.database_interface.get_symbols()
        self.params = params or ["TotalAssets"]
        self.frequency = frequency

    def filter_by_balance_sheet(self):

        for symbol in self.symbol_list:
            try:
                balance_sheet = yahooquery.Ticker(symbol).balance_sheet(frequency=self.frequency)
                for param in self.params:
                    if not are_incremental(balance_sheet[param].tolist()):
                        self.data_df = self.data_df[self.data_df["Ticker"] != symbol]
            except:
                self.logger.warning(f"ticker {symbol} does not exist on yahoo finance")
                continue