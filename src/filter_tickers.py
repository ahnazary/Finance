import ast
from typing import List, Literal, Union

import pandas as pd
import yfinance as yf

from src.postgres_interface import TickersDatabaseInterface
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
