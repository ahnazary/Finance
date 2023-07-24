""" Extract data from yahoo finance and store it in a postgres database"""

from typing import Literal


class FilterTickers:
    def __init__(
        self,
        frequency: Literal["annual", "quarterly"] = "annual",
    ):
        self.frequency = frequency
