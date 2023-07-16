from typing import List, Literal



class FilterTickers:
    def __init__(
        self,
        frequency: Literal["annual", "quarterly"] = "annual",
    ):
        self.frequency = frequency
