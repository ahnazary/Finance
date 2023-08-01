""" Entry point for the finance project. """

from src.extract import Ticker

ticker = Ticker()
ticker.update_valid_tickers_table()
