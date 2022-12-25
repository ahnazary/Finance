from src import extract
from src.filter_tickers import FilterTickers

# ticker = extract.Ticker(["USA", "Germany"])

# filtered_tickers = ticker.extract_tickers_into_db()

# ticker.filter_by_balance_sheet(filtered_tickers)

filter_tickers = FilterTickers()

filter_tickers.filter_by_balance_sheet()