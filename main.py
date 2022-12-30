from src import extract
from src.database import TickersDatabaseInterface
from src.filter_tickers import FilterTickers

# ticker = extract.Ticker(["USA", "Germany"])

# filtered_tickers = ticker.extract_tickers_into_db()

# ticker.filter_by_balance_sheet(filtered_tickers)

# filter_tickers = FilterTickers()

# filter_tickers.update_balance_sheet_table()

db_interface = TickersDatabaseInterface()

db_interface.update_tickers_status()