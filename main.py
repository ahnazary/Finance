from src import extract

ticker = extract.Ticker(["USA", "Germany"])

filtered_tickers = ticker.extract_tickers_into_db()

ticker.filter_by_balance_sheet(filtered_tickers)
