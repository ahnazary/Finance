import yfinance as yf

msft = yf.Ticker("MSFT")

# show balance sheet
msft.balance_sheet
msft.quarterly_balance_sheet