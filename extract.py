import yahooquery

# print balance sheet of Apple
ticker = yahooquery.Ticker('AAPL')
print(ticker.balance_sheet())
