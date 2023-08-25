import os

from dotenv import load_dotenv
from src.extract import Ticker
from src.schedule_jobs import ScheduleJobs

load_dotenv()

provider = os.environ.get("PROVIDER")

schedule_jobs = ScheduleJobs(provider=provider, batch_size=100)

# getting a list[str] of old tickers with batch_size
tickers_list = schedule_jobs.get_tickers_batch(
    table_name="cash_flow", engine=schedule_jobs.engine
)

# getting a list[yf.Ticker] of old tickers with batch_size
tickers_yf_batch = schedule_jobs.get_tickers_batch_yf_object(tickers_list=tickers_list)

ticker_interface = Ticker(provider=provider)

for ticker_yf_obj in tickers_yf_batch:
    ticker_interface.update_cash_flow(engine=schedule_jobs.engine, ticker=ticker_yf_obj)
