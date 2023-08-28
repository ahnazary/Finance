import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
from src.extract import Ticker
from src.schedule_jobs import ScheduleJobs

load_dotenv()

provider = os.environ.get("PROVIDER")

schedule_jobs = ScheduleJobs(provider=provider, batch_size=4)

# getting a list[str] of old tickers with batch_size
tickers_list = schedule_jobs.get_tickers_batch_backfill(
    table_name="financials", engine=schedule_jobs.engine, frequency="quarterly"
)

# getting a list[yf.Ticker] of old tickers with batch_size
tickers_yf_batch = schedule_jobs.get_tickers_batch_yf_object(tickers_list=tickers_list)

ticker_interface = Ticker(provider=provider, frequency="quarterly")

records = []
for ticker_yf_obj in tickers_yf_batch:
    record = ticker_interface.update_financials(ticker=ticker_yf_obj)
    if not record:
        continue
    records.append(record)


# convert list[list[dict]] to list[dict]
flattened_records = [d for sublist in records for d in sublist if sublist]

ticker_interface.flush_records(table_name="financials", records=flattened_records)
