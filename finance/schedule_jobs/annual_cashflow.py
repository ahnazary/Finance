import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from logging import getLogger

from dotenv import load_dotenv
from src.extract import Ticker
from src.schedule_jobs import ScheduleJobs

import config

logger = getLogger(__name__)

load_dotenv()

provider = "NEON"
table_name = "cashflow"
frequency = "annual"

schedule_jobs = ScheduleJobs(provider=provider, batch_size=50)

# getting a list[str] of old tickers with batch_size
tickers_list = schedule_jobs.get_tickers_batch_backfill(
    table_name=table_name, engine=schedule_jobs.engine, frequency=frequency
)
tickers_list = [x for x in tickers_list if x is not None]

# getting a list[yf.Ticker] of old tickers with batch_size
tickers_yf_batch = schedule_jobs.get_tickers_batch_yf_object(tickers_list=tickers_list)

ticker_interface = Ticker(provider=provider, frequency=frequency)

table_columns = ticker_interface.get_columns_names(table_name=table_name)

records = []
for ticker_yf_obj in tickers_yf_batch:
    record = ticker_interface.update_table(
        ticker=ticker_yf_obj,
        table_name=table_name,
        table_columns=table_columns,
    )
    records.append(record)
    logger.info(
        f"record: {record} has been added to records, records length: {len(records)}"
    )


# convert list[list[dict]] to list[dict]
flattened_records = [d for sublist in records for d in sublist if sublist]

ticker_interface.flush_records(table_name=table_name, records=flattened_records)
