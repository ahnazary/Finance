import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from logging import getLogger

from dotenv import load_dotenv
from src.extract import Ticker
from src.schedule_jobs import ScheduleJobs
from src.utils import custom_logger

import config

logger = getLogger(__name__)

load_dotenv()

provider = os.getenv("PROVIDER")
table_name = "cashflow"
frequency = "annual"
logger = custom_logger(logger_name=table_name, log_level=config.LOG_LEVEL)

schedule_jobs = ScheduleJobs(provider=provider, batch_size=config.BATCH_SIZE)

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
    if record is not None:
        records.append(record)
        logger.warning(
            f"record: {record} has been added to records, records length: {len(records)}"
        )


# convert list[list[dict]] to list[dict]
flattened_records = [item for sublist in records for item in sublist]

ticker_interface.flush_records(table_name=table_name, records=flattened_records)