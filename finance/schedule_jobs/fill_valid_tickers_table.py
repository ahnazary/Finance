import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv
from src.utils import custom_logger

import config
from finance.src.jobs import Jobs

load_dotenv()

if __name__ == "__main__":
    provider = os.getenv("PROVIDER")
    table_name = "valid_tickers"
    logger = custom_logger(logger_name=table_name, log_level=config.LOG_LEVEL)

    jobs = Jobs(
        provider=provider,
        table_name=table_name,
    )

    jobs.fill_valid_tickers_table()
