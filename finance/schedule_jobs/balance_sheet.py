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
    frequency = os.getenv("FREQUENCY")
    table_name = "balance_sheet"
    logger = custom_logger(logger_name=table_name, log_level=config.LOG_LEVEL)

    jobs = Jobs(
        provider=provider,
        table_name="balance_sheet",
        frequency=frequency,
    )
    tickers = jobs.run_pipeline(attribute="balance_sheet")
