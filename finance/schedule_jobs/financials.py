import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv
from src.utils import emit_log

import config
from finance.src.jobs import Jobs

load_dotenv()

if __name__ == "__main__":
    provider = os.getenv("PROVIDER")
    frequency = os.getenv("FREQUENCY")
    table_name = "financials"
    emit_log(f"Running pipeline for {table_name} table", log_level=config.LOG_LEVEL)

    jobs = Jobs(
        provider=provider,
        table_name=table_name,
        frequency=frequency,
    )
    tickers = jobs.run_pipeline(attribute="financials")
