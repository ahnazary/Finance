import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from dotenv import load_dotenv
from src.schedule_jobs import ScheduleJobs
from src.utils import custom_logger

import config

load_dotenv()

if __name__ == "__main__":
    provider = os.environ.get("PROVIDER")
    table_name = "income_stmt"
    frequency = "annual"
    logger = custom_logger(logger_name=table_name, log_level=config.LOG_LEVEL)

    schedule_jobs = ScheduleJobs(
        provider=provider,
        batch_size=config.BATCH_SIZE,
        table_name=table_name,
        frequency=frequency,
    )

    # run the pipeline
    schedule_jobs.run_pipeline()
