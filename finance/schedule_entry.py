import os

from dotenv import load_dotenv
from src.schedule_jobs import ScheduleJobs

load_dotenv()

provider = os.environ.get("PROVIDER")

schedule_jobs = ScheduleJobs(provider=provider, batch_size=300)
schedule_jobs.update_table_batch(table_name="cash_flow", engine=schedule_jobs.engine)
