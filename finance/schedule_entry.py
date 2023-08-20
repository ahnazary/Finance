from src.schedule_jobs import ScheduleJobs

schedule_jobs = ScheduleJobs()
schedule_jobs.update_table_batch(
    table_name="cash_flow", engine=schedule_jobs.engine_local
)
