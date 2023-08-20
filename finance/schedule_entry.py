from src.schedule_jobs import ScheduleJobs

schedule_jobs = ScheduleJobs()
schedule_jobs.get_tickers_batch("cash_flow", schedule_jobs.engine_local)
