"""
Runner script for the active tickers check.

Uses the ETLJob class to validate all tickers from the Excel file
against Yahoo Finance API and upsert results into the active_tickers table.

Usage:
    python -m finance.src.run_active_tickers_check [--max-batches N]
"""

import argparse

from finance.src.etl_job import ETLJob
from finance.src.postgres_interface import PostgresInterface


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--mode", choices=["single", "distributed"], default="single",
                        help="single: threaded in one process; distributed: multi-process + threads")
    parser.add_argument("--threads", type=int, default=20, help="Max concurrent HTTP threads per process")
    args = parser.parse_args()

    postgres_interface = PostgresInterface()
    etl_job = ETLJob(
        postgres_interface=postgres_interface,
        mode=args.mode,
        max_threads=args.threads,
    )
    etl_job.run_active_tickers_check(max_batches=args.max_batches)


if __name__ == "__main__":
    main()
