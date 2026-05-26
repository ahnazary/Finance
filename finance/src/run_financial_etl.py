"""
Runner script for financial data ETL jobs.

Usage:
    python -m finance.src.run_financial_etl --table income_stmt [--max-batches N]
    python -m finance.src.run_financial_etl --table cash_flow [--max-batches N]
    python -m finance.src.run_financial_etl --table balance_sheet [--max-batches N]
    python -m finance.src.run_financial_etl --table financials [--max-batches N]
"""

import argparse

from config import ETL_BATCH_SIZE, ETL_THREADS, FINANCIAL_TABLES
from finance.src.financial_data_etl import FinancialDataETL
from finance.src.postgres_interface import PostgresInterface


def main():
    parser = argparse.ArgumentParser(description="Run financial data ETL job")
    parser.add_argument(
        "--table",
        required=True,
        choices=list(FINANCIAL_TABLES.keys()),
        help="Target table to populate",
    )
    parser.add_argument("--max-batches", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=ETL_BATCH_SIZE)
    parser.add_argument("--threads", type=int, default=ETL_THREADS)
    parser.add_argument("--frequency", type=str, default=None, choices=["annual", "quarterly"])
    args = parser.parse_args()

    postgres_interface = PostgresInterface()
    etl = FinancialDataETL(
        table_name=args.table,
        postgres_interface=postgres_interface,
        batch_size=args.batch_size,
        max_threads=args.threads,
        frequency=args.frequency,
    )
    etl.run(max_batches=args.max_batches)


if __name__ == "__main__":
    main()
