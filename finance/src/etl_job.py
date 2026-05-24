"""
ETL job module for the finance data pipeline.

Reads stock tickers from the Excel file, checks them against Yahoo Finance API
using stockdex, and manages the active_tickers table accordingly.

Modes:
- single: ThreadPoolExecutor for concurrent HTTP calls within one process
- distributed: ProcessPoolExecutor across CPU cores, each with thread pools
"""

import os
import logging
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import text
from stockdex import Ticker

from finance.src.postgres_interface import PostgresInterface

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TICKERS_FILE = os.path.join(os.path.dirname(__file__), "data", "tickers_list.xlsx")
BATCH_SIZE = 100
SCHEMA = "finance"
MAX_THREADS = 20  # concurrent HTTP requests per process
TICKER_TIMEOUT = 15  # seconds max per ticker

YAHOO_METHODS = [
    "yahoo_api_income_statement",
    "yahoo_api_cash_flow",
    "yahoo_api_balance_sheet",
    "yahoo_api_financials",
]


def _check_single_ticker(ticker_symbol: str) -> bool:
    """
    Standalone function (picklable for multiprocessing).
    Returns True if at least one Yahoo endpoint returns data.
    """
    try:
        t = Ticker(ticker_symbol)
    except Exception:
        return False

    for method in YAHOO_METHODS:
        try:
            result = getattr(t, method)()
            if result is not None and not result.empty:
                return True
        except Exception:
            continue
    return False


def _check_batch_distributed(ticker_rows: list[dict]) -> list[dict]:
    """
    Process a batch of ticker rows in a subprocess with threaded HTTP calls.
    Used by distributed mode.
    """
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {
            executor.submit(_check_single_ticker, row["ticker"]): row
            for row in ticker_rows
        }
        results = []
        for future in as_completed(futures):
            row = futures[future]
            row["is_active"] = future.result()
            results.append(row)
    return results


class ETLJob:
    """
    ETL job class that:
    - Reads tickers from the Excel file
    - Validates them against Yahoo Finance API (stockdex) in parallel
    - Upserts active/inactive tickers into the active_tickers table

    Modes:
    - 'single': One process, multiple threads (good for CI / small machines)
    - 'distributed': Multiple processes × multiple threads (max throughput)
    """

    def __init__(
        self,
        postgres_interface: PostgresInterface | None = None,
        tickers_file: str = TICKERS_FILE,
        batch_size: int = BATCH_SIZE,
        mode: str = "single",
        max_threads: int = MAX_THREADS,
    ):
        self.postgres_interface = postgres_interface or PostgresInterface()
        self.engine = self.postgres_interface.get_engine()
        self.tickers_file = tickers_file
        self.batch_size = batch_size
        self.mode = mode  # 'single' or 'distributed'
        self.max_threads = max_threads

    def read_tickers_from_excel(self) -> pd.DataFrame:
        """Read tickers from the Excel file."""
        df = pd.read_excel(self.tickers_file)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        logger.info(f"Read {len(df)} tickers from {self.tickers_file}")
        return df

    def _check_batch_single(self, ticker_rows: list[dict]) -> list[dict]:
        """Check a batch using ThreadPoolExecutor (single mode) with per-ticker timeout."""
        with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
            futures = {
                executor.submit(_check_single_ticker, row["ticker"]): row
                for row in ticker_rows
            }
            for future in as_completed(futures):
                row = futures[future]
                try:
                    row["is_active"] = future.result(timeout=TICKER_TIMEOUT)
                except Exception:
                    row["is_active"] = False
        return ticker_rows

    def upsert_active_tickers_batch(self, records: list[dict]) -> None:
        """
        Upsert a batch of tickers into the active_tickers table.
        Uses INSERT ... ON CONFLICT DO UPDATE.
        """
        if not records:
            return

        upsert_sql = text(f"""
            INSERT INTO {SCHEMA}.active_tickers (ticker, name, exchange, category_name, country, is_active, upsert_datetime)
            VALUES (:ticker, :name, :exchange, :category_name, :country, :is_active, :upsert_datetime)
            ON CONFLICT (ticker) DO UPDATE SET
                name = EXCLUDED.name,
                exchange = EXCLUDED.exchange,
                category_name = EXCLUDED.category_name,
                country = EXCLUDED.country,
                is_active = EXCLUDED.is_active,
                upsert_datetime = EXCLUDED.upsert_datetime
        """)

        now = datetime.utcnow()
        db_records = [
            {
                "ticker": r["ticker"],
                "name": r.get("name"),
                "exchange": r.get("exchange"),
                "category_name": r.get("category_name"),
                "country": r.get("country"),
                "is_active": r["is_active"],
                "upsert_datetime": now,
            }
            for r in records
        ]

        with self.engine.begin() as conn:
            conn.execute(upsert_sql, db_records)

    def run_active_tickers_check(self, max_batches: int | None = None) -> None:
        """
        Main method: reads tickers, skips already-checked ones, checks in parallel,
        and upserts results.

        Parameters
        ----------
        max_batches : int | None
            If set, stop after this many batches.
        """
        df = self.read_tickers_from_excel()

        # Skip tickers already in the active_tickers table
        with self.engine.connect() as conn:
            existing = pd.read_sql(
                text(f"SELECT ticker FROM {SCHEMA}.active_tickers"), conn
            )
        existing_set = set(existing["ticker"].str.upper()) if not existing.empty else set()
        before = len(df)
        df = df[~df["ticker"].str.upper().isin(existing_set)].reset_index(drop=True)
        logger.info(f"Skipped {before - len(df)} already in DB, {len(df)} remaining")

        total_tickers = len(df)
        if total_tickers == 0:
            logger.info("No new tickers to check.")
            return

        # Convert to list of dicts for parallel processing
        all_rows = df.to_dict("records")
        total_batches = (total_tickers + self.batch_size - 1) // self.batch_size
        logger.info(
            f"Mode: {self.mode} | threads={self.max_threads} | "
            f"{total_tickers} tickers, batch_size={self.batch_size}, ~{total_batches} batches"
        )

        batches_done = 0
        total_active = 0
        total_inactive = 0

        for i in range(0, total_tickers, self.batch_size):
            batch_start = time.time()
            batch_rows = all_rows[i : i + self.batch_size]

            if self.mode == "distributed":
                # Split batch across processes
                num_workers = min(os.cpu_count() or 4, 4)
                chunk_size = max(1, len(batch_rows) // num_workers)
                chunks = [
                    batch_rows[j : j + chunk_size]
                    for j in range(0, len(batch_rows), chunk_size)
                ]
                results = []
                with ProcessPoolExecutor(max_workers=num_workers) as proc_executor:
                    futures = [
                        proc_executor.submit(_check_batch_distributed, chunk)
                        for chunk in chunks
                    ]
                    for f in as_completed(futures):
                        results.extend(f.result())
            else:
                # Single mode: threaded within this process
                results = self._check_batch_single(batch_rows)

            # Upsert results
            self.upsert_active_tickers_batch(results)
            batches_done += 1

            batch_active = sum(1 for r in results if r["is_active"])
            batch_inactive = len(results) - batch_active
            total_active += batch_active
            total_inactive += batch_inactive
            elapsed = time.time() - batch_start

            logger.info(
                f"Batch {batches_done}/{total_batches} | "
                f"{batch_active} active, {batch_inactive} inactive | "
                f"{elapsed:.1f}s | "
                f"Progress: {min(i + self.batch_size, total_tickers)}/{total_tickers} "
                f"({min(i + self.batch_size, total_tickers) / total_tickers * 100:.1f}%)"
            )

            if max_batches and batches_done >= max_batches:
                logger.info(f"Reached max_batches={max_batches}, stopping.")
                break

        logger.info(
            f"Complete | Total: {total_active + total_inactive}, "
            f"Active: {total_active}, Inactive: {total_inactive}"
        )
