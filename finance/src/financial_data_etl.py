"""
Financial data ETL job module.

Fetches financial statement data (income_stmt, cash_flow, balance_sheet, financials)
from Yahoo Finance via stockdex for active tickers and upserts into Postgres.

Prioritization:
1. Tickers NOT yet present in the target table (new tickers first)
2. Tickers with the oldest insert_datetime (stale data refreshed)
"""

import logging
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import text
from stockdex import Ticker

from config import SCHEMA, ETL_BATCH_SIZE, ETL_THREADS, FINANCIAL_TABLES
from finance.src.postgres_interface import PostgresInterface

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def _fetch_financial_data(ticker_symbol: str, stockdex_method: str) -> pd.DataFrame | None:
    """
    Fetch financial data for a single ticker using the specified stockdex method.
    Returns a melted (long-format) DataFrame or None on failure.
    """
    try:
        t = Ticker(ticker_symbol)
        df = getattr(t, stockdex_method)()
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def _melt_financial_df(ticker_symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a financial DataFrame from stockdex into long format:
    ticker, frequency, report_date, metric, value, insert_datetime

    stockdex returns: index=dates (e.g. '2022-09-30'), columns=metrics,
    values=strings like '99.80B', '1.23M', '456.78K' or plain numbers.
    """
    # Index is dates, columns are metric names
    df = df.copy()
    df.index.name = "report_date"
    df = df.reset_index()

    # Melt: report_date stays, all metric columns become rows
    metric_cols = [c for c in df.columns if c != "report_date"]
    melted = df.melt(id_vars=["report_date"], value_vars=metric_cols, var_name="metric", value_name="value_raw")

    # Parse values: handle suffixes like B, M, K, T
    def parse_value(v):
        if pd.isna(v) or v == "" or v == "--":
            return None
        v = str(v).strip().replace(",", "")
        multipliers = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}
        for suffix, mult in multipliers.items():
            if v.endswith(suffix):
                try:
                    return float(v[:-1]) * mult
                except ValueError:
                    return None
        try:
            return float(v)
        except ValueError:
            return None

    melted["value"] = melted["value_raw"].apply(parse_value)
    melted = melted.dropna(subset=["value"])

    # Parse report_date
    melted["report_date"] = pd.to_datetime(melted["report_date"], errors="coerce").dt.date
    melted = melted.dropna(subset=["report_date"])

    # Determine frequency from metric name prefix (annual/quarterly)
    melted["frequency"] = melted["metric"].apply(
        lambda m: "quarterly" if m.startswith("quarterly") else "annual"
    )

    melted["ticker"] = ticker_symbol
    melted["insert_datetime"] = datetime.utcnow()

    return melted[["ticker", "frequency", "report_date", "metric", "value", "insert_datetime"]]


class FinancialDataETL:
    """
    ETL job that fetches financial data for active tickers and upserts into
    the corresponding Postgres table (income_stmt, cash_flow, balance_sheet, financials).

    Prioritization:
    1. Active tickers NOT in the target table yet
    2. Active tickers with oldest insert_datetime in the target table
    """

    def __init__(
        self,
        table_name: str,
        postgres_interface: PostgresInterface | None = None,
        batch_size: int = ETL_BATCH_SIZE,
        max_threads: int = ETL_THREADS,
    ):
        if table_name not in FINANCIAL_TABLES:
            raise ValueError(f"Unknown table: {table_name}. Must be one of {list(FINANCIAL_TABLES.keys())}")

        self.table_name = table_name
        self.stockdex_method = FINANCIAL_TABLES[table_name]
        self.postgres_interface = postgres_interface or PostgresInterface()
        self.engine = self.postgres_interface.get_engine()
        self.batch_size = batch_size
        self.max_threads = max_threads

    def get_priority_tickers(self) -> list[str]:
        """
        Get batch_size tickers to process, prioritizing:
        1. Active tickers NOT yet in the target table
        2. Active tickers with oldest insert_datetime in the target table
        """
        query = text(f"""
            WITH active AS (
                SELECT ticker FROM {SCHEMA}.active_tickers WHERE is_active = true
            ),
            existing AS (
                SELECT ticker, MAX(insert_datetime) as last_insert
                FROM {SCHEMA}.{self.table_name}
                GROUP BY ticker
            )
            SELECT a.ticker
            FROM active a
            LEFT JOIN existing e ON a.ticker = e.ticker
            ORDER BY
                CASE WHEN e.ticker IS NULL THEN 0 ELSE 1 END,  -- new tickers first
                e.last_insert ASC NULLS FIRST                    -- then oldest data
            LIMIT :batch_size
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"batch_size": self.batch_size})
            tickers = [row[0] for row in result]

        return tickers

    def upsert_financial_data(self, df: pd.DataFrame) -> int:
        """
        Upsert financial data into the target table.
        Uses DELETE + INSERT for the affected tickers (simpler than ON CONFLICT for multi-column keys).
        Returns number of rows inserted.
        """
        if df.empty:
            return 0

        tickers = df["ticker"].unique().tolist()

        with self.engine.begin() as conn:
            # Delete existing data for these tickers
            delete_sql = text(f"DELETE FROM {SCHEMA}.{self.table_name} WHERE ticker = ANY(:tickers)")
            conn.execute(delete_sql, {"tickers": tickers})

            # Insert new data
            records = df.to_dict("records")
            insert_sql = text(f"""
                INSERT INTO {SCHEMA}.{self.table_name}
                (ticker, frequency, report_date, metric, value, insert_datetime)
                VALUES (:ticker, :frequency, :report_date, :metric, :value, :insert_datetime)
            """)
            conn.execute(insert_sql, records)

        return len(df)

    def _process_ticker(self, ticker_symbol: str) -> pd.DataFrame | None:
        """Fetch and melt data for a single ticker."""
        raw_df = _fetch_financial_data(ticker_symbol, self.stockdex_method)
        if raw_df is None:
            return None
        try:
            return _melt_financial_df(ticker_symbol, raw_df)
        except Exception as e:
            logger.debug(f"Failed to melt data for {ticker_symbol}: {e}")
            return None

    def run(self, max_batches: int | None = None) -> None:
        """
        Main entry point. Fetches priority tickers, gets their financial data
        in parallel, and upserts into the target table.
        """
        batches_done = 0
        total_rows_inserted = 0
        total_tickers_processed = 0
        total_tickers_with_data = 0

        while True:
            tickers = self.get_priority_tickers()
            if not tickers:
                logger.info(f"[{self.table_name}] No more active tickers to process.")
                break

            batch_start = time.time()
            all_dfs = []

            # Fetch data in parallel
            with ThreadPoolExecutor(max_workers=self.max_threads) as executor:
                futures = {
                    executor.submit(self._process_ticker, ticker): ticker
                    for ticker in tickers
                }
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        result_df = future.result(timeout=30)
                        if result_df is not None and not result_df.empty:
                            all_dfs.append(result_df)
                            total_tickers_with_data += 1
                    except Exception:
                        pass

            total_tickers_processed += len(tickers)

            # Combine and upsert
            if all_dfs:
                combined_df = pd.concat(all_dfs, ignore_index=True)
                rows_inserted = self.upsert_financial_data(combined_df)
                total_rows_inserted += rows_inserted
            else:
                rows_inserted = 0

            batches_done += 1
            elapsed = time.time() - batch_start

            logger.info(
                f"[{self.table_name}] Batch {batches_done} | "
                f"{len(tickers)} tickers fetched, {len(all_dfs)} had data | "
                f"{rows_inserted} rows inserted | "
                f"{elapsed:.1f}s"
            )

            if max_batches and batches_done >= max_batches:
                logger.info(f"[{self.table_name}] Reached max_batches={max_batches}, stopping.")
                break

        logger.info(
            f"[{self.table_name}] Complete | "
            f"Tickers processed: {total_tickers_processed}, "
            f"With data: {total_tickers_with_data}, "
            f"Total rows inserted: {total_rows_inserted}"
        )
