"""
Flask web application for the Finance data pipeline.
Provides a search interface to explore financial data stored in Postgres.
"""

import os

from flask import Flask, render_template, jsonify, request
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__, template_folder="templates", static_folder="static")

NEON_CONNECTION_STRING = os.environ.get("PG_NEON_FINANCE_URL")
if not NEON_CONNECTION_STRING:
    raise EnvironmentError("PG_NEON_FINANCE_URL environment variable is not set.")

engine = create_engine(NEON_CONNECTION_STRING, pool_pre_ping=True, pool_recycle=300)

SCHEMA = "finance"
TABLES = ["income_stmt", "cash_flow", "balance_sheet", "financials"]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search")
def search_tickers():
    """Return matching tickers for autocomplete."""
    q = request.args.get("q", "").strip().upper()
    if len(q) < 1:
        return jsonify([])

    query = text(f"""
        SELECT ticker FROM {SCHEMA}.active_tickers
        WHERE is_active = true AND UPPER(ticker) LIKE :pattern
        ORDER BY ticker
        LIMIT 20
    """)

    with engine.connect() as conn:
        result = conn.execute(query, {"pattern": f"%{q}%"})
        tickers = [row[0] for row in result]

    return jsonify(tickers)


@app.route("/api/financial_data/<ticker>")
def get_financial_data(ticker: str):
    """Return all financial data for a ticker, grouped by table and frequency."""
    ticker = ticker.strip().upper()
    data = {}

    for table in TABLES:
        query = text(f"""
            SELECT ticker, frequency, report_date, metric, value
            FROM {SCHEMA}.{table}
            WHERE UPPER(ticker) = :ticker
            ORDER BY frequency, report_date, metric
        """)

        with engine.connect() as conn:
            result = conn.execute(query, {"ticker": ticker})
            rows = [
                {
                    "ticker": row[0],
                    "frequency": row[1],
                    "report_date": row[2].isoformat(),
                    "metric": row[3],
                    "value": row[4],
                }
                for row in result
            ]

        data[table] = {
            "annual": [r for r in rows if r["frequency"] == "annual"],
            "quarterly": [r for r in rows if r["frequency"] == "quarterly"],
        }

    return jsonify(data)


if __name__ == "__main__":
    app.run(debug=True, port=5001)
