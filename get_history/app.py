import json
import os
from datetime import date
from pathlib import Path

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://eodhd.com/api"
REQUEST_TIMEOUT = 30


def _load_tickers() -> list[str]:
    """Read tickers from tickers.txt in the same folder as this file."""
    tickers_path = Path(__file__).with_name("tickers.txt")
    if not tickers_path.exists():
        raise RuntimeError(f"tickers.txt not found at {tickers_path}")
    with tickers_path.open() as f:
        return [
            line.strip()
            for line in f
            if line.strip() and not line.strip().startswith("#")
        ]


def _ten_year_window() -> tuple[str, str]:
    """Return (start_date, end_date) as YYYY-MM-DD strings for exactly 10 years."""
    today = date.today()
    start = today - relativedelta(years=10)
    return start.isoformat(), today.isoformat()


def _coerce_float(value):
    try:
        return float(value)
    except Exception:
        return None


def _fetch_eod_for_symbol(symbol: str, start_date: str, end_date: str, api_token: str) -> list[dict]:
    """
    Call EODHD /eod for a single symbol and return parsed OHLCV rows.
    """
    url = f"{BASE_URL}/eod/{symbol}.US"
    params = {
        "from": start_date,
        "to": end_date,
        "api_token": api_token,
        "period": "d",
        "fmt": "json",
        "order": "a",
        "limit": 5000,
    }

    resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    try:
        data = resp.json()
    except ValueError:
        preview = resp.text[:200]
        raise RuntimeError(
            f"EODHD non-JSON response for {symbol}: status {resp.status_code}, body={preview}"
        )

    if isinstance(data, dict):
        if "error" in data:
            raise RuntimeError(f"EODHD error for {symbol}: {data['error']}")
        if "code" in data and "message" in data:
            raise RuntimeError(f"EODHD error for {symbol}: {data['message']}")
        # Some responses may wrap data under a key
        if "data" in data and isinstance(data["data"], list):
            data = data["data"]

    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected response format for {symbol}")

    rows: list[dict] = []
    for entry in data:
        dt = entry.get("date")
        if not dt:
            continue
        rows.append(
            {
                "symbol": symbol,
                "date": dt,
                "open": _coerce_float(entry.get("open")),
                "high": _coerce_float(entry.get("high")),
                "low": _coerce_float(entry.get("low")),
                "close": _coerce_float(entry.get("close")),
                "volume": _coerce_float(entry.get("volume")),
            }
        )

    rows.sort(key=lambda r: r["date"])
    return rows


def write_monthly_parquet(rows: list[dict], output_dir: Path) -> list[str]:
    """
    Group rows by year-month and write one Parquet file per month.
    Returns list of written file paths.
    """
    if not rows:
        return []

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df[["symbol", "date", "open", "high", "low", "close", "volume"]]
    df.sort_values(["date", "symbol"], inplace=True)
    df["year_month"] = df["date"].dt.to_period("M").astype(str)

    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for year_month, group in df.groupby("year_month"):
        out_path = output_dir / f"{year_month}.parquet"
        group.drop(columns=["year_month"]).to_parquet(out_path, index=False)
        written.append(str(out_path))
    return written


def run_pipeline(api_token: str, output_dir: Path) -> dict:
    start_date, end_date = _ten_year_window()
    tickers = _load_tickers()

    rows: list[dict] = []
    errors: list[dict] = []

    for sym in tickers:
        try:
            rows.extend(_fetch_eod_for_symbol(sym, start_date, end_date, api_token))
        except Exception as exc:
            errors.append({"symbol": sym, "error": str(exc)})

    files = write_monthly_parquet(rows, output_dir)

    return {
        "start_date": start_date,
        "end_date": end_date,
        "tickers_count": len(tickers),
        "rows": len(rows),
        "files_written": files,
        "errors": errors,
        "min_date": min((r["date"] for r in rows), default=None),
        "max_date": max((r["date"] for r in rows), default=None),
    }


def lambda_handler(event, context):
    """
    GetHistoryFunction – one-time backfill of last 10 years of data for all tickers.
    Writes /tmp/output/YYYY-MM.parquet in Lambda.
    """
    api_token = os.environ.get("EODHD_API_TOKEN")
    if not api_token:
        raise RuntimeError("EODHD_API_TOKEN environment variable is not set")

    project_output = Path("/tmp/output")
    result = run_pipeline(api_token, project_output)

    return {
        "statusCode": 200,
        "body": json.dumps(result),
    }


if __name__ == "__main__":
    api_token = os.environ.get("EODHD_API_TOKEN")
    if not api_token:
        raise RuntimeError("EODHD_API_TOKEN environment variable is not set")

    project_root = Path(__file__).resolve().parents[1]
    output_dir = project_root / "output"

    summary = run_pipeline(api_token, output_dir)
    print(json.dumps(summary, indent=2))
