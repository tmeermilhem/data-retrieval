import json
import os
import csv
from datetime import date
from pathlib import Path

import requests
from dateutil.relativedelta import relativedelta
import yaml

BASE_URL = "https://api.twelvedata.com"


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

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


def _load_features() -> dict:
    """Read features.yml next to this file."""
    features_path = Path(__file__).with_name("features.yml")
    if not features_path.exists():
        raise RuntimeError(f"features.yml not found at {features_path}")
    with features_path.open() as f:
        return yaml.safe_load(f)


def _ten_year_window() -> tuple[str, str]:
    """Return (start_date, end_date) as YYYY-MM-DD strings for exactly 10 years."""
    today = date.today()
    start = today - relativedelta(years=10)
    return start.isoformat(), today.isoformat()


# ---------------------------------------------------------------------------
# OHLCV fetch (time_series)
# ---------------------------------------------------------------------------

def _fetch_history_for_symbol(symbol: str, start_date: str, end_date: str, api_key: str):
    """
    Call Twelve Data time_series API for daily OHLCV for a single symbol.
    Returns a list of rows (dicts) sorted by date.
    """
    params = {
        "symbol": symbol,
        "interval": "1day",
        "start_date": start_date,
        "end_date": end_date,
        "order": "ASC",
        "apikey": api_key,
        "timezone": "UTC",
        "dp": "5",
    }

    resp = requests.get(f"{BASE_URL}/time_series", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Basic Twelve Data error handling
    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(f"Twelve Data error for {symbol}: {data.get('message')}")

    values = data.get("values", [])
    rows = []
    for v in values:
        dt = v.get("datetime", "")[:10]  # 'YYYY-MM-DD'
        if not dt:
            continue
        rows.append(
            {
                "date": dt,
                "open": v.get("open"),
                "high": v.get("high"),
                "low": v.get("low"),
                "close": v.get("close"),
                "volume": v.get("volume"),
            }
        )

    # Sort by date just in case
    rows.sort(key=lambda r: r["date"])
    return rows


# ---------------------------------------------------------------------------
# Derived features (pure Python)
# ---------------------------------------------------------------------------

def _safe_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _window_std(values, end_idx, window):
    """Std dev of last `window` non-None values up to index end_idx (inclusive)."""
    start = max(0, end_idx - window + 1)
    slice_vals = [v for v in values[start:end_idx + 1] if v is not None]
    n = len(slice_vals)
    if n < 2:
        return None
    mean = sum(slice_vals) / n
    var = sum((v - mean) ** 2 for v in slice_vals) / (n - 1)
    return var ** 0.5


def compute_derived_features(rows, derived_list):
    """
    rows = list of dicts from OHLCV fetch
    derived_list = list from features.yml under core.derived
    Returns updated rows with new derived columns (in-place).
    """
    if not rows:
        return rows

    # ensure sorted by date
    rows.sort(key=lambda r: r["date"])

    closes = [_safe_float(r.get("close")) for r in rows]
    opens = [_safe_float(r.get("open")) for r in rows]
    volumes = [_safe_float(r.get("volume")) for r in rows]

    n = len(rows)

    # daily_return
    if "daily_return" in derived_list:
        prev_close = None
        for i in range(n):
            c = closes[i]
            if c is None or prev_close is None:
                rows[i]["daily_return"] = None
            else:
                rows[i]["daily_return"] = c / prev_close - 1.0
            if c is not None:
                prev_close = c

    # rolling_volatility_5d
    if "rolling_volatility_5d" in derived_list:
        for i in range(n):
            rows[i]["rolling_volatility_5d"] = _window_std(closes, i, 5)

    # rolling_volatility_20d
    if "rolling_volatility_20d" in derived_list:
        for i in range(n):
            rows[i]["rolling_volatility_20d"] = _window_std(closes, i, 20)

    # price_gap = open - prev_close
    if "price_gap" in derived_list:
        prev_close = None
        for i in range(n):
            o = opens[i]
            if o is None or prev_close is None:
                rows[i]["price_gap"] = None
            else:
                rows[i]["price_gap"] = o - prev_close
            if closes[i] is not None:
                prev_close = closes[i]

    # volume_zscore: (vol - mean)/std over 20d window
    if "volume_zscore" in derived_list:
        for i in range(n):
            v = volumes[i]
            if v is None:
                rows[i]["volume_zscore"] = None
                continue
            std = _window_std(volumes, i, 20)
            if std is None or std == 0:
                rows[i]["volume_zscore"] = None
                continue
            # compute mean over same window
            start = max(0, i - 20 + 1)
            slice_vals = [x for x in volumes[start:i + 1] if x is not None]
            if len(slice_vals) == 0:
                rows[i]["volume_zscore"] = None
                continue
            mean = sum(slice_vals) / len(slice_vals)
            rows[i]["volume_zscore"] = (v - mean) / std

    return rows


# ---------------------------------------------------------------------------
# Technical indicators (from Twelve Data)
# ---------------------------------------------------------------------------

def fetch_indicator(symbol, indicator_name, params, start_date, end_date, api_key):
    """
    Generic Twelve Data indicator fetcher.
    Returns: dict[date] -> { feature_name: value, ... }
    """
    url = f"{BASE_URL}/{indicator_name}"
    q = {
        "symbol": symbol,
        "interval": "1day",
        "start_date": start_date,
        "end_date": end_date,
        "apikey": api_key,
    }
    if params:
        q.update(params)

    resp = requests.get(url, params=q, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    if isinstance(data, dict) and data.get("status") == "error":
        raise RuntimeError(f"Twelve Data {indicator_name} error for {symbol}: {data.get('message')}")

    if "values" not in data:
        return {}

    output: dict[str, dict] = {}
    for row in data["values"]:
        dt = row.get("datetime", "")[:10]
        if not dt:
            continue
        output.setdefault(dt, {})
        for k, v in row.items():
            if k == "datetime":
                continue
            output[dt][f"{indicator_name}_{k}"] = v

    return output


def fetch_all_indicators(symbol, tech_cfg, start_date, end_date, api_key):
    """
    Loops through entire 'technicals' section from features.yml.
    Returns dict: date -> merged feature dict for ALL indicators.
    """
    combined: dict[str, dict] = {}

    if not tech_cfg:
        return combined

    for name, cfg in tech_cfg.items():
        if cfg is None:
            cfg = {}

        # indicators with multiple periods (sma, ema, rsi)
        if isinstance(cfg, dict) and "periods" in cfg:
            for p in cfg["periods"]:
                params = {"time_period": p}
                try:
                    result = fetch_indicator(symbol, name, params, start_date, end_date, api_key)
                except Exception:
                    continue
                for dt, feats in result.items():
                    combined.setdefault(dt, {}).update(feats)
            continue

        # others (macd, atr, bollinger_bands, adx, cci, stochastic_oscillator, obv, mfi, etc.)
        params = cfg if isinstance(cfg, dict) else {}
        try:
            result = fetch_indicator(symbol, name, params, start_date, end_date, api_key)
        except Exception:
            continue

        for dt, feats in result.items():
            combined.setdefault(dt, {}).update(feats)

    return combined


# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def _write_csv(symbol: str, rows: list[dict], output_dir: Path) -> str:
    """Write rows to output/<symbol>.csv and return the path as a string."""
    output_dir.mkdir(parents=True, exist_ok=True)
    file_path = output_dir / f"{symbol}.csv"

    if not rows:
        with file_path.open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["date", "open", "high", "low", "close", "volume"])
        return str(file_path)

    # union of all keys across rows
    all_keys = set()
    for r in rows:
        all_keys.update(r.keys())
    fieldnames = ["date"] + sorted(k for k in all_keys if k != "date")

    with file_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    return str(file_path)


# ---------------------------------------------------------------------------
# Orchestrator for one symbol
# ---------------------------------------------------------------------------

def run_for_symbol(symbol: str, start_date: str, end_date: str, api_key: str, features_cfg: dict, output_dir: Path):
    """
    Run full 10y pipeline for a single symbol:
    - OHLCV
    - Derived features
    - Technical indicators
    - Write CSV
    """
    # 1) OHLCV
    rows = _fetch_history_for_symbol(symbol, start_date, end_date, api_key)

    # 2) Derived features
    core_cfg = features_cfg.get("core", {})
    derived_list = core_cfg.get("derived", []) or []
    rows = compute_derived_features(rows, derived_list)

    # 3) Technical indicators
    tech_cfg = features_cfg.get("technicals", {}) or {}
    indicator_dict = fetch_all_indicators(symbol, tech_cfg, start_date, end_date, api_key)

    # Merge indicators into rows (by date)
    for r in rows:
        extras = indicator_dict.get(r["date"], {})
        r.update(extras)
    # symbol column
    for r in rows:
        r["symbol"] = symbol
    # 4) Write CSV
    path = _write_csv(symbol, rows, output_dir)
    return {
        "symbol": symbol,
        "rows": len(rows),
        "file": path,
    }


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """
    GetHistoryFunction – one-time backfill of last 10 years of data for all tickers.
    Writes /tmp/output/<symbol>.csv in Lambda.
    """
    api_key = os.environ.get("TWELVE_DATA_API_KEY")
    if not api_key:
        raise RuntimeError("TWELVE_DATA_API_KEY environment variable is not set")

    start_date, end_date = _ten_year_window()
    tickers = _load_tickers()
    features_cfg = _load_features()

    output_dir = Path("/tmp/output")

    results = []
    errors = []

    for sym in tickers:
        try:
            res = run_for_symbol(sym, start_date, end_date, api_key, features_cfg, output_dir)
            results.append(res)
        except Exception as e:
            errors.append({"symbol": sym, "error": str(e)})

    body = {
        "start_date": start_date,
        "end_date": end_date,
        "tickers_count": len(tickers),
        "success": results,
        "errors": errors,
    }

    return {
        "statusCode": 200,
        "body": json.dumps(body),
    }


# ---------------------------------------------------------------------------
# Local runner – writes to ./output on your machine
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    api_key = os.environ.get("TWELVE_DATA_API_KEY")
    if not api_key:
        raise RuntimeError("TWELVE_DATA_API_KEY environment variable is not set")

    start_date, end_date = _ten_year_window()
    tickers = _load_tickers()
    features_cfg = _load_features()

    project_root = Path(__file__).resolve().parents[1]
    output_dir = project_root / "output"

    results = []
    errors = []

    for sym in tickers:
        try:
            res = run_for_symbol(sym, start_date, end_date, api_key, features_cfg, output_dir)
            results.append(res)
        except Exception as e:
            errors.append({"symbol": sym, "error": str(e)})

    summary = {
        "start_date": start_date,
        "end_date": end_date,
        "tickers_count": len(tickers),
        "success": results,
        "errors": errors,
    }
    print(json.dumps(summary, indent=2))