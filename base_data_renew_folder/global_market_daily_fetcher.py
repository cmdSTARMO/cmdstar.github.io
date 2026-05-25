import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from parquet_incremental import upsert_monthly_parquet
except ImportError:
    from base_data_renew_folder.parquet_incremental import upsert_monthly_parquet


REPO_ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "global_market_daily_data")
FNAME_TPL = "global_market_daily_{symbol_key}_{yyyymm}.parquet"
INITIAL_START = "1999-12-30"
REQUEST_SLEEP_SECONDS = float(os.getenv("GLOBAL_MARKET_DAILY_SLEEP", "3"))
RETRIES = int(os.getenv("GLOBAL_MARKET_DAILY_RETRIES", "5"))
RETRY_SLEEP_SECONDS = float(os.getenv("GLOBAL_MARKET_DAILY_RETRY_SLEEP", "30"))
FULL_REFRESH = os.getenv("GLOBAL_MARKET_DAILY_FULL_REFRESH", "0") == "1"

BASE_URL = "https://query1.finance.yahoo.com/v8/finance/chart"

MARKETS = {
    "^IXIC": "纳斯达克",
    "^DJI": "道琼斯",
    "^SPX": "标普500",
    "^VIX": "恐慌指数",
    "^GSPTSE": "富时加拿大",
    "^BVSP": "巴西IBOVESPA",
    "^MXX": "墨西哥IPC",
    "^MERV": "阿根廷MERVAL",
    "^N100": "泛欧100",
    "^FTSE": "英国富时100",
    "^FTAI": "英国富时AIM全股",
    "^FCHI": "法国CAC40",
    "^GDAXI": "德国DAX",
    "^SSMI": "瑞士SMI",
    "FTSEMIB.MI": "意大利MIB",
    "^AEX": "荷兰AEX",
    "^BFX": "比利时BEL20",
    "^STOXX50E": "欧元区STOXX50",
    "^N225": "日经225",
    "^BSESN": "孟买SENSEX",
    "^SET.BK": "泰国SET",
    "^KS11": "韩国KOSPI",
    "^STI": "新加坡STI",
    "^JKSE": "印尼综合指数",
    "^KLSE": "马来西亚KLCI",
    "^AORD": "澳大利亚综合指数",
    "^NZ50": "新西兰50",
    "^TA125.TA": "以色列TA-125",
    "DFMGI.AE": "迪拜DFM",
    "^TASI.SR": "沙特TASI",
    "^J203.JO": "南非全股",
}


def symbol_key(symbol: str) -> str:
    key = re.sub(r"[^0-9A-Za-z]+", "_", symbol).strip("_")
    return key or "symbol"


def symbol_parquet_dir(parquet_dir: str, symbol: str) -> str:
    return os.path.join(parquet_dir, symbol_key(symbol))


def symbol_filename_template(symbol: str) -> str:
    return FNAME_TPL.format(symbol_key=symbol_key(symbol), yyyymm="{yyyymm}")


def create_retry_session(total_retries=3, backoff_factor=0.3, status_forcelist=(429, 500, 502, 503, 504)):
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": "https://finance.yahoo.com/",
    })
    return session


def date_to_timestamp(value: date) -> int:
    dt = datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)
    return int(dt.timestamp())


def build_url(symbol: str, start_dt: date, end_dt: date) -> tuple[str, dict]:
    params = {
        "events": "capitalGain|div|split",
        "formatted": "true",
        "includeAdjustedClose": "true",
        "interval": "1d",
        "period1": str(date_to_timestamp(start_dt - timedelta(days=10))),
        "period2": str(date_to_timestamp(end_dt + timedelta(days=1))),
        "symbol": symbol,
        "userYfid": "true",
        "lang": "en-US",
        "region": "US",
    }
    return f"{BASE_URL}/{quote(symbol, safe='')}", params


def request_url(symbol: str, start_dt: date, end_dt: date) -> str:
    url, params = build_url(symbol, start_dt, end_dt)
    return requests.Request("GET", url, params=params).prepare().url


def discover_latest_by_symbol(parquet_dir: str = PARQUET_DIR) -> dict[str, date]:
    if not os.path.isdir(parquet_dir):
        return {}

    latest: dict[str, date] = {}
    for root, _, files in os.walk(parquet_dir):
        for name in files:
            if not name.endswith(".parquet"):
                continue
            path = os.path.join(root, name)
            try:
                df = pd.read_parquet(path, columns=["symbol", "dt"])
            except Exception:
                continue
            if df.empty:
                continue
            df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
            grouped = df.dropna(subset=["dt"]).groupby("symbol")["dt"].max()
            for symbol, max_dt in grouped.items():
                symbol = str(symbol)
                if symbol not in latest or max_dt > latest[symbol]:
                    latest[symbol] = max_dt
    return latest


def _series_value(values, idx):
    if not values or idx >= len(values):
        return None
    return values[idx]


def fetch_market(session: requests.Session, symbol: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    url, params = build_url(symbol, start_dt, end_dt)
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    chart = payload.get("chart") or {}
    errors = chart.get("error")
    if errors:
        raise RuntimeError(f"Yahoo chart error: {errors}")

    results = chart.get("result") or []
    if not results:
        return pd.DataFrame()

    result = results[0]
    timestamps = result.get("timestamp") or []
    if not timestamps:
        return pd.DataFrame()

    meta = result.get("meta") or {}
    gmtoff = (
        ((meta.get("currentTradingPeriod") or {}).get("regular") or {}).get("gmtoffset")
        or meta.get("gmtoffset")
        or 0
    )
    quote_data = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    adjclose_data = ((result.get("indicators") or {}).get("adjclose") or [{}])[0]

    rows = []
    for idx, ts in enumerate(timestamps):
        local_dt = datetime.fromtimestamp(int(ts) + int(gmtoff), tz=timezone.utc).replace(tzinfo=None)
        trade_date = local_dt.date()
        open_value = _series_value(quote_data.get("open"), idx)
        if open_value is None:
            continue
        if trade_date < start_dt or trade_date > end_dt:
            continue
        rows.append({
            "market_name": MARKETS.get(symbol, symbol),
            "symbol": symbol,
            "dt": trade_date,
            "datetime_local": local_dt,
            "open": open_value,
            "close": _series_value(quote_data.get("close"), idx),
            "high": _series_value(quote_data.get("high"), idx),
            "low": _series_value(quote_data.get("low"), idx),
            "volume": _series_value(quote_data.get("volume"), idx),
            "adj_close": _series_value(adjclose_data.get("adjclose"), idx),
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    for col in ["open", "close", "high", "low", "adj_close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    return (
        df.dropna(subset=["dt"])
        .drop_duplicates(subset=["symbol", "dt"], keep="last")
        .sort_values(["symbol", "dt"])
        .reset_index(drop=True)
    )


def fetch_market_with_retry(session: requests.Session, symbol: str, start_dt: date, end_dt: date) -> pd.DataFrame | None:
    url = request_url(symbol, start_dt, end_dt)
    for attempt in range(1, RETRIES + 1):
        try:
            return fetch_market(session, symbol, start_dt, end_dt)
        except Exception as exc:
            print(f"[{symbol}] request failed attempt {attempt}/{RETRIES}: {exc}")
            print(f"[{symbol}] url: {url}")
            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
    return None


def run(parquet_dir: str = PARQUET_DIR):
    end_dt = date.today()
    initial_dt = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    latest = discover_latest_by_symbol(parquet_dir)
    session = create_retry_session()

    total_written = 0
    for symbol in MARKETS:
        start_dt = initial_dt if FULL_REFRESH else ((latest[symbol] + timedelta(days=1)) if symbol in latest else initial_dt)
        if start_dt > end_dt:
            print(f"[{symbol}] already up to {latest[symbol]}, skip.")
            continue

        mode = "full refresh" if FULL_REFRESH else "incremental"
        print(f"[{symbol}] mode={mode}; latest={latest.get(symbol)}; fetch {start_dt} -> {end_dt}")
        df = fetch_market_with_retry(session, symbol, start_dt, end_dt)
        if df is None:
            print(f"[{symbol}] failed after retries; skip. Next run will retry from {start_dt}.")
        elif df.empty:
            print(f"[{symbol}] no new rows.")
        else:
            written = upsert_monthly_parquet(
                df,
                parquet_dir=symbol_parquet_dir(parquet_dir, symbol),
                filename_template=symbol_filename_template(symbol),
                key_cols=["symbol", "dt"],
                sort_cols=["symbol", "dt"],
            )
            total_written += written
            print(f"[{symbol}] parquet upserted {written} rows.")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print(f"All done. Total parquet upserted {total_written} rows.")


if __name__ == "__main__":
    run()
