# SZSE margin trading details collector.
# Writes directly to monthly Parquet files under api/data/margin_szse_tab2_data.

import random
import sys
import time
from datetime import date, datetime, timedelta
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from parquet_incremental import discover_latest_date, upsert_monthly_parquet
except ImportError:
    from base_data_renew_folder.parquet_incremental import discover_latest_date, upsert_monthly_parquet


INITIAL_START = "2010-05-04"
REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "margin_szse_tab2_data")
FNAME_TPL = "szse_tab2_{yyyymm}.parquet"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
    "Referer": "https://www.szse.cn/disclosure/margin/margin/index.html",
}

COLUMN_ALIASES = {
    "code": ["证券代码"],
    "name": ["证券简称"],
    "margin_buy_amt": ["融资买入额(元)"],
    "margin_balance": ["融资余额(元)"],
    "short_sell_qty": ["融券卖出量(股/份)"],
    "short_qty": ["融券余量(股/份)"],
    "short_value": ["融券余额(元)", "融券余量金额(元)"],
    "marginnshort_total": ["融资融券余额(元)"],
}

OUT_COLS = [
    "dt",
    "code",
    "name",
    "margin_buy_amt",
    "margin_balance",
    "short_sell_qty",
    "short_qty",
    "short_value",
    "marginnshort_total",
]


def create_retry_session(total_retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
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
    return session


def _to_float(value):
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if text == "" or text.lower() in ("nan", "none"):
        return None
    try:
        return float(text)
    except Exception:
        return None


def build_url(cur_date: date) -> str:
    ds = cur_date.strftime("%Y-%m-%d")
    return (
        "https://www.szse.cn/api/report/ShowReport"
        f"?SHOWTYPE=xlsx&CATALOGID=1837_xxpl&txtDate={ds}"
        "&tab2PAGENO=1&TABKEY=tab2"
    )


def fetch_one_day(session: requests.Session, cur_date: date):
    ds = cur_date.strftime("%Y-%m-%d")
    url = build_url(cur_date)
    resp = session.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    if not resp.content:
        return None

    df = pd.read_excel(BytesIO(resp.content), dtype=str)
    if df is None or df.empty:
        return None
    df.columns = [str(c).strip() for c in df.columns]

    selected = {}
    missing = []
    for target, aliases in COLUMN_ALIASES.items():
        source = next((name for name in aliases if name in df.columns), None)
        if source is None:
            missing.append("/".join(aliases))
        else:
            selected[target] = df[source]
    if missing:
        print(f"[{ds}] missing columns: {missing}; skip")
        print(f"[{ds}] url: {url}")
        print(f"[{ds}] available columns: {list(df.columns)}")
        return None

    df = pd.DataFrame(selected)
    df["dt"] = ds
    for col in OUT_COLS:
        if col not in df.columns:
            df[col] = None
    for col in [
        "margin_buy_amt",
        "margin_balance",
        "short_sell_qty",
        "short_qty",
        "short_value",
        "marginnshort_total",
    ]:
        df[col] = df[col].map(_to_float)
    df["code"] = df["code"].astype(str).str.strip()
    return df[OUT_COLS][df["code"].ne("")]


def fetch_tab2(end_date: date, parquet_dir: str = PARQUET_DIR, fail_streak=0):
    latest = discover_latest_date(parquet_dir)
    init_start = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    start_date = max(latest + timedelta(days=1), init_start) if latest else init_start

    if start_date > end_date:
        print(f"Parquet already contains data through {latest}; no update needed.")
        return
    print(f"Parquet dir: {parquet_dir}")
    print(f"Latest stored date: {latest}; start fetching from {start_date}")

    session = create_retry_session()
    cur_date = start_date

    while cur_date <= end_date:
        time.sleep(25 + 10 * random.random())
        ds = cur_date.strftime("%Y-%m-%d")
        url = build_url(cur_date)
        try:
            df = fetch_one_day(session, cur_date)
        except requests.exceptions.RequestException as exc:
            print(f"[{ds}] request failed: {exc}; retry later")
            print(f"[{ds}] url: {url}")
            fail_streak += 1
            if fail_streak >= 5:
                print("Too many consecutive failures; exit.")
                sys.exit(1)
            time.sleep(5)
            continue
        except Exception as exc:
            print(f"[{ds}] parse failed: {exc}; skip")
            print(f"[{ds}] url: {url}")
            cur_date += timedelta(days=1)
            continue

        if df is None or df.empty:
            print(f"Checking {ds}: records found = 0")
        else:
            print(f"Checking {ds}: records found = {len(df)}")
            written = upsert_monthly_parquet(
                df,
                parquet_dir=parquet_dir,
                filename_template=FNAME_TPL,
                key_cols=["dt", "code"],
                sort_cols=["dt", "code"],
            )
            print(f"[{ds}] parquet upserted {written} rows")

        cur_date += timedelta(days=1)

    print("All done.")


if __name__ == "__main__":
    end_date = datetime.strptime(date.today().strftime("%Y-%m-%d"), "%Y-%m-%d").date()
    fetch_tab2(end_date, PARQUET_DIR)
