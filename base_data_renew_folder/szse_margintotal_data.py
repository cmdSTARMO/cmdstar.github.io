# SZSE margin trading totals collector.
# Writes directly to monthly Parquet files under api/data/margin_szse_tab1_data.

import random
import os
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
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
REPO_ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "margin_szse_tab1_data")
FNAME_TPL = "szse_tab1_{yyyymm}.parquet"
LEGACY_SQLITE = str(REPO_ROOT / "api" / "data" / "szse_tab1.sqlite")

FIELD_MAP = {
    "jrrzye": "margin_balance",
    "jrrzmr": "margin_buy_amt",
    "jrrjyl": "short_qty",
    "jrrjye": "short_value",
    "jrrjmc": "short_sell_qty",
    "jrrzrjye": "marginnshort_total",
}

HEADERS = {}


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


def fetch_one_day(session: requests.Session, cur_date: date):
    ds = cur_date.strftime("%Y-%m-%d")
    url = (
        "https://www.szse.cn/api/report/ShowReport/data"
        "?SHOWTYPE=JSON&CATALOGID=1837_xxpl"
        f"&txtDate={ds}"
    )
    resp = session.get(url, headers=HEADERS, timeout=10)
    resp.raise_for_status()
    body = resp.json()
    tab1 = next((x for x in body if x["metadata"]["tabkey"] == "tab1"), None)
    if not tab1 or not tab1.get("data"):
        return None

    row = tab1["data"][0]
    vals = {
        FIELD_MAP[k]: float(str(v).replace(",", ""))
        for k, v in row.items()
        if k in FIELD_MAP and str(v).strip()
    }
    vals["dt"] = ds
    return vals


def write_rows(rows, parquet_dir: str = PARQUET_DIR):
    rows = [row for row in rows if row]
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    columns = [
        "dt",
        "margin_balance",
        "margin_buy_amt",
        "short_qty",
        "short_value",
        "short_sell_qty",
        "marginnshort_total",
    ]
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns]

    return upsert_monthly_parquet(
        df,
        parquet_dir=parquet_dir,
        filename_template=FNAME_TPL,
        key_cols=["dt"],
        sort_cols=["dt"],
    )


def migrate_legacy_sqlite_if_needed(parquet_dir: str = PARQUET_DIR, sqlite_path: str = LEGACY_SQLITE):
    if discover_latest_date(parquet_dir) or not os.path.isfile(sqlite_path):
        return

    conn = sqlite3.connect(sqlite_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT
              date AS dt,
              margin_balance,
              margin_buy_amt,
              short_qty,
              short_value,
              short_sell_qty,
              marginnshort_total
            FROM tab1_data
            ORDER BY date
            """,
            conn,
        )
    finally:
        conn.close()

    if df.empty:
        return

    written = upsert_monthly_parquet(
        df,
        parquet_dir=parquet_dir,
        filename_template=FNAME_TPL,
        key_cols=["dt"],
        sort_cols=["dt"],
    )
    print(f"Migrated legacy SQLite to Parquet: {written} rows.")


def fetch_tab1(end_date: date, parquet_dir: str = PARQUET_DIR, fail_streak=0):
    migrate_legacy_sqlite_if_needed(parquet_dir=parquet_dir)
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
        try:
            row = fetch_one_day(session, cur_date)
        except requests.exceptions.RequestException as exc:
            print(f"[{ds}] request failed: {exc}; retry later")
            time.sleep(5)
            fail_streak += 1
            if fail_streak == 5:
                sys.exit(1)
            continue

        if row is None:
            print(f"Checking {ds}: records found = 0")
        else:
            print(f"Checking {ds}: records found = 1")
            written = write_rows([row], parquet_dir=parquet_dir)
            print(f"[{ds}] parquet upserted {written} rows")

        cur_date += timedelta(days=1)

    print("All done.")


if __name__ == "__main__":
    end_date = datetime.strptime(date.today().strftime("%Y-%m-%d"), "%Y-%m-%d").date()
    fetch_tab1(end_date, PARQUET_DIR)
