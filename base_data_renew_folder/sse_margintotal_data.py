# SSE margin trading totals collector.
# Writes directly to monthly Parquet files under api/data/margin_sse_tab1_data.

import json
import os
import random
import re
import sqlite3
import time
from datetime import date, datetime, timedelta
from urllib.parse import urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from parquet_incremental import discover_latest_date, upsert_monthly_parquet
except ImportError:
    from base_data_renew_folder.parquet_incremental import discover_latest_date, upsert_monthly_parquet


INITIAL_START = "2010-03-31"
PARQUET_DIR = "../api/data/margin_sse_tab1_data"
FNAME_TPL = "sse_tab1_{yyyymm}.parquet"
LEGACY_SQLITE = "../api/data/sse_tab1.sqlite"
WINDOW_DAYS = 1000
SLEEP_BASE, SLEEP_JITTER = 25, 10

HEADERS = {
    "Referer": "https://www.sse.com.cn/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Connection": "keep-alive",
}

FIELD_MAP = {
    "rzye": "margin_balance",
    "rzmre": "margin_buy_amt",
    "rzche": "margin_repay_amt",
    "rqyl": "short_qty",
    "rqmcl": "short_sell_qty",
    "rqylje": "short_value",
    "rzrqjyzl": "marginnshort_total",
    "opDate": "dt",
}


def create_retry_session(total_retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504, 520, 521, 522)):
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


def parse_jsonp(text: str):
    match = re.search(r"^\s*[\w$]+\((.*)\)\s*$", text, flags=re.S)
    return json.loads(match.group(1) if match else text)


def ymd_compact(d: date) -> str:
    return d.strftime("%Y%m%d")


def ymd_dash(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def build_url(begin_ymd: str, end_ymd: str, page_no: int, page_size: int = 5000) -> str:
    base = "https://query.sse.com.cn/commonSoaQuery.do"
    q = {
        "jsonCallBack": f"jsonpCallback{random.randint(10000000, 99999999)}",
        "isPagination": "true",
        "pageHelp.pageSize": str(page_size),
        "pageHelp.pageNo": str(page_no),
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": str(page_no),
        "stockCode": "",
        "beginDate": begin_ymd,
        "endDate": end_ymd,
        "sqlId": "RZRQ_HZ_INFO",
        "_": str(int(time.time() * 1000)),
    }
    return f"{base}?{urlencode(q)}"


def fetch_window(session: requests.Session, begin_d: date, end_d: date):
    begin_ymd = ymd_compact(begin_d)
    end_ymd = ymd_compact(end_d)
    all_rows = []
    page_no = 1

    while True:
        url = build_url(begin_ymd, end_ymd, page_no=page_no)
        resp = session.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        payload = parse_jsonp(resp.text)

        page = payload.get("pageHelp", {})
        rows = page.get("data") or payload.get("result") or []
        total = page.get("total") or len(rows) or 0
        page_size = page.get("pageSize") or 25

        for row in rows:
            out = {FIELD_MAP[k]: v for k, v in row.items() if k in FIELD_MAP}
            if "dt" in out:
                raw = str(out["dt"])
                out["dt"] = f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"
            all_rows.append(out)

        if page_no * page_size >= total or not rows:
            break
        page_no += 1
        time.sleep(0.3)

    uniq = {}
    for row in all_rows:
        if row.get("dt"):
            uniq[row["dt"]] = row

    result = list(uniq.values())
    result.sort(key=lambda row: row["dt"])
    return result


def write_rows(rows, parquet_dir: str = PARQUET_DIR):
    if not rows:
        return 0
    df = pd.DataFrame(rows)
    columns = [
        "dt",
        "margin_balance",
        "margin_buy_amt",
        "margin_repay_amt",
        "short_qty",
        "short_sell_qty",
        "short_value",
        "marginnshort_total",
    ]
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns]
    for col in columns[1:]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

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
              margin_repay_amt,
              short_qty,
              short_sell_qty,
              short_value,
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


def fetch_totals(end_date_str: str, parquet_dir: str = PARQUET_DIR):
    migrate_legacy_sqlite_if_needed(parquet_dir=parquet_dir)
    latest = discover_latest_date(parquet_dir)
    start_date = latest + timedelta(days=1) if latest else datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    if start_date > end_date:
        print(f"Parquet already contains data through {latest}; no update needed.")
        return

    session = create_retry_session()
    cur_start = start_date
    while cur_start <= end_date:
        cur_end = min(cur_start + timedelta(days=WINDOW_DAYS - 1), end_date)
        print(f"[WINDOW] {ymd_dash(cur_start)} -> {ymd_dash(cur_end)} fetching...")
        try:
            rows = fetch_window(session, cur_start, cur_end)
            written = write_rows(rows, parquet_dir=parquet_dir)
            print(f"  done: fetched {len(rows)} rows, parquet upserted {written} rows.")
        except requests.exceptions.RequestException as exc:
            print(f"  request error: {exc}; retrying this window")
            time.sleep(5)
            continue

        time.sleep(SLEEP_BASE + SLEEP_JITTER * random.random())
        cur_start = cur_end + timedelta(days=1)

    print("All done.")


if __name__ == "__main__":
    fetch_totals(end_date_str=date.today().strftime("%Y-%m-%d"), parquet_dir=PARQUET_DIR)
