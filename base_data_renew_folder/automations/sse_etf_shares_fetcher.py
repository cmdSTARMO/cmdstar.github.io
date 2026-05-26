import json
import os
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


REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "sse_etf_shares_data")
FNAME_TPL = "sse_etf_shares_{yyyymm}.parquet"
INITIAL_START = os.getenv("SSE_ETF_SHARES_INITIAL_START", "2011-10-30")
REQUEST_SLEEP_SECONDS = float(os.getenv("SSE_ETF_SHARES_SLEEP", "0.3"))
RETRIES = int(os.getenv("SSE_ETF_SHARES_RETRIES", "5"))
RETRY_SLEEP_SECONDS = float(os.getenv("SSE_ETF_SHARES_RETRY_SLEEP", "10"))
FULL_REFRESH = os.getenv("SSE_ETF_SHARES_FULL_REFRESH", "0") == "1"

BASE_URL = "http://query.sse.com.cn/commonQuery.do"


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
        "Referer": "http://www.sse.com.cn/",
    })
    return session


def build_params(target_date: date) -> dict:
    return {
        "jsonCallBack": "cb",
        "isPagination": "true",
        "pageHelp.pageSize": "9999",
        "pageHelp.pageNo": "1",
        "pageHelp.beginPage": "1",
        "pageHelp.endPage": "1",
        "pageHelp.cacheSize": "1",
        "sqlId": "COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L",
        "STAT_DATE": target_date.isoformat(),
        "_": datetime.now().strftime("%Y%m%d%H%M%S"),
    }


def request_url(params: dict) -> str:
    return requests.Request("GET", BASE_URL, params=params).prepare().url


def parse_jsonp(text: str) -> dict:
    text = text.strip()
    if text.startswith("cb(") and text.endswith(")"):
        text = text[3:-1]
    return json.loads(text)


def fetch_one_day(session: requests.Session, target_date: date) -> pd.DataFrame:
    params = build_params(target_date)
    resp = session.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = parse_jsonp(resp.text)
    rows = ((payload.get("pageHelp") or {}).get("data")) or []
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(rows)
    source_cols = ["STAT_DATE", "ETF_TYPE", "SEC_CODE", "NUM", "SEC_NAME", "TOT_VOL"]
    for col in source_cols:
        if col not in df.columns:
            df[col] = None
    df = df[source_cols].rename(columns={
        "STAT_DATE": "dt",
        "ETF_TYPE": "etf_type",
        "SEC_CODE": "sec_code",
        "NUM": "quantity",
        "SEC_NAME": "etf_name",
        "TOT_VOL": "total_volume_10k_shares",
    })
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
    df["total_volume_10k_shares"] = pd.to_numeric(df["total_volume_10k_shares"], errors="coerce")
    return (
        df.dropna(subset=["dt", "sec_code"])
        .drop_duplicates(subset=["dt", "sec_code", "etf_type"], keep="last")
        .sort_values(["dt", "sec_code", "etf_type"])
        .reset_index(drop=True)
    )


def fetch_one_day_with_retry(session: requests.Session, target_date: date) -> pd.DataFrame | None:
    params = build_params(target_date)
    url = request_url(params)
    for attempt in range(1, RETRIES + 1):
        try:
            return fetch_one_day(session, target_date)
        except requests.exceptions.RequestException as exc:
            print(f"[{target_date}] request failed attempt {attempt}/{RETRIES}: {exc}")
            print(f"[{target_date}] url: {url}")
            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
    return None


def run(parquet_dir: str = PARQUET_DIR):
    end_dt = date.today()
    initial_dt = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    latest = discover_latest_date(parquet_dir)
    start_dt = initial_dt if FULL_REFRESH or latest is None else latest + timedelta(days=1)
    if start_dt > end_dt:
        print(f"[sse_etf_shares] already up to {latest}, skip.")
        return

    mode = "full refresh" if FULL_REFRESH else "incremental"
    print(f"[sse_etf_shares] mode={mode}; latest={latest}; fetch {start_dt} -> {end_dt}")
    session = create_retry_session()

    total_written = 0
    current = start_dt
    while current <= end_dt:
        df = fetch_one_day_with_retry(session, current)
        if df is None:
            print(f"[{current}] failed after retries; skip. Next run will retry from current latest date.")
        elif df.empty:
            print(f"[{current}] no rows.")
        else:
            written = upsert_monthly_parquet(
                df,
                parquet_dir=parquet_dir,
                filename_template=FNAME_TPL,
                key_cols=["dt", "sec_code", "etf_type"],
                sort_cols=["dt", "sec_code", "etf_type"],
            )
            total_written += written
            print(f"[{current}] parquet upserted {written} rows.")
        current += timedelta(days=1)
        time.sleep(REQUEST_SLEEP_SECONDS)

    print(f"[sse_etf_shares] all done. Total parquet upserted {total_written} rows.")


if __name__ == "__main__":
    run()
