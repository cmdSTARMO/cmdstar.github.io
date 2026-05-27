import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from parquet_incremental import upsert_monthly_parquet
except ImportError:
    from base_data_renew_folder.parquet_incremental import upsert_monthly_parquet


REPO_ROOT = Path(__file__).resolve().parents[2]
CAPITAL_FLOW_ROOT = str(REPO_ROOT / "api" / "data" / "capital_flow_data")
PARQUET_DIR = os.path.join(CAPITAL_FLOW_ROOT, "southbound")
NORTHBOUND_PLACEHOLDER_DIR = os.path.join(CAPITAL_FLOW_ROOT, "northbound")
FNAME_TPL = "capital_flow_southbound_{yyyymm}.parquet"
INITIAL_START = "1999-12-30"
PAGE_SIZE = 500
MAX_PAGES = int(os.getenv("SOUTHBOUND_NETBUY_MAX_PAGES", "300"))
REQUEST_SLEEP_SECONDS = float(os.getenv("SOUTHBOUND_NETBUY_SLEEP", "0.5"))
RETRIES = int(os.getenv("SOUTHBOUND_NETBUY_RETRIES", "5"))
RETRY_SLEEP_SECONDS = float(os.getenv("SOUTHBOUND_NETBUY_RETRY_SLEEP", "30"))
FULL_REFRESH = os.getenv("SOUTHBOUND_NETBUY_FULL_REFRESH", "0") == "1"

BASE_URL = "https://datacenter-web.eastmoney.com/web/api/data/v1/get"


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
        "Referer": "https://data.eastmoney.com/",
    })
    return session


def discover_latest_date(parquet_dir: str = PARQUET_DIR) -> date | None:
    if not os.path.isdir(parquet_dir):
        return None

    latest = None
    for name in os.listdir(parquet_dir):
        if not name.endswith(".parquet"):
            continue
        path = os.path.join(parquet_dir, name)
        try:
            df = pd.read_parquet(path, columns=["dt"])
        except Exception:
            continue
        if df.empty:
            continue
        current = pd.to_datetime(df["dt"], errors="coerce").max()
        if pd.isna(current):
            continue
        current_date = current.date()
        if latest is None or current_date > latest:
            latest = current_date
    return latest


def build_params(page_number: int, query_start: date) -> dict:
    return {
        "reportName": "RPT_SOUTH_ACCUM_NETBUY",
        "columns": "TRADE_DATE,ACCUM_NETBUY_H,ACCUM_NETBUY_S,ACCUM_NETBUY_AMT,INDEX_PRICE,DATE_TYPE_CODE",
        "filter": f'(DATE_TYPE_CODE="001")(TRADE_DATE>=\'{query_start.isoformat()}\')',
        "pageNumber": str(page_number),
        "pageSize": str(PAGE_SIZE),
        "sortColumns": "TRADE_DATE",
        "sortTypes": "1",
        "source": "WEB",
        "client": "WEB",
    }


def request_url(params: dict) -> str:
    return requests.Request("GET", BASE_URL, params=params).prepare().url


def fetch_page(session: requests.Session, page_number: int, query_start: date) -> tuple[list[dict], int | None]:
    params = build_params(page_number, query_start)
    resp = session.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    result = payload.get("result") or {}
    return result.get("data") or [], result.get("pages")


def fetch_accum_rows(session: requests.Session, query_start: date) -> pd.DataFrame:
    all_rows: list[dict] = []
    for page in range(1, MAX_PAGES + 1):
        params = build_params(page, query_start)
        for attempt in range(1, RETRIES + 1):
            try:
                rows, pages = fetch_page(session, page, query_start)
                break
            except requests.exceptions.RequestException as exc:
                print(f"[southbound] page {page} failed attempt {attempt}/{RETRIES}: {exc}")
                print(f"[southbound] url: {request_url(params)}")
                if attempt >= RETRIES:
                    raise
                time.sleep(RETRY_SLEEP_SECONDS * attempt)

        print(f"[southbound] page={page}; rows={len(rows)}; pages={pages}")
        if not rows:
            break
        all_rows.extend(rows)
        if pages and page >= int(pages):
            break
        time.sleep(REQUEST_SLEEP_SECONDS)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(all_rows)
    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce").dt.date
    for col in ["ACCUM_NETBUY_H", "ACCUM_NETBUY_S", "ACCUM_NETBUY_AMT", "INDEX_PRICE"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return (
        df.dropna(subset=["TRADE_DATE"])
        .drop_duplicates(subset=["TRADE_DATE"], keep="last")
        .sort_values("TRADE_DATE")
        .reset_index(drop=True)
    )


def to_southbound_dataset(accum_df: pd.DataFrame, start_dt: date, end_dt: date) -> pd.DataFrame:
    if accum_df.empty:
        return pd.DataFrame()

    df = accum_df.copy()
    df["southbound_hk_sh_daily_netbuy_100m_yuan"] = df["ACCUM_NETBUY_H"].diff()
    df["southbound_hk_sz_daily_netbuy_100m_yuan"] = df["ACCUM_NETBUY_S"].diff()
    df["southbound_daily_netbuy_100m_yuan"] = df["ACCUM_NETBUY_AMT"].diff()
    df = df[(df["TRADE_DATE"] >= start_dt) & (df["TRADE_DATE"] <= end_dt)]
    df = df.dropna(subset=[
        "southbound_daily_netbuy_100m_yuan",
        "southbound_hk_sh_daily_netbuy_100m_yuan",
        "southbound_hk_sz_daily_netbuy_100m_yuan",
    ])
    if df.empty:
        return pd.DataFrame()
    return df.rename(columns={
        "TRADE_DATE": "dt",
        "ACCUM_NETBUY_H": "southbound_hk_sh_accum_netbuy_100m_yuan",
        "ACCUM_NETBUY_S": "southbound_hk_sz_accum_netbuy_100m_yuan",
        "ACCUM_NETBUY_AMT": "southbound_accum_netbuy_100m_yuan",
        "INDEX_PRICE": "hang_seng_close",
        "DATE_TYPE_CODE": "date_type_code",
    })[[
        "dt",
        "southbound_accum_netbuy_100m_yuan",
        "southbound_hk_sh_accum_netbuy_100m_yuan",
        "southbound_hk_sz_accum_netbuy_100m_yuan",
        "hang_seng_close",
        "date_type_code",
        "southbound_daily_netbuy_100m_yuan",
        "southbound_hk_sh_daily_netbuy_100m_yuan",
        "southbound_hk_sz_daily_netbuy_100m_yuan",
    ]].sort_values("dt").reset_index(drop=True)


def run(parquet_dir: str = PARQUET_DIR):
    os.makedirs(NORTHBOUND_PLACEHOLDER_DIR, exist_ok=True)

    end_dt = date.today()
    initial_dt = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    latest = discover_latest_date(parquet_dir)
    start_dt = initial_dt if FULL_REFRESH or latest is None else latest + timedelta(days=1)
    if start_dt > end_dt:
        print(f"[southbound] already up to {latest}, skip.")
        # GitHub Actions has occasionally reported a native-library abort during
        # interpreter teardown after reading Parquet only. Flush and exit directly
        # on this no-op path so the scheduler records the run as successful.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(0)

    query_start = max(initial_dt, start_dt - timedelta(days=10))
    mode = "full refresh" if FULL_REFRESH else "incremental"
    print(f"[southbound] mode={mode}; latest={latest}; fetch {start_dt} -> {end_dt}; query_start={query_start}")

    session = create_retry_session()
    accum_df = fetch_accum_rows(session, query_start)
    out_df = to_southbound_dataset(accum_df, start_dt, end_dt)
    if out_df.empty:
        print("[southbound] no new rows.")
        return

    written = upsert_monthly_parquet(
        out_df,
        parquet_dir=parquet_dir,
        filename_template=FNAME_TPL,
        key_cols=["dt"],
        sort_cols=["dt"],
    )
    print(f"[southbound] parquet upserted {written} rows.")


if __name__ == "__main__":
    run()
