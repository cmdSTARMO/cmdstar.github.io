import os
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
PARQUET_DIR = os.path.join(CAPITAL_FLOW_ROOT, "northbound")
FNAME_TPL = "capital_flow_northbound_{yyyymm}.parquet"
INITIAL_START = "1999-12-30"
PAGE_SIZE = 500
MAX_PAGES = int(os.getenv("NORTHBOUND_DEALAMT_MAX_PAGES", "300"))
REQUEST_SLEEP_SECONDS = float(os.getenv("NORTHBOUND_DEALAMT_SLEEP", "0.5"))
RETRIES = int(os.getenv("NORTHBOUND_DEALAMT_RETRIES", "5"))
RETRY_SLEEP_SECONDS = float(os.getenv("NORTHBOUND_DEALAMT_RETRY_SLEEP", "30"))
FULL_REFRESH = os.getenv("NORTHBOUND_DEALAMT_FULL_REFRESH", "0") == "1"

BASE_URL = "https://datacenter-web.eastmoney.com/web/api/data/v1/get"

COLUMN_RENAME = {
    "TRADE_DATE": "dt",
    "CSI300_INDEX_PRICE": "csi300_index_price",
    "SCI_INDEX_PRICE": "sse_index_price",
    "SZC_INDEX_PRICE": "chinext_index_price",
    "CSI300_INDEX_RATE": "csi300_index_pct_chg",
    "SCI_INDEX_RATE": "sse_index_pct_chg",
    "SZC_INDEX_RATE": "chinext_index_pct_chg",
    "NF_DEAL_AMT": "northbound_deal_amt_million_yuan",
    "NF_QUOTA_BALANCE": "northbound_quota_balance_million_yuan",
    "SSC_DEAL_AMT": "shanghai_connect_deal_amt_million_yuan",
    "SSC_QUOTA_BALANCE": "shanghai_connect_quota_balance_million_yuan",
    "ST_DEAL_AMT": "shenzhen_connect_deal_amt_million_yuan",
    "SSC_LEAD_STOCKS": "shanghai_connect_lead_stock_name",
    "SSC_LEAD_STOCKSCODE": "shanghai_connect_lead_stock_code",
    "ST_LEAD_STOCKS": "shenzhen_connect_lead_stock_name",
    "ST_LEAD_STOCKSCODE": "shenzhen_connect_lead_stock_code",
    "SSC_CHANGE_RATE": "shanghai_connect_lead_stock_pct_chg",
    "ST_CHANGE_RATE": "shenzhen_connect_lead_stock_pct_chg",
    "NF_LEAD_STOCKS": "northbound_lead_stock_name",
    "NF_CHANGE_RATE": "northbound_lead_stock_pct_chg",
    "NF_LEAD_STOCKSCODE": "northbound_lead_stock_code",
    "SSC_DEAL_NUM": "shanghai_connect_deal_count",
    "ST_DEAL_NUM": "shenzhen_connect_deal_count",
}

OUTPUT_COLUMNS = list(COLUMN_RENAME.values())

NUMERIC_COLUMNS = [
    "csi300_index_price",
    "sse_index_price",
    "chinext_index_price",
    "csi300_index_pct_chg",
    "sse_index_pct_chg",
    "chinext_index_pct_chg",
    "northbound_deal_amt_million_yuan",
    "northbound_quota_balance_million_yuan",
    "shanghai_connect_deal_amt_million_yuan",
    "shanghai_connect_quota_balance_million_yuan",
    "shenzhen_connect_deal_amt_million_yuan",
    "shanghai_connect_lead_stock_pct_chg",
    "shenzhen_connect_lead_stock_pct_chg",
    "northbound_lead_stock_pct_chg",
    "shanghai_connect_deal_count",
    "shenzhen_connect_deal_count",
]


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
        "reportName": "RPT_MUTUAL_DEALAMT",
        "columns": "ALL",
        "filter": f"(TRADE_DATE>='{query_start.isoformat()}')",
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


def fetch_rows(session: requests.Session, query_start: date) -> pd.DataFrame:
    all_rows: list[dict] = []
    for page in range(1, MAX_PAGES + 1):
        params = build_params(page, query_start)
        for attempt in range(1, RETRIES + 1):
            try:
                rows, pages = fetch_page(session, page, query_start)
                break
            except requests.exceptions.RequestException as exc:
                print(f"[northbound] page {page} failed attempt {attempt}/{RETRIES}: {exc}")
                print(f"[northbound] url: {request_url(params)}")
                if attempt >= RETRIES:
                    raise
                time.sleep(RETRY_SLEEP_SECONDS * attempt)

        print(f"[northbound] page={page}; rows={len(rows)}; pages={pages}")
        if not rows:
            break
        all_rows.extend(rows)
        if pages and page >= int(pages):
            break
        time.sleep(REQUEST_SLEEP_SECONDS)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(all_rows)
    for source_col in COLUMN_RENAME:
        if source_col not in df.columns:
            df[source_col] = None
    df = df[list(COLUMN_RENAME)].rename(columns=COLUMN_RENAME)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return (
        df.dropna(subset=["dt"])
        .drop_duplicates(subset=["dt"], keep="last")
        .sort_values("dt")
        .reset_index(drop=True)
    )


def run(parquet_dir: str = PARQUET_DIR):
    end_dt = date.today()
    initial_dt = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    latest = discover_latest_date(parquet_dir)
    start_dt = initial_dt if FULL_REFRESH or latest is None else latest + timedelta(days=1)
    if start_dt > end_dt:
        print(f"[northbound] already up to {latest}, skip.")
        return

    query_start = max(initial_dt, start_dt - timedelta(days=10))
    mode = "full refresh" if FULL_REFRESH else "incremental"
    print(f"[northbound] mode={mode}; latest={latest}; fetch {start_dt} -> {end_dt}; query_start={query_start}")

    session = create_retry_session()
    df = fetch_rows(session, query_start)
    df = df[(df["dt"] >= start_dt) & (df["dt"] <= end_dt)]
    if df.empty:
        print("[northbound] no new rows.")
        return

    written = upsert_monthly_parquet(
        df,
        parquet_dir=parquet_dir,
        filename_template=FNAME_TPL,
        key_cols=["dt"],
        sort_cols=["dt"],
    )
    print(f"[northbound] parquet upserted {written} rows.")


if __name__ == "__main__":
    run()
