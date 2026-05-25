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


REPO_ROOT = Path(__file__).resolve().parents[1]
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "index_daily_data")
FNAME_TPL = "index_daily_{yyyymm}.parquet"
INITIAL_START = "2010-05-04"
REQUEST_SLEEP_SECONDS = 20
FULL_REFRESH = os.getenv("INDEX_DAILY_FULL_REFRESH", "0") == "1"
INDEX_RETRIES = int(os.getenv("INDEX_DAILY_RETRIES", "5"))
RETRY_SLEEP_SECONDS = float(os.getenv("INDEX_DAILY_RETRY_SLEEP", "60"))
BASE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"

INDEXES = {
    "1.000001": "上证指数",
    "0.399001": "深证成指",
    "0.399006": "创业板指",
    "1.000016": "上证50",
    "1.000300": "沪深300",
    "1.000905": "中证500",
    "1.000852": "中证1000",
    "0.399372": "大盘成长",
    "0.399373": "大盘价值",
    "0.399374": "中盘成长",
    "0.399375": "中盘价值",
    "0.399376": "小盘成长",
    "0.399377": "小盘价值",
}


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
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh-Hans;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Referer": "https://quote.eastmoney.com/",
    })
    return session


def discover_latest_by_secid(parquet_dir: str = PARQUET_DIR) -> dict[str, date]:
    if not os.path.isdir(parquet_dir):
        return {}

    latest: dict[str, date] = {}
    for name in os.listdir(parquet_dir):
        if not name.endswith(".parquet"):
            continue
        path = os.path.join(parquet_dir, name)
        try:
            df = pd.read_parquet(path, columns=["secid", "dt"])
        except Exception:
            continue
        if df.empty:
            continue
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
        grouped = df.dropna(subset=["dt"]).groupby("secid")["dt"].max()
        for secid, max_dt in grouped.items():
            if secid not in latest or max_dt > latest[secid]:
                latest[secid] = max_dt
    return latest


def build_params(secid: str, start_dt: date, end_dt: date) -> dict:
    return {
        "cb": "",
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": start_dt.strftime("%Y%m%d"),
        "end": end_dt.strftime("%Y%m%d"),
        "lmt": "1000000",
    }


def request_url(session: requests.Session, params: dict) -> str:
    return requests.Request("GET", BASE_URL, params=params).prepare().url


def fetch_klines(session: requests.Session, secid: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    params = build_params(secid, start_dt, end_dt)
    resp = session.get(BASE_URL, params=params, timeout=20)
    resp.raise_for_status()
    payload = resp.json()
    klines = (payload.get("data") or {}).get("klines") or []
    if not klines:
        return pd.DataFrame()

    rows = [item.split(",") for item in klines]
    df = pd.DataFrame(rows, columns=[
        "dt",
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "pct_chg",
        "change",
        "turnover_rate",
    ])
    df.insert(0, "secid", secid)
    df.insert(0, "index_name", INDEXES.get(secid, secid))
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    for col in ["open", "close", "high", "low", "amount", "amplitude", "pct_chg", "change", "turnover_rate"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    return df.dropna(subset=["dt"]).sort_values(["secid", "dt"]).reset_index(drop=True)


def fetch_klines_with_retry(session: requests.Session, secid: str, start_dt: date, end_dt: date) -> pd.DataFrame | None:
    params = build_params(secid, start_dt, end_dt)
    url = request_url(session, params)
    for attempt in range(1, INDEX_RETRIES + 1):
        try:
            return fetch_klines(session, secid, start_dt, end_dt)
        except requests.exceptions.RequestException as exc:
            print(f"[{secid}] request failed attempt {attempt}/{INDEX_RETRIES}: {exc}")
            print(f"[{secid}] url: {url}")
            if attempt < INDEX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
    return None


def run(parquet_dir: str = PARQUET_DIR):
    end_dt = date.today()
    initial_dt = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    latest = discover_latest_by_secid(parquet_dir)
    session = create_retry_session()

    total_written = 0
    for secid in INDEXES:
        start_dt = initial_dt if FULL_REFRESH else ((latest[secid] + timedelta(days=1)) if secid in latest else initial_dt)
        if start_dt > end_dt:
            print(f"[{secid}] already up to {latest[secid]}, skip.")
            continue

        mode = "full refresh" if FULL_REFRESH else "incremental"
        print(f"[{secid}] mode={mode}; latest={latest.get(secid)}; fetch {start_dt} -> {end_dt}")
        df = fetch_klines_with_retry(session, secid, start_dt, end_dt)
        if df is None:
            print(f"[{secid}] failed after retries; skip. Next run will retry from {start_dt}.")
            time.sleep(REQUEST_SLEEP_SECONDS)
            continue
        if df.empty:
            print(f"[{secid}] no new rows.")
        else:
            written = upsert_monthly_parquet(
                df,
                parquet_dir=parquet_dir,
                filename_template=FNAME_TPL,
                key_cols=["secid", "dt"],
                sort_cols=["secid", "dt"],
            )
            total_written += written
            print(f"[{secid}] parquet upserted {written} rows.")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print(f"All done. Total parquet upserted {total_written} rows.")


if __name__ == "__main__":
    run()
