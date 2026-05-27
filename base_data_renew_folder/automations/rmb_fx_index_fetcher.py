import os
import ssl
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
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "rmb_fx_index_data")
FNAME_TPL = "rmb_fx_index_{yyyymm}.parquet"
INITIAL_START = os.getenv("RMB_FX_INDEX_INITIAL_START", "2015-11-30")
CHUNK_DAYS = int(os.getenv("RMB_FX_INDEX_CHUNK_DAYS", "364"))
PUBLISH_LAG_DAYS = int(os.getenv("RMB_FX_INDEX_PUBLISH_LAG_DAYS", "7"))
REQUEST_SLEEP_SECONDS = float(os.getenv("RMB_FX_INDEX_SLEEP", "0.5"))
RETRIES = int(os.getenv("RMB_FX_INDEX_RETRIES", "5"))
RETRY_SLEEP_SECONDS = float(os.getenv("RMB_FX_INDEX_RETRY_SLEEP", "20"))
FULL_REFRESH = os.getenv("RMB_FX_INDEX_FULL_REFRESH", "0") == "1"
USE_LEGACY_TLS = os.getenv("RMB_FX_INDEX_LEGACY_TLS", "1") == "1"
USE_INSECURE_CIPHERS = os.getenv("RMB_FX_INDEX_INSECURE_CIPHERS", "1") == "1"
LEGACY_CIPHERS = os.getenv("RMB_FX_INDEX_LEGACY_CIPHERS", "DEFAULT@SECLEVEL=0")

BASE_URL = "https://www.chinamoney.org.cn/ags/ms/cm-u-bk-fx/RmbIdxHis"


class LegacyTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        if USE_LEGACY_TLS:
            ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
            ctx.options |= getattr(ssl, "OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION", 0x00040000)
        if USE_INSECURE_CIPHERS:
            try:
                ctx.set_ciphers(LEGACY_CIPHERS)
            except Exception:
                pass
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


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
    session.mount("https://www.chinamoney.org.cn", LegacyTLSAdapter(max_retries=retry))
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/148.0.0.0 Safari/537.36 Edg/148.0.0.0"
        ),
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.chinamoney.com.cn/",
    })
    return session


def build_params(start_dt: date, end_dt: date) -> dict:
    return {
        "lang": "cn",
        "startDate": start_dt.isoformat(),
        "endDate": end_dt.isoformat(),
    }


def request_url(params: dict) -> str:
    return requests.Request("GET", BASE_URL, params=params).prepare().url


def fetch_range(session: requests.Session, start_dt: date, end_dt: date) -> pd.DataFrame:
    params = build_params(start_dt, end_dt)
    for attempt in range(1, RETRIES + 1):
        try:
            resp = session.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            payload = resp.json()
            records = payload.get("records") or []
            break
        except requests.exceptions.RequestException as exc:
            print(f"[{start_dt} -> {end_dt}] request failed attempt {attempt}/{RETRIES}: {exc}")
            print(f"[{start_dt} -> {end_dt}] url: {request_url(params)}")
            if attempt >= RETRIES:
                raise
            time.sleep(RETRY_SLEEP_SECONDS * attempt)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records)
    source_cols = [
        "showDate",
        "showDateEn",
        "cfetsIndexRateStr",
        "cfetsIndexRate",
        "bisIndexRateStr",
        "bisIndexRate",
        "sdrIndexRateStr",
        "sdrIndexRate",
    ]
    for col in source_cols:
        if col not in df.columns:
            df[col] = None
    df = df[source_cols].rename(columns={
        "showDate": "dt",
        "showDateEn": "dt_en",
        "cfetsIndexRateStr": "cfets_index_rate_text",
        "cfetsIndexRate": "cfets_index_rate",
        "bisIndexRateStr": "bis_index_rate_text",
        "bisIndexRate": "bis_index_rate",
        "sdrIndexRateStr": "sdr_index_rate_text",
        "sdrIndexRate": "sdr_index_rate",
    })
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    for col in ["cfets_index_rate", "bis_index_rate", "sdr_index_rate"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["range_start"] = start_dt.isoformat()
    df["range_end"] = end_dt.isoformat()
    return (
        df.dropna(subset=["dt"])
        .drop_duplicates(subset=["dt"], keep="last")
        .sort_values("dt")
        .reset_index(drop=True)
    )


def fetch_all(session: requests.Session, start_dt: date, end_dt: date) -> pd.DataFrame:
    frames = []
    current = start_dt
    while current <= end_dt:
        finish = min(current + timedelta(days=CHUNK_DAYS - 1), end_dt)
        print(f"[rmb_fx_index] fetch {current} -> {finish}")
        df = fetch_range(session, current, finish)
        if not df.empty:
            frames.append(df)
        current = finish + timedelta(days=1)
        time.sleep(REQUEST_SLEEP_SECONDS)

    if not frames:
        return pd.DataFrame()
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=["dt"], keep="last")
        .sort_values("dt")
        .reset_index(drop=True)
    )


def run(parquet_dir: str = PARQUET_DIR):
    effective_end = date.today() - timedelta(days=PUBLISH_LAG_DAYS)
    initial_dt = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    latest = discover_latest_date(parquet_dir)
    start_dt = initial_dt if FULL_REFRESH or latest is None else latest + timedelta(days=1)
    if start_dt > effective_end:
        print(f"[rmb_fx_index] already up to {latest}, effective_end={effective_end}; skip.")
        return

    mode = "full refresh" if FULL_REFRESH else "incremental"
    print(f"[rmb_fx_index] mode={mode}; latest={latest}; fetch {start_dt} -> {effective_end}")
    session = create_retry_session()
    df = fetch_all(session, start_dt, effective_end)
    if df.empty:
        print("[rmb_fx_index] no new rows.")
        return

    written = upsert_monthly_parquet(
        df,
        parquet_dir=parquet_dir,
        filename_template=FNAME_TPL,
        key_cols=["dt"],
        sort_cols=["dt"],
    )
    print(f"[rmb_fx_index] parquet upserted {written} rows into {parquet_dir}")


if __name__ == "__main__":
    run()
