import ssl
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

try:
    from parquet_incremental import discover_latest_date, upsert_monthly_parquet
except ImportError:
    from base_data_renew_folder.parquet_incremental import discover_latest_date, upsert_monthly_parquet


BASE_URL = "https://www.chinamoney.org.cn/ags/ms/cm-u-bk-currency/ClsYldCurvHis"
REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "ncd_aaa_yield_curve_data")
FNAME_TPL = "ncd_aaa_yield_curve_{yyyymm}.parquet"
INITIAL_LOOKBACK_DAYS = 92

USE_INSECURE_CIPHERS = True
CIPHERS = "DEFAULT@SECLEVEL=1"

DEFAULT_PARAMS = {
    "lang": "CN",
    "reference": "1",
    "bondType": "CYCC41B",
    "termId": "0.1",
    "pageNum": 1,
    "pageSize": 9999,
}


class LegacyTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        if hasattr(ssl, "OP_LEGACY_SERVER_CONNECT"):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        if USE_INSECURE_CIPHERS:
            try:
                ctx.set_ciphers(CIPHERS)
            except Exception:
                pass
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)


session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.chinamoney.com.cn/",
})
session.mount("https://www.chinamoney.org.cn", LegacyTLSAdapter())


def daterange_chunks(start_dt: date, end_dt: date, days_per_chunk=10):
    cur = start_dt
    while cur <= end_dt:
        chunk_end = min(cur + timedelta(days=days_per_chunk - 1), end_dt)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def fetch_once(start_date: str, end_date: str, params: dict, max_retries=3, pause=0.6):
    query = params.copy()
    query["startDate"] = start_date
    query["endDate"] = end_date

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(BASE_URL, params=query, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            head = data.get("head", {})
            if head.get("rep_code") != "200":
                raise RuntimeError(f"API rep_code={head.get('rep_code')} msg={head.get('rep_message')}")
            return data.get("records", []) or []
        except Exception:
            if attempt == max_retries:
                raise
            time.sleep(pause * attempt)
    return []


def to_dataframe(records):
    columns = ["dt", "term_year", "maturity_yield", "current_yield", "future_yield"]
    if not records:
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame.from_records(records).rename(columns={
        "newDateValueCN": "dt",
        "yearTermStr": "term_year",
        "maturityYieldStr": "maturity_yield",
        "currentYieldStr": "current_yield",
        "futureYieldStr": "future_yield",
    })
    for col in columns:
        if col not in df.columns:
            df[col] = None
    df = df[columns]
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    for col in ["term_year", "maturity_yield", "current_yield", "future_yield"]:
        df[col] = pd.to_numeric(df[col].replace("---", pd.NA), errors="coerce")

    return (
        df.dropna(subset=["dt", "term_year"])
        .drop_duplicates(subset=["dt", "term_year"], keep="last")
        .sort_values(["dt", "term_year"])
        .reset_index(drop=True)
    )


def resolve_start_date(parquet_dir: str, end_dt: date) -> date:
    latest = discover_latest_date(parquet_dir)
    if latest:
        return latest + timedelta(days=1)
    return end_dt - timedelta(days=INITIAL_LOOKBACK_DAYS)


def main(end_date_str: str | None = None, params: dict | None = None, parquet_dir: str = PARQUET_DIR):
    params = params or DEFAULT_PARAMS
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date() if end_date_str else date.today()
    start_dt = resolve_start_date(parquet_dir, end_dt)

    if start_dt > end_dt:
        print(f"Parquet already contains data through {start_dt - timedelta(days=1)}; no update needed.")
        return

    frames = []
    for i, (begin, end) in enumerate(daterange_chunks(start_dt, end_dt, 10), start=1):
        begin_s, end_s = begin.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")
        print(f"[{i}] Fetch {begin_s} ~ {end_s}")
        records = fetch_once(begin_s, end_s, params)
        df = to_dataframe(records)
        print(f"  -> {len(df)} rows")
        frames.append(df)

    if not frames:
        print("No data.")
        return

    full = pd.concat(frames, ignore_index=True)
    full = (
        full.drop_duplicates(subset=["dt", "term_year"], keep="last")
        .sort_values(["dt", "term_year"])
        .reset_index(drop=True)
    )
    written = upsert_monthly_parquet(
        full,
        parquet_dir=parquet_dir,
        filename_template=FNAME_TPL,
        key_cols=["dt", "term_year"],
        sort_cols=["dt", "term_year"],
    )
    print(f"Done. Parquet upserted {written} rows into {parquet_dir}")


if __name__ == "__main__":
    main()
