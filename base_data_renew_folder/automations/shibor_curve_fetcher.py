import os
from datetime import datetime
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
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "shibor_curve_data")
FNAME_TPL = "shibor_curve_{yyyymm}.parquet"
BASE_URL = "https://www.chinamoney.org.cn/ags/ms/cm-u-bk-shibor/ShiborChrt"

COLUMNS = [
    "dt",
    "shibor_on",
    "shibor_1w",
    "shibor_2w",
    "shibor_1m",
    "shibor_3m",
    "shibor_6m",
    "shibor_9m",
    "shibor_1y",
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
        "Referer": "https://www.chinamoney.com.cn/",
    })
    return session


def fetch_shibor_curve(session: requests.Session) -> pd.DataFrame:
    params = {"lang": "CN"}
    resp = session.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    csv_text = ((payload.get("data") or {}).get("csv") or "").strip()
    if not csv_text:
        return pd.DataFrame(columns=COLUMNS)

    rows = []
    for line in csv_text.splitlines():
        parts = [item.strip() for item in line.split(",")]
        if len(parts) < 14 or not parts[0]:
            continue
        rows.append({
            "dt": parts[0],
            "shibor_on": parts[6],
            "shibor_1w": parts[7],
            "shibor_2w": parts[8],
            "shibor_1m": parts[9],
            "shibor_3m": parts[10],
            "shibor_6m": parts[11],
            "shibor_9m": parts[12],
            "shibor_1y": parts[13],
        })

    if not rows:
        return pd.DataFrame(columns=COLUMNS)

    df = pd.DataFrame(rows)
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    for col in COLUMNS[1:]:
        df[col] = pd.to_numeric(df[col].replace("", None), errors="coerce")
    return (
        df.dropna(subset=["dt"])
        .drop_duplicates(subset=["dt"], keep="last")
        .sort_values("dt")
        .reset_index(drop=True)
    )


def run(parquet_dir: str = PARQUET_DIR):
    session = create_retry_session()
    df = fetch_shibor_curve(session)
    if df.empty:
        print("[shibor_curve] no rows fetched.")
        return

    written = upsert_monthly_parquet(
        df,
        parquet_dir=parquet_dir,
        filename_template=FNAME_TPL,
        key_cols=["dt"],
        sort_cols=["dt"],
    )
    print(f"[shibor_curve] parquet upserted {written} rows into {parquet_dir}")
    print(f"[shibor_curve] date range: {df['dt'].min()} -> {df['dt'].max()}")


if __name__ == "__main__":
    run()
