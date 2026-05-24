# SSE margin trading details collector.
# Writes directly to monthly Parquet files under api/data/margin_sse_tab2_data.

import io
import random
import re
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from parquet_incremental import discover_latest_date, upsert_monthly_parquet
except ImportError:
    from base_data_renew_folder.parquet_incremental import discover_latest_date, upsert_monthly_parquet


INITIAL_START = "2010-03-31"
END_DATE = date.today().strftime("%Y-%m-%d")
PARQUET_DIR = "../api/data/margin_sse_tab2_data"
FNAME_TPL = "sse_tab2_{yyyymm}.parquet"
SLEEP_BASE, SLEEP_JITTER = 5, 10

URL_TPL = "https://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk{yyyymmdd}.xls"

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

RENAME_MAP = {
    "标的证券代码": "code",
    "标的证券简称": "name",
    "本日融资余额(元)": "margin_balance",
    "本日融资买入额(元)": "margin_buy_amt",
    "本日融资偿还额(元)": "margin_repay_amt",
    "本日融券余量": "short_qty",
    "本日融券卖出量": "short_sell_qty",
    "本日融券偿还量": "short_repay_qty",
}

COLS = [
    "dt",
    "code",
    "name",
    "margin_balance",
    "margin_buy_amt",
    "margin_repay_amt",
    "short_qty",
    "short_sell_qty",
    "short_repay_qty",
]


def create_retry_session(total_retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504, 520, 521, 522)):
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


def ymd_compact(d: date) -> str:
    return d.strftime("%Y%m%d")


def ymd_dash(d: date) -> str:
    return d.strftime("%Y-%m-%d")


def parse_number(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text or text in {"-", "—", "nan", "None"}:
        return None
    text = text.replace(",", "").replace("，", "")
    try:
        return float(text)
    except ValueError:
        cleaned = re.sub(r"[^\d.\-eE]", "", text)
        return float(cleaned) if cleaned else None


def fetch_one_day(session: requests.Session, d: date):
    url = URL_TPL.format(yyyymmdd=ymd_compact(d))
    resp = session.get(url, headers=HEADERS, timeout=20)
    if resp.status_code != 200 or not resp.content:
        return None

    bio = io.BytesIO(resp.content)
    try:
        df = pd.read_excel(bio, sheet_name="明细信息", dtype=str, engine="xlrd")
    except Exception:
        xls = pd.ExcelFile(bio, engine="xlrd")
        target = next((name for name in xls.sheet_names if "明细" in name), None)
        if not target:
            return None
        df = pd.read_excel(xls, sheet_name=target, dtype=str)

    df = df.dropna(how="all").copy()
    if df.empty:
        return None

    keep_cols = [c for c in df.columns if c in RENAME_MAP]
    df = df[keep_cols].rename(columns=RENAME_MAP)
    if "code" not in df.columns or "name" not in df.columns:
        return None

    for col in ["margin_balance", "margin_buy_amt", "margin_repay_amt", "short_qty", "short_sell_qty", "short_repay_qty"]:
        df[col] = df[col].map(parse_number) if col in df.columns else None

    df["dt"] = ymd_dash(d)
    for col in COLS:
        if col not in df.columns:
            df[col] = None
    df = df[COLS].copy()
    df["code"] = df["code"].astype(str).str.strip()
    return df[df["code"].ne("")]


def run(end_date_str=END_DATE, parquet_dir=PARQUET_DIR):
    latest = discover_latest_date(parquet_dir)
    start_date = latest + timedelta(days=1) if latest else datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    if start_date > end_date:
        print(f"Parquet already contains data through {latest}; no update needed.")
        return

    session = create_retry_session()
    cur_date = start_date
    frames = []

    while cur_date <= end_date:
        ds = ymd_dash(cur_date)
        print(f"[{ds}] downloading/parsing...", end="", flush=True)
        try:
            df = fetch_one_day(session, cur_date)
        except requests.exceptions.RequestException as exc:
            print(f" request failed: {exc}; skip")
            cur_date += timedelta(days=1)
            time.sleep(2)
            continue
        except Exception as exc:
            print(f" parse failed: {exc}; skip")
            cur_date += timedelta(days=1)
            time.sleep(1)
            continue

        if df is None or df.empty:
            print(" no data")
        else:
            frames.append(df)
            print(f" OK, {len(df)} rows")

        time.sleep(SLEEP_BASE + SLEEP_JITTER * random.random())
        cur_date += timedelta(days=1)

    if frames:
        full = pd.concat(frames, ignore_index=True)
        written = upsert_monthly_parquet(
            full,
            parquet_dir=parquet_dir,
            filename_template=FNAME_TPL,
            key_cols=["dt", "code"],
            sort_cols=["dt", "code"],
        )
        print(f"Done. Parquet upserted {written} rows.")
    else:
        print("Done. No new rows.")


if __name__ == "__main__":
    run(end_date_str=END_DATE, parquet_dir=PARQUET_DIR)
