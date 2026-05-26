import ast
import csv
import io
import json
import os
import re
import time
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "fund_new_issue_data")
FNAME_TPL = "fund_new_issue_{yyyymm}.parquet"
PENDING_FILE = "fund_new_issue_pending.parquet"
INITIAL_START = "1999-12-30"
PAGE_SIZE = int(os.getenv("FUND_NEW_ISSUE_PAGE_SIZE", "100"))
MAX_PAGES = int(os.getenv("FUND_NEW_ISSUE_MAX_PAGES", "300"))
FAST_MAX_PAGES = int(os.getenv("FUND_NEW_ISSUE_FAST_MAX_PAGES", "10"))
FAST_SNAPSHOT_MAX_AGE_DAYS = int(os.getenv("FUND_NEW_ISSUE_FAST_SNAPSHOT_MAX_AGE_DAYS", "3"))
REQUEST_SLEEP_SECONDS = float(os.getenv("FUND_NEW_ISSUE_SLEEP", "0.5"))
RETRIES = int(os.getenv("FUND_NEW_ISSUE_RETRIES", "5"))
RETRY_SLEEP_SECONDS = float(os.getenv("FUND_NEW_ISSUE_RETRY_SLEEP", "30"))

BASE_URL = "https://fund.eastmoney.com/data/FundNewIssue.aspx"

COLUMNS = [
    "established_date",
    "fund_code",
    "fund_name",
    "fund_company",
    "company_id",
    "fund_type",
    "raised_shares",
    "unknown_1",
    "fund_manager",
    "subscription_status",
    "subscription_period",
    "unknown_2",
    "unknown_3",
    "fund_company_2",
    "unknown_4",
    "unknown_5",
    "unknown_6",
    "fund_manager_id",
    "discount_rate",
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
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://fund.eastmoney.com/data/fundranking.html",
    })
    return session


def build_params(page: int, page_size: int) -> dict:
    return {
        "t": "xcln",
        "sort": "jzrgq,desc",
        "y": "",
        "page": f"{page},{page_size}",
        "isbuy": "2",
    }


def request_url(params: dict) -> str:
    return requests.Request("GET", BASE_URL, params=params).prepare().url


def _parse_datas(text: str) -> list[list]:
    match = re.search(r"datas\s*:\s*(\[.*?\])\s*,\s*record", text, flags=re.S)
    if not match:
        return []

    raw = match.group(1)
    try:
        rows = json.loads(raw)
    except json.JSONDecodeError:
        try:
            rows = ast.literal_eval(raw)
        except (SyntaxError, ValueError):
            body = raw.strip()
            if body.startswith("["):
                body = body[1:]
            if body.endswith("]"):
                body = body[:-1]
            body = body.replace("],[", "\n")
            body = body.replace("[", "").replace("]", "")
            rows = list(csv.reader(io.StringIO(body)))
    return rows if isinstance(rows, list) else []


def fetch_page(session: requests.Session, page: int, page_size: int) -> pd.DataFrame:
    params = build_params(page, page_size)
    for attempt in range(1, RETRIES + 1):
        try:
            print(f"fetch page {page}: {request_url(params)}")
            resp = session.get(BASE_URL, params=params, timeout=30)
            resp.raise_for_status()
            rows = _parse_datas(resp.content.decode("utf-8", errors="replace"))
            print(f"page {page}: rows={len(rows)}")
            break
        except requests.exceptions.RequestException as exc:
            print(f"[fund_new_issue] page {page} failed attempt {attempt}/{RETRIES}: {exc}")
            print(f"[fund_new_issue] url: {request_url(params)}")
            if attempt >= RETRIES:
                raise
            time.sleep(RETRY_SLEEP_SECONDS * attempt)

    if not rows:
        return pd.DataFrame()

    bad_width = [len(row) for row in rows if len(row) != len(COLUMNS)]
    if bad_width:
        print(f"page {page}: unexpected row widths={sorted(set(bad_width))}")

    source_columns = [
        "fund_code",
        "fund_name",
        "fund_company",
        "company_id",
        "fund_type",
        "raised_shares",
        "established_date",
        "unknown_1",
        "fund_manager",
        "subscription_status",
        "subscription_period",
        "unknown_2",
        "unknown_3",
        "fund_company_2",
        "unknown_4",
        "unknown_5",
        "unknown_6",
        "fund_manager_id",
        "discount_rate",
    ]

    normalized = []
    for row in rows:
        values = list(row[:len(source_columns)])
        values.extend([None] * (len(source_columns) - len(values)))
        normalized.append(values)
    return pd.DataFrame(normalized, columns=source_columns)


def read_existing_snapshot(parquet_dir: str = PARQUET_DIR) -> pd.DataFrame:
    if not os.path.isdir(parquet_dir):
        return pd.DataFrame(columns=COLUMNS + ["snapshot_dt"])

    frames = []
    for name in os.listdir(parquet_dir):
        if not name.startswith("fund_new_issue_") or not name.endswith(".parquet"):
            continue
        path = os.path.join(parquet_dir, name)
        try:
            frames.append(pd.read_parquet(path))
        except Exception as exc:
            print(f"[fund_new_issue] skip unreadable parquet {path}: {exc}")

    if not frames:
        return pd.DataFrame(columns=COLUMNS + ["snapshot_dt"])

    df = pd.concat(frames, ignore_index=True)
    for col in COLUMNS + ["snapshot_dt"]:
        if col not in df.columns:
            df[col] = None
    df["established_date"] = pd.to_datetime(df["established_date"], errors="coerce").dt.date
    df["snapshot_dt"] = pd.to_datetime(df["snapshot_dt"], errors="coerce").dt.date
    return (
        df[COLUMNS + ["snapshot_dt"]]
        .drop_duplicates(subset=["fund_code", "established_date"], keep="last")
        .sort_values(["established_date", "fund_code"], na_position="last")
        .reset_index(drop=True)
    )


def latest_snapshot_date(df: pd.DataFrame) -> date | None:
    if df.empty or "snapshot_dt" not in df.columns:
        return None
    values = pd.to_datetime(df["snapshot_dt"], errors="coerce").dropna()
    if values.empty:
        return None
    return values.max().date()


def use_fast_update(existing_df: pd.DataFrame) -> bool:
    latest = latest_snapshot_date(existing_df)
    if latest is None:
        return False
    return (date.today() - latest).days <= FAST_SNAPSHOT_MAX_AGE_DAYS


def normalize_raw_snapshot(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=COLUMNS + ["snapshot_dt"])

    out = raw_df.copy()
    out["established_date"] = pd.to_datetime(out["established_date"], errors="coerce").dt.date
    out["raised_shares"] = pd.to_numeric(out["raised_shares"], errors="coerce")
    start_date = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    out = out[out["established_date"].isna() | (out["established_date"] >= start_date)]
    out = out.drop_duplicates(subset=["fund_code", "established_date"], keep="last")
    out["snapshot_dt"] = date.today()
    return out[["established_date"] + [col for col in COLUMNS if col != "established_date"] + ["snapshot_dt"]].sort_values(
        ["established_date", "fund_code"],
        na_position="last",
    ).reset_index(drop=True)


def merge_fast_snapshot(existing_df: pd.DataFrame, fast_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df.empty:
        return fast_df
    if fast_df.empty:
        return existing_df

    combined = pd.concat([existing_df, fast_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["fund_code", "established_date"], keep="last")
    return combined[["established_date"] + [col for col in COLUMNS if col != "established_date"] + ["snapshot_dt"]].sort_values(
        ["established_date", "fund_code"],
        na_position="last",
    ).reset_index(drop=True)


def fetch_all(max_pages: int = MAX_PAGES) -> pd.DataFrame:
    session = create_retry_session()
    tables = []
    for page in range(1, max_pages + 1):
        df = fetch_page(session, page, PAGE_SIZE)
        if df.empty:
            break
        tables.append(df)
        time.sleep(REQUEST_SLEEP_SECONDS)

    if not tables:
        return pd.DataFrame(columns=COLUMNS)

    return normalize_raw_snapshot(pd.concat(tables, ignore_index=True))


def _month_key(value) -> str:
    dt = pd.to_datetime(value).date()
    return f"{dt.year:04d}{dt.month:02d}"


def write_sliced_snapshot(df: pd.DataFrame, parquet_dir: str) -> int:
    os.makedirs(parquet_dir, exist_ok=True)

    for name in os.listdir(parquet_dir):
        if name.startswith("fund_new_issue_") and name.endswith(".parquet"):
            os.remove(os.path.join(parquet_dir, name))

    pending = df[df["established_date"].isna()].copy()
    dated = df[df["established_date"].notna()].copy()

    written = 0
    if not pending.empty:
        target = os.path.join(parquet_dir, PENDING_FILE)
        tmp = target + ".tmp"
        pending.to_parquet(tmp, index=False, compression="zstd")
        os.replace(tmp, target)
        written += len(pending)

    if not dated.empty:
        for yyyymm, month_df in dated.groupby(dated["established_date"].map(_month_key)):
            target = os.path.join(parquet_dir, FNAME_TPL.format(yyyymm=yyyymm))
            tmp = target + ".tmp"
            month_df.sort_values(["established_date", "fund_code"]).to_parquet(tmp, index=False, compression="zstd")
            os.replace(tmp, target)
            written += len(month_df)
    return written


def run(parquet_dir: str = PARQUET_DIR):
    existing = read_existing_snapshot(parquet_dir)
    old_rows = len(existing)
    old_latest_snapshot = latest_snapshot_date(existing)
    fast_mode = use_fast_update(existing)

    if fast_mode:
        print(
            "[fund_new_issue] fast update enabled: "
            f"latest snapshot={old_latest_snapshot}, fetch first {FAST_MAX_PAGES} pages."
        )
        fetched = fetch_all(max_pages=FAST_MAX_PAGES)
        df = merge_fast_snapshot(existing, fetched)
    else:
        print(
            "[fund_new_issue] full snapshot update: "
            f"latest snapshot={old_latest_snapshot}, fetch up to {MAX_PAGES} pages."
        )
        fetched = fetch_all(max_pages=MAX_PAGES)
        df = fetched

    if df.empty:
        print("[fund_new_issue] no rows fetched.")
        return

    written = write_sliced_snapshot(df, parquet_dir)

    valid_dates = df["established_date"].dropna()
    print(
        "[fund_new_issue] "
        f"mode={'fast' if fast_mode else 'full'}, "
        f"old_rows={old_rows}, fetched_rows={len(fetched)}, final_rows={len(df)}, "
        f"sliced_rows={written}, parquet_dir={parquet_dir}"
    )
    if not valid_dates.empty:
        print(f"[fund_new_issue] established_date range: {valid_dates.min()} -> {valid_dates.max()}")


if __name__ == "__main__":
    run()
