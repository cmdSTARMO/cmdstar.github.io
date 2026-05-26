import os
from datetime import date, datetime
from typing import Iterable, Sequence

import pandas as pd


def month_key(value) -> str:
    dt = pd.to_datetime(value).date()
    return f"{dt.year:04d}{dt.month:02d}"


def month_bounds(yyyymm: str) -> tuple[date, date]:
    year = int(yyyymm[:4])
    month = int(yyyymm[4:])
    start = date(year, month, 1)
    if month == 12:
        end = date(year + 1, 1, 1)
    else:
        end = date(year, month + 1, 1)
    return start, end


def iter_months(start: date, end: date) -> Iterable[str]:
    year, month = start.year, start.month
    while True:
        yield f"{year:04d}{month:02d}"
        if (year, month) == (end.year, end.month):
            break
        month += 1
        if month == 13:
            year += 1
            month = 1


def discover_latest_date(parquet_dir: str, date_col: str = "dt"):
    if not os.path.isdir(parquet_dir):
        return None

    latest = None
    for name in os.listdir(parquet_dir):
        if not name.endswith(".parquet"):
            continue
        path = os.path.join(parquet_dir, name)
        try:
            df = pd.read_parquet(path, columns=[date_col])
        except Exception:
            continue
        if df.empty:
            continue
        current = pd.to_datetime(df[date_col], errors="coerce").max()
        if pd.isna(current):
            continue
        current_date = current.date()
        if latest is None or current_date > latest:
            latest = current_date
    return latest


def read_month(parquet_dir: str, filename_template: str, yyyymm: str) -> pd.DataFrame:
    path = os.path.join(parquet_dir, filename_template.format(yyyymm=yyyymm))
    if not os.path.isfile(path):
        return pd.DataFrame()
    return pd.read_parquet(path)


def upsert_monthly_parquet(
    df: pd.DataFrame,
    parquet_dir: str,
    filename_template: str,
    key_cols: Sequence[str],
    date_col: str = "dt",
    sort_cols: Sequence[str] | None = None,
) -> int:
    if df is None or df.empty:
        return 0

    os.makedirs(parquet_dir, exist_ok=True)
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce").dt.date
    out = out.dropna(subset=[date_col])
    if out.empty:
        return 0

    written = 0
    for yyyymm, month_df in out.groupby(out[date_col].map(month_key)):
        target = os.path.join(parquet_dir, filename_template.format(yyyymm=yyyymm))
        old = pd.read_parquet(target) if os.path.isfile(target) else pd.DataFrame()
        merged = pd.concat([old, month_df], ignore_index=True)
        merged[date_col] = pd.to_datetime(merged[date_col], errors="coerce").dt.date
        merged = merged.dropna(subset=[date_col])
        merged = merged.drop_duplicates(subset=list(key_cols), keep="last")
        if sort_cols:
            merged = merged.sort_values(list(sort_cols)).reset_index(drop=True)

        tmp = target + ".tmp"
        merged.to_parquet(tmp, index=False, compression="zstd")
        os.replace(tmp, target)
        written += len(month_df)
    return written
