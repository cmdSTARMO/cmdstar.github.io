import os
import re
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from parquet_incremental import upsert_monthly_parquet
except ImportError:
    from base_data_renew_folder.parquet_incremental import upsert_monthly_parquet


REPO_ROOT = Path(__file__).resolve().parents[2]
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "sw_industry_daily_data")
FNAME_TPL = "sw_industry_daily_{code}_{yyyymm}.parquet"
INITIAL_START = "1999-12-30"
REQUEST_SLEEP_SECONDS = float(os.getenv("SW_INDUSTRY_DAILY_SLEEP", "0.3"))
RETRIES = int(os.getenv("SW_INDUSTRY_DAILY_RETRIES", "5"))
RETRY_SLEEP_SECONDS = float(os.getenv("SW_INDUSTRY_DAILY_RETRY_SLEEP", "30"))
CONNECT_TIMEOUT_SECONDS = float(os.getenv("SW_INDUSTRY_DAILY_CONNECT_TIMEOUT", "12"))
READ_TIMEOUT_SECONDS = float(os.getenv("SW_INDUSTRY_DAILY_READ_TIMEOUT", "120"))
PATCH_CURRENT = os.getenv("SW_INDUSTRY_DAILY_PATCH_CURRENT", "1") == "1"
VERIFY_SSL = os.getenv("SW_INDUSTRY_DAILY_VERIFY_SSL", "0") == "1"
COOKIE = os.getenv("SW_INDUSTRY_DAILY_COOKIE", "").strip()
CSRF_TOKEN = os.getenv("SW_INDUSTRY_DAILY_CSRFTOKEN", "").strip()

TREND_URL = "https://www.swsresearch.com/institute-sw/api/index_publish/trend/"
CURRENT_URL = "https://www.swsresearch.com/institute-sw/api/index_publish/current/"

INDUSTRIES = {
    "801010": "农林牧渔",
    "801030": "基础化工",
    "801040": "钢铁",
    "801050": "有色金属",
    "801080": "电子",
    "801110": "家用电器",
    "801120": "食品饮料",
    "801130": "纺织服饰",
    "801140": "轻工制造",
    "801150": "医药生物",
    "801160": "公用事业",
    "801170": "交通运输",
    "801180": "房地产",
    "801200": "商贸零售",
    "801210": "社会服务",
    "801230": "综合",
    "801710": "建筑材料",
    "801720": "建筑装饰",
    "801730": "电力设备",
    "801740": "国防军工",
    "801750": "计算机",
    "801760": "传媒",
    "801770": "通信",
    "801780": "银行",
    "801790": "非银金融",
    "801880": "汽车",
    "801890": "机械设备",
    "801950": "煤炭",
    "801960": "石油石化",
    "801970": "环保",
    "801980": "美容护理",
}


def create_retry_session(total_retries=0, backoff_factor=0, status_forcelist=(500, 502, 504)):
    session = requests.Session()
    session.verify = VERIFY_SSL
    if not VERIFY_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
            "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/18.5 Mobile/15E148 Safari/604.1"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Connection": "close",
        "Pragma": "no-cache",
        "Referer": "https://www.swsresearch.com/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    })
    if COOKIE:
        session.headers["Cookie"] = COOKIE
    if CSRF_TOKEN:
        session.headers["X-CSRFToken"] = CSRF_TOKEN
    return session


def discover_latest_by_code(parquet_dir: str = PARQUET_DIR) -> dict[str, date]:
    if not os.path.isdir(parquet_dir):
        return {}

    latest: dict[str, date] = {}
    for code in INDUSTRIES:
        code_dir = industry_parquet_dir(parquet_dir, code)
        if not os.path.isdir(code_dir):
            continue

        pattern = re.compile(rf"^sw_industry_daily_{re.escape(code)}_(\d{{6}})\.parquet$")
        candidates = []
        for name in os.listdir(code_dir):
            match = pattern.match(name)
            if match:
                candidates.append((match.group(1), os.path.join(code_dir, name)))

        for _, path in sorted(candidates, reverse=True):
            try:
                df = pd.read_parquet(path, columns=["swindexcode", "dt"])
            except Exception:
                continue
            if df.empty:
                continue
            df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
            max_dt = df.dropna(subset=["dt"])["dt"].max()
            if pd.notna(max_dt):
                latest[code] = max_dt
                break
    return latest


def industry_parquet_dir(parquet_dir: str, code: str) -> str:
    return os.path.join(parquet_dir, code)


def industry_filename_template(code: str) -> str:
    return FNAME_TPL.format(code=code, yyyymm="{yyyymm}")


def _to_number(value):
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def fetch_trend(session: requests.Session, code: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    params = {"swindexcode": code, "period": "DAY"}
    resp = session.get(TREND_URL, params=params, timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS))
    resp.raise_for_status()
    payload = resp.json()
    records = payload.get("data") or []
    if not records:
        return pd.DataFrame()

    df = pd.DataFrame.from_records(records)
    source_cols = [
        "bargaindate",
        "openindex",
        "maxindex",
        "minindex",
        "closeindex",
        "hike",
        "markup",
        "bargainamount",
        "bargainsum",
    ]
    for col in source_cols:
        if col not in df.columns:
            df[col] = None
    df = df[source_cols].rename(columns={
        "bargaindate": "dt",
        "openindex": "open_index",
        "maxindex": "high_index",
        "minindex": "low_index",
        "closeindex": "close_index",
        "hike": "change",
        "markup": "pct_chg",
        "bargainamount": "volume_100m_shares",
        "bargainsum": "amount_100m_yuan",
    })
    df.insert(0, "swindexcode", code)
    df.insert(0, "industry_name", INDUSTRIES.get(code, code))
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    df = df[(df["dt"] >= start_dt) & (df["dt"] <= end_dt)]
    for col in [
        "open_index",
        "high_index",
        "low_index",
        "close_index",
        "change",
        "pct_chg",
        "volume_100m_shares",
        "amount_100m_yuan",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["dt"]).sort_values(["swindexcode", "dt"]).reset_index(drop=True)


def fetch_current_patch(session: requests.Session, code: str, end_dt: date) -> pd.DataFrame:
    params = {
        "page": 1,
        "page_size": 999,
        "indextype": "一级行业",
        "sortField": "",
        "rule": "",
    }
    resp = session.get(CURRENT_URL, params=params, timeout=(CONNECT_TIMEOUT_SECONDS, READ_TIMEOUT_SECONDS))
    resp.raise_for_status()
    payload = resp.json()
    results = ((payload.get("data") or {}).get("results")) or []
    match = next((row for row in results if str(row.get("swindexcode")) == code), None)
    if not match:
        return pd.DataFrame()

    close_index = _to_number(match.get("l8")) or 0
    amount = (_to_number(match.get("l5")) or 0) / 100
    return pd.DataFrame([{
        "industry_name": INDUSTRIES.get(code, code),
        "swindexcode": code,
        "dt": end_dt,
        "open_index": 0,
        "high_index": 0,
        "low_index": 0,
        "close_index": close_index,
        "change": 0,
        "pct_chg": 0,
        "volume_100m_shares": 0,
        "amount_100m_yuan": amount,
    }])


def fetch_one_code(session: requests.Session, code: str, start_dt: date, end_dt: date) -> pd.DataFrame:
    df = fetch_trend(session, code, start_dt, end_dt)
    if PATCH_CURRENT and end_dt not in set(df["dt"].tolist() if not df.empty else []):
        patch = fetch_current_patch(session, code, end_dt)
        if not patch.empty:
            df = pd.concat([df, patch], ignore_index=True)
    if df.empty:
        return df
    return (
        df.drop_duplicates(subset=["swindexcode", "dt"], keep="last")
        .sort_values(["swindexcode", "dt"])
        .reset_index(drop=True)
    )


def fetch_one_code_with_retry(session: requests.Session, code: str, start_dt: date, end_dt: date) -> pd.DataFrame | None:
    for attempt in range(1, RETRIES + 1):
        try:
            return fetch_one_code(session, code, start_dt, end_dt)
        except requests.exceptions.RequestException as exc:
            print(f"[{code}] request failed attempt {attempt}/{RETRIES}: {exc}")
            print(f"[{code}] trend url: {TREND_URL}?swindexcode={code}&period=DAY")
            if attempt < RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
    return None


def run(parquet_dir: str = PARQUET_DIR):
    end_dt = date.today()
    initial_dt = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()
    discover_started = time.time()
    latest = discover_latest_by_code(parquet_dir)
    print(f"SW industry daily latest scan finished in {time.time() - discover_started:.2f}s; codes={len(latest)}")
    session = create_retry_session()
    print(
        "SW industry daily request config: "
        f"ssl_verify={VERIFY_SSL}; timeout=({CONNECT_TIMEOUT_SECONDS}, {READ_TIMEOUT_SECONDS}); "
        f"retries={RETRIES}; retry_sleep={RETRY_SLEEP_SECONDS}",
        flush=True,
    )

    total_written = 0
    total_codes = len(INDUSTRIES)
    for index, (code, industry_name) in enumerate(INDUSTRIES.items(), start=1):
        code_started = time.time()
        print(f"[sw_industry_daily] ({index}/{total_codes}) start {code} {industry_name}", flush=True)
        start_dt = (latest[code] + timedelta(days=1)) if code in latest else initial_dt
        if start_dt > end_dt:
            print(
                f"[sw_industry_daily] ({index}/{total_codes}) done {code} {industry_name}: "
                f"already up to {latest[code]}, skip; elapsed={time.time() - code_started:.2f}s",
                flush=True,
            )
            continue

        print(f"[{code}] latest={latest.get(code)}; fetch {start_dt} -> {end_dt}")
        df = fetch_one_code_with_retry(session, code, start_dt, end_dt)
        if df is None:
            print(
                f"[sw_industry_daily] ({index}/{total_codes}) done {code} {industry_name}: "
                f"failed after retries; next retry from {start_dt}; elapsed={time.time() - code_started:.2f}s",
                flush=True,
            )
        elif df.empty:
            print(
                f"[sw_industry_daily] ({index}/{total_codes}) done {code} {industry_name}: "
                f"no new rows; elapsed={time.time() - code_started:.2f}s",
                flush=True,
            )
        else:
            written = upsert_monthly_parquet(
                df,
                parquet_dir=industry_parquet_dir(parquet_dir, code),
                filename_template=industry_filename_template(code),
                key_cols=["swindexcode", "dt"],
                sort_cols=["swindexcode", "dt"],
            )
            total_written += written
            print(
                f"[sw_industry_daily] ({index}/{total_codes}) done {code} {industry_name}: "
                f"parquet upserted {written} rows; elapsed={time.time() - code_started:.2f}s",
                flush=True,
            )
        time.sleep(REQUEST_SLEEP_SECONDS)

    print(f"All done. Total parquet upserted {total_written} rows.")


if __name__ == "__main__":
    run()
