import os
import random
import ssl
import time
from datetime import date, datetime, timedelta, timezone
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
PARQUET_DIR = str(REPO_ROOT / "api" / "data" / "erp_hs300_10y_data")
FNAME_TPL = "erp_hs300_10y_{yyyymm}.parquet"
YEARS = int(os.getenv("ERP_HS300_10Y_YEARS", "3"))
ROLLING_WINDOW = int(os.getenv("ERP_HS300_10Y_ROLLING_WINDOW", "200"))
GOV_CHUNK_MONTHS = int(os.getenv("ERP_HS300_10Y_GOV_CHUNK_MONTHS", "3"))
USE_LEGACY_TLS = os.getenv("ERP_HS300_10Y_LEGACY_TLS", "1") == "1"
USE_INSECURE_CIPHERS = os.getenv("ERP_HS300_10Y_INSECURE_CIPHERS", "1") == "1"
LEGACY_CIPHERS = os.getenv("ERP_HS300_10Y_LEGACY_CIPHERS", "DEFAULT@SECLEVEL=0")

GOV_YIELD_URL = os.getenv(
    "ERP_HS300_10Y_GOV_YIELD_URL",
    "https://www.chinamoney.org.cn/ags/ms/cm-u-bk-currency/SddsIntrRateGovYldHis",
)
YF_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/000300.SS"


class LegacyTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        if USE_LEGACY_TLS:
            # Python/OpenSSL builds do not always expose this constant, but
            # OpenSSL uses 0x4 for OP_LEGACY_SERVER_CONNECT.
            ctx.options |= getattr(ssl, "OP_LEGACY_SERVER_CONNECT", 0x4)
            # OpenSSL 3 may also require SSL_OP_ALLOW_UNSAFE_LEGACY_RENEGOTIATION.
            # Python 3.12 does not expose it, but OpenSSL defines it as 0x00040000.
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
    })
    return session


def get_json_with_retry(session: requests.Session, url: str, params: dict, headers: dict | None = None):
    last_err = None
    for attempt in range(1, 7):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=30)
            text = (resp.text or "").strip()
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {resp.status_code}; head={text[:160]}")
            resp.raise_for_status()
            if not text:
                raise RuntimeError("empty response body")
            if text.startswith("<") or "<html" in text[:200].lower():
                raise RuntimeError(f"non-json response; head={text[:160]}")
            return resp.json()
        except Exception as exc:
            last_err = exc
            print(f"[retry {attempt}/6] {url}: {exc}")
            if attempt < 6:
                time.sleep(0.8 * (2 ** (attempt - 1)) + random.random() * 0.3)
    raise last_err if last_err else RuntimeError("request failed")


def add_months(d: date, months: int) -> date:
    ts = pd.Timestamp(d) + pd.DateOffset(months=months)
    return ts.date()


def fetch_gov_yield_segment(session: requests.Session, start_dt: date, end_dt: date, page_size: int = 50) -> list[dict]:
    headers = {"Referer": "https://www.chinamoney.com.cn/"}
    params = {
        "lang": "CN",
        "startDate": start_dt.isoformat(),
        "endDate": end_dt.isoformat(),
        "pageNum": 1,
        "pageSize": page_size,
    }
    payload = get_json_with_retry(session, GOV_YIELD_URL, params=params, headers=headers)
    data = payload.get("data", payload)
    page_total = int(data.get("pageTotal", 1) or 1)
    records = list(data.get("records", payload.get("records", [])) or [])

    for page in range(2, page_total + 1):
        params["pageNum"] = page
        payload = get_json_with_retry(session, GOV_YIELD_URL, params=params, headers=headers)
        data = payload.get("data", payload)
        records.extend(data.get("records", payload.get("records", [])) or [])
        time.sleep(0.15 + random.random() * 0.15)
    return records


def fetch_gov_yield(session: requests.Session, start_dt: date, end_dt: date) -> pd.DataFrame:
    all_records = []
    cur_start = start_dt
    while cur_start <= end_dt:
        cur_end = min(add_months(cur_start, GOV_CHUNK_MONTHS) - timedelta(days=1), end_dt)
        print(f"[erp] fetch gov yield {cur_start} -> {cur_end}")
        all_records.extend(fetch_gov_yield_segment(session, cur_start, cur_end))
        cur_start = cur_end + timedelta(days=1)

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    for col in ["dateString", "tenRate"]:
        if col not in df.columns:
            df[col] = None
    df = df[["dateString", "tenRate"]].rename(columns={
        "dateString": "dt",
        "tenRate": "cn_gov_10y_yield_pct",
    })
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce").dt.date
    df["cn_gov_10y_yield_pct"] = pd.to_numeric(df["cn_gov_10y_yield_pct"], errors="coerce")
    return (
        df.dropna(subset=["dt"])
        .drop_duplicates(subset=["dt"], keep="last")
        .sort_values("dt")
        .reset_index(drop=True)
    )


def unix_seconds(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def fetch_hs300(session: requests.Session, start_dt: date, end_dt: date) -> pd.DataFrame:
    params = {
        "interval": "1d",
        "includeAdjustedClose": "true",
        "events": "div|split|capitalGain",
        "formatted": "true",
        "period1": str(unix_seconds(start_dt)),
        "period2": str(unix_seconds(end_dt + timedelta(days=1))),
        "lang": "zh-Hant-HK",
        "region": "HK",
    }
    payload = get_json_with_retry(session, YF_CHART_URL, params=params)
    chart = payload.get("chart", {})
    if chart.get("error"):
        raise RuntimeError(f"Yahoo chart error: {chart['error']}")
    result = chart.get("result") or []
    if not result:
        return pd.DataFrame()

    r0 = result[0]
    timestamps = r0.get("timestamp") or []
    quote = ((r0.get("indicators") or {}).get("quote") or [{}])[0]
    closes = quote.get("close") or []
    rows = []
    for ts, close in zip(timestamps, closes):
        if close is None:
            continue
        dt = pd.to_datetime(ts, unit="s", utc=True).tz_convert("Asia/Shanghai").date()
        rows.append({"dt": dt, "hs300_close": float(close)})

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.drop_duplicates(subset=["dt"], keep="last").sort_values("dt").reset_index(drop=True)
    df["hs300_ret_pct"] = df["hs300_close"].pct_change() * 100
    return df


def build_erp_dataset(end_dt: date | None = None) -> pd.DataFrame:
    if end_dt is None:
        end_dt = date.today()
    start_dt = (pd.Timestamp(end_dt) - pd.DateOffset(years=YEARS)).date()
    session = create_retry_session()

    gov_df = fetch_gov_yield(session, start_dt, end_dt)
    print(f"[erp] gov rows={len(gov_df)}")
    hs300_df = fetch_hs300(session, start_dt, end_dt)
    print(f"[erp] hs300 rows={len(hs300_df)}")
    if gov_df.empty or hs300_df.empty:
        return pd.DataFrame()

    merged = pd.merge(hs300_df, gov_df, on="dt", how="inner").sort_values("dt").reset_index(drop=True)
    merged["rf_daily_pct"] = merged["cn_gov_10y_yield_pct"] / 252.0
    merged["erp_daily_pct"] = merged["hs300_ret_pct"] - merged["rf_daily_pct"]
    erp = merged["erp_daily_pct"]
    merged["erp_ma200_pct"] = erp.rolling(ROLLING_WINDOW).mean()
    merged["erp_sigma200_pct"] = erp.rolling(ROLLING_WINDOW).std()
    merged["erp_ma200_plus_2sigma_pct"] = merged["erp_ma200_pct"] + 2 * merged["erp_sigma200_pct"]
    merged["erp_ma200_minus_2sigma_pct"] = merged["erp_ma200_pct"] - 2 * merged["erp_sigma200_pct"]
    merged["rolling_window"] = ROLLING_WINDOW
    merged["calc_years"] = YEARS
    return merged[[
        "dt",
        "hs300_close",
        "hs300_ret_pct",
        "cn_gov_10y_yield_pct",
        "rf_daily_pct",
        "erp_daily_pct",
        "erp_ma200_pct",
        "erp_sigma200_pct",
        "erp_ma200_plus_2sigma_pct",
        "erp_ma200_minus_2sigma_pct",
        "rolling_window",
        "calc_years",
    ]]


def run(parquet_dir: str = PARQUET_DIR):
    df = build_erp_dataset()
    if df.empty:
        print("[erp] no rows generated.")
        return
    written = upsert_monthly_parquet(
        df,
        parquet_dir=parquet_dir,
        filename_template=FNAME_TPL,
        key_cols=["dt"],
        sort_cols=["dt"],
    )
    print(f"[erp] parquet upserted {written} rows into {parquet_dir}")
    print(f"[erp] date range: {df['dt'].min()} -> {df['dt'].max()}")


if __name__ == "__main__":
    run()
