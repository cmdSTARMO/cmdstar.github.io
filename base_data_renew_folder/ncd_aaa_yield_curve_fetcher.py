import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import ssl
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager

BASE_URL = "https://www.chinamoney.org.cn/ags/ms/cm-u-bk-currency/ClsYldCurvHis"

# ===== 配置：必要时把 SECLEVEL 降到 1（有些环境不需要） =====
USE_INSECURE_CIPHERS = True  # True 时尝试 DEFAULT@SECLEVEL=1；不行再关掉
CIPHERS = "DEFAULT@SECLEVEL=1"

class LegacyTLSAdapter(HTTPAdapter):
    """允许与仅支持旧式 TLS 重协商的站点建立连接"""
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        # 允许旧式（不安全）重协商
        if hasattr(ssl, "OP_LEGACY_SERVER_CONNECT"):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        # 某些环境还需要把 OpenSSL 安全级别降到 1
        if USE_INSECURE_CIPHERS:
            try:
                ctx.set_ciphers(CIPHERS)
            except Exception:
                pass
        kwargs["ssl_context"] = ctx
        return super().init_poolmanager(*args, **kwargs)

# 统一会话（带自定义 TLS）
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.chinamoney.com.cn/",
})
session.mount("https://www.chinamoney.org.cn", LegacyTLSAdapter())

# ======= 需要你填写的时间范围（含首尾） =======
START_DATE = "2024-08-29"   # yyyy-mm-dd
END_DATE   = "2025-09-20"   # yyyy-mm-dd
# ============================================

DEFAULT_PARAMS = {
    "lang": "CN",
    "reference": "1",
    "bondType": "CYCC41B",
    "termId": "0.1",
    "pageNum": 1,
    "pageSize": 9999
}

def daterange_chunks(start_dt: datetime, end_dt: datetime, days_per_chunk=10):
    cur = start_dt
    while cur <= end_dt:
        chunk_end = min(cur + timedelta(days=days_per_chunk - 1), end_dt)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)

def fetch_once(start_date: str, end_date: str, params: dict, max_retries=3, pause=0.6):
    q = params.copy()
    q["startDate"] = start_date
    q["endDate"] = end_date

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(BASE_URL, params=q, timeout=30)  # 用带自定义 TLS 的 session
            resp.raise_for_status()
            data = resp.json()
            head = data.get("head", {})
            if head.get("rep_code") != "200":
                raise RuntimeError(f"API rep_code={head.get('rep_code')} msg={head.get('rep_message')}")
            return data.get("records", []) or [], data
        except Exception as e:
            if attempt == max_retries:
                raise
            time.sleep(pause * attempt)
    return [], {}

def to_dataframe(records):
    if not records:
        return pd.DataFrame(columns=["日期","期限(年)","到期收益率(%)","当前收益率(%)","远期收益率(%)"])
    df = pd.DataFrame.from_records(records).rename(columns={
        "newDateValueCN": "日期",
        "yearTermStr": "期限(年)",
        "maturityYieldStr": "到期收益率(%)",
        "currentYieldStr": "当前收益率(%)",
        "futureYieldStr": "远期收益率(%)",
    })
    keep = ["日期","期限(年)","到期收益率(%)","当前收益率(%)","远期收益率(%)"]
    df = df[[c for c in keep if c in df.columns]]
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce").dt.date
    for col in ["期限(年)","到期收益率(%)","当前收益率(%)"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].replace("---", pd.NA), errors="coerce")
    df = df.drop_duplicates(subset=["日期","期限(年)"]).sort_values(["日期","期限(年)"]).reset_index(drop=True)
    return df

def main(start_date_str: str, end_date_str: str, params: dict = None, outfile_stub: str = "cls_yield_curve"):
    params = params or DEFAULT_PARAMS
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    if end_dt < start_dt:
        raise ValueError("END_DATE 不能早于 START_DATE")

    frames = []
    for i, (a, b) in enumerate(daterange_chunks(start_dt, end_dt, 10), start=1):
        ra, rb = a.strftime("%Y-%m-%d"), b.strftime("%Y-%m-%d")
        print(f"[{i}] Fetch {ra} ~ {rb}")
        recs, _meta = fetch_once(ra, rb, params)
        df = to_dataframe(recs)
        print(f"  -> {len(df)} rows")
        frames.append(df)

    if not frames:
        print("No data.")
        return

    full = pd.concat(frames, ignore_index=True)
    full = full.drop_duplicates(subset=["日期","期限(年)"]).sort_values(["日期","期限(年)"]).reset_index(drop=True)

    csv_path = f"{outfile_stub}_{start_date_str}_to_{end_date_str}.csv"
    json_path = f"{outfile_stub}_{start_date_str}_to_{end_date_str}.json"
    full.to_csv(csv_path, index=False, encoding="utf-8-sig")
    full.to_json(json_path, orient="records", force_ascii=False)
    print(f"Done. Rows={len(full)}\nCSV : {csv_path}\nJSON: {json_path}")

if __name__ == "__main__":
    main(START_DATE, END_DATE)
