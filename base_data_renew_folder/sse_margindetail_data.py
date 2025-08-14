# sse_margindetail_data.py
# 上交所 融资融券 明细数据收集（Excel，sheet=明细信息）
# 来源： https://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygkYYYYMMDD.xls

import io
import re
import time
import json
import random
import sqlite3
import requests
import pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ───────── 配置 ─────────
INITIAL_START = "2010-03-31"         # 当DB无记录时起始日
END_DATE = "2025-08-14"              # 终止日（你可改或外部传入）
DB_PATH = "../api/data/sse_tab2.sqlite"
SLEEP_BASE, SLEEP_JITTER = 5, 10    # 休眠基数+抖动（秒）

URL_TPL = "https://www.sse.com.cn/market/dealingdata/overview/margin/a/rzrqjygk{yyyymmdd}.xls"

HEADERS = {
    "Referer": "https://www.sse.com.cn/",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/138.0.0.0 Safari/537.36"),
    "Accept": "*/*",
    "Connection": "keep-alive",
}

# Excel列名 → 统一字段名
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

# 最终入库列顺序
COLS = [
    "date",                # yyyy-mm-dd
    "code", "name",
    "margin_balance", "margin_buy_amt", "margin_repay_amt",
    "short_qty", "short_sell_qty", "short_repay_qty",
]

def create_retry_session(total_retries=3, backoff_factor=0.3,
                         status_forcelist=(500, 502, 503, 504, 520, 521, 522)):
    s = requests.Session()
    retry = Retry(
        total=total_retries, read=total_retries, connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

def ensure_table(conn: sqlite3.Connection):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS tab2_data (
        date TEXT NOT NULL,          -- yyyy-mm-dd
        code TEXT NOT NULL,
        name TEXT,
        margin_balance    REAL,
        margin_buy_amt    REAL,
        margin_repay_amt  REAL,
        short_qty         REAL,
        short_sell_qty    REAL,
        short_repay_qty   REAL,
        PRIMARY KEY (date, code)
    ) WITHOUT ROWID
    """)
    conn.commit()

def get_last_date(conn: sqlite3.Connection):
    cur = conn.execute("SELECT MAX(date) FROM tab2_data")
    v = cur.fetchone()[0]
    return v  # 'YYYY-MM-DD' 或 None

def ymd_compact(d: datetime.date) -> str:
    return d.strftime("%Y%m%d")

def ymd_dash(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")

def parse_number(x):
    """把字符串数字（含千分位/空白）转float；空返回None。"""
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s or s in {"-", "—", "nan", "None"}:
        return None
    s = s.replace(",", "")
    # 一些xls可能用全角逗号
    s = s.replace("，", "")
    try:
        return float(s)
    except ValueError:
        # 极端情况可再清一遍非数字字符
        s2 = re.sub(r"[^\d.\-eE]", "", s)
        return float(s2) if s2 else None

def fetch_one_day(session: requests.Session, d: datetime.date):
    """下载当日xls并返回DataFrame（只含明细信息sheet），若无数据返回None。"""
    url = URL_TPL.format(yyyymmdd=ymd_compact(d))
    resp = session.get(url, headers=HEADERS, timeout=20)
    # 404/无文件/节假日：返回 None
    if resp.status_code != 200 or not resp.content:
        return None

    bio = io.BytesIO(resp.content)
    # 只读“明细信息”sheet，尽量保留原始列名类型
    try:
        df = pd.read_excel(bio, sheet_name="明细信息", dtype=str, engine="xlrd")
    except Exception:
        # 有时sheet名可能带空格或变体，兜底遍历匹配
        xls = pd.ExcelFile(bio, engine="xlrd")
        target = None
        for name in xls.sheet_names:
            if "明细" in name:
                target = name
                break
        if not target:
            return None
        df = pd.read_excel(xls, sheet_name=target, dtype=str)

    # 去除全空行、空列
    df = df.dropna(how="all").copy()
    if df.empty:
        return None

    # 只保留我们关心的列（若源头新增列不影响）
    keep_cols = [c for c in df.columns if c in RENAME_MAP]
    df = df[keep_cols].rename(columns=RENAME_MAP)

    # 若关键列缺失，认为无效
    if "code" not in df.columns or "name" not in df.columns:
        return None

    # 数值列清洗
    for num_col in ["margin_balance", "margin_buy_amt", "margin_repay_amt",
                    "short_qty", "short_sell_qty", "short_repay_qty"]:
        if num_col in df.columns:
            df[num_col] = df[num_col].map(parse_number)
        else:
            df[num_col] = None

    # 添加 date 列
    df["date"] = ymd_dash(d)

    # 统一列顺序
    for col in COLS:
        if col not in df.columns:
            df[col] = None
    df = df[COLS].copy()

    # code 去空白
    df["code"] = df["code"].astype(str).str.strip()

    # 去除 code 为空的行
    df = df[df["code"].ne("")]

    return df

def upsert_df(conn: sqlite3.Connection, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    rows = [tuple(x) for x in df.itertuples(index=False, name=None)]
    cur = conn.cursor()
    cur.executemany("""
    INSERT INTO tab2_data
      (date, code, name, margin_balance, margin_buy_amt, margin_repay_amt,
       short_qty, short_sell_qty, short_repay_qty)
    VALUES (?,?,?,?,?,?,?,?,?)
    ON CONFLICT(date, code) DO UPDATE SET
      name=excluded.name,
      margin_balance=excluded.margin_balance,
      margin_buy_amt=excluded.margin_buy_amt,
      margin_repay_amt=excluded.margin_repay_amt,
      short_qty=excluded.short_qty,
      short_sell_qty=excluded.short_sell_qty,
      short_repay_qty=excluded.short_repay_qty
    """, rows)
    conn.commit()
    return len(rows)

def run(end_date_str=END_DATE, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    ensure_table(conn)

    last = get_last_date(conn)
    if last:
        start_date = datetime.strptime(last, "%Y-%m-%d").date() + timedelta(days=1)
    else:
        start_date = datetime.strptime(INITIAL_START, "%Y-%m-%d").date()

    end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
    if start_date > end_date:
        print(f"数据库已包含至 {last}，无需更新。")
        conn.close()
        return

    session = create_retry_session()

    cur_date = start_date
    while cur_date <= end_date:
        ds = ymd_dash(cur_date)
        print(f"[{ds}] 下载解析…", end="", flush=True)
        try:
            df = fetch_one_day(session, cur_date)
        except requests.exceptions.RequestException as e:
            print(f" 请求失败：{e}，重试下一天")
            # 不终止全局任务，继续下一天
            cur_date += timedelta(days=1)
            time.sleep(2)
            continue
        except Exception as e:
            print(f" 解析失败：{e}，跳过")
            cur_date += timedelta(days=1)
            time.sleep(1)
            continue

        if df is None or df.empty:
            print(" 无数据")
        else:
            n = upsert_df(conn, df)
            print(f" OK，入库 {n} 行")

        # 间隔
        time.sleep(SLEEP_BASE + SLEEP_JITTER * random.random())
        cur_date += timedelta(days=1)

    conn.close()
    print("完成。")

if __name__ == "__main__":
    run(end_date_str=END_DATE, db_path=DB_PATH)
