# szse_margindetail_data.py
# 融资融券交易明细 - 深交所 - 数据收集 (tab2 via XLSX)
import random
import sys
import requests
import sqlite3
import time
from io import BytesIO
import pandas as pd
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 默认开始抓取日期（当数据库无记录时使用）
INITIAL_START = '2010-05-04'

# 可按需补充请求头（一般 UA + Referer 足够）
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                  ' AppleWebKit/537.36 (KHTML, like Gecko)'
                  ' Chrome/138.0.0.0 Safari/537.36',
    'Referer': 'https://www.szse.cn/disclosure/margin/margin/index.html',
}

# 创建带重试机制的 Session
def create_retry_session(total_retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504)):
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    return session

def _to_float(x):
    if x is None:
        return None
    s = str(x).strip().replace(',', '')
    if s == '' or s.lower() in ('nan', 'none'):
        return None
    try:
        return float(s)
    except Exception:
        return None

def fetch_tab2(end_date, db_path, fail_streak=0):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # 1) 建表（若不存在）
    cur.execute('''
    CREATE TABLE IF NOT EXISTS tab2_data (
        date TEXT,
        zqdm TEXT,                  -- 证券代码
        zqjc TEXT,                  -- 证券简称
        margin_buy_amt REAL,        -- 融资买入额(元)
        margin_balance REAL,        -- 融资余额(元)
        short_sell_qty REAL,        -- 融券卖出量(股/份)
        short_qty REAL,             -- 融券余量(股/份)
        short_value REAL,           -- 融券余额(元)
        marginnshort_total REAL,    -- 融资融券余额(元)
        PRIMARY KEY (date, zqdm)
    );
    ''')
    conn.commit()

    # 2) 读数据库里已有的最晚日期 → 下一天；与 INITIAL_START 取较晚值
    cur.execute("SELECT MAX(date) FROM tab2_data")
    last = cur.fetchone()[0]
    if last:
        candidate = datetime.strptime(last, '%Y-%m-%d').date() + timedelta(days=1)
        init_start = datetime.strptime(INITIAL_START, '%Y-%m-%d').date()
        start_date = max(candidate, init_start)
    else:
        start_date = datetime.strptime(INITIAL_START, '%Y-%m-%d').date()

    if start_date > end_date:
        print(f"数据库已包含到 {last}，无需更新。")
        conn.close()
        return

    # 3) 循环抓取 XLSX
    session = create_retry_session()
    cur_date = start_date

    while cur_date <= end_date:
        # 节流：25~35 秒
        time.sleep(25 + 10 * random.random())
        ds = cur_date.strftime('%Y-%m-%d')

        # XLSX 接口（tab2）
        url = (
            "https://www.szse.cn/api/report/ShowReport"
            f"?SHOWTYPE=xlsx&CATALOGID=1837_xxpl&txtDate={ds}"
            "&tab2PAGENO=1&TABKEY=tab2"
        )
        try:
            resp = session.get(url, headers=HEADERS, timeout=20)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"[{ds}] 请求失败：{e}，稍后重试")
            fail_streak += 1
            if fail_streak >= 5:
                print("连续失败次数达到 5，程序退出。")
                conn.close()
                sys.exit(1)
            time.sleep(5)
            continue

        if not resp.content:
            print(f"Checking {ds}: records found = 0")
            print(f"[{ds}] XLSX 内容为空，跳过")
            cur_date += timedelta(days=1)
            continue

        try:
            df = pd.read_excel(BytesIO(resp.content), dtype=str)
        except Exception as e:
            print(f"[{ds}] 解析 XLSX 失败：{e}，跳过")
            cur_date += timedelta(days=1)
            continue

        if df is None or df.empty:
            print(f"Checking {ds}: records found = 0")
            print(f"[{ds}] 解析结果为空，跳过")
            cur_date += timedelta(days=1)
            continue

        # 列名清洗（去空格）
        df.columns = [str(c).strip() for c in df.columns]

        # 期望列（官方表头）
        # 证券代码 证券简称 融资买入额(元) 融资余额(元) 融券卖出量(股/份) 融券余量(股/份) 融券余额(元) 融资融券余额(元)
        expected = [
            '证券代码', '证券简称',
            '融资买入额(元)', '融资余额(元)',
            '融券卖出量(股/份)', '融券余量(股/份)',
            '融券余额(元)', '融资融券余额(元)'
        ]
        missing = [c for c in expected if c not in df.columns]
        if missing:
            print(f"[{ds}] 表头缺失列：{missing}，跳过")
            cur_date += timedelta(days=1)
            continue

        count = len(df)
        print(f"Checking {ds}: records found = {count}")

        # 数字列转换
        df['_date'] = ds
        df['_margin_buy_amt']   = df['融资买入额(元)'].map(_to_float)
        df['_margin_balance']   = df['融资余额(元)'].map(_to_float)
        df['_short_sell_qty']   = df['融券卖出量(股/份)'].map(_to_float)
        df['_short_qty']        = df['融券余量(股/份)'].map(_to_float)
        df['_short_value']      = df['融券余额(元)'].map(_to_float)
        df['_marginnshort_total']= df['融资融券余额(元)'].map(_to_float)

        # 入库（逐行或 executemany）
        rows = [
            (
                ds,
                r['证券代码'],
                r['证券简称'],
                r['_margin_buy_amt'],
                r['_margin_balance'],
                r['_short_sell_qty'],
                r['_short_qty'],
                r['_short_value'],
                r['_marginnshort_total'],
            )
            for _, r in df.iterrows()
        ]
        cur.executemany('''
            INSERT OR REPLACE INTO tab2_data
              (date, zqdm, zqjc, margin_buy_amt, margin_balance,
               short_sell_qty, short_qty, short_value, marginnshort_total)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', rows)
        conn.commit()

        print(f"[{ds}] 已抓取并存储 {count} 条明细")
        cur_date += timedelta(days=1)

    conn.close()

if __name__ == '__main__':
    # 终止日期（可改）
    end_date = datetime.strptime('2025-08-15', '%Y-%m-%d').date()
    # 输出到 tab2 的独立库
    fetch_tab2(end_date, '../api/data/szse_tab2.sqlite')
