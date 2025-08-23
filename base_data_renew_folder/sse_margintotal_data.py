# sse_margintotal_data.py
# 融资融券交易总量数据 - 上交所 - 数据收集
# Shanghai Stock Exchange Margin Trading Totals (RZRQ_HZ_INFO)

import random
import sys
import re
import json
import time
import sqlite3
import requests
from datetime import datetime, timedelta
from urllib.parse import urlencode
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# -------- 配置 --------
INITIAL_START = '2010-03-31'   # 当 DB 无记录时的起始日（可改）
DB_PATH = '../api/data/sse_tab1.sqlite'
WINDOW_DAYS = 1000            # 每轮抓取的日期窗口上限
SLEEP_BASE, SLEEP_JITTER = 25, 10

HEADERS = {
    'Referer': 'https://www.sse.com.cn/',
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) '
                   'Chrome/138.0.0.0 Safari/537.36'),
    'Accept': '*/*',
    'Connection': 'keep-alive',
}

FIELD_MAP = {
    'rzye': 'margin_balance',        # 融资余额(元)
    'rzmre': 'margin_buy_amt',       # 融资买入额(元)
    'rzche': 'margin_repay_amt',     # 融资偿还额(元) - SSE独有
    'rqyl': 'short_qty',             # 融券余量(股/份)
    'rqmcl': 'short_sell_qty',       # 融券卖出量(股/份)
    'rqylje': 'short_value',         # 融券余额(元)
    'rzrqjyzl': 'marginnshort_total',# 融资融券余额(元) = rzye + rqylje
    'opDate': 'date',                # 日期 YYYYMMDD
}

def create_retry_session(total_retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504, 520, 521, 522)):
    s = requests.Session()
    retry = Retry(
        total=total_retries,
        read=total_retries,
        connect=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount('https://', adapter)
    s.mount('http://', adapter)
    return s

def parse_jsonp(text: str):
    # 去掉 jsonpCallbackxxxxx( ... )
    m = re.search(r'^\s*[\w$]+\((.*)\)\s*$', text, flags=re.S)
    if not m:
        # 兼容直接 JSON（极少数情况）
        return json.loads(text)
    return json.loads(m.group(1))

def ensure_table(conn: sqlite3.Connection):
    conn.execute('''
    CREATE TABLE IF NOT EXISTS tab1_data (
        date TEXT PRIMARY KEY,
        margin_balance      REAL,
        margin_buy_amt      REAL,
        margin_repay_amt    REAL,
        short_qty           REAL,
        short_sell_qty      REAL,
        short_value         REAL,
        marginnshort_total  REAL
    )
    ''')
    conn.commit()

def get_last_date(conn: sqlite3.Connection):
    cur = conn.execute("SELECT MAX(date) FROM tab1_data")
    v = cur.fetchone()[0]
    return v  # 'YYYY-MM-DD' or None

def ymd_compact(d: datetime.date) -> str:
    return d.strftime('%Y%m%d')

def ymd_dash(d: datetime.date) -> str:
    return d.strftime('%Y-%m-%d')

def build_url(begin_ymd: str, end_ymd: str, page_no: int, page_size: int = 5000) -> str:
    base = "https://query.sse.com.cn/commonSoaQuery.do"
    q = {
        "jsonCallBack": f"jsonpCallback{random.randint(10000000,99999999)}",
        "isPagination": "true",
        "pageHelp.pageSize": str(page_size),
        "pageHelp.pageNo": str(page_no),
        "pageHelp.beginPage": "1",
        "pageHelp.cacheSize": "1",
        "pageHelp.endPage": str(page_no),
        "stockCode": "",
        "beginDate": begin_ymd,
        "endDate": end_ymd,
        "sqlId": "RZRQ_HZ_INFO",
        "_": str(int(time.time()*1000))
    }
    return f"{base}?{urlencode(q)}"

def fetch_window(session: requests.Session, begin_d, end_d):
    begin_ymd = ymd_compact(begin_d)
    end_ymd = ymd_compact(end_d)

    all_rows = []
    page_no = 1
    while True:
        url = build_url(begin_ymd, end_ymd, page_no=page_no)
        resp = session.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = parse_jsonp(resp.text)

        page = data.get('pageHelp', {})
        rows = page.get('data') or data.get('result') or []
        total = page.get('total') or len(rows) or 0
        page_size = page.get('pageSize') or 25

        # 规范化：只保留我们需要的字段
        for r in rows:
            out = {}
            for k, v in r.items():
                if k in FIELD_MAP:
                    out[FIELD_MAP[k]] = v
            # 日期转 yyyy-mm-dd
            if 'date' in out:
                d = out['date']
                # d 形如 '20100402'
                out['date'] = f"{d[0:4]}-{d[4:6]}-{d[6:8]}"
            all_rows.append(out)

        # 翻页判断
        got = page_no * (page.get('pageSize') or 25)
        if got >= total or not rows:
            break
        page_no += 1
        time.sleep(0.3)  # 页内短暂间隔，避免过快

    # 去重（同日只留一条，保留最后出现的）
    uniq = {}
    for r in all_rows:
        if 'date' in r:
            uniq[r['date']] = r

    # 排序：旧 → 新
    result = list(uniq.values())
    result.sort(key=lambda r: r['date'])
    return result

def upsert_rows(conn: sqlite3.Connection, rows):
    if not rows:
        return 0
    cur = conn.cursor()
    for r in rows:
        # 将缺失字段补 None，避免 KeyError
        row = {
            'date': r.get('date'),
            'margin_balance': r.get('margin_balance'),
            'margin_buy_amt': r.get('margin_buy_amt'),
            'margin_repay_amt': r.get('margin_repay_amt'),
            'short_qty': r.get('short_qty'),
            'short_sell_qty': r.get('short_sell_qty'),
            'short_value': r.get('short_value'),
            'marginnshort_total': r.get('marginnshort_total'),
        }
        cur.execute('''
        INSERT INTO tab1_data
          (date, margin_balance, margin_buy_amt, margin_repay_amt, short_qty, short_sell_qty, short_value, marginnshort_total)
        VALUES (:date, :margin_balance, :margin_buy_amt, :margin_repay_amt, :short_qty, :short_sell_qty, :short_value, :marginnshort_total)
        ON CONFLICT(date) DO UPDATE SET
          margin_balance=excluded.margin_balance,
          margin_buy_amt=excluded.margin_buy_amt,
          margin_repay_amt=excluded.margin_repay_amt,
          short_qty=excluded.short_qty,
          short_sell_qty=excluded.short_sell_qty,
          short_value=excluded.short_value,
          marginnshort_total=excluded.marginnshort_total
        ''', row)
    conn.commit()
    return len(rows)

def fetch_totals(end_date_str: str, db_path: str = DB_PATH):
    conn = sqlite3.connect(db_path)
    ensure_table(conn)

    last = get_last_date(conn)  # 'YYYY-MM-DD' or None
    if last:
        start_date = datetime.strptime(last, '%Y-%m-%d').date() + timedelta(days=1)
    else:
        start_date = datetime.strptime(INITIAL_START, '%Y-%m-%d').date()

    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    if start_date > end_date:
        print(f"数据库已包含至 {last}，无需更新。")
        conn.close()
        return

    session = create_retry_session()
    cur_start = start_date
    while cur_start <= end_date:
        cur_end = min(cur_start + timedelta(days=WINDOW_DAYS - 1), end_date)
        print(f"[WINDOW] {ymd_dash(cur_start)} → {ymd_dash(cur_end)}  正在抓取…")

        try:
            rows = fetch_window(session, cur_start, cur_end)
            n = upsert_rows(conn, rows)
            print(f"  完成：{len(rows)} 行，入库/更新 {n} 行。")
        except requests.exceptions.RequestException as e:
            print(f"  请求异常：{e}  将重试该窗口")
            time.sleep(5)
            continue

        # 间隔
        time.sleep(SLEEP_BASE + SLEEP_JITTER * random.random())
        cur_start = cur_end + timedelta(days=1)

    conn.close()
    print("全部完成。")

if __name__ == '__main__':
    # 与你的 SZSE 脚本一致的调用方式
    fetch_totals(end_date_str='2025-08-23', db_path=DB_PATH)
