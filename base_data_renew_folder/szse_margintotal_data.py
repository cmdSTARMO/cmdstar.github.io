# szse_margintotal_data.py
# 融资融券交易总量数据 - 深交所 - 数据收集
# Shenzhen Stock Exchange Margin Trading Data
import random
import sys

import requests
import sqlite3
import time
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# JSON key → 新字段名 映射
FIELD_MAP = {
    'jrrzye': 'margin_balance',       # 融资余额(元)
    'jrrzmr': 'margin_buy_amt',       # 融资买入额(元)
    'jrrjyl': 'short_qty',            # 融券余量
    'jrrjye': 'short_value',          # 融券余量金额(元)
    'jrrjmc': 'short_sell_qty',       # 融券卖出量
    'jrrzrjye': 'marginnshort_total', # 融资融券余额(元)
}

# 默认开始抓取日期（当数据库无记录时使用）
INITIAL_START = '2010-05-04'

# HTTP 请求头
HEADERS = {
    # 'Accept': 'application/json, text/javascript, */*; q=0.01',
    # 'Accept-Encoding': 'gzip, deflate, br, zstd',
    # 'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    # 'Cache-Control': 'no-cache',
    # 'Connection': 'keep-alive',
    # 'Content-Type': 'application/json',
    # 'Host': 'www.szse.cn',
    # 'Pragma': 'no-cache',
    # 'Referer': 'https://www.szse.cn/disclosure/margin/margin/index.html',
    # 'Sec-CH-UA': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    # 'Sec-CH-UA-Mobile': '?0',
    # 'Sec-CH-UA-Platform': '"Windows"',
    # 'Sec-Fetch-Dest': 'empty',
    # 'Sec-Fetch-Mode': 'cors',
    # 'Sec-Fetch-Site': 'same-origin',
    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
    #               '(KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    # 'X-Request-Type': 'ajax',
    # 'X-Requested-With': 'XMLHttpRequest',
}
a = 0
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


def fetch_tab1(end_date, db_path, a = a):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    # 1. 建表（如果不存在）
    cur.execute('''
    CREATE TABLE IF NOT EXISTS tab1_data (
        date TEXT PRIMARY   KEY,
        margin_balance      REAL,
        margin_buy_amt      REAL,
        short_qty           REAL,
        short_value         REAL,
        short_sell_qty      REAL,
        marginnshort_total  REAL
    );
    ''')
    conn.commit()

    # 2. 读数据库里已有的最晚日期
    cur.execute("SELECT MAX(date) FROM tab1_data")
    last = cur.fetchone()[0]
    if last:
        db_last = datetime.strptime(last, '%Y-%m-%d').date()
        candidate = db_last + timedelta(days=1)
        init_start = datetime.strptime(INITIAL_START, '%Y-%m-%d').date()
        start_date = max(candidate, init_start)
    else:
        start_date = datetime.strptime(INITIAL_START, '%Y-%m-%d').date()

    if start_date > end_date:
        print(f"数据库已包含到 {last}，无需更新。")
        conn.close()
        return

    # 3. 按天拉取
    session = create_retry_session()
    cur_date = start_date
    while cur_date <= end_date:
        time.sleep(25+10*random.random())
        ds = cur_date.strftime('%Y-%m-%d')
        # 打印当前进度
        # Checking YYYY-MM-DD: records found = count
        session = session  # noqa: F841
        url = (
            "https://www.szse.cn/api/report/ShowReport/data"
            "?SHOWTYPE=JSON&CATALOGID=1837_xxpl"
            f"&txtDate={ds}"
        )
        try:
            resp = session.get(url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"[{ds}] 请求失败：{e}，稍后重试")
            time.sleep(5)
            a += 1
            if a == 5:
                sys.exit()
            continue

        body = resp.json()
        tab1 = next((x for x in body if x['metadata']['tabkey']=='tab1'), None)
        if not tab1 or not tab1.get('data'):
            print(f"Checking {ds}: records found = 0")
            print(f"[{ds}] 未返回 tab1 数据或 data 为空，跳过")
            cur_date += timedelta(days=1)
            continue

        # 记录找到的数据条数
        count = len(tab1['data'])
        print(f"Checking {ds}: records found = {count}")

        row = tab1['data'][0]
        # 清洗并按映射转换
        vals = {FIELD_MAP[k]: float(v.replace(',', '')) for k, v in row.items() if k in FIELD_MAP}

        # 入库
        cur.execute('''
        INSERT OR REPLACE INTO tab1_data
          (date, margin_balance, margin_buy_amt, short_qty, short_value, short_sell_qty, marginnshort_total)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            ds,
            vals['margin_balance'],
            vals['margin_buy_amt'],
            vals['short_qty'],
            vals['short_value'],
            vals['short_sell_qty'],
            vals['marginnshort_total'],
        ))
        conn.commit()

        print(f"[{ds}] 已抓取并存入一条记录")
        cur_date += timedelta(days=1)
    conn.close()

if __name__ == '__main__':
    # 配置抓取终止日
    end_date = datetime.strptime('2025-08-10', '%Y-%m-%d').date()
    fetch_tab1(end_date, '../api/data/szse_tab1.sqlite')