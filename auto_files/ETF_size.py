import requests
import pandas as pd
import sqlite3
import random
import re
import io
import logging
import sys
from auto_files.push_related.event_logger import log_push_event
import os

# 当前脚本（ETF_size.py）所在目录
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 定义 data 目录：脚本的同级目录的上一级 data/
DB_DIR  = os.path.normpath(os.path.join(SCRIPT_DIR, os.pardir, "data"))
DB_PATH = os.path.join(DB_DIR, "SZSE_ETF_vol.sqlite")

# 确保 data 目录存在
os.makedirs(DB_DIR, exist_ok=True)

# -------------------- Configuration --------------------
JSON_URL    = "https://www.szse.cn/api/report/ShowReport/data"
XLSX_URL    = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1945&tab1PAGENO=1&TABKEY=tab1"
CATALOGID   = "1945"
SAMPLE_SIZE = 5
# DB_PATH     = ".../data/SZSE_ETF_vol.sqlite"
LOG_LEVEL   = logging.INFO

# **模拟浏览器的 User‑Agent**
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}

# -------------------- Logging Setup --------------------
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# -------------------- Functions --------------------

def fetch_json():
    params = {"SHOWTYPE": "JSON", "CATALOGID": CATALOGID}
    r = requests.get(JSON_URL, params=params, headers=HEADERS, timeout=15)
    r.raise_for_status()
    payload = r.json()[0]
    date = payload['metadata']['subname']
    return date, payload['data']


def fetch_xlsx():
    r = requests.get(XLSX_URL, headers=HEADERS, timeout=15)
    r.raise_for_status()
    return pd.read_excel(io.BytesIO(r.content), engine='openpyxl')


def clean_sys_key(html_str):
    return re.sub(r'<.*?>', '', html_str).strip()


def validate_sample(json_sample, xlsx_df):
    for entry in json_sample:
        code = clean_sys_key(entry['sys_key'])
        json_val = float(entry['dqgm'].replace(',', '')) * 10000
        xlsx_row = xlsx_df[xlsx_df['证券代码'].astype(str) == code]
        if xlsx_row.empty:
            raise ValueError(f"Code {code} not found in XLSX for validation.")
        xlsx_val = float(str(xlsx_row.iloc[0]['当前规模(份)']).replace(',', ''))
        if int(json_val/100) != int(xlsx_val/100):
            raise ValueError(f"Validation mismatch for {code}")
    logger.info("Sample validation passed.")


def init_db(conn):
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS etf_data (
            dt TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            index_code TEXT,
            size REAL,
            manager TEXT,
            PRIMARY KEY (dt, code)
        )
    """)
    conn.commit()


def record_exists(conn, dt):
    c = conn.cursor()
    c.execute("SELECT 1 FROM etf_data WHERE dt = ? LIMIT 1", (dt,))
    return c.fetchone() is not None


def insert_all(conn, dt, xlsx_df):
    c = conn.cursor()
    count_new = 0
    for _, row in xlsx_df.iterrows():
        code = str(row['证券代码']).strip()
        size = float(str(row['当前规模(份)']).replace(',', ''))
        c.execute(
            "INSERT OR IGNORE INTO etf_data (dt, code, name, index_code, size, manager) VALUES (?, ?, ?, ?, ?, ?)",
            (dt,
             code,
             str(row['证券简称']).strip(),
             str(row['拟合指数']).strip(),
             size,
             str(row['基金管理人']).strip())
        )
        if c.rowcount > 0:
            count_new += 1
    conn.commit()
    return count_new

# -------------------- Main --------------------
if __name__ == "__main__":
    try:
        dt, json_data = fetch_json()
        xlsx_df      = fetch_xlsx()
        validate_sample(random.sample(json_data, min(SAMPLE_SIZE, len(json_data))), xlsx_df)

        conn = sqlite3.connect(DB_PATH)
        init_db(conn)
        if record_exists(conn, dt):
            logger.info(f"{dt} already processed.")
            sys.exit(0)
        new_count = insert_all(conn, dt, xlsx_df)
        conn.close()

        # 记录推送事件（未推送）
        event_id = log_push_event(
            related_subject="深交所ETF规模更新",
            report_title=f"{dt} SZSE ETF 规模数据更新成功！",
            report_details=f"本次共插入 {new_count} 条记录",
            large_status="成功推送"
        )
        logger.info(f"Logged event {event_id}")

    except Exception as e:
        # 异常时也记录事件
        event_id = log_push_event(
            related_subject="深交所ETF规模更新",
            report_title="深交所ETF规模数据 更新异常 :(",
            report_details=str(e),
            large_status="异常推送"
        )
        logger.exception("Update failed.")
        sys.exit(1)
