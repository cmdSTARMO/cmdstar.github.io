import os
import sys
import io
import re
import random
import logging
import sqlite3
import requests
import pandas as pd

from auto_files.push_related.event_logger import log_push_event

# ------- 脚本目录 & 数据库路径 -------
# 当前脚本的路径
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# 目标数据库目录（假设 data 被放到 api/ 下）
DB_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, "api", "data"))

# 拼接最终数据库路径
DB_PATH = os.path.join(DB_DIR, "SZSE_ETF_vol.sqlite")


# ------- 配置 -------
JSON_URL    = "https://www.szse.cn/api/report/ShowReport/data"
XLSX_URL    = "https://www.szse.cn/api/report/ShowReport?SHOWTYPE=xlsx&CATALOGID=1945&tab1PAGENO=1&TABKEY=tab1"
CATALOGID   = "1945"
SAMPLE_SIZE = 5
LOG_LEVEL   = logging.INFO

# 浏览器伪装 + 重试机制
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/115.0.0.0 Safari/537.36"
    )
}
# 超时：connect 5s，read 30s
TIMEOUT = (5, 30)

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retries = Retry(
    total=3,
    backoff_factor=2,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["GET", "POST"]
)
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))

# ------- 日志 -------
logging.basicConfig(level=LOG_LEVEL,
                    format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# ------- 数据抓取函数 -------

def fetch_json():
    """从 JSON 接口拉取元数据与列表，重试 + 超时，网络异常转为 RuntimeError"""
    try:
        resp = session.get(
            JSON_URL,
            params={"SHOWTYPE": "JSON", "CATALOGID": CATALOGID},
            headers=HEADERS,
            timeout=TIMEOUT
        )
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"JSON 拉取失败: {e}")
    try:
        payload = resp.json()[0]
        return payload['metadata']['subname'], payload['data']
    except Exception as e:
        raise RuntimeError(f"JSON 解析失败: {e}")

def fetch_xlsx():
    """下载并解析 XLSX 文件，重试 + 超时，网络异常转为 RuntimeError"""
    try:
        resp = session.get(XLSX_URL, headers=HEADERS, timeout=TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"XLSX 下载失败: {e}")
    try:
        return pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")
    except Exception as e:
        raise RuntimeError(f"XLSX 解析失败: {e}")

def clean_sys_key(html_str: str) -> str:
    return re.sub(r'<.*?>', '', html_str).strip()

def validate_sample(json_sample, xlsx_df):
    for entry in json_sample:
        code = clean_sys_key(entry['sys_key'])
        json_val = float(entry['dqgm'].replace(',', '')) * 10000
        xlsx_row = xlsx_df[xlsx_df['证券代码'].astype(str) == code]
        if xlsx_row.empty:
            raise RuntimeError(f"样本校验失败：代码 {code} 未在 XLSX 中找到")
        xlsx_val = float(str(xlsx_row.iloc[0]['当前规模(份)']).replace(',', ''))
        if int(json_val/100) != int(xlsx_val/100):
            raise RuntimeError(f"样本校验失败：{code} JSON≈{int(json_val/100)}00 vs XLSX≈{int(xlsx_val/100)}00")
    logger.info("Sample validation passed.")

# ------- 数据库存取 -------

def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS etf_data (
            dt TEXT NOT NULL,
            code TEXT NOT NULL,
            name TEXT,
            index_code TEXT,
            size REAL,
            manager TEXT,
            PRIMARY KEY(dt, code)
        )
    """)
    conn.commit()

def record_exists(conn, dt: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM etf_data WHERE dt=? LIMIT 1", (dt,))
    return cur.fetchone() is not None

def insert_all(conn, dt: str, xlsx_df: pd.DataFrame) -> int:
    cur = conn.cursor()
    count = 0
    for _, row in xlsx_df.iterrows():
        code = str(row['证券代码']).strip()
        size = float(str(row['当前规模(份)']).replace(',', ''))
        cur.execute(
            "INSERT OR IGNORE INTO etf_data(dt, code, name, index_code, size, manager) VALUES (?, ?, ?, ?, ?, ?)",
            (dt,
             code,
             str(row['证券简称']).strip(),
             str(row['拟合指数']).strip(),
             size,
             str(row['基金管理人']).strip())
        )
        if cur.rowcount:
            count += 1
    conn.commit()
    return count

# ------- 主流程 -------

if __name__ == "__main__":
    error_happened = False
    try:
        dt, json_data = fetch_json()
        xlsx_df       = fetch_xlsx()
        validate_sample(
            random.sample(json_data, min(SAMPLE_SIZE, len(json_data))),
            xlsx_df
        )

        conn = sqlite3.connect(DB_PATH)
        init_db(conn)
        if record_exists(conn, dt):
            logger.info(f"{dt} already processed.")
        else:
            new_count = insert_all(conn, dt, xlsx_df)
            # 正常记录一条“成功”事件
            evt = log_push_event(
                related_subject="深交所ETF规模更新",
                report_title=f"{dt} SZSE ETF 规模数据更新成功",
                report_details=f"本次共插入 {new_count} 条记录",
                large_status="成功推送"
            )
            logger.info(f"Logged success event {evt}")
    except Exception as e:
        error_happened = True
        # 只记录，不退出
        evt = log_push_event(
            related_subject="深交所ETF规模更新",
            report_title="深交所ETF规模数据 更新异常",
            report_details=str(e),
            large_status="异常推送"
        )
        logger.exception("捕获异常，记录错误事件： %s", evt)

    # 最终返回 0，让后续发送脚本也能跑~~
    # 如果你想失败时也让 CI 标红，可改成 sys.exit(1) 但要把发送那步和提交移到另一个 Job
    sys.exit(0)
