import time
import requests

URL = "https://api.huangdapao.com/szse_etf_shares/shares?startdate=2025-07-18&enddate=2026-07-25&offset=0"

def fetch_data():
    try:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()  # 抛出HTTP错误（如果有）
        data = response.json()
        print("成功获取数据，记录数：", len(data.get("data", [])))
    except Exception as e:
        print("请求失败：", e)

if __name__ == "__main__":
    while True:
        print("开始请求：", time.strftime("%Y-%m-%d %H:%M:%S"))
        fetch_data()
        time.sleep(600)  # 每10分钟执行一次（600秒）
