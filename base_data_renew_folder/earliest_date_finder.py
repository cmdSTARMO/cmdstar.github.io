# code3_find_earliest.py
import time
import requests
import pandas as pd
from datetime import datetime, timedelta

def find_earliest_date(
    end_date_str: str,
    output_csv: str = 'earliest_available.csv',
    max_empty_days: int = 60,
    max_retries: int = 3
):
    """
    从 end_date_str 开始，向前遍历 SZSE tab1 JSON 数据，记录每个日期的返回条数。
    • headers 伪装浏览器请求
    • 遇到网络错误重试 max_retries 次
    • 一旦连续 max_empty_days 天都返回 0 条，停止遍历
    • 把所有记录写入 output_csv
    """
    base_url = (
        "https://www.szse.cn/api/report/ShowReport/data"
        "?SHOWTYPE=JSON&CATALOGID=1837_xxpl&txtDate="
    )
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                      ' AppleWebKit/537.36 (KHTML, like Gecko)'
                      ' Chrome/115.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': 'https://www.szse.cn/',
    }
    session = requests.Session()
    session.headers.update(headers)

    end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    curr_date = end_date
    empty_streak = 0
    records = []

    while empty_streak < max_empty_days:
        ds = curr_date.strftime('%Y-%m-%d')
        url = base_url + ds
        count = 0

        # 网络请求并重试
        for attempt in range(1, max_retries + 1):
            try:
                resp = session.get(url, timeout=10)
                resp.raise_for_status()
                body = resp.json()
                tab1 = next((x for x in body if x['metadata']['tabkey']=='tab1'), None)
                if tab1 and tab1.get('data'):
                    count = len(tab1['data'])
                else:
                    count = 0
                break  # 成功拿到数据就跳出重试循环
            except requests.exceptions.RequestException as e:
                print(f"[{ds}] 网络错误，第 {attempt} 次尝试: {e}")
                if attempt < max_retries:
                    time.sleep(2 ** attempt)
                else:
                    print(f"[{ds}] 达到最大重试次数，视为无数据")
                    count = 0

        # 打印当前进度
        print(f"Checking {ds}: records found = {count}")
        records.append({'date': ds, 'count': count})

        # 更新连续无数据计数
        if count == 0:
            empty_streak += 1
        else:
            empty_streak = 0

        curr_date -= timedelta(days=1)

    # 保存到 CSV
    df = pd.DataFrame(records)
    df.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"遍历结束，共检查了 {len(records)} 天。结果已保存到 {output_csv}")

if __name__ == '__main__':
    # 从 2025-08-05 开始向前找，或自定义结束日期
    find_earliest_date('2014-10-06')
