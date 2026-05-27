import csv
import os
import re
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import requests


AUTOMATION_DIR = Path(__file__).resolve().parent
REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = AUTOMATION_DIR / "logs"
LOG_FILE = LOG_DIR / "fetcher_daily_runs.csv"

TIMEOUT_SECONDS = int(os.getenv("FETCHER_TIMEOUT_SECONDS", "7200"))
FEISHU_WEBHOOK_URL = os.getenv("FEISHU_WEBHOOK_URL", "").strip()
FETCHER_RETRY_SLEEP_SECONDS = os.getenv("FETCHER_RETRY_SLEEP_SECONDS", "5")

RETRY_SLEEP_ENV_KEYS = [
    "SW_INDUSTRY_DAILY_RETRY_SLEEP",
    "SOUTHBOUND_NETBUY_RETRY_SLEEP",
    "NORTHBOUND_DEALAMT_RETRY_SLEEP",
    "FUND_NEW_ISSUE_RETRY_SLEEP",
    "RMB_FX_INDEX_RETRY_SLEEP",
    "SSE_ETF_SHARES_RETRY_SLEEP",
    "GLOBAL_MARKET_DAILY_RETRY_SLEEP",
    "ERP_HS300_10Y_RETRY_SLEEP",
]

FETCHERS = [
    {"name": "SZSE margin total", "script": "szse_margintotal_data.py"},
    {"name": "SSE margin total", "script": "sse_margintotal_data.py"},
    {"name": "SZSE margin detail", "script": "szse_margindetail_data.py"},
    {"name": "SSE margin detail", "script": "sse_margindetail_data.py"},
    {"name": "Global market daily", "script": "global_market_daily_fetcher.py"},
    {"name": "SW industry daily", "script": "sw_industry_daily_fetcher.py"},
    {"name": "SSE ETF shares", "script": "sse_etf_shares_fetcher.py"},
    {"name": "NCD AAA yield curve", "script": "ncd_aaa_yield_curve_fetcher.py"},
    {"name": "RMB FX index", "script": "rmb_fx_index_fetcher.py"},
    {"name": "Shibor curve", "script": "shibor_curve_fetcher.py"},
    {"name": "Southbound netbuy", "script": "southbound_netbuy_fetcher.py"},
    {"name": "Northbound deal amount", "script": "northbound_dealamt_fetcher.py"},
    {"name": "Fund new issue", "script": "fund_new_issue_fetcher.py"},
    {"name": "ERP HS300 10Y", "script": "erp_hs300_10y_fetcher.py"},
]

METADATA_UPDATER = {"name": "API showcase metadata", "script": "update_api_showcase_metadata.py"}

CSV_FIELDS = [
    "run_id",
    "run_date",
    "fetcher",
    "script",
    "status",
    "updated_rows",
    "exit_code",
    "started_at",
    "ended_at",
    "duration_seconds",
    "command",
    "error_excerpt",
    "stdout",
    "stderr",
]


def now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def append_csv(row: dict):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    exists = LOG_FILE.is_file()
    with LOG_FILE.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def compact_text(text: str, max_chars: int = 1200) -> str:
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n...[truncated]"


def parse_updated_rows(output: str) -> int:
    text = output or ""
    patterns = [
        r"Total parquet upserted\s+(\d+)\s+rows",
        r"all done\. Total parquet upserted\s+(\d+)\s+rows",
        r"sliced_rows=(\d+)",
        r"parquet upserted\s+(\d+)\s+rows",
        r"Parquet upserted\s+(\d+)\s+rows",
        r"snapshot rows=\d+\s+sliced rows=(\d+)",
    ]

    total_markers = []
    for pattern in patterns[:2]:
        matches = re.findall(pattern, text, flags=re.I)
        if matches:
            total_markers.extend(int(value) for value in matches)
    if total_markers:
        return total_markers[-1]

    sliced = re.findall(patterns[2], text, flags=re.I)
    if sliced:
        return int(sliced[-1])

    row_counts = []
    for pattern in patterns[3:]:
        row_counts.extend(int(value) for value in re.findall(pattern, text, flags=re.I))
    return sum(row_counts)


def run_fetcher(item: dict, run_id: str) -> dict:
    script = AUTOMATION_DIR / item["script"]
    started_at = now_text()
    start_ts = time.time()
    command = [sys.executable, str(script)]
    child_env = os.environ.copy()
    for key in RETRY_SLEEP_ENV_KEYS:
        child_env.setdefault(key, FETCHER_RETRY_SLEEP_SECONDS)

    if not script.is_file():
        ended_at = now_text()
        return {
            "run_id": run_id,
            "run_date": datetime.now().strftime("%Y-%m-%d"),
            "fetcher": item["name"],
            "script": item["script"],
            "status": "failed",
            "updated_rows": 0,
            "exit_code": -1,
            "started_at": started_at,
            "ended_at": ended_at,
            "duration_seconds": round(time.time() - start_ts, 2),
            "command": " ".join(command),
            "error_excerpt": f"script not found: {script}",
            "stdout": "",
            "stderr": "",
        }

    try:
        proc = subprocess.run(
            command,
            cwd=str(REPO_ROOT),
            env=child_env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=TIMEOUT_SECONDS,
        )
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        combined = stdout + "\n" + stderr
        status = "success" if proc.returncode == 0 else "failed"
        error_excerpt = "" if status == "success" else compact_text(stderr or stdout)
        updated_rows = parse_updated_rows(combined) if status == "success" else 0
        exit_code = proc.returncode
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        status = "failed"
        updated_rows = 0
        exit_code = -9
        error_excerpt = f"timeout after {TIMEOUT_SECONDS}s\n{compact_text(stderr or stdout)}"
    except Exception:
        stdout = ""
        stderr = traceback.format_exc()
        status = "failed"
        updated_rows = 0
        exit_code = -1
        error_excerpt = compact_text(stderr)

    ended_at = now_text()
    return {
        "run_id": run_id,
        "run_date": datetime.now().strftime("%Y-%m-%d"),
        "fetcher": item["name"],
        "script": item["script"],
        "status": status,
        "updated_rows": updated_rows,
        "exit_code": exit_code,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": round(time.time() - start_ts, 2),
        "command": " ".join(command),
        "error_excerpt": error_excerpt,
        "stdout": stdout,
        "stderr": stderr,
    }


def result_line(result: dict) -> str:
    icon = "✅" if result["status"] == "success" else "❌"
    if result["status"] == "success":
        return f"{icon} **{result['fetcher']}**：成功，更新/写入 {result['updated_rows']} 行"
    excerpt = compact_text(result.get("error_excerpt", ""), 260).replace("\n", " ")
    return f"{icon} **{result['fetcher']}**：失败，{excerpt}"


def send_feishu_card(results: list[dict], run_id: str, started_at: str, ended_at: str):
    if not FEISHU_WEBHOOK_URL:
        print("[run_all_fetchers] FEISHU_WEBHOOK_URL is not set; skip Feishu notification.")
        return

    success_count = sum(1 for item in results if item["status"] == "success")
    failed_count = len(results) - success_count
    total_rows = sum(int(item.get("updated_rows") or 0) for item in results if item["status"] == "success")
    title = "数据自动化运行完成" if failed_count == 0 else "数据自动化运行存在失败"
    color = "green" if failed_count == 0 else "red"

    details = "\n".join(result_line(item) for item in results)
    content = (
        f"**运行批次**：{run_id}\n"
        f"**开始时间**：{started_at}\n"
        f"**结束时间**：{ended_at}\n"
        f"**成功/失败**：{success_count}/{failed_count}\n"
        f"**成功写入/更新行数合计**：{total_rows}\n"
        f"**CSV 日志**：`{LOG_FILE}`\n\n"
        f"{details}"
    )

    payload = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": color,
                "title": {"tag": "plain_text", "content": title},
            },
            "elements": [
                {"tag": "markdown", "content": content},
            ],
        },
    }

    try:
        resp = requests.post(FEISHU_WEBHOOK_URL, json=payload, timeout=20)
        resp.raise_for_status()
        print("[run_all_fetchers] Feishu notification sent.")
    except Exception as exc:
        print(f"[run_all_fetchers] Feishu notification failed: {exc}")


def main():
    run_id = datetime.now().strftime("%Y%m%d%H%M%S")
    started_at = now_text()
    results = []

    print(f"[run_all_fetchers] run_id={run_id}")
    print(f"[run_all_fetchers] log_file={LOG_FILE}")

    for item in FETCHERS:
        print(f"[run_all_fetchers] start {item['name']} ({item['script']})")
        result = run_fetcher(item, run_id)
        append_csv(result)
        results.append(result)
        print(
            f"[run_all_fetchers] {item['name']} -> {result['status']}; "
            f"updated_rows={result['updated_rows']}; duration={result['duration_seconds']}s"
        )

    print(f"[run_all_fetchers] start {METADATA_UPDATER['name']} ({METADATA_UPDATER['script']})")
    metadata_result = run_fetcher(METADATA_UPDATER, run_id)
    append_csv(metadata_result)
    results.append(metadata_result)
    print(
        f"[run_all_fetchers] {METADATA_UPDATER['name']} -> {metadata_result['status']}; "
        f"duration={metadata_result['duration_seconds']}s"
    )

    ended_at = now_text()
    send_feishu_card(results, run_id, started_at, ended_at)

    failed = [item for item in results if item["status"] != "success"]
    print(f"[run_all_fetchers] done. success={len(results) - len(failed)}, failed={len(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
