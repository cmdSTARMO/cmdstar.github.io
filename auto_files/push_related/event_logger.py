import os, csv, uuid
from datetime import datetime

# 固定到本文件所在目录下的 data/ 目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EVENTS_DIR  = os.path.join(BASE_DIR, "data")
EVENTS_FILE = os.path.join(EVENTS_DIR, "push_events.csv")
os.makedirs(EVENTS_DIR, exist_ok=True)

FIELDNAMES = [
    "id", "timestamp", "related_subject",
    "large_status", "report_title",
    "report_details", "push_status"
]

def init_events_file():
    if not os.path.exists(EVENTS_FILE):
        with open(EVENTS_FILE, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()

def log_push_event(related_subject, report_title, report_details, large_status="成功推送"):
    print(f"[DEBUG] Logging to {EVENTS_FILE}")
    init_events_file()
    event_id = uuid.uuid4().hex
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row = {
        "id": event_id,
        "timestamp": timestamp,
        "related_subject": related_subject,
        "large_status": large_status,
        "report_title": report_title,
        "report_details": report_details,
        "push_status": "未推送"
    }
    with open(EVENTS_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writerow(row)
    return event_id

def update_push_status(event_id, push_status, large_status=None):
    init_events_file()
    rows, updated = [], False
    with open(EVENTS_FILE, mode="r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r["id"] == event_id:
                r["push_status"] = push_status
                if large_status:
                    r["large_status"] = large_status
                updated = True
            rows.append(r)
    if not updated:
        raise KeyError(f"Event ID {event_id} not found.")
    with open(EVENTS_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
