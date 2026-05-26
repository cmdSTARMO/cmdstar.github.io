import os
import pathlib
from datetime import date, datetime

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("FUND_NEW_ISSUE_PQ_DIR", os.path.join(API_DIR, "data", "fund_new_issue_data"))
FNAME_TPL = "fund_new_issue_{yyyymm}.parquet"
PENDING_FILE = "fund_new_issue_pending.parquet"

COLUMNS_ZH = {
    "established_date": "成立日期",
    "fund_code": "基金代码",
    "fund_name": "基金简称",
    "fund_company": "发行公司",
    "company_id": "公司ID",
    "fund_type": "基金类型",
    "raised_shares": "募集份额",
    "unknown_1": "未知1",
    "fund_manager": "基金经理",
    "subscription_status": "申购状态",
    "subscription_period": "集中认购期",
    "unknown_2": "未知2",
    "unknown_3": "未知3",
    "fund_company_2": "发行公司2",
    "unknown_4": "未知4",
    "unknown_5": "未知5",
    "unknown_6": "未知6",
    "fund_manager_id": "基金经理ID",
    "discount_rate": "优惠费率",
    "snapshot_dt": "快照日期",
}

COLUMNS = list(COLUMNS_ZH)
DEFAULT_COLUMNS = [name for name in COLUMNS if not name.startswith("unknown_")]


def _columns_zh(columns: list[str]) -> dict[str, str]:
    return {name: COLUMNS_ZH.get(name, name) for name in columns}


def _iter_yyyymm(start: date, end: date):
    y, m = start.year, start.month
    while True:
        yield f"{y:04d}{m:02d}"
        if (y, m) == (end.year, end.month):
            break
        m += 1
        if m == 13:
            y += 1
            m = 1


def _parquet_files(startdate: date | None, enddate: date | None, include_pending: str) -> list[str]:
    base = pathlib.Path(PARQUET_DIR)
    files: list[str] = []
    seen: set[str] = set()

    def add(path: pathlib.Path):
        if not path.is_file():
            return
        normalized = str(path).replace("\\", "/")
        if normalized not in seen:
            seen.add(normalized)
            files.append(normalized)

    if startdate and enddate:
        for ym in _iter_yyyymm(startdate, enddate):
            add(base / FNAME_TPL.format(yyyymm=ym))
    else:
        for path in sorted(base.glob("fund_new_issue_[0-9][0-9][0-9][0-9][0-9][0-9].parquet")):
            add(path)

    if include_pending == "yes":
        add(base / PENDING_FILE)
    return files


def _empty_payload(
    startdate: date | None,
    enddate: date | None,
    limit: int,
    offset: int,
    include_pending: str,
    selected_columns: list[str],
):
    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {
                "start_date": startdate.isoformat() if startdate else None,
                "end_date": enddate.isoformat() if enddate else None,
            },
            "include_pending": include_pending,
            "include_unknown": "yes" if selected_columns == COLUMNS else "no",
            "columns_zh": _columns_zh(selected_columns),
            "source": "Eastmoney fund new issue latest snapshot (Parquet)",
            "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
        },
        "data": [],
    }


@router.get("/data", summary="Fund new issue latest snapshot query")
async def get_fund_new_issue(
    startdate: date | None = Query(None, description="Optional established start date, YYYY-MM-DD"),
    enddate: date | None = Query(None, description="Optional established end date, YYYY-MM-DD"),
    include_pending: str = Query("yes", pattern="^(yes|no)$", description="yes keeps rows with empty established_date"),
    include_unknown: str = Query("no", pattern="^(yes|no)$", description="yes returns unknown_* columns"),
    fund_type: str | None = Query(None, description="Optional fund type exact match"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if startdate and enddate and enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    selected_columns = COLUMNS if include_unknown == "yes" else DEFAULT_COLUMNS
    parquet_files = _parquet_files(startdate, enddate, include_pending)
    if not parquet_files:
        if format == "csv":
            return csv_response([], "fund_new_issue_latest.csv")
        return _empty_payload(startdate, enddate, limit, offset, include_pending, selected_columns)

    where = []
    params: list = parquet_files
    if startdate:
        if include_pending == "yes":
            where.append("(established_date IS NULL OR established_date >= ?)")
        else:
            where.append("established_date >= ?")
        params.append(startdate.isoformat())
    if enddate:
        if include_pending == "yes":
            where.append("(established_date IS NULL OR established_date <= ?)")
        else:
            where.append("established_date <= ?")
        params.append(enddate.isoformat())
    if include_pending == "no":
        where.append("established_date IS NOT NULL")
    if fund_type:
        where.append("fund_type = ?")
        params.append(fund_type)

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    select_sql = ",\n              ".join(selected_columns)

    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in parquet_files])
        sql = f"""
            SELECT
              {select_sql}
            FROM ({union_sql})
            {where_sql}
            ORDER BY (established_date IS NOT NULL) ASC, established_date ASC NULLS FIRST, fund_code ASC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
        rows = con.execute(sql, params).fetchall()
        cols = [d[0] for d in con.description]
        data = [dict(zip(cols, row)) for row in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        try:
            con.close()
        except Exception:
            pass

    if format == "csv":
        return csv_response(data, "fund_new_issue_latest.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {
                "start_date": startdate.isoformat() if startdate else None,
                "end_date": enddate.isoformat() if enddate else None,
            },
            "include_pending": include_pending,
            "include_unknown": include_unknown,
            "columns_zh": _columns_zh(selected_columns),
            "source": "Eastmoney fund new issue latest snapshot (Parquet)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
