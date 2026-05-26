import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("SSE_ETF_SHARES_PQ_DIR", os.path.join(API_DIR, "data", "sse_etf_shares_data"))
FNAME_TPL = "sse_etf_shares_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "etf_type": "ETF类型",
    "sec_code": "证券代码",
    "quantity": "数量",
    "etf_name": "ETF名称",
    "total_volume_10k_shares": "总份额（万份）",
}

COLUMNS = list(COLUMNS_ZH)


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


def _month_files(startdate: date, enddate: date) -> List[str]:
    files = []
    for ym in _iter_yyyymm(startdate, enddate):
        fp = os.path.join(PARQUET_DIR, FNAME_TPL.format(yyyymm=ym))
        if pathlib.Path(fp).is_file():
            files.append(fp.replace("\\", "/"))
    return files


def _codes_from_query(sec_codes: str | None) -> list[str]:
    if not sec_codes:
        return []
    return [item.strip() for item in sec_codes.replace(" ", "").split(",") if item.strip()]


def _empty_payload(startdate: date, enddate: date, limit: int, offset: int):
    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SSE ETF shares data (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
        },
        "data": [],
    }


@router.get("/shares", summary="SSE ETF shares query")
@router.get("/data", summary="SSE ETF shares query")
async def get_sse_etf_shares(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    sec_codes: str | None = Query(None, description="Comma-separated ETF security codes"),
    etf_type: str | None = Query(None, description="Optional ETF type exact match"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    month_files = _month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"sse_etf_shares_{startdate}_{enddate}.csv")
        return _empty_payload(startdate, enddate, limit, offset)

    code_list = _codes_from_query(sec_codes)
    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in month_files])
        where = ["dt BETWEEN ? AND ?"]
        params: list = month_files + [startdate.isoformat(), enddate.isoformat()]
        if code_list:
            where.append("sec_code IN (" + ",".join(["?"] * len(code_list)) + ")")
            params.extend(code_list)
        if etf_type:
            where.append("etf_type = ?")
            params.append(etf_type)
        where_sql = " AND ".join(where)
        select_sql = ",\n              ".join(COLUMNS)
        sql = f"""
            SELECT
              {select_sql}
            FROM ({union_sql})
            WHERE {where_sql}
            ORDER BY dt DESC, sec_code ASC
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
        return csv_response(data, f"sse_etf_shares_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SSE ETF shares data (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
