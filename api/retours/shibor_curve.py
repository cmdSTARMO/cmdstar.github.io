import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("SHIBOR_CURVE_PQ_DIR", os.path.join(API_DIR, "data", "shibor_curve_data"))
FNAME_TPL = "shibor_curve_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "shibor_on": "隔夜(O/N)",
    "shibor_1w": "1周",
    "shibor_2w": "2周",
    "shibor_1m": "1月",
    "shibor_3m": "3月",
    "shibor_6m": "6月",
    "shibor_9m": "9月",
    "shibor_1y": "1年",
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


def _empty_payload(startdate: date, enddate: date, limit: int, offset: int):
    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "ChinaMoney Shibor curve (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
        },
        "data": [],
    }


@router.get("/data", summary="Shibor curve query")
async def get_shibor_curve(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    month_files = _month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"shibor_curve_{startdate}_{enddate}.csv")
        return _empty_payload(startdate, enddate, limit, offset)

    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in month_files])
        select_sql = ",\n              ".join(COLUMNS)
        sql = f"""
            SELECT
              {select_sql}
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            ORDER BY dt DESC
            LIMIT ? OFFSET ?
        """
        params = month_files + [startdate.isoformat(), enddate.isoformat(), limit, offset]
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
        return csv_response(data, f"shibor_curve_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "ChinaMoney Shibor curve (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
