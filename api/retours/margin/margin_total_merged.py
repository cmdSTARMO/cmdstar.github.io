import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

RET_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
API_DIR = os.path.dirname(RET_DIR)
SZ_PARQUET_DIR = os.getenv("MARGIN_TOTAL_SZ_PQ_DIR", os.path.join(API_DIR, "data", "margin_szse_tab1_data"))
SH_PARQUET_DIR = os.getenv("MARGIN_TOTAL_SH_PQ_DIR", os.path.join(API_DIR, "data", "margin_sse_tab1_data"))
SZ_FNAME_TPL = "szse_tab1_{yyyymm}.parquet"
SH_FNAME_TPL = "sse_tab1_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "sz_margin_balance_100m_yuan": "深交所融资余额（亿元）",
    "sh_margin_balance_100m_yuan": "上交所融资余额（亿元）",
    "total_margin_balance_100m_yuan": "总融资余额（亿元）",
    "sz_short_value_100m_yuan": "深交所融券余额（亿元）",
    "sh_short_value_100m_yuan": "上交所融券余额（亿元）",
    "total_short_value_100m_yuan": "总融券余额（亿元）",
    "total_margin_and_short_balance_100m_yuan": "融资融券余额合计（亿元）",
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


def _month_files(parquet_dir: str, filename_template: str, startdate: date, enddate: date) -> List[str]:
    files = []
    for ym in _iter_yyyymm(startdate, enddate):
        fp = os.path.join(parquet_dir, filename_template.format(yyyymm=ym))
        if pathlib.Path(fp).is_file():
            files.append(fp.replace("\\", "/"))
    return files


def _empty_payload(startdate: date, enddate: date, limit: int, offset: int):
    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SZSE/SSE margin total data (dynamic inner join by dt)",
            "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
        },
        "data": [],
    }


@router.get("/total", summary="Merged SZSE/SSE margin total query")
async def get_margin_total_merged(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    sz_files = _month_files(SZ_PARQUET_DIR, SZ_FNAME_TPL, startdate, enddate)
    sh_files = _month_files(SH_PARQUET_DIR, SH_FNAME_TPL, startdate, enddate)
    if not sz_files or not sh_files:
        if format == "csv":
            return csv_response([], f"margin_total_merged_{startdate}_{enddate}.csv")
        return _empty_payload(startdate, enddate, limit, offset)

    try:
        con = duckdb.connect()
        sz_union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in sz_files])
        sh_union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in sh_files])
        select_sql = ",\n              ".join(COLUMNS)
        sql = f"""
            WITH sz AS (
              SELECT dt, margin_balance, short_value
              FROM ({sz_union_sql})
              WHERE dt BETWEEN ? AND ?
            ),
            sh AS (
              SELECT dt, margin_balance, short_value
              FROM ({sh_union_sql})
              WHERE dt BETWEEN ? AND ?
            ),
            merged AS (
              SELECT
                sz.dt AS dt,
                sz.margin_balance AS sz_margin_balance_100m_yuan,
                sh.margin_balance / 100000000.0 AS sh_margin_balance_100m_yuan,
                sz.margin_balance + sh.margin_balance / 100000000.0 AS total_margin_balance_100m_yuan,
                sz.short_value AS sz_short_value_100m_yuan,
                sh.short_value / 100000000.0 AS sh_short_value_100m_yuan,
                sz.short_value + sh.short_value / 100000000.0 AS total_short_value_100m_yuan,
                (
                  sz.margin_balance
                  + sh.margin_balance / 100000000.0
                  + sz.short_value
                  + sh.short_value / 100000000.0
                ) AS total_margin_and_short_balance_100m_yuan
              FROM sz
              INNER JOIN sh ON sz.dt = sh.dt
            )
            SELECT
              {select_sql}
            FROM merged
            WHERE dt BETWEEN ? AND ?
            ORDER BY dt DESC
            LIMIT ? OFFSET ?
        """
        params = (
            sz_files
            + [startdate.isoformat(), enddate.isoformat()]
            + sh_files
            + [startdate.isoformat(), enddate.isoformat()]
            + [startdate.isoformat(), enddate.isoformat(), limit, offset]
        )
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
        return csv_response(data, f"margin_total_merged_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SZSE/SSE margin total data (dynamic inner join by dt)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
