import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("RMB_FX_INDEX_PQ_DIR", os.path.join(API_DIR, "data", "rmb_fx_index_data"))
FNAME_TPL = "rmb_fx_index_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "cfets_index_rate": "CFETS人民币汇率指数",
    "bis_index_rate": "BIS货币篮子人民币汇率指数",
    "sdr_index_rate": "SDR货币篮子人民币汇率指数",
    "dt_en": "英文日期",
    "cfets_index_rate_text": "CFETS人民币汇率指数文本",
    "bis_index_rate_text": "BIS货币篮子人民币汇率指数文本",
    "sdr_index_rate_text": "SDR货币篮子人民币汇率指数文本",
    "range_start": "请求区间开始",
    "range_end": "请求区间结束",
}

DEFAULT_COLUMNS = [
    "dt",
    "cfets_index_rate",
    "bis_index_rate",
    "sdr_index_rate",
]

FULL_COLUMNS = [
    "dt",
    "cfets_index_rate",
    "bis_index_rate",
    "sdr_index_rate",
    "dt_en",
    "cfets_index_rate_text",
    "bis_index_rate_text",
    "sdr_index_rate_text",
    "range_start",
    "range_end",
]


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


def _month_files(startdate: date, enddate: date) -> List[str]:
    files = []
    for ym in _iter_yyyymm(startdate, enddate):
        fp = os.path.join(PARQUET_DIR, FNAME_TPL.format(yyyymm=ym))
        if pathlib.Path(fp).is_file():
            files.append(fp.replace("\\", "/"))
    return files


def _empty_payload(startdate: date, enddate: date, limit: int, offset: int, columns: list[str], fulldata: str):
    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "fulldata": fulldata,
            "columns_zh": _columns_zh(columns),
            "source": "ChinaMoney RMB FX index history (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
        },
        "data": [],
    }


@router.get("/data", summary="RMB FX index query")
async def get_rmb_fx_index(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    fulldata: str = Query("no", pattern="^(yes|no)$", description="yes returns source text/range columns"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    selected_columns = FULL_COLUMNS if fulldata == "yes" else DEFAULT_COLUMNS
    month_files = _month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"rmb_fx_index_{startdate}_{enddate}.csv")
        return _empty_payload(startdate, enddate, limit, offset, selected_columns, fulldata)

    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in month_files])
        select_sql = ",\n              ".join(selected_columns)
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
        return csv_response(data, f"rmb_fx_index_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "fulldata": fulldata,
            "columns_zh": _columns_zh(selected_columns),
            "source": "ChinaMoney RMB FX index history (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
