import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("SW_INDEX_DAILY_PQ_DIR", os.path.join(API_DIR, "data", "sw_index_daily_data"))
FNAME_TPL = "sw_index_daily_{yyyymm}.parquet"

COLUMNS_ZH = {
    "swindexcode": "申万指数代码",
    "dt": "日期",
    "open": "开盘指数",
    "high": "最高指数",
    "low": "最低指数",
    "close": "收盘指数",
    "change": "涨跌点数",
    "pct_chg": "涨跌幅(%)",
    "amount": "成交额",
    "volume": "成交量",
}


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
            files.append(fp)
    return files


def _codes_from_query(swindexcodes: str | None) -> list[str]:
    if not swindexcodes:
        return []
    return [item.strip() for item in swindexcodes.replace(" ", "").split(",") if item.strip()]


@router.get("/data", summary="SW index daily data query")
async def get_sw_index_daily(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate: date = Query(..., description="结束日期 YYYY-MM-DD"),
    swindexcodes: str | None = Query(None, description="多个申万指数代码用逗号分隔，例如 801010,801020"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="返回格式：json 或 csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    month_files = _month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"sw_index_daily_{startdate}_{enddate}.csv")
        return {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "SW index daily data (Parquet/monthly)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
            },
            "data": [],
        }

    code_list = _codes_from_query(swindexcodes)
    try:
        con = duckdb.connect()
        file_params = [p.replace("\\", "/") for p in month_files]
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in file_params])
        code_clause = ""
        code_params: list = []
        if code_list:
            code_clause = " AND swindexcode IN (" + ",".join(["?"] * len(code_list)) + ")"
            code_params = code_list

        sql = f"""
            SELECT
              swindexcode, dt,
              open, high, low, close,
              change, pct_chg, amount, volume
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            {code_clause}
            ORDER BY swindexcode ASC, dt ASC
            LIMIT ? OFFSET ?
        """
        params = file_params + [startdate.isoformat(), enddate.isoformat()] + code_params + [limit, offset]
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
        return csv_response(data, f"sw_index_daily_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SW index daily data (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
