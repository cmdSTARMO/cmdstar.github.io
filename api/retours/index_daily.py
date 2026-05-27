import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("INDEX_DAILY_PQ_DIR", os.path.join(API_DIR, "data", "index_daily_data"))
FNAME_TPL = "index_daily_{yyyymm}.parquet"

COLUMNS_ZH = {
    "index_name": "指数名称",
    "secid": "secid",
    "dt": "日期",
    "open": "开盘价",
    "close": "收盘价",
    "high": "最高价",
    "low": "最低价",
    "volume": "成交量",
    "amount": "成交额",
    "amplitude": "振幅",
    "pct_chg": "涨跌幅(%)",
    "change": "涨跌额",
    "turnover_rate": "换手率(%)",
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


def _secids_from_query(secids: str | None) -> list[str]:
    if not secids:
        return []
    return [item.strip() for item in secids.replace(" ", "").split(",") if item.strip()]


@router.get("/data", summary="Index daily data query")
async def get_index_daily(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate: date = Query(..., description="结束日期 YYYY-MM-DD"),
    secids: str | None = Query(None, description="多个 secid 用逗号分隔，例如 1.000001,0.399001"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="返回格式：json 或 csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    month_files = _month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"index_daily_{startdate}_{enddate}.csv")
        return {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "Index daily data (Parquet/monthly)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
            },
            "data": [],
        }

    secid_list = _secids_from_query(secids)
    try:
        con = duckdb.connect()
        file_params = [p.replace("\\", "/") for p in month_files]
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in file_params])
        secid_clause = ""
        secid_params: list = []
        if secid_list:
            secid_clause = " AND secid IN (" + ",".join(["?"] * len(secid_list)) + ")"
            secid_params = secid_list

        sql = f"""
            SELECT
              index_name, secid, dt,
              open, close, high, low,
              volume, amount, amplitude, pct_chg, change, turnover_rate
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            {secid_clause}
            ORDER BY dt DESC, secid ASC
            LIMIT ? OFFSET ?
        """
        params = file_params + [startdate.isoformat(), enddate.isoformat()] + secid_params + [limit, offset]
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
        return csv_response(data, f"index_daily_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "Index daily data (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
