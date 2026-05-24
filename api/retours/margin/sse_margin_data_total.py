import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from retours.export_utils import csv_response


router = APIRouter()

RET_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
API_DIR = os.path.dirname(RET_DIR)
PARQUET_DIR = os.getenv("SSE_TOTAL_PQ_DIR", os.path.join(API_DIR, "data", "margin_sse_tab1_data"))
FNAME_TPL = "sse_tab1_{yyyymm}.parquet"


class SseMarginTotal(BaseModel):
    dt: date
    margin_balance: float
    margin_buy_amt: float
    margin_repay_amt: float
    short_qty: float
    short_sell_qty: float
    short_value: float
    marginnshort_total: float


COLUMNS_ZH = {
    "dt": "日期",
    "margin_balance": "融资余额(元)",
    "margin_buy_amt": "融资买入额(元)",
    "margin_repay_amt": "融资偿还额(元)",
    "short_qty": "融券余量(股/份)",
    "short_sell_qty": "融券卖出量(股/份)",
    "short_value": "融券余额(元)",
    "marginnshort_total": "融资融券余额(元)",
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


@router.get("/total", summary="上交所 融资融券交易总量（SSE tab1）")
async def get_sse_margin_total(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate: date = Query(..., description="结束日期 YYYY-MM-DD"),
    limit: int = Query(3000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="返回格式：json 或 csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    month_files = _month_files(startdate, enddate)
    if not month_files:
        empty_payload = {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "SSE tab1 (Parquet/月度单文件)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
            },
            "data": [],
        }
        if format == "csv":
            return csv_response([], f"margin_sse_total_{startdate}_{enddate}.csv")
        return empty_payload

    try:
        con = duckdb.connect()
        file_params = [p.replace("\\", "/") for p in month_files]
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in file_params])
        sql = f"""
            SELECT
              dt, margin_balance, margin_buy_amt, margin_repay_amt,
              short_qty, short_sell_qty, short_value, marginnshort_total
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            ORDER BY dt DESC
            LIMIT ? OFFSET ?
        """
        params = file_params + [startdate.isoformat(), enddate.isoformat(), limit, offset]
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
        return csv_response(data, f"margin_sse_total_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SSE tab1 (Parquet/月度单文件)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
