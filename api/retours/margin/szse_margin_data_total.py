import os
import pathlib
from datetime import date, datetime
from typing import List, Optional

import duckdb
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel


router = APIRouter()

RET_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
API_DIR = os.path.dirname(RET_DIR)
PARQUET_DIR = os.getenv("SZSE_TOTAL_PQ_DIR", os.path.join(API_DIR, "data", "margin_szse_tab1_data"))
FNAME_TPL = "szse_tab1_{yyyymm}.parquet"


class MarginTotalRecord(BaseModel):
    dt: date
    margin_balance: float
    margin_buy_amt: float
    short_qty: float
    short_value: float
    short_sell_qty: float
    marginnshort_total: float


COLUMNS_ZH = {
    "dt": "日期",
    "margin_balance": "融资余额(元)",
    "margin_buy_amt": "融资买入额(元)",
    "short_qty": "融券余量(股/份)",
    "short_value": "融券余额(元)",
    "short_sell_qty": "融券卖出量(股/份)",
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


@router.get("/total", summary="深交所 融资融券交易总量（SZSE tab1）")
async def get_szse_margin_totals(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate: date = Query(..., description="结束日期 YYYY-MM-DD"),
    limit: int = Query(3000, ge=1, le=5000),
    offset: Optional[int] = Query(0, ge=0),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    month_files = _month_files(startdate, enddate)
    if not month_files:
        return {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "SZSE tab1 (Parquet/月度单文件)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
            },
            "data": [],
        }

    try:
        con = duckdb.connect()
        file_params = [p.replace("\\", "/") for p in month_files]
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in file_params])
        sql = f"""
            SELECT
              dt, margin_balance, margin_buy_amt, short_qty,
              short_value, short_sell_qty, marginnshort_total
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

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SZSE tab1 (Parquet/月度单文件)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
