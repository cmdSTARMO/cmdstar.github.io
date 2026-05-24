import os
import pathlib
from datetime import date, datetime
from typing import List, Optional

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

RET_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(RET_DIR)
PARQUET_DIR = os.getenv("SZSE_PQ_DIR", os.path.join(BASE_DIR, "data", "margin_szse_tab2_data"))
FNAME_TPL = "szse_tab2_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "code": "证券代码",
    "name": "证券简称",
    "margin_buy_amt": "融资买入额(元)",
    "margin_balance": "融资余额(元)",
    "short_sell_qty": "融券卖出量(股/份)",
    "short_qty": "融券余量(股/份)",
    "short_value": "融券余额(元)",
    "marginnshort_total": "融资融券余额(元)",
}


def _codes_from_query(codes: Optional[List[str]]) -> Optional[List[str]]:
    if not codes:
        return None
    if len(codes) == 1 and ("," in codes[0] or " " in codes[0]):
        codes = [p.strip() for p in codes[0].replace(" ", "").split(",") if p.strip()]
    if len(codes) > 900:
        raise HTTPException(status_code=400, detail="单次 codes 数量过多，请分批查询")
    return codes


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


@router.get("/details", summary="深交所 融资融券交易明细（SZSE tab2）")
async def get_szse_margin_detail(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate: date = Query(..., description="结束日期 YYYY-MM-DD"),
    codes: Optional[List[str]] = Query(None, description="?codes=000001&codes=300750 或 ?codes=000001,300750"),
    limit: int = Query(3000, ge=1, le=5000, description="返回条数上限"),
    offset: int = Query(0, ge=0, description="偏移量"),
    format: str = Query("json", pattern="^(json|csv)$", description="返回格式：json 或 csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")
    codes = _codes_from_query(codes)

    month_files = _month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"margin_szse_details_{startdate}_{enddate}.csv")
        return {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "SZSE tab2 (Parquet/monthly)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
            },
            "data": [],
        }

    try:
        con = duckdb.connect()
        file_params = [p.replace("\\", "/") for p in month_files]
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in file_params])

        code_clause = ""
        code_params: list = []
        if codes:
            code_clause = " AND code IN (" + ",".join(["?"] * len(codes)) + ")"
            code_params = codes

        sql = f"""
            SELECT
              dt, code, name,
              margin_buy_amt, margin_balance,
              short_sell_qty, short_qty, short_value, marginnshort_total
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            {code_clause}
            ORDER BY dt DESC, code ASC
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
        return csv_response(data, f"margin_szse_details_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SZSE tab2 (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
