import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("NCD_AAA_PQ_DIR", os.path.join(API_DIR, "data", "ncd_aaa_yield_curve_data"))
FNAME_TPL = "ncd_aaa_yield_curve_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "term_year": "期限(年)",
    "maturity_yield": "到期收益率(%)",
    "current_yield": "当前收益率(%)",
    "future_yield": "远期收益率(%)",
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


@router.get("/data", summary="NCD AAA yield curve query")
async def get_ncd_aaa_yield_curve(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD", examples=["2026-03-01"]),
    enddate: date = Query(..., description="结束日期 YYYY-MM-DD", examples=["2026-05-25"]),
    term_year: str | None = Query(None, description="可选期限，多个用逗号分隔，例如 0.1,0.25,0.5", examples=["0.1,0.25,0.5"]),
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
                "source": "ChinaMoney NCD AAA yield curve (Parquet/月度单文件)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
            },
            "data": [],
        }
        if format == "csv":
            return csv_response([], f"ncd_aaa_yield_curve_{startdate}_{enddate}.csv")
        return empty_payload

    try:
        con = duckdb.connect()
        file_params = [p.replace("\\", "/") for p in month_files]
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in file_params])
        term_clause = ""
        term_params: list = []
        if term_year is not None:
            terms = [float(item.strip()) for item in term_year.split(",") if item.strip()]
            if terms:
                term_clause = " AND term_year IN (" + ",".join(["?"] * len(terms)) + ")"
                term_params = terms

        sql = f"""
            SELECT dt, term_year, maturity_yield, current_yield, future_yield
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            {term_clause}
            ORDER BY dt DESC, term_year ASC
            LIMIT ? OFFSET ?
        """
        params = file_params + [startdate.isoformat(), enddate.isoformat()] + term_params + [limit, offset]
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
        return csv_response(data, f"ncd_aaa_yield_curve_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "ChinaMoney NCD AAA yield curve (Parquet/月度单文件)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
