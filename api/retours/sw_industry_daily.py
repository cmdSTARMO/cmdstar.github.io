import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("SW_INDUSTRY_DAILY_PQ_DIR", os.path.join(API_DIR, "data", "sw_industry_daily_data"))
FNAME_TPL = "sw_industry_daily_{code}_{yyyymm}.parquet"

COLUMNS_ZH = {
    "industry_name": "行业名称",
    "swindexcode": "申万行业代码",
    "dt": "日期",
    "open_index": "开盘指数",
    "high_index": "最高指数",
    "low_index": "最低指数",
    "close_index": "收盘指数",
    "change": "涨跌点数",
    "pct_chg": "涨跌幅(%)",
    "volume_100m_shares": "成交量(亿股)",
    "amount_100m_yuan": "成交额(亿元)",
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


def _codes_from_query(swindexcodes: str | None) -> list[str]:
    if not swindexcodes:
        return []
    return [item.strip() for item in swindexcodes.replace(" ", "").split(",") if item.strip()]


def _month_files(startdate: date, enddate: date, codes: list[str]) -> List[str]:
    base = pathlib.Path(PARQUET_DIR)
    files: list[str] = []
    seen: set[str] = set()

    def add(path: pathlib.Path):
        if not path.is_file():
            return
        normalized = str(path).replace("\\", "/")
        if normalized not in seen:
            seen.add(normalized)
            files.append(normalized)

    for ym in _iter_yyyymm(startdate, enddate):
        if codes:
            for code in codes:
                add(base / code / FNAME_TPL.format(code=code, yyyymm=ym))
        else:
            for path in base.glob(f"*/sw_industry_daily_*_{ym}.parquet"):
                add(path)

        # Backward compatibility for the earlier flat monthly layout.
        add(base / f"sw_industry_daily_{ym}.parquet")

    return files


@router.get("/data", summary="SW industry daily data query")
async def get_sw_industry_daily(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    swindexcodes: str | None = Query(None, description="Comma-separated SW industry codes, e.g. 801010,801030"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    code_list = _codes_from_query(swindexcodes)
    month_files = _month_files(startdate, enddate, code_list)
    if not month_files:
        if format == "csv":
            return csv_response([], f"sw_industry_daily_{startdate}_{enddate}.csv")
        return {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "SW industry daily data (Parquet/monthly/code-partitioned)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
            },
            "data": [],
        }

    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in month_files])
        code_clause = ""
        code_params: list[str] = []
        if code_list:
            code_clause = " AND swindexcode IN (" + ",".join(["?"] * len(code_list)) + ")"
            code_params = code_list

        sql = f"""
            SELECT
              industry_name, swindexcode, dt,
              open_index, high_index, low_index, close_index,
              change, pct_chg, volume_100m_shares, amount_100m_yuan
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            {code_clause}
            ORDER BY dt DESC, swindexcode ASC
            LIMIT ? OFFSET ?
        """
        params = month_files + [startdate.isoformat(), enddate.isoformat()] + code_params + [limit, offset]
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
        return csv_response(data, f"sw_industry_daily_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SW industry daily data (Parquet/monthly/code-partitioned)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
