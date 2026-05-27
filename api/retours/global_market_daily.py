import os
import pathlib
import re
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("GLOBAL_MARKET_DAILY_PQ_DIR", os.path.join(API_DIR, "data", "global_market_daily_data"))
FNAME_TPL = "global_market_daily_{symbol_key}_{yyyymm}.parquet"

COLUMNS_ZH = {
    "market_name": "指数名称",
    "symbol": "指数代码",
    "dt": "交易日期",
    "datetime_local": "本地日期时间",
    "open": "开盘",
    "close": "收盘",
    "high": "最高",
    "low": "最低",
    "volume": "成交量",
    "adj_close": "复权收盘",
}


def _symbol_key(symbol: str) -> str:
    key = re.sub(r"[^0-9A-Za-z]+", "_", symbol).strip("_")
    return key or "symbol"


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


def _symbols_from_query(symbols: str | None) -> list[str]:
    if not symbols:
        return []
    return [item.strip() for item in symbols.replace(" ", "").split(",") if item.strip()]


def _month_files(startdate: date, enddate: date, symbols: list[str]) -> List[str]:
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
        if symbols:
            for symbol in symbols:
                key = _symbol_key(symbol)
                add(base / key / FNAME_TPL.format(symbol_key=key, yyyymm=ym))
        else:
            for path in base.glob(f"*/global_market_daily_*_{ym}.parquet"):
                add(path)

    return files


@router.get("/data", summary="Global market daily data query")
async def get_global_market_daily(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    symbols: str | None = Query(None, description="Comma-separated market symbols, e.g. ^IXIC,^DJI,^SPX"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    symbol_list = _symbols_from_query(symbols)
    month_files = _month_files(startdate, enddate, symbol_list)
    if not month_files:
        if format == "csv":
            return csv_response([], f"global_market_daily_{startdate}_{enddate}.csv")
        return {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "Global market daily data (Parquet/monthly/symbol-partitioned)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
            },
            "data": [],
        }

    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in month_files])
        symbol_clause = ""
        symbol_params: list[str] = []
        if symbol_list:
            symbol_clause = " AND symbol IN (" + ",".join(["?"] * len(symbol_list)) + ")"
            symbol_params = symbol_list

        sql = f"""
            SELECT
              market_name, symbol, dt, datetime_local,
              open, close, high, low, volume, adj_close
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            {symbol_clause}
            ORDER BY dt DESC, symbol ASC
            LIMIT ? OFFSET ?
        """
        params = month_files + [startdate.isoformat(), enddate.isoformat()] + symbol_params + [limit, offset]
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
        return csv_response(data, f"global_market_daily_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "Global market daily data (Parquet/monthly/symbol-partitioned)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
