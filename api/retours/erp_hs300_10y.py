import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter()

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_DIR = os.getenv("ERP_HS300_10Y_PQ_DIR", os.path.join(API_DIR, "data", "erp_hs300_10y_data"))
FNAME_TPL = "erp_hs300_10y_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "hs300_close": "沪深300收盘",
    "hs300_ret_pct": "沪深300当日收益率(%)",
    "cn_gov_10y_yield_pct": "10年期国债收益率(年化,%)",
    "rf_daily_pct": "10年期国债当日收益率(折算,%)",
    "erp_daily_pct": "ERP_日度(%)",
    "erp_ma200_pct": "ERP_200日均线(%)",
    "erp_sigma200_pct": "ERP_200日标准差(%)",
    "erp_ma200_plus_2sigma_pct": "ERP_200日均线_上轨(+2σ)(%)",
    "erp_ma200_minus_2sigma_pct": "ERP_200日均线_下轨(-2σ)(%)",
    "rolling_window": "滚动窗口",
    "calc_years": "计算回看年数",
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


def _month_files(startdate: date, enddate: date) -> List[str]:
    files = []
    for ym in _iter_yyyymm(startdate, enddate):
        fp = os.path.join(PARQUET_DIR, FNAME_TPL.format(yyyymm=ym))
        if pathlib.Path(fp).is_file():
            files.append(fp.replace("\\", "/"))
    return files


def _empty_payload(startdate: date, enddate: date, limit: int, offset: int, frequency: str):
    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "frequency": frequency,
            "columns_zh": COLUMNS_ZH,
            "source": "HS300 ERP based on Yahoo HS300 and Shibor 10Y gov yield (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
        },
        "data": [],
    }


@router.get("/data", summary="HS300 ERP with 10Y gov yield query")
async def get_erp_hs300_10y(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    frequency: str = Query("daily", pattern="^(daily|weekly)$", description="daily or weekly; weekly uses W-FRI last observation"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    month_files = _month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"erp_hs300_10y_{frequency}_{startdate}_{enddate}.csv")
        return _empty_payload(startdate, enddate, limit, offset, frequency)

    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in month_files])
        select_sql = ",\n              ".join(COLUMNS)
        sql = f"""
            SELECT
              {select_sql}
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            ORDER BY dt ASC
        """
        rows = con.execute(sql, month_files + [startdate.isoformat(), enddate.isoformat()]).fetchall()
        cols = [d[0] for d in con.description]
        df = pd.DataFrame([dict(zip(cols, row)) for row in rows])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        try:
            con.close()
        except Exception:
            pass

    if not df.empty and frequency == "weekly":
        df["dt"] = pd.to_datetime(df["dt"])
        df = df.sort_values("dt").set_index("dt").resample("W-FRI").last().dropna(subset=["hs300_close"]).reset_index()
        df["dt"] = df["dt"].dt.date

    data = df.iloc[offset:offset + limit].to_dict(orient="records") if not df.empty else []

    if format == "csv":
        return csv_response(data, f"erp_hs300_10y_{frequency}_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "frequency": frequency,
            "columns_zh": COLUMNS_ZH,
            "source": "HS300 ERP based on Yahoo HS300 and Shibor 10Y gov yield (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(df) > offset + limit},
        },
        "data": data,
    }
