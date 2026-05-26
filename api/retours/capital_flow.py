import os
import pathlib
from datetime import date, datetime
from typing import List

import duckdb
from fastapi import APIRouter, HTTPException, Query

from retours.export_utils import csv_response


router = APIRouter(prefix="/capital_flow", tags=["Capital Flow"])

API_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CAPITAL_FLOW_ROOT = os.getenv("CAPITAL_FLOW_PQ_ROOT", os.path.join(API_DIR, "data", "capital_flow_data"))
SOUTHBOUND_DIR = os.getenv("SOUTHBOUND_NETBUY_PQ_DIR", os.path.join(CAPITAL_FLOW_ROOT, "southbound"))
NORTHBOUND_DIR = os.getenv("NORTHBOUND_NETBUY_PQ_DIR", os.path.join(CAPITAL_FLOW_ROOT, "northbound"))
SOUTHBOUND_FNAME_TPL = "capital_flow_southbound_{yyyymm}.parquet"
NORTHBOUND_FNAME_TPL = "capital_flow_northbound_{yyyymm}.parquet"

SOUTHBOUND_COLUMNS_ZH = {
    "dt": "交易日期",
    "southbound_accum_netbuy_100m_yuan": "南向资金累计净买入（亿元）",
    "southbound_hk_sh_accum_netbuy_100m_yuan": "港股通（沪）累计净买入（亿元）",
    "southbound_hk_sz_accum_netbuy_100m_yuan": "港股通（深）累计净买入（亿元）",
    "hang_seng_close": "恒指收盘价",
    "date_type_code": "日期类型代码",
    "southbound_daily_netbuy_100m_yuan": "南向资金日净买入（亿元）",
    "southbound_hk_sh_daily_netbuy_100m_yuan": "港股通（沪）日净买入（亿元）",
    "southbound_hk_sz_daily_netbuy_100m_yuan": "港股通（深）日净买入（亿元）",
}

SOUTHBOUND_DEFAULT_COLUMNS = [
    "dt",
    "southbound_daily_netbuy_100m_yuan",
    "southbound_hk_sh_daily_netbuy_100m_yuan",
    "southbound_hk_sz_daily_netbuy_100m_yuan",
]

SOUTHBOUND_FULL_COLUMNS = [
    "dt",
    "southbound_accum_netbuy_100m_yuan",
    "southbound_hk_sh_accum_netbuy_100m_yuan",
    "southbound_hk_sz_accum_netbuy_100m_yuan",
    "hang_seng_close",
    "date_type_code",
    "southbound_daily_netbuy_100m_yuan",
    "southbound_hk_sh_daily_netbuy_100m_yuan",
    "southbound_hk_sz_daily_netbuy_100m_yuan",
]

NORTHBOUND_COLUMNS_ZH = {
    "dt": "交易日期",
    "csi300_index_price": "沪深300指数点位",
    "sse_index_price": "上证指数点位",
    "chinext_index_price": "创业板指数点位",
    "csi300_index_pct_chg": "沪深300涨跌幅（%）",
    "sse_index_pct_chg": "上证指数涨跌幅（%）",
    "chinext_index_pct_chg": "创业板指数涨跌幅（%）",
    "northbound_deal_amt_million_yuan": "北向资金成交额（百万元）",
    "northbound_quota_balance_million_yuan": "沪股通余额（百万元）",
    "shanghai_connect_deal_amt_million_yuan": "沪股通成交额（百万元）",
    "shanghai_connect_quota_balance_million_yuan": "深圳股通余额（百万元）",
    "shenzhen_connect_deal_amt_million_yuan": "深股通成交额（百万元）",
    "shanghai_connect_lead_stock_name": "沪股通主导个股名称",
    "shanghai_connect_lead_stock_code": "沪股通主导个股代码",
    "shenzhen_connect_lead_stock_name": "深股通主导个股名称",
    "shenzhen_connect_lead_stock_code": "深股通主导个股代码",
    "shanghai_connect_lead_stock_pct_chg": "沪主导股涨跌幅（%）",
    "shenzhen_connect_lead_stock_pct_chg": "深主导股涨跌幅（%）",
    "northbound_lead_stock_name": "北向主导个股名称",
    "northbound_lead_stock_pct_chg": "北向主导股涨跌幅（%）",
    "northbound_lead_stock_code": "北向主导个股代码",
    "shanghai_connect_deal_count": "沪股通成交笔数",
    "shenzhen_connect_deal_count": "深股通成交笔数",
}

NORTHBOUND_DEFAULT_COLUMNS = [
    "dt",
    "northbound_deal_amt_million_yuan",
    "shanghai_connect_deal_amt_million_yuan",
    "shenzhen_connect_deal_amt_million_yuan",
]

NORTHBOUND_FULL_COLUMNS = [
    "dt",
    "csi300_index_price",
    "sse_index_price",
    "chinext_index_price",
    "csi300_index_pct_chg",
    "sse_index_pct_chg",
    "chinext_index_pct_chg",
    "northbound_deal_amt_million_yuan",
    "northbound_quota_balance_million_yuan",
    "shanghai_connect_deal_amt_million_yuan",
    "shanghai_connect_quota_balance_million_yuan",
    "shenzhen_connect_deal_amt_million_yuan",
    "shanghai_connect_lead_stock_name",
    "shanghai_connect_lead_stock_code",
    "shenzhen_connect_lead_stock_name",
    "shenzhen_connect_lead_stock_code",
    "shanghai_connect_lead_stock_pct_chg",
    "shenzhen_connect_lead_stock_pct_chg",
    "northbound_lead_stock_name",
    "northbound_lead_stock_pct_chg",
    "northbound_lead_stock_code",
    "shanghai_connect_deal_count",
    "shenzhen_connect_deal_count",
]


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


def _southbound_month_files(startdate: date, enddate: date) -> List[str]:
    files = []
    for ym in _iter_yyyymm(startdate, enddate):
        fp = os.path.join(SOUTHBOUND_DIR, SOUTHBOUND_FNAME_TPL.format(yyyymm=ym))
        if pathlib.Path(fp).is_file():
            files.append(fp.replace("\\", "/"))
    return files


def _northbound_month_files(startdate: date, enddate: date) -> List[str]:
    files = []
    for ym in _iter_yyyymm(startdate, enddate):
        fp = os.path.join(NORTHBOUND_DIR, NORTHBOUND_FNAME_TPL.format(yyyymm=ym))
        if pathlib.Path(fp).is_file():
            files.append(fp.replace("\\", "/"))
    return files


def _columns_zh(columns: list[str], mapping: dict[str, str]) -> dict[str, str]:
    return {name: mapping.get(name, name) for name in columns}


def _empty_payload(
    startdate: date,
    enddate: date,
    limit: int,
    offset: int,
    columns: list[str],
    columns_zh_mapping: dict[str, str],
    source: str,
):
    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": _columns_zh(columns, columns_zh_mapping),
            "source": source,
            "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
        },
        "data": [],
    }


@router.get("/southbound/data", summary="Southbound daily net buy query")
async def get_southbound_netbuy(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    fulldata: str = Query("no", pattern="^(yes|no)$", description="yes returns all stored columns; no returns default daily net-buy columns"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    selected_columns = SOUTHBOUND_FULL_COLUMNS if fulldata == "yes" else SOUTHBOUND_DEFAULT_COLUMNS
    month_files = _southbound_month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"capital_flow_southbound_{startdate}_{enddate}.csv")
        return _empty_payload(
            startdate,
            enddate,
            limit,
            offset,
            selected_columns,
            SOUTHBOUND_COLUMNS_ZH,
            "Eastmoney southbound daily net buy (Parquet/monthly)",
        )

    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in month_files])
        select_sql = ",\n              ".join(selected_columns)
        sql = f"""
            SELECT
              {select_sql}
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            ORDER BY dt ASC
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
        return csv_response(data, f"capital_flow_southbound_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": _columns_zh(selected_columns, SOUTHBOUND_COLUMNS_ZH),
            "source": "Eastmoney southbound daily net buy (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }


@router.get("/northbound/data", summary="Northbound daily deal amount query")
async def get_northbound_dealamt(
    startdate: date = Query(..., description="Start date, YYYY-MM-DD"),
    enddate: date = Query(..., description="End date, YYYY-MM-DD"),
    fulldata: str = Query("no", pattern="^(yes|no)$", description="yes returns all stored columns; no returns default capital-flow columns"),
    limit: int = Query(5000, ge=1, le=20000),
    offset: int = Query(0, ge=0),
    format: str = Query("json", pattern="^(json|csv)$", description="Response format: json or csv"),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    selected_columns = NORTHBOUND_FULL_COLUMNS if fulldata == "yes" else NORTHBOUND_DEFAULT_COLUMNS
    month_files = _northbound_month_files(startdate, enddate)
    if not month_files:
        if format == "csv":
            return csv_response([], f"capital_flow_northbound_{startdate}_{enddate}.csv")
        return _empty_payload(
            startdate,
            enddate,
            limit,
            offset,
            selected_columns,
            NORTHBOUND_COLUMNS_ZH,
            "Eastmoney northbound daily deal amount (Parquet/monthly)",
        )

    try:
        con = duckdb.connect()
        union_sql = " UNION ALL ".join(["SELECT * FROM read_parquet(?)" for _ in month_files])
        select_sql = ",\n              ".join(selected_columns)
        sql = f"""
            SELECT
              {select_sql}
            FROM ({union_sql})
            WHERE dt BETWEEN ? AND ?
            ORDER BY dt ASC
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
        return csv_response(data, f"capital_flow_northbound_{startdate}_{enddate}.csv")

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": _columns_zh(selected_columns, NORTHBOUND_COLUMNS_ZH),
            "source": "Eastmoney northbound daily deal amount (Parquet/monthly)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
