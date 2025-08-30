# api/retours/szse_margin_totals.py

from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import date, datetime
from pydantic import BaseModel
import os
from databases import Database

router = APIRouter()

# ─────── 数据库连接 ───────
# 目录结构与现有模块保持一致：api/data/szse_tab1.sqlite
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "data", "szse_tab1.sqlite")
DB_URL   = f"sqlite:///{DB_PATH}"
database = Database(DB_URL)

# ─────── 字段模型（仅做类型提示，返回仍为 dict 列表）───────
class MarginTotalRecord(BaseModel):
    dt: date
    margin_balance: float       # 融资余额(元)
    margin_buy_amt: float       # 融资买入额(元)
    short_qty: float            # 融券余量
    short_value: float          # 融券余量金额(元)
    short_sell_qty: float       # 融券卖出量
    marginnshort_total: float   # 融资融券余额(元)

# ─────── 中英文对照（写进 meta.columns_zh）───────
COLUMNS_ZH = {
    "dt": "日期",
    "margin_balance": "融资余额(元)",
    "margin_buy_amt": "融资买入额(元)",
    "short_qty": "融券余量",
    "short_value": "融券余量金额(元)",
    "short_sell_qty": "融券卖出量",
    "marginnshort_total": "融资融券余额(元)"
}

@router.get("/totals", summary="融资融券交易总量（深交所 tab1）")
async def get_szse_margin_totals(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate:   date = Query(..., description="结束日期 YYYY-MM-DD"),
    limit: Optional[int] = Query(None, ge=1, le=1000),
    offset: Optional[int] = Query(0, ge=0)
):
    """
    查询表 tab1_data（由你的抓取脚本写入），按日期区间返回每日一行数据。
    """
    try:
        # 字段名与抓取脚本建表一致；将 date 别名为 dt 以与现有风格统一
        sql = """
            SELECT
                date AS dt,
                margin_balance,
                margin_buy_amt,
                short_qty,
                short_value,
                short_sell_qty,
                marginnshort_total
            FROM tab1_data
            WHERE date BETWEEN :start AND :end
            ORDER BY date DESC
        """
        values = {"start": startdate.isoformat(), "end": enddate.isoformat()}

        if limit is not None:
            sql += " LIMIT :limit OFFSET :offset"
            values.update({"limit": limit, "offset": offset})

        rows = await database.fetch_all(query=sql, values=values)
        data = [dict(r) for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {
                "start_date": startdate.isoformat(),
                "end_date": enddate.isoformat()
            },
            "columns_zh": COLUMNS_ZH,
            "source": "SZSE tab1 (融资融券交易总量)"
        },
        "data": data
    }
