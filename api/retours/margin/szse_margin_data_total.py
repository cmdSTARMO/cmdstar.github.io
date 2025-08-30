# api/retours/margin/szse_margin_data_total.py
from fastapi import APIRouter, Query, HTTPException
from typing import Optional
from datetime import date, datetime
from pydantic import BaseModel
from databases import Database
import os

router = APIRouter()  # 子层不写 prefix；由父层 __init__.py 组合出 /margin/szse

# ── DB 连接（与你的抓取脚本保持一致） ──
# 通用三步：
RET_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../api/retours
API_DIR  = os.path.dirname(RET_DIR)                                     # .../api
DB_PATH  = os.path.join(API_DIR, "data", "szse_tab1.sqlite")
DB_URL   = f"sqlite:///{DB_PATH}"
database = Database(DB_URL)

# ── 字段模型（类型提示；返回仍是 dict） ──
class MarginTotalRecord(BaseModel):
    dt: date
    margin_balance: float       # 融资余额(亿元)
    margin_buy_amt: float       # 融资买入额(亿元)
    short_qty: float            # 融券余量(亿股/亿份)
    short_value: float          # 融券余额(亿元)
    short_sell_qty: float       # 融券卖出量(亿股/亿份)
    marginnshort_total: float   # 融资融券余额(亿元)

COLUMNS_ZH = {
    "dt": "日期",
    "margin_balance": "融资余额(亿元)",
    "margin_buy_amt": "融资买入额(亿元)",
    "short_qty": "融券余量(亿股/亿份)",
    "short_value": "融券余额(亿元)",
    "short_sell_qty": "融券卖出量(亿股/亿份)",
    "marginnshort_total": "融资融券余额(亿元)"
}

@router.get("/total", summary="深交所 融资融券交易总量（SZSE tab1）")
async def get_szse_margin_totals(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate:   date = Query(..., description="结束日期 YYYY-MM-DD"),
    limit: int = Query(3000, ge=1, le=5000),
    offset: Optional[int] = Query(0, ge=0)
):
    """
    读取表 tab1_data：你的抓取脚本 szse_margintotal_data.py 已将每日一行写入该表。
    """
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate"
                                                    "回答我！为什么startdate比enddate大？ 回答我！")

    try:
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
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SZSE tab1 (融资融券交易总量)"
        },
        "data": data
    }
