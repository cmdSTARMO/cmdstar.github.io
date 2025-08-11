# api/retours/margin/sse_margin_data_total.py
from fastapi import APIRouter, Query, HTTPException
from datetime import date, datetime
from pydantic import BaseModel
from databases import Database
import os

router = APIRouter()  # 前缀由父层 /margin + 本层 /sse 组合

# ── DB 连接：指向采集脚本写入的 ../api/data/sse_tab1.sqlite ──
RET_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../api/retours
API_DIR  = os.path.dirname(RET_DIR)                                     # .../api
DB_PATH  = os.path.join(API_DIR, "data", "sse_tab1.sqlite")
DB_URL   = f"sqlite:///{DB_PATH}"
database = Database(DB_URL)

# 类型提示（返回仍是 dict）
class SseMarginTotal(BaseModel):
    dt: date
    margin_balance: float
    margin_buy_amt: float
    margin_repay_amt: float      # 上交所有这个字段
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

@router.get("/total", summary="上交所 融资融券交易总量（SSE tab1）")
async def get_sse_margin_total(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate:   date = Query(..., description="结束日期 YYYY-MM-DD"),
    limit: int = Query(3000, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="参数错误：enddate 必须 ≥ startdate")

    try:
        sql = """
            SELECT
                date AS dt,
                margin_balance,
                margin_buy_amt,
                margin_repay_amt,
                short_qty,
                short_sell_qty,
                short_value,
                marginnshort_total
            FROM tab1_data
            WHERE date BETWEEN :start AND :end
            ORDER BY date DESC
            LIMIT :limit OFFSET :offset
        """
        values = {
            "start": startdate.isoformat(),
            "end": enddate.isoformat(),
            "limit": limit,
            "offset": offset,
        }
        rows = await database.fetch_all(query=sql, values=values)
        data = [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SSE tab1 (融资融券交易总量)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
        },
        "data": data,
    }
