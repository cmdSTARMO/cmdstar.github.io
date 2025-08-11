# api/retours/margin/sse_margin_data_detail.py
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from datetime import date, datetime
from pydantic import BaseModel
from databases import Database
import os

router = APIRouter()  # 前缀由父层 /margin + 本层 /sse 组合

# ── DB 连接 ──  指向你采集脚本写入的 ../api/data/sse_tab2.sqlite
RET_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # .../api/retours
API_DIR  = os.path.dirname(RET_DIR)                                      # .../api
DB_PATH  = os.path.join(API_DIR, "data", "sse_tab2.sqlite")
DB_URL   = f"sqlite:///{DB_PATH}"
database = Database(DB_URL)

# ── 类型提示（返回仍是 dict）──
class MarginDetailRecord(BaseModel):
    dt: date
    code: str
    name: str
    margin_balance: float
    margin_buy_amt: float
    margin_repay_amt: float
    short_qty: float
    short_sell_qty: float
    short_repay_qty: float

COLUMNS_ZH = {
    "dt": "日期",
    "code": "证券代码",
    "name": "证券简称",
    "margin_balance": "本日融资余额(元)",
    "margin_buy_amt": "本日融资买入额(元)",
    "margin_repay_amt": "本日融资偿还额(元)",
    "short_qty": "本日融券余量(股/份)",
    "short_sell_qty": "本日融券卖出量(股/份)",
    "short_repay_qty": "本日融券偿还量(股/份)",
}

@router.get("/detail", summary="上交所 融资融券交易明细（SSE tab2）")
async def get_sse_margin_detail(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate:   date = Query(..., description="结束日期 YYYY-MM-DD"),
    codes: Optional[List[str]] = Query(
        None, description="可多值：?codes=600000&codes=601318；也兼容逗号分隔"
    ),
    limit: int = Query(200, ge=1, le=5000, description="返回条数上限"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    # 基本校验
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="参数错误：enddate 必须 ≥ startdate")

    # 兼容 ?codes=600000,601318
    if codes and len(codes) == 1 and ("," in codes[0] or " " in codes[0]):
        codes = [p.strip() for p in codes[0].replace(" ", "").split(",") if p.strip()]

    if codes and len(codes) > 900:  # 预留 SQLite 绑定变量上限余量
        raise HTTPException(status_code=400, detail="单次 codes 数量过多，请分批查询")

    try:
        sql = """
            SELECT
                date AS dt,
                code,
                name,
                margin_balance,
                margin_buy_amt,
                margin_repay_amt,
                short_qty,
                short_sell_qty,
                short_repay_qty
            FROM tab2_data
            WHERE date BETWEEN :start AND :end
        """
        values = {"start": startdate.isoformat(), "end": enddate.isoformat()}

        if codes:
            ph = []
            for i, c in enumerate(codes):
                key = f"code_{i}"
                values[key] = c
                ph.append(f":{key}")
            sql += f" AND code IN ({', '.join(ph)})"

        sql += " ORDER BY date DESC, code ASC LIMIT :limit OFFSET :offset"
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
            "source": "SSE tab2 (融资融券明细)"
        },
        "data": data
    }
