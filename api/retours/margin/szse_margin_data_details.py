# api/retours/margin/szse_margin_data_details.py
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from datetime import date, datetime
from pydantic import BaseModel
from databases import Database
import os

router = APIRouter()  # 前缀由父层组合成 /margin/szse

# ── DB 连接：指向 tab2 的独立库 ──
# __file__ = .../api/retours/margin/szse_margin_data_details.py
RET_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # .../api/retours
BASE_DIR = os.path.dirname(RET_DIR)                                      # .../api
DB_PATH  = os.path.join(BASE_DIR, "data", "szse_tab2.sqlite")
DB_URL   = f"sqlite:///{DB_PATH}"
database = Database(DB_URL)

# ── 数据模型（类型提示） ──
class MarginDetailRecord(BaseModel):
    dt: date
    code: str
    name: str
    margin_buy_amt: float
    margin_balance: float
    short_sell_qty: float
    short_qty: float
    short_value: float
    marginnshort_total: float

COLUMNS_ZH = {
    "dt": "日期",
    "code": "证券代码",
    "name": "证券简称",
    "margin_buy_amt": "融资买入额(元)",
    "margin_balance": "融资余额(元)",
    "short_sell_qty": "融券卖出量(股/份)",
    "short_qty": "融券余量(股/份)",
    "short_value": "融券余额(元)",
    "marginnshort_total": "融资融券余额(元)"
}

@router.get("/detail", summary="融资融券交易明细（SZSE tab2）")
async def get_szse_margin_detail(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate:   date = Query(..., description="结束日期 YYYY-MM-DD"),
    codes: Optional[List[str]] = Query(
        None,
        description="可多值：?codes=000001&codes=300750；也兼容单个值里用逗号分隔"
    ),
    limit: int = Query(3000, ge=1, le=5000, description="返回条数上限"),
    offset: Optional[int] = Query(0, ge=0, description="偏移量")
):
    """
    读取表 tab2_data（date, zqdm, zqjc, ...），返回每日逐证券的明细。
    支持日期区间与代码过滤，默认按日期倒序 + 代码正序。
    """
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="enddate must be >= startdate")

    # 兼容把一个 codes 值里塞多个逗号分隔的情况
    if codes:
        if len(codes) > 900:
            raise HTTPException(status_code=400, detail="Too many codes in one request")
    if codes and len(codes) == 1 and ("," in codes[0] or " " in codes[0]):
        parts = [p.strip() for p in codes[0].replace(" ", "").split(",") if p.strip()]
        codes = parts or None
    if limit is None and offset:
        raise HTTPException(status_code=400, detail="offset requires limit")

    try:
        base_sql = """
            SELECT
                date AS dt,
                zqdm AS code,
                zqjc AS name,
                margin_buy_amt,
                margin_balance,
                short_sell_qty,
                short_qty,
                short_value,
                marginnshort_total
            FROM tab2_data
            WHERE date BETWEEN :start AND :end
        """
        values = {"start": startdate.isoformat(), "end": enddate.isoformat()}

        # 代码过滤（IN 子句占位符）
        if codes:
            placeholders = []
            for i, c in enumerate(codes):
                key = f"code_{i}"
                placeholders.append(f":{key}")
                values[key] = c
            base_sql += f" AND zqdm IN ({', '.join(placeholders)})"

        # 排序 + 分页
        base_sql += " ORDER BY date DESC, zqdm ASC"
        if limit is not None:
            base_sql += " LIMIT :limit OFFSET :offset"
            values.update({"limit": limit, "offset": offset})

        rows = await database.fetch_all(query=base_sql, values=values)
        data = [dict(r) for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SZSE tab2 (融资融券交易明细)"
        },
        "data": data
    }
