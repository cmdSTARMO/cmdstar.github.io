# api/retours/szse_etf_shares.py

from fastapi import APIRouter, Query, HTTPException
from typing import List, Optional
from datetime import date, datetime
from pydantic import BaseModel
import os
from databases import Database

router = APIRouter()

# ─────── 数据库连接 ───────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "data", "SZSE_ETF_vol.sqlite")
DB_URL   = f"sqlite:///{DB_PATH}"
database = Database(DB_URL)

# ─────── 数据模型与映射 ───────
class ETFRecord(BaseModel):
    dt: date
    code: str
    name: str
    index_code: str
    size: float
    manager: str

COLUMN_ZH = {
    "dt": "日期",
    "code": "基金代码",
    "name": "基金名称",
    "index_code": "指数代码",
    "size": "规模(亿)",
    "manager": "基金公司"
}

# ─────── 查询接口 ───────
@router.get("/shares", summary="ETF 查询")
async def get_etf_data(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate:   date = Query(..., description="结束日期 YYYY-MM-DD"),
    limit: Optional[int] = Query(None, ge=1, le=1000),
    offset: Optional[int] = Query(0, ge=0)
):
    try:
        sql = """
            SELECT dt, code, name, index_code, size, manager
            FROM etf_data
            WHERE dt BETWEEN :start AND :end
            ORDER BY dt DESC, code
        """
        values = {"start": startdate.isoformat(), "end": enddate.isoformat()}
        if limit is not None:
            sql += " LIMIT :limit OFFSET :offset"
            values.update({"limit": limit, "offset": offset})
        rows = await database.fetch_all(query=sql, values=values)
        data = [dict(row) for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {
                "start_date": startdate.isoformat(),
                "end_date": enddate.isoformat()
            },
            "columns_zh": COLUMN_ZH
        },
        "data": data
    }
