# api/routers/szse_etf_shares.py

from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import ORJSONResponse
from pydantic import BaseModel
from databases import Database

# ─────── 数据库配置 ───────
DB_URL = "sqlite:////app/data/SZSE_ETF_vol.sqlite"
database = Database(DB_URL)

# ─────── Pydantic 模型 ───────
class ETFRecord(BaseModel):
    dt: date
    code: str
    name: str
    index_code: str
    size: float
    manager: str

router = APIRouter()

# ─────── 生命周期事件 ───────
@router.on_event("startup")
async def startup():
    await database.connect()

@router.on_event("shutdown")
async def shutdown():
    await database.disconnect()

# ─────── ETF 查询路由 ───────
@router.get(
    "/data",
    response_class=ORJSONResponse,      # 采用 ORJSONResponse 保证非 ASCII 原样输出
    summary="深交所 ETF 规模查询"
)
async def get_etf_data(
    startdate: date = Query(..., description="起始日期 (YYYY-MM-DD)"),
    enddate:   date = Query(..., description="结束日期 (YYYY-MM-DD)"),
    limit: Optional[int]  = Query(None, ge=1, le=1000, description="单页记录数上限"),
    offset: Optional[int] = Query(0,  ge=0, description="记录偏移量"),
) -> dict:
    """
    返回 JSON：
    {
      \"meta\": {
        \"timestamp\": \"2025-07-25T12:34:56.789Z\",
        \"startdate\": \"2025-07-18\",
        \"enddate\": \"2025-07-25\",
        \"columns\": { ... 字段中文对照 ... }
      },
      \"data\": [ {dt, code, name, ...}, ... ]
    }
    """
    sql = """
        SELECT dt, code, name, index_code, size, manager
          FROM etf_data
         WHERE dt BETWEEN :start AND :end
         ORDER BY dt, code
    """
    params = {
        "start": startdate.isoformat(),
        "end":   enddate.isoformat()
    }
    if limit is not None:
        sql += " LIMIT :limit OFFSET :offset"
        params.update({"limit": limit, "offset": offset})

    try:
        rows = await database.fetch_all(query=sql, values=params)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败：{e}")

    # 转成原始 dict 列表
    records = [ETFRecord(**row).dict() for row in rows]

    # 构造元数据
    meta = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "startdate": startdate.isoformat(),
        "enddate":   enddate.isoformat(),
        "columns": {
            "dt":         "日期",
            "code":       "证券代码",
            "name":       "证券简称",
            "index_code":"拟合指数",
            "size":       "当前规模(份)",
            "manager":    "基金管理人"
        }
    }

    return ORJSONResponse(content={"meta": meta, "data": records})
