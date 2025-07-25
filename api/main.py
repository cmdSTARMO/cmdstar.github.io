# api/main.py

import os
from datetime import date
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from databases import Database
from aiocache import cached, Cache
from contextlib import asynccontextmanager

# ─────── 数据库配置 ───────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "data", "SZSE_ETF_vol.sqlite")
DB_URL   = f"sqlite:///{DB_PATH}"
database = Database(DB_URL)

# ─────── Pydantic 数据模型 ───────
class ETFRecord(BaseModel):
    dt: date
    code: str
    name: str
    index_code: str
    size: float
    manager: str

# ─────── Lifespan 事件管理 ───────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    yield
    await database.disconnect()

# ─────── FastAPI 应用与中间件 ───────
app = FastAPI(
    title="SZSE ETF Data API",
    description="深交所 ETF 规模数据，支持按日期区间、分页查询",
    version="1.0.111",
    lifespan=lifespan
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─────── 健康检查路由 ───────
@app.get("/health", summary="健康检查")
async def health_check() -> dict:
    return {"status": "ok"}

# ─────── 缓存包装的数据库查询 ───────
@cached(ttl=60, cache=Cache.MEMORY)  # 缓存 60 秒
async def fetch_data_from_db(
    start: str,
    end: str,
    limit: Optional[int],
    offset: Optional[int]
):
    sql = """
        SELECT dt, code, name, index_code, size, manager
        FROM etf_data
        WHERE dt BETWEEN :start AND :end
        ORDER BY dt, code
    """
    params = {"start": start, "end": end}
    if limit is not None:
        sql += " LIMIT :limit OFFSET :offset"
        params.update({"limit": limit, "offset": offset})
    return await database.fetch_all(query=sql, values=params)

# ─────── 数据查询路由 ───────
@app.get(
    "/data",
    response_model=List[ETFRecord],
    summary="按日期区间和分页查询 ETF 数据",
)
async def get_data(
    startdate: date = Query(..., description="起始日期 (YYYY-MM-DD)"),
    enddate:   date = Query(..., description="结束日期 (YYYY-MM-DD)"),
    limit: Optional[int]  = Query(None, ge=1, le=1000, description="单页记录数上限"),
    offset: Optional[int] = Query(0,  ge=0, description="记录偏移量"),
) -> List[ETFRecord]:
    """
    异步查询 dt 在 [startdate, enddate] 之间的记录，
    支持 limit/offset 分页，并使用内存缓存。
    """
    try:
        rows = await fetch_data_from_db(
            startdate.isoformat(),
            enddate.isoformat(),
            limit,
            offset
        )
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"查询失败：{err}")

    return [ETFRecord(**row) for row in rows]
