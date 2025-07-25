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

# 导入子路由模块
from routers import szse_etf_shares, foo

# ─────── 数据库配置 ───────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH  = os.path.join(BASE_DIR, "data", "SZSE_ETF_vol.sqlite")
DB_URL   = f"sqlite:///{DB_PATH}"
database = Database(DB_URL)

# ─────── Lifespan 事件管理 ───────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.connect()
    yield
    await database.disconnect()

# ─────── FastAPI 应用 & 中间件 ───────
app = FastAPI(
    title="HuangDaPao API",
    description="统一接口服务，支持 ETF 查询与其他业务",
    version="1.0.0",
    lifespan=lifespan
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─────── 路由挂载 ───────
# 健康检查
@app.get("/health", summary="健康检查")
async def health_check() -> dict:
    return {"status": "ok"}

# 深交所ETF份额数据！ 路由组挂载到 /szse_etf_shares
app.include_router(
    szse_etf_shares.router,
    prefix="/szse_etf_shares",
    tags=["szse_etf_shares"]
)

# Foo 示例路由组挂载到 /foo
app.include_router(
    foo.router,
    prefix="/foo",
    tags=["foo"]
)
