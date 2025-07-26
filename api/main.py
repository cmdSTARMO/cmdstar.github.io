# api/main.py

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from retours import szse_etf_shares, foo

# ─────── 生命周期控制 ───────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await szse_etf_shares.database.connect()
    yield
    await szse_etf_shares.database.disconnect()

# ─────── FastAPI 应用与中间件 ───────
app = FastAPI(
    title="HuangDapao's Data API",
    description="2025-07-26 深交所 ETF 规模数据查询服务更新！",
    version="1.0.3",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# ─────── 注册路由模块 ───────
app.include_router(szse_etf_shares.router, prefix="/etf", tags=["SZSE ETF"])
app.include_router(foo.router, prefix="/foo", tags=["测试接口"])

# ─────── 健康检查 ───────
@app.get("/health", summary="健康检查")
async def health_check() -> dict:
    return {"status": "ok"}
