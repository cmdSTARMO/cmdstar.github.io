# main.py
import os
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

# 现有模块
from retours import szse_etf_shares, foo
# 新增：父路由（树状挂载）
from retours.margin import router as margin_router
# 为了在 lifespan 里连接数据库（只连接真正用到 DB 的子模块）
from retours.margin import szse_margin_data_total as szse_margin_data_total
from retours.margin import sse_margin_data_total as sse_margin_data_total
# from retours.margin import sse_margin_data_details as sse_margin_data_details

class ORJSONUTF8Response(ORJSONResponse):
    media_type = "application/json; charset=utf-8"

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 连接需要 DB 的模块
    await szse_etf_shares.database.connect()
    await szse_margin_data_total.database.connect()
    await sse_margin_data_total.database.connect()
    # await sse_margin_data_details.database.connect()
    try:
        yield
    finally:
        await szse_etf_shares.database.disconnect()
        await szse_margin_data_total.database.disconnect()
        await sse_margin_data_total.database.disconnect()
        # await sse_margin_data_details.database.disconnect()

app = FastAPI(
    title="HuangDapao's Data API",
    description="新增深交所上交所双融数据及每日细节查询api！v1.1版本开发圆满结束！（本次小版本优化了数据储存方式。*2）",
    version="1.1.8",
    lifespan=lifespan,
    default_response_class=ORJSONUTF8Response
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

# 路由挂载
app.include_router(szse_etf_shares.router, prefix="/szse_etf_shares", tags=["SZSE ETF份额数据"])
app.include_router(margin_router)  # /margin/* 双融数据全家桶
app.include_router(foo.router, prefix="/foo", tags=["测试接口"])

@app.get("/health", summary="健康检查")
async def health_check() -> dict:
    return {"status": "ok"}

from fastapi import Request, HTTPException
from fastapi.exceptions import RequestValidationError

# 让 HTTPException 也用 UTF-8 + orjson
@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    payload = exc.detail if isinstance(exc.detail, dict) else {"detail": exc.detail}
    return ORJSONUTF8Response(payload, status_code=exc.status_code)

# 让 422 校验错误也用 UTF-8 + orjson
@app.exception_handler(RequestValidationError)
async def validation_exc_handler(request: Request, exc: RequestValidationError):
    return ORJSONUTF8Response(
        {"detail": "请求参数不合法", "errors": exc.errors()},
        status_code=422
    )
