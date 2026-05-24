from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

from retours import foo, szse_etf_shares
import retours.ncd_aaa_yield_curve as ncd_aaa_yield_curve
from retours.margin import router as margin_router


class ORJSONUTF8Response(ORJSONResponse):
    media_type = "application/json; charset=utf-8"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await szse_etf_shares.database.connect()
    try:
        yield
    finally:
        await szse_etf_shares.database.disconnect()


app = FastAPI(
    title="HuangDapao's Data API",
    description="SZSE/SSE margin data, ETF data, and NCD AAA yield curve query API.",
    version="1.2.0",
    lifespan=lifespan,
    default_response_class=ORJSONUTF8Response,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(szse_etf_shares.router, prefix="/szse_etf_shares", tags=["SZSE ETF shares"])
app.include_router(margin_router)
app.include_router(ncd_aaa_yield_curve.router, prefix="/ncd_aaa_yield_curve", tags=["NCD AAA Yield Curve"])
app.include_router(foo.router, prefix="/foo", tags=["Test"])


@app.get("/health", summary="Health check")
async def health_check() -> dict:
    return {"status": "ok"}


@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    payload = exc.detail if isinstance(exc.detail, dict) else {"detail": exc.detail}
    return ORJSONUTF8Response(payload, status_code=exc.status_code)


@app.exception_handler(RequestValidationError)
async def validation_exc_handler(request: Request, exc: RequestValidationError):
    return ORJSONUTF8Response(
        {"detail": "请求参数不合法", "errors": exc.errors()},
        status_code=422,
    )
