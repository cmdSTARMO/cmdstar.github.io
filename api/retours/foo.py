# api/retours/foo.py

from fastapi import APIRouter
from fastapi.responses import ORJSONResponse

router = APIRouter()

@router.get("/ping", response_class=ORJSONResponse, summary="测试 Ping")
async def ping():
    return {"msg": "pong", "time": "✅"}
