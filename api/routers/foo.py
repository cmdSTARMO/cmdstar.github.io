# api/routers/foo.py

from fastapi import APIRouter

router = APIRouter()

@router.get("/ping", summary="Foo 服务自检")
async def foo_ping():
    return {"msg": "pong from foo"}
