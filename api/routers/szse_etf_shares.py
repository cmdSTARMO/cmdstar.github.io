# api/routers/etf.py

from fastapi import APIRouter, HTTPException, Query
from typing        import List, Optional
from datetime      import date
from pydantic      import BaseModel
from databases     import Database

# 数据库客户端（共用同一实例）
DB_URL = "sqlite:///../data/SZSE_ETF_vol.sqlite"
database = Database(DB_URL)

# Pydantic 模型
class ETFRecord(BaseModel):
    dt: date
    code: str
    name: str
    index_code: str
    size: float
    manager: str

router = APIRouter()

@router.get(
    "/data",
    response_model=List[ETFRecord],
    summary="深交所 ETF 规模查询"
)
async def etf_data(
    startdate: date = Query(..., description="起始日期 (YYYY-MM-DD)"),
    enddate:   date = Query(..., description="结束日期 (YYYY-MM-DD)"),
    limit: Optional[int]  = Query(None, ge=1, le=1000),
    offset:Optional[int] = Query(0,  ge=0),
) -> List[ETFRecord]:
    """
    查询 dt 在 [startdate, enddate] 之间的 ETF 数据，
    支持分页（limit/offset）。
    """
    sql = """
        SELECT dt, code, name, index_code, size, manager
          FROM etf_data
         WHERE dt BETWEEN :start AND :end
         ORDER BY dt, code
    """
    params = {"start": startdate.isoformat(), "end": enddate.isoformat()}
    if limit is not None:
        sql += " LIMIT :limit OFFSET :offset"
        params.update({"limit": limit, "offset": offset})

    try:
        rows = await database.fetch_all(query=sql, values=params)
    except Exception as e:
        raise HTTPException(500, f"查询失败：{e}")

    return [ETFRecord(**r) for r in rows]
