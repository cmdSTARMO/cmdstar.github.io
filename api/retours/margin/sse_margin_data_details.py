# api/retours/margin/sse_margin_data_detail.py
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from datetime import date, datetime
import os, pathlib, duckdb

router = APIRouter()  # 前缀由父层 /margin + 本层 /sse 组合

# ── Parquet 目录配置 ──
RET_DIR     = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../api/retours
BASE_DIR    = os.path.dirname(RET_DIR)                                     # .../api
PARQUET_DIR = os.getenv("SSE_PQ_DIR", os.path.join(BASE_DIR, "data", "margin_sse_tab2_data"))
FNAME_TPL   = "sse_tab2_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "code": "证券代码",
    "name": "证券简称",
    "margin_balance": "本日融资余额(元)",
    "margin_buy_amt": "本日融资买入额(元)",
    "margin_repay_amt": "本日融资偿还额(元)",
    "short_qty": "本日融券余量(股/份)",
    "short_sell_qty": "本日融券卖出量(股/份)",
    "short_repay_qty": "本日融券偿还量(股/份)",
}

def _codes_from_query(codes: Optional[List[str]]) -> Optional[List[str]]:
    """兼容 ?codes=600000&codes=601318 以及 ?codes=600000,601318"""
    if not codes:
        return None
    if len(codes) == 1 and ("," in codes[0] or " " in codes[0]):
        codes = [p.strip() for p in codes[0].replace(" ", "").split(",") if p.strip()]
    if len(codes) > 900:
        raise HTTPException(status_code=400, detail="单次 codes 数量过多，请分批查询")
    return codes

def _iter_yyyymm(start: date, end: date):
    y, m = start.year, start.month
    while True:
        yield f"{y:04d}{m:02d}"
        if (y, m) == (end.year, end.month):
            break
        m += 1
        if m == 13:
            y += 1; m = 1

@router.get("/details", summary="上交所 融资融券交易明细（SSE tab2，Parquet/月度单文件）")
async def get_sse_margin_detail(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate:   date = Query(..., description="结束日期 YYYY-MM-DD"),
    codes: Optional[List[str]] = Query(None, description="?codes=600000&codes=601318 或 ?codes=600000,601318"),
    limit: int = Query(200, ge=1, le=5000, description="返回条数上限"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    # 基本校验
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="参数错误：enddate 必须 ≥ startdate")
    codes = _codes_from_query(codes)

    # 命中月份对应的 parquet 文件列表
    month_files: List[str] = []
    for ym in _iter_yyyymm(startdate, enddate):
        fp = os.path.join(PARQUET_DIR, FNAME_TPL.format(yyyymm=ym))
        if pathlib.Path(fp).is_file():
            month_files.append(fp)

    # 没有可读文件，直接返回空
    if not month_files:
        return {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "SSE tab2 (Parquet/月度单文件)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
                "storage_dir": PARQUET_DIR,
                "files_scanned": []
            },
            "data": []
        }

    # DuckDB：把多个 parquet 以 UNION ALL 拼接，做区间/代码过滤与分页
    try:
        con = duckdb.connect()
        file_params = [p.replace("\\", "/") for p in month_files]  # DuckDB 用 / 更稳
        file_selects = ["SELECT * FROM read_parquet(?)" for _ in file_params]
        union_sql = " UNION ALL ".join(file_selects)

        code_clause = ""
        code_params: list = []
        if codes:
            code_clause = " AND code IN (" + ",".join(["?"] * len(codes)) + ")"
            code_params = codes

        sql = f"""
            SELECT
              dt, code, name,
              margin_balance, margin_buy_amt, margin_repay_amt,
              short_qty, short_sell_qty, short_repay_qty
            FROM (
              {union_sql}
            )
            WHERE dt BETWEEN ? AND ?
            {code_clause}
            ORDER BY dt DESC, code ASC
            LIMIT ? OFFSET ?
        """
        # 参数顺序：所有文件路径 → 时间区间 → codes → 分页
        params = (
            file_params
            + [startdate.isoformat(), enddate.isoformat()]
            + code_params
            + [limit, offset]
        )

        rows = con.execute(sql, params).fetchall()
        cols = [d[0] for d in con.description]
        data = [dict(zip(cols, r)) for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            con.close()
        except:
            pass

    return {
        "meta": {
            "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
            "columns_zh": COLUMNS_ZH,
            "source": "SSE tab2 (Parquet/月度单文件)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
            "storage_dir": PARQUET_DIR,
            "files_scanned": [os.path.basename(x) for x in month_files]
        },
        "data": data
    }
