# api/retours/margin/szse_margin_data_details.py
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List
from datetime import date, datetime
import os, pathlib, duckdb

router = APIRouter()  # 仍由父层组合出 /margin/szse

# Parquet 目录（你的路径）
# __file__ = .../api/retours/margin/szse_margin_data_details.py
RET_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # .../api/retours
BASE_DIR = os.path.dirname(RET_DIR)                                     # .../api
PARQUET_DIR = os.getenv("SZSE_PQ_DIR", os.path.join(BASE_DIR, "data", "margin_szse_tab2_data"))
FNAME_TPL   = "szse_tab2_{yyyymm}.parquet"

COLUMNS_ZH = {
    "dt": "日期",
    "code": "证券代码",
    "name": "证券简称",
    "margin_buy_amt": "融资买入额(元)",
    "margin_balance": "融资余额(元)",
    "short_sell_qty": "融券卖出量(股/份)",
    "short_qty": "融券余量(股/份)",
    "short_value": "融券余额(元)",
    "marginnshort_total": "融资融券余额(元)"
}

def _codes_from_query(codes: Optional[List[str]]) -> Optional[List[str]]:
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

@router.get("/details", summary="深交所 融资融券交易明细（SZSE tab2，Parquet/月度单文件）")
async def get_szse_margin_detail(
    startdate: date = Query(..., description="起始日期 YYYY-MM-DD"),
    enddate:   date = Query(..., description="结束日期 YYYY-MM-DD"),
    codes: Optional[List[str]] = Query(None, description="?codes=000001&codes=300750 或 ?codes=000001,300750"),
    limit: int = Query(3000, ge=1, le=5000, description="返回条数上限"),
    offset: int = Query(0, ge=0, description="偏移量")
):
    if enddate < startdate:
        raise HTTPException(status_code=400, detail="参数错误：enddate 必须 ≥ startdate")
    codes = _codes_from_query(codes)

    # 只挑选区间内“命中的月份文件”
    month_files: List[str] = []
    for ym in _iter_yyyymm(startdate, enddate):
        fp = os.path.join(PARQUET_DIR, FNAME_TPL.format(yyyymm=ym))
        if pathlib.Path(fp).is_file():
            month_files.append(fp)

    if not month_files:
        return {
            "meta": {
                "query_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "data_range": {"start_date": startdate.isoformat(), "end_date": enddate.isoformat()},
                "columns_zh": COLUMNS_ZH,
                "source": "SZSE tab2 (Parquet/月度单文件)",
                "pagination": {"limit": limit, "offset": offset, "returned": 0, "has_more": False},
                "storage_dir": PARQUET_DIR,
                "files_scanned": []
            },
            "data": []
        }

    # DuckDB 连接按请求创建，线程安全且开销小
    try:
        con = duckdb.connect()
        # 把 N 个 parquet 做 UNION ALL，再做区间/代码过滤与分页
        # 规范路径分隔符，避免 Windows 反斜杠
        file_params = [p.replace("\\", "/") for p in month_files]

        # 为每个文件生成一个 read_parquet(?)，用 UNION ALL 串起来
        file_selects = ["SELECT * FROM read_parquet(?)" for _ in file_params]
        union_sql = " UNION ALL ".join(file_selects)

        # codes 参数化（避免拼接引号）
        code_clause = ""
        code_params: list = []
        if codes:
            code_clause = " AND code IN (" + ",".join(["?"] * len(codes)) + ")"
            code_params = codes

        sql = f"""
            SELECT
              dt, code, name,
              margin_buy_amt, margin_balance,
              short_sell_qty, short_qty, short_value, marginnshort_total
            FROM (
              {union_sql}
            )
            WHERE dt BETWEEN ? AND ?
            {code_clause}
            ORDER BY dt DESC, code ASC
            LIMIT ? OFFSET ?
        """

        # 参数顺序：先所有文件路径 → 再时间区间 → 再 codes → 再分页
        params = (
                file_params
                + [startdate.isoformat(), enddate.isoformat()]
                + code_params
                + [limit, offset]
        )

        rows = con.execute(sql, params).fetchall()
        cols = [d[0] for d in con.description]
        data = [dict(zip(cols, r)) for r in rows]

        code_filter = ""
        if codes:
            in_list = ", ".join([f"'{c}'" for c in codes])
            code_filter = f"AND code IN ({in_list})"
        sql = sql.replace("{code_filter}", code_filter)

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
            "source": "SZSE tab2 (Parquet/月度单文件)",
            "pagination": {"limit": limit, "offset": offset, "returned": len(data), "has_more": len(data) == limit},
            "storage_dir": PARQUET_DIR,
            "files_scanned": [os.path.basename(x) for x in month_files]
        },
        "data": data
    }
