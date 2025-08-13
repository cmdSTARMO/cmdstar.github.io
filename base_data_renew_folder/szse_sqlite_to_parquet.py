# base_data_renew_folder/szse_sqlite_to_parquet.py
# 将 SZSE 明细 SQLite(tab2_data) 拆解为“按月单文件 Parquet”，支持增量重跑与原子替换。
# 依赖：duckdb, pyarrow
#
# 用法示例：
#   python base_data_renew_folder/szse_sqlite_to_parquet.py \
#       --sqlite api/data/szse_tab2.sqlite \
#       --outdir api/data/margin_szse \
#       --start 2010-05 --end 2025-08
#
#   # 仅增量（默认）：若目标月文件存在且行数一致则跳过
#   python .../szse_sqlite_to_parquet.py --sqlite ... --outdir ...
#
#   # 强制全量重导（不看行数）
#   python .../szse_sqlite_to_parquet.py --sqlite ... --outdir ... --force
#
# 环境变量（可选，作默认值）：
#   SZSE_SQLITE_PATH    默认 SQLite 路径
#   SZSE_PARQUET_DIR    默认输出目录
#
# 说明：
#   - 增量判断采用“月内行数比较”，行数一致但内容变化的极端场景，可用 --force 重导覆盖。
#   - 写入采用“临时文件 + os.replace”原子替换，避免读到半截文件。

import argparse
import os
import sys
import duckdb
from datetime import date
from typing import List, Tuple

# 默认配置（可被命令行/环境变量覆盖）
DEFAULT_SQLITE = os.getenv("SZSE_SQLITE_PATH", "../api/data/szse_tab2.sqlite")
DEFAULT_OUTDIR = os.getenv("SZSE_PARQUET_DIR", "../api/data/margin_szse_tab2_data")
TABLE_NAME = "tab2_data"  # SQLite 表名
ATTACH_SCHEMA = "szse"   # ← 你的 ATTACH 别名
FNAME_TPL = "szse_tab2_{yyyymm}.parquet"  # 月度单文件命名

def qtbl(name: str = TABLE_NAME) -> str:
    return f"{ATTACH_SCHEMA}.{name}"

def _duck_path_literal(p: str) -> str:
    # 让 Windows 路径更通用 & 转义单引号
    return p.replace("\\", "/").replace("'", "''")

def yyyymm_iter(start_ym: str, end_ym: str):
    """生成 YYYYMM 从起始到终止（含）的序列。"""
    sy, sm = int(start_ym[:4]), int(start_ym[4:])
    ey, em = int(end_ym[:4]), int(end_ym[4:])
    y, m = sy, sm
    while True:
        yield f"{y:04d}{m:02d}"
        if (y, m) == (ey, em):
            break
        m += 1
        if m == 13:
            y += 1; m = 1

def ym_bounds(yyyymm: str) -> Tuple[str, str]:
    """给定 YYYYMM → 返回当月 [YYYY-MM-01, 下月-01) 边界字符串。"""
    y = int(yyyymm[:4]); m = int(yyyymm[4:])
    start = f"{y:04d}-{m:02d}-01"
    if m == 12:
        end = f"{y+1:04d}-01-01"
    else:
        end = f"{y:04d}-{m+1:02d}-01"
    return start, end

def detect_months_from_sqlite(con: duckdb.DuckDBPyConnection) -> List[str]:
    """从 SQLite 检索有哪些月份（YYYYMM）。"""
    # 通过 duckdb 的 sqlite 扩展读取
    rows = con.execute(f"""
        SELECT REPLACE(substr(date,1,7),'-','') AS ym
        FROM {qtbl()}
        GROUP BY ym
        ORDER BY ym
    """).fetchall()
    return [r[0] for r in rows]

def count_sqlite_month(con: duckdb.DuckDBPyConnection, yyyymm: str) -> int:
    """统计 SQLite 中该月行数。"""
    s, e = ym_bounds(yyyymm)
    (cnt,) = con.execute(f"""
        SELECT COUNT(*) FROM {qtbl()}
        WHERE date >= ? AND date < ?
    """, [s, e]).fetchone()
    return int(cnt)

def count_parquet_rows(con: duckdb.DuckDBPyConnection, file_path: str) -> int:
    if not os.path.isfile(file_path):
        return -1
    path = file_path.replace("\\", "/")
    try:
        (cnt,) = con.execute(
            "SELECT COALESCE(SUM(num_rows), 0) FROM parquet_metadata(?)", [path]
        ).fetchone()
        return int(cnt)
    except Exception:
        return -1

def export_one_month(con: duckdb.DuckDBPyConnection, sqlite_attached_name: str,
                     yyyymm: str, outdir: str, force: bool, smart_skip: bool) -> Tuple[str, int, int, bool]:
    """
    导出一个月份：
      - smart_skip=True：若目标文件存在且与 SQLite 行数相等，则跳过
      - 否则：整月重导（原子替换）
    返回：(目标文件名, sqlite_count, parquet_count_before, did_write)
    """
    os.makedirs(outdir, exist_ok=True)
    target = os.path.join(outdir, FNAME_TPL.format(yyyymm=yyyymm))
    s, e = ym_bounds(yyyymm)

    # 统计行数，用于智能跳过
    sqlite_cnt = count_sqlite_month(con, yyyymm)
    pq_cnt_old = count_parquet_rows(con, target)

    need_write = True
    if smart_skip and not force and pq_cnt_old >= 0 and pq_cnt_old == sqlite_cnt:
        need_write = False

    if not need_write and pq_cnt_old >= 0:
        return (target, sqlite_cnt, pq_cnt_old, False)

    tmp = target + ".tmp"
    tmp_lit = _duck_path_literal(tmp)

    con.execute(f"""
        COPY (
          SELECT
            CAST(date AS DATE)           AS dt,
            zqdm                         AS code,
            zqjc                         AS name,
            margin_buy_amt,
            margin_balance,
            short_sell_qty,
            short_qty,
            short_value,
            marginnshort_total
          FROM {qtbl()}
          WHERE date >= ? AND date < ?
        )
        TO '{tmp_lit}'
        (FORMAT PARQUET, COMPRESSION ZSTD, OVERWRITE_OR_IGNORE TRUE);
    """, [s, e])

    os.replace(tmp, target)
    return target, sqlite_cnt, pq_cnt_old, True

def main():
    parser = argparse.ArgumentParser(
        description="将 SZSE 明细 SQLite(tab2_data) 拆解为按月 Parquet（可增量重跑，原子替换）。"
    )
    parser.add_argument("--sqlite", default=DEFAULT_SQLITE, help=f"SQLite 路径（默认：{DEFAULT_SQLITE}）")
    parser.add_argument("--outdir", default=DEFAULT_OUTDIR, help=f"Parquet 输出目录（默认：{DEFAULT_OUTDIR}）")
    parser.add_argument("--start", help="起始月份 YYYY-MM 或 YYYYMM（不填则扫描 SQLite 自动确定）")
    parser.add_argument("--end", help="结束月份 YYYY-MM 或 YYYYMM（不填则扫描 SQLite 自动确定）")
    parser.add_argument("--force", action="store_true", help="无条件重导所有命中月份（忽略行数对比）")
    parser.add_argument("--no-smart-skip", action="store_true", help="禁用智能跳过（总是重导或受 --force 控制）")
    args = parser.parse_args()

    sqlite_path = args.sqlite
    outdir = args.outdir
    start_ym = args.start
    end_ym = args.end
    force = args.force
    smart_skip = not args.no_smart_skip

    if not os.path.isfile(sqlite_path):
        print(f"❌ 找不到 SQLite 文件：{sqlite_path}")
        sys.exit(1)

    # 建立 duckdb 连接并加载 sqlite 扩展
    con = duckdb.connect()
    con.execute("INSTALL sqlite; LOAD sqlite;")
    con.execute(f"ATTACH '{sqlite_path}' AS szse (TYPE sqlite)")

    # 自动推断月份范围
    months = detect_months_from_sqlite(con)
    if not months:
        print("⚠️  SQLite 表中没有数据，退出。")
        con.close()
        return

    # 解析 CLI 的 start/end（可传 YYYY-MM 或 YYYYMM）
    def norm_ym(s):
        s = s.strip()
        return s.replace("-", "") if "-" in s else s

    if start_ym:
        start_ym = norm_ym(start_ym)
    else:
        start_ym = months[0]
    if end_ym:
        end_ym = norm_ym(end_ym)
    else:
        end_ym = months[-1]

    # 计算实际要处理的月份
    todo = [ym for ym in yyyymm_iter(start_ym, end_ym) if ym in set(months)]
    if not todo:
        print("⚠️  指定范围内无可处理月份。")
        con.close()
        return

    print(f"➡️  SQLite: {sqlite_path}")
    print(f"➡️  输出目录: {outdir}")
    print(f"➡️  处理月份: {todo[0]} … {todo[-1]}（共 {len(todo)} 个月）")
    if force:
        print("⚙️  模式：强制全量重导 (--force)")
    elif smart_skip:
        print("⚙️  模式：智能增量（行数一致则跳过）")
    else:
        print("⚙️  模式：常规重导（不看行数，但非 --force）")

    total_written = 0
    for ym in todo:
        try:
            target, sqlite_cnt, pq_cnt_old, did_write = export_one_month(
                con, "szse", ym, outdir, force=force, smart_skip=smart_skip
            )
            if did_write:
                total_written += 1
                print(f"✅ [{ym}] 写入完成 → {os.path.basename(target)}   rows(sqlite)={sqlite_cnt}")
            else:
                print(f"⏭️  [{ym}] 跳过（行数一致） → {os.path.basename(target)}   rows={sqlite_cnt}")
        except Exception as e:
            print(f"❌ [{ym}] 失败：{e}")
            # 不中断整个流程，继续下一个月
            continue

    con.close()
    print(f"🏁 完成。成功写入 {total_written}/{len(todo)} 个文件。")

if __name__ == "__main__":
    main()
