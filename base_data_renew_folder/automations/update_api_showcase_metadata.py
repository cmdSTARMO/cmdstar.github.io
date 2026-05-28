import json
import glob
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SHOWCASE_DIR = REPO_ROOT / "api_showcase"
API_META_DIR = SHOWCASE_DIR / "metadata" / "apis"
CATALOG_FILE = SHOWCASE_DIR / "metadata" / "catalog.json"
API_DATA_DIR = REPO_ROOT / "api" / "data"


COMMON_DATE_PARAMS = [
    {"name": "startdate", "type": "date", "required": True, "label": "开始日期", "default": "2025-01-01"},
    {"name": "enddate", "type": "date", "required": True, "label": "结束日期", "default": "2026-05-25"},
    {"name": "offset", "type": "number", "required": False, "label": "偏移量", "default": "0"},
    {"name": "format", "type": "select", "required": False, "label": "返回格式", "default": "json", "options": [
        {"value": "json", "label": "JSON"},
        {"value": "csv", "label": "CSV"},
    ]},
]


def p(pattern: str) -> str:
    return str((API_DATA_DIR / pattern).as_posix())


DEFAULT_APIS = [
    {
        "id": "margin_total_merged",
        "title": "融资融券余额合并",
        "summary": "按交易日合并深交所与上交所融资、融券余额，默认只返回两市都有数据的日期。",
        "group": {"level1": "金融数据", "level2": "融资融券"},
        "endpoint": "/margin/merged/total",
        "method": "GET",
        "params": COMMON_DATE_PARAMS,
        "columns_zh": {
            "dt": "日期", "sz_margin_balance_100m_yuan": "深交所融资余额（亿元）",
            "sh_margin_balance_100m_yuan": "上交所融资余额（亿元）", "total_margin_balance_100m_yuan": "总融资余额（亿元）",
            "sz_short_value_100m_yuan": "深交所融券余额（亿元）", "sh_short_value_100m_yuan": "上交所融券余额（亿元）",
            "total_short_value_100m_yuan": "总融券余额（亿元）",
        },
        "data_sources": [
            {"type": "parquet", "glob": p("margin_szse_tab1_data/*.parquet"), "date_column": "dt"},
            {"type": "parquet", "glob": p("margin_sse_tab1_data/*.parquet"), "date_column": "dt"},
        ],
    },
    {
        "id": "szse_margin_total",
        "title": "深交所融资融券总量",
        "summary": "深交所融资融券总量日度数据。",
        "group": {"level1": "金融数据", "level2": "融资融券"},
        "endpoint": "/margin/szse/total",
        "method": "GET",
        "params": COMMON_DATE_PARAMS,
        "columns_zh": {"dt": "日期", "margin_balance": "融资余额(元)", "margin_buy_amt": "融资买入额(元)", "short_qty": "融券余量", "short_value": "融券余量金额(元)", "short_sell_qty": "融券卖出量", "marginnshort_total": "融资融券余额(元)"},
        "data_sources": [{"type": "parquet", "glob": p("margin_szse_tab1_data/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "sse_margin_total",
        "title": "上交所融资融券总量",
        "summary": "上交所融资融券总量日度数据。",
        "group": {"level1": "金融数据", "level2": "融资融券"},
        "endpoint": "/margin/sse/total",
        "method": "GET",
        "params": COMMON_DATE_PARAMS,
        "columns_zh": {"dt": "日期", "margin_balance": "融资余额(元)", "margin_buy_amt": "融资买入额(元)", "margin_repay_amt": "融资偿还额(元)", "short_qty": "融券余量", "short_sell_qty": "融券卖出量", "short_value": "融券余量金额(元)", "marginnshort_total": "融资融券余额(元)"},
        "data_sources": [{"type": "parquet", "glob": p("margin_sse_tab1_data/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "szse_margin_detail",
        "title": "深交所融资融券明细",
        "summary": "深交所按证券代码拆分的融资融券交易明细。",
        "group": {"level1": "金融数据", "level2": "融资融券"},
        "endpoint": "/margin/szse/details",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "codes", "type": "text", "required": False, "label": "证券代码", "placeholder": "000001,300750"}],
        "columns_zh": {"dt": "日期", "code": "证券代码", "name": "证券简称", "margin_balance": "融资余额(元)", "margin_buy_amt": "融资买入额(元)", "short_qty": "融券余量", "short_value": "融券余量金额(元)", "short_sell_qty": "融券卖出量", "marginnshort_total": "融资融券余额(元)"},
        "data_sources": [{"type": "parquet", "glob": p("margin_szse_tab2_data/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "sse_margin_detail",
        "title": "上交所融资融券明细",
        "summary": "上交所按证券代码拆分的融资融券交易明细。",
        "group": {"level1": "金融数据", "level2": "融资融券"},
        "endpoint": "/margin/sse/details",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "codes", "type": "text", "required": False, "label": "证券代码", "placeholder": "600000,601318"}],
        "columns_zh": {"dt": "日期", "code": "证券代码", "name": "证券简称", "margin_balance": "融资余额(元)", "margin_buy_amt": "融资买入额(元)", "margin_repay_amt": "融资偿还额(元)", "short_qty": "融券余量", "short_sell_qty": "融券卖出量", "short_value": "融券余量金额(元)", "marginnshort_total": "融资融券余额(元)"},
        "data_sources": [{"type": "parquet", "glob": p("margin_sse_tab2_data/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "sw_industry_daily",
        "title": "申万一级行业日度行情",
        "summary": "申万一级行业指数日度开高低收、涨跌幅、成交量与成交额。",
        "group": {"level1": "金融数据", "level2": "市场行情"},
        "show_in_catalog": False,
        "endpoint": "/sw_industry_daily/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "swindexcodes", "type": "multiselect", "required": False, "label": "申万行业", "option_source": "swindexcode", "placeholder": "801010,801030"}],
        "columns_zh": {"industry_name": "行业名称", "swindexcode": "申万行业代码", "dt": "日期", "open_index": "开盘指数", "high_index": "最高指数", "low_index": "最低指数", "close_index": "收盘指数", "change": "涨跌点数", "pct_chg": "涨跌幅(%)", "volume_100m_shares": "成交量(亿股)", "amount_100m_yuan": "成交额(亿元)"},
        "static_options": {"swindexcode": [
            {"value": "801010", "label": "农林牧渔"}, {"value": "801030", "label": "基础化工"}, {"value": "801040", "label": "钢铁"}, {"value": "801050", "label": "有色金属"}, {"value": "801080", "label": "电子"}, {"value": "801110", "label": "家用电器"}, {"value": "801120", "label": "食品饮料"}, {"value": "801130", "label": "纺织服饰"}, {"value": "801140", "label": "轻工制造"}, {"value": "801150", "label": "医药生物"}, {"value": "801160", "label": "公用事业"}, {"value": "801170", "label": "交通运输"}, {"value": "801180", "label": "房地产"}, {"value": "801200", "label": "商贸零售"}, {"value": "801210", "label": "社会服务"}, {"value": "801230", "label": "综合"}, {"value": "801710", "label": "建筑材料"}, {"value": "801720", "label": "建筑装饰"}, {"value": "801730", "label": "电力设备"}, {"value": "801740", "label": "国防军工"}, {"value": "801750", "label": "计算机"}, {"value": "801760", "label": "传媒"}, {"value": "801770", "label": "通信"}, {"value": "801780", "label": "银行"}, {"value": "801790", "label": "非银金融"}, {"value": "801880", "label": "汽车"}, {"value": "801890", "label": "机械设备"}, {"value": "801950", "label": "煤炭"}, {"value": "801960", "label": "石油石化"}, {"value": "801970", "label": "环保"}, {"value": "801980", "label": "美容护理"}
        ]},
        "data_sources": [{"type": "parquet", "glob": p("sw_industry_daily_data/**/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "global_market_daily",
        "title": "全球主要市场指数日度行情",
        "summary": "全球主要股票市场指数日度行情，按 Yahoo Finance symbol 查询。",
        "group": {"level1": "金融数据", "level2": "市场行情"},
        "endpoint": "/global_market_daily/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "symbols", "type": "multiselect", "required": False, "label": "市场指数", "option_source": "symbol", "placeholder": "^IXIC,^DJI,^SPX"}],
        "columns_zh": {"market_name": "指数名称", "symbol": "指数代码", "dt": "交易日期", "datetime_local": "本地日期时间", "open": "开盘", "close": "收盘", "high": "最高", "low": "最低", "volume": "成交量", "adj_close": "复权收盘"},
        "static_options": {"symbol": [
            {"value": "^IXIC", "label": "纳斯达克"}, {"value": "^DJI", "label": "道琼斯"}, {"value": "^SPX", "label": "标普500"}, {"value": "^VIX", "label": "恐慌指数"}, {"value": "^GSPTSE", "label": "富时加拿大"}, {"value": "^BVSP", "label": "巴西IBOVESPA"}, {"value": "^MXX", "label": "墨西哥IPC"}, {"value": "^MERV", "label": "阿根廷MERVAL"}, {"value": "^N100", "label": "泛欧100"}, {"value": "^FTSE", "label": "英国富时100"}, {"value": "^FTAI", "label": "英国富时AIM全股"}, {"value": "^FCHI", "label": "法国CAC40"}, {"value": "^GDAXI", "label": "德国DAX"}, {"value": "^SSMI", "label": "瑞士SMI"}, {"value": "FTSEMIB.MI", "label": "意大利MIB"}, {"value": "^AEX", "label": "荷兰AEX"}, {"value": "^BFX", "label": "比利时BEL20"}, {"value": "^STOXX50E", "label": "欧元区STOXX50"}, {"value": "^N225", "label": "日经225"}, {"value": "^BSESN", "label": "孟买SENSEX"}, {"value": "^SET.BK", "label": "泰国SET"}, {"value": "^KS11", "label": "韩国KOSPI"}, {"value": "^STI", "label": "新加坡STI"}, {"value": "^JKSE", "label": "印尼综合指数"}, {"value": "^KLSE", "label": "马来西亚KLCI"}, {"value": "^AORD", "label": "澳大利亚综合指数"}, {"value": "^NZ50", "label": "新西兰50"}, {"value": "^TA125.TA", "label": "以色列TA-125"}, {"value": "DFMGI.AE", "label": "迪拜DFM"}, {"value": "^TASI.SR", "label": "沙特TASI"}, {"value": "^J203.JO", "label": "南非全股"}
        ]},
        "data_sources": [{"type": "parquet", "glob": p("global_market_daily_data/**/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "ncd_aaa_yield_curve",
        "title": "同业存单 AAA 收益率曲线",
        "summary": "中国货币网同业存单 AAA 曲线，支持按期限筛选。",
        "group": {"level1": "金融数据", "level2": "利率曲线"},
        "endpoint": "/ncd_aaa_yield_curve/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "term_year", "type": "multiselect", "required": False, "label": "期限(年)", "option_source": "term_year", "placeholder": "0.1,0.25,0.5"}],
        "columns_zh": {"dt": "日期", "term_year": "期限(年)", "maturity_yield": "到期收益率(%)", "current_yield": "当前收益率(%)", "future_yield": "远期收益率(%)"},
        "data_sources": [{"type": "parquet", "glob": p("ncd_aaa_yield_curve_data/*.parquet"), "date_column": "dt", "option_columns": ["term_year"]}],
    },
    {
        "id": "capital_flow_southbound",
        "title": "南向资金日净买入",
        "summary": "南向资金、港股通沪/深日净买入，默认返回常用净买入列。",
        "group": {"level1": "金融数据", "level2": "资金流"},
        "endpoint": "/capital_flow/southbound/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "fulldata", "type": "select", "required": False, "label": "完整字段", "default": "no", "options": [{"value": "no", "label": "默认字段"}, {"value": "yes", "label": "全部字段"}]}],
        "columns_zh": {"dt": "交易日期", "southbound_daily_netbuy_100m_yuan": "南向资金日净买入（亿元）", "southbound_hk_sh_daily_netbuy_100m_yuan": "港股通（沪）日净买入（亿元）", "southbound_hk_sz_daily_netbuy_100m_yuan": "港股通（深）日净买入（亿元）"},
        "data_sources": [{"type": "parquet", "glob": p("capital_flow_data/southbound/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "capital_flow_northbound",
        "title": "北向资金成交额",
        "summary": "北向资金、沪股通、深股通成交额，默认返回常用成交额列。",
        "group": {"level1": "金融数据", "level2": "资金流"},
        "endpoint": "/capital_flow/northbound/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "fulldata", "type": "select", "required": False, "label": "完整字段", "default": "no", "options": [{"value": "no", "label": "默认字段"}, {"value": "yes", "label": "全部字段"}]}],
        "columns_zh": {"dt": "交易日期", "northbound_deal_amt_million_yuan": "北向资金成交额（百万元）", "shanghai_connect_deal_amt_million_yuan": "沪股通成交额（百万元）", "shenzhen_connect_deal_amt_million_yuan": "深股通成交额（百万元）"},
        "data_sources": [{"type": "parquet", "glob": p("capital_flow_data/northbound/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "fund_new_issue",
        "title": "新发基金数据",
        "summary": "东方财富新发基金快照，默认包含尚未成立基金并隐藏 unknown 字段。",
        "group": {"level1": "金融数据", "level2": "基金"},
        "endpoint": "/fund_new_issue/data",
        "method": "GET",
        "params": [
            {"name": "startdate", "type": "date", "required": False, "label": "成立开始日期", "default": ""},
            {"name": "enddate", "type": "date", "required": False, "label": "成立结束日期", "default": ""},
            {"name": "include_pending", "type": "select", "required": False, "label": "尚未成立基金", "default": "yes", "options": [{"value": "yes", "label": "包含"}, {"value": "no", "label": "不包含"}]},
            {"name": "fund_type", "type": "text", "required": False, "label": "基金类型", "placeholder": "混合型-偏股"},
            {"name": "offset", "type": "number", "required": False, "label": "偏移量", "default": "0"},
            {"name": "format", "type": "select", "required": False, "label": "返回格式", "default": "json", "options": [{"value": "json", "label": "JSON"}, {"value": "csv", "label": "CSV"}]},
        ],
        "hidden_params": ["include_unknown"],
        "columns_zh": {"established_date": "成立日期", "fund_code": "基金代码", "fund_name": "基金简称", "fund_company": "发行公司", "fund_type": "基金类型", "raised_shares": "募集份额", "fund_manager": "基金经理", "subscription_status": "申购状态", "subscription_period": "集中认购期", "discount_rate": "优惠费率"},
        "data_sources": [{"type": "parquet", "glob": p("fund_new_issue_data/**/*.parquet"), "date_column": "established_date", "option_columns": ["fund_type"]}],
    },
    {
        "id": "rmb_fx_index",
        "title": "人民币汇率指数",
        "summary": "CFETS、BIS、SDR 三类人民币汇率指数。",
        "group": {"level1": "金融数据", "level2": "宏观指标"},
        "endpoint": "/rmb_fx_index/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "fulldata", "type": "select", "required": False, "label": "完整字段", "default": "no", "options": [{"value": "no", "label": "默认字段"}, {"value": "yes", "label": "全部字段"}]}],
        "columns_zh": {"dt": "日期", "cfets_index_rate": "CFETS人民币汇率指数", "bis_index_rate": "BIS货币篮子人民币汇率指数", "sdr_index_rate": "SDR货币篮子人民币汇率指数"},
        "data_sources": [{"type": "parquet", "glob": p("rmb_fx_index_data/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "shibor_curve",
        "title": "Shibor 利率曲线",
        "summary": "Shibor 隔夜至一年期限利率曲线。",
        "group": {"level1": "金融数据", "level2": "利率曲线"},
        "endpoint": "/shibor_curve/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS,
        "columns_zh": {"dt": "日期", "shibor_on": "隔夜(O/N)", "shibor_1w": "1周", "shibor_2w": "2周", "shibor_1m": "1月", "shibor_3m": "3月", "shibor_6m": "6月", "shibor_9m": "9月", "shibor_1y": "1年"},
        "data_sources": [{"type": "parquet", "glob": p("shibor_curve_data/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "erp_hs300_10y",
        "title": "沪深300 ERP",
        "summary": "基于沪深300收益率和 10 年期国债收益率计算的 ERP 及 200 日通道。",
        "group": {"level1": "金融数据", "level2": "派生指标"},
        "endpoint": "/erp_hs300_10y/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "frequency", "type": "select", "required": False, "label": "频率", "default": "daily", "options": [{"value": "daily", "label": "日度"}, {"value": "weekly", "label": "周度"}]}],
        "columns_zh": {"dt": "日期", "hs300_close": "沪深300收盘", "hs300_ret_pct": "沪深300当日收益率(%)", "cn_gov_10y_yield_pct": "10年期国债收益率(年化,%)", "rf_daily_pct": "10年期国债当日收益率(折算,%)", "erp_daily_pct": "ERP_日度(%)", "erp_ma200_pct": "ERP_200日均线(%)"},
        "data_sources": [{"type": "parquet", "glob": p("erp_hs300_10y_data/*.parquet"), "date_column": "dt"}],
    },
    {
        "id": "sse_etf_shares",
        "title": "上交所 ETF 份额",
        "summary": "上交所 ETF 每日份额数据。",
        "group": {"level1": "金融数据", "level2": "ETF"},
        "endpoint": "/sse_etf_shares/data",
        "method": "GET",
        "params": COMMON_DATE_PARAMS + [{"name": "sec_codes", "type": "text", "required": False, "label": "ETF代码", "placeholder": "510300,588000"}, {"name": "etf_type", "type": "text", "required": False, "label": "ETF类型"}],
        "columns_zh": {"dt": "日期", "etf_type": "ETF类型", "sec_code": "证券代码", "quantity": "数量", "etf_name": "ETF名称", "total_volume_10k_shares": "总份额（万份）"},
        "data_sources": [{"type": "parquet", "glob": p("sse_etf_shares_data/*.parquet"), "date_column": "dt", "option_columns": ["etf_type"]}],
    },
    {
        "id": "szse_etf_shares",
        "title": "深交所 ETF 规模",
        "summary": "深交所 ETF 规模 SQLite 数据源。",
        "group": {"level1": "金融数据", "level2": "ETF"},
        "endpoint": "/szse_etf_shares/shares",
        "method": "GET",
        "params": COMMON_DATE_PARAMS,
        "columns_zh": {"dt": "日期", "code": "基金代码", "name": "基金名称", "index_code": "指数代码", "size": "规模(亿)", "manager": "基金公司"},
        "data_sources": [{"type": "sqlite", "path": p("SZSE_ETF_vol.sqlite"), "table": "etf_data", "date_column": "dt"}],
    },
]


def read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def normalize_date(value):
    if value is None or pd.isna(value):
        return None
    try:
        return pd.to_datetime(value).date().isoformat()
    except Exception:
        return None


def parquet_stats(source: dict) -> dict:
    files = [Path(item) for item in sorted(glob.glob(source["glob"], recursive=True))]
    total = 0
    min_date = None
    max_date = None
    options: dict[str, set] = {col: set() for col in source.get("option_columns", [])}
    date_col = source.get("date_column")

    for file in files:
        if not file.is_file() or file.suffix != ".parquet":
            continue
        try:
            cols = [date_col] + list(options) if date_col else None
            df = pd.read_parquet(file, columns=cols)
        except Exception:
            try:
                df = pd.read_parquet(file)
            except Exception:
                continue
        total += len(df)
        if date_col and date_col in df.columns and not df.empty:
            dates = pd.to_datetime(df[date_col], errors="coerce").dropna()
            if not dates.empty:
                local_min = dates.min().date().isoformat()
                local_max = dates.max().date().isoformat()
                min_date = local_min if min_date is None or local_min < min_date else min_date
                max_date = local_max if max_date is None or local_max > max_date else max_date
        for col in options:
            if col in df.columns:
                for item in df[col].dropna().astype(str).unique().tolist():
                    if item and item.lower() not in {"nan", "none", "null"}:
                        options[col].add(item)

    return {
        "row_count": total,
        "min_date": min_date,
        "max_date": max_date,
        "options": {col: [{"value": v, "label": v} for v in sorted(values)] for col, values in options.items()},
    }


def sqlite_stats(source: dict) -> dict:
    path = Path(source["path"])
    if not path.is_file():
        return {"row_count": 0, "min_date": None, "max_date": None, "options": {}}
    table = source["table"]
    date_col = source.get("date_column")
    with sqlite3.connect(path) as con:
        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        min_date = max_date = None
        if date_col:
            row = con.execute(f"SELECT MIN({date_col}), MAX({date_col}) FROM {table}").fetchone()
            min_date, max_date = normalize_date(row[0]), normalize_date(row[1])
    return {"row_count": row_count, "min_date": min_date, "max_date": max_date, "options": {}}


def merge_stats(chunks: list[dict], static_options: dict | None = None) -> dict:
    total = sum(int(item.get("row_count") or 0) for item in chunks)
    min_dates = [item.get("min_date") for item in chunks if item.get("min_date")]
    max_dates = [item.get("max_date") for item in chunks if item.get("max_date")]
    options: dict[str, list] = {}
    for item in chunks:
        for key, values in (item.get("options") or {}).items():
            seen = {opt["value"] for opt in options.get(key, [])}
            options.setdefault(key, [])
            for opt in values:
                if opt["value"] not in seen:
                    options[key].append(opt)
                    seen.add(opt["value"])
    for key, values in (static_options or {}).items():
        options[key] = values
    return {
        "row_count": total,
        "min_date": min(min_dates) if min_dates else None,
        "max_date": max(max_dates) if max_dates else None,
        "options": options,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def stats_for(api: dict) -> dict:
    chunks = []
    for source in api.get("data_sources", []):
        if source["type"] == "parquet":
            chunks.append(parquet_stats(source))
        elif source["type"] == "sqlite":
            chunks.append(sqlite_stats(source))
    return merge_stats(chunks, api.get("static_options"))


def update_one(default_api: dict) -> dict:
    path = API_META_DIR / f"{default_api['id']}.json"
    existing = read_json(path)
    merged = {**default_api, **existing}
    merged["stats"] = stats_for(merged)
    merged.setdefault("hidden_params", [])
    write_json(path, merged)
    return {
        "id": merged["id"],
        "title": merged["title"],
        "endpoint": merged["endpoint"],
        "group": merged.get("group", {}),
        "file": f"apis/{merged['id']}.json",
        "show_in_catalog": merged.get("show_in_catalog", True),
        "stats": merged["stats"],
    }


def main():
    entries = [entry for item in DEFAULT_APIS if (entry := update_one(item)).get("show_in_catalog", True)]
    catalog = {
        "title": "HuangDapao API Catalog",
        "base_url": "https://api.huangdapao.com",
        "health_endpoint": "/health",
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "apis": entries,
    }
    write_json(CATALOG_FILE, catalog)
    print(f"[api_showcase] updated {len(entries)} API metadata files.")
    print(f"[api_showcase] catalog: {CATALOG_FILE}")


if __name__ == "__main__":
    main()
