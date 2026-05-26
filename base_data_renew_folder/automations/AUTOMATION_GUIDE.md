# 数据自动化脚本说明

本目录存放当前仍在使用的数据抓取、清洗和 Parquet 入库脚本。旧的探针脚本、SQLite 中转脚本、临时产物和已废弃脚本已移动到 `archive/`。

## 目录结构

```text
base_data_renew_folder/automations/
  *.py                # 当前使用的数据自动化脚本
  AUTOMATION_GUIDE.md # 本说明文档
  archive/
    legacy/           # 已废弃旧脚本
    probes/           # 探针验证脚本
    artifacts/        # 历史临时 CSV/JSON 产物
```

## 运行方式

建议在仓库根目录运行脚本：

```powershell
C:\Users\16240\AppData\Local\Programs\Python\Python312\python.exe D:\GitHub\cmdstar.github.io\base_data_renew_folder\automations\index_daily_fetcher.py
```

脚本中的数据目录均已按当前目录结构修正，会写入：

```text
D:\GitHub\cmdstar.github.io\api\data\
```

## 公共工具

### `run_all_fetchers.py`

顺序运行当前活跃的数据抓取脚本，并记录每日运行日志。

运行：

```powershell
C:\Users\16240\AppData\Local\Programs\Python\Python312\python.exe D:\GitHub\cmdstar.github.io\base_data_renew_folder\automations\run_all_fetchers.py
```

输出日志：

```text
base_data_renew_folder/automations/logs/fetcher_daily_runs.csv
```

每个 fetcher 会追加一行，包含：

- 运行批次 `run_id`
- 成功/失败状态
- 解析出的更新/写入行数
- 退出码
- 开始/结束时间
- 完整 stdout/stderr
- 失败摘要

运行结束后，如果环境变量 `FEISHU_WEBHOOK_URL` 存在，会发送飞书富文本卡片，展示成功项、失败项和成功写入行数。

可调环境变量：

```text
FETCHER_TIMEOUT_SECONDS=7200
FETCHER_RETRY_SLEEP_SECONDS=5
FEISHU_WEBHOOK_URL=...
```

`FETCHER_RETRY_SLEEP_SECONDS` 会在批量运行时注入到各 fetcher 的重试等待环境变量中；单独运行某个 fetcher 时仍使用该脚本自己的默认值，除非手动设置对应环境变量。

### `parquet_incremental.py`

Parquet 月度切片与增量写入工具。

主要能力：

- 发现本地最新日期：`discover_latest_date`
- 按月 upsert Parquet：`upsert_monthly_parquet`
- 按日期列去重
- 默认使用 `zstd` 压缩

大多数时间序列脚本都依赖它。

## 活跃自动化脚本

### `index_daily_fetcher.py`

A 股主要指数日度行情。

写入：

```text
api/data/index_daily_data/
```

对应 API：

```text
/index_daily/data
```

特点：

- 东方财富日 K 数据。
- 按月切片。
- 支持 `INDEX_DAILY_FULL_REFRESH=1` 全量刷新。

### `global_market_daily_fetcher.py`

全球主要市场指数日度行情。

写入：

```text
api/data/global_market_daily_data/{symbol_key}/
```

对应 API：

```text
/global_market_daily/data
```

特点：

- Yahoo Finance 数据源。
- 按市场代码分目录，再按月切片。
- 使用交易所本地交易日。

### `sw_industry_daily_fetcher.py`

申万一级行业指数日度行情。

写入：

```text
api/data/sw_industry_daily_data/{swindexcode}/
```

对应 API：

```text
/sw_industry_daily/data
```

特点：

- 按行业代码分目录。
- 默认关闭该脚本对申万站点的 SSL 校验，可用 `SW_INDUSTRY_DAILY_VERIFY_SSL=1` 重新打开。
- 支持 Cookie/CSRF 环境变量。

### `sse_etf_shares_fetcher.py`

上交所 ETF 规模/份额数据。

写入：

```text
api/data/sse_etf_shares_data/
```

对应 API：

```text
/sse_etf_shares/shares
/sse_etf_shares/data
```

特点：

- 按单日请求。
- 按月切片。
- 首次日期可用 `SSE_ETF_SHARES_INITIAL_START` 控制。

### `ncd_aaa_yield_curve_fetcher.py`

NCD AAA 收益率曲线。

写入：

```text
api/data/ncd_aaa_yield_curve_data/
```

对应 API：

```text
/ncd_aaa_yield_curve/data
```

特点：

- 数据源通常只能请求近三个月。
- 首次无数据时自动使用近三个月作为基底。
- 支持多期限查询。

### `rmb_fx_index_fetcher.py`

人民币汇率指数。

写入：

```text
api/data/rmb_fx_index_data/
```

对应 API：

```text
/rmb_fx_index/data
```

特点：

- ChinaMoney 数据源。
- 按 364 天窗口分段请求。
- API 默认返回三条核心指数；`fulldata=yes` 返回文本展示列和请求区间列。

### `shibor_curve_fetcher.py`

Shibor 期限曲线。

写入：

```text
api/data/shibor_curve_data/
```

对应 API：

```text
/shibor_curve/data
```

特点：

- ChinaMoney Shibor CSV 数据源。
- 接口一次返回全量数据。
- 脚本全量解析后按月 upsert。

### `erp_hs300_10y_fetcher.py`

沪深300 ERP 指标。

写入：

```text
api/data/erp_hs300_10y_data/
```

对应 API：

```text
/erp_hs300_10y/data
```

特点：

- 拉取沪深300日线和 10 年期国债收益率。
- 计算 ERP 日度、200 日均线、200 日标准差和上下 2σ 通道。
- 每次重算近三年并 upsert，避免滚动窗口边界误差。
- API 支持 `frequency=daily|weekly`。

### `southbound_netbuy_fetcher.py`

南向资金净买入。

写入：

```text
api/data/capital_flow_data/southbound/
```

对应 API：

```text
/capital_flow/southbound/data
```

特点：

- Parquet 保留累计值、恒指收盘价和日净买入。
- API 默认返回日净买入核心列。
- `fulldata=yes` 返回完整字段。

### `northbound_dealamt_fetcher.py`

北向资金成交额与相关市场信息。

写入：

```text
api/data/capital_flow_data/northbound/
```

对应 API：

```text
/capital_flow/northbound/data
```

特点：

- 保存北向成交额、沪深股通成交额、指数点位、主导个股等字段。
- API 默认返回核心成交额字段。
- `fulldata=yes` 返回完整字段。

### `fund_new_issue_fetcher.py`

基金发行最新快照。

写入：

```text
api/data/fund_new_issue_data/
  fund_new_issue_YYYYMM.parquet
  fund_new_issue_pending.parquet
```

对应 API：

```text
/fund_new_issue/data
```

特点：

- 状态快照型数据，每次全量抓取并重建切片。
- 已成立基金按 `established_date` 月度切片。
- 尚未成立基金进入 `fund_new_issue_pending.parquet`。
- API 默认包含 pending 并排在前面。
- `unknown_*` 字段默认不返回，使用 `include_unknown=yes` 打开。

## 融资融券脚本

### `szse_margintotal_data.py`

深交所融资融券总量。

写入：

```text
api/data/margin_szse_tab1_data/
```

对应 API：

```text
/margin/szse/total
```

当前初始日期：

```text
2010-12-30
```

### `sse_margintotal_data.py`

上交所融资融券总量。

写入：

```text
api/data/margin_sse_tab1_data/
```

对应 API：

```text
/margin/sse/total
```

当前初始日期：

```text
2010-12-30
```

### `szse_margindetail_data.py`

深交所融资融券明细。

写入：

```text
api/data/margin_szse_tab2_data/
```

对应 API：

```text
/margin/szse/detail
```

### `sse_margindetail_data.py`

上交所融资融券明细。

写入：

```text
api/data/margin_sse_tab2_data/
```

对应 API：

```text
/margin/sse/detail
```

## 合并型 API

双融合并数据不再预生成独立 `merged` Parquet。

现在 API：

```text
/margin/merged/total
```

会动态读取：

```text
api/data/margin_szse_tab1_data/
api/data/margin_sse_tab1_data/
```

然后按 `dt` 做 inner join，只返回深交所和上交所都有数据的日期。

## 归档说明

`archive/legacy/` 中的脚本已在文件头标注归档或废弃原因。不要再接入自动化运行。

`archive/probes/` 中的脚本是数据源探针，用于验证接口结构和字段解析。正式链路已由对应 fetcher 接管。

`archive/artifacts/` 中是历史临时 CSV/JSON 文件，不参与 API 查询。

## 建议运行顺序

基础数据可独立运行。若要完整刷新当前 API 数据，建议按以下顺序：

1. 融资融券：`szse_margintotal_data.py`、`sse_margintotal_data.py`、明细脚本。
2. 市场行情：`index_daily_fetcher.py`、`global_market_daily_fetcher.py`、`sw_industry_daily_fetcher.py`。
3. 利率与曲线：`ncd_aaa_yield_curve_fetcher.py`、`rmb_fx_index_fetcher.py`、`shibor_curve_fetcher.py`。
4. 资金流：`southbound_netbuy_fetcher.py`、`northbound_dealamt_fetcher.py`。
5. 基金发行：`fund_new_issue_fetcher.py`。
6. 派生指标：`erp_hs300_10y_fetcher.py`。

其中 `erp_hs300_10y_fetcher.py` 依赖外部接口直接计算，不依赖本项目已落地的 Shibor/行情 Parquet，但建议在基础利率和行情数据之后运行，便于排查。
