# API Catalog And Fetcher Switches

本文记录数据自动化脚本与 API 展示页之间的开关机制，避免后续忘记某个接口为什么没有被自动更新或没有出现在 `api_showcase` 页面中。

## 放置位置

相关功能放在以下目录：

```text
base_data_renew_folder/automations/
```

原因是这些开关主要服务于自动化链路：

- `run_all_fetchers.py` 决定 GitHub Actions 批量更新时是否运行某个 fetcher。
- `update_api_showcase_metadata.py` 决定 API 展示页的 metadata 如何生成。
- `api_showcase/index.html` 只消费 metadata，不负责维护接口清单。

## 自动化运行开关

批量运行入口：

```text
base_data_renew_folder/automations/run_all_fetchers.py
```

单个 fetcher 可以配置：

```python
{
    "name": "SW industry daily",
    "script": "sw_industry_daily_fetcher.py",
    "enabled_env": "RUN_SW_INDUSTRY_DAILY",
    "default_enabled": False,
}
```

含义：

- `enabled_env`：控制该 fetcher 是否运行的环境变量。
- `default_enabled`：没有设置环境变量时的默认状态。
- `False`：默认不跑。
- `True`：默认运行。

当前 `SW industry daily` 默认关闭。要在 GitHub Actions 中重新启用，给 workflow env 加：

```yaml
RUN_SW_INDUSTRY_DAILY: "1"
```

如果关闭，批量任务会记录为成功跳过：

```text
skip SW industry daily: disabled by RUN_SW_INDUSTRY_DAILY; set RUN_SW_INDUSTRY_DAILY=1 to enable
```

这样可以避免某个暂时不稳定的数据源阻断整个数据更新链路。

## API 展示页开关

metadata 生成入口：

```text
base_data_renew_folder/automations/update_api_showcase_metadata.py
```

单个 API 可以配置：

```json
"show_in_catalog": false
```

含义：

- `true` 或不写：出现在 `api_showcase` 页面。
- `false`：保留该 API 的 metadata 文件，但不加入 `catalog.json`，页面也不会展示。

当前 `sw_industry_daily` 已设置：

```json
"show_in_catalog": false
```

对应文件仍会保留：

```text
api_showcase/metadata/apis/sw_industry_daily.json
```

但不会出现在：

```text
api_showcase/metadata/catalog.json
```

## 恢复 SW 行业 API 的步骤

如果之后申万数据源在 GitHub Actions 上稳定了，恢复方式如下：

1. 在 `update_api_showcase_metadata.py` 中把 `sw_industry_daily` 的 `show_in_catalog` 改为 `true`，或删除该字段。
2. 在 Actions 的 `Data Fetchers` env 中加入：

```yaml
RUN_SW_INDUSTRY_DAILY: "1"
```

3. 运行：

```powershell
C:\Users\16240\AppData\Local\Programs\Python\Python312\python.exe base_data_renew_folder\automations\update_api_showcase_metadata.py
```

4. 提交以下文件：

```text
base_data_renew_folder/automations/update_api_showcase_metadata.py
api_showcase/metadata/catalog.json
api_showcase/metadata/apis/sw_industry_daily.json
```

## 设计原则

这套开关分两层：

- 自动化层：控制是否更新数据。
- 展示层：控制是否在 API 展示页公开显示。

两者互不强制绑定。某个 API 可以继续保留后端接口和历史数据，但暂时不参与自动化更新，也不出现在展示页中。

