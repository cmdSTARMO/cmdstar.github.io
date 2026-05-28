# 用GitHub + Actions + Render零成本搭造一个金融数据中心

——从Excel周报到零成本API数据系统的工程化实践

> 本项目隆重感谢Codex与Gemini的支持

## 0.开始的开始

该项目衍生于市场周报项目，又逐渐演变为一个独立的API项目。

自五月初起，最开始我只是想做一套自己的市场周报系统。Excel里有PowerQuery，有图表，有宏，有一堆从不同网站扒下来的数据源。刚开始这套东西很好用，点一下刷新，图表更新，周报也能继续写。

但数据源越来越多之后，问题开始变得很明显：

- 每次做周报都要**重新请求**所有数据源。
- 某个接口慢了，整个Excel都**卡住**。
- 某个网页改了字段，PowerQuery**就炸**。
- 历史数据反复下载，完全**没有必要**。
- 周报、图表、分析逻辑和数据获取**绑得太死**。

后来我意识到，这不是Excel的问题，而是架构的问题。

所以这个项目真正想解决的是：

>**如何把“数据获取”和“数据分析”拆开。**

拆开之后，**数据可以长期维护，分析可以稳定复用**，周报也不需要每周重新碰一堆外部数据源。

整个系统就是围绕这个目标慢慢长出来的。

![图1](/contents/blogs/blog-folder/api_yes/1.png)

![图2：旧版Excel/PowerQuery工作流截图](/contents/blogs/blog-folder/api_yes/2.png)

## 1.在正式开始之前：API到底是什么？

如果完全不懂后端，其实可以先把API理解成“**程序之间的数据交流方式**”。

平时我们打开网页，浏览器返回的是一个页面；而访问API时，服务器返回的通常不是给人看的页面，而是一段给程序看的数据。最常见的格式就是JSON，看起来大概像这样：

```json
{
  "status":"ok"
}
```

这段内容本身不复杂。关键在于：为什么访问一个URL会返回它？

在这个项目里，答案是FastAPI。FastAPI就像一个接待员，它提前知道“哪个URL对应哪个处理函数”。当用户访问某个API地址时，FastAPI会接收请求、找到对应路由、执行里面的代码，然后把结果包装成HTTP响应返回。

最简单的流程是这样：

![图3](/contents/blogs/blog-folder/api_yes/3.png)

比如访问：

```text
https://api.huangdapao.com/health
```

背后并没有复杂数据库，也没有复杂金融计算。它只是触发了`api/main.py`里预先定义好的一个函数：

```python
@app.get("/health",summary="Health check")
async def health_check()->dict:
    return {"status":"ok"}
```

这段代码的意思可以翻译成人话：

>如果有人访问`/health`，就返回`{"status":"ok"}`。

所以`/health`是理解API最好的入口。它没有业务逻辑，却完整展示了API的基本工作方式。

![图4](/contents/blogs/blog-folder/api_yes/4.png)


复杂一点的金融数据接口也是同一个原理。只是`/health`直接返回固定JSON，而`/global_market_daily/data`这类接口会先解析参数，再读取Parquet文件，最后把查询结果返回成JSON或CSV。

所以这篇文章里说的**API，本质上就是一组稳定的URL**。Excel、网页、Python脚本都可以访问它们，并拿到结构化数据。

## 2.从PowerQuery到数据中心

这个项目最早并不是从“我要做一个API服务”开始的。

更早的时候，它其实就是Excel里的PowerQuery。

PowerQuery到底是什么？放在这个项目里看，它本质上就是Excel内置的一套数据获取和整理工具。它可以访问网页、请求API、读取文件、清洗字段、转换类型，最后把数据变成Excel里的表格。

也就是说，**PowerQuery本身已经在做一件很像爬虫的事情**：请求网络数据、拿到返回内容、解析结构、整理成表格。它和Python里用`requests`拉接口没有本质区别，只是一个在Excel里面完成，一个在脚本里面完成。

所以**真正的变化，并不是“PowerQuery升级成API”**。

真正的变化是：我开始意识到，这些每周都会被请求、被清洗、被使用的数据，不应该只是周报生成过程中的临时中间产物。

以前的工作流更像这样：

![图5](/contents/blogs/blog-folder/api_yes/5.png)

这个流程能跑，但它有一个问题：**数据没有自己的生命周期**。

只要周报结束，这次请求到的数据就基本结束了。下周再做一次周报，Excel又重新去请求一遍。数据源慢了就等，字段变了就修，历史数据也不断重复下载。

后来思路变成了：

![图6](/contents/blogs/blog-folder/api_yes/6.png)

这一步很关键。

**数据不再只是Excel的附属品，而是变成了一个独立层**。它可以自己更新、自己存储、自己被查询、自己被版本管理。Excel只是它的一个使用者，后面的Python图表、Markdown周报、本地大模型评论，也都可以成为它的使用者。

换句话说，项目的核心演化不是“Excel不够高级，所以换成API”。

而是：

>**既然数据已经被我获取到了，为什么不把它真正沉淀成自己的数据中心？**

![图7：PowerQuery查询编辑器截图，直接在Excel里请求外部数据的状态](/contents/blogs/blog-folder/api_yes/7.png)

## 3.为什么没有继续直接用数据库

项目中间其实走过一段SQLite路线。

最初的数据流大概是这样：

![图8](/contents/blogs/blog-folder/api_yes/8.png)

这条路一开始很自然。

**SQLite对个人项目非常友好**：不用部署数据库服务，不用维护账号权限，不用考虑网络连接。一个`.sqlite`文件放在本地，脚本写进去，Excel或者API读出来，简单直接。当时数据量也不大，所有东西放进一个本地库里，反而是最省事的方案。

所以最开始选择SQLite并不是错的。它解决了早期最重要的问题：

- 本地化。
- 简单。
- 不需要额外服务。
- 单人项目容易管理。
- 适合小规模结构化数据。

但问题是，数据系统一旦开始长大，SQLite单体库的缺点也会一起长出来。

尤其是SSETAB2这类明细数据进来以后，数据量明显变大。单个SQLite文件越来越重，GitHub提交开始变慢，GitHubLFS开始介入，数据更新脚本也越来越像是在围绕一个大文件打补丁。

慢慢地，我发现这个结构不太适合长期维护：

- SQLite文件越来越大。
- GitHub不适合频繁提交大型二进制数据库。
- 单体库只要变动一次，版本系统就要处理整个大文件。
- 数据更新和数据切片被拆成两步，流程变长。
- 如果某个表出了问题，排查也要进入同一个大库里看。
- 这个库越重要，后面越不敢随便改结构。

那时候系统其实已经出现了一个信号：我不是缺数据库能力，而是**不应该让整个项目围绕一个越来越大的中心数据库旋转**。

如果继续走“大型单体SQLite”路线，短期还能用，但长期一定会越来越难维护。这个项目需要的不是一个更重的数据库，而是一个**更适合GitHub、自动提交、增量更新和API读取的数据仓库结构**。

这也是后来转向Parquet分片的原因。

![图9：旧SQLite文件](/contents/blogs/blog-folder/api_yes/9.png)


## 4.为什么最后转向Parquet分片

**Parquet在这个项目里不只是一个“文件格式”**。

更准确地说，它是这套免费系统能够长期运行的重要基础设施。

因为这个项目的核心约束很特殊：数据要能被GitHub管理，要能被Actions自动更新，要能被Render上的API快速读取，还不能让每次提交都变成一个巨大的数据库文件。

Parquet刚好适合这个场景：

- 文件体积小。
- 读取速度快。
- 字段类型稳定。
- 适合时间序列。
- 适合增量更新。
- 适合按月份切片。
- 适合GitHub仓库管理。
- 适合长期历史数据维护。

**真正的转折点不是“用了Parquet”，而是“按月切片”**。

类似这样：

```text
api/data/sse_tab2/
├──sse_tab2_202501.parquet
├──sse_tab2_202502.parquet
└──sse_tab2_202503.parquet
```

或者再按标的拆一层：

```text
api/data/global_market_daily_data/
├──DJI/
│  ├──global_market_daily_DJI_202505.parquet
│  └──global_market_daily_DJI_202506.parquet
└──SPX/
   ├──global_market_daily_SPX_202505.parquet
   └──global_market_daily_SPX_202506.parquet
```

这种结构出现以后，系统终于不再依赖一个巨大的中心数据库，而是开始拥有一个“**可分段管理的数据仓库**”。

它对存储友好，也对API查询友好。

因为API并不需要每次扫描全部历史数据。用户传入`startdate/enddate`之后，查询层可以按月份定位需要读取的Parquet文件，再交给DuckDB执行过滤、排序、分页和CSV导出。

也就是说，Parquet分片同时解决了两件事：

- **数据更新时只动少量文件**。
- **API查询时只读相关切片**。

这对免费系统很重要。因为免费资源不是无限的，GitHub提交不能太重，Actions不能跑太久，Render也不适合做很重的在线计算。分片之后，每一层压力都下降了。

## 5.项目现在长什么样

当前仓库不是单纯的后端项目，也不是单纯的静态网站。它更像一个把数据、自动化、API和展示页面放在一起的小型数据工程仓库。

核心目录大概是这样：

```text
/
├──api/
│  ├──main.py                     #FastAPI入口，注册所有API路由
│  ├──requirements.txt             #Render/API服务依赖
│  ├──Dockerfile                   #容器化部署参考，当前Render主要依赖启动命令
│  ├──retours/                     #各类API查询模块
│  │  ├──capital_flow.py
│  │  ├──fund_new_issue.py
│  │  ├──global_market_daily.py
│  │  ├──ncd_aaa_yield_curve.py
│  │  ├──rmb_fx_index.py
│  │  ├──shibor_curve.py
│  │  ├──sse_etf_shares.py
│  │  ├──sw_industry_daily.py
│  │  └──margin/
│  └──data/                        #真正的数据中心，Parquet/SQLite都在这里
├──base_data_renew_folder/
│  └──automations/                 #数据采集、清洗、入库、统一调度脚本
│     ├──run_all_fetchers.py        #GitHubActions批量更新入口
│     ├──parquet_incremental.py     #按月切片和upsert工具
│     ├──update_api_showcase_metadata.py
│     └──logs/fetcher_daily_runs.csv
├──api_showcase/
│  ├──index.html                   #API展示页
│  └──metadata/                    #展示页读取的接口清单和字段说明JSON
├──.github/workflows/
│  ├──data-fetchers.yml            #数据自动更新workflow
│  ├──sw-industry-debug.yml        #申万接口单独排查workflow
│  └──site-build.yml               #GitHubPages静态页面部署
├──教程.md                         #系统入门说明
└──api_yes.md                      #本文
```

这个结构的重点是：`api/data/`是数据中心，`base_data_renew_folder/automations/`负责更新数据，`api/retours/`负责把数据变成HTTP接口，`api_showcase/`负责把接口展示给人看。

## 6.免费架构为什么成立

这个项目一开始就有一个很现实的约束：尽量不花钱。

如果为这个系统单独买服务器、数据库、对象存储、调度平台，那维护成本会很快超过项目本身的意义。最后它变成了一套“白嫖但不糊弄”的组合。

![图10](/contents/blogs/blog-folder/api_yes/10.png)


这里的“免费”不是说系统没有任何代价。

更准确地说，它是在免费额度和免费能力之内，把每个服务放在它最擅长的位置上。GitHub负责存储和版本，Actions负责调度，Pages负责静态展示，Render负责在线API，飞书负责提醒。每一层都不重，但组合起来就能跑出一个长期可用的数据系统。

这个设计的核心不是无限扩张，而是在**免费资源限制内尽量优化结构**。

### GitHub仓库承担什么

GitHub在这里不是只放代码。它同时承担了：

- 数据文件存储。
- Parquet版本管理。
- 自动化更新记录。
- 历史回滚能力。
- 多端同步能力。
- API展示页源码托管。

这也是为什么我没有一开始就上数据库。对个人项目来说，**GitHub仓库加Parquet已经足够支撑一个轻量数据中心**。

更重要的是，GitHub天然适合“留下痕迹”。每次数据更新、每次metadata变化、每次日志追加，都会变成一次commit。它不是专业数仓，但对个人项目来说，这种可追踪性非常有用。

### GitHubActions承担什么

自动更新主要靠：

```text
.github/workflows/data-fetchers.yml
```

当前配置支持手动触发，也支持工作日定时触发：

```yaml
on:
  workflow_dispatch:
  schedule:
    - cron: "0 0 * * 1-5"
```

这个时间对应北京时间工作日早上8点。

核心步骤是：

```yaml
- name: Run fetcher batch
  run: |
    test -f base_data_renew_folder/automations/run_all_fetchers.py
    python base_data_renew_folder/automations/run_all_fetchers.py
```

跑完之后会把更新后的`api/data`、运行日志和`api_showcase/metadata`提交回仓库：

```yaml
if [ -d api/data ];then
  git add api/data
fi
if [ -f base_data_renew_folder/automations/logs/fetcher_daily_runs.csv ];then
  git add base_data_renew_folder/automations/logs/fetcher_daily_runs.csv
fi
if [ -d api_showcase/metadata ];then
  git add api_showcase/metadata
fi
```

也就是说，**Actions不是只跑脚本，它还负责把数据中心更新后的状态重新写回GitHub**。

目前整个批量更新脚本被控制在一个比较轻的运行规模里。`data-fetchers.yml`的job超时是60分钟，单个fetcher通过`FETCHER_TIMEOUT_SECONDS`控制在900秒以内；剔除已暂时关闭的申万调试源后，主流程就是按每天不到15分钟的运行窗口来设计的。

这点很关键。GitHubActions每月有2000分钟免费额度，如果每天工作日运行一次，单次十几分钟以内（实际下来单次运行耗费12分钟左右），整体就能长期处在免费额度范围内。

![图11：GitHub Actions成功运行截图](/contents/blogs/blog-folder/api_yes/11.png)

### Render承担什么

Render负责运行真正的API服务。仓库里可以看到`api/requirements.txt`和`api/Dockerfile`，Render实际部署时使用FastAPI+Uvicorn启动服务。

部署日志里对应的启动方式是：

```bash
[ -d api ]&&cd api;uvicorn main:app --host 0.0.0.0 --port $PORT --workers 2
```

Render免费版的特点也很明确：闲置后会休眠，第一次访问会冷启动。所以它不适合高并发生产服务，但非常适合个人数据API、Excel调用、周报数据源这种低频场景。

这一层非常重要。

因为数据放在GitHub里，并不会自动变成API。**真正让这些Parquet、SQLite和metadata变成可访问接口的，是`api/`目录里的FastAPI服务**。

实际关系更像这样：

![图12](/contents/blogs/blog-folder/api_yes/12.png)

Render只负责让服务在线运行，数据仍然来自仓库。每次Actions更新数据并push之后，Render在下一次部署或拉取最新代码后，就能把新数据暴露成HTTP接口。

仓库中`api/Dockerfile`也保留了容器化部署方式：

```dockerfile
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10
WORKDIR /app/api
COPY . /app/api
RUN pip install --no-cache-dir -r requirements.txt
```

当前实际启动命令更直接：进入`api`目录后用`uvicorn main:app`启动。Dockerfile更像是一个可迁移的部署备份：以后如果Render部署方式变成Docker，它也能继续支撑。

![图13：Render服务部署页面截图，启动命令与部署状态](/contents/blogs/blog-folder/api_yes/13.png)

![图14：Dockerfile截图，展示API目录被复制进容器并安装requirements](/contents/blogs/blog-folder/api_yes/14.png)

### GitHubPages承担什么

API展示页是静态页面，放在：

```text
api_showcase/index.html
```

它不需要后端服务器，只需要读取`api_showcase/metadata/`里的JSON文件，然后在浏览器里生成接口目录、参数输入、请求链接、CSV按钮和前五条数据预览。

静态页面托管在GitHubPages上，几乎没有额外成本。

### 飞书机器人承担什么

自动化跑完之后，如果环境变量里有：

```text
FEISHU_WEBHOOK_URL=***
```

`run_all_fetchers.py`会把结果推送到飞书。通知里包含运行批次、成功/失败数量、写入行数、失败摘要和CSV日志位置。

这里没有把webhook写在仓库文件里~而是通过GitHubSecrets注入（不然被人偷了


![图15：飞书机器人推送截图 - 自动更新后的成功](/contents/blogs/blog-folder/api_yes/15.png)

![图16：飞书机器人推送截图 - 自动更新后的失败](/contents/blogs/blog-folder/api_yes/16.png)

## 7.Parquet按月切片在代码里怎么落地

一开始如果只是几张表，CSV当然够用。但金融时间序列越积越多以后，CSV的问题会越来越明显：文件大、提交慢、读取慢、字段类型不稳定。

所以现在系统主要使用Parquet。

公共写入工具是：

```text
base_data_renew_folder/automations/parquet_incremental.py
```

核心逻辑可以概括为几步：

```python
def upsert_monthly_parquet(df,parquet_dir,filename_template,key_cols,date_col="dt",sort_cols=None):
    out=df.copy()
    out[date_col]=pd.to_datetime(out[date_col],errors="coerce").dt.date
    for yyyymm,month_df in out.groupby(out[date_col].map(month_key)):
        target=os.path.join(parquet_dir,filename_template.format(yyyymm=yyyymm))
        old=pd.read_parquet(target)if os.path.isfile(target)else pd.DataFrame()
        merged=pd.concat([old,month_df],ignore_index=True)
        merged=merged.drop_duplicates(subset=list(key_cols),keep="last")
        merged.to_parquet(tmp,index=False,compression="zstd")
        os.replace(tmp,target)
```

它做了几件事：

- 把日期统一成`date`。
- 按`yyyymm`分组。
- 读取已有月份文件。
- 新旧数据合并。
- 按主键去重。
- 用`zstd`压缩写回Parquet。

典型目录长这样：

```text
api/data/global_market_daily_data/DJI/global_market_daily_DJI_202605.parquet
api/data/fund_new_issue_data/fund_new_issue_202605.parquet
api/data/capital_flow_data/southbound/capital_flow_southbound_202605.parquet
api/data/shibor_curve_data/shibor_curve_202605.parquet
```

有些数据还会先按标的分目录，再按月份切片。比如全球市场指数：

```text
api/data/global_market_daily_data/
├──DJI/
├──IXIC/
├──SPX/
└──N225/
```

这样做的好处是，某一天只更新少量文件，不会每次都重写一个巨大的总表。对GitHub这种版本仓库来说，这一点很重要。

## 8.为什么最后用了DuckDB？

Parquet解决了“数据怎么存”的问题，但API还需要解决“数据怎么查”的问题。

一开始很容易想到继续用数据库：把Parquet导进去，或者重新维护一套查询库。但这样又会把系统带回原来的复杂度。这个项目真正需要的不是一个常驻数据库服务，而是一层能快速查询Parquet文件的轻量查询引擎。

这就是DuckDB适合这个项目的地方。

DuckDB最关键的能力是：它可以直接查询Parquet文件。

也就是说，数据不需要先导入MySQL、PostgreSQL或者另一个SQLite库。FastAPI拿到请求参数后，可以根据日期找到需要读取的月份文件，然后让DuckDB直接对这些文件执行SQL。

![图17](/contents/blogs/blog-folder/api_yes/17.png)

这对当前系统非常合适。因为大多数金融数据查询都是按时间范围来的：最近一年、某个月、某几个交易日。Parquet按月切片以后，DuckDB只需要读取相关文件，再做`WHERE dt BETWEEN ...`、`ORDER BY`、`LIMIT/OFFSET`这类操作。

在代码里可以看到很多类似逻辑：

```python
union_sql=" UNION ALL ".join(["SELECT * FROM read_parquet(?)"for _ in month_files])
```

这行看起来普通，但它其实让整个系统轻了很多。**没有常驻数据库，没有导入步骤，没有额外数据同步**，API服务直接把文件当作查询对象。

所以DuckDB在这里不是为了“显得高级”，而是刚好填上了Parquet和FastAPI之间的空位：**Parquet负责存，DuckDB负责查，FastAPI负责对外返回**。

## 9.为什么用了Docker？

仓库里有：

```text
api/Dockerfile
```

这并不意味着这篇文章要变成Docker教程。放在这个项目里，**Docker的作用比较明确：把API服务运行环境封起来**。

本地能跑，不代表Render上一定能跑；Windows能跑，也不代表Linux服务器一定能跑。Python版本、依赖库、工作目录、启动命令，只要有一个不一致，就可能出现“我电脑上没问题，部署上炸了”的情况。

Dockerfile做的事情就是把这些环境约束写清楚：

```dockerfile
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.10
WORKDIR /app/api
COPY . /app/api
RUN pip install --no-cache-dir -r requirements.txt
```

它表达了几个事实：

- API服务运行在Python/FastAPI环境中。
- 工作目录是`/app/api`。
- 部署时复制`api/`目录。
- 依赖来自`requirements.txt`。

当前Render实际启动可以直接用`uvicorn main:app`，不一定每次都走Docker镜像。但Dockerfile保留了一个很重要的能力：如果未来要换部署方式，或者需要更稳定地复现环境，API服务可以比较自然地容器化。

在这套系统里，Docker不是主角。它更像部署层的保险：让FastAPI服务有一个可复制、可迁移的运行环境。

## 10.为什么最后没有继续使用传统后端架构

走到这里时，其实还有另一条路可以选：搭一套更传统的后端系统。

比如租一台云服务器，装Nginx，跑FastAPI或其他后端服务，再配MySQL、Redis、定时任务、数据库备份、日志服务。这个路线当然没有问题，很多商业系统也确实应该这样做。

但回到这个项目本身，我后来发现**它真正需要的并不是一个复杂的中心后端**。它的数据访问量不高，用户主要是我自己的Excel、API展示页、后续周报脚本和少量浏览器访问。它的核心压力也不是高并发，而是长期维护、低成本更新、历史数据稳定保存和随时可追踪。

所以这套系统最后没有继续往“MySQL+Redis+Nginx+云服务器”的方向走，而是变成了**几个轻量组件的组合**：

![图18](/contents/blogs/blog-folder/api_yes/18.png)

这里每一层都只承担自己最小必要的职责。GitHub负责保存数据和版本历史，Parquet负责把历史数据拆成可管理的文件，DuckDB负责在文件上做查询，FastAPI负责把查询结果变成HTTP接口，Render负责让接口能被公网访问，GitHubActions负责每天把数据更新一遍。

这不是传统意义上的大型后端系统，也不适合拿去承载高并发商业服务。但对个人金融数据项目来说，它的优点很实际：**结构轻、成本低、可追踪、可迁移**，而且每一层坏了都比较容易定位。

也正因为它不是一个重型后端，我才可以把精力放在数据本身：哪些数据该收集、如何增量更新、字段怎么标准化、API怎么更容易被Excel和周报系统调用。

## 11.API服务层：数据文件是怎么真正变成API的

数据中心只解决“数据在哪里”的问题。

但API服务层解决的是另一个问题：这些数据怎么被Excel、浏览器、脚本、API展示页统一访问。

这个项目里，服务层主要由三部分组成：

- FastAPI负责路由和HTTP响应。
- DuckDB负责高效读取Parquet。
- Render负责把`api/`目录部署成公网服务。

从运行角度看，Render会在部署时进入`api`目录，安装`api/requirements.txt`里的依赖，然后执行类似这样的启动命令：

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT --workers 2
```

这里的`main:app`指向`api/main.py`里的FastAPI实例；`--host 0.0.0.0`表示服务对容器外部可访问；`$PORT`是Render分配给服务的端口。启动完成后，Render会把外部访问`api.huangdapao.com`的请求转发到这个Uvicorn/FastAPI进程。

整体直觉可以画成这样：

![图19](/contents/blogs/blog-folder/api_yes/19.png)

如果把这套系统拆成人话，FastAPI做的事情主要有三件：

第一，它定义URL。比如`/health`、`/global_market_daily/data`、`/capital_flow/southbound/data`这些路径，都是在FastAPI应用里注册出来的。

第二，它接收参数。用户在URL里传入`startdate`、`enddate`、`format`、`limit`之类的参数，FastAPI会把这些参数交给对应的处理函数。

第三，它返回结果。处理函数查完数据后，FastAPI会把Python字典、列表或者CSV响应变成浏览器和Excel都能接收的HTTP结果。

大致结构是：

![图20](/contents/blogs/blog-folder/api_yes/20.png)

API入口是：

```text
api/main.py
```

里面注册了各类路由：

```python
app.include_router(szse_etf_shares.router,prefix="/szse_etf_shares",tags=["SZSE ETF shares"])
app.include_router(sse_etf_shares.router,prefix="/sse_etf_shares",tags=["SSE ETF shares"])
app.include_router(margin_router)
app.include_router(ncd_aaa_yield_curve.router,prefix="/ncd_aaa_yield_curve",tags=["NCD AAA Yield Curve"])
app.include_router(global_market_daily.router,prefix="/global_market_daily",tags=["Global Market Daily"])
app.include_router(capital_flow.router)
app.include_router(fund_new_issue.router,prefix="/fund_new_issue",tags=["Fund New Issue"])
app.include_router(rmb_fx_index.router,prefix="/rmb_fx_index",tags=["RMB FX Index"])
app.include_router(shibor_curve.router,prefix="/shibor_curve",tags=["Shibor Curve"])
app.include_router(erp_hs300_10y.router,prefix="/erp_hs300_10y",tags=["ERP HS300 10Y"])
```

这意味着一个接口通常会分成两层：

第一层是`main.py`注册路由前缀，例如`/global_market_daily`。

第二层是`api/retours/`下面的具体模块，负责参数解析、文件定位、DuckDB查询、字段中文解释、JSON/CSV响应。

查询层大量使用DuckDB读取Parquet。以全球市场日度数据为例，接口会根据`startdate/enddate/symbols`定位对应月份文件，然后拼成`read_parquet`查询：

```python
union_sql=" UNION ALL ".join(["SELECT * FROM read_parquet(?)"for _ in month_files])
sql=f"""
SELECT market_name,symbol,dt,datetime_local,open,close,high,low,volume,adj_close
FROM({union_sql})
WHERE dt BETWEEN ? AND ?
ORDER BY dt DESC,symbol ASC
LIMIT ? OFFSET ?
"""
```

这种写法的好处是，API不需要先把所有Parquet读进Pandas。DuckDB可以直接对文件执行SQL，日期过滤、排序、分页都在查询层完成。

返回结果统一包含`meta`和`data`。`meta.columns_zh`用于告诉用户英文字段对应的中文含义。

```json
{
  "meta":{
    "query_time":"2026-05-28 08:00:00",
    "data_range":{"start_date":"2026-01-01","end_date":"2026-05-28"},
    "columns_zh":{"dt":"交易日期","close":"收盘"},
    "pagination":{"limit":2000,"offset":0,"returned":5,"has_more":false}
  },
  "data":[]
}
```

接口同时支持`format=json`和`format=csv`。CSV输出由：

```text
api/retours/export_utils.py
```

统一处理。

所以一个典型请求会经历这样的链路：

![图21](/contents/blogs/blog-folder/api_yes/21.png)

这也是为什么我把API层单独放在`api/`目录里。Render部署时可以直接进入这个目录启动服务，GitHubPages则只负责静态展示，两者互不混在一起。

### 健康检查和简单API

项目里还有两个体量很小、但很有用的接口。

`/health`在`api/main.py`里：

```python
@app.get("/health",summary="Health check")
async def health_check()->dict:
    return {"status":"ok"}
```

它的作用不是返回业务数据，而是确认FastAPI服务是否已经启动。API展示页的悬浮状态栏也会用它判断Render服务是否可用、是否还在冷启动。

另一个是测试路由：

```text
/foo/ping
```

它来自`api/retours/foo.py`，更像开发阶段的连通性测试接口。

这些接口很小，但它们让这个系统更像一个真正运行中的API服务，而不是单纯放了一堆静态数据文件。

![图22：FastAPI自动生成的docs页面](/contents/blogs/blog-folder/api_yes/22.png)

![图23：API返回JSON示例：meta、columns_zh与data结构](/contents/blogs/blog-folder/api_yes/23.png)


## 12.一次真实API请求会发生什么？

前面分别讲了FastAPI、DuckDB和Parquet，但真正访问API时，它们不是分开工作的，而是在一次请求里连续协作。

用一个实际接口举例：

```text
https://api.huangdapao.com/shibor_curve/data?startdate=2025-01-01&enddate=2025-03-31&limit=2000&format=json
```

这个请求的意思是：查询2025年1月1日到2025年3月31日之间的Shibor利率曲线，并用JSON格式返回。

系统内部大概会走完下面这条路：

![图24](/contents/blogs/blog-folder/api_yes/24.png)

第一步，Render把公网请求转给正在运行的FastAPI服务。这里用户看到的是一个域名和URL，服务内部看到的是一个HTTP请求。

第二步，FastAPI根据路径找到对应路由。`/shibor_curve`这个前缀是在`api/main.py`里注册的，真正处理`/data`查询逻辑的是`api/retours/shibor_curve.py`。这一步相当于把请求分发到正确的查询模块。

第三步，接口函数会读取URL参数，例如`startdate=2025-01-01`和`enddate=2025-03-31`。这些参数决定了要查哪段时间，也决定了后面需要读取哪些月份的Parquet文件。

第四步，DuckDB开始工作。它不需要把Parquet先导入数据库，而是可以直接执行类似`read_parquet(?)`的查询。对于这个例子来说，查询层会尽量只读取2025年1月到3月相关的切片文件，再做日期过滤、排序、分页。

第五步，查询结果会被整理成统一结构。这个项目里的数据接口通常不只返回`data`，还会返回`meta`，里面包含查询时间、数据范围、分页信息和`columns_zh`字段中文解释。这样Excel或浏览器拿到结果后，不只是有数据，也能知道每一列是什么意思。

如果用户在API页面上点“预览数据”，前端页面会发出类似请求，只是自动限制`limit`，然后把返回的前几条记录渲染成表格。如果用户点“下载CSV”，页面会把`format`切换成`csv`，FastAPI就会返回CSV文件。

所以一次真实API请求并不是某个组件单独完成的。它更像一条轻量流水线：**Render负责入口，FastAPI负责路由和响应，DuckDB负责查询，Parquet负责存储**，API页面或Excel负责消费结果。

![图25](/contents/blogs/blog-folder/api_yes/25.png)

这也是这套系统后来变轻的原因：它没有依赖一个巨大的中心服务器来处理所有事情，而是让**几个小组件在正确的位置上各自完成一小段工作**。

## 13.目前咱有哪些API

下面这部分不是完整APIReference，而是从当前仓库真实路由和metadata整理出来的项目视角说明。

### API名称：**融资融券余额合并**

- 接口路径：`/margin/merged/total`
- 数据来源：深交所融资融券总量和上交所融资融券总量。
- 数据类型：日度余额数据。
- 主要用途：合并观察两市融资余额、融券余额和总量变化。
- 核心参数：`startdate`、`enddate`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 数据更新方式：由双融fetcher更新底层Parquet。
- 存储位置：`api/data/margin_szse_tab1_data/`和`api/data/margin_sse_tab1_data/`。
- 对应脚本：`szse_margintotal_data.py`、`sse_margintotal_data.py`。
- 系统角色：这是一个合并型API，不额外生成合并Parquet，而是在查询时动态读取两边数据并按日期合并。

![图26：融资融券合并API页面](/contents/blogs/blog-folder/api_yes/26.png)

### API名称：**深交所融资融券总量**

- 接口路径：`/margin/szse/total`
- 数据来源：深交所融资融券总量数据。
- 数据类型：日度总量。
- 主要用途：查询深交所融资余额、融资买入额、融券余额等指标。
- 核心参数：`startdate`、`enddate`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/margin_szse_tab1_data/`。
- 对应脚本：`base_data_renew_folder/automations/szse_margintotal_data.py`。
- 数据获取方式：脚本保留原有请求路径，按最新日期增量拉取并写入Parquet。

### API名称：**上交所融资融券总量**

- 接口路径：`/margin/sse/total`
- 数据来源：上交所融资融券总量数据。
- 数据类型：日度总量。
- 主要用途：查询上交所融资余额、融资买入额、融资偿还额、融券余额等指标。
- 核心参数：`startdate`、`enddate`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/margin_sse_tab1_data/`。
- 对应脚本：`base_data_renew_folder/automations/sse_margintotal_data.py`。

### API名称：**深交所融资融券明细**

- 接口路径：`/margin/szse/details`
- 数据来源：深交所融资融券明细。
- 数据类型：按证券代码拆分的日度明细。
- 主要用途：查询单只证券或多只证券的融资融券状态。
- 核心参数：`startdate`、`enddate`、`codes`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/margin_szse_tab2_data/`。
- 对应脚本：`szse_margindetail_data.py`。

### API名称：**上交所融资融券明细**

- 接口路径：`/margin/sse/details`
- 数据来源：上交所融资融券明细。
- 数据类型：按证券代码拆分的日度明细。
- 主要用途：查询上交所单证券融资融券明细。
- 核心参数：`startdate`、`enddate`、`codes`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/margin_sse_tab2_data/`。
- 对应脚本：`sse_margindetail_data.py`。

### API名称：**全球主要市场指数日度行情**

- 接口路径：`/global_market_daily/data`
- 数据来源：YahooFinanceChartAPI。
- 数据类型：全球主要指数日度OHLCV。
- 主要用途：给周报提供海外市场指数、波动率指数、主要区域股指表现。
- 核心参数：`startdate`、`enddate`、`symbols`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/global_market_daily_data/{symbol_key}/`。
- 对应脚本：`global_market_daily_fetcher.py`。
- 获取方式：通过`https://query1.finance.yahoo.com/v8/finance/chart/{symbol} `请求，使用`period1/period2`控制日期范围。
- 系统角色：这是Excel周报里非常常用的市场行情层数据。

![图27：ChromeDevToolsNetwork截图 - YahooFinanceChart请求地址与参数](/contents/blogs/blog-folder/api_yes/27.png)

### API名称：**同业存单AAA收益率曲线**

- 接口路径：`/ncd_aaa_yield_curve/data`
- 数据来源：中国货币网收益率曲线接口。
- 数据类型：不同期限的收益率曲线。
- 主要用途：查询NCDAAA期限利率，用于利率观察和资金面分析。
- 核心参数：`startdate`、`enddate`、`term_year`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/ncd_aaa_yield_curve_data/`。
- 对应脚本：`ncd_aaa_yield_curve_fetcher.py`。
- 特殊处理：脚本里使用`LegacyTLSAdapter`处理中国货币网的TLS兼容问题；首次无数据时默认只往前取近三个月左右作为基底。

### API名称：**南向资金日净买入**

- 接口路径：`/capital_flow/southbound/data`
- 数据来源：东方财富数据中心南向资金接口。
- 数据类型：南向资金累计净买入和日净买入。
- 主要用途：观察南向资金、港股通沪、港股通深的日度流入流出。
- 核心参数：`startdate`、`enddate`、`fulldata`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/capital_flow_data/southbound/`。
- 对应脚本：`southbound_netbuy_fetcher.py`。
- 获取方式：请求东方财富`RPT_SOUTH_ACCUM_NETBUY`，先保存累计值，再计算日净买入。
- 系统角色：资金流模块的一部分，默认只返回周报常用净买入字段，`fulldata=yes`时返回完整字段。

### API名称：**北向资金成交额**

- 接口路径：`/capital_flow/northbound/data`
- 数据来源：东方财富数据中心北向资金成交额接口。
- 数据类型：北向、沪股通、深股通成交额以及相关指数和主导个股字段。
- 主要用途：观察北向资金活跃度和市场联动。
- 核心参数：`startdate`、`enddate`、`fulldata`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/capital_flow_data/northbound/`。
- 对应脚本：`northbound_dealamt_fetcher.py`。
- 获取方式：请求东方财富`RPT_MUTUAL_DEALAMT`。

### API名称：**新发基金数据**

- 接口路径：`/fund_new_issue/data`
- 数据来源：东方财富基金发行页面。
- 数据类型：新发基金快照。
- 主要用途：观察新成立基金、正在发行基金、基金类型和发行状态。
- 核心参数：`startdate`、`enddate`、`include_pending`、`fund_type`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/fund_new_issue_data/`。
- 对应脚本：`fund_new_issue_fetcher.py`。
- 获取方式：请求`fund.eastmoney.com/data/FundNewIssue.aspx`，解析页面中类似数组的数据结构。
- 特殊处理：已成立基金按`established_date`月度切片；尚未成立基金放入`fund_new_issue_pending.parquet`；`unknown_*`字段默认不展示。

![图28：新发基金API返回结果，展示pending待成立基金排在前面](/contents/blogs/blog-folder/api_yes/28.png)


### API名称：**人民币汇率指数**

- 接口路径：`/rmb_fx_index/data`
- 数据来源：中国货币网人民币汇率指数接口。
- 数据类型：CFETS、BIS、SDR三类人民币汇率指数。
- 主要用途：观察人民币一篮子汇率变化。
- 核心参数：`startdate`、`enddate`、`fulldata`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/rmb_fx_index_data/`。
- 对应脚本：`rmb_fx_index_fetcher.py`。
- 特殊处理：按364天窗口分段请求，并处理中国货币网TLS兼容问题。

### API名称：**Shibor利率曲线**

- 接口路径：`/shibor_curve/data`
- 数据来源：中国货币网ShiborChart接口。
- 数据类型：隔夜到一年期Shibor。
- 主要用途：资金利率曲线观察。
- 核心参数：`startdate`、`enddate`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/shibor_curve_data/`。
- 对应脚本：`shibor_curve_fetcher.py`。
- 获取方式：接口返回CSV文本，脚本拆列后标准化成`shibor_on`、`shibor_1w`等字段。

### API名称：**沪深300ERP**

- 接口路径：`/erp_hs300_10y/data`
- 数据来源：沪深300行情和10年期国债收益率。
- 数据类型：派生指标。
- 主要用途：计算沪深300收益率、无风险收益率折算、ERP、200日均线和2σ通道。
- 核心参数：`startdate`、`enddate`、`frequency`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/erp_hs300_10y_data/`。
- 对应脚本：`erp_hs300_10y_fetcher.py`。
- 系统角色：这是从“原始数据API”走向“轻量派生指标API”的第一步。

### API名称：**上交所ETF份额**

- 接口路径：`/sse_etf_shares/data`
- 数据来源：上交所`commonQuery.do`接口。
- 数据类型：ETF日度份额和规模相关字段。
- 主要用途：观察上交所ETF份额变化。
- 核心参数：`startdate`、`enddate`、`sec_codes`、`etf_type`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/sse_etf_shares_data/`。
- 对应脚本：`sse_etf_shares_fetcher.py`。
- 获取方式：请求`http://query.sse.com.cn/commonQuery.do `，使用`sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L`，返回JSONP后解析。

### API名称：**深交所ETF规模**

- 接口路径：`/szse_etf_shares/shares`
- 数据来源：项目中已有`SZSE_ETF_vol.sqlite`。
- 数据类型：深交所ETF规模数据。
- 主要用途：查询深交所ETF规模和相关信息。
- 核心参数：`startdate`、`enddate`、`limit`、`offset`、`format`。
- 返回格式：JSON。
- 存储位置：`api/data/SZSE_ETF_vol.sqlite`。
- 对应模块：`api/retours/szse_etf_shares.py`。
- 说明：这是当前系统里仍保留SQLite读取方式的接口之一。

### API名称：**申万一级行业日度行情**

- 接口路径：`/sw_industry_daily/data`
- 数据来源：申万研究网站接口。
- 数据类型：申万一级行业指数日度行情。
- 主要用途：行业指数行情分析。
- 核心参数：`startdate`、`enddate`、`swindexcodes`、`limit`、`offset`、`format`。
- 返回格式：JSON或CSV。
- 存储位置：`api/data/sw_industry_daily_data/{swindexcode}/`。
- 对应脚本：`sw_industry_daily_fetcher.py`。
- 当前状态：路由和历史数据结构仍保留，但由于该数据源在GitHubActions环境中访问不稳定，批量自动化默认关闭，并且API展示页中暂时隐藏。
- 开关文档：`base_data_renew_folder/automations/API_CATALOG_AND_FETCHER_SWITCHES.md`。

### 其他说明

`api/main.py`里还能看到`index_daily`路由和`foo`测试路由。`index_daily`是A股主要指数日度数据接口，当前数据源仍在调整中；`foo`更像测试接口。它们不作为本文主线展开。

## 14.API是怎么“扒”出来的

这个项目里并不是所有数据源都一样。有些更像公开接口，有些更像从网页Network里反推出来的接口。

大致可以分几类。

### 公开程度比较高的接口

- YahooFinanceChartAPI：全球指数日线，URL结构清晰，参数主要是`period1/period2/interval`。
- 中国货币网部分接口：可以直接请求JSON或CSV，但有TLS兼容问题。
- 上交所`commonQuery.do`：接口公开暴露在网页请求里，但需要特定`sqlId`和Referer。

### 更像从Network里反推的接口

- 东方财富南向资金：`RPT_SOUTH_ACCUM_NETBUY`。
- 东方财富北向资金：`RPT_MUTUAL_DEALAMT`。
- 东方财富新发基金：页面返回类似JS数组，需要自己解析。
- 申万行业指数：接口能在浏览器中访问，但在Actions里出现过非JSON、防护页或超时问题。

通常一个数据源进入系统会经历这个流程：

![图29](/contents/blogs/blog-folder/api_yes/29.png)

代码里可以看到很多这种工程化痕迹：

- `requests.Session()`复用会话。
- `HTTPAdapter`和`Retry`处理临时失败。
- `timeout`防止脚本无限卡住。
- `Referer`、`User-Agent`、`Accept-Language`模拟浏览器请求。
- 中国货币网接口需要`LegacyTLSAdapter`。
- 上交所ETF接口返回JSONP，需要先去掉`cb(...)`。
- 新发基金接口不是标准JSON，要处理缺失逗号和字段错位。

这部分不需要包装得很复杂，本质就是把原来PowerQuery里零散的请求，挪到Python里做成可维护、可记录、可自动化的采集层。

## 15.自动化更新流程

自动化更新的入口是：

```text
base_data_renew_folder/automations/run_all_fetchers.py
```

它按顺序运行当前活跃fetcher，实时输出日志，并把每个fetcher的结果追加到CSV：

```text
base_data_renew_folder/automations/logs/fetcher_daily_runs.csv
```

CSV字段包括：

```text
run_id,run_date,fetcher,script,status,updated_rows,exit_code,started_at,ended_at,duration_seconds,command,error_excerpt,stdout,stderr
```

这意味着每次自动更新后，我不仅知道成功还是失败，还能知道哪个脚本写入了多少行、耗时多久、失败时stderr是什么。

![图30](/contents/blogs/blog-folder/api_yes/30.png)

飞书通知使用webhook，不会把真实地址写进仓库~ 配置方式是：

```text
FEISHU_WEBHOOK_URL=***
```

![图31：fetcher_daily_runs.csv截图，展示单次运行中每个脚本的状态Status与耗时Duration_seconds](/contents/blogs/blog-folder/api_yes/31.png)

## 16.API页面：让数据真正可见

如果只有API地址，对很多读者来说仍然不够直观。

比如你看到：

```text
https://api.huangdapao.com/global_market_daily/data?startdate=2025-01-01&enddate=2025-12-31&symbols=SPX&format=json
```

这条链接背后是在查询标普500的日度行情，但如果没有上下文，参数含义、返回字段、下载方式都不容易马上看出来。API页面就是为了解决这个问题：它把后端接口变成一个可浏览、可点选、可复制、可预览的数据目录。

页面在：

```text
api_showcase/index.html
```

它不是手写死每个接口，而是读取：

```text
api_showcase/metadata/catalog.json
api_showcase/metadata/apis/*.json
```

这些metadata由：

```text
base_data_renew_folder/automations/update_api_showcase_metadata.py
```

生成。它会扫描每个API的数据源，写入接口路径、参数、字段中文名、数据行数、日期范围、所属分类等信息。

页面本身不直接存业务数据。它更像一个可视化入口：左边是分类目录，中间是接口卡片，卡片展开后可以看到参数、字段说明、示例链接和操作按钮。

页面能力包括：

- 两级目录浏览，例如金融数据/资金流、金融数据/利率曲线。
- 搜索接口。
- 展开单个API卡片。
- 输入参数并自动生成请求链接。
- 复制链接。
- 打开JSON。
- 下载CSV。
- 请求并预览前五条数据。
- 展示`columns_zh`中文字段解释。
- 请求`/health`判断RenderAPI是否已经唤醒。

它的工作流大概是这样：

![](/contents/blogs/blog-folder/api_yes/32.png)

这个页面还有一个很实用的小功能：状态悬浮栏。

因为Render免费版会休眠，第一次打开API时服务可能还在冷启动。页面会先请求`/health`，如果返回`{"status":"ok"}`，就显示服务正常；如果暂时没有返回，就提醒用户API服务可能正在启动。这个设计很小，但对免费部署很重要，因为它把Render冷启动这件事显性化了。

从体验上看，API页面完成了三件事：

- 给人看：有哪些数据、属于什么分类、字段是什么意思。
- 给程序用：生成可以直接复制到Excel或脚本里的URL。
- 给调试用：直接预览前五条数据，快速判断接口是否正常。

API页面本质上是把“给程序看的接口”变成“人也能看懂的接口目录”。它不是后端，但它让这套数据中心从“能访问”变成了“能理解”。

![图33：API首页截图，展示两级目录、API域名、可用接口与搜索框](/contents/blogs/blog-folder/api_yes/33.png)

![图34：单个API详情截图，展示参数输入、字段说明、JSON链接与CSV下载按钮](/contents/blogs/blog-folder/api_yes/34.png)

![图35：CSV下载功能展示](/contents/blogs/blog-folder/api_yes/35.png)

![图36：点击请求并预览后，表格展示前五条数据的截图](/contents/blogs/blog-folder/api_yes/36.png)


## 17.为什么这套系统能继续扩展

这个系统现在已经形成了几层：

![图37](/contents/blogs/blog-folder/api_yes/37.png)

新增一个数据接口时，流程已经比较固定：

1. 在`base_data_renew_folder/automations/`写fetcher。
2. 把数据写入`api/data/某个目录/`。
3. 在`api/retours/`写查询模块。
4. 在`api/main.py`注册路由。
5. 在`update_api_showcase_metadata.py`增加metadata。
6. 本地跑一次生成数据。
7. 推送后让Actions接管后续更新。

这就是项目从“脚本集合”变成“数据系统”的关键。每个新数据源进入系统后，不只是多了一个文件，而是同时进入**采集、存储、查询、展示、自动化和通知这条链路**。

## 18.接下来可能会往哪走

目前这套系统还不是完整的自动周报工厂，但方向已经很清楚。

下一阶段我更想做的是把Excel里越来越多的分析逻辑逐步迁到Python自动化脚本里。

可能会包括：

- 用Python自动生成市场图表。
- 把API数据直接变成图表图片。
- 用Markdown生成周报正文草稿。
- 通过LMStudio接入本地LLM。
- 用LLM生成市场评论。
- 把数据、图表、AI评论合成完整周报。
- 把Markdown转换成微信公众号可以直接复制的格式。
- 最后做到点击几个按钮生成一篇市场周报。

这里我不会说它已经完成了。现在更准确的状态是：**数据层、API层、自动化层已经搭起来了**，周报生成层还在继续拆解和迁移。

但这件事已经从“每周手动刷新Excel”变成了一个更长期的工程方向。

## 19.最后再回到一开始的问题

这个项目表面上看是在写API。

但真正让我觉得它值得继续做的地方，是它把周报工作流里最脆弱、最重复、最容易坏的那一层抽了出来。

以前是：

```text
Excel直接连所有外部数据源
```

现在是：

```text
外部数据源-->自动化采集-->Parquet数据中心-->API-->Excel/页面/周报/分析系统
```

这套架构不豪华，也不是企业级数据平台。

但它有几个很实际的优点：

- 成本接近0。
- 数据可版本管理。
- 更新过程可追踪。
- 接口能被Excel和浏览器直接调用。
- API页面能让人快速理解字段和参数。
- 出问题时有日志和飞书通知。
- 后续可以继续接自动图表和AI评论。

所以它不是一个“为了写API而写API”的项目。

它更像是一个**被周报需求逼出来的个人金融数据基础设施**。

而这恰好是我最想要的东西。
