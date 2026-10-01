# Seen Archive

`seen/` 是正式文件归档：

- `records/<id>/record.json`：记录元数据。
- `records/<id>/content.md`：正文。
- `records/<id>/assets/`、`thumbs/`：原图与缩略图。
- `routes/<id>.json`：有序记录列表。
- `indexes/*.json`：供公开页面读取的生成索引。

公开页面读取已发布文件。管理器通过 File System Access API 写入用户选定的本地目录（需要支持该 API 的桌面浏览器）；写入本地目录不会自动部署到 GitHub Pages。

## 草稿与备份

草稿保存在 `seen-drafts` IndexedDB 中，图片以 Blob 保存，整个快照在一个事务中更新。旧 localStorage 草稿首次成功启动时迁移；仅在事务成功后清理旧键。失败时保留旧数据并显示错误。

浏览器储存不等于备份。清理网站数据前，使用草稿箱里的“导出草稿备份”。“恢复草稿备份”按 ID 合并，相同 ID 保留当前页面版本；备份包含照片，请妥善保管。不同域名及 localhost 的草稿互不共享。

“提交全部”保留失败项以便重试，保存失败不会显示成功。目录授权在刷新后需重新连接。

## 索引和缓存

手动改动归档文件后运行：

```powershell
node scripts/seen-archive.mjs rebuild-indexes
```

索引内嵌记录和路线元数据，当前归档首屏只需两次数据请求；旧索引仍兼容逐条读取。浏览器管理器只将已实际写入目录的文件加入内嵌索引。

图片 URL 使用记录更新时间作为版本。Service Worker 只处理同源归档图片，以网络及 HTTP 缓存更新，离线时回退到图片缓存；最多保留 120 张、每张不超过 2 MiB。不会缓存记录 JSON、正文或地图瓦片，不会清理其他功能的缓存。图片解码引用最多保留 16 项。

## 地图服务

`map-config.js` 配置 OSM 标准底图和现有地理查询服务。地图使用浏览器正常 HTTP 缓存，保留署名，不提供批量预取或离线下载。OSM 是尽力提供的公共服务，请遵守 [瓦片使用政策](https://operations.osmfoundation.org/policies/tiles/)。

现有 Nominatim 边界查询只在管理器打开后启用；请求串行、间隔至少 1.1 秒，缓存结果，在 429/503 后退避。支持 Web Locks 的浏览器会协调多个标签页。[Nominatim 政策](https://operations.osmfoundation.org/policies/nominatim/)要求整个应用合计每秒最多 1 次请求，浏览器端无法协调不同设备。当前适合单作者使用；多人同时管理时，应更换服务或使用服务器统一限流。未新增远程自动补全。

## 动画与布局

粒子动画在离屏、后台时暂停，速度适配不同刷新率；尊重系统“减少动态效果”。平板使用两列卡片，长文本换行，保留键盘焦点，手机允许页面缩放。

`seen-responsive.css` 管理详情与搜索的自适应布局。详情卡片与路线面板放在同一 Grid 中，宽屏并排、窄屏上下排列，横屏低高度另行适配；长正文与路线面板分别滚动。照片控件采用 Grid 叠层，浮动工具统一以 Flex 排列，避免各自硬编码底部偏移。地图坐标、全屏遮罩及锚定弹出层保留必要定位。

搜索使用全视口背景虚化，包含 footer；背景在搜索期间不可交互，关闭后恢复原状态。支持 Escape、键盘焦点约束及动态视口高度。

路线仅在选中时显示方向流动，起终点有独立样式；记录坐标点使用与详情小地图当前点一致的绿色亮点及扩散光环，低缩放时按屏幕网格合并显示数量。路线站点恢复小圆点、点状连接线与当前站绿色光环，保留无障碍站点编号和原生横向滚动，切换时平滑定位；减少动态效果时关闭动画。详情小地图在尺寸变化时复用实例。

响应式验证使用 Edge 模拟 1440×1000、1024×768、768×1024、390×844、320×568、844×390、640×360、320×360，包含长正文、路线名称、搜索底部遮罩及减少动态效果检查；未替代真实 iOS/Safari 设备测试。

## 命令行管理

```powershell
node scripts/seen-archive.mjs init
node scripts/seen-archive.mjs create-route --name "东京夜行" --id route_tokyo_night_test
node scripts/seen-archive.mjs create-record --title "东京塔的雨夜" --id record_tokyo_tower_rain_test --route route_tokyo_night_test --position end
node scripts/seen-archive.mjs update-route --route route_tokyo_night_test --records record_a_test,record_b_test
node scripts/seen-archive.mjs delete-route --route route_tokyo_night_test
node scripts/seen-archive.mjs rebuild-indexes
```

夜间主题仅对 OSM 底图图层应用颜色滤镜，不影响路线、标记、照片或署名。它不是 OSM 原生深色样式，地图类别颜色会变化；如需原色，将 `map-config.js` 的 `nightFilter` 设为 `false`。切换主题不重新加载瓦片。

回归检查：`node --test scripts/seen-regression.test.mjs`。

详情面板浏览器检查：`node scripts/seen-detail-browser-check.mjs`（Node 22+，默认 Windows Edge，可通过 `BROWSER_PATH` 指定 Chromium）。检查深浅主题 × 4 种尺寸 × 路线/坐标/全线，共 24 种组合：背景计算值、透明子层、上下边缘、继承的 aside 留白、面板重叠及地图实际显示。截图保存在系统临时目录；测试屏蔽 OSM 瓦片请求，不验证在线底图加载。使用本机 8770/9230 端口。

详情磨砂由弹窗 backdrop 统一模糊，卡片与 RouteDock 使用相同的 90% 不透明底色；避免独立背景采样产生明显色差。RouteDock 显式清除全站 aside 的上下留白，面板自身裁切内容，透明弹窗外壳允许阴影完整显示。
