# 流变工坊 · 实时CDC与流式数据同步平台

面向数据库CDC事件捕获、变更日志解析、流式数据转换、维表关联补全、水位线管理、乱序容忍、结果落湖归档与端到端延监控的实时CDC与流式同步平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| CDC数据源 | `cdc_source` | 链路源端 | 源端编号、数据库类型、连接地址 |
| 日志解析 | `binlog_parse` | 解析规则 | 规则编号、源端、日志格式 |
| 事件转换 | `event_trans` | 转换管道 | 管道编号、源端、事件类型 |
| 维表关联 | `dim_join` | 关联配置 | 配置编号、事实表、维度表 |
| 水位管理 | `watermark_mgr` | 水位线 | 水位编号、数据流、当前位点 |
| 乱序处理 | `out_of_order` | 乱序策略 | 策略编号、数据流、时间字段 |
| 结果落盘 | `sink_writer` | 落盘管道 | 管道编号、数据流、目标载体 |
| 延迟监控 | `lag_monitor` | 延迟指标 | 指标编号、数据流、源端位点 |
| DDL处理 | `ddl_handle` | DDL适配 | 适配编号、源端、DDL类型 |
| 断点恢复 | `checkpoint` | 检查点 | 检查点编号、数据流、保存位点 |
| Schema演进 | `schema_evo` | 演进记录 | 记录编号、数据流、变更列 |
| 流去重 | `dedup_stream` | 去重窗口 | 窗口编号、数据流、去重键 |
| 湖仓提交 | `iceberg_commit` | 提交快照 | 快照编号、目标表、数据文件 |
| 小文件合并 | `compaction` | 合并任务 | 任务编号、目标表、文件数量 |
| 快照过期 | `expire_snap` | 过期策略 | 策略编号、目标表、保留数量 |
| 流聚合 | `metric_aggr` | 聚合窗口 | 窗口编号、数据流、聚合键 |
| 增量回填 | `backfill_run` | 回填任务 | 任务编号、目标表、起始位点 |
| 告警规则 | `alert_rule` | 告警策略 | 策略编号、监控指标、告警条件 |
| 数据比对 | `data_diff` | 比对任务 | 任务编号、源端、目标端 |
| 编排调度 | `task_schedule` | 调度计划 | 计划编号、关联链路、Cron表达式 |
| 流审计 | `stream_audit` | 审计记录 | 记录编号、数据流、操作类型 |
| 健康报告 | `stream_health` | 健康评估 | 评估编号、数据流、健康分数 |

## 快照维护数据视图

湖仓提交只记录"提交了哪些文件"，不记录"哪些文件现在还能被合并维护"。  
`backend/app/services/snapshot_maintenance.py` 在已提交快照之上提供一层维护视图：

- 打开视图时冻结当时可见的已提交快照，每个数据文件只列一次，并带上
  **基准快照**（文件第一次出现的快照）和来源快照；待处理文件一定能追溯回
  当时存在的快照。
- 同一张表的后续快照可以共用旧文件；视图打开后再提交的同表快照只会进入
  新视图，并以 `newSnapshotsSinceOpen` / `newFilesSinceOpen` 单独标注，
  不会混进旧视图或旧计划的已确认范围。
- 确认维护范围后范围冻结：即使数据文件条件变化，`GET /plans/{id}` 仍能查到
  原确认范围和已替换范围；续做只消费确认范围，后来追加的文件不会被算作
  "已替换"。
- 数据文件的写入占用全局唯一：两个合并作业同时提交时只有一方拿到有效占用，
  另一方拿到 `conflicts`。写入占用和替换记录都在
  `GET /views/{id}?candidate_only=true` 的候选里逐条体现。
- 提交维护时用视图里的已确认对象建立合并任务（`compaction` 模块），任务同时
  记录「试图替换路径」「真实生效路径」「未生效路径」，可直接核对哪些路径没生效。
- `GET /plans/compare?left=&right=` 对照两份计划：共有/各自独有的确认范围、
  已替换范围、仍可治理范围，以及各自视角下后来追加的文件。

接口（同时支持 `/api/snapshot-maintenance` 与 `/api/snapshot_maintenance` 前缀）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/tables` | 可维护的目标表、快照与候选数量 |
| POST | `/views` | 打开视图，`{targetTable, snapshotId?}` |
| GET | `/views/{id}` | 读取视图，`?candidate_only=true` 只返回候选 |
| GET | `/views/{id}/candidates` | 可交给合并的候选，逐条带基准快照 |
| POST | `/views/{id}/plans` | 确认维护范围，`{objectIds?, batchSize?}` |
| POST | `/views/{id}/submit` | 提交一批，`{planId?, jobId?, batchSize?}` |
| POST | `/snapshots` | 追加一份同表已提交快照，复现实时写入 |
| GET | `/plans?table=` | 计划列表 |
| GET | `/plans/{id}` | 计划详情：已确认/已替换/剩余/仍可治理/后来追加 |
| GET | `/plans/compare?left=&right=` | 两份计划的范围对照 |

前端工作台在「快照维护视图」页（`/snapshot-maintenance`）操作同一套接口。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数仓与数据湖
