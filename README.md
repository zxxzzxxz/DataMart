# DataMart 数据集市调度平台

基于 Apache Airflow 3 的数据集市 ETL 调度与监控环境，包含真实数据仓库（ods → dwh → dm 三层）与完整的生产级功能演示。

## 架构

```
┌─────────────────────────── Airflow 3 (CeleryExecutor) ───────────────────────────┐
│  airflow-apiserver (UI :8080)   airflow-scheduler   airflow-dag-processor        │
│  airflow-triggerer              airflow-worker ×2   flower (Worker 监控 :5555)   │
│  postgres (元数据库)            redis (消息队列)                                  │
└──────────────┬───────────────────────────────────────────────────────────────────┘
               │ statsd 指标                          │ conn_id = warehouse
┌──────────────▼────────────── 可观测性栈 ────────────▼────────────────────────────┐
│  statsd-exporter → prometheus (:9090) → grafana (:3000 看板 + 告警规则)          │
│  alert-webhook (:9099 告警中心)  ← Airflow 失败/Deadline 回调 + Grafana 告警     │
├──────────────────────────── warehouse (PostgreSQL, :15432) ──────────────────────┤
│  ods 源层（模拟业务库） → dwh.fact_orders 明细层 → dm.daily_city_sales 集市层     │
│                                                → dm.daily_summary_report 日报    │
└──────────────────────────────────────────────────────────────────────────────────┘
```

## DAG 一览

| DAG | 调度 | 说明 |
|---|---|---|
| `datamart_daily_etl` | 每天（@daily） | ods→dwh→dm 全链路：并行抽取 → 打宽 → 汇总 → 对账质检 → 发布数据资产 |
| `datamart_incremental_etl` | 每 15 分钟 | 水位线增量：ods→staging→dwh upsert→dm 按受影响日期增量刷新 |
| `datamart_backfill` | 仅手动（带 conf） | 区间补跑：`{"start_date","end_date"}` 逐日重建集市层 + 刷新日报 |
| `datamart_daily_report` | Dataset 驱动 | 上游 ETL 发布数据资产后自动触发，生成全局日报（UI Datasets 页可见血缘图） |
| `datamart_failure_demo` | 仅手动 | 失败路径演示：重试耗尽 → 告警回调 → all_done 通知 |
| `datamart_deadline_demo` | 仅手动 | Deadline 演示：长任务超时未完成触发告警 |

## 功能清单

- **Web UI / 监控**：DAG Grid/Graph 视图、任务日志、Dataset 血缘图、Admin → Config 配置页
- **用户权限**：FAB 认证管理器，Admin → Security 管理用户/角色
- **调度**：cron/间隔定时 + Dataset 数据血缘触发 + 手动触发（支持传参 conf）
- **依赖编排**：串行、并行分支（TaskGroup）、汇聚、trigger_rule 条件触发
- **重试**：统一 default_args，指数退避；失败告警回调（plugins/alerting.py，支持 webhook）
- **可观测性**：Airflow → StatsD → Prometheus 指标链 + Grafana 看板（心跳/失败数/运行时长/执行器负载）
- **告警闭环**：内置告警中心（:9099）接收三类告警 —— 任务失败回调、Deadline 超时/漏跑、Grafana 规则告警（Scheduler 心跳丢失、任务失败率）
- **Deadline 漏跑告警**：ETL Run 入队 1 小时未完成即告警（Airflow 3 DeadlineAlert）
- **日志**：按 任务/运行/尝试次数 持久化在 `./logs`，UI 可直接查看
- **连接与变量管理**：Admin → Connections / Variables（已预置 warehouse 连接，Fernet 加密存储）
- **幂等设计**：`ON CONFLICT DO NOTHING/DO UPDATE` + 先删后插，重复执行/补跑不产生重复数据
- **增量框架**：plugins/incremental.py 水位线模板（staging 落地 → merge upsert → 最后才推进水位线，失败自动重抽不丢数）
- **区间补跑**：conf 传参 `{"start_date","end_date"}` 逐日重建，带格式/顺序/31 天上限校验
- **水平扩展**：Worker 默认 2 副本，`--scale airflow-worker=N` 继续扩

## 快速开始

```bash
# 首次启动：初始化数据库并创建管理员账号
docker compose up airflow-init

# 启动全部服务
docker compose up -d

# 停止
docker compose down
```

## 访问入口

| 入口 | 地址 | 说明 |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin123（DAG 监控、日志、重跑、配置查看、用户管理） |
| Grafana | http://localhost:3000 | admin / admin123（Airflow 指标看板、告警规则） |
| 告警中心 | http://localhost:9099 | 接收所有告警（任务失败 / Deadline / Grafana 规则） |
| Prometheus | http://localhost:9090 | 指标查询 |
| Flower | http://localhost:5555 | Celery Worker 监控 |
| 数据仓库 | localhost:15432 | warehouse / warehouse（可用任意 PG 客户端直连查看分层数据） |

## 目录结构

```
dags/                  DAG 定义（调度任务编排）
plugins/               通用插件（alerting.py 告警回调）
config/                airflow.cfg 覆盖配置（可选）
logs/                  任务运行日志（自动生成，勿提交）
tests/                 DAG 完整性测试
docker/warehouse-init/ 数据仓库初始化 SQL（分层 schema + 示例源数据）
docker-compose.yaml
```

## 常用操作

```bash
# 手动触发 / 查看运行
docker compose exec airflow-scheduler airflow dags trigger datamart_daily_etl
docker compose exec airflow-scheduler airflow dags list-runs datamart_daily_etl

# 查看某个运行内各任务状态
docker compose exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>

# 横向扩展 Worker
docker compose up -d --scale airflow-worker=3
```

## 运行测试

scheduler 容器启动时会自动安装 pytest（compose 中 `_PIP_ADDITIONAL_REQUIREMENTS`），直接运行：

```bash
docker compose exec airflow-scheduler bash -c \
  "python -m pytest /opt/airflow/tests -q"
```

## 开发工作流

```bash
# 代码检查与格式化（本地 venv 已含 ruff，或 pip install ruff）
.venv\Scripts\ruff check dags plugins tests        # lint
.venv\Scripts\ruff format dags plugins tests       # 格式化

# 提交代码即触发 CI（.github/workflows/ci.yml）：ruff 检查 + DAG 完整性测试
# DAG 测试在 CI 中用 sqlite + 官方 constraints 运行，无需启动 Docker
```

## 环境分离

```bash
# 开发（默认）：所有端口暴露、内置开发密钥
docker compose up -d

# 生产：密钥强制从环境变量传入（缺失即拒绝启动）、数据仓库不暴露端口、Worker 4 副本
export AIRFLOW__CORE__FERNET_KEY=... AIRFLOW__API_AUTH__JWT_SECRET=... _AIRFLOW_WWW_USER_PASSWORD=...
docker compose -f docker-compose.yaml -f docker-compose.prod.yml up -d
```

## 验收基线（本环境已实测通过）

- ETL 全链路落库：dm.daily_city_sales 按 业务日期×城市 聚合
- 增量抽取：新插/更新 2 行被精确拾取（105 cancel→paid upsert 生效），水位线按微秒精度推进
- 空增量边界：0 行抽取、跳过刷新、水位线不变、运行成功
- 区间补跑：删除 09-06 数据后经 conf 区间重建恢复，日报同步刷新；缺 conf 明确报错
- Dataset 血缘：ETL 成功后报表 DAG 以 `asset_triggered__` 前缀自动运行
- 失败路径：重试耗尽 → failed → 告警回调送达告警中心 → all_done 通知执行
- Deadline 漏跑告警：长任务运行超过截止时间 → 告警中心收到含 DAG/Run/截止时间的告警
- Grafana 告警规则：Scheduler 心跳丢失、任务失败率两条规则 firing 后送达告警中心
- 指标链：Prometheus 采集到调度器心跳、任务完成计数（按状态）、执行器槽位、DAG 运行时长
- 单任务重跑：UI/API Clear + run 重新排队 → 任务按新尝试次数重跑
- 定时调度：@daily 调度运行自动产生（scheduled__ 前缀）
- 多 Worker：2 副本均注册并处理任务
- DAG 完整性测试：9 项全部通过

## 已知设计说明

- Airflow 3 手动触发的运行 `logical_date` 为空，DAG 内用 `_stat_date()` 兜底取当前业务日期
- DAG Run 状态由叶子任务决定：失败任务若被 all_done 通知节点吸收，Run 整体仍显示 success
- SLA 已在 Airflow 3.0 移除，超时告警请使用 Deadline Alerts

## 生产部署注意

- 更换 `docker-compose.yaml` 中的 `AIRFLOW__CORE__FERNET_KEY`、`AIRFLOW__API_AUTH__JWT_SECRET`、管理员密码及各数据库密码
- 当前使用 FAB 认证管理器；如需 SSO 可切换 Keycloak 等认证方式
- 告警 webhook 在 Admin → Variables 配置 `datamart_alert_webhook` 即生效
- 邮件告警需配置 SMTP 后打开 DAG 中的 `email_on_failure`
