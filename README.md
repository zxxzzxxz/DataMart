# DataMart 数据集市调度平台

基于 Apache Airflow 3 的数据集市 ETL 调度与监控环境，包含真实数据仓库（ods → dwh → dm 三层）与完整的生产级功能演示。

## 架构

```
                ┌──────── nginx（TLS 终结，:443 / :8443，HTTP 自动跳 HTTPS）────────┐
                │  / → Airflow UI(×2)   /grafana/  /prometheus/  /alertmanager/   │
                │  /flower/   /alerts/(告警中心)    :8443 → 可视化设计器            │
                └──────────────┬──────────────────────────┬───────────────────────┘
┌─────────────────────────── Airflow 3 (CeleryExecutor) ──┼──────────────────────┐
│  airflow-apiserver ×2     airflow-scheduler ×2   airflow-dag-processor ×2      │
│  airflow-triggerer ×2     airflow-worker ×2(N)   flower (Worker 监控)          │
│  postgres(主) ⇄ postgres-replica(热备·流复制)    redis (消息队列, AOF 持久化)   │
└──────────────┬──────────────────────────────────────────┬──────────────────────┘
               │ statsd 指标                              │ conn_id = warehouse
┌──────────────▼────────────── 可观测性/告警 ─────────────▼─────────────────────┐
│  statsd-exporter → prometheus → alertmanager(分组/静默/抑制/Watchdog)          │
│  grafana(看板+规则告警)  alert-webhook(告警中心)  alloy → loki(集中日志14d)    │
├────────────── warehouse (PostgreSQL 主备, :15432) ────────────────────────────┤
│  ods 源层（模拟业务库） → dwh.fact_orders 明细层 → dm.daily_city_sales 集市层  │
│                                                → dm.daily_summary_report 日报  │
│  flyway 结构迁移版本管理（docker/warehouse-migrations，启动即 migrate）        │
├────────────── 数据质量 ───────────────────────────────────────────────────────┤
│  datamart_data_quality DAG（Dataset 驱动）→ 质检得分/失败数/漂移 → Pushgateway │
│  error 级失败写入 dwh_quarantine 隔离区；DataQualityScoreLow 告警规则          │
├────────────── 安全体系 ───────────────────────────────────────────────────────┤
│  vault（密钥管理，Airflow secrets backend 可选启用）                           │
│  keycloak（SSO，Airflow FAB OAuth 可选启用，/keycloak/ 经 nginx 透传）         │
│  设计器多用户 RBAC（viewer/editor/admin，bcrypt + 审计日志）                   │
├────────────── 灾备 ───────────────────────────────────────────────────────────┤
│  每日 pg_dump + pg_basebackup → ./backups ──(60s 镜像)──► minio(异地副本)      │
│  WAL 持续归档 → pg-wal-archive 卷 + minio（PITR 任意时间点恢复）              │
│  restore-drill 一键恢复演练（备份可用性自动验证）                             │
└──────────────────────────────────────────────────────────────────────────────┘
```

## DAG 一览

| DAG | 调度 | 说明 |
|---|---|---|
| `datamart_daily_etl` | 每天（@daily） | 同一快照全量校准：明细、集市、日报、质检与批次记录原子提交，再发布资产 |
| `datamart_incremental_etl` | 每 15 分钟 | 消费事务变更日志，处理更新/删除/迟到提交/用户维度变更，刷新旧日期与新日期 |
| `datamart_backfill` | 仅手动（带 conf） | 从已提交明细按 `{"start_date","end_date"}` 原子重建集市与日报，最多 31 天 |
| `datamart_daily_report` | Asset 驱动 | 只读核验和展示已随 ETL 原子提交的日报 |
| `datamart_data_quality` | Asset 驱动 | 已发布数据的只读审计；完整质量门禁已前移到 ETL 提交事务内 |
| `datamart_publish_outbox` | 每分钟 | 自动补发数据库提交后尚未确认的资产事件，发布成功后才确认，支持进程中断恢复 |
| `datamart_failure_demo` | 仅手动 | 失败路径演示：重试耗尽 → 告警回调 → all_done 通知 |
| `datamart_deadline_demo` | 仅手动 | Deadline 演示：长任务超时未完成触发告警 |

## 功能清单

- **Web UI / 监控**：DAG Grid/Graph 视图、任务日志、Dataset 血缘图、Admin → Config 配置页
- **用户权限**：FAB 认证管理器，Admin → Security 管理用户/角色
- **调度**：cron/间隔定时 + Dataset 数据血缘触发 + 手动触发（支持传参 conf）
- **依赖编排**：串行、并行分支（TaskGroup）、汇聚、trigger_rule 条件触发
- **重试**：统一 default_args，指数退避；失败告警回调（plugins/alerting.py，支持 webhook）
- **可观测性**：Airflow → StatsD → Prometheus 指标链 + Grafana 看板（心跳/失败数/运行时长/执行器负载）+ Loki 集中日志（Grafana Explore 检索，带 dag_id/task_id 标签）
- **告警闭环**：Alertmanager 告警中枢（分组/静默/抑制）统一分发到告警中心（/alerts/）——任务失败回调、Deadline 超时/漏跑、Prometheus 规则告警（Scheduler 心跳、失败率、组件掉线、端点探测、复制中断、备份异常）
- **Deadline 漏跑告警**：ETL Run 入队 1 小时未完成即告警（Airflow 3 DeadlineAlert）
- **日志**：按 任务/运行/尝试次数 持久化在 `./logs`，UI 可直接查看
- **连接与变量管理**：Admin → Connections / Variables（已预置 warehouse 连接，Fernet 加密存储）
- **数据正确性**：事务变更捕获 + REPEATABLE READ 快照 + 跨 DAG 数据库锁；更新、删除和迟到提交可恢复消费，日期修正同时重建旧/新分区
- **原子发布与重试**：明细/集市/日报/质检/变更确认/批次记录同事务提交；批次 ID 幂等重放，outbox 补发资产事件（至少一次）
- **区间补跑**：conf 传参 `{"start_date","end_date"}` 原子重建已提交明细对应的集市与日报，带格式/顺序/31 天上限校验
- **水平扩展**：Worker 默认 2 副本，`--scale airflow-worker=N` 继续扩
- **可视化 DAG 设计器**：拖拽连线配置 Job 依赖（:8443），7 种节点类型（SQL/校验/通知/Bash/HTTP/SQL 传感器/占位）+ 任务级 trigger_rule/retries；画布内一键触发运行、运行状态实时着色回显、版本历史（快照/预览/回滚）、SQL 预检（EXPLAIN）、运行中删除保护；YAML 为唯一事实源进 git，dag-factory 渲染成真实 DAG——设计器下线不影响已生成 DAG 的运行（设计见 docs/visual-dag-designer.md）
- **TLS 统一入口**：nginx 终结 TLS，所有 Web UI 收拢到 `https://localhost/`（HTTP 自动跳转），开发证书一键生成（`scripts/gen-certs.sh`），生产替换真实证书即可
- **数据库主备**：airflow 元库与 warehouse 均为主库 + 热备（异步流复制，RPO 取决于复制延迟）；主库宕机由 `scripts/pg-failover.sh` 手动接管，实际 RTO 需演练；WAL 持续归档支持 PITR（`scripts/restore-pitr.sh`）
- **Broker 持久化**：Redis AOF everysec，容器/宿主机重启后队列任务不丢失
- **全组件多副本**：apiserver / scheduler / dag-processor / triggerer / worker 全部 ≥2 副本，任一容器宕机服务不中断
- **异地备份副本**：`backup-shipper` 每 60 秒把备份与 WAL 归档镜像到 MinIO 对象存储，宿主机磁盘损坏仍可恢复
- **恢复演练**：`scripts/restore-drill.sh` 一键起临时实例恢复最新备份并校验关键表，失败触发 critical 告警——备份可用性常态化验证
- **告警中枢**：Alertmanager 统一分组/去重/静默/抑制/路由；Watchdog 常鸣告警做告警链路死信开关；复制中断、复制延迟、备份中断、演练失败均有自动告警规则
- **集中日志**：Alloy 采集全部容器日志 + Airflow 任务日志（带 dag_id/task_id 标签）入 Loki（保留 14 天），Grafana Explore 检索
- **数据质量框架**：YAML 质检套件在 ETL 事务内作为提交门禁；失败整批回滚并保留源变更，发布后审计只读。得分/失败数在事务结束后推送 Pushgateway；隔离功能仅用于显式离线处理（plugins/data_quality.py + config/data_quality/）
- **密钥管理（可选启用）**：Vault 作为 Airflow secrets backend，`scripts/vault-init.sh` 一键初始化/解封/写入种子密钥，连接与变量可从 Vault 读取（.env 设 `AIRFLOW_SECRETS_BACKEND` + `VAULT_TOKEN` 启用；未启用时走环境变量/metastore，零侵入）
- **SSO 单点登录（可选启用）**：Keycloak 26 + Airflow FAB OAuth（OIDC），realm 角色自动映射 FAB 角色（airflow-admin→Admin、airflow-viewer→Viewer），默认仍走本地账号互不影响；`.env` 设 `AIRFLOW_OAUTH_ENABLED=true` 启用（详见 docs/sso.md）
- **设计器多用户 RBAC**：viewer/editor/admin 三角色（bcrypt 密码哈希、签名会话 token、TTL 过期），`scripts/designer-add-user.sh` 管理账号；全部操作落审计 JSONL（logs/designer-audit/）；无 users.json 时自动回退单 token 模式
- **DB 迁移版本管理**：Flyway 管理 warehouse 结构变更（docker/warehouse-migrations/V*.sql），启动即 migrate（幂等、checksum 校验、baseline 兼容存量库）；结构回滚走前滚迁移而非改回旧结构
- **资源治理**：全部 30+ 服务声明 CPU/内存硬上限（xs/s/m/l/xl 五档锚点），Worker 按队列负载 `--autoscale` 弹性伸缩进程数（默认 2~8）
- **安全扫描 + CD 流水线**：security.yml（trivy fs/镜像扫描 + pip-audit + bandit，CRITICAL 门禁）+ cd.yml（tag 推送 → 6 镜像构建推 ghcr → Release → 自动部署 staging → 审批后部署生产），`scripts/release.sh`/`rollback.sh`/`deploy.sh` 配套（见 docs/deployment.md）

## 快速开始

```bash
# 0. 生成开发用 TLS 证书（自签名，仅需一次；生产用真实证书替换 docker/nginx/certs/）
scripts/gen-certs.sh

# 0.1 创建设计器登录账号（多用户 RBAC；跳过则设计器回退单 token 模式）
scripts/designer-add-user.sh admin admin <你的密码>

# 首次启动：初始化数据库并创建管理员账号
docker compose up airflow-init

# 启动全部服务
docker compose up -d

# 可选：启用 Vault 密钥后端（初始化并写入种子密钥后按提示改 .env）
scripts/vault-init.sh

# 停止
docker compose down
```

> **老环境升级注意**（本次改造前已有数据卷）：`docker compose up -d` 重建 postgres/warehouse 后，
> 执行一次 `scripts/pg-enable-replication.sh` 补齐复制用户与 pg_hba 规则，再
> `docker compose up -d postgres-replica warehouse-replica` 拉起热备。全新部署无需此步。

## 访问入口

所有 Web 界面统一经 nginx TLS 入口（自签名证书首次访问需在浏览器确认一次）：

| 入口 | 地址 | 说明 |
|---|---|---|
| Airflow UI | https://localhost/ | admin / admin123（DAG 监控、日志、重跑、配置查看、用户管理） |
| 可视化设计器 | https://localhost:8443 | 拖拽连线建 DAG（多用户登录，账号用 scripts/designer-add-user.sh 创建；无 users.json 时回退 DESIGNER_TOKEN） |
| Grafana | https://localhost/grafana/ | admin / admin123（Airflow 看板、Loki 日志检索、告警规则） |
| Alertmanager | https://localhost/alertmanager/ | 告警分组/静默/抑制管理 |
| 告警中心 | https://localhost/alerts/ | 接收所有告警（任务失败 / Deadline / Alertmanager 路由） |
| Keycloak | https://localhost/keycloak/ | admin / admin（SSO 控制台；启用 OAuth 后 Airflow 登录走这里） |
| Prometheus | https://localhost/prometheus/ | 指标查询 |
| Flower | https://localhost/flower/ | Celery Worker 监控 |
| MinIO Console | http://127.0.0.1:9001 | minioadmin / minioadmin123（备份/WAL 对象存储，仅回环地址，生产不暴露） |
| 数据仓库 | localhost:15432 | warehouse / warehouse（可用任意 PG 客户端直连查看分层数据） |

## 目录结构

```
dags/                  DAG 定义（调度任务编排）
dags/designer_loader.py dag-factory 引导：扫描 designer/*.yaml 渲染成 DAG
dags/designer/         可视化设计器产物（YAML 即 DAG，进 git 版本管理）
plugins/               通用插件（warehouse_pipeline.py 事务引擎、pipeline_tasks.py 调度适配、alerting.py 告警）
plugins/dag_designer/  设计器渲染层（图模型 schema / 节点目录 catalog / YAML 双向转换）
designer/              可视化 DAG 设计器（backend=FastAPI，frontend=React+React Flow）
config/                airflow.cfg 覆盖配置（可选）
logs/                  任务运行日志（自动生成，勿提交；同时被 Alloy 采入 Loki）
backups/               数据库备份（自动生成，勿提交；同步副本在 MinIO）
scripts/               运维脚本（备份/恢复/故障切换/恢复演练/证书生成，详见 docs/ha-dr.md；install-centos7.sh 为 CentOS 7 一键部署）
tests/                 DAG 完整性测试 + 设计器渲染层单测
docker/postgres/       PG 主备镜像（流复制 + WAL 归档配置）
docker/airflow/        Airflow 一体化镜像（官方镜像 + 固化 dag-factory/pytest，双用户可用）
docker/backup/         备份/恢复演练镜像（pg_dump + basebackup + MinIO 同步）
docker/nginx/          TLS 统一入口（certs/ 为证书目录，勿提交）
docker/vault/          Vault 配置（config.hcl；keys.json 初始化后生成，勿提交）
docker/keycloak/       SSO realm 导入配置（realm-datamart.json，详见 docs/sso.md）
docker/warehouse-migrations/  Flyway 迁移脚本（V*.sql，warehouse 结构版本管理）
docker/alertmanager/   告警中枢路由配置（含邮件/企业微信扩展指引）
docker/prometheus/     指标采集配置 + rules/ 告警规则
docker/loki/  docker/alloy/  集中日志（存储 14 天 / 容器与任务日志采集；Alloy 为 Promtail 官方替代品）
docker/warehouse-init/ 数据仓库初始化 SQL（分层 schema + 示例源数据）
config/data_quality/   数据质量套件定义 + baselines/（schema 漂移基线）
docs/                  设计文档与运维手册（ha-dr / sso / deployment / visual-dag-designer）
.github/workflows/     CI（ruff+pytest）/ Security（trivy+pip-audit+bandit）/ CD（发布部署）
docker-compose.yaml    开发环境（+ docker-compose.staging.yml 演练、docker-compose.prod.yml 生产覆盖层）
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

# 数据质量：手动触发质检套件（正常由上游 ETL 的 Dataset 发布自动触发）
docker compose exec airflow-scheduler airflow dags trigger datamart_data_quality

# 设计器用户管理（viewer/editor/admin 三角色；生成/更新 designer/users.json）
scripts/designer-add-user.sh <username> <role> [password]

# Vault 密钥管理（可选）：初始化 → .env 启用后端 → 连接/变量即从 Vault 读取
scripts/vault-init.sh
#   .env 加: AIRFLOW_SECRETS_BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
#            VAULT_TOKEN=<脚本输出的 root token>
docker compose up -d

# SSO（可选）：.env 设 AIRFLOW_OAUTH_ENABLED=true 后 docker compose up -d
# 验证: scripts/test-sso.sh（详见 docs/sso.md）

# 恢复演练（独立 profile，不随 up 启动）
docker compose --profile drill run --rm restore-drill
```

## 运行测试

宿主机 venv 跑 DAG、设计器和纯逻辑回归（未配置测试库时真实 PostgreSQL 用例会跳过）：

```bash
.venv\Scripts\python -m pytest tests/ -q
```

数据正确性验收必须连接真实 PostgreSQL，不能以跳过用例作为验收通过：

```powershell
# PostgreSQL 账号需 CREATEDB 权限；测试只创建并删除随机命名的临时验收数据库
$env:DATAMART_TEST_DSN = 'postgresql://postgres:<测试密码>@127.0.0.1:5432/postgres'
.venv\Scripts\python scripts/run-postgres-acceptance.py --all
# 或使用本机便携 PostgreSQL 二进制，自动启动/停止隔离临时实例
.venv\Scripts\python scripts/run-postgres-acceptance.py --pg-bin 'C:\PostgreSQL\bin' --all
```

事务语义、业务口径、升级步骤与验收边界见 [docs/data-correctness.md](docs/data-correctness.md)。

也可在 scheduler 容器内运行（tests/ 已挂载，pytest 已固化进镜像；test_designer_auth.py 与
test_designer_runtime.py 因依赖 designer/backend 代码在容器内自动跳过；真实数据库验收建议从宿主机运行）：

```bash
docker compose exec airflow-scheduler bash -c \
  "python -m pytest /opt/airflow/tests -q"
```

## 开发工作流

共享代码已整理为 `src/datamart` 内部包，开发环境先安装 `pip install -r requirements.txt`。
设计器编辑接口使用内容 revision，保存与回滚必须携带版本；架构、兼容与验收见
[代码审查修复说明](docs/code-review-fixes.md)。

前端开发：`npm run typecheck`、`npm run lint`、`npm test`、`npm run build`；
节点目录由 `python scripts/export-node-catalog.py` 生成，CI 会检查是否与后端同步。

```bash
# 代码检查与格式化（本地 venv 已含 ruff，或 pip install ruff）
.venv\Scripts\ruff check dags plugins tests        # lint
.venv\Scripts\ruff format dags plugins tests       # 格式化

# 提交代码即触发 CI（.github/workflows/ci.yml）：ruff 检查 + DAG 完整性测试
# DAG 测试在 CI 中用 sqlite + 官方 constraints 运行，无需启动 Docker
```

## 环境分离

```bash
# 开发（默认）：自签名证书 TLS 入口、内置开发密钥、MinIO Console 绑回环地址
docker compose up -d

# 生产：密钥强制从环境变量传入（缺失即拒绝启动）、数据仓库与 MinIO 不暴露端口、Worker 4 副本
export AIRFLOW__CORE__FERNET_KEY=... AIRFLOW__API_AUTH__JWT_SECRET=... _AIRFLOW_WWW_USER_PASSWORD=...
export POSTGRES_REPLICATION_PASSWORD=... MINIO_ROOT_PASSWORD=...
docker compose -f docker-compose.yaml -f docker-compose.prod.yml up -d
```

## 高可用与灾备

完整的架构指标、故障切换/PITR 操作步骤与告警/日志使用说明见 **[docs/ha-dr.md](docs/ha-dr.md)**，要点：

**无状态组件全冗余**：scheduler / apiserver / dag-processor / triggerer 均 2 副本、worker 2(N) 副本，任一容器宕机自动接管，无需人工干预。Redis 开启 AOF，重启后 Celery 队列任务不丢失。

**数据库主备 + PITR**：airflow 元库与 warehouse 各有一个异步流复制热备；当前配置不能承诺已返回成功的提交零丢失。主库宕机执行 `scripts/pg-failover.sh <airflow|warehouse>` 手动接管（人工确认与隔离旧主防脑裂；自动 failover 需 Patroni，见 docs/ha-dr.md「已知限制」），实际 RPO/RTO 需演练。WAL 持续归档 + 每日 base backup 支持 `scripts/restore-pitr.sh` 任意时间点恢复。

**三层备份 + 异地副本**：每日 `pg_dump` + `pg_basebackup` 落 `./backups`（各保留 7 份），`backup-shipper` 每 60 秒把备份与 WAL 归档镜像到 MinIO 对象存储。备份中断超 26h 触发 `BackupStale` 告警。

```bash
scripts/backup.sh                    # 立即手动备份一次（不等每日定时）
scripts/restore.sh backups/airflow_20260908_042223.dump    # 整库恢复（回到最近 dump）
scripts/restore-pitr.sh warehouse "2026-09-15 10:30:00"    # PITR 任意时间点恢复
scripts/restore-drill.sh             # 恢复演练：验证备份真的可恢复（失败触发 critical 告警）
```

**告警中枢**：Prometheus 告警统一进 Alertmanager 做分组、去重、静默、抑制与路由（https://localhost/alertmanager/），再送达告警中心；Watchdog 常鸣告警监控告警链路自身死活。复制中断、复制延迟、备份中断、恢复演练失败均有内置告警规则。

## 数据正确性验收（2026-10-02）

完整回归 **176 项通过、0 项跳过**，包含 psycopg2/psycopg3 各 29 项真实 PostgreSQL 场景，
以及 Airflow 实际 PostgresHook 连接工厂验收。覆盖迟到提交、源删除/日期修正、整批回滚、
跨 DAG 并发、Worker 强制终止、提交响应丢失与质量失败阻断发布。
详情与容量基线见 [验收报告](docs/data-correctness-acceptance.md)。

## 历史验收基线（本次改造前）

以下为原环境交付记录；当前代码与验收范围以本次数据正确性报告为准。

- ETL 全链路落库：dm.daily_city_sales 按 业务日期×城市 聚合
- 增量抽取：新插/更新 2 行被精确拾取（105 cancel→paid upsert 生效），水位线按微秒精度推进
- 空增量边界：0 行抽取、跳过刷新、水位线不变、运行成功
- 区间补跑：删除 09-06 数据后经 conf 区间重建恢复，日报同步刷新；缺 conf 明确报错
- Dataset 血缘：ETL 成功后报表 DAG 以 `asset_triggered__` 前缀自动运行
- 失败路径：重试耗尽 → failed → 告警回调送达告警中心 → all_done 通知执行
- Deadline 漏跑告警：长任务运行超过截止时间 → 告警中心收到含 DAG/Run/截止时间的告警
- Grafana 告警规则：Scheduler 心跳丢失、任务失败率等规则已迁移至 Prometheus 侧（docker/prometheus/rules/），firing 后经 Alertmanager 送达告警中心
- 指标链：Prometheus 采集到调度器心跳、任务完成计数（按状态）、执行器槽位、DAG 运行时长
- 单任务重跑：UI/API Clear + run 重新排队 → 任务按新尝试次数重跑
- 定时调度：@daily 调度运行自动产生（scheduled__ 前缀）
- 多 Worker：2 副本均注册并处理任务
- 多 Scheduler 容灾：停掉 1 个 Scheduler 后手动触发的 DAG 仍被另一实例调度并成功执行
- 灾备恢复：备份 → 删除 4 条 dag_run → 从 dump 恢复 → 记录完整还原，Airflow 组件自动重启
- 可视化设计器端到端：API 创建 3 节点连线 DAG → dag-factory 渲染 → Airflow 触发 success → 删除后消失
- 设计器 API：token 认证 / 409 冲突 / mtime 乐观锁 / 成环与缺参校验拦截 全部实测通过
- 坏 YAML 隔离：损坏文件不影响其他设计器 DAG 与既有 6 个 DAG 的加载
- DAG 完整性测试：32 项全部通过（9 项 DAG 结构 + 23 项设计器渲染层）

以下 P0 高可用能力（操作步骤见 docs/ha-dr.md，2026-09-15 已全部实测通过）：

- TLS 统一入口：https://localhost/ 全部 UI 可达（Airflow/Grafana/Prometheus/Alertmanager/Flower/告警中心/设计器:8443），http 自动 301 跳转
- PG 流复制：两对主备均 `pg_stat_replication` 可见 streaming 连接，备库 `pg_is_in_recovery()=t`
- PG 故障切换：停 warehouse 主库 → `scripts/pg-failover.sh warehouse` promote 成功 → 新主写入验证通过 → 重建备卷后复制链恢复（主库存活时脚本拒绝执行，防脑裂实测通过）
- WAL 归档：归档卷与 MinIO `datamart-wal` 桶持续有新段文件（archive_timeout=60s）
- 备份异地副本：`scripts/backup.sh` 后 60s 内 MinIO `datamart-backups` 出现同名 dump 与基准备份
- 恢复演练：`scripts/restore-drill.sh` PASS（dag_run=68 行、daily_city_sales=6 行成功还原），指标入 Prometheus
- Redis 持久化：AOF 已开启，`docker compose restart redis` 后 broker 队列不丢
- 告警中枢：Watchdog 每 5 分钟送达告警中心；TargetDown 告警实测 firing→送达；Grafana 历史规则已迁移 Prometheus 侧
- 集中日志：Grafana Explore 按 `{compose_service="airflow-scheduler"}`、`{dag_id="datamart_daily_etl"}` 均可检索；ETL 手动触发 success 且 Dataset 血缘触发日报 DAG 正常

以下 P1 商业交付能力（2026-09-15 已全部实测通过；操作细节见 docs/deployment.md 与 docs/sso.md）：

- Vault secrets backend：种子连接写入 Vault 专属识别参数后，`airflow connections get warehouse` 返回值命中该参数，证明连接确实从 Vault 读取；未启用后端时行为与旧版完全一致
- Keycloak SSO：`scripts/test-sso.sh` 3/3 PASS（可达性/direct grant 换 token 角色映射/登录页）；`AIRFLOW_OAUTH_ENABLED=true` 时 `/auth/login/keycloak` 302 跳转 Keycloak authorize 端点并跟随可达（端到端链路实证）
- 设计器 RBAC：三角色登录/错误密码 401；权限矩阵实测 viewer 读 200、写 403、用户管理 403，admin 全通；审计 JSONL 逐行落盘（含 403 拒绝记录）
- 数据质量：`datamart_data_quality` 手动触发 success，两套件得分=1 入 Pushgateway 并被 Prometheus 抓取；`DataQualityScoreLow` 规则已装载
- Flyway 迁移：启动即对存量 warehouse baseline 并应用 V1（dwh_quarantine 隔离区 schema），flyway_schema_history 版本落库
- 资源治理：抽查 worker(2C/2G)、postgres(1C/1G)、redis(0.5C/512M)、nginx(0.25C/128M)、vault(0.5C/256M) 硬上限全部生效；Worker celery autoscale 池以 MIN=2 起步按需扩至 8
- CD/安全流水线：`docker compose config`（dev/staging/prod）全部校验通过；release.sh/cd.yml/security.yml/dependabot 镜像清单统一为 6 个自建镜像；deploy.sh 语法校验通过
- 测试双侧通过：宿主机全套 104 项 pytest PASS；scheduler 容器内 72 项 PASS（设计器后端测试按 conftest 规则自动跳过）
- 全量 35 容器（含 vault/keycloak/flyway 新增）运行无异常

## 已知设计说明

- Airflow 3 手动触发的运行 `logical_date` 为空，DAG 内用 `_stat_date()` 兜底取当前业务日期
- DAG Run 状态由叶子任务决定：失败任务若被 all_done 通知节点吸收，Run 整体仍显示 success
- SLA 已在 Airflow 3.0 移除，超时告警请使用 Deadline Alerts

## 生产部署注意

- 更换 `docker-compose.yaml` 中的 `AIRFLOW__CORE__FERNET_KEY`、`AIRFLOW__API_AUTH__JWT_SECRET`、管理员密码、各数据库密码、PG 复制口令与 MinIO 密码（prod 覆盖层会对关键项强制检查）
- 用真实证书替换 `docker/nginx/certs/server.{crt,key}`（开发为自签名，浏览器会有警告）；对外域名变化时同步调整 `AIRFLOW_BASE_URL` 与各组件 `--web.external-url` / `GF_SERVER_ROOT_URL`
- TLS 终结在 nginx，容器网络内部仍是 HTTP（内网 mTLS 属后续路线，见 docs/ha-dr.md「已知限制」）
- 当前使用 FAB 认证管理器；如需 SSO 可切换 Keycloak 等认证方式
- 告警 webhook 在 Admin → Variables 配置 `datamart_alert_webhook` 即生效；Alertmanager 侧扩展邮件/企业微信/值班表见 `docker/alertmanager/alertmanager.yml` 尾部注释
- 邮件告警需配置 SMTP 后打开 DAG 中的 `email_on_failure`
- 本形态为单宿主机高可用（任一容器/进程宕机无感）；跨主机容灾需 K8s/Helm 部署形态（后续路线）
