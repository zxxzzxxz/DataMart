# 高可用与灾备运维手册（HA / DR Runbook）

本文档是 DataMart 平台 P0 级高可用能力的操作手册：架构与指标、故障切换、PITR 恢复、备份体系、恢复演练、告警与日志。

## 1. 架构与可靠性指标

```
                        ┌──────────── nginx（TLS 终结 :443/:8443）────────────┐
  浏览器/客户端 ──https──►  / → airflow-apiserver ×2   /grafana/  /prometheus/ │
                        │  /alertmanager/  /flower/  /alerts/  :8443 设计器   │
                        └───────────────────────┬─────────────────────────────┘
  ┌──────────── Airflow ────────────┐           │
  │ scheduler ×2  dag-processor ×2  │  元数据    │  statsd
  │ triggerer ×2  worker ×2(N)      ├───────────┼──────────────────────┐
  └───────────┬─────────────────────┘           ▼                      ▼
              │ SQL              postgres（主）──流复制──► postgres-replica（热备）
              │                     │  WAL 归档（archive_timeout=60s）
        redis（AOF 持久化）          ▼
        (Celery broker)      pg-wal-archive ──┐
                                              │  warehouse / warehouse-replica 同构
  ┌──────────── 灾备 ─────────────────────────▼────────────────────────────┐
  │ postgres-backup：每日 pg_dump + pg_basebackup → ./backups（本地保留 7 份）│
  │ backup-shipper：每 60s 镜像 ./backups 与 WAL 归档 → MinIO（异地副本）     │
  │ restore-drill：一键恢复演练（临时 PG 实例 + 行数校验 + 指标上报）          │
  └──────────────────────────────────────────────────────────────────────────┘
  ┌──────────── 告警 / 日志 ─────────────────────────────────────────────────┐
  │ Prometheus 规则 + Grafana 规则 → Alertmanager（分组/静默/抑制/Watchdog）   │
  │   → alert-webhook 告警中心                                                │
  │ Alloy（容器日志 + Airflow 任务日志）→ Loki（14 天）→ Grafana Explore      │
  └──────────────────────────────────────────────────────────────────────────┘
```

| 指标 | 能力 | 依据 |
|---|---|---|
| 调度层 RTO | 秒级，自动 | scheduler ×2 / apiserver ×2 / triggerer ×2 / dag-processor ×2，共享元数据库协调，实例宕机自动接管 |
| Worker RTO | 秒级，自动 | Celery 多副本，broker 消息重投 |
| Broker 数据 | 重启不丢 | redis AOF everysec |
| 数据库 RPO | 取决于异步复制延迟，需实测 | 当前未配置同步复制；WAL 归档是额外恢复来源，不是已提交数据零丢失保证 |
| 数据库 RTO | 分钟级，**手动** | `scripts/pg-failover.sh` promote 热备（自动 failover 需 Patroni，见「已知限制」） |
| 备份 RPO | dump 间隔 24h；WAL 随归档/同步进度变化 | `archive_timeout=60` 是切段配置；归档及对象同步可能延迟，MinIO 默认仍在同一宿主机 |
| PITR | 支持 | base backup + WAL 归档重放到任意时间点 |

## 2. 数据库故障切换（主库宕机）

故障切换是**人工确认后执行**的操作（避免脑裂双写）。以 airflow 元库为例（warehouse 同构，参数换 `warehouse`）：

```bash
# 1. 确认主库确实不可用（不要凭监控告警一面之词，防止脑裂）
docker compose ps postgres

# 2. 提升热备为新主（若主库其实还活着，脚本会拒绝执行，--force 才放行）
scripts/pg-failover.sh airflow

# 3. 脚本会打印后续指引，核心两步：
#    a) 停掉旧主，防止其恢复后双写：docker compose stop postgres
#    b) 切换应用连接：Airflow 连接串指向服务名 postgres。
#       最简方式 —— 修改 docker-compose.yaml，把 postgres 服务的 build/数据卷角色
#       与 postgres-replica 对调（或让新主容器继承 postgres 服务名）后
#       docker compose up -d，Airflow 组件重启生效（RTO 主要耗在这里）。

# 4. 旧主修复后反向重建为新备（pg-failover.sh 输出含详细三步指引）：
#    清空旧数据卷 → 以 replica-entrypoint 重新 basebackup → 验证 standby 状态
```

验证复制链路健康：

```bash
# 主库上应看到 1 行 state=streaming
docker compose exec postgres psql -U airflow -c "SELECT client_addr, state, sync_state FROM pg_stat_replication;"
# 备库上应返回 t（处于恢复模式）
docker compose exec postgres-replica psql -U airflow -c "SELECT pg_is_in_recovery();"
```

复制中断/延迟由 Prometheus 规则 `PostgresReplicationMissing` / `PostgresReplicationLagHigh` 自动告警。

## 3. PITR 任意时间点恢复

适用场景：误删数据、错误写入，需要回到某个历史时间点。

```bash
# 恢复到指定时间点（不触碰正在运行的正式库，起独立实例核对）
scripts/restore-pitr.sh airflow "2026-09-15 10:30:00"
```

流程：取最近一份 `pg_basebackup` 基准备份 → 解包到临时实例 → 配置 `restore_command` 从 WAL 归档重放 → `recovery_target_time` 到达后自动 promote → 开放本机 55432 端口供核对。确认数据无误后，按脚本输出的指引 pg_dump 导出并人工替换正式数据（**脚本绝不自动替换正式库**）。

WAL 归档有两份副本：本地卷 `pg-wal-archive-*` 与 MinIO 的 `datamart-wal` 桶，本地卷损坏时脚本支持从 MinIO 取（设置 `WAL_ARCHIVE_VOLUME` 或按脚本提示操作）。

## 4. 备份体系（三层）

| 层 | 内容 | 位置 | 保留 |
|---|---|---|---|
| 逻辑备份 | 每日 `pg_dump -Fc`（两库） | `./backups/*.dump` + MinIO `datamart-backups` | 本地 7 份；远端不限 |
| PITR 基线 | 每日 `pg_basebackup -Ft -z` | `./backups/base/` + MinIO `datamart-backups/base/` | 本地 7 份；远端不限 |
| WAL 归档 | 持续归档（60s 强制切换） | `pg-wal-archive-*` 卷 + MinIO `datamart-wal/` | 本地 7 天（`WAL_LOCAL_KEEP_DAYS`）/ 远端默认 30 天（`WAL_REMOTE_EXPIRY_DAYS`，shipper 幂等维护 MinIO 过期策略） |

- 立即手动备份：`scripts/backup.sh`
- 常规恢复（整库回到最近一次 dump）：`scripts/restore.sh backups/<db>_<ts>.dump`
- 备份中断告警：备份成功时间戳推送 Pushgateway，超过 26h 无成功备份触发 `BackupStale` 告警
- MinIO Console（开发）：http://127.0.0.1:9001 （minioadmin / minioadmin123；生产环境不暴露）

## 5. 恢复演练（备份可用性证明）

备份不可恢复比没备份更危险，演练必须常态化：

```bash
# 手动执行一次演练（建议加入 crontab/计划任务，如每日凌晨）
scripts/restore-drill.sh
```

演练内容：起一次性临时 PG 实例 → 恢复两库最新 dump → 校验关键表行数（`dag_run`、`dm.daily_city_sales`）→ 结果追加 `backups/restore-drill.log` 并推送指标。演练失败触发 `RestoreDrillFailed` critical 告警。退出码 0/1，可直接接入 CI 或计划任务。

## 6. 告警体系

```
Prometheus 规则（docker/prometheus/rules/datamart.yml：Watchdog/TargetDown/ ─┐
  端点探测/Scheduler 心跳/任务失败率/复制中断与延迟/备份陈旧/演练失败）        ├─► Alertmanager ─► 告警中心 /alerts/
Airflow 任务失败 / Deadline 回调（plugins/alerting.py）──────────────────────┴───────────────►（直连）
```

- **Alertmanager**（https://localhost/alertmanager/）：分组（alertname+severity+job）、去重、静默（Silences 页，数据持久化在 alertmanager-data 卷）、抑制（critical 抑制同名 warning）、重复提醒（critical 1h / 默认 4h）
- **Watchdog 常鸣告警**：每 5 分钟推送一次到告警中心。告警中心**收不到 Watchdog = 告警链路本身断了**，这是告警系统的死信开关
- 扩展邮件 / 企业微信 / 飞书 / 值班表（OnCall）：见 `docker/alertmanager/alertmanager.yml` 尾部注释

## 7. 集中日志

- Grafana → Explore → 数据源 **Loki**
- 容器日志：标签 `compose_service`（如 `airflow-scheduler`）、`compose_project`
- Airflow 任务日志：标签 `dag_id`、`task_id`、`filename`，示例查询：`{dag_id="datamart_daily_etl"}`
- 保留 14 天（loki-config.yaml `retention_period`）

## 8. 已知限制（后续路线）

- **数据库 failover 是手动的**。自动 failover 需要 Patroni + etcd（或云 RDS），是下一阶段工作；当前 RTO 分钟级、且有人工确认防脑裂，是有意取舍
- **容器网络内部仍是明文 HTTP**（TLS 终结在 nginx）。如需全链路加密（内网 mTLS），后续加服务网格或内部 CA
- **单宿主机**：Docker Compose 无法跨主机容灾。多机部署形态（K8s + Helm）是 P1 路线；当前所有无状态组件已多副本化，迁移无架构障碍
- MinIO 为单节点（纠删码需 4 盘起步）；异地/异质容灾可再把 MinIO 桶版本化后同步到云对象存储
- 老环境（已有数据卷）升级后，需执行一次 `scripts/pg-enable-replication.sh` 补齐复制用户与 pg_hba 规则，备库才能拉起
