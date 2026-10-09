# 生产数据正确性与恢复

本次覆盖内置销售流水线的全量、增量、补跑、质量门禁、日报和资产发布恢复。
核心实现为 `plugins/warehouse_pipeline.py`；Airflow 只负责调度、重试、发布事件和告警。

## 业务数据契约

- ODS 与目标数仓在同一 PostgreSQL 数据库。普通 INSERT/UPDATE/DELETE 与变更日志同事务提交。
- 增量捕获依据数据库触发器，不依赖 `updated_at` 的精度、唯一性或提交顺序；业务可以修正历史日期。
- 用户维度采用 Type 1：用户改名或变更城市后，相关历史订单按当前用户资料重新打宽、重新汇总。
  如果业务要求保留成交时城市，必须在源订单中提供交易快照或改为 SCD Type 2，不能混用这两种口径。
- 删除源订单会删除对应事实，并刷新旧日期；订单日期修正同时刷新旧日期、新日期。
- 空源和没有已支付订单是合法状态，相应集市和日报可以为空。此时新鲜度警告仍保留。
- 补跑从已提交的 DWH 明细重建指定日期区间，最多 31 天；不消费待处理源变更。
  要先同步源端历史修正，应先运行增量或全量，再进行补跑。
- 禁止关闭捕获触发器、使用 `session_replication_role=replica` 绕过捕获，或直接修改受管理目标表。
  运维批量导入如曾绕过捕获，恢复触发器后必须执行一次全量校准。流水线会拒绝在捕获触发器缺失或禁用时发布。

## 事务与并发

所有内置写入 DAG 在数据库中使用同一个 session advisory lock。先获得锁，再开启 REPEATABLE READ
事务，避免等待锁期间提前取得旧快照。连接必须是独立会话；不能经过 PgBouncer 的 transaction pooling。
锁等待上限默认 30 秒，超时由 Airflow 重试；单条 SQL 上限 10 分钟。连接关闭/进程终止时释放锁。

同一事务完成：读取可见源变更 → 明细更新/删除 → 受影响日期汇总 → 日报 → 字段级对账 →
全表城市/日期/单量/金额对账 → YAML 质量门禁 → 确认源变更 → 批次记录与待发布事件。
任何 SQL、对账或必过质检失败都会回滚整个事务；读者继续看到上一份完整数据。
使用 DELETE/INSERT 的同一事务替代 TRUNCATE 分开提交，普通 SELECT 不会看到刷新中的空表。

变更序号只作唯一标识。仅删除本批快照实际读取的日志行，不使用 `change_id <= max(id)`：
序号较小但较晚提交的事务，仍会留给下一批处理。第一次增量会自动做全量校准；每日全量用于恢复基线。

`dwh.etl_batches.batch_id` 使用 `dag_id/run_id`。同一逻辑批次的重试直接返回已经提交的结果；
即使客户端没有收到 COMMIT 响应，也不会重复应用后续源变更。批次内容、模式与补跑参数均可核验。
质量门禁不会隔离/删除已经发布的数据；非法数据阻止整批发布，源变更保留到修正后重试。
发布后的 DQ DAG 与日报 DAG 使用只读一致快照。指标网络请求在事务结束后执行，监控故障不改变提交结果。

机制依据：[PostgreSQL 16 advisory locks](https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS)
与 [REPEATABLE READ](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-REPEATABLE-READ)。

## 数据库与 Airflow 之间的发布恢复

数据库提交会同时留下 `event_delivered_at IS NULL` 的持久化记录。
生产 DAG 的 `publish_dataset` 成功时，Airflow SDK 在同一个任务成功请求中携带资产事件；
其后 `acknowledge_publication` 使用 all_success 依赖确认本次实际发布的批次 ID。

`datamart_publish_outbox` 每分钟补发待确认批次，新建时默认启用。无待发批次时跳过，避免制造空资产事件。
发布后、确认前进程中断可能导致重复事件；投递语义为至少一次。日报与 DQ 读者读取最新完整、已验证快照，
不将每个通知当成互斥的历史版本，也不重复写日报。如果未来添加收费、邮件等有副作用的消费者，
必须按批次 ID 或 revision 去重。

## 升级已有环境

1. 暂停内置写入 DAG，等待正在执行及排队的旧版本运行全部结束；本次任务 ID 有变化，不能在旧任务中途换代码。
2. 先备份 warehouse，然后运行 Flyway 的 V2 迁移。V1 未改动，不需要修复历史 checksum。
3. 发布包含新引擎、DAG 和质量配置的代码/镜像。基础 Compose 已要求 Flyway 成功后再启动 Airflow。
4. 执行 `datamart_daily_etl` 做首次全量校准；确认质检、批次记录及目标数据通过后恢复调度。
5. 确认 `datamart_publish_outbox` 未暂停。已有资产消费 DAG 继续订阅同一个资产 URI。

```bash
docker compose run --rm flyway
# 恢复调度前完成首次全量校准并检查运行结果
docker compose exec airflow-scheduler airflow dags trigger datamart_daily_etl
docker compose exec airflow-scheduler airflow dags unpause datamart_publish_outbox
```

运维查看（只读）：

```sql
SELECT * FROM dwh.etl_pipeline_state;
SELECT batch_id, mode, revision, committed_at, event_delivered_at
FROM dwh.etl_batches ORDER BY revision DESC LIMIT 20;
SELECT source_table, count(*), min(changed_at) FROM dwh.etl_changes GROUP BY source_table;
SELECT count(*) AS pending_events FROM dwh.etl_batches WHERE event_delivered_at IS NULL;
```

失败排查：优先看 Airflow apply_batch 日志中的字段对账/质量报告。修正 ODS 后清理失败任务重试；
切勿手工删除待处理日志或仅推进旧水位线。合法的业务规则变化需要先修改并评审质量套件。
旧 `plugins/incremental.py` 已删除（仅演示辅助函数，与事务引擎水位线诊断字段存在机制冲突），内置生产 DAG 均不依赖。

## 高可用边界

协调信息、源变更与业务数据都存放在同一 PostgreSQL 中，支持多个 Scheduler/Worker 竞争，
不依赖 Worker 本地文件或进程内锁。Worker 被强制终止和数据库会话断开后的回滚、锁释放、重试已验收。
连接配置包含连接超时、TCP keepalive 与 read-write 目标检查；只连接可写主库。

数据库主机高可用仍由既有主备或托管 PostgreSQL 提供：应用应使用稳定的主库入口，切换必须隔离旧主。
异步复制不能承诺已经返回成功的提交零丢失；如业务要求此承诺，应在独立宿主机/故障域上部署
同步复制或具备相应承诺的托管数据库，接受同步副本不可用时暂停写入的取舍。
本次没有将单宿主机 Compose 宣称为跨主机高可用；实际主备切换、跨宿主机容灾和生产 RPO/RTO 需环境演练。

本方案针对目前同库 PostgreSQL ODS；外部业务库需先通过可靠 CDC 接入 ODS，不能把本触发器当作跨库 CDC。
质量套件与全局汇总对账会扫描目标表，容量扩展应依据真实量级压测；万级订单验收不代表千万级性能承诺。

## 可重复验收

```powershell
# 便携 PostgreSQL 16：临时目录、随机端口、只监听回环地址，执行后停止服务
.venv\Scripts\python scripts/run-postgres-acceptance.py --pg-bin 'C:\PostgreSQL\bin' --all
# 已有专用测试 PostgreSQL：仅创建/删除随机命名的验收数据库，需要 CREATEDB
$env:DATAMART_TEST_DSN = 'postgresql://postgres:<测试密码>@127.0.0.1:5432/postgres'
.venv\Scripts\python scripts/run-postgres-acceptance.py --all
```

脚本在缺少数据库时直接失败，不将 skipped 当成验收通过。JUnit 默认输出 `logs/acceptance/postgres.xml`，
包含万级订单与千条变更的耗时属性。CI 使用独立 PostgreSQL 16 服务运行真实验收并保存 JUnit。
同一套数据/故障场景分别运行于 psycopg2 和 psycopg3；完整回归还会经过 Airflow 实际 PostgresHook 连接工厂。
覆盖矩阵与当前运行证据见 [data-correctness-acceptance.md](data-correctness-acceptance.md)。
