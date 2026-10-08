# 数据正确性改造验收记录

验收日期：2026-10-02（Asia/Shanghai）。范围为内置销售流水线的数据正确性、事务恢复、并发与资产发布契约。
最终完整回归：**176 passed，0 failed，0 errors，0 skipped**；耗时约 131 秒，5 项第三方运行/弃用警告。

## 环境与证据

- Windows，Python 3.13.14；Apache Airflow 3.3.2。
- 真实 PostgreSQL 16.15 临时实例，只监听回环地址，随机端口，验收后已停止。
- psycopg2-binary 2.9.13 与 psycopg 3.3.5；同一套 29 项数据库场景分别运行，合计 58 项。
- 另有 2 项通过 Airflow 实际 PostgresHook 连接工厂运行事务、审计与 outbox 确认。
- 其余 116 项包含原有设计器/认证/DAG 回归、请求约束和实际 Airflow SDK 资产元数据契约。
- JUnit 原始证据：`logs/acceptance/postgres.xml`（运行产物，git 忽略）。测试只创建和删除其自身随机命名的数据库，未迁移正式 warehouse。

运行命令：

```powershell
.venv\Scripts\python scripts/run-postgres-acceptance.py --pg-bin '<PostgreSQL 16 的 bin 目录>' --all
.venv\Scripts\ruff check dags plugins tests scripts/run-postgres-acceptance.py
.venv\Scripts\ruff format --check dags plugins tests scripts/run-postgres-acceptance.py
```

## 场景结果

| 验收项 | 验收方法及通过条件 | 结果 |
|---|---|---|
| 首次增量与迁移重入 | 首次自动全量校准；重复执行 V2 不产生双重捕获触发器 | 通过 |
| 订单字段修正 | 日期、用户、金额、状态、主键修正；独立 Python oracle 对账明细、城市汇总和日报 | 通过 |
| 用户维度变化 | 仅改用户城市/姓名也重算历史订单和旧城市汇总 | 通过 |
| 删除和空源 | 单条删除、全部删除、TRUNCATE；清理旧事实/汇总/日报，合法空源可提交 | 通过 |
| 同时戳、旧时间戳、迟到提交 | 较小日志序号事务晚于较大序号提交，后续批次仍完整消费 | 通过 |
| 快照之后提交的变更 | 首批读取旧快照；新变更保留至下批，不混用不同时间的源数据 | 通过 |
| 中途失败 | 删除汇总后、质检后、提交前分别注入故障；所有目标、水位线、批次状态保持原样 | 通过 |
| 数据库连接中断 | PostgreSQL 主动终止处理会话；事务回滚、锁释放、相同批次重试成功 | 通过 |
| Worker 强制终止 | 独立 Python Worker 持有写入事务时强制结束进程；读者仍见旧完整数据，重试成功 | 通过 |
| 质量失败 | 非法订单状态和错误质检 SQL 都阻止提交；不删除已发布数据、不生成失败批次事件 | 通过 |
| 并发写入及读取 | 全量、增量和补跑同时请求；后两者等待数据库锁，读者不见刷新中的空表 | 通过 |
| 重复批次 | 并发同批次只提交一次；不同参数复用已提交批次 ID 被拒绝 | 通过 |
| 提交响应丢失 | 提交后注入进程异常；相同 ID 返回已提交结果，后续源变更不被误消费 | 通过 |
| 补跑 | 修复指定日期的集市与日报，保留尚未消费的源端变更 | 通过 |
| 捕获失效与锁超时 | 禁用捕获时拒绝发布；锁等待超时不留下部分写入，释放后可重试 | 通过 |
| 已发布数据审计 | 严格只读快照；发现非法数据时报告失败，已发布行和表保持原样 | 通过 |
| 发布恢复契约 | 资产包含批次 ID/revision；空 outbox 跳过；all_success 发布后才确认实际发出的 ID | 通过 |
| 驱动与监控故障 | psycopg2/3 及实际 PostgresHook 可执行；监控请求失败不改变已经提交的结果 | 通过 |

## 容量基线

真实库中生成 10,000 笔附加订单，总计 10,010 笔；全量校准后再修改 1,000 笔订单并运行增量。
耗时仅计算事务引擎，不包括初始化库、源端造数、调度等待和网络指标推送。

| 驱动 | 10,010 笔全量校准 | 1,000 条变更增量 |
|---|---:|---:|
| psycopg2 | 0.306 秒 | 0.222 秒 |
| psycopg3 | 0.317 秒 | 0.230 秒 |

这是本机验收基线，不代表生产 SLO 或千万级规模性能承诺。

## 工程检查与上线条件

- Ruff lint、format check、git diff whitespace 检查全部通过。
- 开发、生产、staging、mini 四种 Compose 配置解析通过。
- CI 已增加独立 PostgreSQL 16 验收，以及 Airflow + PostgreSQL 的完整回归；保存 JUnit 证据。
- 上线前先排空旧版本运行、执行 V2 迁移、发布新代码并完成首次全量校准；具体步骤见 [改造说明](data-correctness.md)。

## 未宣称已验收的能力

- 本机 Docker 引擎不可用，因此没有运行整套 Compose 服务及真实 Scheduler/Worker 的端到端资产调度。
  Airflow DAG 依赖/失败传播、SDK 事件元数据、实际 PostgresHook 和真实数据库事务均已分别验证。
- 未进行数据库主机宕机、真实主备切换、跨宿主机恢复或生产 RPO/RTO 演练。
- 普通查询在单条语句或 REPEATABLE READ 快照内读取完整提交；多个 READ COMMITTED 查询之间可能读到不同 revision。
- 用户城市采用 Type 1 当前归属口径；外部源 CDC、多租户权限、任意设计器 DAG 的跨节点事务不属于本次内置流水线验收。
