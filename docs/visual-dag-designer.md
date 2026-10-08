# 可视化 DAG 设计器 — 功能规划与技术设计

> 状态：MVP（第 1 期）已于 2026-09-08 验收；第 2 期已于 2026-10-02 完成（试跑触发/运行状态回显/删除保护/任务级 trigger_rule 与 retries/版本历史/SQL 预检，节点类型扩至 7 种；增量同步与 Python 函数节点仍未做）。剩余路线见 §6。
> 本文档是开发的蓝图与基线，后续分期实施必须达到对应验收标准。

## 1. 目标与边界

**目标**：让使用者通过拖拽图标 + 连线的方式配置 Job 及前后依赖关系，保存后自动生成 Airflow DAG 并参与调度、监控、告警、重试等既有全部能力。

**明确不做**（第一版）：
- 不替换 Airflow UI，不与 Airflow 插件机制耦合（独立 Web 应用）
- 不做多人实时协作（先单用户编辑 + 文件锁提示）
- 不做任意 Python 代码编辑（节点从预置任务模板中选择，填参数）

## 2. 总体架构

```
┌─────────────────────┐   HTTP/JSON   ┌──────────────────────┐
│  designer-ui        │ ────────────► │  designer-api        │
│  React + React Flow │ ◄──────────── │  FastAPI             │
│  (拖拽画布/属性表单) │               │  (DAG CRUD/校验/触发) │
└─────────────────────┘               └──────────┬───────────┘
                                                 │ 读写 YAML（唯一事实源）
                                                 ▼
                                      dags/designer/*.yaml  ──► git 版本管理
                                                 │
                                                 │ dag-factory 扫描渲染
                                                 ▼
                                      Airflow Scheduler（既有栈，零改动）
                                      → 执行 / 日志 / 重试 / 告警 / Grafana
```

**核心原则：YAML 文件是唯一事实源（single source of truth）**
- 画布上的图结构不存数据库，直接落盘为 YAML，进 git、可 Code Review、可回滚
- designer-api 负责 JSON ↔ YAML 双向转换；渲染成 Airflow DAG 由 dag-factory 完成
- 这样即使 designer 整体下线，生成的 DAG 依然在 Airflow 中正常运行——设计器只是"编辑器"，不是运行时依赖

## 3. 模块设计

### 3.1 dag-renderer（Python 包，`plugins/designer/`）

核心转换层，无 Web 依赖，可单测：

- `schema.py`：pydantic 模型 `DesignerDag`（dag_id / schedule / 描述 / 节点列表 / 连线列表 / 每个节点的类型与参数），含校验（dag_id 合法、无环、无悬空节点、必填参数齐全）
- `to_yaml()` / `from_yaml()`：DesignerDag ↔ dag-factory 格式 YAML 的**双向**转换（双向是硬要求：保证手改 YAML 后界面仍能打开）
- `node_catalog.py`：节点类型目录（见 §4），每种类型声明参数 schema，供前端渲染属性表单

渲染引擎默认用 dag-factory（Astronomer 维护，兼容 Airflow 3）；`plugins/designer/loader.py` 提供引导 DAG 文件扫描 `dags/designer/*.yaml`。若后期 dag-factory 表达力不够，替换为自研薄 loader（约 150 行），接口不变。

### 3.2 designer-api（FastAPI，`designer/backend/`）

| 端点 | 说明 |
|---|---|
| `GET /api/dags` | 列出所有设计器 DAG（读 YAML 目录） |
| `GET /api/dags/{id}` | YAML → JSON，供画布加载 |
| `PUT /api/dags/{id}` | JSON → 校验 → 写 YAML（整体保存） |
| `DELETE /api/dags/{id}` | 删除 YAML（DAG 从 Airflow 消失） |
| `POST /api/dags/validate` | dry-run 校验，返回错误列表（不落盘） |
| `GET /api/node-types` | 节点类型目录 + 参数 schema（驱动前端表单） |
| `POST /api/dags/{id}/trigger` | 透传调 Airflow REST API 触发试跑 |
| `GET /api/dags/{id}/runs` | 透传查 Airflow 运行历史（画运行状态回画布） |

认证：v1 用简单 token（环境变量配置）；v2 再考虑对接 Airflow FAB 账号体系。

### 3.3 designer-ui（React + TypeScript + Vite + React Flow，`designer/frontend/`）

三个页面：

- **列表页**：DAG 卡片（名称、调度、状态、最近运行），新建/删除入口
- **画布页**（核心）：
  - 左侧：节点面板（按目录分组，拖入画布）
  - 中间：React Flow 画布（连线 = 依赖，支持框选、对齐、撤销）
  - 右侧：属性面板（选中节点后按 `node-types` 返回的 schema 渲染表单）；DAG 级属性（dag_id、调度 cron、告警开关、重试）
  - 顶部：保存（先调 validate，有错标红节点）、校验、试跑按钮
- **运行页**：嵌 Airflow Grid 视图链接 + 运行状态轮询着色（节点绿/红）

### 3.4 部署形态

compose 新增一个服务 `dag-designer`（后端 uvicorn + 前端构建产物由 nginx 托管，或开发期 vite dev server）：

- 挂载 `./dags/designer`（读写 YAML）和 `./plugins`（复用 alerting/incremental）
- 端口 `8081`
- 与 Airflow 通过内部网络通信（调 `http://airflow-apiserver:8080/api/v2`）

## 4. 节点类型目录（当前 7 种，可扩展）

| 节点类型 | 底层实现 | 关键参数 |
|---|---|---|
| SQL 转换 | SQLExecuteQueryOperator（conn=warehouse） | SQL 语句 |
| 数据校验 | SQLValueCheckOperator | SQL、期望值 |
| 通知 | PythonOperator（固定 webhook，复用 `plugins/alerting.py`） | 消息模板 |
| Bash | BashOperator | bash_command（worker 容器内执行） |
| HTTP | HttpOperator | endpoint（建议绝对 URL）、method、data、headers |
| SQL 传感器 | SQLSensor（common.sql provider） | sql、poke_interval、timeout、mode |
| 占位 | EmptyOperator | 无（汇聚/分组占位） |

所有节点类型共享两个可选高级字段：`trigger_rule`（all_success/all_done/all_failed/none_failed/one_success/one_failed/none_skipped）与任务级 `retries`（留空继承 DAG 级）。

尚未实现（路线图）：增量同步节点（复用 `plugins/incremental.py` 水位线框架）、Python 函数节点（预注册函数库下拉选择）。

新增节点类型 = 在 `plugins/dag_designer/catalog.py` 注册一条 + 底层 operator 实现，前端零改动（表单由 schema 驱动）。

## 5. YAML 存储格式示例

`dags/designer/orders_daily.yaml`（dag-factory 兼容格式，附加 designer 元数据段）：

```yaml
orders_daily:
  default_args:
    owner: designer
    retries: 2
  schedule: "0 2 * * *"
  description: "订单日汇总（可视化配置）"
  designer:                       # designer 私有段：画布坐标等，渲染时忽略
    nodes: {t1: {x: 100, y: 80}, t2: {x: 100, y: 220}}
  tasks:
    extract_orders:
      operator: airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator
      conn_id: warehouse
      sql: "INSERT INTO dwh.orders ..."
    check_orders:
      operator: ...
      dependencies: [extract_orders]
```

## 6. 分期路线图

**MVP（第 1 期，约 1 周）**
- dag-renderer：schema + 双向转换 + 无环校验 + 单测
- dag-factory 集成 POC：手写的 YAML 能被渲染成 DAG 并跑通
- designer-api：DAG CRUD + validate + node-types（五种节点先出 SQL/校验/通知三种）
- designer-ui：画布 + 属性表单 + 保存/加载/删除/新建
- compose 服务接入，端口 8081

**第 2 期（已完成，2026-10-02）**
- ~~试跑触发 + 运行状态回显画布~~ ✅ 画布内触发（可带 conf）+ 任务状态着色 + 轮询收敛
- ~~删除保护（有正在运行的实例时禁止删除）~~ ✅ 运行中删除返回 409，可确认后强制删除
- ~~trigger_rule 配置面板~~ ✅ 任务级 trigger_rule + retries（schema 驱动表单自动生效）
- ~~版本历史~~ ✅ 文件快照式（保存/回滚自动快照，保留 20 份，可预览/回滚；非 git log 实现）
- SQL 预检（EXPLAIN 预执行校验，防"画对了跑错"）✅
- 未做：增量同步 + Python 函数节点类型（保留至后续）

**第 3 期（按需）**
- 模板库（把现有 6 个 Python DAG 沉淀为可复用模板）
- 对接 FAB 认证（当前为独立三角色 RBAC + 审计 JSONL，已满足多用户管控）
- 任务组（TaskGroup）嵌套、动态任务映射（dag-factory 表达力边界，见 §8）

## 7. 验收标准

**MVP 验收**：
1. 拖拽 3 个节点（SQL → SQL → 通知）连线，填参数，保存
2. 60 秒内 Airflow UI 出现该 DAG，Graph 拓扑与画布一致
3. 触发运行成功，日志、重试、Grafana 指标与手写 DAG 无差异
4. 重开设计器能完整还原画布（含节点位置）；手工改 YAML 后画布仍一致
5. 删除 DAG 后 Airflow 中消失；非法配置（成环/缺参数）保存被拦并标红
6. dag-renderer 单测覆盖双向转换与校验，CI 全绿

**不破坏既有**：9 项 DAG 测试、P0~P3 全部能力回归通过。

## 8. 风险与对策

- **dag-factory 表达力不足**（如动态映射、复杂 trigger_rule）：渲染层已抽象为接口，可切换自研 loader；MVP 第 2 步先做 POC 验证再铺开
- **双向转换失真**（YAML 手改后画布丢信息）：designer 私有段存坐标；转换器单测做往返一致性（round-trip）断言
- **用户把 YAML 改坏导致 DAG 解析失败**：dag-factory 加载器 try/except 隔离单文件；Airflow 的 DAG 解析错误本身不会拖垮调度器，错误在 UI Import Errors 可见
- **并发编辑覆盖**：保存时带 YAML 文件 mtime 做乐观锁，冲突返回 409 提示刷新
