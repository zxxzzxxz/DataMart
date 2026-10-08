# 代码审查问题修复

本次覆盖设计器存储一致性、运行 API 契约、节点参数校验、应用边界、依赖和持续集成。

## 已修复的缺陷

- DAG 创建、读取、更新、删除、历史读取及回滚使用同一份按 DAG 划分的跨进程文件锁。
  校验版本、快照、写入、清理均在锁内；锁文件独立于 YAML，应用不主动删除锁文件。
- 新编辑器使用 YAML 内容的 SHA-256 revision。更新必须携带 `client_revision` 或兼容的 `client_mtime`；
  原子写入结果直接返回当前请求的版本，编辑器不再用后续 GET 的版本冒充已保存版本。
- 回滚先读取并校验历史目标，再保存当前快照并写回；成功写入后才执行历史保留策略。
  最旧的第 20 份快照可正常回滚，写入失败不会提前删除该目标。
- DAG ID 与 Airflow run ID 分别校验；原生带时区的 run ID 可用，HTTP 路径统一编码。
  测试保留真实 AirflowClient，只替换 HTTP transport，覆盖认证及实际请求编码。
- 节点参数有独立 Pydantic 模型，校验必填、类型、枚举、上下限与未知字段；调度表达式进行 cron 校验。
  HTTP headers 的 JSON 文本转换为字典；通知节点保留高级参数；传感器使用实际的 SqlSensor 类名及 poke/reschedule 模式。

## 当前模块职责

| 模块 | 职责 |
|---|---|
| `src/datamart/workflows/` | 图模型、节点参数、目录和 YAML 转换；不依赖 Airflow |
| `src/datamart/application/sales.py` | 销售流水线编排；保持整批事务边界 |
| `src/datamart/infrastructure/postgres/` | 事务单元、销售 SQL、批次及 outbox 存储 |
| `src/datamart/infrastructure/dag_store.py` | 文件锁、内容版本、原子 YAML 写入和历史策略 |
| `src/datamart/quality.py`、`ports.py` | 可注入查询执行器的质量检查 |
| `src/datamart/adapters/metrics.py` | 监控网络发送，应用事务结束后调用 |
| `designer/backend/app/*_routes.py` | HTTP 转换与路由 |
| `designer/backend/app/services.py` | 编辑、运行、回滚、账号及 SQL 检查用例 |
| `designer/backend/app/main.py` | 配置、资源组装、生命周期及异常映射 |
| `designer/frontend/src/hooks/` | 编辑状态/保存与运行轮询分别管理 |
| `tests/postgres_fixtures.py`、`postgres_assertions.py` | 共用隔离库 fixture 与独立数据 oracle |

共享代码以内部 wheel 安装；已有 `plugins/` 导入保留为兼容入口。API 无需再修改 sys.path。
文件锁通过 [filelock 的跨进程机制](https://py-filelock.readthedocs.io/en/latest/) 实现；
目前保证相同宿主机或具备可靠锁语义的共享文件系统。多主机网络盘需要验证锁语义，不能直接承诺分布式一致性。

## 规范与依赖

- Python lint/format 覆盖 src、DAG、插件、API、配置、测试和全部 Python 脚本；统一 LF。
- TypeScript 启用 strict、未使用项检查；ESLint 检查代码及 hooks 依赖。
- 前端使用 [Vitest](https://vitest.dev/guide/) 验证保存中继续编辑、网络轮询恢复、卸载清理及 mock 版本冲突。
- mock 的七种节点目录及参数 JSON Schema 由后端生成；CI 执行 `export-node-catalog.py --check` 防止漂移。
- dag-factory 在本地/CI/容器统一为 1.1.0；HTTP provider 明确声明为 6.1.0。
- API 的 FastAPI 0.142.2 与 Airflow 自身所需版本分环境安装，CI 对独立 API 镜像依赖另设验收，
  避免只在 Airflow 的环境中测试 API。

## 更新及兼容

1. 开发环境重新执行 `.venv/Scripts/python -m pip install -r requirements.txt`；仅使用 API 时安装内部包及 backend requirements。
2. 重新构建 API/Airflow 镜像。两者的 Docker build context 已调整为仓库根目录，以包含 pyproject 和 src。
   Compose、CD 及镜像扫描的构建路径同步调整；根 dockerignore 排除密钥、日志、备份、venv 和 node_modules。
3. API、前端应一起升级。PUT /api/dags/{id} 和回滚需要版本；新前端发送 `client_revision`，旧客户端仍可发送 `client_mtime`。
4. 历史中 `normal` 传感器模式和旧 SQLSensor 名称可读取，保存时使用可运行的规范表达。
5. 本次不改变数仓表结构，无新增 Flyway 迁移。

## 验收证据

- 完整 Python + 双驱动 PostgreSQL：**199 项通过，0 失败、0 跳过**，证据为 `logs/acceptance/postgres.xml`。
- 独立 API 依赖环境：`logs/acceptance/designer-runtime.xml`，55 项通过。
- 前端：5 项 Vitest 通过；strict typecheck、lint、生产构建通过。
- 打包：wheel 构建成功，隔离导入验证不依赖仓库 sys.path。
- Docker 引擎未运行，本次仅验证四套 Compose 解析和构建定义，没有执行整套镜像启动。

执行：

```bash
python scripts/export-node-catalog.py --check
ruff check src dags plugins designer/backend/app config tests scripts
ruff format --check src dags plugins designer/backend/app config tests scripts
python scripts/run-postgres-acceptance.py --all
```

```bash
cd designer/frontend
npm ci
npm run typecheck
npm run lint
npm test
npm run build
```
