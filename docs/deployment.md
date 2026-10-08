# 部署与发布

本文档覆盖：环境分层（dev / staging / prod）、版本发布流程、回滚流程、CI 安全门禁。

## 环境分层

基础文件 `docker-compose.yaml` 是开发环境，其余环境用覆盖层叠加（覆盖层只写差异项，
`environment` 按键合并、`ports`/`command` 整体替换）：

```bash
# 开发（默认，宿主机直接起）
docker compose up -d

# staging：独立项目名 + 端口平移，用于验证注册表中的某版本镜像
IMAGE_TAG=1.2.3 docker compose -p datamart-staging -f docker-compose.yaml -f docker-compose.staging.yml pull
IMAGE_TAG=1.2.3 docker compose -p datamart-staging -f docker-compose.yaml -f docker-compose.staging.yml up -d

# 生产：密钥强制传入（缺失即报错），不暴露仓库/MinIO 端口，worker 4 副本
docker compose -f docker-compose.yaml -f docker-compose.prod.yml up -d
```

注意 staging 必须先 `pull` 再 `up -d`：基础文件带 `build:` 段，本地没有对应 tag 的镜像时
compose 会用源码现场构建，那样验证的就不是注册表里的发布镜像了。

### 端口与入口对照表

| 入口 | dev | staging | prod |
| --- | --- | --- | --- |
| Airflow / Grafana / Prometheus（nginx 主入口） | https://localhost/（80→443） | https://localhost:8443/（8080→8443） | https://localhost/ |
| DAG 设计器 | https://localhost:8443 | https://localhost:8444 | https://localhost:8443 |
| warehouse 数据库（宿主机 psql 直连） | 15432 | 25432 | 不暴露 |
| MinIO Console | 127.0.0.1:9001 | 127.0.0.1:29001 | 不暴露 |

staging 与 dev 同机并存时：项目名不同（`datamart-staging` 前缀）保证容器/卷/网络隔离；
但 staging 主入口 8443 与 dev 的设计器端口 8443 冲突，同机只能二选一，或自行再平移
staging 的 `8443:443` 映射。

## CentOS 7 一键部署

`scripts/install-centos7.sh`（root 执行，幂等可重复）在 CentOS 7 裸机上全自动完成：
yum 源切换（EOL 修复）→ 安装 Docker CE 26.1.4 + compose 插件 2.27.1（el7 最后可用版本）→
libseccomp/seccomp 内核兼容修复 → 生成 .env 密钥与自签名证书 → 构建镜像 →
初始化元数据库并拉起全栈（35 容器）→ 健康等待与冒烟自检。

```bash
sudo bash scripts/install-centos7.sh                 # 生产模式（默认，叠加 prod 覆盖层）
sudo DEPLOY_ENV=dev bash scripts/install-centos7.sh  # 开发模式（含调试端口）
```

注意事项：

- CentOS 7 已于 2024-06 EOL，仅建议临时/内网验证使用；生产推荐 Rocky/Alma/Ubuntu LTS
- 3.10 内核 + 旧 libseccomp 会拦截新系统调用，脚本自动探测并修复（升级 libseccomp 2.5.2；
  兜底 daemon 级 `seccomp=unconfined`，此时会打印安全降级警告）
- 默认使用阿里云 vault/docker-ce 镜像源，可用 `YUM_MIRROR` / `DOCKER_CE_REPO` 环境变量改回官方源；
  国内网络另可用 `REGISTRY_MIRRORS=<加速器地址>` 配置 Docker Hub 加速
- 已有发布镜像时可跳过本地构建：`IMAGE_REGISTRY=ghcr.io/<owner> IMAGE_TAG=x.y.z sudo -E bash scripts/install-centos7.sh`
- 部署完成会打印全部入口地址与随机生成的管理员密码（同时落在 ./.env，权限 600）

## 镜像与版本

6 个自建镜像（第三方镜像 redis/prometheus 等不发布，仍由 Docker Hub 直拉）：

| 构建上下文 | 镜像名 |
| --- | --- |
| `designer/backend` | `datamart-designer-api` |
| `designer/frontend` | `datamart-dag-designer` |
| `docker/postgres` | `datamart-postgres`（postgres / 主备 / warehouse 共用） |
| `docker/backup` | `datamart-postgres-backup`（backup / shipper / restore-drill 共用） |
| `docker/nginx` | `datamart-nginx` |
| `docker/airflow` | `datamart-airflow`（apiserver / scheduler / worker 等全部 Airflow 组件共用） |

compose 的 `image:` 参数化约定：`${IMAGE_REGISTRY:+${IMAGE_REGISTRY}/}<名称>:${IMAGE_TAG:-latest}`。
不设环境变量时退化为本地镜像名，开发无感；发布/部署时由脚本注入。

## 供应链与许可证风险

- **MinIO 社区版已停发**：MinIO 自 2025-10 起不再发布社区版二进制/Docker 镜像（Web 管理功能移入商业版 AIStor，此后仅有存量 CVE 修复）。本项目钉在最后社区版 `RELEASE.2025-10-15T17-29-55Z`，客户端 mc 同理停在 `RELEASE.2025-08-13`（已是其最后发布）。当前 MinIO 仅承担备份/WAL 归档的副本存储，替换成本低；如需长期演进，候选：SeaweedFS（S3 兼容、单二进制）、Garage、Ceph，或转商业版 AIStor。切换时同步替换 `docker/backup/Dockerfile` 的 mc 客户端即可（S3 API 兼容）。
- **Redis 许可证分界**：7.2 是 BSD-3-Clause 许可证的最后一代，本项目钉 `redis:7.2.14-bookworm`（7.2 线仍获安全修复）。7.4 起转 RSALv2/SSPLv1，8.x 起增加 AGPLv3 三选一。Redis 在本项目仅作 Celery broker 内部使用，升级 8.x 时选 AGPLv3 通常可接受（不对外提供服务即无网络分发义务），但这属于法务决策——升级前请组织内确认，故暂不升级。

## 发布流程

```
scripts/release.sh v1.2.3          # 本地：校验 → 构建 → 推镜像 → 打 tag（不推送）
        │  git push origin v1.2.3  # 人工确认后推送 tag
        ▼
CD build-push   6 个镜像构建并推 ghcr.io/<owner>/<名称>:<1.2.3> 与 :latest（gha 缓存）
        ▼
CD release      生成 GitHub Release（镜像清单 + 部署命令 + 自动 Release Notes）
        ▼
CD deploy-staging   自动部署 staging（self-hosted runner 执行 scripts/deploy.sh staging）
        ▼  人工验证 staging
Actions → CD → Run workflow → environment=production
        ▼  GitHub Environments「Required reviewers」审批通过
CD deploy-production  scripts/deploy.sh production（版本取目标机 .env 的 IMAGE_TAG）
```

- `scripts/release.sh v1.2.3`：校验语义化版本与工作区干净（含已暂存改动）、解析
  `IMAGE_REGISTRY`（默认 `ghcr.io/<git remote owner>`，可环境变量覆盖）、构建并推送
  6 个镜像、本地打 annotated tag。非交互环境加 `--yes`。脚本不自动推 tag。
- 审批闸配置：仓库 Settings → Environments → 建 `staging` / `production` 两个环境，
  `production` 勾选 Required reviewers 指定审批人；`staging` 不配（自动通过）。
- 部署 job 使用 `runs-on: self-hosted`，需要一台能访问目标 Docker 主机的 runner
  （Settings → Actions → Runners 注册）。没有 runner 时部署 job 排队等待，这是有意设计。

## 回滚流程

```bash
scripts/rollback.sh 1.2.2
```

原理：把 `IMAGE_TAG=1.2.2` 写入 `./.env`（保留其他行），`docker compose pull` 拉旧镜像，
`up -d` 重建容器。脚本结束会打印验证命令（`docker compose ps` / `images` / 健康检查）。

回滚语义边界：

- 只回滚镜像，**不回滚数据库结构**。结构迁移由 Flyway 前滚管理：新版本结构有问题时，
  修复方式是再发布一个前滚迁移版本，而不是把结构改回旧样。
- 数据不受影响；要恢复数据用 `scripts/restore.sh` / `scripts/restore-pitr.sh`。
- 生产部署 job 部署的正是 `.env` 里 `IMAGE_TAG` 指定的版本，因此「回滚后再触发
  deploy-production」等于把生产钉在旧版本上。

## CI 安全门禁

`.github/workflows/ci.yml`：ruff 代码检查 + DAG pytest（本文件不展开）。
`.github/workflows/security.yml`（push/PR 到 main + 每周一 03:00 UTC 定时）：

| Job | 工具 | 管什么 | 门禁 |
| --- | --- | --- | --- |
| `trivy-fs` | trivy fs | 仓库内依赖漏洞、硬编码密钥、Dockerfile/compose 误配置 | CRITICAL 失败；HIGH 只报告并上传 Code Scanning |
| `python-sca` | pip-audit | requirements 中 Python 依赖的已知 CVE（`--strict`） | 有洞即失败 |
| `python-sca` | bandit | 代码危险用法（shell 注入、硬编码密码等），medium 及以上 | 命中即失败 |
| `trivy-image` | trivy image | 6 个自建镜像的 OS/依赖漏洞 | CRITICAL（已有修复版本）失败 |

误报/不可修复的处理纪律（不允许 `|| true` 全局放水）：

- trivy：在仓库根 `.trivyignore` 中逐条写漏洞 ID + 豁免理由；`ignore-unfixed` 已放开
  无修复版本的 CRITICAL。
- bandit：在命中行加 `# nosec Bxxx —— 理由`（精确到规则号）。
- pip-audit：在 workflow 命令中追加 `--ignore-vuln GHSA-xxxx-yyyy-zzzz` 并在注释写明理由。
- SARIF 上传到 Code Scanning 对 public 仓库免费；private 仓库需 GitHub Advanced Security，
  上传步骤已 `continue-on-error` 兜底，失败不影响门禁。

## Dependabot

`.github/dependabot.yml` 每周检查：pip（根与 designer/backend）、npm（designer/frontend）、
docker（docker/postgres、docker/backup、docker/nginx、docker/airflow）、github-actions。每类最多 5 个
开启中的 PR，配合上述安全 CI 把关升级质量。

## 可选安全组件（Vault / Keycloak）

两个组件默认随 `docker compose up -d` 启动但对业务**零侵入**——不启用对应开关时，
Airflow 认证与密钥读取行为与旧版完全一致。

- **Vault（secrets backend）**：`scripts/vault-init.sh` 一次性完成初始化/解封/种子写入
  （密钥与 root token 存 `docker/vault/keys.json`，已 gitignore；生产请改放正式密钥库）。
  然后在 `.env` 设置 `AIRFLOW_SECRETS_BACKEND` 与 `VAULT_TOKEN` 并 `docker compose up -d`，
  连接（`secret/connections/<conn_id>` 的 `conn_uri` 键）与变量
  （`secret/variables/<key>` 的 `value` 键）即优先从 Vault 读取；Vault 不可达时
  Airflow 记日志并回退到环境变量/metastore。容器重启后 Vault 处于密封状态，
  用 `scripts/vault-unseal.sh` 解封。
- **Keycloak（SSO）**：realm 导入配置在 `docker/keycloak/realm-datamart.json`
  （含 airflow-web client、sso-admin/sso-viewer 演示账号与角色映射）。`.env` 设
  `AIRFLOW_OAUTH_ENABLED=true` 后 Airflow 登录页出现 keycloak 登录入口；
  角色映射 `airflow-admin→Admin`、`airflow-viewer→Viewer`，首次登录自动建 FAB 账号。
  端到端步骤与排错见 [sso.md](sso.md)；冒烟验证 `scripts/test-sso.sh`。

## 资源治理

`docker-compose.yaml` 用五档 YAML 锚点（xs/s/m/l/xl）为全部服务声明
`deploy.resources.limits` 硬上限（如 worker 2C/2G、数据库 1C/1G、轻量代理 0.25C/128M）。
limits 是上限不是预留，单机开发不会被挤爆，生产可按机器规格直接改锚点定义统一调整。
Worker 以 `celery worker --autoscale=$CELERY_AUTOSCALE_MAX,$CELERY_AUTOSCALE_MIN`
（默认 8,2）按队列负载弹性伸缩进程数，空闲省内存、高峰提速。
