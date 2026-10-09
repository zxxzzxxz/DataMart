#!/usr/bin/env bash
# ==========================================================================
# 生产环境一键部署脚本（目标 Docker 主机上执行）
# 用法: scripts/start-prod.sh [--pull] [--yes]
#
# 与 scripts/deploy.sh 的分工：
#   deploy.sh   面向 CD 流水线（self-hosted runner 调），只做 pull + up，假定
#               镜像已由 release.sh/cd.yml 发布到镜像仓库。
#   本脚本      面向人工首次部署/整栈拉起：前置检查（.env/密钥/证书）→ 构建或
#               拉取镜像 → base+prod 覆盖层全量 up -d → 健康门等待 → 打印入口。
#
# 选项：
#   --pull   从镜像仓库拉取镜像（需 .env 中已设 IMAGE_REGISTRY/IMAGE_TAG，
#            且已 docker login）；缺省为本地 docker compose build。
#   --yes    跳过执行前的人工确认（供自动化调用）。
#
# 启动顺序无需手工编排：compose 依赖图已声明 flyway(service_completed_successfully)
# → airflow-init(迁移+建管理员) → apiserver/scheduler/worker/triggerer/dag-processor，
# 一条 up -d 即可全量拉起（docker-compose.yaml:60、:379）。
# ==========================================================================
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# MSYS_NO_PATHCONV: 防止 Git Bash 把 / 开头的参数误转换为 Windows 路径（Linux 下无害）
export MSYS_NO_PATHCONV=1

COMPOSE_FILES=(-f docker-compose.yaml -f docker-compose.prod.yml)
USE_PULL=0
ASSUME_YES=0
for arg in "$@"; do
  case "$arg" in
    --pull) USE_PULL=1 ;;
    --yes|-y) ASSUME_YES=1 ;;
    -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
    *) echo "错误: 未知参数 '$arg'（支持 --pull / --yes）" >&2; exit 1 ;;
  esac
done

# ---------- 前置检查 1: docker compose ----------
if ! docker compose version >/dev/null 2>&1; then
  echo "错误: 未检测到 docker compose v2，请先安装 Docker（含 compose 插件）" >&2
  exit 1
fi

# ---------- 前置检查 2: .env 与生产强制密钥 ----------
# prod 覆盖层对以下变量使用 :? 强制校验（docker-compose.prod.yml:12-85），
# 这里提前汇总缺失项一次性报错，而不是让 compose 在启动中途逐个抛错。
if [[ ! -f .env ]]; then
  echo "错误: 缺少 .env，请先执行: cp .env.example .env 并填写各项生产密钥" >&2
  exit 1
fi

REQUIRED_VARS=(
  AIRFLOW__CORE__FERNET_KEY
  AIRFLOW__API_AUTH__JWT_SECRET
  _AIRFLOW_WWW_USER_PASSWORD
  AIRFLOW_ADMIN_PASSWORD
  POSTGRES_PASSWORD
  WAREHOUSE_PASSWORD
  POSTGRES_REPLICATION_PASSWORD
  MINIO_ROOT_PASSWORD
  GRAFANA_ADMIN_PASSWORD
  KEYCLOAK_ADMIN_PASSWORD
  DESIGNER_TOKEN
  DESIGNER_AUTH_SECRET
)
MISSING=()
for v in "${REQUIRED_VARS[@]}"; do
  # 环境变量已设置（CI 注入）或 .env 中有非空值，均视为已提供
  if [[ -z "${!v:-}" ]] && ! grep -qE "^${v}=.+$" .env; then
    MISSING+=("$v")
  fi
done
if [[ "${#MISSING[@]}" -gt 0 ]]; then
  echo "错误: 以下生产必需密钥未设置（.env 或环境变量）:" >&2
  printf '  - %s\n' "${MISSING[@]}" >&2
  exit 1
fi

# ---------- 前置检查 3: TLS 证书 ----------
# nginx 容器只读挂载 ./docker/nginx/certs（docker-compose.yaml:631），缺失时容器起不来。
if [[ ! -f docker/nginx/certs/server.crt || ! -f docker/nginx/certs/server.key ]]; then
  echo ">> 未检测到 TLS 证书，先生成开发用自签名证书（生产请尽快替换为真实证书）"
  scripts/gen-certs.sh
fi

# ---------- 执行前确认 ----------
if [[ "$USE_PULL" -eq 1 ]]; then
  IMG_MODE="从镜像仓库拉取（IMAGE_REGISTRY/IMAGE_TAG 取 .env 当前值）"
else
  IMG_MODE="本地构建 6 个自建镜像（airflow/postgres/backup/nginx/designer-api/dag-designer）"
fi
cat <<EOF
即将执行生产环境一键部署：
  compose 文件: docker-compose.yaml + docker-compose.prod.yml
  镜像来源:     ${IMG_MODE}
  服务规模:     33 个服务定义，Airflow 核心组件多副本（prod 下 worker ×4），
                稳态常驻容器约 37 个，请确认机器资源充足。
EOF

if [[ "$ASSUME_YES" -ne 1 ]]; then
  if [[ ! -t 0 ]]; then
    echo "错误: 非交互环境，确认执行请追加 --yes" >&2
    exit 1
  fi
  read -r -p "确认继续？[y/N] " reply
  [[ "$reply" =~ ^[Yy]$ ]] || { echo "已取消"; exit 1; }
fi

# ---------- 1/3 镜像 ----------
if [[ "$USE_PULL" -eq 1 ]]; then
  echo ">> 1/3 拉取镜像"
  docker compose "${COMPOSE_FILES[@]}" pull
else
  echo ">> 1/3 构建镜像"
  docker compose "${COMPOSE_FILES[@]}" build
fi

# ---------- 2/3 全量启动 ----------
echo ">> 2/3 启动全部服务（依赖图自动编排 flyway → airflow-init → Airflow 各组件）"
docker compose "${COMPOSE_FILES[@]}" up -d --remove-orphans

# ---------- 3/3 健康门 ----------
# 与 scripts/deploy.sh 同一判定逻辑：逐个 docker inspect，不依赖宿主机 python；
# ps -a 让 flyway/airflow-init 这类"退出即完成"的一次性容器可见（exited:0 视为正常）。
echo ">> 3/3 等待容器健康（最长 5 分钟；无 healthcheck 的服务以 running 为准，exited 非 0 视为失败）"
deadline=$(( $(date +%s) + 300 ))
while :; do
  unhealthy=""
  for id in $(docker compose "${COMPOSE_FILES[@]}" ps -a -q 2>/dev/null); do
    st="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}:{{.State.ExitCode}}{{end}}' "$id" 2>/dev/null || echo unknown)"
    case "$st" in
      healthy | running | exited:0) ;;
      *) unhealthy="$unhealthy $st" ;;
    esac
  done
  if [[ -z "$unhealthy" ]]; then
    break
  fi
  if (( $(date +%s) >= deadline )); then
    echo "错误: 超时仍不健康的容器: $unhealthy" >&2
    docker compose "${COMPOSE_FILES[@]}" ps -a >&2
    echo "排查: docker compose ${COMPOSE_FILES[*]} logs <服务名>" >&2
    exit 1
  fi
  sleep 5
done

cat <<'EOF'

>> 生产部署完成，统一入口：
     Airflow UI / API:  https://localhost/
     DAG 设计器:        https://localhost:8443/
     Grafana 监控:      https://localhost/grafana/
     告警中心:          https://localhost/alerts/
   回滚版本:            scripts/rollback.sh
EOF
docker compose "${COMPOSE_FILES[@]}" ps
