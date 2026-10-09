#!/usr/bin/env bash
# ==========================================================================
# Demo 级一键启动脚本：仅拉起 5 个必要服务
#   pgsql(postgres) + redis + nginx + 设计器后端(designer-api) + 设计器前端(dag-designer)
# 用法: scripts/start-demo.sh
#
# 适用场景：只演示可视化 DAG 设计器（编辑/保存 DAG 到 dags/designer/），
# 不起 Airflow 调度、数仓、监控告警栈，内存占用约 1G。
#
# 必读——这套裁剪组合的功能边界（均为有意取舍，非故障）：
#   1. nginx 443 主入口的 /、/grafana/ 等路由会 502（后端 Airflow/Grafana 未启动），
#      demo 入口走 8443 端口的 DAG 设计器整站（nginx.conf:185）。
#      由于 nginx 全部使用「变量 proxy_pass + resolver」模式，上游缺失不影响 nginx 自身启动。
#   2. 设计器可以创建/编辑/校验/保存 DAG，但不能触发运行（无 airflow-apiserver）、
#      SQL 预检不可用（无 warehouse）。需要时把对应服务名加进下方 SERVICES 即可。
#   3. postgres/redis 在此组合中暂无消费者，为后续追加 Airflow 预留。
#
# 停止: docker compose stop nginx dag-designer designer-api redis postgres
# 清场: docker compose down（会连 volume 之外的容器网络一起收掉）
# ==========================================================================
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# MSYS_NO_PATHCONV: 防止 Git Bash 把 / 开头的参数误转换为 Windows 路径（Linux 下无害）
export MSYS_NO_PATHCONV=1

# 只用基础 compose 文件：不使用 staging/prod 覆盖层（prod 会强制要求全套生产密钥），
# 也不使用 mini 覆盖层（mini 面向含 Airflow 的演示形态）。
COMPOSE_FILES=(-f docker-compose.yaml)
SERVICES=(postgres redis designer-api dag-designer nginx)
# 其中需要本地构建的镜像（redis 为官方镜像，直接拉取）
BUILD_IMAGES=(postgres designer-api dag-designer nginx)

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  sed -n '2,24p' "$0"
  exit 0
fi

# ---------- 前置检查 ----------
if ! docker compose version >/dev/null 2>&1; then
  echo "错误: 未检测到 docker compose v2，请先安装 Docker（含 compose 插件）" >&2
  exit 1
fi

# nginx 挂载 ./docker/nginx/certs（docker-compose.yaml:631），缺失则先生成自签名证书
if [[ ! -f docker/nginx/certs/server.crt || ! -f docker/nginx/certs/server.key ]]; then
  echo ">> 未检测到 TLS 证书，先生成开发用证书"
  scripts/gen-certs.sh
fi

# 设计器多用户账号文件（docker-compose.yaml:732 挂载）；缺失时回退示例文件，
# 再缺失则由 designer-api 自动降级为 DESIGNER_TOKEN 单 token 模式（默认 designer-dev-token）
if [[ ! -f designer/users.json && -f designer/users.example.json ]]; then
  echo ">> designer/users.json 不存在，从 users.example.json 复制一份"
  cp designer/users.example.json designer/users.json
fi

# ---------- 1/3 构建镜像 ----------
echo ">> 1/3 构建自建镜像（${BUILD_IMAGES[*]}）"
docker compose "${COMPOSE_FILES[@]}" build "${BUILD_IMAGES[@]}"

# ---------- 2/3 按依赖顺序启动 ----------
# 必须 --no-deps：nginx 的 depends_on 声明了 airflow-apiserver/grafana/flower/keycloak
# （docker-compose.yaml:638），不加 --no-deps 会把大半套栈一起拉起来。
echo ">> 2/3 启动服务（--no-deps，按依赖顺序分批）"
docker compose "${COMPOSE_FILES[@]}" up -d --no-deps postgres redis

echo ">> 等待 postgres 就绪（最长 60 秒）"
deadline=$(( $(date +%s) + 60 ))
while :; do
  st="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
        "$(docker compose "${COMPOSE_FILES[@]}" ps -q postgres 2>/dev/null)" 2>/dev/null || echo unknown)"
  [[ "$st" == "healthy" ]] && break
  if (( $(date +%s) >= deadline )); then
    echo "错误: postgres 未在 60 秒内就绪（当前状态: $st），排查: docker compose logs postgres" >&2
    exit 1
  fi
  sleep 3
done

docker compose "${COMPOSE_FILES[@]}" up -d --no-deps designer-api dag-designer
docker compose "${COMPOSE_FILES[@]}" up -d --no-deps nginx

# ---------- 3/3 验证入口 ----------
# 不等 nginx 自身 healthcheck：它探测的是 https://127.0.0.1/（上游 Airflow 未启动必 502），
# demo 下 nginx 会显示 unhealthy 但 8443 设计器站点不受影响，故直接探测真实入口。
echo ">> 3/3 验证 DAG 设计器入口（https://127.0.0.1:8443/，最长 60 秒）"
deadline=$(( $(date +%s) + 60 ))
while :; do
  if curl -ks -o /dev/null --max-time 5 https://127.0.0.1:8443/; then
    break
  fi
  if (( $(date +%s) >= deadline )); then
    echo "错误: 设计器入口 60 秒内无响应，排查: docker compose logs dag-designer designer-api nginx" >&2
    exit 1
  fi
  sleep 3
done

cat <<'EOF'

>> Demo 启动完成：
     DAG 设计器:  https://localhost:8443/  （自签名证书，浏览器警告确认即可）
     登录账号:    见 designer/users.json；无账号文件时用 DESIGNER_TOKEN（默认 designer-dev-token）

   注意：443 主入口（Airflow/Grafana 等）未启动，访问会 502，属预期。
   停止: docker compose stop nginx dag-designer designer-api redis postgres
EOF
docker compose "${COMPOSE_FILES[@]}" ps "${SERVICES[@]}"
