#!/bin/bash
# 部署脚本（在目标 Docker 主机上执行，通常由 .github/workflows/cd.yml 的
# self-hosted runner 调用，也可手工执行）。
# 用法:
#   IMAGE_REGISTRY=ghcr.io/<owner> IMAGE_TAG=1.2.3 scripts/deploy.sh staging
#   IMAGE_REGISTRY=ghcr.io/<owner>                  scripts/deploy.sh production
#
# 语义：
# - staging：必须提供 IMAGE_TAG（CD 发布 job 传入刚构建的版本），
#   项目名 -p datamart-staging，与开发环境同机隔离并存。
# - production：IMAGE_TAG 可缺省——沿用目标机 ./.env 中现有值
#   （由 scripts/rollback.sh 维护），符合「先定版本、再审批部署」的闸口设计。
# - 两个环境都只更新 .env 中的 IMAGE_REGISTRY/IMAGE_TAG 两行，其余密钥行原样保留。
# - 结束后等待所有带 healthcheck 的容器转为 healthy（最长 5 分钟），超时非零退出。
set -euo pipefail
cd "$(dirname "$0")/.."

# MSYS_NO_PATHCONV: 防止 Git Bash 把 / 开头的参数误转换为 Windows 路径（Linux 下无害）
export MSYS_NO_PATHCONV=1

ENV_NAME="${1:-}"
if [[ -z "$ENV_NAME" ]]; then
  echo "用法: scripts/deploy.sh <staging|production>（IMAGE_REGISTRY/IMAGE_TAG 经环境变量传入）" >&2
  exit 1
fi

if [[ -z "${IMAGE_REGISTRY:-}" ]]; then
  echo "错误: 必须提供 IMAGE_REGISTRY（如 ghcr.io/<owner>）" >&2
  exit 1
fi
# ghcr 要求全小写（runner 上 github.repository_owner 可能含大写）
IMAGE_REGISTRY="$(echo "$IMAGE_REGISTRY" | tr '[:upper:]' '[:lower:]')"
export IMAGE_REGISTRY

case "$ENV_NAME" in
  staging)
    COMPOSE_FILES=(-f docker-compose.yaml -f docker-compose.staging.yml)
    PROJECT=(-p datamart-staging)
    if [[ -z "${IMAGE_TAG:-}" ]]; then
      echo "错误: staging 部署必须显式提供 IMAGE_TAG（发布版本号）" >&2
      exit 1
    fi
    ;;
  production)
    COMPOSE_FILES=(-f docker-compose.yaml -f docker-compose.prod.yml)
    PROJECT=()
    ;;
  *)
    echo "错误: 未知环境 '$ENV_NAME'（期望 staging 或 production）" >&2
    exit 1
    ;;
esac

# 提供了 IMAGE_TAG 才更新 .env（与 scripts/rollback.sh 同一写入约定）；
# production 缺省时沿用 .env 现有 IMAGE_TAG。
if [[ -n "${IMAGE_TAG:-}" ]]; then
  touch .env
  if grep -q '^IMAGE_REGISTRY=' .env; then
    sed -i.bak "s|^IMAGE_REGISTRY=.*|IMAGE_REGISTRY=${IMAGE_REGISTRY}|" .env
    rm -f .env.bak
  else
    echo "IMAGE_REGISTRY=${IMAGE_REGISTRY}" >> .env
  fi
  if grep -q '^IMAGE_TAG=' .env; then
    sed -i.bak "s|^IMAGE_TAG=.*|IMAGE_TAG=${IMAGE_TAG}|" .env
    rm -f .env.bak
  else
    echo "IMAGE_TAG=${IMAGE_TAG}" >> .env
  fi
  echo ">> .env 已设置 IMAGE_REGISTRY=${IMAGE_REGISTRY} IMAGE_TAG=${IMAGE_TAG}"
else
  echo ">> 未提供 IMAGE_TAG，沿用 .env 现有值（production 模式）"
fi

echo ">> 拉取镜像（${ENV_NAME}）"
docker compose "${PROJECT[@]}" "${COMPOSE_FILES[@]}" pull

echo ">> 滚动更新容器"
docker compose "${PROJECT[@]}" "${COMPOSE_FILES[@]}" up -d --remove-orphans

echo ">> 等待容器健康（最长 5 分钟；无 healthcheck 的服务以 running 为准，exited 非 0 视为失败）"
# 用 docker inspect 逐个判定：不依赖宿主机 python（旧实现用 python 解析 JSON 且 || true，
# 目标机无 python 时健康门静默失效）；ps -a 让"退出即失败"的一次性/崩溃容器可见
deadline=$(( $(date +%s) + 300 ))
while :; do
  unhealthy=""
  for id in $(docker compose "${PROJECT[@]}" "${COMPOSE_FILES[@]}" ps -a -q 2>/dev/null); do
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
    docker compose "${PROJECT[@]}" "${COMPOSE_FILES[@]}" ps -a >&2
    echo "排查: docker compose ${PROJECT[*]} ${COMPOSE_FILES[*]} logs <服务名>" >&2
    exit 1
  fi
  sleep 5
done

echo ">> 部署完成（${ENV_NAME}）"
docker compose "${PROJECT[@]}" "${COMPOSE_FILES[@]}" ps
