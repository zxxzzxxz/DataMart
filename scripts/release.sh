#!/bin/bash
# 版本发布脚本（宿主机 Git Bash 运行）。
# 用法: scripts/release.sh v1.2.3 [--yes]
#
# 流程：语义校验 → 工作区干净检查 → 确认 → 本地构建全部镜像（打上
# ${IMAGE_REGISTRY}/<名称>:<版本> 标签）→ 推送镜像 → 打 git tag（不推送）。
# 确认无误后请手动推送 tag 触发 CD 流水线（.github/workflows/cd.yml）：
#   git push origin v1.2.3
# 脚本有意不自动推 tag：推送即发布，把这一步留给人确认。
set -euo pipefail
cd "$(dirname "$0")/.."

# MSYS_NO_PATHCONV: 防止 Git Bash 把 / 开头的参数误转换为 Windows 路径（Linux 下无害）
export MSYS_NO_PATHCONV=1

# ---------- 参数解析 ----------
VERSION=""
ASSUME_YES=0
for arg in "$@"; do
  case "$arg" in
    --yes|-y) ASSUME_YES=1 ;;
    -h|--help) sed -n '2,11p' "$0"; exit 0 ;;
    *) VERSION="$arg" ;;
  esac
done

if [[ -z "$VERSION" ]]; then
  echo "用法: scripts/release.sh v1.2.3 [--yes]" >&2
  exit 1
fi

# 语义化版本校验：v主.次.修订，允许 -rc.1 之类的预发布后缀
if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "错误: '$VERSION' 不是合法的语义化版本号（期望形如 v1.2.3）" >&2
  exit 1
fi

# 镜像标签不带 v 前缀（ghcr.io/<owner>/<name>:1.2.3）
IMAGE_TAG="${VERSION#v}"

# ---------- 前置检查 ----------
# 工作区必须干净（未暂存与已暂存的改动都不允许），保证 tag 内容即构建内容
if ! git diff --quiet || ! git diff --cached --quiet; then
  echo "错误: 工作区有未提交的改动，请先提交或 stash：" >&2
  git status --short >&2
  exit 1
fi

# tag 已存在则拒绝，防止覆盖已发布版本
if git rev-parse -q --verify "refs/tags/${VERSION}" >/dev/null 2>&1; then
  echo "错误: tag ${VERSION} 已存在（如需重发请先删除本地/远程 tag）" >&2
  exit 1
fi

# 镜像注册表：默认 ghcr.io/<owner>，owner 从 git remote 解析；可用 IMAGE_REGISTRY 环境变量覆盖
if [[ -z "${IMAGE_REGISTRY:-}" ]]; then
  REMOTE_URL="$(git remote get-url origin)"
  # 兼容 https://github.com/owner/repo.git 与 git@github.com:owner/repo.git 两种形式
  OWNER="$(echo "$REMOTE_URL" | sed -E 's#^(https://[^/]+/|git@[^:]+:)##; s#/[^/]+(\.git)?$##' | tr '[:upper:]' '[:lower:]')"
  if [[ -z "$OWNER" ]]; then
    echo "错误: 无法从 git remote '$REMOTE_URL' 解析 owner，请用 IMAGE_REGISTRY=ghcr.io/<owner> 显式指定" >&2
    exit 1
  fi
  IMAGE_REGISTRY="ghcr.io/${OWNER}"
fi
export IMAGE_REGISTRY IMAGE_TAG

# ---------- 执行前确认 ----------
cat <<EOF
即将执行版本发布：
  版本号:      ${VERSION}（镜像标签 ${IMAGE_TAG}）
  镜像注册表:  ${IMAGE_REGISTRY}
  步骤:
    1. docker compose build     # 构建 6 个自建镜像，标签 ${IMAGE_REGISTRY}/<名称>:${IMAGE_TAG}
    2. docker push 推送上述 6 个镜像（需已 docker login ${IMAGE_REGISTRY%%/*}）
    3. git tag -a ${VERSION}    # 仅本地打 tag，不自动推送
  完成后手动执行: git push origin ${VERSION}   # 触发 CD 构建/Release/部署
EOF

if [[ "$ASSUME_YES" -ne 1 ]]; then
  if [[ ! -t 0 ]]; then
    echo "错误: 非交互环境，确认执行请追加 --yes" >&2
    exit 1
  fi
  read -r -p "确认继续？[y/N] " reply
  [[ "$reply" =~ ^[Yy]$ ]] || { echo "已取消"; exit 1; }
fi

# ---------- 1. 构建 ----------
# compose 的 image: 已参数化为 ${IMAGE_REGISTRY}/<名称>:${IMAGE_TAG}，
# export 上述两个变量后 build 出来的镜像即带发布标签。
echo ">> 1/3 构建镜像（${IMAGE_REGISTRY}/*:${IMAGE_TAG}）"
docker compose build

# ---------- 2. 推送 ----------
# 镜像名约定（与 .github/workflows/cd.yml、docker-compose.staging.yml 一致）：
#   designer/backend → datamart-designer-api     designer/frontend → datamart-dag-designer
#   docker/postgres  → datamart-postgres         docker/backup     → datamart-postgres-backup
#   docker/nginx     → datamart-nginx            docker/airflow    → datamart-airflow
# 注意不用 docker compose push：它会连 redis/prometheus 等第三方镜像一起推，必然失败。
echo ">> 2/3 推送镜像"
for name in datamart-designer-api datamart-dag-designer datamart-postgres datamart-postgres-backup datamart-nginx datamart-airflow; do
  docker push "${IMAGE_REGISTRY}/${name}:${IMAGE_TAG}"
done

# ---------- 3. 打 tag（不推送） ----------
echo ">> 3/3 打 git tag ${VERSION}"
git tag -a "$VERSION" -m "release ${VERSION}"

cat <<EOF
发布准备完成。确认无误后手动推送 tag 触发 CD：
  git push origin ${VERSION}
撤销本地 tag（未推送前）：
  git tag -d ${VERSION}
EOF
