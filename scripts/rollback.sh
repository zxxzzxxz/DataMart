#!/bin/bash
# 镜像回滚脚本（宿主机 Git Bash 运行）。
# 用法: scripts/rollback.sh <版本号>     # 如 1.2.2，可带或不带 v 前缀
#
# 回滚语义（重要）：
# - 只回滚「镜像版本」：把 IMAGE_TAG=<版本> 写入 ./.env，compose 据此拉取
#   注册表中的旧版本镜像并重建容器。应用代码/DAG 配置随镜像回退。
# - 不包含数据库结构回退：airflow 元数据库与 warehouse 的结构变更一律前滚修复
#   （结构迁移走 Flyway 管理：出错时发布新的前滚迁移版本，而不是把结构改回去）。
# - 数据本身不受影响；如需恢复数据请用 scripts/restore.sh / scripts/restore-pitr.sh。
set -euo pipefail
cd "$(dirname "$0")/.."

# MSYS_NO_PATHCONV: 防止 Git Bash 把 / 开头的参数误转换为 Windows 路径（Linux 下无害）
export MSYS_NO_PATHCONV=1

VERSION="${1:-}"
if [[ -z "$VERSION" ]]; then
  echo "用法: scripts/rollback.sh <版本号>（如 1.2.2，可带或不带 v 前缀）" >&2
  exit 1
fi

# 版本号校验：去掉 v 前缀后必须是 主.次.修订（允许 -rc.1 预发布后缀）
VERSION="${VERSION#v}"
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "错误: '$VERSION' 不是合法版本号（期望形如 1.2.2）" >&2
  exit 1
fi

# 把 IMAGE_TAG 写入 ./.env（compose 默认读取 .env 做变量插值）：
# 已存在该行则就地更新，否则追加到末尾；文件中的其他行原样保留。
if [[ -f .env ]] && grep -q '^IMAGE_TAG=' .env; then
  sed -i.bak "s/^IMAGE_TAG=.*/IMAGE_TAG=${VERSION}/" .env
  rm -f .env.bak
else
  echo "IMAGE_TAG=${VERSION}" >> .env
fi
echo ">> .env 已设置 IMAGE_TAG=${VERSION}"

# 拉取旧版本镜像（需已 docker login 镜像注册表）
echo ">> 拉取镜像"
docker compose pull

# 以旧版本镜像重建容器（配置/密钥仍读 .env 与 compose 文件，仅镜像版本变化）
echo ">> 重建容器"
docker compose up -d

cat <<EOF
回滚完成。验证命令：
  docker compose ps                                  # 容器状态
  docker compose images                              # 实际运行的镜像 tag
  curl -k https://localhost/api/v2/monitor/health    # Airflow 健康检查（经 nginx）
再次提醒：本次仅回滚镜像；若新版本已执行过数据库结构迁移，结构不会自动回退，
请按前滚原则修复（发布新的迁移版本），不要手工把结构改回去。
EOF
