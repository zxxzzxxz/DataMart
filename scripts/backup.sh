#!/bin/bash
# 立即触发一次数据库备份（不等 postgres-backup 服务的每日定时任务）。
# 用法: scripts/backup.sh
set -euo pipefail
cd "$(dirname "$0")/.."

# MSYS_NO_PATHCONV: 防止 Git Bash 把容器内绝对路径误转换为 Windows 路径（Linux 下无害）
MSYS_NO_PATHCONV=1 docker compose exec -T postgres-backup /usr/local/bin/backup.sh

echo ">> 最近的备份文件："
ls -1t backups/*.dump | head -5
