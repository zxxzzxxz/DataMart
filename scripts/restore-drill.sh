#!/bin/bash
# 手动触发一次全自动恢复演练（ops profile 下的 restore-drill 一次性容器）。
# 用法: scripts/restore-drill.sh
# 退出码与容器内演练结果一致：0=PASS，1=FAIL。
set -euo pipefail
cd "$(dirname "$0")/.."

RC=0
# MSYS_NO_PATHCONV: 防止 Git Bash 把容器内绝对路径误转换为 Windows 路径（Linux 下无害）
MSYS_NO_PATHCONV=1 docker compose --profile ops run --rm restore-drill || RC=$?

echo ">> 最近的演练记录（backups/restore-drill.log）："
tail -n 5 backups/restore-drill.log 2>/dev/null || echo "（暂无演练记录）"

if [ "$RC" -eq 0 ]; then
  echo ">> 演练 PASS"
else
  echo ">> 演练 FAIL（退出码 $RC），请查看上方容器日志" >&2
fi
exit "$RC"
