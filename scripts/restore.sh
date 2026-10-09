#!/bin/bash
# 从备份文件恢复数据库（灾备恢复 / 恢复演练）。
# 用法: scripts/restore.sh backups/airflow_20260908_120000.dump
#       scripts/restore.sh backups/warehouse_20260908_120000.dump
#
# 流程：停止 Airflow 组件（释放数据库连接）-> pg_restore --clean -> 重新启动。
set -euo pipefail
cd "$(dirname "$0")/.."

DUMP_FILE=${1:?用法: scripts/restore.sh <dump文件>}
[ -f "$DUMP_FILE" ] || { echo "文件不存在: $DUMP_FILE"; exit 1; }

# 从文件名识别目标库
case "$DUMP_FILE" in
  *airflow_*)   DB=airflow;   HOST=postgres;  USER=airflow ;;
  *warehouse_*) DB=warehouse; HOST=warehouse; USER=warehouse ;;
  *) echo "无法从文件名识别目标库（应包含 airflow_ 或 warehouse_）: $DUMP_FILE"; exit 1 ;;
esac

if [ "$DB" = "airflow" ]; then
  echo ">> 停止 Airflow 组件（释放元数据库连接）..."
  docker compose stop airflow-apiserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer flower
fi

# 无论恢复成功还是失败，退出前必须把停掉的组件拉回来——否则 set -e 在 pg_restore
# 失败时直接中止，平台会被留在"Airflow 全停"状态。trap 保持脚本原退出码。
restart_components() {
  local rc=$?
  if [ "$DB" = "airflow" ]; then
    echo ">> 重新启动 Airflow 组件（脚本退出码 $rc）..."
    docker compose start airflow-apiserver airflow-scheduler airflow-dag-processor airflow-worker airflow-triggerer flower || true
  fi
}
trap restart_components EXIT

echo ">> 恢复 $DB 库 <- $DUMP_FILE"
docker compose exec -T "$HOST" pg_restore -U "$USER" -d "$DB" --clean --if-exists < "$DUMP_FILE"

echo ">> 恢复完成"
