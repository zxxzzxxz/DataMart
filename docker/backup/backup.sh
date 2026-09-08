#!/bin/bash
# 数据库备份脚本（在 postgres-backup 容器内执行）：
# 对 Airflow 元数据库和数据仓库各做一次 pg_dump（自定义格式），
# 输出到 /backups，并清理超出保留份数（BACKUP_KEEP）的旧备份。
set -euo pipefail

TS=$(date +%Y%m%d_%H%M%S)
KEEP=${BACKUP_KEEP:-7}

dump() {
  local host=$1 user=$2 password=$3 db=$4
  local file="/backups/${db}_${TS}.dump"
  PGPASSWORD="$password" pg_dump -h "$host" -U "$user" -Fc -f "$file" "$db"
  echo "[backup] $db -> $file ($(du -h "$file" | cut -f1))"
}

# Airflow 元数据库（调度状态、DAG 运行历史、连接/变量等全部在这里）
dump postgres airflow "${AIRFLOW_DB_PASSWORD:-airflow}" airflow
# 数据仓库（ods/dwh/dm 三层业务数据）
dump warehouse warehouse "${WAREHOUSE_DB_PASSWORD:-warehouse}" warehouse

# 每个库只保留最近 KEEP 份
for db in airflow warehouse; do
  ls -1t /backups/"${db}"_*.dump 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f
done

echo "[backup] done at $(date -Iseconds)"
