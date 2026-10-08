#!/bin/bash
# 数据库备份脚本（在 postgres-backup 容器内执行）：
#   1) 对 Airflow 元数据库和数据仓库各做一次 pg_dump（自定义格式）到 /backups；
#   2) 对每个库做一次 PITR 基准备份（pg_basebackup，tar+gzip）到 /backups/base；
#   3) 每个库全部成功后向 Pushgateway 推送最近成功时间戳（推送失败仅告警，不影响备份）；
#   4) 本地清理超出保留份数（BACKUP_KEEP，默认 7）的旧 dump 与旧基准备份。
# 语义约定：
#   - pg_dump 失败 -> 立即中断，脚本非零退出（保持原有语义）；
#   - pg_basebackup 失败 -> 记录并继续另一个库，最后以非零退出码汇总；
#   - 上传到 MinIO 由 backup-shipper 服务负责，本脚本不碰对象存储。
set -euo pipefail

TS=$(date +%Y%m%d_%H%M%S)
KEEP=${BACKUP_KEEP:-7}
REPL_PASSWORD=${POSTGRES_REPLICATION_PASSWORD:-replicator}
PUSHGATEWAY=${PUSHGATEWAY_URL:-http://pushgateway:9091}

mkdir -p /backups/base

# pg_dump 逻辑备份：失败时 set -e 直接中断脚本
dump() {
  local host=$1 user=$2 password=$3 db=$4
  local file="/backups/${db}_${TS}.dump"
  PGPASSWORD="$password" pg_dump -h "$host" -U "$user" -Fc -f "$file" "$db"
  echo "[backup] $db 逻辑备份完成 -> $file ($(du -h "$file" | cut -f1))"
}

# PITR 基准备份：失败返回 1，由调用方汇总（不中断其他库）
base_backup() {
  local host=$1 db=$2
  local dir="/backups/base/${db}_${TS}"
  # pg_basebackup 要求目标目录不存在或为空，防御性清理同名旧目录
  rm -rf "$dir"
  if pg_basebackup -h "$host" -U replicator \
      -d "host=$host port=5432 user=replicator password=$REPL_PASSWORD" \
      -D "$dir" -Ft -z -P; then
    echo "[backup] $db 基准备份完成 -> $dir ($(du -sh "$dir" | cut -f1))"
    return 0
  fi
  echo "[backup] 警告: $db 基准备份失败（PITR 基线缺失，请检查 replicator 账号与 pg_hba 复制权限）" >&2
  rm -rf "$dir"
  return 1
}

# 推送指标到 Pushgateway：curl 失败仅告警（监控未就绪不影响备份本身）
# db 取值为 airflow/warehouse（纯字母），可直接拼进 grouping key，无需 base64
push_metric() {
  local db=$1
  if ! echo "datamart_backup_last_success_timestamp_seconds{db=\"$db\"} $(date +%s)" \
      | curl -sf --data-binary @- "$PUSHGATEWAY/metrics/job/datamart_backup/db/$db"; then
    echo "[backup] 警告: 推送 $db 备份指标到 Pushgateway 失败（忽略）" >&2
  fi
}

BASE_FAILED=0

# Airflow 元数据库（调度状态、DAG 运行历史、连接/变量等全部在这里）
dump postgres airflow "${AIRFLOW_DB_PASSWORD:-airflow}" airflow
if base_backup postgres airflow; then
  push_metric airflow
else
  BASE_FAILED=1
  echo "[backup] 警告: airflow 基准备份失败，本库不推送成功指标" >&2
fi

# 数据仓库（ods/dwh/dm 三层业务数据）
dump warehouse warehouse "${WAREHOUSE_DB_PASSWORD:-warehouse}" warehouse
if base_backup warehouse warehouse; then
  push_metric warehouse
else
  BASE_FAILED=1
  echo "[backup] 警告: warehouse 基准备份失败，本库不推送成功指标" >&2
fi

# 每个库只保留最近 KEEP 份（dump 与基准备份分别清理；无匹配时 ls 失败不中断）
for db in airflow warehouse; do
  ls -1t /backups/"${db}"_*.dump 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -f || true
  ls -1dt /backups/base/"${db}"_* 2>/dev/null | tail -n +$((KEEP + 1)) | xargs -r rm -rf || true
done

echo "[backup] done at $(date -Iseconds)"

# 任一基准备份失败 -> 非零退出（pg_dump 失败已在上面直接中断）
if [ "$BASE_FAILED" -ne 0 ]; then
  echo "[backup] 存在失败的基准备份，以退出码 1 汇总" >&2
  exit 1
fi
