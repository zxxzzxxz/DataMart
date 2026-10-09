#!/bin/bash
# 备份同步脚本（在 backup-shipper 容器、minio/mc 镜像内循环执行）：
#   /backups              -> minio/datamart-backups        （pg_dump + PITR 基准备份）
#   /walarchive-airflow   -> minio/datamart-wal/airflow    （airflow 库 WAL 归档）
#   /walarchive-warehouse -> minio/datamart-wal/warehouse  （warehouse 库 WAL 归档）
# 保留策略：备份本地按 BACKUP_KEEP 清理（backup.sh）、WAL 本地按 WAL_LOCAL_KEEP_DAYS（默认 7 天）
# 清理（本脚本 find -delete）；远端由本脚本维护 MinIO 过期策略（datamart-wal 默认 30 天，
# WAL_REMOTE_EXPIRY_DAYS 可调；datamart-backups 不设过期，长期保留）。
# MinIO 未就绪或同步失败时等待重试，自身保持健壮不退出（不依赖 restart: always）。
set -euo pipefail

MINIO_ALIAS=minio
MINIO_URL=${MINIO_URL:-http://minio:9000}
INTERVAL=${SHIP_INTERVAL_SECONDS:-60}

# 配置 alias（本地配置写入，不需要 MinIO 在线）
mc alias set "$MINIO_ALIAS" "$MINIO_URL" \
  "${MINIO_ROOT_USER:-minioadmin}" "${MINIO_ROOT_PASSWORD:-minioadmin123}" --api S3v4

# 单轮同步：任一步失败返回非零，由主循环捕获后等待下一轮
sync_once() {
  # MinIO 就绪探测
  if ! mc ready "$MINIO_ALIAS" >/dev/null 2>&1; then
    echo "[ship] $(date -Iseconds) MinIO 未就绪，稍后重试"
    return 1
  fi

  # 确保桶存在
  mc mb --ignore-existing "$MINIO_ALIAS/datamart-backups" || return 1
  mc mb --ignore-existing "$MINIO_ALIAS/datamart-wal" || return 1

  # pg_dump + 基准备份（mc mirror 会打印同步摘要）
  echo "[ship] $(date -Iseconds) 同步 /backups -> $MINIO_ALIAS/datamart-backups"
  mc mirror --overwrite /backups "$MINIO_ALIAS/datamart-backups" || return 1

  # WAL 归档（PITR 重放所需），按库分前缀
  echo "[ship] $(date -Iseconds) 同步 /walarchive-airflow -> $MINIO_ALIAS/datamart-wal/airflow"
  mc mirror --overwrite /walarchive-airflow "$MINIO_ALIAS/datamart-wal/airflow" || return 1
  echo "[ship] $(date -Iseconds) 同步 /walarchive-warehouse -> $MINIO_ALIAS/datamart-wal/warehouse"
  mc mirror --overwrite /walarchive-warehouse "$MINIO_ALIAS/datamart-wal/warehouse" || return 1

  # 本地 WAL 保留：已镜像到 MinIO 的历史段本地只保留 WAL_LOCAL_KEEP_DAYS 天（默认 7 天）。
  # 不清理会按每主库约 22.5GB/天 无限增长，最终打满磁盘使 archive_command 失败、主库停写。
  find /walarchive-airflow /walarchive-warehouse -type f \
    -mtime "+${WAL_LOCAL_KEEP_DAYS:-7}" -delete 2>/dev/null || true

  # 远端 WAL 保留：MinIO 桶过期策略（幂等，默认 30 天）；mc 版本不支持时仅告警，不阻断同步
  mc ilm rule add "$MINIO_ALIAS/datamart-wal" --expiry-days "${WAL_REMOTE_EXPIRY_DAYS:-30}" \
    >/dev/null 2>&1 || true
}

echo "[ship] 启动，目标 $MINIO_URL，同步间隔 ${INTERVAL}s"
while true; do
  if sync_once; then
    echo "[ship] $(date -Iseconds) 本轮同步完成，${INTERVAL}s 后下一轮"
  else
    echo "[ship] $(date -Iseconds) 本轮同步未完成，${INTERVAL}s 后重试" >&2
  fi
  sleep "$INTERVAL"
done
