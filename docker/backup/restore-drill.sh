#!/bin/bash
# 全自动恢复演练（restore-drill 服务 entrypoint，postgres:16 基底镜像，root 启动经 gosu 降权）：
#   1) 在 /tmp/drill 初始化临时 PostgreSQL 实例（仅 unix socket，端口 55432）；
#   2) 取 /backups 中最新的 airflow / warehouse pg_dump，分别恢复到 airflow_drill / warehouse_drill；
#   3) 关键表行数校验：airflow_drill.dag_run、warehouse_drill.dm.daily_city_sales；
#   4) 停库清理 /tmp/drill，结果追加到 /backups/restore-drill.log 并推送 Pushgateway；
#   5) 全部通过退出码 0（PASS），任一失败退出码 1（FAIL）。
# 用法：docker compose --profile ops run --rm restore-drill（宿主机可用 scripts/restore-drill.sh）
set -euo pipefail

DRILL_DIR=/tmp/drill
PGPORT=55432
LOG_FILE=/backups/restore-drill.log
PUSHGATEWAY=${PUSHGATEWAY_URL:-http://pushgateway:9091}

RESULT=1          # 默认 FAIL，全部校验通过才置 0
AIRFLOW_ROWS=-    # dag_run 行数（未执行到时保持 -）
WAREHOUSE_ROWS=-  # dm.daily_city_sales 行数

log() { echo "[drill] $*"; }

# 收尾：停止临时实例并清理目录（trap 兜底，任何退出路径都会执行）
cleanup() {
  gosu postgres pg_ctl -D "$DRILL_DIR/data" -m fast stop >/dev/null 2>&1 || true
  rm -rf "$DRILL_DIR"
}
trap cleanup EXIT

# 推送指标：curl 失败仅告警，不影响演练结论
push_metric() {
  if ! echo "$1" | curl -sf --data-binary @- "$PUSHGATEWAY/metrics/job/datamart_restore_drill"; then
    log "警告: 推送指标到 Pushgateway 失败（忽略）"
  fi
}

# 汇总结果：写日志行 + 推指标 + 按 RESULT 退出
finish() {
  local verdict metric line
  if [ "$RESULT" -eq 0 ]; then verdict=PASS; metric=1; else verdict=FAIL; metric=0; fi
  line="$(date -Iseconds) $verdict airflow_dag_run=${AIRFLOW_ROWS} warehouse_daily_city_sales=${WAREHOUSE_ROWS}"
  # /backups 可能以只读方式挂载：写不进去只告警，不掩盖演练结论
  if ( echo "$line" >> "$LOG_FILE" ) 2>/dev/null; then
    log "结果已追加到 $LOG_FILE"
  else
    log "警告: 无法写入 $LOG_FILE（/backups 为只读挂载？），结果行: $line"
  fi
  push_metric "datamart_restore_drill_last_result{job=\"datamart_restore_drill\"} $metric"
  push_metric "datamart_restore_drill_last_run_timestamp_seconds $(date +%s)"
  log "演练结束: $verdict"
  exit "$RESULT"
}

# ---- 1. 初始化并启动临时实例 ----
log "初始化临时 PostgreSQL 实例（$DRILL_DIR/data，仅 unix socket，端口 $PGPORT）..."
mkdir -p "$DRILL_DIR"
chown postgres:postgres "$DRILL_DIR"
if ! gosu postgres initdb -D "$DRILL_DIR/data" -U postgres >/dev/null; then
  log "FAIL: initdb 失败"
  finish
fi
if ! gosu postgres pg_ctl -D "$DRILL_DIR/data" -l "$DRILL_DIR/server.log" \
    -o "-c listen_addresses='' -p $PGPORT -k $DRILL_DIR" -w start; then
  log "FAIL: 临时实例启动失败（详见容器内 $DRILL_DIR/server.log）"
  finish
fi
# 就绪等待（pg_ctl -w 已等待，这里再兜底）
READY=0
for _ in $(seq 1 30); do
  if pg_isready -h "$DRILL_DIR" -p "$PGPORT" -U postgres >/dev/null 2>&1; then READY=1; break; fi
  sleep 1
done
if [ "$READY" -ne 1 ]; then
  log "FAIL: 临时实例 30s 内未就绪"
  finish
fi
log "临时实例已就绪（socket 目录: $DRILL_DIR）"

# ---- 2. 找最新 dump ----
AIRFLOW_DUMP=$(ls -1t /backups/airflow_*.dump 2>/dev/null | head -1 || true)
WAREHOUSE_DUMP=$(ls -1t /backups/warehouse_*.dump 2>/dev/null | head -1 || true)
if [ -z "$AIRFLOW_DUMP" ] || [ -z "$WAREHOUSE_DUMP" ]; then
  [ -n "$AIRFLOW_DUMP" ] || log "FAIL: /backups 中没有 airflow_*.dump，请先执行备份"
  [ -n "$WAREHOUSE_DUMP" ] || log "FAIL: /backups 中没有 warehouse_*.dump，请先执行备份"
  finish
fi
log "airflow 演练备份: $AIRFLOW_DUMP"
log "warehouse 演练备份: $WAREHOUSE_DUMP"

# dump 内含 OWNER TO airflow/warehouse，预建同名角色，避免 pg_restore --exit-on-error 因缺角色中断
psql -h "$DRILL_DIR" -p "$PGPORT" -U postgres -d postgres -q \
  -c "CREATE ROLE airflow LOGIN;" -c "CREATE ROLE warehouse LOGIN;"

# ---- 3. 恢复单个库 + 4. 关键表行数校验 ----
drill_db() {
  local db=$1 dump=$2 table=$3 rows_var=$4 out
  log "恢复 $db <- $dump"
  if ! gosu postgres createdb -h "$DRILL_DIR" -p "$PGPORT" -U postgres "$db"; then
    log "FAIL: createdb $db 失败"
    return 1
  fi
  if ! pg_restore -h "$DRILL_DIR" -p "$PGPORT" -U postgres -d "$db" --exit-on-error "$dump"; then
    log "FAIL: pg_restore $db 失败"
    return 1
  fi
  if out=$(psql -h "$DRILL_DIR" -p "$PGPORT" -U postgres -d "$db" -qAt \
      -c "SELECT count(*) FROM $table" 2>&1); then
    log "$db.$table 行数: $out"
    printf -v "$rows_var" '%s' "$out"
    return 0
  fi
  log "FAIL: $db 中查询 $table 失败（表不存在或恢复内容异常）: $out"
  return 1
}

ALL_OK=0
drill_db airflow_drill "$AIRFLOW_DUMP" dag_run AIRFLOW_ROWS || ALL_OK=1
drill_db warehouse_drill "$WAREHOUSE_DUMP" dm.daily_city_sales WAREHOUSE_ROWS || ALL_OK=1

if [ "$ALL_OK" -eq 0 ]; then
  RESULT=0
fi
finish
