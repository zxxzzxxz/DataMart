#!/bin/bash
# PITR（时间点恢复）引导式恢复脚本 —— 本脚本未经完整环境实测，使用前请先在测试环境演练！
# 用法: scripts/restore-pitr.sh <airflow|warehouse> "<recovery_target_time，如 2026-09-15 10:30:00>"
#
# 流程：
#   1. 从 ./backups/base/ 找该库最新 pg_basebackup 基准备份（没有则报错，提示先跑备份）；
#   2. 探测 compose 项目创建的 WAL 归档卷（pg-wal-archive-<db>，可用 WAL_ARCHIVE_VOLUME 覆盖）；
#   3. 用一次性容器把 base.tar.gz 解包到临时卷，写入 recovery.signal，
#      并在 postgresql.auto.conf 追加 restore_command / recovery_target_time / recovery_target_action=promote；
#   4. 用 postgres:16 一次性容器在本机 55432 端口启动恢复实例（WAL 卷只读挂载重放）；
#   5. 等待恢复完成后打印核对与接管指引。
# 安全约定：本脚本绝不触碰正式库！恢复实例仅供核对；替换正式数据必须人工确认后【手动】执行。
set -euo pipefail
cd "$(dirname "$0")/.."

DB=${1:-}
TARGET_TIME=${2:-}
if { [ "$DB" != "airflow" ] && [ "$DB" != "warehouse" ]; } || [ -z "$TARGET_TIME" ]; then
  echo "用法: scripts/restore-pitr.sh <airflow|warehouse> \"<recovery_target_time，如 2026-09-15 10:30:00>\"" >&2
  exit 1
fi

# 目标时间格式校验（防止写坏 postgresql.auto.conf）
TIME_RE='^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}(:[0-9]{2})?(\.[0-9]+)?(Z|[+-][0-9]{2}:?[0-9]{2})?$'
if ! [[ "$TARGET_TIME" =~ $TIME_RE ]]; then
  echo "错误: recovery_target_time 格式不合法: $TARGET_TIME（示例: 2026-09-15 10:30:00）" >&2
  exit 1
fi

PORT=${PITR_PORT:-55432}
IMAGE=${POSTGRES_IMAGE:-postgres:16}
CONTAINER=${PITR_CONTAINER_NAME:-datamart-pitr-$DB}
TS=$(date +%Y%m%d_%H%M%S)
TEMP_VOLUME="pitr-restore-${DB}-${TS}"
# Git Bash 下 -v 需要 Windows 风格宿主路径（pwd -W），Linux 下退化为 pwd
HOST_PWD=$(pwd -W 2>/dev/null || pwd)

echo ">> [1/5] 查找 $DB 最新基准备份..."
BASE_DIR=$(ls -1dt backups/base/"${DB}"_* 2>/dev/null | head -1 || true)
if [ -z "$BASE_DIR" ]; then
  echo "错误: backups/base/ 下没有 ${DB}_* 基准备份目录，请先执行备份（scripts/backup.sh 或等待 postgres-backup 服务）" >&2
  exit 1
fi
if [ ! -f "$BASE_DIR/base.tar.gz" ]; then
  echo "错误: $BASE_DIR 中缺少 base.tar.gz（基准备份不完整）" >&2
  exit 1
fi
BASE_NAME=$(basename "$BASE_DIR")
echo "   使用基准备份: $BASE_DIR"

echo ">> [2/5] 探测 WAL 归档卷..."
WAL_VOLUME=${WAL_ARCHIVE_VOLUME:-$(docker volume ls --format '{{.Name}}' | grep "pg-wal-archive-${DB}" | head -1 || true)}
if [ -z "$WAL_VOLUME" ]; then
  echo "错误: 未找到 pg-wal-archive-${DB} 卷（可用 WAL_ARCHIVE_VOLUME=<卷名> 环境变量指定）" >&2
  exit 1
fi
echo "   WAL 归档卷: $WAL_VOLUME"

# 残留检查：不自动删除同名容器，避免误清用户正在核对的实例
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "错误: 容器 $CONTAINER 已存在（可能是上次演练残留）。" >&2
  echo "确认无用后请手动清理: docker stop $CONTAINER && docker rm $CONTAINER" >&2
  exit 1
fi

echo ""
echo "==================== 安全提示 ===================="
echo "即将启动一个【独立的恢复实例】（容器 $CONTAINER，宿主端口 $PORT），"
echo "WAL 归档只读挂载，不会修改任何正式数据。"
echo "恢复目标时间: $TARGET_TIME"
echo "注意: 恢复实例时区为容器默认 UTC，请按需换算目标时间。"
echo "=================================================="
echo ""

echo ">> [3/5] 解包基准备份到临时卷 $TEMP_VOLUME ..."
docker volume create "$TEMP_VOLUME" >/dev/null
# MSYS_NO_PATHCONV: 防止 Git Bash 把 -v 的容器侧路径误转换为 Windows 路径
if ! MSYS_NO_PATHCONV=1 docker run --rm \
    -v "$HOST_PWD/backups:/backups:ro" \
    -v "$TEMP_VOLUME:/data" \
    "$IMAGE" bash -c "
set -euo pipefail
tar -xzf '/backups/base/$BASE_NAME/base.tar.gz' -C /data
if [ -f '/backups/base/$BASE_NAME/pg_wal.tar.gz' ]; then
  mkdir -p /data/pg_wal
  tar -xzf '/backups/base/$BASE_NAME/pg_wal.tar.gz' -C /data/pg_wal
fi
touch /data/recovery.signal
{
  echo \"restore_command = 'cp /wal/%f %p'\"
  echo \"recovery_target_time = '$TARGET_TIME'\"
  echo \"recovery_target_action = 'promote'\"
} >> /data/postgresql.auto.conf
"; then
  echo "错误: 解包基准备份失败" >&2
  docker volume rm "$TEMP_VOLUME" >/dev/null 2>&1 || true
  exit 1
fi

echo ">> [4/5] 启动恢复实例（$IMAGE，宿主端口 $PORT -> 容器 5432）..."
MSYS_NO_PATHCONV=1 docker run -d --name "$CONTAINER" \
  -p "$PORT:5432" \
  -v "$TEMP_VOLUME:/var/lib/postgresql/data" \
  -v "$WAL_VOLUME:/wal:ro" \
  "$IMAGE" >/dev/null

echo ">> [5/5] 等待 WAL 重放到目标时间点（最长 180s）..."
READY=0
for _ in $(seq 1 90); do
  if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "错误: 恢复实例容器已退出，日志如下：" >&2
    docker logs "$CONTAINER" >&2 || true
    exit 1
  fi
  if docker logs "$CONTAINER" 2>&1 | grep -q "database system is ready to accept connections"; then
    READY=1
    break
  fi
  # 备选探测：实例已提升为主库且可连接
  if docker exec "$CONTAINER" pg_isready -U postgres >/dev/null 2>&1; then
    READY=1
    break
  fi
  sleep 2
done
if [ "$READY" -ne 1 ]; then
  echo "错误: 等待恢复超时，请用 docker logs $CONTAINER 排查（WAL 归档是否完整、目标时间是否超出归档范围）" >&2
  exit 1
fi

echo ""
echo "==================== 恢复实例已就绪 ===================="
echo "容器: $CONTAINER   宿主端口: $PORT   临时数据卷: $TEMP_VOLUME"
echo "实时日志: docker logs -f $CONTAINER"
echo ""
echo "【第 1 步：核对数据】（容器内 local 连接免密）"
echo "  docker exec $CONTAINER psql -U $DB -d $DB -c 'SELECT now();'"
if [ "$DB" = "airflow" ]; then
  echo "  docker exec $CONTAINER psql -U $DB -d $DB -c 'SELECT count(*) FROM dag_run;'"
else
  echo "  docker exec $CONTAINER psql -U $DB -d $DB -c 'SELECT count(*) FROM dm.daily_city_sales;'"
fi
echo "  或宿主机连接: psql -h localhost -p $PORT -U $DB -d $DB（密码与正式库同名账号一致）"
echo ""
echo "【第 2 步：导出核对无误的数据】"
echo "  MSYS_NO_PATHCONV=1 docker exec $CONTAINER pg_dump -U $DB -Fc $DB > backups/${DB}_pitr_${TS}.dump"
echo ""
echo "【第 3 步：接管正式库（危险操作！脚本不会自动执行，必须人工确认后手动进行）】"
echo "  ⚠ 以下步骤会覆盖正式库数据，务必先确认第 1/2 步核对无误："
echo "  1) 先给当前正式库做一份兜底备份: scripts/backup.sh"
echo "  2) 灌回正式库: scripts/restore.sh backups/${DB}_pitr_${TS}.dump"
echo "     （该脚本会先停相关组件，再 pg_restore --clean 覆盖）"
echo "  3) 核对正式库无误后，清理恢复实例:"
echo "     docker stop $CONTAINER && docker rm $CONTAINER && docker volume rm $TEMP_VOLUME"
echo "======================================================"
