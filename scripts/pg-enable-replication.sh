#!/bin/bash
# 一次性脚本：在【已有数据卷】的老环境上启用 PG 流复制。
#
# 背景：docker/postgres/init-primary/00-replication.sh 只在空数据卷 initdb 时执行。
# 老环境升级（postgres/warehouse 数据卷已有数据）后，复制用户与 pg_hba 放行规则
# 不会自动存在，postgres-replica / warehouse-replica 会因认证失败无法拉取基线。
# 本脚本在两个主库上幂等补齐这两项配置，执行一次即可。
#
# 用法: scripts/pg-enable-replication.sh
# 之后: docker compose up -d postgres-replica warehouse-replica
set -euo pipefail
cd "$(dirname "$0")/.."

REPL_USER="${POSTGRES_REPLICATION_USER:-replicator}"
REPL_PASSWORD="${POSTGRES_REPLICATION_PASSWORD:-replicator}"

# MSYS_NO_PATHCONV: 防止 Git Bash 把容器内绝对路径误转换为 Windows 路径（Linux 下无害）
export MSYS_NO_PATHCONV=1

enable_on() {
  local svc=$1 db_user=$2 db_name=$3
  echo ">> [${svc}] 创建复制用户 ${REPL_USER}（已存在则跳过）"
  # 注意：psql 变量替换在 DO $$ ... $$ 美元引用块内不生效，
  # 因此用 SELECT format(...) \gexec 模式（用户已存在时查询无行、\gexec 不执行，天然幂等）
  docker compose exec -T "$svc" psql -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 \
    -v repl_user="$REPL_USER" -v repl_password="$REPL_PASSWORD" <<'EOSQL'
SELECT format('CREATE USER %I WITH REPLICATION LOGIN PASSWORD %L', :'repl_user', :'repl_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'repl_user') \gexec
EOSQL

  echo ">> [${svc}] 放行 pg_hba replication 规则（已存在则跳过）"
  docker compose exec -T "$svc" bash -c '
    set -euo pipefail
    HBA="$PGDATA/pg_hba.conf"
    if grep -qE "^host[[:space:]]+replication[[:space:]]+'"$REPL_USER"'[[:space:]]" "$HBA"; then
      echo ">> pg_hba.conf 已存在该规则，跳过"
    else
      echo "host replication '"$REPL_USER"' 0.0.0.0/0 scram-sha-256" >> "$HBA"
      echo ">> 已追加 replication 放行规则"
    fi
  '
  docker compose exec -T "$svc" psql -U "$db_user" -d "$db_name" -c "SELECT pg_reload_conf();" >/dev/null
  echo ">> [${svc}] 已重载配置"
}

enable_on postgres airflow airflow
enable_on warehouse warehouse warehouse

echo ""
echo ">> 完成。现在启动备库："
echo "   docker compose up -d postgres-replica warehouse-replica"
echo ">> 验证复制链路（应看到 1 行 streaming 记录）："
echo "   docker compose exec postgres psql -U airflow -c \"SELECT client_addr, state FROM pg_stat_replication;\""
