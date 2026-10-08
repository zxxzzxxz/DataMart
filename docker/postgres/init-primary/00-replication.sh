#!/bin/bash
# =========================================================
# 主库初始化脚本：创建流复制用户并放行 replication 连接
#
# 执行时机：官方镜像 initdb 阶段（数据库已临时启动、以 postgres
# 用户运行、通过本地 unix socket 连接）。compose 以文件级挂载注入：
#   ./docker/postgres/init-primary/00-replication.sh:/docker-entrypoint-initdb.d/00-replication.sh:ro
# initdb 机制本身只执行一次，但脚本仍做幂等判断，可重复执行不报错。
# =========================================================
set -euo pipefail

# 复制账号：优先取环境变量，缺省 replicator/replicator
REPL_USER="${POSTGRES_REPLICATION_USER:-replicator}"
REPL_PASSWORD="${POSTGRES_REPLICATION_PASSWORD:-replicator}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"

echo ">> [init-primary] 配置流复制用户：${REPL_USER}"

# 1) 创建复制用户（已存在则跳过）。
#    注意：psql 变量替换在 DO $$ ... $$ 美元引用块内不生效，
#    因此用 SELECT format(...) \gexec 模式：变量在普通 SQL 中替换，
#    format 的 %I/%L 负责安全转义；用户已存在时查询无行、\gexec 不执行，天然幂等。
psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname "$POSTGRES_DB" \
     -v repl_user="$REPL_USER" \
     -v repl_password="$REPL_PASSWORD" <<'EOSQL'
SELECT format('CREATE USER %I WITH REPLICATION LOGIN PASSWORD %L', :'repl_user', :'repl_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'repl_user') \gexec
EOSQL

# 2) 放行 replication 连接（postgres:16 默认密码加密为 scram-sha-256）。
#    已有该用户的 replication 行则不重复追加。
HBA_FILE="$PGDATA/pg_hba.conf"
if grep -qE "^host[[:space:]]+replication[[:space:]]+${REPL_USER}[[:space:]]" "$HBA_FILE"; then
    echo ">> [init-primary] pg_hba.conf 已存在 ${REPL_USER} 的 replication 行，跳过"
else
    echo "host replication ${REPL_USER} 0.0.0.0/0 scram-sha-256" >> "$HBA_FILE"
    echo ">> [init-primary] 已向 pg_hba.conf 追加 replication 放行规则"
fi

echo ">> [init-primary] 流复制初始化完成"
