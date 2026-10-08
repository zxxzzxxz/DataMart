#!/bin/bash
# =========================================================
# 备库（replica）容器入口脚本（root 启动，模仿官方镜像做法）
#
# 流程：
#   1. PGDATA 为空 -> 等待主库就绪 -> 清空 PGDATA -> pg_basebackup
#      （-R 自动生成 standby.signal 与 postgresql.auto.conf 中的
#        primary_conninfo，即以流复制 standby 身份启动）；
#   2. PGDATA 已有数据 -> 直接按现有 standby 状态启动；
#   3. 确保 /walarchive 存在且 postgres 可写（promote 后归档继续使用）；
#   4. exec 官方 docker-entrypoint.sh，以 primary.conf 启动。
#
# 该脚本也是“旧主反向重建为新备”的入口：清空数据卷后重新执行
# basebackup 即可加入复制链路。
# =========================================================
set -euo pipefail

PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PRIMARY_HOST="${PRIMARY_HOST:?必须设置 PRIMARY_HOST 环境变量（主库服务名）}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
REPL_USER="${POSTGRES_REPLICATION_USER:-replicator}"
REPL_PASSWORD="${POSTGRES_REPLICATION_PASSWORD:-replicator}"

log() { echo ">> [replica-entrypoint] $*"; }

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    log "PGDATA 为空，准备从主库 ${PRIMARY_HOST} 做基础备份"

    # 1) 等待主库可连接（pg_isready 不做真实认证，仅探测）
    log "等待主库 ${PRIMARY_HOST}:5432 就绪 ……"
    until gosu postgres pg_isready -h "$PRIMARY_HOST" -U "$POSTGRES_USER" >/dev/null 2>&1; do
        sleep 2
    done
    log "主库已就绪"

    # 2) 清空 PGDATA（named volume 首次挂载可能残留文件）
    mkdir -p "$PGDATA"
    find "$PGDATA" -mindepth 1 -delete

    # 3) 拉取基础备份：-X stream 随备份带齐 WAL；-R 自动写入
    #    standby.signal 与 primary_conninfo（含复制账号口令）
    log "开始 pg_basebackup（用户 ${REPL_USER}）……"
    gosu postgres pg_basebackup \
        -h "$PRIMARY_HOST" \
        -D "$PGDATA" \
        -U "$REPL_USER" \
        -d "host=$PRIMARY_HOST port=5432 user=$REPL_USER password=$REPL_PASSWORD" \
        -v -P -R -X stream
    log "基础备份完成，已生成 standby.signal 与 primary_conninfo"
else
    log "检测到已有数据目录（PG_VERSION 存在），跳过基础备份"
fi

# 4) WAL 归档目录：promote 为主库后继续向这里归档
mkdir -p /walarchive
chown postgres:postgres /walarchive

# 5) 交给官方 entrypoint 启动（root 身份进入，内部会 gosu 到 postgres）。
#    主备共用同一份 primary.conf；备库 promote 后归档配置不变、不中断。
log "以 standby 模式启动 PostgreSQL（config_file=/etc/postgresql/postgresql.conf）"
exec docker-entrypoint.sh postgres -c config_file=/etc/postgresql/postgresql.conf
