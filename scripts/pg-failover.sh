#!/bin/bash
# =========================================================
# PostgreSQL 手动故障切换 runbook（流复制备库提升）
#
# 用法（宿主机 Git Bash 中运行）：
#   scripts/pg-failover.sh airflow            # Airflow 元库：postgres -> postgres-replica
#   scripts/pg-failover.sh warehouse          # 业务库：  warehouse -> warehouse-replica
#   scripts/pg-failover.sh <服务对> --force    # 旧主仍在运行时强制提升（二次确认）
#
# 原则：
#   - 本脚本只做“提升备库”这一件事，绝不自动修改 docker-compose.yaml；
#   - 旧主还活着时必须加 --force，防止脑裂双写（两个库同时可写）；
#   - 提升后的连接切换、旧主反向重建，脚本只打印中文指引，由人工执行。
# =========================================================
set -euo pipefail
cd "$(dirname "$0")/.."

# 防止 Git Bash 把容器内绝对路径误转换为 Windows 路径
export MSYS_NO_PATHCONV=1

PAIR="${1:-}"
FORCE="${2:-}"

case "$PAIR" in
    airflow)   PRIMARY="postgres";  REPLICA="postgres-replica";  DBUSER="airflow";   DBNAME="airflow" ;;
    warehouse) PRIMARY="warehouse"; REPLICA="warehouse-replica"; DBUSER="warehouse"; DBNAME="warehouse" ;;
    *)
        echo "用法: $0 <airflow|warehouse> [--force]" >&2
        exit 1
        ;;
esac

echo "== 故障切换目标：${PRIMARY}（旧主） -> ${REPLICA}（提升为新主） =="

# ---------- 0. 前置检查：备库必须在运行 ----------
if ! docker compose ps --status running --services 2>/dev/null | grep -qx "$REPLICA"; then
    echo "!! 备库服务 ${REPLICA} 未运行，无法提升。请先 docker compose up -d ${REPLICA}" >&2
    exit 1
fi

# ---------- 1. 检查旧主状态（防脑裂） ----------
PRIMARY_ALIVE=0
if docker compose ps --status running --services 2>/dev/null | grep -qx "$PRIMARY"; then
    PRIMARY_ALIVE=1
fi

if [ "$PRIMARY_ALIVE" -eq 1 ]; then
    echo "!! 警告：旧主库 ${PRIMARY} 容器仍在运行！"
    echo "!! 直接提升备库会出现“双主脑裂”，两边同时可写、数据分叉。"
    if [ "$FORCE" != "--force" ]; then
        echo "!! 如确认旧主已不可服务（或业务已停止写入），请加 --force 二次确认后再执行：" >&2
        echo "     $0 $PAIR --force" >&2
        exit 1
    fi
    echo ">> 已提供 --force，继续提升。提升完成后请立即执行：docker compose stop ${PRIMARY}"
else
    echo ">> 旧主 ${PRIMARY} 已停止，符合切换条件"
fi

# ---------- 2. 确认备库仍处于 standby（防止重复提升） ----------
IN_RECOVERY="$(docker compose exec -T "$REPLICA" gosu postgres psql -U "$DBUSER" -d "$DBNAME" -tAc 'SELECT pg_is_in_recovery();' | tr -d '[:space:]')"
if [ "$IN_RECOVERY" != "t" ]; then
    echo "!! ${REPLICA} 当前不在 standby 状态（pg_is_in_recovery=${IN_RECOVERY}），可能已提升过，退出。" >&2
    exit 1
fi

# ---------- 3. 提升备库 ----------
echo ">> 正在提升 ${REPLICA} 为新主库 ……"
docker compose exec -T "$REPLICA" gosu postgres pg_ctl promote -D /var/lib/postgresql/data

# ---------- 4. 校验提升结果 ----------
IN_RECOVERY="$(docker compose exec -T "$REPLICA" gosu postgres psql -U "$DBUSER" -d "$DBNAME" -tAc 'SELECT pg_is_in_recovery();' | tr -d '[:space:]')"
if [ "$IN_RECOVERY" != "f" ]; then
    echo "!! 提升校验失败：pg_is_in_recovery=${IN_RECOVERY}，请人工检查：docker compose logs ${REPLICA}" >&2
    exit 1
fi
echo ">> ${REPLICA} 已成功提升为可写主库（沿用同一配置，归档继续写入 /walarchive）"

# ---------- 5. 打印后续人工操作指引（脚本不自动改 compose） ----------
cat <<EOF

======================== 后续步骤（人工执行） ========================

1) 立即停掉旧主，避免脑裂双写：
     docker compose stop ${PRIMARY}

2) 切换应用连接（当前连接串仍指向服务名 “${PRIMARY}”，新主是 “${REPLICA}”）：

   方式 A（快速恢复业务）：
     编辑 docker-compose.yaml，把应用（airflow-* 等）的数据库连接
     host 从 “${PRIMARY}” 改为 “${REPLICA}”，然后：
       docker compose up -d

   方式 B（保持服务名不变，适合长期）：
     编辑 docker-compose.yaml，让 “${REPLICA}” 继承原 “${PRIMARY}” 的
     角色（端口映射、应用依赖改指 “${REPLICA}”，或将 ${REPLICA} 服务
     改名为 “${PRIMARY}”）。本脚本不自动修改 compose，请人工编辑后：
       docker compose up -d

3) 旧主修复后，以“新备库”身份反向重建（replica-entrypoint 会在
   PGDATA 为空时自动从当前新主重新 pg_basebackup）：
     a. 清理旧主容器与数据卷（卷名可用 docker volume ls | grep postgres 确认）：
          docker compose stop ${PRIMARY}
          docker compose rm -f ${PRIMARY}
          docker volume rm <旧主数据卷名>
     b. 编辑 docker-compose.yaml：给 ${PRIMARY} 服务换上 replica 配置
        （entrypoint: /usr/local/bin/replica-entrypoint.sh，
          环境变量 PRIMARY_HOST=${REPLICA}），或与 ${REPLICA} 服务对调角色，然后：
          docker compose up -d ${PRIMARY}
     c. 确认其进入 standby（期望返回 t）：
          docker compose exec -T ${PRIMARY} gosu postgres psql -tAc 'SELECT pg_is_in_recovery();'

4) 如计划回切（流量切回原服务名），待新备同步后重复本流程即可。
======================================================================
EOF
