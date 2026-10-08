# =========================================================
# PostgreSQL 16 高可用镜像（流复制 + WAL 归档 / PITR 基线）
#
# 基础镜像与现网保持一致（postgres:16），已有 named volume 的
# PGDATA 可直接复用。同一镜像同时用于 4 个服务：
#   postgres / postgres-replica / warehouse / warehouse-replica
#
# 说明：
#   - primary.conf 由 compose 以文件级挂载注入（本镜像不 COPY）；
#   - init-primary/00-replication.sh 直接打入 /docker-entrypoint-initdb.d/
#     （initdb 时自动执行，创建复制用户并放行 pg_hba）。
#     之所以不走 compose 挂载：warehouse 还需挂载初始化 SQL 目录，
#     只读目录挂载会让同级文件挂载无法创建挂载点（read-only file system）。
# =========================================================
FROM postgres:16

# WAL 归档目录：主库 archive_command 写入 /walarchive，
# 备库 promote 后沿用同一配置继续归档，归档不中断。
# named volume 首次挂载时继承此处的属主（postgres 可写）。
RUN mkdir -p /walarchive && chown postgres:postgres /walarchive

# 主库 initdb 脚本：创建流复制用户 + pg_hba 放行（脚本内部已幂等）。
# 已有数据卷的老环境 initdb 不会重跑，需执行一次 scripts/pg-enable-replication.sh。
COPY init-primary/00-replication.sh /docker-entrypoint-initdb.d/00-replication.sh
RUN chmod +x /docker-entrypoint-initdb.d/00-replication.sh

# 备库入口脚本：等待主库就绪 -> pg_basebackup -> 以 standby 启动
COPY replica-entrypoint.sh /usr/local/bin/replica-entrypoint.sh
RUN chmod +x /usr/local/bin/replica-entrypoint.sh
