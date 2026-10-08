# 备份 / 恢复演练一体化镜像。
# postgres:16 自带 pg_dump / pg_basebackup / pg_restore / initdb / pg_ctl / gosu，
# 追加 curl 与 ca-certificates（向 Pushgateway 推送备份与演练指标），
# 以及 MinIO 客户端 mc（backup-shipper 服务用它把备份与 WAL 归档同步到对象存储）。
# 注：dl.min.io 已不再提供预编译二进制（MinIO 转源码分发），mc 改从 quay.io 官方镜像拷贝。
FROM quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z AS mc

FROM postgres:16

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=mc /usr/bin/mc /usr/local/bin/mc

# postgres-backup 服务循环执行 backup.sh；restore-drill 服务以 restore-drill.sh 为入口；
# backup-shipper 服务以 ship-to-minio.sh 为入口（minio/mc 官方镜像无 bash，故统一用本镜像运行）。
COPY backup.sh /usr/local/bin/backup.sh
COPY restore-drill.sh /usr/local/bin/restore-drill.sh
COPY ship-to-minio.sh /usr/local/bin/ship-to-minio.sh
RUN chmod +x /usr/local/bin/backup.sh /usr/local/bin/restore-drill.sh /usr/local/bin/ship-to-minio.sh
