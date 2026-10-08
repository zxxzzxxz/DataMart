#!/usr/bin/env bash
# ==========================================================================
# DataMart CentOS 7 一键部署脚本（Docker 容器化全栈）
#
# 用法：
#   sudo bash scripts/install-centos7.sh            # 生产模式（默认，密钥自动生成）
#   sudo DEPLOY_ENV=dev bash scripts/install-centos7.sh   # 开发模式（暴露调试端口）
#
# 覆盖范围（含前后端与全部中间件，共 35 个容器）：
#   Airflow 3(apiserver/scheduler/dag-processor/triggerer/worker/flower)
#   PostgreSQL 16 元库主备 + warehouse 数仓主备 + Flyway 迁移
#   Redis / MinIO / Vault / Keycloak / nginx(TLS)
#   Prometheus / Grafana / Loki / Alloy / Alertmanager / 告警中心
#   可视化 DAG 设计器（FastAPI 后端 + React 前端）
#
# CentOS 7 特有问题本脚本自动处理：
#   1. 官方 yum 源已随 EOL 下线    → 自动切到 vault 镜像源（可用 YUM_MIRROR 覆盖）
#   2. Docker 新版已不为 el7 出包  → 钉装最后支持 el7 的 docker-ce 26.1.4 + compose 插件 2.27.1
#   3. 3.10 内核 + 旧 libseccomp 拦截新系统调用（alpine/debian 新镜像起不来）
#      → 实测探测 → 优先升级 libseccomp 2.5.2 → 兜底 daemon 级 seccomp=unconfined
#   4. openssl 1.0.2 不支持 -addext → 用扩展配置文件方式生成自签名证书（带 SAN）
#   5. SELinux Enforcing 会拦 bind mount → 默认切 permissive（打印警告；--keep-selinux 跳过）
#
# 幂等：可重复执行，已完成的步骤自动跳过（Docker/.env/证书/设计器账号均不重复生成）。
#
# 可选环境变量：
#   DEPLOY_ENV=prod|dev            部署形态（默认 prod）
#   YUM_MIRROR=<url>               centos-vault 镜像源（默认阿里云，可换 https://vault.centos.org）
#   DOCKER_CE_REPO=<url>           docker-ce yum 仓库（默认阿里云镜像，可换官方）
#   REGISTRY_MIRRORS=<url1,url2>   可选：Docker Hub 镜像加速器（国内网络建议配置）
#   IMAGE_REGISTRY + IMAGE_TAG     设置后跳过本地构建，直接从镜像仓库拉取发布镜像
#   SKIP_FIREWALL=1                不改动 firewalld 规则
# ==========================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DEPLOY_ENV="${DEPLOY_ENV:-prod}"
YUM_MIRROR="${YUM_MIRROR:-https://mirrors.aliyun.com/centos-vault}"
DOCKER_CE_REPO="${DOCKER_CE_REPO:-https://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo}"
DOCKER_CE_VERSION="26.1.4-1.el7"          # el7 可得的最后版本（此后 Docker 停止为 el7 出包）
COMPOSE_PLUGIN_VERSION="2.27.1-1.el7"     # 支持 compose 文件中的 !override 标签
LIBSECCOMP_RPM_PATH="8.5.2111/BaseOS/x86_64/os/Packages/libseccomp-2.5.2-2.el8.x86_64.rpm"

log()  { echo -e "\033[1;32m[install]\033[0m $*"; }
warn() { echo -e "\033[1;33m[warn]\033[0m $*" >&2; }
die()  { echo -e "\033[1;31m[error]\033[0m $*" >&2; exit 1; }

# ---------------------------------------------------------------- 前置检查
[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { sed -n '2,40p' "$0"; exit 0; }
[[ "$(id -u)" -eq 0 ]] || die "请用 root 执行（或 sudo bash $0）"
[[ -f docker-compose.yaml && -d docker && -d dags ]] || die "未找到项目文件——本脚本必须在 DataMart 仓库检出内执行"
[[ "$(uname -m)" == "x86_64" ]] || die "仅支持 x86_64 架构"

if [[ -r /etc/centos-release ]]; then
    OS_REL="$(cat /etc/centos-release)"
    log "检测到系统：$OS_REL"
    [[ "$OS_REL" == *"CentOS Linux release 7"* ]] || warn "非 CentOS 7 系统，脚本按 CentOS 7 兼容路径执行（其他 EL7 同源发行版通常也适用）"
else
    warn "未检测到 /etc/centos-release，按 CentOS 7 兼容路径继续"
fi

MEM_MB=$(awk '/MemTotal/ {print int($2/1024)}' /proc/meminfo)
DISK_GB=$(df -BG "$ROOT_DIR" | awk 'NR==2 {gsub("G",""); print $4}')
(( MEM_MB < 6144 ))  && warn "内存 ${MEM_MB}MB < 6GB：全栈 35 容器可能吃紧（prod 模式 worker×4）"
(( DISK_GB < 20 ))   && warn "磁盘剩余 ${DISK_GB}GB < 20GB：镜像+数据卷可能不足"

KEEP_SELINUX=0
[[ "${2:-}" == "--keep-selinux" || "${1:-}" == "--keep-selinux" ]] && KEEP_SELINUX=1
if command -v getenforce >/dev/null 2>&1 && [[ "$(getenforce)" == "Enforcing" ]]; then
    if [[ "$KEEP_SELINUX" -eq 1 ]]; then
        warn "SELinux=Enforcing 且指定 --keep-selinux：bind mount 可能因标签被拒，若容器起不来请执行 chcon -Rt container_file_t $ROOT_DIR"
    else
        warn "SELinux 处于 Enforcing：bind mount 会被拒绝访问，切换为 permissive（重启后仍生效）"
        setenforce 0
        sed -i.bak 's/^SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config
    fi
fi

# ---------------------------------------------------------------- 1. 修复 yum 源（EOL）
if grep -qs 'centos\.org' /etc/yum.repos.d/CentOS-Base.repo; then
    log "CentOS 7 官方源已 EOL 下线，切换到 vault 镜像源：$YUM_MIRROR"
    sed -i.eolbak \
        -e 's|^metalink=|#metalink=|g' \
        -e 's|^mirrorlist=|#mirrorlist=|g' \
        -e "s|^#\s*baseurl=http://mirror.centos.org/centos/\$releasever|baseurl=$YUM_MIRROR/7.9.2009|g" \
        -e "s|^baseurl=http://mirror.centos.org/centos/\$releasever|baseurl=$YUM_MIRROR/7.9.2009|g" \
        /etc/yum.repos.d/CentOS-Base.repo
    yum clean all -q
    yum makecache -q || true   # 个别失效仓库（如陈旧 epel）不阻断，后续 yum install 是真正的检验
fi

log "安装基础工具（yum-utils curl openssl tar）..."
yum install -y -q yum-utils curl openssl tar >/dev/null

# ---------------------------------------------------------------- 2. 安装 Docker CE
compose_ok() { command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; }

if compose_ok; then
    log "Docker 已就绪（$(docker --version | awk '{print $3}' | tr -d ',')，compose $(docker compose version --short)），跳过安装"
else
    log "安装 Docker CE $DOCKER_CE_VERSION + compose 插件 $COMPOSE_PLUGIN_VERSION（el7 最后可用版本）..."
    yum-config-manager --add-repo "$DOCKER_CE_REPO" >/dev/null
    yum install -y -q "docker-ce-$DOCKER_CE_VERSION" "docker-ce-cli-$DOCKER_CE_VERSION" \
        containerd.io "docker-compose-plugin-$COMPOSE_PLUGIN_VERSION" >/dev/null
    systemctl enable --now docker containerd >/dev/null 2>&1 || systemctl enable --now docker
fi

# daemon.json：日志切割 + 可选镜像加速器（已存在则不覆盖，只提示）
if [[ ! -f /etc/docker/daemon.json ]]; then
    MIRRORS=""
    if [[ -n "${REGISTRY_MIRRORS:-}" ]]; then
        MIRRORS=', "registry-mirrors": ["'"$(echo "$REGISTRY_MIRRORS" | sed 's/,/", "/g')"'"]'
    fi
    printf '{\n  "log-driver": "json-file",\n  "log-opts": {"max-size": "50m", "max-file": "3"}%s\n}\n' "$MIRRORS" > /etc/docker/daemon.json
    systemctl restart docker
    log "已写入 /etc/docker/daemon.json（日志 50M×3 切割）"
elif [[ -n "${REGISTRY_MIRRORS:-}" ]]; then
    warn "/etc/docker/daemon.json 已存在，未写入 registry-mirrors，请自行合并后 systemctl restart docker"
fi

# ---------------------------------------------------------------- 3. seccomp 兼容探测与修复
# el7 的 3.10 内核 + libseccomp 2.3.1 会把 faccessat2 等新系统调用拦成 EPERM，
# 导致 alpine>=3.14 / debian bookworm 系镜像起不来（本栈大量镜像受影响）。
seccomp_probe() {
    docker run --rm alpine:3.20 date >/dev/null 2>&1 && \
    docker run --rm debian:bookworm-slim true >/dev/null 2>&1
}

# 先大声拉取探测镜像：拉取失败 = 网络问题（提示配加速器），与 seccomp 故障区分开
docker pull -q alpine:3.20 >/dev/null || die "拉取 alpine:3.20 失败——无法访问 Docker Hub，请用 REGISTRY_MIRRORS=<加速器地址> 重试"
docker pull -q debian:bookworm-slim >/dev/null || die "拉取 debian:bookworm-slim 失败——无法访问 Docker Hub，请用 REGISTRY_MIRRORS=<加速器地址> 重试"

log "探测容器运行时兼容性（alpine:3.20 + debian:bookworm-slim）..."
if seccomp_probe; then
    log "兼容性正常，无需修复"
else
    warn "检测到旧 libseccomp 拦截新系统调用，尝试升级 libseccomp 2.5.2（el8 构建，与 el7 glibc 兼容）..."
    if yum localinstall -y -q "$YUM_MIRROR/$LIBSECCOMP_RPM_PATH" >/dev/null 2>&1; then
        systemctl restart docker
    fi
    if seccomp_probe; then
        log "libseccomp 升级后兼容性正常"
    else
        warn "libseccomp 升级无效，兜底：daemon 级 seccomp=unconfined（安全性降级，仅限内网环境）"
        python - <<'PYEOF' 2>/dev/null || {
import json
p = "/etc/docker/daemon.json"
d = json.load(open(p)) if open(p).read().strip() else {}
d["seccomp-profile"] = "unconfined"
json.dump(d, open(p, "w"), indent=2)
PYEOF
            # python 改写失败（如 daemon.json 非 JSON）时的兜底：整体重写为本脚标准内容
            grep -q 'seccomp-profile' /etc/docker/daemon.json 2>/dev/null || \
                printf '{\n  "log-driver": "json-file",\n  "log-opts": {"max-size": "50m", "max-file": "3"},\n  "seccomp-profile": "unconfined"\n}\n' > /etc/docker/daemon.json
        }
        systemctl restart docker
        seccomp_probe || die "seccomp 修复失败，请手工检查（可尝试升级内核或换用 EL8+ 系统）"
        warn "已以 seccomp=unconfined 运行，请在文档中记录该安全降级"
    fi
fi

docker info >/dev/null 2>&1 || die "Docker 守护进程不可用"
log "Docker 运行时就绪：$(docker --version)"

# ---------------------------------------------------------------- 4. 生成 .env 密钥（幂等）
rand_hex() { openssl rand -hex "$1"; }
rand_fernet() { openssl rand 32 | base64 | tr '+/' '-_'; }

if [[ -f .env ]]; then
    log ".env 已存在，沿用现有密钥（不覆盖）"
    for v in AIRFLOW__CORE__FERNET_KEY AIRFLOW__API_AUTH__JWT_SECRET _AIRFLOW_WWW_USER_PASSWORD POSTGRES_REPLICATION_PASSWORD MINIO_ROOT_PASSWORD; do
        grep -q "^$v=." .env || warn ".env 缺少生产必填项：$v（prod 模式 compose 会拒绝启动）"
    done
else
    log "生成 .env（全部密钥随机生成，权限 600）..."
    ADMIN_PASSWORD="$(rand_hex 16)"
    cat > .env <<EOF
# 由 scripts/install-centos7.sh 生成于 $(date '+%F %T')，请妥善保管
AIRFLOW__CORE__FERNET_KEY=$(rand_fernet)
AIRFLOW__API_AUTH__JWT_SECRET=$(rand_hex 32)
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=$ADMIN_PASSWORD
_AIRFLOW_WWW_USER_PASSWORD=$ADMIN_PASSWORD

POSTGRES_PASSWORD=$(rand_hex 16)
WAREHOUSE_PASSWORD=$(rand_hex 16)
POSTGRES_REPLICATION_USER=replicator
POSTGRES_REPLICATION_PASSWORD=$(rand_hex 16)

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=$(rand_hex 16)
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=$(rand_hex 16)

DESIGNER_AUTH_SECRET=$(rand_hex 32)
EOF
    chmod 600 .env
fi

# ---------------------------------------------------------------- 5. 自签名 TLS 证书（el7 openssl 1.0.2 兼容）
CERT_DIR="docker/nginx/certs"
if [[ -f "$CERT_DIR/server.crt" && -f "$CERT_DIR/server.key" ]]; then
    log "TLS 证书已存在，跳过生成"
else
    log "生成自签名 TLS 证书（含 SAN：localhost/127.0.0.1/本机 IP；生产请后续替换真实证书）..."
    mkdir -p "$CERT_DIR"
    HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
    SAN="DNS:localhost,DNS:*.localhost,IP:127.0.0.1"
    [[ -n "$HOST_IP" ]] && SAN="$SAN,IP:$HOST_IP"
    # openssl 1.0.2 没有 -addext，用扩展配置文件方式（全版本兼容）
    cat > "$CERT_DIR/.san.cnf" <<EOF
[req]
distinguished_name = dn
x509_extensions = v3_req
prompt = no
[dn]
CN = localhost
[v3_req]
subjectAltName = $SAN
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
EOF
    ( cd "$CERT_DIR" && openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout server.key -out server.crt -days 825 -config .san.cnf 2>/dev/null )
    rm -f "$CERT_DIR/.san.cnf"
    chmod 600 "$CERT_DIR/server.key"
fi

# ---------------------------------------------------------------- 6. 防火墙放行
if [[ "${SKIP_FIREWALL:-0}" != "1" ]] && systemctl is-active firewalld >/dev/null 2>&1; then
    log "firewalld 放行 80/443/8443..."
    firewall-cmd --permanent --add-port=80/tcp --add-port=443/tcp --add-port=8443/tcp >/dev/null
    firewall-cmd --reload >/dev/null
fi

# ---------------------------------------------------------------- 7. 构建或拉取镜像
if [[ "$DEPLOY_ENV" == "prod" ]]; then
    COMPOSE_FILES=(-f docker-compose.yaml -f docker-compose.prod.yml)
else
    COMPOSE_FILES=(-f docker-compose.yaml)
    warn "DEPLOY_ENV=dev：开发默认值密钥将生效（admin123 等），请勿用于生产"
fi

if [[ -n "${IMAGE_REGISTRY:-}" ]]; then
    log "从镜像仓库拉取：$IMAGE_REGISTRY（tag: ${IMAGE_TAG:-latest}）..."
    docker compose "${COMPOSE_FILES[@]}" pull
else
    log "本地构建 6 个自建镜像（airflow/postgres/backup/nginx/designer-api/designer 前端，约 10-20 分钟）..."
    docker compose "${COMPOSE_FILES[@]}" build
fi

# ---------------------------------------------------------------- 8. 设计器管理员账号（先于 up：users.json 是文件级挂载，缺失会被 docker 建成目录）
if [[ ! -s designer/users.json ]]; then
    DESIGNER_ADMIN_PASSWORD="$(rand_hex 12)"
    log "创建设计器管理员账号 admin（密码见末尾汇总）..."
    # 用 designer-api 镜像内的 python3.12 生成与后端一致的 pbkdf2 哈希（el7 自带 python2 无法胜任）；
    # 注意必须 -i 才能把 heredoc 喂给容器 stdin；文件由容器内 python 直接写入挂载目录
    docker run --rm -i -v "$ROOT_DIR/designer:/data" \
        "${IMAGE_REGISTRY:+$IMAGE_REGISTRY/}datamart-designer-api:${IMAGE_TAG:-latest}" \
        python - /data/users.json admin "$DESIGNER_ADMIN_PASSWORD" <<'PYEOF'
import hashlib, json, os, sys
path, username, password = sys.argv[1:4]
salt = os.urandom(16).hex()
digest = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 100_000).hex()
with open(path, "w", encoding="utf-8") as f:
    f.write(json.dumps({"users": [{"username": username, "password": salt + "$" + digest, "role": "admin"}]}, indent=2) + "\n")
PYEOF
else
    log "designer/users.json 已存在，跳过账号创建"
fi

# ---------------------------------------------------------------- 9. 初始化元数据库并启动全栈
log "初始化 Airflow 元数据库并创建管理员（airflow-init）..."
docker compose "${COMPOSE_FILES[@]}" up -d airflow-init >/dev/null
deadline=$(( $(date +%s) + 600 ))
while :; do
    st="$(docker inspect -f '{{.State.Status}}:{{.State.ExitCode}}' \
        "$(docker compose "${COMPOSE_FILES[@]}" ps -q airflow-init)" 2>/dev/null || echo unknown)"
    [[ "$st" == "exited:0" ]] && break
    [[ "$st" == exited:* ]] && { docker compose "${COMPOSE_FILES[@]}" logs airflow-init; die "airflow-init 失败（$st）"; }
    (( $(date +%s) >= deadline )) && die "airflow-init 超时（10 分钟）"
    sleep 5
done

log "启动全栈（35 容器）..."
docker compose "${COMPOSE_FILES[@]}" up -d

log "等待全部容器转为健康（最长 10 分钟）..."
deadline=$(( $(date +%s) + 600 ))
while :; do
    pending=""
    for id in $(docker compose "${COMPOSE_FILES[@]}" ps -q); do
        st="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}:{{.State.ExitCode}}{{end}}' "$id" 2>/dev/null || echo unknown)"
        case "$st" in
            healthy|running|exited:0) ;;
            *) pending="$pending $st" ;;
        esac
    done
    [[ -z "$pending" ]] && break
    (( $(date +%s) >= deadline )) && { docker compose "${COMPOSE_FILES[@]}" ps; die "超时仍有容器未就绪"; }
    sleep 5
done

# ---------------------------------------------------------------- 10. 冒烟自检
log "冒烟检查 https://127.0.0.1/ ..."
curl -kfsS --max-time 15 https://127.0.0.1/ >/dev/null || die "nginx 入口不可达"
curl -kfsS --max-time 15 https://127.0.0.1/api/v2/monitor/health >/dev/null || die "Airflow API 健康检查失败"

# ---------------------------------------------------------------- 汇总
HOST_IP="$(hostname -I 2>/dev/null | awk '{print $1}')"
HOST_SHOW="${HOST_IP:-127.0.0.1}"
cat <<EOF

======================================================================
  DataMart 部署完成（$DEPLOY_ENV 模式）
======================================================================
  Airflow UI     https://$HOST_SHOW/          admin / $(grep '^AIRFLOW_ADMIN_PASSWORD=' .env | cut -d= -f2)
  DAG 设计器     https://$HOST_SHOW:8443/      admin / ${DESIGNER_ADMIN_PASSWORD:-（沿用既有 users.json）}
  Grafana        https://$HOST_SHOW/grafana/   admin / $(grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2)
  Prometheus     https://$HOST_SHOW/prometheus/
  Alertmanager   https://$HOST_SHOW/alertmanager/
  告警中心       https://$HOST_SHOW/alerts/
  Keycloak       https://$HOST_SHOW/keycloak/  admin / admin（SSO 默认未启用）
----------------------------------------------------------------------
  密钥文件：./.env（权限 600，勿提交 git）    证书：docker/nginx/certs/（自签名，浏览器需确认一次）
  常用命令：
    docker compose ${COMPOSE_FILES[*]} ps            # 查看状态
    docker compose ${COMPOSE_FILES[*]} logs -f <服务> # 查看日志
    scripts/backup.sh                                 # 立即备份
    docker compose --profile drill run --rm restore-drill   # 恢复演练
  可选增强：
    scripts/vault-init.sh        # 启用 Vault 密钥后端
    .env 设 AIRFLOW_OAUTH_ENABLED=true 后重启   # 启用 Keycloak SSO（docs/sso.md）
  运维手册：docs/ha-dr.md（HA/灾备）  docs/deployment.md（发布/回滚）
======================================================================
EOF
