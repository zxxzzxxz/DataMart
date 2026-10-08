#!/usr/bin/env bash
# ==========================================================================
# 生成 nginx 统一入口的开发用 TLS 证书
#   输出：docker/nginx/certs/server.crt / server.key
# 用法：scripts/gen-certs.sh [--force]
#   --force  覆盖已存在的证书重新生成
# 说明：certs 目录由 compose 挂载进 nginx 容器（/etc/nginx/certs:ro），不进 git。
# ==========================================================================
set -euo pipefail

# 定位项目根目录（本脚本位于 <root>/scripts/ 下，与当前工作目录无关）
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CERT_DIR="$ROOT_DIR/docker/nginx/certs"
CRT="$CERT_DIR/server.crt"
KEY="$CERT_DIR/server.key"

FORCE=0
# 注意：这里必须用 if 语句。写成 `[[ ... ]] && FORCE=1` 时，
# 条件不成立整条命令返回非 0，会被 set -e 直接终止脚本。
if [[ "${1:-}" == "--force" ]]; then
    FORCE=1
fi

mkdir -p "$CERT_DIR"

if [[ -f "$CRT" && "$FORCE" -eq 0 ]]; then
    echo "证书已存在：$CRT"
    echo "如需重新生成，请执行：scripts/gen-certs.sh --force"
    exit 0
fi

if command -v mkcert >/dev/null 2>&1; then
    # mkcert：本地受信 CA，浏览器无警告（会写入系统信任库）
    echo "检测到 mkcert，生成本地受信证书（localhost / *.localhost / 127.0.0.1 / ::1）..."
    mkcert -install
    mkcert -cert-file "$CRT" -key-file "$KEY" localhost "*.localhost" 127.0.0.1 ::1
    SELF_SIGNED=0
else
    # openssl 自签名兜底：CN=localhost，SAN 覆盖 *.localhost 与 127.0.0.1，825 天
    echo "未检测到 mkcert，改用 openssl 生成自签名证书（CN=localhost，有效期 825 天）..."
    # 两个 MSYS 路径坑要同时避开：
    # 1) Git Bash(MSYS) 会把以 / 开头的参数（如 /CN=localhost）自动转换成 Windows 路径，
    #    必须用 MSYS_NO_PATHCONV=1 关闭该行为，否则 -subj 内容会被破坏；
    # 2) 但 MSYS_NO_PATHCONV=1 会同时关闭所有路径转换，openssl（原生 Windows 程序）
    #    将无法识别 Git Bash 风格的 POSIX 输出路径，因此先 cd 进证书目录、
    #    对 -keyout/-out 一律使用相对文件名（子 shell 执行，不影响脚本其余部分的 cwd）。
    (
        cd "$CERT_DIR"
        MSYS_NO_PATHCONV=1 openssl req -x509 -newkey rsa:2048 -nodes \
            -keyout server.key -out server.crt \
            -days 825 \
            -subj "/CN=localhost" \
            -addext "subjectAltName=DNS:localhost,DNS:*.localhost,IP:127.0.0.1"
    )
    SELF_SIGNED=1
fi

# 私钥收紧权限（Windows/Git Bash 下 chmod 可能无效，忽略错误）
chmod 600 "$KEY" 2>/dev/null || true

echo
echo "证书生成完成："
echo "  证书：$CRT"
echo "  私钥：$KEY"
echo
echo "使用提示："
if [[ "$SELF_SIGNED" -eq 1 ]]; then
    echo "  1. 当前为自签名证书，浏览器首次访问 https://localhost/ 与 https://localhost:8443/ 会出现证书警告，手动确认继续即可（安装 mkcert 后重新生成可消除警告）。"
else
    echo "  1. mkcert 证书已被本机信任，访问 https://localhost/ 与 https://localhost:8443/ 无警告。"
fi
echo "  2. 生产环境请用真实证书替换 docker/nginx/certs/server.crt 与 server.key，然后重启 nginx 容器即可。"
echo "  3. docker/nginx/certs/ 目录存放私钥，切勿提交 git。"
