#!/bin/bash
# Keycloak SSO 冒烟验证（需在 keycloak 与 airflow 均已启动后执行）。
# 用法: scripts/test-sso.sh
# 退出码：全部 PASS 为 0，任一 FAIL 为 1。
set -euo pipefail
cd "$(dirname "$0")/.."

# 可用环境变量覆盖（默认值与 docker/keycloak/realm-datamart.json、config/webserver_config.py 一致）
KEYCLOAK_URL="${KEYCLOAK_URL:-https://localhost/keycloak}"
AIRFLOW_URL="${AIRFLOW_URL:-https://localhost}"
REALM="${REALM:-datamart}"
CLIENT_ID="${OAUTH_CLIENT_ID:-airflow-web}"
CLIENT_SECRET="${OAUTH_CLIENT_SECRET:-datamart-dev-client-secret}"
SSO_USER="${SSO_USER:-sso-viewer}"
SSO_PASS="${SSO_PASS:-viewer123}"
EXPECT_ROLE="${EXPECT_ROLE:-airflow-viewer}"

PASS=0
FAIL=0
ok()  { echo "PASS  $1"; PASS=$((PASS + 1)); }
bad() { echo "FAIL  $1"; FAIL=$((FAIL + 1)); }

# ---- 1. Keycloak 可达性探测 ----
# 自签名证书用 -k；连接失败时 curl 退出码非 0，|| true 防 set -e 直接中断（此时 http_code 为 000）
# 注意：根路径 302 跳转到 /keycloak/admin/ 控制台是 Keycloak 的正常行为，与 200 一样视为可达
code=$(curl -sk -o /dev/null -w "%{http_code}" "$KEYCLOAK_URL/" || true)
if [ "$code" = "200" ] || [ "$code" = "302" ]; then
    ok "Keycloak 可达（$KEYCLOAK_URL/ -> HTTP $code）"
else
    bad "Keycloak 不可达（$KEYCLOAK_URL/ -> HTTP $code，期望 200/302；keycloak 容器是否在运行？nginx /keycloak/ 是否已接线？）"
fi

# ---- 2. direct access grant 换 token，并校验 payload 中 realm_access.roles 含期望角色 ----
TOKEN_URL="$KEYCLOAK_URL/realms/$REALM/protocol/openid-connect/token"
if ! resp=$(curl -sk -X POST "$TOKEN_URL" \
    -d grant_type=password \
    -d client_id="$CLIENT_ID" \
    -d client_secret="$CLIENT_SECRET" \
    -d username="$SSO_USER" \
    -d password="$SSO_PASS"); then
    bad "token 接口连接失败（$TOKEN_URL）"
else
    # 解析 JSON 与 JWT 用 python（Git Bash 环境一般有 python；依次尝试 python/python3/py）
    PYBIN=""
    for c in python python3 py; do
        if command -v "$c" >/dev/null 2>&1; then PYBIN="$c"; break; fi
    done
    if [ -z "$PYBIN" ]; then
        bad "未找到 python/python3/py，无法解析 token 响应（或改用 jq 手工核对）"
    elif TOKEN_JSON="$resp" EXPECT_ROLE="$EXPECT_ROLE" "$PYBIN" - <<'PYEOF'
import base64, json, os, sys

resp = json.loads(os.environ["TOKEN_JSON"])
if "access_token" not in resp:
    print("token 接口未返回 access_token: %s" % (resp.get("error_description") or resp), file=sys.stderr)
    sys.exit(1)
payload = resp["access_token"].split(".")[1]
payload += "=" * (-len(payload) % 4)  # base64url 补齐 padding
claims = json.loads(base64.urlsafe_b64decode(payload))
roles = claims.get("realm_access", {}).get("roles", [])
print("token payload realm_access.roles = %s" % roles)
if os.environ["EXPECT_ROLE"] not in roles:
    print("期望角色 %s 不在 token roles 中" % os.environ["EXPECT_ROLE"], file=sys.stderr)
    sys.exit(1)
PYEOF
    then
        ok "direct grant 换 token 成功，roles 含 $EXPECT_ROLE（用户 $SSO_USER）"
    else
        bad "direct grant 换 token 或 roles 校验失败（用户 $SSO_USER；检查 client secret / 用户密码 / realm 角色与 roles scope mapper）"
    fi
fi

# ---- 3. Airflow 登录页探测 ----
# 302/200 均可：OAuth 开启后的跳转链路（-> /auth/login -> keycloak）由调用者在浏览器人工核对
code=$(curl -sk -o /dev/null -w "%{http_code}" "$AIRFLOW_URL/login/" || true)
case "$code" in
    200 | 301 | 302 | 303 | 307 | 308)
        ok "Airflow 登录页可达（$AIRFLOW_URL/login/ -> HTTP $code）；OAuth 开启后请人工核对浏览器访问 https://localhost/ 出现 'Sign in with keycloak' 并跳转 Keycloak"
        ;;
    *)
        bad "Airflow 登录页不可达（$AIRFLOW_URL/login/ -> HTTP $code）"
        ;;
esac

# ---- 汇总 ----
echo "----------------------------------------"
echo "汇总: PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
