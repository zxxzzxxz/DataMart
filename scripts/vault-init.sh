#!/bin/bash
# Vault 一次性初始化（server 模式 + file 存储）：
#   1. vault operator init（1 份密钥份额）——密钥与 root token 存 docker/vault/keys.json（不进 git）
#   2. 解封（unseal）
#   3. 启用 kv-v2（secret/）并写入平台种子密钥（warehouse 连接、告警 webhook 变量）
# 幂等：已初始化则跳过 init，只做解封与种子写入。
# 用法: scripts/vault-init.sh
set -euo pipefail
cd "$(dirname "$0")/.."

KEYS_FILE=docker/vault/keys.json
export MSYS_NO_PATHCONV=1

v() { docker compose exec -T vault vault "$@"; }
# 带 root token 的调用：必须用 exec -e 注入容器内（宿主机侧变量前缀传不进容器）
vt() { docker compose exec -T -e VAULT_TOKEN="$ROOT_TOKEN" vault vault "$@"; }

if [ ! -f "$KEYS_FILE" ]; then
  echo ">> 首次初始化 Vault（生成解封密钥与 root token）..."
  docker compose exec -T vault vault operator init -key-shares=1 -key-threshold=1 -format=json > "$KEYS_FILE"
  echo ">> 密钥已写入 $KEYS_FILE（妥善保管，切勿提交 git）"
else
  echo ">> 检测到已有 $KEYS_FILE，跳过 operator init"
fi

UNSEAL_KEY=$(.venv/Scripts/python -c "import json;print(json.load(open(r'$KEYS_FILE'))['unseal_keys_b64'][0])" 2>/dev/null \
  || python -c "import json;print(json.load(open(r'$KEYS_FILE'))['unseal_keys_b64'][0])")
ROOT_TOKEN=$(.venv/Scripts/python -c "import json;print(json.load(open(r'$KEYS_FILE'))['root_token'])" 2>/dev/null \
  || python -c "import json;print(json.load(open(r'$KEYS_FILE'))['root_token'])")

echo ">> 解封 Vault..."
v operator unseal "$UNSEAL_KEY" >/dev/null

echo ">> 启用 kv-v2（secret/，已启用则跳过）..."
vt secrets enable -path=secret kv-v2 2>/dev/null || echo "   （secret/ 已存在，跳过）"

echo ">> 写入种子密钥..."
# Airflow VaultBackend 约定：connections 读 secret/connections/<conn_id> 的 conn_uri 键；
# variables 读 secret/variables/<var_key> 的 value 键（见 docker-compose.yaml 中 BACKEND_KWARGS）
# warehouse 口令：优先环境变量，其次 .env（已随机化口令的部署不再写入公开默认值）
WAREHOUSE_PWD="${WAREHOUSE_PASSWORD:-}"
if [[ -z "$WAREHOUSE_PWD" && -f .env ]]; then
  WAREHOUSE_PWD="$(grep '^WAREHOUSE_PASSWORD=' .env | tail -1 | cut -d= -f2-)"
fi
if [[ -z "$WAREHOUSE_PWD" ]]; then
  WAREHOUSE_PWD=warehouse
  echo "!! 警告：未找到 WAREHOUSE_PASSWORD（环境变量或 .env），写入开发默认口令 warehouse——仅适用于未改密的开发环境"
fi
vt kv put secret/connections/warehouse \
  conn_uri="postgresql://warehouse:${WAREHOUSE_PWD}@warehouse:5432/warehouse" >/dev/null
vt kv put secret/variables/datamart_alert_webhook \
  value="${DATAMART_ALERT_WEBHOOK:-http://alert-webhook:9099/alert}" >/dev/null

echo ""
echo ">> 完成。启用 Airflow Vault 密钥后端（在 .env 中加两行后重建 airflow 组件）："
echo "   AIRFLOW_SECRETS_BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend"
echo "   VAULT_TOKEN=$ROOT_TOKEN"
echo "   docker compose up -d"
echo ">> 验证：docker compose exec airflow-scheduler airflow connections get warehouse -o json | grep conn_uri"
