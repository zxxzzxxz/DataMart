#!/bin/bash
# Vault 解封（容器重启后 Vault 进入密封状态，需用初始化时生成的密钥解封）。
# 用法: scripts/vault-unseal.sh
set -euo pipefail
cd "$(dirname "$0")/.."

KEYS_FILE=docker/vault/keys.json
[ -f "$KEYS_FILE" ] || { echo "未找到 $KEYS_FILE，请先执行 scripts/vault-init.sh"; exit 1; }
export MSYS_NO_PATHCONV=1

UNSEAL_KEY=$(.venv/Scripts/python -c "import json;print(json.load(open(r'$KEYS_FILE'))['unseal_keys_b64'][0])" 2>/dev/null \
  || python -c "import json;print(json.load(open(r'$KEYS_FILE'))['unseal_keys_b64'][0])")

docker compose exec -T vault vault operator unseal "$UNSEAL_KEY" >/dev/null
echo ">> Vault 已解封"
