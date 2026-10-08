#!/usr/bin/env bash
# 设计器用户管理脚本（宿主机 Git Bash 运行）
#
# 用法：scripts/designer-add-user.sh <username> <role> [password]
#   - role：viewer / editor / admin
#   - password 缺省时自动生成 12 位随机密码并打印
# 幂等：用户已存在则更新其密码与角色，否则追加到 ./designer/users.json。
set -euo pipefail

# 仓库根目录（本脚本位于 scripts/ 下），用户文件固定为 ./designer/users.json
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USERS_FILE="designer/users.json"

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "用法：$0 <username> <role> [password]" >&2
  exit 1
fi
USERNAME="$1"
ROLE="$2"
PASSWORD="${3:-}"
GENERATED=0

if [[ ! "$USERNAME" =~ ^[A-Za-z0-9_.-]+$ ]]; then
  echo "错误：username 只能包含字母、数字、下划线、点、横线" >&2
  exit 1
fi
if [[ "$ROLE" != "viewer" && "$ROLE" != "editor" && "$ROLE" != "admin" ]]; then
  echo "错误：role 必须是 viewer / editor / admin 之一" >&2
  exit 1
fi

# 密码缺省时用 openssl 生成 12 位随机串（hex）
if [[ -z "$PASSWORD" ]]; then
  PASSWORD="$(openssl rand -hex 6)"
  GENERATED=1
fi

# 优先使用本机 .venv 的 python；找不到再退化到 PATH 里的 python/python3
if [[ -x "$ROOT/.venv/Scripts/python.exe" ]]; then
  PYTHON="$ROOT/.venv/Scripts/python.exe"
elif command -v python >/dev/null 2>&1; then
  PYTHON="python"
else
  PYTHON="python3"
fi

# 在仓库根目录执行，python 直接用相对路径读写 users 文件（避免 Git Bash 路径转换问题）
cd "$ROOT"
"$PYTHON" - "$USERS_FILE" "$USERNAME" "$ROLE" "$PASSWORD" <<'PYEOF'
import hashlib
import json
import os
import sys
from pathlib import Path

path, username, role, password = sys.argv[1:5]

# 与后端一致的哈希格式：<salt_hex>$<pbkdf2_hex>（pbkdf2_hmac sha256，100k 轮）
salt = os.urandom(16).hex()
digest = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 100_000).hex()
stored = f"{salt}${digest}"

p = Path(path)
data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {"users": []}
users = data.setdefault("users", [])
for record in users:
    if record.get("username") == username:  # 已存在：更新密码与角色
        record["password"] = stored
        record["role"] = role
        break
else:  # 不存在：追加
    users.append({"username": username, "password": stored, "role": role})

p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PYEOF

echo "已写入 $USERS_FILE：$USERNAME（$ROLE）"
if [[ "$GENERATED" -eq 1 ]]; then
  echo "生成的密码：$PASSWORD"
fi
