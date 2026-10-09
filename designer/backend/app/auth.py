"""设计器多用户认证 / 角色 RBAC / 操作审计。

- 用户文件：JSON（env DESIGNER_USERS_FILE，默认 /app/users.json），格式：
  {"users": [{"username": ..., "password": "<salt_hex>$<pbkdf2_hex>", "role": ...}]}
  密码哈希：hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt), 100_000)
- 登录令牌（无状态签名）：base64url(json{u,role,exp}) + "." + hmac_sha256_hex(secret, payload)
  secret 取 env DESIGNER_AUTH_SECRET；缺省生成进程级随机值并告警（不回退公开默认值），有效期 12h
- 向后兼容：env DESIGNER_TOKEN 仍有效，等价于内置 admin（用户名 token-admin）
- 审计：变更类请求追加一行 JSON 到 env DESIGNER_AUDIT_LOG（默认 /data/audit/designer-audit.jsonl）；
  目录自动创建，写入失败只告警不阻断请求
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

log = logging.getLogger(__name__)

ROLES = ("viewer", "editor", "admin")
# 角色等级：viewer < editor < admin，高级角色隐含低级角色的全部权限
ROLE_LEVEL = {"viewer": 1, "editor": 2, "admin": 3}

PBKDF2_ITERATIONS = 100_000
TOKEN_TTL_SECONDS = 12 * 3600  # 登录令牌有效期 12h


@dataclass(frozen=True)
class AuthUser:
    username: str
    role: str

    def has_at_least(self, role: str) -> bool:
        return ROLE_LEVEL.get(self.role, 0) >= ROLE_LEVEL[role]


def legacy_token() -> str:
    """内置 admin 的静态 token（env DESIGNER_TOKEN；未配置即停用该通道，无公开默认值）。"""
    return os.environ.get("DESIGNER_TOKEN", "")


_process_secret: str | None = None


def _secret() -> str:
    """会话签名密钥：优先取 DESIGNER_AUTH_SECRET；未配置时生成进程级随机值并告警
    （重启后所有会话失效，但绝不回退到仓库公开值——旧行为可被离线伪造 admin token）。"""
    configured = os.environ.get("DESIGNER_AUTH_SECRET")
    if configured:
        return configured
    global _process_secret
    if _process_secret is None:
        _process_secret = secrets.token_hex(32)
        log.warning(
            "DESIGNER_AUTH_SECRET 未配置：已生成进程级随机会话密钥（重启后会话全部失效），"
            "生产环境请显式配置该变量"
        )
    return _process_secret


# ---------- 用户文件 ----------


def users_file() -> Path:
    return Path(os.environ.get("DESIGNER_USERS_FILE", "/app/users.json"))


def load_users() -> list[dict]:
    """读取用户文件；文件不存在返回空列表，格式错误告警并视为空（不拖垮服务）。"""
    path = users_file()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, json.JSONDecodeError):
        log.warning("用户文件不可读或格式错误：%s", path, exc_info=True)
        return []
    users = data.get("users") if isinstance(data, dict) else None
    return users if isinstance(users, list) else []


def save_users(users: list[dict]) -> None:
    """原子写入用户文件（tmp + fsync + os.replace）：写窗口内的进程终止/磁盘满
    只会留下临时文件，不会把 users.json 截断成半个 JSON 导致全员登录锁定。"""
    path = users_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps({"users": users}, ensure_ascii=False, indent=2) + "\n"
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def hash_password(password: str, salt_hex: str | None = None) -> str:
    """生成 <salt_hex>$<pbkdf2_hex>；salt 缺省时随机生成 16 字节。"""
    if salt_hex is None:
        salt_hex = os.urandom(16).hex()
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), bytes.fromhex(salt_hex), PBKDF2_ITERATIONS)
    return f"{salt_hex}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        salt_hex, expected = stored.split("$", 1)
        salt = bytes.fromhex(salt_hex)
    except ValueError:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, PBKDF2_ITERATIONS).hex()
    return hmac.compare_digest(actual, expected)


# ---------- 登录令牌（无状态签名） ----------


def issue_token(username: str, role: str) -> tuple[str, int]:
    """签发令牌并返回 (token, 过期时间 epoch 秒)。"""
    exp = int(time.time()) + TOKEN_TTL_SECONDS
    payload = (
        base64.urlsafe_b64encode(
            json.dumps({"u": username, "role": role, "exp": exp}, separators=(",", ":")).encode()
        )
        .decode()
        .rstrip("=")
    )
    sig = hmac.new(_secret().encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}", exp


def verify_token(token: str) -> AuthUser | None:
    """校验签名与有效期；任何不合法都返回 None（调用方统一按 401 处理）。"""
    try:
        payload, sig = token.split(".", 1)
    except ValueError:
        return None
    expected = hmac.new(_secret().encode(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig.encode(), expected.encode()):
        return None
    try:
        padded = payload + "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(padded.encode()))
    except (ValueError, json.JSONDecodeError):
        return None
    if not isinstance(data, dict) or not isinstance(data.get("exp"), int | float):
        return None
    if data["exp"] < time.time():
        return None
    username, role = data.get("u"), data.get("role")
    if not isinstance(username, str) or role not in ROLES:
        return None
    return AuthUser(username=username, role=role)


# ---------- 审计 ----------


def audit_log_path() -> Path:
    return Path(os.environ.get("DESIGNER_AUDIT_LOG", "/data/audit/designer-audit.jsonl"))


def write_audit(
    *, user: str, role: str, method: str, path: str, status: int, detail: dict | None = None
) -> None:
    """追加一行审计 JSON；目录自动创建，写不进只告警不阻断请求。"""
    record = {
        "ts": datetime.now(UTC).isoformat(),
        "user": user,
        "role": role,
        "method": method,
        "path": path,
        "status": status,
    }
    if detail:
        record["detail"] = detail
    try:
        target = audit_log_path()
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError:
        log.warning("审计日志写入失败：%s", audit_log_path(), exc_info=True)
