"""设计器后端认证 / RBAC / 审计测试。

用 fastapi.testclient.TestClient 直跑 designer/backend 的 ASGI app；
env（DESIGNER_USERS_FILE / DESIGNER_AUDIT_LOG / DESIGNER_TOKEN / DAGS_DIR / PLUGINS_DIR）
经 monkeypatch 指向 tmp_path，后端在请求期读取这些 env，无需 reload 模块。
"""

import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_BACKEND_DIR = Path(__file__).resolve().parent.parent / "designer" / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from app import auth, main  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

LEGACY_TOKEN = "test-legacy-token"
# 密码哈希只算一次（pbkdf2 100k 轮有刻意开销），fixture 写文件时复用
_USERS = [
    {"username": "admin1", "password": auth.hash_password("admin-pass"), "role": "admin"},
    {"username": "editor1", "password": auth.hash_password("editor-pass"), "role": "editor"},
    {"username": "viewer1", "password": auth.hash_password("viewer-pass"), "role": "viewer"},
]


def _dag_body(dag_id: str = "t_dag") -> dict:
    return {
        "dag_id": dag_id,
        "description": "测试 DAG",
        "schedule": "0 1 * * *",
        "nodes": [{"id": "a", "type": "sql", "params": {"sql": "SELECT 1"}, "position": {"x": 0, "y": 0}}],
        "edges": [],
    }


def _audit_entries() -> list[dict]:
    path = Path(os.environ["DESIGNER_AUDIT_LOG"])
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


@pytest.fixture()
def client(tmp_path, monkeypatch):
    users_file = tmp_path / "users.json"
    users_file.write_text(json.dumps({"users": _USERS}), encoding="utf-8")
    monkeypatch.setenv("DESIGNER_USERS_FILE", str(users_file))
    monkeypatch.setenv("DESIGNER_AUDIT_LOG", str(tmp_path / "audit" / "designer-audit.jsonl"))
    monkeypatch.setenv("DESIGNER_TOKEN", LEGACY_TOKEN)
    monkeypatch.delenv("DESIGNER_AUTH_SECRET", raising=False)
    monkeypatch.setenv("DAGS_DIR", str(tmp_path / "dags"))
    monkeypatch.setenv("PLUGINS_DIR", str(tmp_path / "plugins"))
    return TestClient(main.create_app(airflow=SimpleNamespace(count_active_runs=lambda _id: 0)))


def _login(client: TestClient, username: str, password: str) -> str:
    resp = client.post("/api/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()["token"]


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------- 认证 ----------


def test_health_is_public(client):
    assert client.get("/api/health").status_code == 200


def test_no_token_401(client):
    resp = client.get("/api/dags")
    assert resp.status_code == 401
    assert "Authorization" in resp.json()["detail"]


def test_tampered_token_401(client):
    token = _login(client, "viewer1", "viewer-pass")
    bad = token[:-1] + ("0" if token[-1] != "0" else "1")
    assert client.get("/api/dags", headers=_bearer(bad)).status_code == 401


def test_legacy_token_full_access(client):
    headers = _bearer(LEGACY_TOKEN)
    assert client.get("/api/dags", headers=headers).status_code == 200
    assert client.get("/api/node-types", headers=headers).status_code == 200
    assert client.post("/api/dags", json=_dag_body(), headers=headers).status_code == 201
    assert (
        client.put(
            "/api/dags/t_dag",
            json={
                **_dag_body(),
                "client_revision": client.get("/api/dags/t_dag", headers=headers).json()["revision"],
            },
            headers=headers,
        ).status_code
        == 200
    )
    assert client.post("/api/validate", json=_dag_body("other"), headers=headers).status_code == 200
    assert client.get("/api/auth/users", headers=headers).status_code == 200
    assert client.delete("/api/dags/t_dag", headers=headers).status_code == 200


# ---------- RBAC ----------


def test_viewer_get_ok_post_forbidden(client):
    headers = _bearer(_login(client, "viewer1", "viewer-pass"))
    assert client.get("/api/dags", headers=headers).status_code == 200
    assert client.get("/api/node-types", headers=headers).status_code == 200
    resp = client.post("/api/dags", json=_dag_body(), headers=headers)
    assert resp.status_code == 403
    assert client.put("/api/dags/t_dag", json=_dag_body(), headers=headers).status_code == 403
    assert client.post("/api/validate", json=_dag_body(), headers=headers).status_code == 403
    assert client.delete("/api/dags/t_dag", headers=headers).status_code == 403
    assert client.get("/api/auth/users", headers=headers).status_code == 403


def test_editor_write_ok_delete_forbidden(client):
    headers = _bearer(_login(client, "editor1", "editor-pass"))
    assert client.post("/api/dags", json=_dag_body(), headers=headers).status_code == 201
    assert (
        client.put(
            "/api/dags/t_dag",
            json={
                **_dag_body(),
                "client_revision": client.get("/api/dags/t_dag", headers=headers).json()["revision"],
            },
            headers=headers,
        ).status_code
        == 200
    )
    resp = client.post("/api/validate", json=_dag_body(), headers=headers)
    assert resp.status_code == 200 and resp.json()["ok"] is True
    assert client.delete("/api/dags/t_dag", headers=headers).status_code == 403
    assert client.get("/api/auth/users", headers=headers).status_code == 403


def test_admin_delete_ok(client):
    headers = _bearer(_login(client, "admin1", "admin-pass"))
    assert client.post("/api/dags", json=_dag_body(), headers=headers).status_code == 201
    assert client.delete("/api/dags/t_dag", headers=headers).status_code == 200
    # 用户管理：创建 viewer2 后可登录
    resp = client.post(
        "/api/auth/users",
        json={"username": "viewer2", "password": "v2-pass", "role": "viewer"},
        headers=headers,
    )
    assert resp.status_code == 200
    assert client.get("/api/dags", headers=_bearer(_login(client, "viewer2", "v2-pass"))).status_code == 200


# ---------- 登录 ----------


def test_login_success_token_usable(client):
    resp = client.post("/api/auth/login", json={"username": "admin1", "password": "admin-pass"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["username"] == "admin1" and body["role"] == "admin"
    assert isinstance(body["expires_at"], int) and body["token"]
    headers = _bearer(body["token"])
    assert client.get("/api/dags", headers=headers).status_code == 200
    assert client.delete("/api/dags/whatever", headers=headers).status_code == 404  # 过了 403 这关


def test_login_wrong_password_401_and_audited(client):
    resp = client.post("/api/auth/login", json={"username": "viewer1", "password": "wrong"})
    assert resp.status_code == 401
    fails = [e for e in _audit_entries() if e["path"] == "/api/auth/login" and e["status"] == 401]
    assert len(fails) == 1
    assert fails[0]["user"] == "viewer1"
    assert fails[0]["method"] == "POST"


def test_login_unknown_user_401(client):
    resp = client.post("/api/auth/login", json={"username": "nobody", "password": "x"})
    assert resp.status_code == 401


# ---------- 审计 ----------


def test_audit_records_mutations(client):
    headers = _bearer(LEGACY_TOKEN)
    client.get("/api/dags", headers=headers)  # GET 不应审计
    client.post("/api/dags", json=_dag_body(), headers=headers)
    client.delete("/api/dags/t_dag", headers=headers)

    entries = _audit_entries()
    assert {e["method"] for e in entries} == {"POST", "DELETE"}
    create = next(e for e in entries if e["method"] == "POST")
    assert create["user"] == "token-admin" and create["role"] == "admin"
    assert create["path"] == "/api/dags" and create["status"] == 201
    assert create["ts"]
    delete = next(e for e in entries if e["method"] == "DELETE")
    assert delete["path"] == "/api/dags/t_dag" and delete["status"] == 200
    assert delete["detail"] == {"dag_id": "t_dag"}
