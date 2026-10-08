"""设计器后端运行期能力测试：Airflow 运行监控 / 触发 / 版本历史 / 回滚 / SQL 预检 / 删除保护。

用 fastapi.testclient.TestClient 直跑 designer/backend 的 ASGI app；
- airflow_client 换为 _FakeAirflowClient（假 runs / trigger / tasks / active_runs，可注入不可用）
- main.psycopg2.connect 换为假连接（假 EXPLAIN 成功 / 失败 / 连接失败）
- env 经 monkeypatch 指向 tmp_path（DAGS_DIR / DESIGNER_HISTORY_DIR / 审计 / 用户文件）
"""

import json
import os
import sys
from pathlib import Path

import psycopg2
import pytest

_BACKEND_DIR = Path(__file__).resolve().parent.parent / "designer" / "backend"
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))

from app import airflow_client, auth, main  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

LEGACY_TOKEN = "runtime-test-token"
_USERS = [
    {"username": "admin1", "password": auth.hash_password("admin-pass"), "role": "admin"},
    {"username": "editor1", "password": auth.hash_password("editor-pass"), "role": "editor"},
    {"username": "viewer1", "password": auth.hash_password("viewer-pass"), "role": "viewer"},
]

_FAKE_RUNS = [
    {
        "dag_run_id": "scheduled__2026-09-17T02:00:00+00:00",
        "state": "success",
        "logical_date": "2026-09-17T02:00:00+00:00",
        "start_date": "2026-09-17T02:00:01+00:00",
        "end_date": "2026-09-17T02:03:00+00:00",
        "conf": {},
    },
    {
        "dag_run_id": "manual__2026-09-18T00:00:00+00:00",
        "state": "running",
        "logical_date": "2026-09-18T00:00:00+00:00",
        "start_date": "2026-09-18T00:00:02+00:00",
        "end_date": None,
        "conf": {"mode": "full"},
    },
]

_FAKE_TASKS = [
    {"task_id": "a", "state": "success", "try_number": 1, "duration": 1.25},
    {"task_id": "b", "state": "running", "try_number": 2, "duration": None},
]


class _FakeAirflowClient:
    """airflow_client.AirflowClient 的测试替身；unavailable=True 时所有调用抛 AirflowUnavailable。"""

    def __init__(self) -> None:
        self.runs = [dict(r) for r in _FAKE_RUNS]
        self.tasks = [dict(t) for t in _FAKE_TASKS]
        self.trigger_result = {"dag_run_id": "manual__2026-09-18T01:00:00+00:00", "state": "queued"}
        self.active = 0
        self.unavailable = False
        self.trigger_calls: list = []
        # psycopg2.connect 替身开关
        self.connect_ok = True
        self.explain_ok = True
        self.sql_statements: list = []

    def _maybe_fail(self) -> None:
        if self.unavailable:
            raise airflow_client.AirflowUnavailable("连接拒绝（测试注入）")

    def list_runs(self, dag_id: str, limit: int = 10):
        self._maybe_fail()
        return self.runs[:limit]

    def trigger(self, dag_id: str, conf: dict | None = None):
        self._maybe_fail()
        self.trigger_calls.append((dag_id, conf))
        return self.trigger_result

    def list_task_instances(self, dag_id: str, run_id: str):
        self._maybe_fail()
        return self.tasks

    def count_active_runs(self, dag_id: str):
        self._maybe_fail()
        return self.active

    # ---- psycopg2.connect 替身（挂到同一个 fake 上便于按用例配置）----
    def db_connect(self, dsn: str):
        if not self.connect_ok:
            raise psycopg2.OperationalError("could not connect to warehouse（测试注入）")
        fake = self

        class _Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, sql, params=None):
                fake.sql_statements.append(sql)
                if sql.startswith("EXPLAIN") and not fake.explain_ok:
                    raise psycopg2.ProgrammingError('syntax error at or near "FROM"（测试注入）')

        class _Conn:
            def cursor(self):
                return _Cursor()

            def rollback(self):
                pass

            def close(self):
                pass

        return _Conn()


def _dag_body(dag_id: str = "t_dag", description: str = "测试 DAG") -> dict:
    return {
        "dag_id": dag_id,
        "description": description,
        "schedule": "0 1 * * *",
        "nodes": [{"id": "a", "type": "sql", "params": {"sql": "SELECT 1"}, "position": {"x": 0, "y": 0}}],
        "edges": [],
    }


@pytest.fixture()
def client(tmp_path, monkeypatch):
    users_file = tmp_path / "users.json"
    users_file.write_text(json.dumps({"users": _USERS}), encoding="utf-8")
    monkeypatch.setenv("DESIGNER_USERS_FILE", str(users_file))
    monkeypatch.setenv("DESIGNER_AUDIT_LOG", str(tmp_path / "audit" / "designer-audit.jsonl"))
    monkeypatch.setenv("DESIGNER_TOKEN", LEGACY_TOKEN)
    monkeypatch.delenv("DESIGNER_AUTH_SECRET", raising=False)
    monkeypatch.setenv("DAGS_DIR", str(tmp_path / "dags"))
    monkeypatch.setenv("DESIGNER_HISTORY_DIR", str(tmp_path / "history"))
    monkeypatch.setenv("PLUGINS_DIR", str(tmp_path / "plugins"))
    fake = _FakeAirflowClient()
    return TestClient(main.create_app(airflow=fake, sql_connect=fake.db_connect)), fake


def _login(client: TestClient, username: str, password: str) -> str:
    resp = client.post("/api/auth/login", json={"username": username, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()["token"]


def _bearer(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _dag_file_path(tmp_path: Path, dag_id: str) -> Path:
    return tmp_path / "dags" / "designer" / f"{dag_id}.yaml"


# ---------- 运行监控（runs / tasks） ----------


def test_list_runs_ok(client):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "viewer1", "viewer-pass"))
    resp = test_client.get("/api/dags/t_dag/runs?limit=5", headers=headers)
    assert resp.status_code == 200
    runs = resp.json()["runs"]
    assert len(runs) == 2
    first = runs[0]
    assert first["run_id"] == "scheduled__2026-09-17T02:00:00+00:00"
    assert first["state"] == "success"
    assert first["conf"] == {}
    assert runs[1]["conf"] == {"mode": "full"}


def test_list_runs_airflow_unavailable_503(client):
    test_client, fake = client
    fake.unavailable = True
    headers = _bearer(_login(test_client, "viewer1", "viewer-pass"))
    resp = test_client.get("/api/dags/t_dag/runs", headers=headers)
    assert resp.status_code == 503
    assert resp.json()["detail"].startswith("Airflow 不可达：")


def test_list_run_tasks_ok(client):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "viewer1", "viewer-pass"))
    resp = test_client.get("/api/dags/t_dag/runs/manual__1/tasks", headers=headers)
    assert resp.status_code == 200
    tasks = resp.json()["tasks"]
    assert tasks == [
        {"task_id": "a", "state": "success", "try_number": 1, "duration": 1.25},
        {"task_id": "b", "state": "running", "try_number": 2, "duration": None},
    ]


# ---------- 触发 ----------


def test_trigger_ok(client, tmp_path):
    test_client, fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    assert test_client.post("/api/dags", json=_dag_body(), headers=headers).status_code == 201
    resp = test_client.post("/api/dags/t_dag/trigger", json={"conf": {"mode": "full"}}, headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == "manual__2026-09-18T01:00:00+00:00"
    assert body["state"] == "queued"
    assert fake.trigger_calls == [("t_dag", {"mode": "full"})]


def test_trigger_without_conf_body(client, tmp_path):
    test_client, fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    test_client.post("/api/dags", json=_dag_body(), headers=headers)
    resp = test_client.post("/api/dags/t_dag/trigger", headers=headers)
    assert resp.status_code == 200
    assert fake.trigger_calls == [("t_dag", None)]


def test_trigger_unknown_dag_404_without_calling_airflow(client):
    test_client, fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    resp = test_client.post("/api/dags/ghost/trigger", json={}, headers=headers)
    assert resp.status_code == 404
    assert "ghost" in resp.json()["detail"]
    assert fake.trigger_calls == []


def test_trigger_airflow_unavailable_503(client, tmp_path):
    test_client, fake = client
    fake.unavailable = True
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    test_client.post("/api/dags", json=_dag_body(), headers=headers)
    resp = test_client.post("/api/dags/t_dag/trigger", json={}, headers=headers)
    assert resp.status_code == 503
    assert resp.json()["detail"].startswith("Airflow 不可达：")


# ---------- 版本历史 / 回滚 ----------


def _create_and_update(client, tmp_path, headers):
    """创建 DAG 并两次更新（内容 A → B → C），返回各版本的文件内容。"""
    assert client.post("/api/dags", json=_dag_body(description="版本A"), headers=headers).status_code == 201
    content_a = _dag_file_path(tmp_path, "t_dag").read_text(encoding="utf-8")
    assert (
        client.put(
            "/api/dags/t_dag",
            json={
                **_dag_body(description="版本B"),
                "client_revision": client.get("/api/dags/t_dag", headers=headers).json()["revision"],
            },
            headers=headers,
        ).status_code
        == 200
    )
    content_b = _dag_file_path(tmp_path, "t_dag").read_text(encoding="utf-8")
    assert (
        client.put(
            "/api/dags/t_dag",
            json={
                **_dag_body(description="版本C"),
                "client_revision": client.get("/api/dags/t_dag", headers=headers).json()["revision"],
            },
            headers=headers,
        ).status_code
        == 200
    )
    content_c = _dag_file_path(tmp_path, "t_dag").read_text(encoding="utf-8")
    assert len({content_a, content_b, content_c}) == 3
    return content_a, content_b, content_c


def test_versions_list_and_read(client, tmp_path):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    content_a, content_b, _c = _create_and_update(test_client, tmp_path, headers)

    resp = test_client.get("/api/dags/t_dag/versions", headers=headers)
    assert resp.status_code == 200
    versions = resp.json()["versions"]
    assert len(versions) == 2  # 创建不产生快照，两次更新各一份
    assert [v["version_id"] for v in versions] == sorted(
        (v["version_id"] for v in versions), reverse=True
    )  # 时间倒序
    assert all(v["saved_at"] and v["size_bytes"] > 0 for v in versions)

    newest = versions[0]
    resp = test_client.get(f"/api/dags/t_dag/versions/{newest['version_id']}", headers=headers)
    assert resp.status_code == 200
    body = resp.json()
    assert body["version_id"] == newest["version_id"]
    assert body["saved_at"] == newest["saved_at"]
    assert body["content"] == content_b  # 最新快照 = 上一版内容（版本B）

    oldest = versions[-1]
    resp = test_client.get(f"/api/dags/t_dag/versions/{oldest['version_id']}", headers=headers)
    assert resp.json()["content"] == content_a


def test_versions_empty_for_unknown_dag(client):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "viewer1", "viewer-pass"))
    resp = test_client.get("/api/dags/nope/versions", headers=headers)
    assert resp.status_code == 200
    assert resp.json() == {"versions": []}


def test_get_version_unknown_404(client, tmp_path):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    test_client.post("/api/dags", json=_dag_body(), headers=headers)
    resp = test_client.get("/api/dags/t_dag/versions/29990101T000000Z", headers=headers)
    assert resp.status_code == 404


def test_get_version_illegal_id_400(client, tmp_path):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    test_client.post("/api/dags", json=_dag_body(), headers=headers)
    # 非法 version_id（非时间戳格式）应被校验拦截，返回 400 而非误当文件路径
    resp = test_client.get("/api/dags/t_dag/versions/not-a-timestamp", headers=headers)
    assert resp.status_code == 400
    assert "version_id" in resp.json()["detail"]


def test_rollback_snapshots_current_and_restores(client, tmp_path):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    content_a, _b, content_c = _create_and_update(test_client, tmp_path, headers)

    versions = test_client.get("/api/dags/t_dag/versions", headers=headers).json()["versions"]
    target = versions[-1]  # 最旧版本 = 内容A

    resp = test_client.post(
        "/api/dags/t_dag/rollback",
        json={
            "version_id": target["version_id"],
            "client_revision": test_client.get("/api/dags/t_dag", headers=headers).json()["revision"],
        },
        headers=headers,
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["restored_from"] == target["version_id"]
    assert body["version_id"] != target["version_id"]

    # 当前文件回写为版本A内容
    assert _dag_file_path(tmp_path, "t_dag").read_text(encoding="utf-8") == content_a
    # 回滚本身产生新快照，且快照内容 = 回滚前内容（版本C）
    versions = test_client.get("/api/dags/t_dag/versions", headers=headers).json()["versions"]
    assert len(versions) == 3
    snapshot = next(v for v in versions if v["version_id"] == body["version_id"])
    resp = test_client.get(f"/api/dags/t_dag/versions/{snapshot['version_id']}", headers=headers)
    assert resp.json()["content"] == content_c


def test_rollback_unknown_version_404(client, tmp_path):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    test_client.post("/api/dags", json=_dag_body(), headers=headers)
    resp = test_client.post(
        "/api/dags/t_dag/rollback",
        json={
            "version_id": "29990101T000000Z",
            "client_revision": test_client.get("/api/dags/t_dag", headers=headers).json()["revision"],
        },
        headers=headers,
    )
    assert resp.status_code == 404


# ---------- 删除保护 ----------


def _create_dag(client, headers, dag_id: str) -> None:
    assert client.post("/api/dags", json=_dag_body(dag_id), headers=headers).status_code == 201


def test_delete_blocked_by_active_runs_then_force(client):
    test_client, fake = client
    headers = _bearer(_login(test_client, "admin1", "admin-pass"))
    _create_dag(test_client, headers, "t_dag")
    fake.active = 2
    resp = test_client.delete("/api/dags/t_dag", headers=headers)
    assert resp.status_code == 409
    body = resp.json()
    assert body["active_runs"] == 2
    assert "2" in body["detail"] and "running" in body["detail"]

    resp = test_client.delete("/api/dags/t_dag?force=true", headers=headers)
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    assert not (Path(os.environ["DAGS_DIR"]) / "designer" / "t_dag.yaml").exists()


def test_delete_ok_when_no_active_runs(client):
    test_client, fake = client
    headers = _bearer(_login(test_client, "admin1", "admin-pass"))
    _create_dag(test_client, headers, "t_dag")
    fake.active = 0
    assert test_client.delete("/api/dags/t_dag", headers=headers).status_code == 200


def test_delete_not_blocked_when_airflow_unavailable(client):
    test_client, fake = client
    headers = _bearer(_login(test_client, "admin1", "admin-pass"))
    _create_dag(test_client, headers, "t_dag")
    fake.unavailable = True
    resp = test_client.delete("/api/dags/t_dag", headers=headers)
    assert resp.status_code == 200


# ---------- SQL 预检 ----------


def test_check_sql_ok(client):
    test_client, fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    resp = test_client.post(
        "/api/check-sql", json={"sql": "SELECT 1", "conn_id": "warehouse"}, headers=headers
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert isinstance(body["duration_ms"], float)
    assert any("statement_timeout" in s for s in fake.sql_statements)
    assert fake.sql_statements[-1] == "EXPLAIN SELECT 1"


def test_check_sql_explain_failure_returns_ok_false(client):
    test_client, fake = client
    fake.explain_ok = False
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    resp = test_client.post(
        "/api/check-sql", json={"sql": "SELECT bad FROM", "conn_id": "warehouse"}, headers=headers
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert "syntax error" in body["error"]


def test_check_sql_connect_failure_returns_ok_false(client):
    test_client, fake = client
    fake.connect_ok = False
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    resp = test_client.post(
        "/api/check-sql", json={"sql": "SELECT 1", "conn_id": "warehouse"}, headers=headers
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is False
    assert "could not connect" in body["error"]


def test_check_sql_unknown_conn_id_400(client):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    resp = test_client.post(
        "/api/check-sql", json={"sql": "SELECT 1", "conn_id": "analytics"}, headers=headers
    )
    assert resp.status_code == 400
    assert "conn_id" in resp.json()["detail"]


def test_check_sql_empty_sql_400(client):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "editor1", "editor-pass"))
    resp = test_client.post("/api/check-sql", json={"sql": "  ;  ", "conn_id": "warehouse"}, headers=headers)
    assert resp.status_code == 400


# ---------- RBAC ----------


def test_viewer_cannot_trigger_rollback_or_check_sql(client):
    test_client, _fake = client
    headers = _bearer(_login(test_client, "viewer1", "viewer-pass"))
    assert test_client.post("/api/dags/t_dag/trigger", json={}, headers=headers).status_code == 403
    assert (
        test_client.post("/api/dags/t_dag/rollback", json={"version_id": "x"}, headers=headers).status_code
        == 403
    )
    assert (
        test_client.post(
            "/api/check-sql", json={"sql": "SELECT 1", "conn_id": "warehouse"}, headers=headers
        ).status_code
        == 403
    )
    # viewer 读接口仍可用
    assert test_client.get("/api/dags/t_dag/runs", headers=headers).status_code == 200
    assert test_client.get("/api/dags/t_dag/versions", headers=headers).status_code == 200
