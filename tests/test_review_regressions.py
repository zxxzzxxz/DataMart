"""Architecture review regressions: real client, process-safe CAS, and operator contracts."""

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from datamart.infrastructure.dag_store import DagStore, MtimeConflictError
from datamart.workflows import convert
from datamart.workflows.schema import DesignerDag, DesignerNode
from pydantic import ValidationError


def dag(description="first", params=None, node_type="empty"):
    return DesignerDag(
        dag_id="review",
        description=description,
        nodes=[DesignerNode(id="start", type=node_type, params=params or {})],
    )


def store_for(tmp_path):
    return DagStore(str(tmp_path / "dags"), str(tmp_path / "history"))


def test_two_store_instances_cannot_save_the_same_revision(tmp_path):
    first = store_for(tmp_path)
    second = store_for(tmp_path)
    original = first.create_document(dag())
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(store.update_document, dag(text), client_revision=original.revision)
            for store, text in ((first, "A"), (second, "B"))
        ]
    outcomes = []
    for future in futures:
        try:
            future.result()
            outcomes.append("saved")
        except MtimeConflictError:
            outcomes.append("conflict")
    assert sorted(outcomes) == ["conflict", "saved"]
    assert len(first.list_versions("review")) == 1


def test_processes_share_the_same_compare_and_swap_lock(tmp_path):
    store = store_for(tmp_path)
    original = store.create_document(dag())
    code = """
import sys
from datamart.infrastructure.dag_store import DagStore, MtimeConflictError
from datamart.workflows.schema import DesignerDag, DesignerNode
store=DagStore(sys.argv[1], sys.argv[2])
print('READY', flush=True)
sys.stdin.readline()
try:
    store.update_document(DesignerDag(dag_id='review', description=sys.argv[4], nodes=[DesignerNode(id='start',type='empty')]), client_revision=sys.argv[3])
    print('saved', flush=True)
except MtimeConflictError:
    print('conflict', flush=True)
"""
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-c",
                code,
                str(tmp_path / "dags"),
                str(tmp_path / "history"),
                original.revision,
                name,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=flags,
        )
        for name in ("A", "B")
    ]
    try:
        for process in processes:
            assert process.stdout.readline().strip() == b"READY"
        for process in processes:
            process.stdin.write(b"go\n")
            process.stdin.flush()
        results = [process.communicate(timeout=10)[0].strip().decode() for process in processes]
        assert sorted(results) == ["conflict", "saved"]
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)


def test_missing_update_version_is_rejected(tmp_path):
    store = store_for(tmp_path)
    store.create(dag())
    with pytest.raises(ValueError, match="client_revision"):
        store.update(dag("new"))


def test_content_revision_detects_external_edit_with_identical_mtime(tmp_path):
    store = store_for(tmp_path)
    original = store.create_document(dag())
    path = store.dir / "review.yaml"
    previous = path.stat()
    path.write_text(convert.to_yaml_text(dag("outside")), encoding="utf-8")
    os.utime(path, ns=(previous.st_atime_ns, previous.st_mtime_ns))
    with pytest.raises(MtimeConflictError):
        store.update_document(dag("stale"), client_revision=original.revision)


def test_oldest_of_twenty_versions_can_be_restored(tmp_path):
    store = store_for(tmp_path)
    current = store.create_document(dag("version 0"))
    for index in range(1, 21):
        current = store.update_document(dag(f"version {index}"), client_revision=current.revision)
    versions = store.list_versions("review")
    assert len(versions) == 20
    restored, _snapshot, _target = store.restore_document(
        "review",
        versions[-1]["version_id"],
        client_revision=current.revision,
    )
    assert restored.dag.description == "version 0"
    assert len(store.list_versions("review")) == 20


def test_failed_document_write_does_not_prune_the_requested_history(tmp_path, monkeypatch):
    store = store_for(tmp_path)
    current = store.create_document(dag("0"))
    for index in range(20):
        current = store.update_document(dag(str(index + 1)), client_revision=current.revision)
    oldest = store.list_versions("review")[-1]["version_id"]

    def fail(*_args):
        raise OSError("disk unavailable")

    monkeypatch.setattr(store, "_write", fail)
    with pytest.raises(OSError):
        store.restore_document("review", oldest, client_revision=current.revision)
    assert store.get_version("review", oldest)
    assert store.load_document("review").revision == current.revision


@pytest.mark.parametrize(
    "params",
    [
        {"trigger_rule": "invalid_rule"},
        {"retries": -1},
        {"retries": 11},
        {"retries": True},
        {"retries": 1.5},
        {"operator": "arbitrary.Operator"},
    ],
)
def test_invalid_operator_parameters_are_rejected(params):
    with pytest.raises(ValidationError):
        dag(params=params)


@pytest.mark.parametrize("schedule", ["not-a-cron", "99 25 * * *", "* * *", "@unsupported"])
def test_invalid_schedule_is_rejected(schedule):
    with pytest.raises(ValidationError):
        DesignerDag(dag_id="review", schedule=schedule, nodes=[DesignerNode(id="start", type="empty")])


def test_notify_advanced_options_survive_yaml_roundtrip():
    model = dag(params={"message": "done", "trigger_rule": "all_done", "retries": 3}, node_type="notify")
    restored = convert.from_yaml_dict(convert.to_yaml_dict(model))
    assert restored.nodes[0].params == model.nodes[0].params


def test_http_headers_are_normalized_to_a_dictionary():
    model = dag(params={"endpoint": "/health", "headers": '{"X-Test":"1"}'}, node_type="http")
    task = convert.to_yaml_dict(model)["review"]["tasks"]["start"]
    assert task["headers"] == {"X-Test": "1"}


@pytest.mark.parametrize(
    "run_id",
    [
        "manual__2026-10-02T00:00:00+00:00",
        "scheduled__2026-10-02T08:00:00+08:00",
    ],
)
def test_real_airflow_client_accepts_and_encodes_native_run_ids(run_id):
    from app.airflow_client import AirflowClient

    requests = []

    def respond(request):
        requests.append(request)
        if request.url.path == "/auth/token":
            return httpx.Response(200, json={"access_token": "test-token"})
        return httpx.Response(200, json={"task_instances": [{"task_id": "start", "state": "success"}]})

    client = AirflowClient("http://airflow.invalid")
    client._http.close()
    client._http = httpx.Client(base_url=client.base_url, transport=httpx.MockTransport(respond))
    try:
        assert client.list_task_instances("review", run_id)[0]["state"] == "success"
        assert requests[-1].url.path.endswith(f"/{run_id}/taskInstances")
        assert b"%3A" in requests[-1].url.raw_path and b"%2B" in requests[-1].url.raw_path
    finally:
        client.close()


def test_api_has_no_file_side_effects_at_construction_and_closes_owned_client(tmp_path, monkeypatch):
    from app.main import create_app
    from app.settings import Settings
    from fastapi.testclient import TestClient

    class Client:
        closed = False

        def close(self):
            self.closed = True

    client = Client()
    monkeypatch.setenv("DAGS_DIR", str(tmp_path / "dags"))
    monkeypatch.setenv("DESIGNER_HISTORY_DIR", str(tmp_path / "history"))
    app = create_app(Settings.from_env(), airflow=client)
    assert not (tmp_path / "dags").exists()
    with TestClient(app) as api:
        assert api.get("/api/health").status_code == 200
    assert client.closed


def test_frontend_catalog_is_generated_from_the_canonical_backend():
    from datamart.workflows.catalog import list_node_types

    path = Path(__file__).resolve().parents[1] / "designer/frontend/src/generated/node-catalog.json"
    assert json.loads(path.read_text(encoding="utf-8")) == list_node_types()
