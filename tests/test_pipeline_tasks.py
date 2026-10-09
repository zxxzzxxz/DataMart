"""Publication contracts against the actual Airflow SDK event accessor."""

import json
from types import SimpleNamespace

import pipeline_tasks
import pytest
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk.execution_time.context import OutletEventAccessors
from postgres_assertions import assert_matches_source
from psycopg2.extensions import parse_dsn
from warehouse_pipeline import WarehousePipeline


def test_asset_metadata_uses_committed_batch_revision(monkeypatch):
    events = OutletEventAccessors()
    monkeypatch.setattr(pipeline_tasks, "get_current_context", lambda: {"outlet_events": events})
    ids = pipeline_tasks._emit([{"batch_id": "a", "revision": 3}, {"batch_id": "b", "revision": 4}])
    assert ids == ["a", "b"]
    assert events[pipeline_tasks.DAILY_SALES_ASSET].extra == {
        "batch_ids": ["a", "b"],
        "revision": 4,
        "validated": True,
    }


def test_empty_outbox_skips_asset_emission(monkeypatch):
    monkeypatch.setattr(pipeline_tasks, "pipeline", lambda: SimpleNamespace(pending_events=lambda: []))
    with pytest.raises(AirflowSkipException):
        pipeline_tasks.load_outbox()


def test_metrics_outage_does_not_turn_committed_data_into_failed_task(monkeypatch):
    report = {"table": "dm.daily_city_sales", "results": [], "drift": None}

    def unavailable(*_args, **_kwargs):
        raise ConnectionError("Pushgateway unavailable")

    monkeypatch.setattr(pipeline_tasks.Variable, "get", unavailable)
    pipeline_tasks.push_quality_metrics([report])


def test_acknowledgement_uses_emitted_ids_not_latest_pending_batch(monkeypatch):
    seen = []
    monkeypatch.setattr(
        pipeline_tasks,
        "pipeline",
        lambda: SimpleNamespace(
            acknowledge_events=lambda ids: seen.extend(ids) or len(ids),
        ),
    )
    monkeypatch.setattr(
        pipeline_tasks,
        "get_current_context",
        lambda: {
            "ti": SimpleNamespace(xcom_pull=lambda task_ids: ["emitted-revision-1"]),
        },
    )
    assert pipeline_tasks.acknowledge_publication() == 1
    assert seen == ["emitted-revision-1"]


@pytest.mark.postgres
def test_actual_airflow_postgres_hook_runs_transaction_engine(database):
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from airflow.sdk import Connection

    params = parse_dsn(database.test_dsn)
    hook = PostgresHook(postgres_conn_id="warehouse")
    # Inject only credential resolution; the provider and both database transactions are real.
    hook._connection = Connection(
        conn_id="warehouse",
        conn_type="postgres",
        host=params["host"],
        port=int(params["port"]),
        login=params["user"],
        password=params.get("password"),
        schema=params["dbname"],
        extra=json.dumps({"connect_timeout": "10", "target_session_attrs": "read-write"}),
    )
    engine = WarehousePipeline(hook.get_conn)
    engine.run("sdk-hook-full", "full")
    assert_matches_source(database)
    engine.run("sdk-hook-incremental", "incremental")
    assert engine.audit()["revision"] == 2
    assert engine.acknowledge_events(["sdk-hook-full"]) == 1
    assert engine.pending_events() == [{"batch_id": "sdk-hook-incremental", "revision": 2}]


# ---------- 空增量不发布：publish_batch 守卫回归 ----------


def _empty_increment_result(**overrides):
    result = {
        "mode": "incremental",
        "full_reconcile": False,
        "source_changes": 0,
        "batch_id": "b-empty",
        "revision": 9,
    }
    result.update(overrides)
    return result


def test_empty_incremental_skips_publication(monkeypatch):
    monkeypatch.setattr(
        pipeline_tasks,
        "get_current_context",
        lambda: {"ti": SimpleNamespace(xcom_pull=lambda task_ids: _empty_increment_result())},
    )
    with pytest.raises(AirflowSkipException):
        pipeline_tasks.publish_batch()


def test_nonempty_incremental_still_publishes(monkeypatch):
    events = OutletEventAccessors()
    monkeypatch.setattr(
        pipeline_tasks,
        "get_current_context",
        lambda: {
            "ti": SimpleNamespace(
                xcom_pull=lambda task_ids: _empty_increment_result(source_changes=2, batch_id="b-nonempty")
            ),
            "outlet_events": events,
        },
    )
    assert pipeline_tasks.publish_batch() == ["b-nonempty"]


def test_full_reconcile_still_publishes_even_when_empty(monkeypatch):
    events = OutletEventAccessors()
    monkeypatch.setattr(
        pipeline_tasks,
        "get_current_context",
        lambda: {
            "ti": SimpleNamespace(xcom_pull=lambda task_ids: _empty_increment_result(full_reconcile=True)),
            "outlet_events": events,
        },
    )
    assert pipeline_tasks.publish_batch() == ["b-empty"]
