"""Real PostgreSQL acceptance. Creates/drops only its own randomly named database.

Set DATAMART_TEST_DSN to a PostgreSQL admin connection with CREATEDB privilege.
Absent DSN skips these tests; CI runs them in a dedicated PostgreSQL 16 service.
"""

import os
import queue
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

import pytest
from postgres_assertions import assert_matches_source, query, snapshot
from warehouse_pipeline import DEFAULT_CONFIG_DIR, LOCK_KEY, PipelineQualityError, WarehousePipeline

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.postgres


def test_bootstrap_and_migration_reentry(database):
    query(
        database,
        (ROOT / "docker/warehouse-migrations/V2__transactional_pipeline.sql").read_text(encoding="utf-8"),
    )
    result = WarehousePipeline(database).run("bootstrap", "incremental")
    assert result["full_reconcile"]
    assert result["changed_orders"] == 10
    assert_matches_source(database)
    query(database, "UPDATE ods.orders SET amount = 21 WHERE order_id = 101")
    assert query(database, "SELECT count(*) FROM dwh.etl_changes") == [(1,)]


@pytest.mark.parametrize(
    "change",
    [
        "order_date = order_date + 5",
        "user_id = 4",
        "amount = 12.34",
        "status = 'refund'",
        "order_id = 901",
    ],
)
def test_incremental_field_corrections(database, change):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    query(database, f"UPDATE ods.orders SET {change}, updated_at = '2000-01-01' WHERE order_id = 101")
    result = engine.run("correction", "incremental")
    assert result["source_changes"] >= 1
    assert_matches_source(database)
    assert query(database, "SELECT count(*) FROM dwh.etl_changes") == [(0,)]


def test_user_changes_restate_historical_city_and_name(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    query(database, "UPDATE ods.users SET city = '杭州', user_name = '新名字' WHERE user_id = 1")
    engine.run("dimension", "incremental")
    assert_matches_source(database)


@pytest.mark.parametrize(
    "statement",
    [
        "DELETE FROM ods.orders WHERE order_id = 101",
        "DELETE FROM ods.orders; DELETE FROM ods.users",
        "TRUNCATE ods.orders, ods.users",
    ],
)
def test_deletes_and_legal_empty_source(database, statement):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    query(database, statement)
    engine.run("delete", "incremental")
    assert_matches_source(database)


def test_late_commit_with_smaller_change_id_is_not_lost(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    late = database()
    try:
        with late.cursor() as cur:
            cur.execute("UPDATE ods.orders SET amount = 501, updated_at = '2000-01-01' WHERE order_id = 101")
            cur.execute("SELECT max(change_id) FROM dwh.etl_changes")
            lower_id = cur.fetchone()[0]
        query(database, "UPDATE ods.orders SET amount = 502, updated_at = '2000-01-01' WHERE order_id = 102")
        engine.run("overtake", "incremental")
        assert query(database, "SELECT amount FROM dwh.fact_orders WHERE order_id = 101") == [
            (Decimal("199.00"),)
        ]
        late.commit()
        assert query(database, "SELECT change_id FROM dwh.etl_changes") == [(lower_id,)]
        engine.run("late", "incremental")
        assert_matches_source(database)
    finally:
        late.close()


def test_changes_after_snapshot_wait_for_next_batch(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")

    def insert_after_snapshot(phase, _conn):
        if phase == "after_snapshot":
            query(database, "UPDATE ods.orders SET amount = 1234 WHERE order_id = 103")

    engine.run("snapshot", "incremental", checkpoint=insert_after_snapshot)
    assert query(database, "SELECT amount FROM dwh.fact_orders WHERE order_id = 103") == [
        (Decimal("899.00"),)
    ]
    assert query(database, "SELECT count(*) FROM dwh.etl_changes") == [(1,)]
    engine.run("next-snapshot", "incremental")
    assert_matches_source(database)


@pytest.mark.parametrize("fault_phase", ["after_mart_delete", "after_quality", "before_commit"])
def test_failures_rollback_everything_and_retry(database, fault_phase):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    before = snapshot(database)
    query(database, "UPDATE ods.orders SET amount = 1777, order_date = order_date + 1 WHERE order_id = 101")

    def fail(phase, _conn):
        if phase == fault_phase:
            raise RuntimeError("injected worker failure")

    with pytest.raises(RuntimeError, match="injected"):
        engine.run("failed", "incremental", checkpoint=fail)
    assert snapshot(database) == before
    assert query(database, "SELECT count(*) FROM dwh.etl_changes") == [(1,)]
    engine.run("failed", "incremental")
    assert_matches_source(database)


def test_database_disconnect_releases_lock_and_rolls_back(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    before = snapshot(database)
    query(database, "UPDATE ods.orders SET amount = 2000 WHERE order_id = 101")

    def terminate(phase, conn):
        if phase == "after_mart_delete":
            with conn.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                backend_pid = cur.fetchone()[0]
            query(database, "SELECT pg_terminate_backend(%s)", (backend_pid,))

    with pytest.raises(database.error_type):
        engine.run("disconnect", "incremental", checkpoint=terminate)
    assert snapshot(database) == before
    WarehousePipeline(database, lock_timeout_ms=1000).run("disconnect", "incremental")
    assert_matches_source(database)


def test_bad_quality_never_commits_or_moves_published_rows(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    before = snapshot(database)
    query(database, "UPDATE ods.orders SET status = 'unknown' WHERE order_id = 101")
    with pytest.raises(PipelineQualityError):
        engine.run("bad", "incremental")
    assert snapshot(database) == before
    assert not query(
        database, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dwh_quarantine'"
    )
    assert all(event["batch_id"] != "bad" for event in engine.pending_events())
    query(database, "UPDATE ods.orders SET status = 'paid' WHERE order_id = 101")
    engine.run("bad", "incremental")
    assert_matches_source(database)


def test_commit_lost_response_is_replayed_and_event_is_durable(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    engine.acknowledge_events(["initial"])
    query(database, "UPDATE ods.orders SET amount = 2000 WHERE order_id = 101")

    def lose_response(phase, _conn):
        if phase == "after_commit":
            raise RuntimeError("lost commit response")

    with pytest.raises(RuntimeError, match="lost commit"):
        engine.run("committed", "incremental", checkpoint=lose_response)
    assert_matches_source(database)
    query(database, "UPDATE ods.orders SET amount = 3000 WHERE order_id = 101")
    result = engine.run("committed", "incremental")
    assert result["replayed"]
    assert query(database, "SELECT amount FROM dwh.fact_orders WHERE order_id = 101") == [
        (Decimal("2000.00"),)
    ]
    assert [item["batch_id"] for item in engine.pending_events()] == ["committed"]
    assert engine.acknowledge_events(["committed"]) == 1
    assert engine.acknowledge_events(["committed"]) == 0
    assert engine.pending_events() == []
    engine.run("new", "incremental")
    assert_matches_source(database)


def test_full_incremental_backfill_serialize_and_readers_see_complete_data(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    before = query(database, "SELECT * FROM dm.daily_city_sales ORDER BY stat_date, city")
    day = query(database, "SELECT min(order_date)::text FROM ods.orders")[0][0]
    query(database, "UPDATE ods.orders SET amount = 3210 WHERE order_id = 101")
    held = threading.Event()
    release = threading.Event()

    def hold(phase, _conn):
        if phase == "after_mart_delete":
            held.set()
            if not release.wait(10):
                raise RuntimeError("acceptance barrier timed out")

    with ThreadPoolExecutor(max_workers=3) as executor:
        first = executor.submit(engine.run, "full", "full", checkpoint=hold)
        try:
            assert held.wait(5)
            second = executor.submit(engine.run, "incremental", "incremental")
            third = executor.submit(engine.run, "backfill", "backfill", start_date=day, end_date=day)
            deadline = time.monotonic() + 3
            waiting = 0
            while time.monotonic() < deadline:
                waiting = query(
                    database, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
                )[0][0]
                if waiting >= 2:
                    break
                time.sleep(0.01)
            assert waiting >= 2
            assert query(database, "SELECT * FROM dm.daily_city_sales ORDER BY stat_date, city") == before
        finally:
            release.set()
        results = [future.result(timeout=10) for future in (first, second, third)]
    assert sorted(result["revision"] for result in results) == [2, 3, 4]
    assert_matches_source(database)


def test_concurrent_duplicate_batch_is_applied_once(database):
    engine = WarehousePipeline(database)
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: engine.run("same", "full"), range(2)))
    assert sorted(result["replayed"] for result in results) == [False, True]
    assert query(database, "SELECT count(*) FROM dwh.etl_batches") == [(1,)]
    with pytest.raises(ValueError, match="different arguments"):
        engine.run("same", "incremental")
    assert_matches_source(database)


def test_backfill_repairs_report_and_preserves_pending_source_changes(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    day = query(database, "SELECT min(stat_date)::text FROM dm.daily_city_sales")[0][0]
    query(database, "DELETE FROM dm.daily_city_sales WHERE stat_date = %s", (day,))
    query(database, "UPDATE ods.orders SET amount = 9999 WHERE order_id = 103")
    result = engine.run("backfill", "backfill", start_date=day, end_date=day)
    assert result["affected_dates"] == [day]
    assert query(database, "SELECT count(*) FROM dwh.etl_changes") == [(1,)]
    assert engine.audit()["revision"] == 2
    engine.run("pending", "incremental")
    assert_matches_source(database)


def test_ten_thousand_orders_full_and_incremental(database, record_property):
    query(
        database,
        "INSERT INTO ods.orders (order_id, user_id, amount, status, order_date) "
        "SELECT i, 1 + i % 5, 1 + i % 100, 'paid', CURRENT_DATE - i % 7 FROM generate_series(1001,11000) i",
    )
    engine = WarehousePipeline(database)
    started = time.perf_counter()
    engine.run("volume-full", "full")
    record_property("full_10010_orders_seconds", round(time.perf_counter() - started, 3))
    query(database, "UPDATE ods.orders SET amount = amount + 1 WHERE order_id BETWEEN 1001 AND 2000")
    started = time.perf_counter()
    result = engine.run("volume-incremental", "incremental")
    record_property("incremental_1000_changes_seconds", round(time.perf_counter() - started, 3))
    assert result["source_changes"] == 1000
    assert result["changed_orders"] == 1000
    assert_matches_source(database)


def test_quality_sql_error_uses_savepoint_and_rolls_back(database, tmp_path):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    before = snapshot(database)
    config = tmp_path / "quality"
    config.mkdir()
    (config / "broken.yaml").write_text(
        "table: dwh.fact_orders\nchecks:\n  - type: custom_sql\n"
        "    sql: SELECT missing_column FROM dwh.fact_orders\n    expect: empty\n",
        encoding="utf-8",
    )
    with pytest.raises(PipelineQualityError):
        WarehousePipeline(database, config).run("bad-sql", "incremental")
    assert snapshot(database) == before
    assert DEFAULT_CONFIG_DIR.is_dir()


def test_oltp_rollback_does_not_emit_change(database):
    conn = database()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE ods.orders SET amount = 500 WHERE order_id = 101")
        conn.rollback()
    finally:
        conn.close()
    assert query(database, "SELECT count(*) FROM dwh.etl_changes") == [(0,)]


def test_backfill_before_bootstrap_is_rejected(database):
    with pytest.raises(ValueError, match="bootstrap"):
        WarehousePipeline(database).run("early", "backfill", start_date="2026-01-01", end_date="2026-01-01")
    assert query(database, "SELECT count(*) FROM dwh.etl_batches") == [(0,)]


def test_disabled_change_capture_blocks_publication(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    before = snapshot(database)
    query(database, "ALTER TABLE ods.orders DISABLE TRIGGER datamart_orders_changes")
    query(database, "UPDATE ods.orders SET amount = 333 WHERE order_id = 101")
    with pytest.raises(PipelineQualityError, match="capture"):
        engine.run("disabled", "incremental")
    assert snapshot(database) == before
    query(database, "ALTER TABLE ods.orders ENABLE TRIGGER datamart_orders_changes")
    engine.run("reconcile", "full")
    assert_matches_source(database)


def test_busy_pipeline_lock_times_out_without_changes(database):
    engine = WarehousePipeline(database, lock_timeout_ms=100)
    conn = database()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
        with pytest.raises(database.lock_error_type):
            engine.run("blocked", "full")
        assert query(database, "SELECT count(*) FROM dwh.etl_batches") == [(0,)]
    finally:
        conn.close()
    engine.run("blocked", "full")
    assert_matches_source(database)


def test_force_killed_worker_rolls_back_and_retry_succeeds(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    before = snapshot(database)
    query(database, "UPDATE ods.orders SET amount = 4500 WHERE order_id = 101")
    code = """
import os, sys, time
sys.path.insert(0, 'plugins')
import psycopg2
from warehouse_pipeline import WarehousePipeline
def checkpoint(phase, conn):
    if phase == 'after_mart_delete':
        print('READY', flush=True)
        time.sleep(30)
WarehousePipeline(lambda: psycopg2.connect(os.environ['DATAMART_WORKER_TEST_DSN'])).run(
    'killed-worker', 'incremental', checkpoint=checkpoint)
"""
    env = {**os.environ, "DATAMART_WORKER_TEST_DSN": database.test_dsn}
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    worker = subprocess.Popen(
        [sys.executable, "-c", code],
        cwd=ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        creationflags=flags,
    )
    ready = queue.Queue()
    threading.Thread(target=lambda: ready.put(worker.stdout.readline()), daemon=True).start()
    try:
        assert ready.get(timeout=10).strip() == b"READY"
        assert snapshot(database) == before
    finally:
        worker.kill()
        worker.communicate(timeout=5)
    engine.run("killed-worker", "incremental")
    assert_matches_source(database)


def test_read_only_audit_never_quarantines_published_rows(database):
    engine = WarehousePipeline(database)
    engine.run("initial", "full")
    query(database, "UPDATE dwh.fact_orders SET status = 'unknown' WHERE order_id = 105")
    before = snapshot(database)
    with pytest.raises(PipelineQualityError):
        engine.audit()
    assert snapshot(database) == before
    assert not query(
        database, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dwh_quarantine'"
    )
