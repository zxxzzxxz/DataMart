"""Request validation without a database; real data/fault checks live separately."""

import pytest
from warehouse_pipeline import WarehousePipeline


@pytest.mark.parametrize(
    "start,end",
    [
        (None, "2026-01-01"),
        ("bad", "2026-01-01"),
        ("2026-01-02", "2026-01-01"),
        ("2026-01-01", "2026-02-01"),
        (123, "2026-01-01"),
    ],
)
def test_invalid_backfill_fails_before_database_access(start, end):
    def forbidden_connect():
        raise AssertionError("Invalid request must not open a database connection")

    with pytest.raises(ValueError):
        WarehousePipeline(forbidden_connect).run("invalid", "backfill", start_date=start, end_date=end)


def test_31_day_backfill_boundary():
    assert WarehousePipeline._arguments("backfill", "2026-01-01", "2026-01-31") == {
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
    }


# ---------- H7 写锁/语句超时参数化回归 ----------


def test_timeout_parameters_forwarded_and_validated():
    pipeline = WarehousePipeline(None, lock_timeout_ms=123, statement_timeout_ms=456)
    assert pipeline.lock_timeout_ms == 123
    assert pipeline.statement_timeout_ms == 456
    with pytest.raises(ValueError):
        WarehousePipeline(None, lock_timeout_ms=0)
    with pytest.raises(ValueError):
        WarehousePipeline(None, statement_timeout_ms=0)


def test_unit_of_work_applies_configured_timeouts():
    from datamart.infrastructure.postgres.transaction import PostgresUnitOfWork

    calls = []

    class _Cur:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            calls.append((sql, params))

    class _Conn:
        autocommit = False
        closed = False

        def cursor(self):
            return _Cur()

        def close(self):
            pass

    with PostgresUnitOfWork(_Conn, lock_timeout_ms=1000, statement_timeout_ms=42000, read_only=True):
        pass

    set_config_params = [params for sql, params in calls if "set_config" in sql]
    assert ("1000ms",) in set_config_params
    assert ("42000ms",) in set_config_params


# ---------- 空增量不发布：台账送达标记回归 ----------


def test_commit_record_marks_empty_batch_delivered_at_insert():
    from datamart.infrastructure.postgres.batches import PostgresBatchRepository

    calls = []

    class _Cur:
        rowcount = 0

        def execute(self, sql, params=None):
            calls.append((sql, params))

    repo = PostgresBatchRepository(_Cur())
    repo.commit_record("b-empty", "incremental", {}, {"revision": 7}, publish_pending=False)
    insert_sql, insert_params = calls[0]
    assert "event_delivered_at" in insert_sql
    assert insert_params[-1] is False  # CASE WHEN %s THEN NULL ELSE now() END → 提交即送达

    repo.commit_record("b-full", "full", {}, {"revision": 8}, publish_pending=True)
    assert calls[2][1][-1] is True  # 待发布：event_delivered_at 为 NULL，outbox 会拾取
