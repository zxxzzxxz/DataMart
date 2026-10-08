"""Sales pipeline: one PostgreSQL snapshot, one commit, durable change acknowledgement.

All managed writers use the same session advisory lock, acquired BEFORE starting
the repeatable-read transaction. Closing a failed connection releases the lock.
The connection factory must return a fresh, dedicated connection (not a pool lease).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import asdict
from datetime import date
from pathlib import Path

from datamart.errors import PipelineQualityError
from datamart.infrastructure.postgres.batches import PostgresBatchRepository
from datamart.infrastructure.postgres.sales import PostgresSalesRepository
from datamart.infrastructure.postgres.transaction import PostgresUnitOfWork, TransactionQueryHook
from datamart.quality import load_suite, run_suite

log = logging.getLogger(__name__)
LOCK_KEY = 0x444154414D415254  # DATAMART; fixed across DAGs and worker processes
ASSET_URI = "postgres://warehouse/warehouse/dm/daily_city_sales"


class SalesPipeline:
    def __init__(self, connect: Callable, config_dir, *, lock_timeout_ms=30_000, repository=None):
        self.repository = repository or PostgresSalesRepository()
        self.connect = connect
        self.config_dir = Path(config_dir)
        self.lock_timeout_ms = int(lock_timeout_ms)
        if self.lock_timeout_ms <= 0:
            raise ValueError("lock_timeout_ms must be positive")

    def run(self, batch_id: str, mode: str, *, start_date=None, end_date=None, checkpoint=None) -> dict:
        """Apply/replay a logical batch. checkpoint is an acceptance-test fault hook.

        A successful ledger row and pending asset event commit with the data.
        Repeating the same batch ID returns that result without applying new changes.
        Changes committed after the snapshot remain pending, including smaller IDs.
        """
        if not batch_id or len(batch_id) > 1000:
            raise ValueError("batch_id must contain 1..1000 characters")
        if mode not in ("full", "incremental", "backfill"):
            raise ValueError(f"Unknown pipeline mode: {mode}")
        arguments = self._arguments(mode, start_date, end_date)
        with PostgresUnitOfWork(
            self.connect, lock_key=LOCK_KEY, lock_timeout_ms=self.lock_timeout_ms
        ) as unit:
            conn, cur = unit.connection, unit.cursor
            cur.execute("SET LOCAL TIME ZONE 'Asia/Shanghai'")
            batches = PostgresBatchRepository(cur)
            previous = batches.find(batch_id)
            if previous:
                if previous[0] != mode or previous[1] != arguments:
                    raise ValueError("batch_id was already committed with different arguments")
                return {**previous[2], "replayed": True}
            state = batches.state()
            if state is None:
                raise ValueError("Apply warehouse migration V2 before running the pipeline")
            if mode == "backfill" and not state[0]:
                raise ValueError("Run full/incremental bootstrap before backfill")
            if mode != "backfill":
                self.repository.assert_capture_enabled(cur)
            cur.execute("CREATE TEMP TABLE batch_dates (stat_date date PRIMARY KEY) ON COMMIT DROP")
            if mode == "backfill":
                cur.execute(
                    "INSERT INTO batch_dates SELECT generate_series(%s::date, %s::date, '1 day')::date",
                    (arguments["start_date"], arguments["end_date"]),
                )
                source_changes = 0
                changed_orders = deleted_orders = 0
                full = False
            else:
                cur.execute("CREATE TEMP TABLE batch_changes ON COMMIT DROP AS SELECT * FROM dwh.etl_changes")
                cur.execute("SELECT count(*), bool_or(operation = 'TRUNCATE') FROM batch_changes")
                source_changes, truncated = cur.fetchone()
                full = mode == "full" or not state[0] or bool(truncated)
                self.repository.capture_keys(cur, full)
                self._checkpoint(checkpoint, "after_snapshot", conn)
                changed_orders, deleted_orders = self.repository.merge(cur)
            self.repository.refresh(cur, checkpoint, conn)
            if mode != "backfill":
                self.repository.assert_source_matches(cur)
            self.repository.assert_aggregates(cur)
            reports = self._quality(conn, batch_id)
            self._checkpoint(checkpoint, "after_quality", conn)
            if mode != "backfill":
                # Acknowledge the exact visible set, never change_id <= max(id).
                cur.execute(
                    "DELETE FROM dwh.etl_changes c USING batch_changes b WHERE c.change_id = b.change_id"
                )
                # Legacy timestamp watermarks remain diagnostics, not extraction checkpoints.
                cur.execute(
                    "INSERT INTO dwh.etl_watermark (table_name, last_watermark) "
                    "SELECT 'ods.orders', coalesce(max(updated_at), 'epoch'::timestamptz) "
                    "FROM ods.orders "
                    "UNION ALL SELECT 'ods.users', coalesce(max(updated_at), 'epoch'::timestamptz) "
                    "FROM ods.users ON CONFLICT (table_name) DO UPDATE SET "
                    "last_watermark = EXCLUDED.last_watermark, updated_at = now()"
                )
            cur.execute("SELECT stat_date::text FROM batch_dates ORDER BY stat_date")
            result = {
                "batch_id": batch_id,
                "mode": mode,
                "full_reconcile": full,
                "revision": state[1] + 1,
                "source_changes": source_changes,
                "changed_orders": changed_orders,
                "deleted_orders": deleted_orders,
                "affected_dates": [row[0] for row in cur.fetchall()],
                "quality": reports,
                "replayed": False,
            }
            batches.commit_record(batch_id, mode, arguments, result)
            self._checkpoint(checkpoint, "before_commit", conn)
        self._checkpoint(checkpoint, "after_commit", conn)
        return result

    @staticmethod
    def _arguments(mode, start_date, end_date):
        if mode != "backfill":
            if start_date is not None or end_date is not None:
                raise ValueError("Date range is only valid for backfill")
            return {}
        try:
            start = date.fromisoformat(start_date)
            end = date.fromisoformat(end_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("Backfill dates must be YYYY-MM-DD strings") from exc
        if not 0 <= (end - start).days < 31:
            raise ValueError("Backfill range must be ordered and contain at most 31 days")
        return {"start_date": start.isoformat(), "end_date": end.isoformat()}

    @staticmethod
    def _checkpoint(callback, phase, conn):
        if callback:
            callback(phase, conn)

    def _quality(self, conn, batch_id):
        files = sorted(self.config_dir.glob("*.yaml"))
        if not files:
            raise PipelineQualityError(f"No quality suites in {self.config_dir}")
        reports = []
        failures = []
        for path in files:
            suite = load_suite(path)
            report = run_suite(
                "warehouse",
                suite,
                self.config_dir / "baselines",
                run_id=batch_id,
                hook=TransactionQueryHook(conn),
                read_only=True,
            )
            for line in report.to_log_lines():
                log.info(line)
            reports.append(asdict(report))
            if report.score < suite.severity_threshold:
                failures.append(f"{suite.table}: {report.score:.2f} < {suite.severity_threshold}")
        if failures:
            raise PipelineQualityError("Required quality checks failed: " + "; ".join(failures), reports)
        return reports

    def audit(self) -> dict:
        """Read-only snapshot of already validated data; never quarantine published rows."""
        with PostgresUnitOfWork(self.connect, read_only=True) as unit:
            conn, cur = unit.connection, unit.cursor
            cur.execute("SET LOCAL TIME ZONE 'Asia/Shanghai'")
            state = PostgresBatchRepository(cur).state()
            if not state or not state[0]:
                raise PipelineQualityError("No validated batch has been committed")
            self.repository.assert_aggregates(cur)
            reports = self._quality(conn, f"audit/{state[1]}")
            cur.execute(
                "SELECT stat_date::text, city_cnt, order_cnt, total_amount::text "
                "FROM dm.daily_summary_report ORDER BY stat_date DESC LIMIT 1"
            )
            latest = cur.fetchone()
            return {"revision": state[1], "quality": reports, "latest_report": latest}

    def pending_events(self, limit=100) -> list[dict]:
        with PostgresUnitOfWork(self.connect, read_only=True) as unit:
            return PostgresBatchRepository(unit.cursor).pending(limit)

    def acknowledge_events(self, batch_ids: list[str]) -> int:
        with PostgresUnitOfWork(self.connect) as unit:
            return PostgresBatchRepository(unit.cursor).acknowledge(batch_ids)
