"""PostgreSQL unit of work and observational query adapter (psycopg2/3)."""

import logging
from contextlib import contextmanager

log = logging.getLogger(__name__)


@contextmanager
def transaction_cursor(conn, *, read_only=False):
    """Explicit SQL transaction boundaries work with both psycopg2 and psycopg3."""
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ" + (" READ ONLY" if read_only else ""))
        try:
            yield cur
            cur.execute("COMMIT")
        except BaseException:
            if not conn.closed:
                try:
                    cur.execute("ROLLBACK")
                except Exception:
                    log.warning("Rollback connection lost; retry using the durable batch ID", exc_info=True)
            raise


class TransactionQueryHook:
    """DQ queries share the pipeline transaction; query errors use savepoints."""

    def __init__(self, connection):
        self.connection = connection

    def get_records(self, sql, parameters=None):
        with self.connection.cursor() as cur:
            cur.execute("SAVEPOINT dq_query")
            try:
                cur.execute(sql, parameters)
                rows = cur.fetchall() if cur.description else []
            except Exception:
                cur.execute("ROLLBACK TO SAVEPOINT dq_query")
                cur.execute("RELEASE SAVEPOINT dq_query")
                raise
            # Quality SQL is observational, even if a trusted custom check writes.
            cur.execute("ROLLBACK TO SAVEPOINT dq_query")
            cur.execute("RELEASE SAVEPOINT dq_query")
            return rows

    def get_first(self, sql, parameters=None):
        rows = self.get_records(sql, parameters)
        return rows[0] if rows else None


class PostgresUnitOfWork:
    """Own a fresh connection, optional session lock, and one repeatable-read commit."""

    def __init__(
        self,
        connect,
        *,
        lock_key=None,
        lock_timeout_ms=30_000,
        statement_timeout_ms=600_000,
        read_only=False,
    ):
        self.connect = connect
        self.lock_key = lock_key
        self.lock_timeout_ms = lock_timeout_ms
        self.statement_timeout_ms = statement_timeout_ms
        self.read_only = read_only

    def __enter__(self):
        self.connection = self.connect()
        try:
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT set_config('lock_timeout', %s, false)", (f"{self.lock_timeout_ms}ms",))
                cursor.execute(
                    "SELECT set_config('statement_timeout', %s, false)",
                    (f"{self.statement_timeout_ms}ms",),
                )
                if self.lock_key is not None:
                    cursor.execute("SELECT pg_advisory_lock(%s)", (self.lock_key,))
            self.transaction = transaction_cursor(self.connection, read_only=self.read_only)
            self.cursor = self.transaction.__enter__()
            self.cursor.execute("SET LOCAL TIME ZONE 'Asia/Shanghai'")
            return self
        except BaseException:
            self.connection.close()
            raise

    def __exit__(self, exc_type, exc, traceback):
        try:
            return self.transaction.__exit__(exc_type, exc, traceback)
        finally:
            self.connection.close()
