"""Durable batch/outbox storage. The caller owns its transaction boundary."""

import json


class PostgresBatchRepository:
    def __init__(self, cursor):
        self.cursor = cursor

    def find(self, batch_id):
        self.cursor.execute(
            "SELECT mode, arguments, result FROM dwh.etl_batches WHERE batch_id = %s", (batch_id,)
        )
        return self.cursor.fetchone()

    def state(self):
        self.cursor.execute(
            "SELECT bootstrapped, revision FROM dwh.etl_pipeline_state WHERE pipeline = 'sales'"
        )
        return self.cursor.fetchone()

    def commit_record(self, batch_id, mode, arguments, result):
        self.cursor.execute(
            "INSERT INTO dwh.etl_batches (batch_id, mode, arguments, revision, result) "
            "VALUES (%s, %s, %s::jsonb, %s, %s::jsonb)",
            (batch_id, mode, json.dumps(arguments), result["revision"], json.dumps(result)),
        )
        self.cursor.execute(
            "UPDATE dwh.etl_pipeline_state SET bootstrapped = true, revision = %s WHERE pipeline = 'sales'",
            (result["revision"],),
        )

    def pending(self, limit):
        self.cursor.execute(
            "SELECT batch_id, revision FROM dwh.etl_batches WHERE event_delivered_at IS NULL "
            "ORDER BY revision LIMIT %s",
            (limit,),
        )
        return [{"batch_id": row[0], "revision": row[1]} for row in self.cursor.fetchall()]

    def acknowledge(self, batch_ids):
        self.cursor.execute(
            "UPDATE dwh.etl_batches SET event_delivered_at = clock_timestamp() "
            "WHERE batch_id = ANY(%s) AND event_delivered_at IS NULL",
            (batch_ids,),
        )
        return self.cursor.rowcount
