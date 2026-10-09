"""数据接入算子（plugins/ingestion.py）单元测试：不连真实库，全部用替身验证行为。"""

import io
import json

import ingestion
import pytest

# ---------- 替身 ----------


class _FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(f"{self.status_code} Error")

    def json(self):
        return self._payload


class _FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))

    def executemany(self, sql, values):
        self.conn.batches.append((sql, [list(v) for v in values]))

    def copy_expert(self, sql, buffer):
        self.conn.copies.append((sql, buffer.read()))


class _FakeConn:
    def __init__(self):
        self.executed = []
        self.batches = []
        self.copies = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1


class _FakeHook:
    """PostgresHook 替身：information_schema 返回固定列，get_conn 返回 _FakeConn。"""

    def __init__(self, columns):
        self._columns = columns
        self.conn = _FakeConn()

    def get_records(self, sql, parameters=None):
        assert "information_schema.columns" in sql
        return [(column,) for column in self._columns]

    def get_conn(self):
        return self.conn


@pytest.fixture()
def hook(monkeypatch):
    instance = _FakeHook(columns=["order_id", "customer", "city", "amount"])
    monkeypatch.setattr(ingestion, "_hook", lambda conn_id: instance)
    return instance


# ---------- api_load ----------


def test_api_load_extracts_data_path_and_filters_columns(hook, monkeypatch):
    payload = {
        "data": {
            "items": [
                {"order_id": 1, "customer": "张三", "city": "上海", "amount": 9.9, "junk": True},
                {"order_id": 2, "customer": "李四", "city": "北京", "amount": 19.9},
            ]
        }
    }
    import requests as real_requests

    monkeypatch.setattr(real_requests, "request", lambda *a, **k: _FakeResponse(payload))
    written = ingestion.api_load("http://mock/api", "dwh.t", data_path="data.items", conn_id="warehouse")
    assert written == 2
    insert_sql, values = hook.conn.batches[0]
    assert insert_sql == "INSERT INTO dwh.t (order_id, customer, city, amount) VALUES (%s, %s, %s, %s)"
    assert values == [[1, "张三", "上海", 9.9], [2, "李四", "北京", 19.9]]
    assert hook.conn.commits == 1 and hook.conn.closed == 1


def test_api_load_http_error_fails(hook, monkeypatch):
    import requests as real_requests

    monkeypatch.setattr(real_requests, "request", lambda *a, **k: _FakeResponse({}, status_code=500))
    with pytest.raises(real_requests.HTTPError):
        ingestion.api_load("http://mock/api", "dwh.t")
    assert hook.conn.batches == []  # 任何写入都不应发生


def test_api_load_missing_json_path_raises(hook, monkeypatch):
    import requests as real_requests

    monkeypatch.setattr(real_requests, "request", lambda *a, **k: _FakeResponse({"data": {}}))
    with pytest.raises(ValueError, match="data.items"):
        ingestion.api_load("http://mock/api", "dwh.t", data_path="data.items")


def test_api_load_upsert_sql_shape(hook, monkeypatch):
    import requests as real_requests

    monkeypatch.setattr(
        real_requests,
        "request",
        lambda *a, **k: _FakeResponse([{"order_id": 1, "customer": "x", "amount": 1.0}]),
    )
    ingestion.api_load("http://mock/api", "dwh.t", mode="upsert", upsert_key="order_id")
    insert_sql, _ = hook.conn.batches[0]
    assert "ON CONFLICT (order_id) DO UPDATE SET" in insert_sql
    assert "customer = EXCLUDED.customer" in insert_sql
    assert "order_id = EXCLUDED.order_id" not in insert_sql  # 键列不更新


def test_api_load_upsert_requires_key(hook):
    with pytest.raises(ValueError, match="upsert"):
        ingestion.api_load("http://mock/api", "dwh.t", mode="upsert")


def test_api_load_rejects_bad_table(hook):
    with pytest.raises(ValueError):
        ingestion.api_load("http://mock/api", "dwh.t; DROP TABLE x")


def test_api_load_empty_records_skip(hook, monkeypatch):
    import requests as real_requests

    monkeypatch.setattr(real_requests, "request", lambda *a, **k: _FakeResponse({"items": []}))
    assert ingestion.api_load("http://mock/api", "dwh.t", data_path="items") == 0
    assert hook.conn.batches == []


def test_api_load_headers_string_parsed(hook, monkeypatch):
    import requests as real_requests

    seen = {}

    def fake_request(method, url, headers=None, timeout=None):
        seen.update({"method": method, "headers": headers, "timeout": timeout})
        return _FakeResponse([{"order_id": 1, "amount": 1.0}])

    monkeypatch.setattr(real_requests, "request", fake_request)
    ingestion.api_load("http://mock/api", "dwh.t", headers='{"Authorization":"Bearer x"}')
    assert seen["headers"] == {"Authorization": "Bearer x"}


# ---------- file_load ----------


def test_file_load_csv_copy_path(hook, tmp_path):
    csv_file = tmp_path / "items.csv"
    csv_file.write_text(
        "order_id,customer,city,amount\n1,张三,上海,9.9\n2,李四,北京,19.9\n", encoding="utf-8"
    )
    written = ingestion.file_load(str(csv_file), "dwh.t", format="csv")
    assert written == 2
    copy_sql, payload = hook.conn.copies[0]
    assert (
        copy_sql
        == "COPY dwh.t (order_id, customer, city, amount) FROM STDIN WITH (FORMAT csv, DELIMITER ',')"
    )
    assert "1,张三,上海,9.9" in payload
    assert hook.conn.commits == 1


def test_file_load_csv_truncate_first(hook, tmp_path):
    csv_file = tmp_path / "items.csv"
    csv_file.write_text("order_id,amount\n1,9.9\n", encoding="utf-8")
    ingestion.file_load(str(csv_file), "dwh.t", format="csv", mode="truncate")
    assert hook.conn.executed[0][0] == "TRUNCATE TABLE dwh.t"


def test_file_load_csv_upsert_via_temp_table(hook, tmp_path):
    csv_file = tmp_path / "items.csv"
    csv_file.write_text("order_id,customer,amount\n1,张三,9.9\n", encoding="utf-8")
    ingestion.file_load(str(csv_file), "dwh.t", format="csv", mode="upsert", upsert_key="order_id")
    executed_sql = [sql for sql, _ in hook.conn.executed]
    assert executed_sql[0].startswith("CREATE TEMP TABLE _ingest_stage")
    merge_sql = executed_sql[-1]
    assert "ON CONFLICT (order_id) DO UPDATE SET" in merge_sql
    assert hook.conn.copies[0][0].startswith("COPY _ingest_stage")


def test_file_load_json_and_jsonl(hook, tmp_path):
    json_file = tmp_path / "items.json"
    json_file.write_text(json.dumps([{"order_id": 1, "amount": 9.9}, {"order_id": 2, "amount": 1.0}]))
    assert ingestion.file_load(str(json_file), "dwh.t", format="json") == 2

    jsonl_file = tmp_path / "items.jsonl"
    jsonl_file.write_text('{"order_id": 3, "amount": 2.5}\n{"order_id": 4, "amount": 3.5}\n')
    hook.conn.batches.clear()
    assert ingestion.file_load(str(jsonl_file), "dwh.t", format="jsonl") == 2
    # 仅写记录中出现的列（order_id, amount），不做全表列 None 填充
    assert hook.conn.batches[-1][0].startswith("INSERT INTO dwh.t (order_id, amount)")
    assert hook.conn.batches[-1][1] == [[3, 2.5], [4, 3.5]]


def test_file_load_csv_empty_file_skips(hook, tmp_path):
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("order_id,customer\n", encoding="utf-8")
    assert ingestion.file_load(str(csv_file), "dwh.t", format="csv") == 0
    assert hook.conn.copies == []


def test_file_load_s3_uses_minio_client(hook, monkeypatch):
    csv_bytes = io.BytesIO(b"order_id,amount\n1,9.9\n")

    class _FakeS3:
        def get_object(self, Bucket, Key):
            assert (Bucket, Key) == ("datamart-ingest", "items.csv")
            return {"Body": csv_bytes}

    import boto3

    monkeypatch.setattr(boto3, "client", lambda *a, **k: _FakeS3())
    assert ingestion.file_load("s3://datamart-ingest/items.csv", "dwh.t", format="csv") == 1


def test_file_load_rejects_bad_delimiter_and_format(hook, tmp_path):
    csv_file = tmp_path / "items.csv"
    csv_file.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(ValueError):
        ingestion.file_load(str(csv_file), "dwh.t", format="xml")
    with pytest.raises(ValueError):
        ingestion.file_load(str(csv_file), "dwh.t", delimiter=";;")


# ---------- catalog / convert 集成 ----------


def test_catalog_contains_load_nodes_with_param_models():
    from datamart.workflows import catalog

    for node_type in ("api_load", "file_load"):
        spec = catalog.get_node_type(node_type)
        assert spec is not None
        assert spec["parameter_schema"]["type"] == "object"
        assert catalog.PYTHON_CALLABLES[node_type].startswith("ingestion.")


def test_load_nodes_yaml_round_trip():
    from datamart.workflows import convert
    from datamart.workflows.schema import DesignerDag, DesignerEdge, DesignerNode, NodePosition

    dag = DesignerDag(
        dag_id="ingest_demo",
        schedule=None,
        nodes=[
            DesignerNode(
                id="load_csv",
                type="file_load",
                params={"source": "s3://b/k.csv", "target_table": "dwh.t", "mode": "truncate"},
                position=NodePosition(x=0, y=0),
            ),
            DesignerNode(
                id="load_api",
                type="api_load",
                params={
                    "url": "http://mock/api",
                    "target_table": "dwh.t",
                    "mode": "upsert",
                    "upsert_key": "order_id",
                },
                position=NodePosition(x=200, y=0),
            ),
        ],
        edges=[DesignerEdge(upstream="load_csv", downstream="load_api")],
    )
    text = convert.to_yaml_text(dag)
    assert "python_callable: ingestion.file_load" in text
    assert "python_callable: ingestion.api_load" in text
    restored = convert.from_yaml_text(text)
    by_id = {node.id: node for node in restored.nodes}
    assert "format: csv" in text  # 默认值经 field_defaults 合并进 YAML
    assert by_id["load_csv"].params["mode"] == "truncate"
    assert "format" not in by_id["load_csv"].params  # 等于默认值的参数回读时按约定剥离
    assert by_id["load_api"].params["mode"] == "upsert"
    assert by_id["load_api"].params["upsert_key"] == "order_id"


def test_copy_rows_supports_psycopg3_cursor(hook, tmp_path):
    """生产栈实测 worker 拿到的是 psycopg3 Cursor（无 copy_expert），COPY 必须走 copy() 分支。"""
    csv_file = tmp_path / "items.csv"
    csv_file.write_text("order_id,amount\n1,9.9\n", encoding="utf-8")

    class _Copy:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def write(self, payload):
            self.conn.copies.append(("copy()", payload))

    class _Psycopg3Cursor:
        """只有 copy()，没有 copy_expert——正是生产栈的形态。"""

        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            self.conn.executed.append((sql, params))

        def copy(self, sql):
            self.conn.copies.append((sql, None))
            return _Copy(self.conn)

    hook.conn.cursor = lambda: _Psycopg3Cursor(hook.conn)
    written = ingestion.file_load(str(csv_file), "dwh.t", format="csv")
    assert written == 1
    assert hook.conn.copies[0][0] == (
        "COPY dwh.t (order_id, amount) FROM STDIN WITH (FORMAT csv, DELIMITER ',')"
    )
    assert "1,9.9" in hook.conn.copies[1][1]
    assert hook.conn.commits == 1
