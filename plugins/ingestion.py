"""数据接入算子：接口入库（api_load）与文件入库（file_load）。

供可视化设计器节点经 PythonOperator 以字符串路径调用（与 alerting._post_webhook 同模式）：
- api_load：HTTP 拉取 JSON → 点分路径抽取记录数组 → 批量写入目标表
- file_load：本地路径或 s3://（默认栈内 MinIO）读取 CSV/JSON/JSONL → COPY/批量写入目标表

约定：
- 连接一律走 PostgresHook(conn_id)（默认 warehouse）；目标表须已存在（结构由 Flyway 管理）
- JSON 记录按键与表列名匹配，不匹配的键忽略；无任何匹配列时整批跳过（返回 0）
- 表名/列名/upsert 键全部经标识符白名单校验，防 SQL 注入
- upsert 语义 = INSERT ... ON CONFLICT (upsert_key) DO UPDATE（非键列全更新）
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re

log = logging.getLogger(__name__)

_IDENT_RE = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*")
_MODES = ("append", "truncate", "upsert")


# ---------- 校验与小工具 ----------


def _ident(name: str, what: str = "标识符") -> str:
    if not _IDENT_RE.fullmatch(name or ""):
        raise ValueError(f"非法{what}：{name!r}")
    return name


def _table_name(name: str) -> str:
    """允许 schema.table 或裸表名；各段必须是合法标识符。"""
    parts = (name or "").split(".")
    if len(parts) > 2 or not all(parts):
        raise ValueError(f"非法目标表：{name!r}")
    for part in parts:
        _ident(part, "表名")
    return ".".join(parts)


def _check_mode(mode: str, upsert_key: str | None) -> str:
    if mode not in _MODES:
        raise ValueError(f"mode 必须是 {_MODES} 之一：{mode!r}")
    if mode == "upsert":
        _ident(upsert_key or "", "upsert 键")
    return mode


def _delimiter(value: str) -> str:
    if not isinstance(value, str) or len(value) != 1 or value in "'\\\n\r":
        raise ValueError(f"非法分隔符：{value!r}")
    return value


def _hook(conn_id: str):
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id=conn_id)


def _table_columns(hook, table: str) -> list[str]:
    schema, _, bare = table.partition(".")
    if not bare:
        schema, bare = "public", schema
    rows = hook.get_records(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, bare),
    )
    if not rows:
        raise ValueError(f"目标表不存在或无列：{table}")
    return [row[0] for row in rows]


def _resolve_path(payload, path: str | None):
    """按点分路径（如 data.items）从 JSON 提取记录数组；path 为空时根即数组或单对象。"""
    current = payload
    if path:
        for key in path.split("."):
            if not isinstance(current, dict) or key not in current:
                raise ValueError(f"JSON 路径 '{path}' 不存在（断在 '{key}'）")
            current = current[key]
    if isinstance(current, list):
        return current
    if isinstance(current, dict):
        return [current]
    raise ValueError(f"JSON 路径 '{path or '<root>'}' 的值不是对象或数组：{type(current).__name__}")


def _records_to_rows(records) -> list[dict]:
    rows = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"第 {index + 1} 条记录不是 JSON 对象：{type(record).__name__}")
        rows.append(record)
    return rows


def _s3_get(source: str) -> bytes:
    """读取 s3://bucket/key 对象（默认栈内 MinIO；凭证与端点取环境变量）。"""
    bucket, _, key = source[len("s3://") :].partition("/")
    if not bucket or not key:
        raise ValueError(f"s3 来源必须是 s3://<bucket>/<key>：{source!r}")
    import boto3  # 延迟导入：不走 s3 的节点不强求 boto3

    client = boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin123"),
    )
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def _read_source(source: str) -> bytes:
    if source.startswith("s3://"):
        return _s3_get(source)
    with open(source, "rb") as handle:
        return handle.read()


# ---------- 写入 ----------


def _write_records(hook, table: str, records: list[dict], mode: str, upsert_key: str | None) -> int:
    if not records:
        log.info("无可写入记录（%s），整批跳过", table)
        return 0
    columns = _table_columns(hook, table)
    usable = [column for column in columns if any(column in record for record in records)]
    if not usable:
        log.info("记录键与目标表 %s 的列无任何匹配，整批跳过", table)
        return 0
    col_sql = ", ".join(_ident(column, "列名") for column in usable)
    placeholders = ", ".join(["%s"] * len(usable))
    insert_sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"
    if mode == "upsert":
        update_cols = [column for column in usable if column != upsert_key]
        if update_cols:
            assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_cols)
            insert_sql += f" ON CONFLICT ({upsert_key}) DO UPDATE SET {assignments}"
        else:
            insert_sql += f" ON CONFLICT ({upsert_key}) DO NOTHING"
    values = [[record.get(column) for column in usable] for record in records]
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            if mode == "truncate":
                cur.execute(f"TRUNCATE TABLE {table}")
            cur.executemany(insert_sql, values)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info("已写入 %s：%d 行（列 %s，模式 %s）", table, len(values), col_sql, mode)
    return len(values)


def _write_csv(
    hook, table: str, raw: bytes, delimiter: str, header: bool, mode: str, upsert_key: str | None
) -> int:
    reader = csv.reader(io.TextIOWrapper(io.BytesIO(raw), encoding="utf-8-sig"), delimiter=delimiter)
    rows = list(reader)
    if not rows:
        log.info("CSV 为空（%s），整批跳过", table)
        return 0
    if header:
        columns = [_ident(name.strip(), "CSV 列名") for name in rows[0]]
        data_rows = rows[1:]
    else:
        columns = _table_columns(hook, table)[: len(rows[0])]
        data_rows = rows
    if not data_rows:
        log.info("CSV 仅含表头（%s），整批跳过", table)
        return 0
    col_sql = ", ".join(columns)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            if mode == "truncate":
                cur.execute(f"TRUNCATE TABLE {table}")
            if mode == "upsert":
                # COPY 不支持 ON CONFLICT：先入临时表再 merge
                cur.execute(f"CREATE TEMP TABLE _ingest_stage (LIKE {table}) ON COMMIT DROP")
                _copy_rows(cur, "_ingest_stage", col_sql, data_rows, delimiter)
                update_cols = [column for column in columns if column != upsert_key]
                action = (
                    f"DO UPDATE SET {', '.join(f'{column} = EXCLUDED.{column}' for column in update_cols)}"
                    if update_cols
                    else "DO NOTHING"
                )
                cur.execute(
                    f"INSERT INTO {table} ({col_sql}) SELECT {col_sql} FROM _ingest_stage "
                    f"ON CONFLICT ({upsert_key}) {action}"
                )
            else:
                _copy_rows(cur, table, col_sql, data_rows, delimiter)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    log.info("已写入 %s：%d 行（COPY，模式 %s）", table, len(data_rows), mode)
    return len(data_rows)


def _copy_rows(cur, table: str, col_sql: str, data_rows: list[list[str]], delimiter: str) -> None:
    sql = f"COPY {table} ({col_sql}) FROM STDIN WITH (FORMAT csv, DELIMITER '{delimiter}')"
    with io.StringIO() as buffer:
        csv.writer(buffer, delimiter=delimiter).writerows(data_rows)
        payload = buffer.getvalue()
    if hasattr(cur, "copy_expert"):
        # psycopg2
        with io.StringIO(payload) as stream:
            cur.copy_expert(sql, stream)
    else:
        # psycopg3（生产栈实测的 Cursor 形态，无 copy_expert）
        with cur.copy(sql) as copy:
            copy.write(payload)


# ---------- 算子入口（设计器节点经 PythonOperator 调用） ----------


def api_load(
    url,
    target_table,
    method="GET",
    headers=None,
    data_path=None,
    mode="append",
    upsert_key=None,
    conn_id="warehouse",
    timeout=30,
):
    """接口入库：HTTP 拉取 JSON → 抽取记录数组 → 写入目标表。返回写入行数。"""
    import requests

    _table_name(target_table)
    _check_mode(mode, upsert_key)
    method = (method or "GET").upper()
    if method not in ("GET", "POST"):
        raise ValueError(f"api_load 仅支持 GET/POST：{method!r}")
    if isinstance(headers, str):
        headers = json.loads(headers) if headers.strip() else None
    response = requests.request(method, url, headers=headers, timeout=timeout)
    response.raise_for_status()  # 4xx/5xx 一律判失败（裸 HttpOperator 无状态码检查的教训）
    records = _records_to_rows(_resolve_path(response.json(), data_path))
    return _write_records(_hook(conn_id), _table_name(target_table), records, mode, upsert_key)


def file_load(
    source,
    target_table,
    format="csv",
    mode="append",
    upsert_key=None,
    delimiter=",",
    header=True,
    conn_id="warehouse",
):
    """文件入库：本地路径或 s3:// 读取 CSV/JSON/JSONL → 写入目标表。返回写入行数。"""
    _table_name(target_table)
    _check_mode(mode, upsert_key)
    _delimiter(delimiter)
    if format not in ("csv", "json", "jsonl"):
        raise ValueError(f"format 必须是 csv/json/jsonl 之一：{format!r}")
    raw = _read_source(source)
    if format == "csv":
        return _write_csv(
            _hook(conn_id), _table_name(target_table), raw, delimiter, bool(header), mode, upsert_key
        )
    text = raw.decode("utf-8-sig")
    if format == "json":
        records = _resolve_path(json.loads(text), None)
    else:
        records = [json.loads(line) for line in text.splitlines() if line.strip()]
    return _write_records(
        _hook(conn_id), _table_name(target_table), _records_to_rows(records), mode, upsert_key
    )
