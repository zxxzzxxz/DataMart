"""增量抽取模板：水位线（watermark）模式。

适用场景：源表带单调递增时间戳列（默认 updated_at）。

标准用法（DAG 任务内）：
    1. cnt, new_wm = extract_incremental(CONN, "ods.orders", "dwh.stg_orders")
       —— 清空 staging，装入 last_watermark 之后的增量，返回 (行数, 新水位线)
    2. 用 staging 数据 merge 到目标层（upsert）
    3. commit_watermark(CONN, "ods.orders", new_wm)
       —— 最后一步才推进水位线：merge 失败时水位线不动，下次自动重抽，保证不丢数

边界语义：
- 空增量：返回 (0, None)，commit_watermark 收到 None 直接跳过
- 同一时刻多条记录：水位线取 max(updated_at)，下次用严格大于比较，不重不漏
"""

import logging
from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

WATERMARK_TABLE = "dwh.etl_watermark"
# 首次抽取的起始水位线（等价于全量）
EPOCH = "1970-01-01 00:00:00+00"


def _fmt(value) -> str:
    # 必须保留微秒：截断到秒会导致水位线永远追不上数据（updated_at 的微秒部分 > 截断水位线）
    return value.strftime("%Y-%m-%d %H:%M:%S.%f%z") if isinstance(value, datetime) else str(value)


def get_watermark(conn_id: str, table_name: str) -> str:
    """读取源表当前水位线；无记录时返回 EPOCH（首次=全量）。"""
    hook = PostgresHook(postgres_conn_id=conn_id)
    row = hook.get_first(
        f"SELECT last_watermark FROM {WATERMARK_TABLE} WHERE table_name = %(t)s",
        parameters={"t": table_name},
    )
    return _fmt(row[0]) if row else EPOCH


def extract_incremental(
    conn_id: str,
    source_table: str,
    staging_table: str,
    wm_col: str = "updated_at",
) -> tuple[int, str | None]:
    """抽取增量到 staging 表（staging = 源表结构 + loaded_at）。

    返回 (抽取行数, 新水位线)；空增量返回 (0, None)。
    """
    hook = PostgresHook(postgres_conn_id=conn_id)
    last_wm = get_watermark(conn_id, source_table)
    hook.run(f"TRUNCATE TABLE {staging_table}")
    hook.run(
        f"INSERT INTO {staging_table} SELECT *, now() FROM {source_table} "
        f"WHERE {wm_col} > %(wm)s::timestamptz",
        parameters={"wm": last_wm},
    )
    cnt = hook.get_first(f"SELECT count(*) FROM {staging_table}")[0]
    new_wm = None
    if cnt:
        new_wm = _fmt(hook.get_first(f"SELECT max({wm_col}) FROM {staging_table}")[0])
    log.info(
        "增量抽取 %s -> %s: %d 行（水位线 %s -> %s）",
        source_table,
        staging_table,
        cnt,
        last_wm,
        new_wm or last_wm,
    )
    return cnt, new_wm


def commit_watermark(conn_id: str, table_name: str, new_watermark: str | None) -> None:
    """推进水位线。new_watermark 为 None（空增量）时不动作。"""
    if not new_watermark:
        log.info("无增量，水位线不变: %s", table_name)
        return
    hook = PostgresHook(postgres_conn_id=conn_id)
    hook.run(
        f"INSERT INTO {WATERMARK_TABLE} (table_name, last_watermark, updated_at) "
        f"VALUES (%(t)s, %(wm)s::timestamptz, now()) "
        f"ON CONFLICT (table_name) DO UPDATE SET "
        f"last_watermark = EXCLUDED.last_watermark, updated_at = now()",
        parameters={"t": table_name, "wm": new_watermark},
    )
    log.info("水位线已推进: %s -> %s", table_name, new_watermark)
