"""Independent PostgreSQL assertions used by acceptance suites."""

from collections import defaultdict
from decimal import Decimal


def query(connect, statement, parameters=None):
    conn = connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(statement, parameters)
            return cur.fetchall() if cur.description else []
    finally:
        conn.close()


def snapshot(connect):
    tables = {
        "dwh.dim_users": "user_id",
        "dwh.fact_orders": "order_id",
        "dm.daily_city_sales": "stat_date, city",
        "dm.daily_summary_report": "stat_date",
        "dwh.etl_watermark": "table_name",
        "dwh.etl_batches": "batch_id",
        "dwh.etl_pipeline_state": "pipeline",
    }
    return {
        table: query(connect, f"SELECT * FROM {table} ORDER BY {order}") for table, order in tables.items()
    }


def assert_matches_source(connect):
    expected = query(
        connect,
        "SELECT o.order_id, o.user_id, u.user_name, u.city, o.amount, o.status, o.order_date "
        "FROM ods.orders o JOIN ods.users u USING (user_id) ORDER BY o.order_id",
    )
    actual = query(
        connect,
        "SELECT order_id, user_id, user_name, city, amount, status, order_date "
        "FROM dwh.fact_orders ORDER BY order_id",
    )
    assert actual == expected
    assert query(
        connect, "SELECT user_id, user_name, city, updated_at FROM dwh.dim_users ORDER BY user_id"
    ) == query(connect, "SELECT user_id, user_name, city, updated_at FROM ods.users ORDER BY user_id")
    # Independent Python oracle, not a repeat of the engine's SQL aggregation.
    cities = defaultdict(lambda: [0, Decimal(0)])
    for _oid, _uid, _name, city, amount, status, day in expected:
        if status == "paid":
            cities[(day, city)][0] += 1
            cities[(day, city)][1] += amount
    assert query(connect, "SELECT * FROM dm.daily_city_sales ORDER BY stat_date, city") == [
        (day, city, values[0], values[1]) for (day, city), values in sorted(cities.items())
    ]
    daily = defaultdict(lambda: [0, 0, Decimal(0)])
    for (day, _city), (count, amount) in cities.items():
        daily[day][0] += 1
        daily[day][1] += count
        daily[day][2] += amount
    assert query(
        connect,
        "SELECT stat_date, city_cnt, order_cnt, total_amount FROM dm.daily_summary_report ORDER BY stat_date",
    ) == [(day, *values) for day, values in sorted(daily.items())]
