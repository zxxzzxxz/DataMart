"""
DataMart 区间补跑 DAG：按日期区间重建集市层（参数化手动触发）。

触发方式（UI -> Trigger DAG w/ JSON 或 CLI --conf）：
    {"start_date": "2026-09-06", "end_date": "2026-09-07"}

行为：对区间内每个业务日期，从 dwh.fact_orders 重建 dm.daily_city_sales（先删后插，幂等），
最后刷新 dm.daily_summary_report 日报。

典型场景：源数据回溯修正后，重算受影响日期的集市层。
"""

import logging
from datetime import date, timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context
from alerting import send_failure_alert

log = logging.getLogger(__name__)

CONN_ID = "warehouse"


def _parse_date(value: str, name: str) -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as e:
        raise ValueError(f"参数 {name} 格式非法（应为 YYYY-MM-DD）: {value!r}") from e


def backfill_range():
    """从 dag_run.conf 读取 start_date/end_date，逐日重建 dm.daily_city_sales。"""
    conf = get_current_context()["dag_run"].conf or {}
    start = _parse_date(conf.get("start_date"), "start_date")
    end = _parse_date(conf.get("end_date"), "end_date")
    if start > end:
        raise ValueError(f"start_date({start}) 不能晚于 end_date({end})")

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    days = (end - start).days + 1
    if days > 31:
        raise ValueError(f"单次补跑区间不能超过 31 天（当前 {days} 天），请分段执行")

    d = start
    while d <= end:
        hook.run("DELETE FROM dm.daily_city_sales WHERE stat_date = %(d)s", parameters={"d": d})
        hook.run(
            """
            INSERT INTO dm.daily_city_sales (stat_date, city, order_cnt, total_amount)
            SELECT order_date, city, count(*), sum(amount)
            FROM dwh.fact_orders
            WHERE order_date = %(d)s AND status = 'paid'
            GROUP BY order_date, city
            """,
            parameters={"d": d},
        )
        log.info("已重建: %s", d)
        d += timedelta(days=1)
    log.info("补跑完成: %s ~ %s（共 %d 天）", start, end, days)
    return days


def refresh_report():
    """重建完成后刷新日报（与 datamart_daily_report 相同逻辑）。"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(
        "DELETE FROM dm.daily_summary_report "
        "WHERE stat_date = (SELECT max(stat_date) FROM dm.daily_city_sales)"
    )
    hook.run(
        """
        INSERT INTO dm.daily_summary_report (stat_date, city_cnt, order_cnt, total_amount)
        SELECT stat_date, count(*), sum(order_cnt), sum(total_amount)
        FROM dm.daily_city_sales
        WHERE stat_date = (SELECT max(stat_date) FROM dm.daily_city_sales)
        GROUP BY stat_date
        """
    )
    log.info("日报已刷新")


with DAG(
    dag_id="datamart_backfill",
    description="区间补跑：conf 传 {start_date, end_date} 重建集市层（单次不超过 31 天）",
    schedule=None,  # 仅手动触发（必须带 conf）
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    default_args={
        "owner": "datamart",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": send_failure_alert,
    },
    tags=["datamart", "backfill"],
) as dag:
    backfill = PythonOperator(task_id="backfill_range", python_callable=backfill_range)
    report = PythonOperator(task_id="refresh_report", python_callable=refresh_report)

    backfill >> report
