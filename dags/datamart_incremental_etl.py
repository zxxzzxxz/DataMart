"""
DataMart 增量 ETL DAG：水位线驱动的增量流水线（每 15 分钟）

与全量主 DAG（datamart_daily_etl）互补：
- extract 增量抽取：ods -> staging（plugins/incremental.py 水位线模板，空增量安全跳过）
- merge：staging upsert 到 dim_users / fact_orders（变更也会同步）
- refresh：只重建本批增量影响的业务日期（非全量刷新）
- 对账质检：staging 行数与 merge 结果一致性校验
- 水位线在 merge 成功后才推进：失败自动重抽，保证不丢数
- 成功后发布与主 DAG 相同的数据资产，报表 DAG 同样被联动触发
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset, get_current_context, task_group
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, SyncCallback
from alerting import send_failure_alert
from incremental import commit_watermark, extract_incremental

log = logging.getLogger(__name__)

CONN_ID = "warehouse"
DS_DAILY_CITY_SALES = Asset(uri="postgres://warehouse/warehouse/dm/daily_city_sales")

default_args = {
    "owner": "datamart",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": send_failure_alert,
}


def extract_orders():
    return extract_incremental(CONN_ID, "ods.orders", "dwh.stg_orders")


def extract_users():
    return extract_incremental(CONN_ID, "ods.users", "dwh.stg_users")


def merge():
    """staging -> 目标层 upsert（先维表后事实表）。"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(
        """
        INSERT INTO dwh.dim_users (user_id, user_name, city, updated_at)
        SELECT user_id, user_name, city, updated_at FROM dwh.stg_users
        ON CONFLICT (user_id) DO UPDATE SET
            user_name = EXCLUDED.user_name, city = EXCLUDED.city, updated_at = EXCLUDED.updated_at
        """
    )
    hook.run(
        """
        INSERT INTO dwh.fact_orders (order_id, user_id, user_name, city, amount, status, order_date, etl_date)
        SELECT o.order_id, o.user_id, u.user_name, u.city, o.amount, o.status, o.order_date, CURRENT_DATE
        FROM dwh.stg_orders o
        JOIN dwh.dim_users u ON u.user_id = o.user_id
        ON CONFLICT (order_id) DO UPDATE SET
            amount = EXCLUDED.amount, status = EXCLUDED.status,
            user_name = EXCLUDED.user_name, city = EXCLUDED.city, etl_date = EXCLUDED.etl_date
        """
    )
    log.info(
        "merge 完成：dim_users <- %d 行，fact_orders <- %d 行",
        hook.get_first("SELECT count(*) FROM dwh.stg_users")[0],
        hook.get_first("SELECT count(*) FROM dwh.stg_orders")[0],
    )


def refresh_mart():
    """只重建本批增量涉及的业务日期（增量刷新集市层）。"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    dates = [r[0] for r in hook.get_records("SELECT DISTINCT order_date FROM dwh.stg_orders")]
    if not dates:
        log.info("空增量，集市层无需刷新")
        return
    for d in dates:
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
    log.info("集市层增量刷新完成: %s", [str(d) for d in dates])


def quality_check():
    """对账：staging 中的订单必须全部出现在事实表中。"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    stg = hook.get_first("SELECT count(*) FROM dwh.stg_orders")[0]
    merged = hook.get_first(
        "SELECT count(*) FROM dwh.stg_orders s JOIN dwh.fact_orders f ON f.order_id = s.order_id"
    )[0]
    dm = hook.get_first("SELECT count(*) FROM dm.daily_city_sales")[0]
    log.info("对账: staging=%d, merged=%d, dm=%d", stg, merged, dm)
    if stg != merged:
        raise ValueError(f"merge 丢数：staging={stg} vs fact={merged}")
    if dm == 0:
        raise ValueError("dm.daily_city_sales 无数据")
    log.info("质量校验通过")


def commit_watermarks():
    """merge 成功后推进两表水位线（从 XCom 取回各抽取任务返回的新水位线）。"""
    ti = get_current_context()["ti"]
    orders_cnt, orders_wm = ti.xcom_pull(task_ids="extract.extract_orders_incr")
    users_cnt, users_wm = ti.xcom_pull(task_ids="extract.extract_users_incr")
    log.info("提交水位线: orders=%s(%d行), users=%s(%d行)", orders_wm, orders_cnt, users_wm, users_cnt)
    commit_watermark(CONN_ID, "ods.orders", orders_wm)
    commit_watermark(CONN_ID, "ods.users", users_wm)


with DAG(
    dag_id="datamart_incremental_etl",
    description="DataMart 增量 ETL：水位线驱动 ods -> staging -> dwh -> dm（每 15 分钟）",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,  # staging 表是共享暂存区，禁止并发运行
    default_args=default_args,
    tags=["datamart", "etl", "incremental"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=30),
        callback=SyncCallback("alerting.send_deadline_alert"),
        name="incremental_run_timeout",
    ),
) as dag:
    start = EmptyOperator(task_id="start")

    @task_group(group_id="extract")
    def extract_group():
        PythonOperator(task_id="extract_orders_incr", python_callable=extract_orders)
        PythonOperator(task_id="extract_users_incr", python_callable=extract_users)

    merge_task = PythonOperator(task_id="merge_to_dwh", python_callable=merge)
    refresh = PythonOperator(task_id="refresh_mart", python_callable=refresh_mart)
    check = PythonOperator(task_id="quality_check", python_callable=quality_check)
    commit = PythonOperator(task_id="commit_watermarks", python_callable=commit_watermarks)
    publish = EmptyOperator(task_id="publish_dataset", outlets=[DS_DAILY_CITY_SALES])
    notify = EmptyOperator(task_id="notify_done", trigger_rule="all_done")
    end = EmptyOperator(task_id="end")

    start >> extract_group() >> merge_task >> refresh >> check >> commit >> publish >> end
    check >> notify
