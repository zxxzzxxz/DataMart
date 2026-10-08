"""从已提交明细按日期区间补跑，同一事务重建汇总和日报；单次最多 31 天。"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from alerting import send_failure_alert
from pipeline_tasks import DAILY_SALES_ASSET, acknowledge_publication, apply_batch, publish_batch

with DAG(
    dag_id="datamart_backfill",
    description="事务补跑：conf 传 {start_date, end_date}，质检通过才提交和发布",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "datamart",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": send_failure_alert,
    },
    tags=["datamart", "backfill"],
) as dag:
    apply = PythonOperator(task_id="apply_batch", python_callable=apply_batch, op_kwargs={"mode": "backfill"})
    publish = PythonOperator(
        task_id="publish_dataset", python_callable=publish_batch, outlets=[DAILY_SALES_ASSET]
    )
    acknowledge = PythonOperator(task_id="acknowledge_publication", python_callable=acknowledge_publication)
    apply >> publish >> acknowledge
