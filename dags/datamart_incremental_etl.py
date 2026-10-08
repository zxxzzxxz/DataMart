"""增量消费 ODS 事务变更日志：支持删除、迟到提交、日期修正和用户维度变更。"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, SyncCallback
from alerting import send_failure_alert
from pipeline_tasks import DAILY_SALES_ASSET, acknowledge_publication, apply_batch, publish_batch

with DAG(
    dag_id="datamart_incremental_etl",
    description="事务日志增量：加工、质检、日报与变更确认原子提交",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "datamart",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "on_failure_callback": send_failure_alert,
    },
    tags=["datamart", "etl", "incremental"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=30),
        callback=SyncCallback("alerting.send_deadline_alert"),
        name="incremental_run_timeout",
    ),
) as dag:
    start = EmptyOperator(task_id="start")
    apply = PythonOperator(
        task_id="apply_batch", python_callable=apply_batch, op_kwargs={"mode": "incremental"}
    )
    publish = PythonOperator(
        task_id="publish_dataset", python_callable=publish_batch, outlets=[DAILY_SALES_ASSET]
    )
    acknowledge = PythonOperator(task_id="acknowledge_publication", python_callable=acknowledge_publication)
    end = EmptyOperator(task_id="end")
    start >> apply >> publish >> acknowledge >> end
