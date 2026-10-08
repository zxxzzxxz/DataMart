"""每日全量校准：一个事务完成明细、汇总、日报、质检与变更确认，再发布资产。"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, SyncCallback
from alerting import send_failure_alert
from pipeline_tasks import DAILY_SALES_ASSET, acknowledge_publication, apply_batch, publish_batch

with DAG(
    dag_id="datamart_daily_etl",
    description="每日全量校准：原子加工与质检提交，成功后发布数据资产",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "datamart",
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "on_failure_callback": send_failure_alert,
    },
    tags=["datamart", "etl"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(hours=1),
        callback=SyncCallback("alerting.send_deadline_alert"),
        name="etl_run_timeout",
    ),
) as dag:
    start = EmptyOperator(task_id="start")
    apply = PythonOperator(task_id="apply_batch", python_callable=apply_batch, op_kwargs={"mode": "full"})
    publish = PythonOperator(
        task_id="publish_dataset", python_callable=publish_batch, outlets=[DAILY_SALES_ASSET]
    )
    acknowledge = PythonOperator(task_id="acknowledge_publication", python_callable=acknowledge_publication)
    end = EmptyOperator(task_id="end")
    start >> apply >> publish >> acknowledge >> end
