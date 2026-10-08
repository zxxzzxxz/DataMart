"""补发已提交但尚未确认发布的资产事件；至少一次投递，下游读取最新已验证快照。"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from alerting import send_failure_alert
from pipeline_tasks import DAILY_SALES_ASSET, acknowledge_publication, load_outbox, publish_outbox

with DAG(
    dag_id="datamart_publish_outbox",
    description="数据库提交后进程中断时，补发资产事件并在成功后确认",
    schedule="* * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    default_args={
        "owner": "datamart",
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "on_failure_callback": send_failure_alert,
    },
    tags=["datamart", "recovery"],
) as dag:
    load = PythonOperator(task_id="load_outbox", python_callable=load_outbox)
    publish = PythonOperator(
        task_id="publish_dataset", python_callable=publish_outbox, outlets=[DAILY_SALES_ASSET]
    )
    acknowledge = PythonOperator(task_id="acknowledge_publication", python_callable=acknowledge_publication)
    load >> publish >> acknowledge
