"""消费已验证资产：日报已随 ETL 原子提交，此 DAG 在只读快照中核验并展示。"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from alerting import send_failure_alert
from pipeline_tasks import DAILY_SALES_ASSET, audit_published

with DAG(
    dag_id="datamart_daily_report",
    description="读取已经原子提交并通过质检的日报快照",
    schedule=[DAILY_SALES_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "datamart",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": send_failure_alert,
    },
    tags=["datamart", "report"],
) as dag:
    PythonOperator(task_id="read_validated_report", python_callable=audit_published)
