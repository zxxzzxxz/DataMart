"""对已验证资产做只读审计；发布前门禁由 warehouse_pipeline 在加工事务内完成。"""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset
from alerting import send_failure_alert
from pipeline_tasks import DAILY_SALES_ASSET, audit_published

DS_DQ_REPORT = Asset(uri="datamart://data_quality/report")

with DAG(
    dag_id="datamart_data_quality",
    description="已发布数据的只读质检审计，不移动或删除已发布明细",
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
    tags=["datamart", "dq"],
) as dag:
    PythonOperator(task_id="audit_published", python_callable=audit_published, outlets=[DS_DQ_REPORT])
