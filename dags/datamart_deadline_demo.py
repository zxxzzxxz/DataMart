"""
Deadline 告警演示 DAG：任务运行时间超过截止时间，验证"超时未完成 -> 告警"链路。

- slow_task：休眠 150 秒后成功
- deadline：入队 1 分钟后未完成即触发告警（在 slow_task 运行期间触发）
- 预期：运行约 1 分钟时收到 Deadline 告警，约 2.5 分钟时任务正常成功

此 DAG 不设定调度（schedule=None），仅供手动触发验收 Deadline 链路。
"""

import logging
import time
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, SyncCallback

log = logging.getLogger(__name__)


def slow_task():
    log.info("模拟长任务：休眠 150 秒")
    time.sleep(150)
    log.info("长任务完成")


with DAG(
    dag_id="datamart_deadline_demo",
    description="Deadline 演示：任务运行超过 1 分钟触发超时告警",
    schedule=None,  # 仅手动触发
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    default_args={
        "owner": "datamart",
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
    },
    tags=["datamart", "demo"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=1),
        callback=SyncCallback("alerting.send_deadline_alert"),
        name="demo_deadline",
    ),
) as dag:
    PythonOperator(task_id="slow_task", python_callable=slow_task)
