"""
失败路径演示 DAG：验证"重试耗尽 -> 任务失败 -> 告警回调 -> 下游 all_done 通知"完整链路。

- always_fail：必失败任务，重试 2 次（间隔 10 秒）后进入 failed 状态，触发 on_failure_callback 告警
- notify_done：trigger_rule=all_done，无论上游成败都会执行（模拟"失败也要通知值班人"）
- 注意：DAG Run 状态由叶子任务决定。always_fail 的失败被 notify_done 吸收后，
  Run 整体显示 success，但失败任务在 Grid 视图中仍为红色、告警照常触发；
  如需 Run 整体标红，请将通知节点改为非叶子（即再加一个下游任务）。

此 DAG 不设定调度（schedule=None），仅供手动触发验收失败处理链路。
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, get_current_context
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, SyncCallback
from alerting import send_failure_alert

log = logging.getLogger(__name__)


def always_fail():
    ti = get_current_context()["ti"]
    raise RuntimeError(f"模拟任务失败（第 {ti.try_number} 次尝试），用于验收重试与告警链路")


def notify():
    log.info("notify_done 已执行：无论上游成败都会运行（trigger_rule=all_done）")


with DAG(
    dag_id="datamart_failure_demo",
    description="失败路径演示：重试耗尽 -> 告警 -> all_done 通知",
    schedule=None,  # 仅手动触发
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    default_args={
        "owner": "datamart",
        "retries": 2,
        "retry_delay": timedelta(seconds=10),
        "on_failure_callback": send_failure_alert,
    },
    tags=["datamart", "demo"],
    # Deadline 演示：入队 1 分钟后若 Run 仍未成功结束即触发告警（用于验收 Deadline 链路）
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(minutes=1),
        callback=SyncCallback("alerting.send_deadline_alert"),
        name="demo_deadline",
    ),
) as dag:
    fail = PythonOperator(task_id="always_fail", python_callable=always_fail)
    notify_done = PythonOperator(
        task_id="notify_done",
        python_callable=notify,
        trigger_rule="all_done",
    )

    fail >> notify_done
