"""DataMart 通用告警插件：任务失败回调 + Deadline 超时/漏跑回调。

使用方式：
    default_args = {"on_failure_callback": send_failure_alert}
    DAG(deadline=DeadlineAlert(..., callback=SyncCallback("alerting.send_deadline_alert")))

告警通道：读取 Airflow Variable `datamart_alert_webhook`（Admin -> Variables 配置）。
未配置时降级为日志告警；配置后向 webhook POST JSON（默认指向内置告警中心，
生产环境换成企业微信/飞书/钉钉机器人地址即可）。
"""

import json
import logging
import urllib.request

from airflow.sdk import Variable

log = logging.getLogger(__name__)


def _post_webhook(message: str) -> None:
    """向告警通道发送消息；未配置通道时降级为日志。"""
    webhook = Variable.get("datamart_alert_webhook", default="")
    if not webhook:
        log.warning("%s（未配置 datamart_alert_webhook，仅记录日志）", message)
        return
    try:
        req = urllib.request.Request(
            webhook,
            data=json.dumps({"msgtype": "text", "text": {"content": message}}).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        log.info("告警已发送: %s", message)
    except Exception:
        log.exception("告警发送失败: %s", message)


def send_failure_alert(context) -> None:
    """任务最终失败（重试耗尽）时的回调。"""
    ti = context["task_instance"]
    _post_webhook(
        f"[DataMart 告警-任务失败] DAG={ti.dag_id} | Task={ti.task_id} | "
        f"Run={context['run_id']} | 状态=FAILED | 日志={ti.log_url}"
    )


def send_deadline_alert(context=None, **kwargs) -> None:
    """Deadline 告警：DAG Run 超过截止时间仍未完成（超时/漏跑）。

    由 DeadlineAlert + SyncCallback 调用，context 形态随版本不同，做防御性取值。
    """
    ctx = context or {}
    dag_run = ctx.get("dag_run") or {}
    if isinstance(dag_run, dict):
        # 调度器传入的是 DAGRunResponse 序列化后的 dict（run id 字段名为 dag_run_id）
        dag_id = dag_run.get("dag_id")
        run_id = dag_run.get("dag_run_id") or dag_run.get("run_id")
    else:
        dag_id = getattr(dag_run, "dag_id", None)
        run_id = getattr(dag_run, "run_id", None)
    dag_id = dag_id or ctx.get("dag_id") or "unknown"
    run_id = run_id or ctx.get("run_id") or "unknown"
    deadline_time = (ctx.get("deadline") or {}).get("deadline_time", "unknown")
    _post_webhook(
        f"[DataMart 告警-超时漏跑] DAG={dag_id} | Run={run_id} | "
        f"截止={deadline_time} 仍未完成，请检查调度器与任务状态"
    )
