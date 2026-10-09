"""Airflow adapters for the transactional warehouse engine and its durable outbox."""

import logging
import os

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Asset, Variable, get_current_context
from airflow.sdk.exceptions import AirflowSkipException
from datamart.adapters.metrics import push_metrics
from datamart.quality import CheckResult, Drift, Report
from warehouse_pipeline import ASSET_URI, DEFAULT_CONFIG_DIR, PipelineQualityError, WarehousePipeline

log = logging.getLogger(__name__)
DAILY_SALES_ASSET = Asset(uri=ASSET_URI)


def pipeline():
    # 写者串行等待与单语句超时的容量参数：默认 30min（须大于 P99 批次时长），可按容量调优
    return WarehousePipeline(
        PostgresHook(postgres_conn_id="warehouse").get_conn,
        os.environ.get("DATAMART_DQ_CONFIG_DIR", DEFAULT_CONFIG_DIR),
        lock_timeout_ms=int(os.environ.get("DATAMART_LOCK_TIMEOUT_MS", 30 * 60 * 1000)),
        statement_timeout_ms=int(os.environ.get("DATAMART_STATEMENT_TIMEOUT_MS", 30 * 60 * 1000)),
    )


def apply_batch(mode: str):
    context = get_current_context()
    arguments = {}
    if mode == "backfill":
        conf = context["dag_run"].conf or {}
        arguments = {"start_date": conf.get("start_date"), "end_date": conf.get("end_date")}
    try:
        result = pipeline().run(f"{context['dag'].dag_id}/{context['run_id']}", mode, **arguments)
    except PipelineQualityError as exc:
        push_quality_metrics(exc.reports)
        raise
    push_quality_metrics(result["quality"])
    log.info("Validated warehouse batch: %s", result)
    return result


def publish_batch():
    result = get_current_context()["ti"].xcom_pull(task_ids="apply_batch")
    # 空增量（无源变更且未触发全量对账）不发布：提交时事件已确认为送达，acknowledge 随 skip，
    # 下游审计 DAG 不再每 15 分钟空转
    if result["mode"] == "incremental" and not result["full_reconcile"] and result["source_changes"] == 0:
        raise AirflowSkipException("No source changes in this incremental batch")
    return _emit([result])


def load_outbox():
    results = pipeline().pending_events()
    if not results:
        raise AirflowSkipException("No committed batches waiting for asset publication")
    return results


def publish_outbox():
    return _emit(get_current_context()["ti"].xcom_pull(task_ids="load_outbox"))


def _emit(results):
    if not results:
        raise AirflowSkipException("No validated batch to publish")
    batch_ids = [result["batch_id"] for result in results]
    get_current_context()["outlet_events"][DAILY_SALES_ASSET].extra = {
        "batch_ids": batch_ids,
        "revision": max(result["revision"] for result in results),
        "validated": True,
    }
    return batch_ids


def acknowledge_publication():
    batch_ids = get_current_context()["ti"].xcom_pull(task_ids="publish_dataset")
    return pipeline().acknowledge_events(batch_ids)


def audit_published():
    result = pipeline().audit()
    push_quality_metrics(result["quality"])
    log.info("Published warehouse snapshot: %s", result)
    return result


def push_quality_metrics(reports):
    """Network I/O happens after commit/rollback and never changes the batch result."""
    if not reports:
        return
    try:
        url = Variable.get("datamart_dq_pushgateway", default="http://pushgateway:9091")
        for item in reports:
            report = Report(
                table=item["table"],
                results=[CheckResult(**result) for result in item["results"]],
                drift=Drift(**item["drift"]) if item["drift"] else None,
            )
            push_metrics(url, report)
    except Exception:
        log.warning("DQ metrics unavailable; warehouse result is unchanged", exc_info=True)
