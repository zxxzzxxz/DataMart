"""Monitoring transport; no application transaction is owned here."""

import base64
import logging
import urllib.request

from datamart.quality import Report

log = logging.getLogger(__name__)


def _pushgateway_url(base_url: str, table: str) -> str:
    """table 含点号，不能直接做分组标签值路径段，按 Pushgateway 约定用 base64(urlsafe) 分组键。"""
    b64 = base64.urlsafe_b64encode(table.encode("utf-8")).decode("ascii")
    return f"{base_url.rstrip('/')}/metrics/job/datamart_dq/table@base64/{b64}"


def push_metrics(base_url: str, report: Report) -> None:
    """推送 DQ 指标；失败仅告警，不影响质检结论。"""
    drift_flag = int(any(r.name == "schema_drift" and not r.passed for r in report.results))
    body = (
        f"datamart_dq_score {report.score:.4f}\n"
        f"datamart_dq_checks_failed {report.failed_checks}\n"
        f"datamart_dq_schema_drift {drift_flag}\n"
    )
    try:
        req = urllib.request.Request(
            _pushgateway_url(base_url, report.table),
            data=body.encode("utf-8"),
            headers={"Content-Type": "text/plain"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
        log.info("DQ 指标已推送: %s score=%.4f failed=%d", report.table, report.score, report.failed_checks)
    except Exception as e:
        log.warning("DQ 指标推送失败（不影响质检结论）: %s -> %s", report.table, e)
