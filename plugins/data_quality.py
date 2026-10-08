"""Legacy Airflow entry point; shared quality logic lives in datamart.quality."""

from datamart.adapters.metrics import _pushgateway_url as _pushgateway_url
from datamart.adapters.metrics import push_metrics as _push_metrics
from datamart.quality import CHECK_TYPES as CHECK_TYPES
from datamart.quality import EXPECTS as EXPECTS
from datamart.quality import IDENTIFIER_RE as IDENTIFIER_RE
from datamart.quality import QUARANTINE_SCHEMA as QUARANTINE_SCHEMA
from datamart.quality import QUARANTINE_TYPES as QUARANTINE_TYPES
from datamart.quality import SEVERITIES as SEVERITIES
from datamart.quality import Check as Check
from datamart.quality import CheckResult as CheckResult
from datamart.quality import Drift as Drift
from datamart.quality import Report as Report
from datamart.quality import Suite as Suite
from datamart.quality import _check_expect as _check_expect
from datamart.quality import _check_schema_drift as _check_schema_drift
from datamart.quality import _default_name as _default_name
from datamart.quality import _diff_schema as _diff_schema
from datamart.quality import _dispatch_check as _dispatch_check
from datamart.quality import _ident as _ident
from datamart.quality import _int_param as _int_param
from datamart.quality import _parse_check as _parse_check
from datamart.quality import _quarantine as _quarantine
from datamart.quality import _quarantine_table_name as _quarantine_table_name
from datamart.quality import _run_check as _run_check
from datamart.quality import _value_params as _value_params
from datamart.quality import _violation_sql as _violation_sql
from datamart.quality import load_suite as load_suite
from datamart.quality import log as log
from datamart.quality import run_suite as evaluate_suite


def run_suite(
    conn_id, suite, baselines_dir, pushgateway_url=None, run_id=None, *, hook=None, read_only=False
):
    if hook is None:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=conn_id)
    report = evaluate_suite(conn_id, suite, baselines_dir, run_id=run_id, hook=hook, read_only=read_only)
    if pushgateway_url:
        _push_metrics(pushgateway_url, report)
    return report
