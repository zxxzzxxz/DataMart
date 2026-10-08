"""DQ 引擎纯逻辑单测：不连库，覆盖套件加载/校验、注入防护、漂移判定、得分与 expect 语义、指标 URL 编码。"""

import base64
from pathlib import Path

import pytest
from data_quality import (
    CheckResult,
    Report,
    _check_expect,
    _diff_schema,
    _ident,
    _pushgateway_url,
    load_suite,
)

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "data_quality"

VALID_SUITE = """
table: dm.daily_city_sales
severity_threshold: 0.8
baseline: dm.daily_city_sales
allow_extra_columns: true
checks:
  - type: row_count
    min: 1
  - type: null_rate
    column: city
    max_pct: 0
    severity: warn
  - type: accepted_values
    column: status
    values: [paid, cancel]
    quarantine: true
"""


def _write(tmp_path, content) -> Path:
    path = tmp_path / "suite.yaml"
    path.write_text(content, encoding="utf-8")
    return path


# ---------- 套件加载 ----------


def test_load_suite_ok(tmp_path):
    suite = load_suite(_write(tmp_path, VALID_SUITE))
    assert suite.table == "dm.daily_city_sales"
    assert suite.severity_threshold == 0.8
    assert suite.baseline == "dm.daily_city_sales"
    assert suite.allow_extra_columns is True
    assert len(suite.checks) == 3
    # 缺省 severity=error；name 自动生成
    assert suite.checks[0].severity == "error"
    assert suite.checks[1].name == "null_rate:city"
    assert suite.checks[2].quarantine is True


def test_load_suite_defaults(tmp_path):
    suite = load_suite(_write(tmp_path, "table: dwh.fact_orders\nchecks:\n  - type: row_count\n"))
    assert suite.severity_threshold == 1.0
    assert suite.baseline is None
    assert suite.allow_extra_columns is False
    assert suite.checks[0].params["min"] == 1  # row_count 缺省下限 1


def test_load_real_suites():
    """仓库内两份真实套件必须能通过校验（防配置写错）。"""
    files = sorted(CONFIG_DIR.glob("*.yaml"))
    assert {f.name for f in files} == {"dm_daily_city_sales.yaml", "dwh_fact_orders.yaml"}
    for f in files:
        suite = load_suite(f)
        assert suite.table.count(".") == 1
        assert suite.checks


@pytest.mark.parametrize(
    "content, hint",
    [
        ("checks:\n  - type: row_count\n", "缺 table"),
        ("table: fact_orders\nchecks:\n  - type: row_count\n", "table 非两段式"),
        ("table: dm.x\nchecks: []\n", "checks 为空"),
        ("table: dm.x\nseverity_threshold: 1.5\nchecks:\n  - type: row_count\n", "阈值越界"),
        ("table: dm.x\nchecks:\n  - type: nope\n", "未知类型"),
        ("table: dm.x\nchecks:\n  - type: row_count\n    severity: fatal\n", "severity 非法"),
        ("table: dm.x\nchecks:\n  - type: null_rate\n    max_pct: 0\n", "null_rate 缺 column"),
        ("table: dm.x\nchecks:\n  - type: null_rate\n    column: c\n    max_pct: 101\n", "max_pct 越界"),
        ("table: dm.x\nchecks:\n  - type: unique\n    columns: []\n", "unique 空 columns"),
        ("table: dm.x\nchecks:\n  - type: accepted_values\n    column: c\n    values: []\n", "空 values"),
        ("table: dm.x\nchecks:\n  - type: freshness\n    date_column: d\n    max_lag_days: -1\n", "lag 为负"),
        ("table: dm.x\nchecks:\n  - type: custom_sql\n    sql: ''\n    expect: empty\n", "空 sql"),
        ("table: dm.x\nchecks:\n  - type: custom_sql\n    sql: SELECT 1\n    expect: zero\n", "expect 非法"),
        ("table: dm.x\nchecks:\n  - type: row_count\n    quarantine: true\n", "row_count 不支持隔离"),
    ],
)
def test_load_suite_validation_errors(tmp_path, content, hint):
    with pytest.raises(ValueError):
        load_suite(_write(tmp_path, content)), hint


# ---------- 标识符白名单（防注入） ----------


@pytest.mark.parametrize(
    "bad", ["city; DROP TABLE x--", "1abc", "col-name", "col name", "x'", "x--", "", None, 123]
)
def test_ident_rejects_injection(bad):
    with pytest.raises(ValueError):
        _ident(bad, "column")


@pytest.mark.parametrize("ok", ["dm.daily_city_sales", "stat_date", "_tmp$1", "dwh.fact_orders"])
def test_ident_accepts_valid(ok):
    assert _ident(ok, "column") == ok


def test_suite_rejects_malicious_identifiers(tmp_path):
    with pytest.raises(ValueError):
        load_suite(_write(tmp_path, "table: dm.x; DROP TABLE ods.users--\nchecks:\n  - type: row_count\n"))
    with pytest.raises(ValueError):
        load_suite(
            _write(
                tmp_path,
                "table: dm.x\nchecks:\n  - type: null_rate\n    column: c OR 1=1--\n    max_pct: 0\n",
            )
        )


# ---------- schema 漂移三类差异 ----------


def test_diff_schema_no_drift():
    baseline = {"a": "integer", "b": "text"}
    drift = _diff_schema(baseline, dict(baseline))
    assert not drift.any


def test_diff_schema_three_kinds():
    baseline = {"keep": "text", "chg": "integer", "gone": "date"}
    actual = {"keep": "text", "chg": "numeric", "new1": "text", "new2": "date"}
    drift = _diff_schema(baseline, actual)
    assert drift.added == ["new1", "new2"]
    assert drift.dropped == ["gone"]
    assert drift.type_changed == {"chg": ("integer", "numeric")}
    assert drift.any


# ---------- 得分与失败计数 ----------


def _r(severity, passed):
    return CheckResult(name="c", severity=severity, passed=passed, detail="")


def test_report_score_error_only():
    # warn 未过不计分：2 个 error 过 1 -> 0.5
    report = Report(table="dm.t", results=[_r("error", True), _r("error", False), _r("warn", False)])
    assert report.score == 0.5
    assert report.failed_errors == 1
    assert report.failed_checks == 2  # 指标口径含 warn


def test_report_score_no_error_checks_is_full():
    report = Report(table="dm.t", results=[_r("warn", False)])
    assert report.score == 1.0
    assert report.failed_errors == 0


def test_report_log_lines_readable():
    report = Report(table="dm.t", results=[_r("error", False), _r("warn", True)])
    text = "\n".join(report.to_log_lines())
    assert "dm.t" in text and "失败" in text


# ---------- custom_sql expect 语义 ----------


def test_expect_empty():
    assert _check_expect([], "empty") is True
    assert _check_expect([(1,)], "empty") is False


def test_expect_non_empty():
    assert _check_expect([(1,)], "non_empty") is True
    assert _check_expect([], "non_empty") is False


def test_expect_scalar_zero():
    assert _check_expect([(0,)], "scalar_zero") is True
    assert _check_expect([(3,)], "scalar_zero") is False
    assert _check_expect([(None,)], "scalar_zero") is False
    assert _check_expect([], "scalar_zero") is False


# ---------- Pushgateway 分组键编码 ----------


def test_pushgateway_url_base64_grouping_key():
    url = _pushgateway_url("http://pushgateway:9091/", "dm.daily_city_sales")
    prefix = "http://pushgateway:9091/metrics/job/datamart_dq/table@base64/"
    assert url.startswith(prefix)
    b64 = url[len(prefix) :]
    # urlsafe base64 且能还原回原始表名（含点号必须走 base64 分组键）
    assert "/" not in b64
    assert base64.urlsafe_b64decode(b64).decode() == "dm.daily_city_sales"
