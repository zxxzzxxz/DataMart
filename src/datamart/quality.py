"""DataMart 数据质量（DQ）引擎：YAML 套件驱动的独立质检框架。

与 ETL 内嵌对账不同，本引擎把质检规则外置为 YAML 套件（config/data_quality/*.yaml），
由 datamart_data_quality DAG 统一调度执行。

标准用法（DAG 任务内）：
    suite = load_suite(Path("config/data_quality/dm_daily_city_sales.yaml"))
    report = run_suite("warehouse", suite, baselines_dir, pushgateway_url=url, run_id=run_id)
    # report.score < suite.severity_threshold 时由 DAG 决定是否 raise，引擎本身不抛

语义约定：
- severity=error 的检查计入得分（score = 通过数/总数）并可导致任务失败；
  severity=warn 只记录日志与指标，不影响得分。
- schema 漂移：对比 baselines/<name>.json 与 information_schema.columns，
  报告 added/dropped/type_changed；漂移是一条 error 级检查，
  套件顶层 allow_extra_columns: true 可放行新增列。
- quarantine（隔离）：accepted_values / custom_sql 可标 quarantine: true，
  违规行同事务搬入 dwh_quarantine.<表>__<检查>_<序号>（含 _dq_ts/_dq_run_id 列）。
- 指标：可选推送 Pushgateway（urllib，5s 超时），推送失败仅 warning，不影响质检结论。

安全约定：表名/列名来自 YAML（人工维护但仍按不可信输入处理），一律经
IDENTIFIER_RE 白名单校验后才拼入 SQL，杜绝注入；取值用参数化传递。
custom_sql 是受信任的只读文本（仓库内评审管理），仅 quarantine 时要求
其返回原表整行（SELECT * 语义、列序与表一致）。
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from datamart.ports import QueryExecutor as PostgresHook

log = logging.getLogger(__name__)

# 标识符白名单：字母/下划线开头，允许字母数字下划线、点（schema.table）与 $（PG 标识符合法字符）
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$.]*$")

# 隔离区 schema：假定由数据库迁移创建，引擎侧 CREATE SCHEMA IF NOT EXISTS 兜底
QUARANTINE_SCHEMA = "dwh_quarantine"

SEVERITIES = ("error", "warn")
EXPECTS = ("empty", "non_empty", "scalar_zero")
CHECK_TYPES = ("row_count", "null_rate", "unique", "accepted_values", "freshness", "custom_sql")
# 只有行级检查支持隔离（其余检查没有"违规行"概念）
QUARANTINE_TYPES = ("accepted_values", "custom_sql")


# ---------- 数据结构 ----------


@dataclass
class Check:
    """一条检查规则；params 存放类型特定参数（min/max_pct/column/values/sql 等）。"""

    type: str
    severity: str = "error"
    name: str = ""
    quarantine: bool = False
    params: dict = field(default_factory=dict)


@dataclass
class Suite:
    """一套针对单表的质检规则。"""

    table: str  # 必须是 schema.table 两段式
    checks: list[Check]
    severity_threshold: float = 1.0  # 得分（通过/总数，仅计 error 级）低于阈值则视为不达标
    baseline: str | None = None  # 指向 baselines/<name>.json，缺省不查漂移
    allow_extra_columns: bool = False  # True 时新增列不算漂移失败


@dataclass
class CheckResult:
    """单条检查的执行结果。"""

    name: str
    severity: str
    passed: bool
    detail: str
    quarantined_rows: int = 0


@dataclass
class Drift:
    """schema 漂移：相对基线的新增列 / 缺失列 / 类型变化（列 -> (基线类型, 实际类型)）。"""

    added: list[str] = field(default_factory=list)
    dropped: list[str] = field(default_factory=list)
    type_changed: dict[str, tuple[str, str]] = field(default_factory=dict)

    @property
    def any(self) -> bool:
        return bool(self.added or self.dropped or self.type_changed)


@dataclass
class Report:
    """一套件的质检结论汇总。"""

    table: str
    results: list[CheckResult]
    drift: Drift | None = None

    @property
    def _error_results(self) -> list[CheckResult]:
        return [r for r in self.results if r.severity == "error"]

    @property
    def score(self) -> float:
        """得分 = error 级检查通过数/总数；无 error 级检查时满分。warn 不计分。"""
        errors = self._error_results
        if not errors:
            return 1.0
        return sum(1 for r in errors if r.passed) / len(errors)

    @property
    def failed_errors(self) -> int:
        """未通过的 error 级检查数。"""
        return sum(1 for r in self._error_results if not r.passed)

    @property
    def failed_checks(self) -> int:
        """全部未通过检查数（含 warn，供指标使用）。"""
        return sum(1 for r in self.results if not r.passed)

    @property
    def quarantined_rows(self) -> int:
        return sum(r.quarantined_rows for r in self.results)

    def to_log_lines(self) -> list[str]:
        """中文可读的结论行，供任务日志逐行输出。"""
        errors = self._error_results
        passed = sum(1 for r in errors if r.passed)
        lines = [
            f"【DQ】{self.table} 得分 {self.score:.2f}"
            f"（error 检查 {passed}/{len(errors)} 通过，失败 {self.failed_errors} 项，"
            f"隔离 {self.quarantined_rows} 行）"
        ]
        for r in self.results:
            if r.passed:
                tag = "通过"
            elif r.severity == "warn":
                tag = "警告"  # warn 未过不影响得分
            else:
                tag = "失败"
            lines.append(f"  [{tag}] {r.name}: {r.detail}")
        if self.drift and self.drift.any:
            d = self.drift
            lines.append(f"  [漂移] added={d.added} dropped={d.dropped} type_changed={d.type_changed}")
        return lines


# ---------- 标识符白名单 ----------


def _ident(value, what: str) -> str:
    """校验 YAML 中的表名/列名，拒绝任何不像标识符的字符串（防 SQL 注入）。"""
    if not isinstance(value, str) or not IDENTIFIER_RE.match(value):
        raise ValueError(f"{what} 非法（须匹配 {IDENTIFIER_RE.pattern}）: {value!r}")
    return value


# ---------- 套件加载 ----------


def load_suite(path: str | Path) -> Suite:
    """加载并校验 YAML 质检套件；结构非法时抛 ValueError。"""
    path = Path(path)
    with open(path, encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, dict):
        raise ValueError(f"套件文件须为 YAML mapping: {path}")

    table = _ident(raw.get("table"), "table")
    if len(table.split(".")) != 2:
        raise ValueError(f"table 必须是 schema.table 两段式: {table!r}")

    threshold = raw.get("severity_threshold", 1.0)
    if not isinstance(threshold, (int, float)) or isinstance(threshold, bool) or not 0 < threshold <= 1:
        raise ValueError(f"severity_threshold 须在 (0, 1] 区间: {threshold!r}")

    baseline = raw.get("baseline")
    if baseline is not None:
        _ident(baseline, "baseline")

    allow_extra = raw.get("allow_extra_columns", False)
    if not isinstance(allow_extra, bool):
        raise ValueError(f"allow_extra_columns 须为布尔: {allow_extra!r}")

    raw_checks = raw.get("checks")
    if not isinstance(raw_checks, list) or not raw_checks:
        raise ValueError(f"checks 须为非空列表: {path}")

    checks = [_parse_check(i, c) for i, c in enumerate(raw_checks)]
    return Suite(
        table=table,
        checks=checks,
        severity_threshold=float(threshold),
        baseline=baseline,
        allow_extra_columns=allow_extra,
    )


def _parse_check(index: int, raw) -> Check:
    """解析单条检查：按类型校验必填参数与标识符。"""
    if not isinstance(raw, dict):
        raise ValueError(f"checks[{index}] 须为 mapping: {raw!r}")
    ctype = raw.get("type")
    if ctype not in CHECK_TYPES:
        raise ValueError(f"checks[{index}] 未知检查类型: {ctype!r}（支持 {CHECK_TYPES}）")

    severity = raw.get("severity", "error")
    if severity not in SEVERITIES:
        raise ValueError(f"checks[{index}] severity 须为 {SEVERITIES}: {severity!r}")

    quarantine = raw.get("quarantine", False)
    if not isinstance(quarantine, bool):
        raise ValueError(f"checks[{index}] quarantine 须为布尔: {quarantine!r}")
    if quarantine and ctype not in QUARANTINE_TYPES:
        raise ValueError(f"checks[{index}] 仅 {QUARANTINE_TYPES} 支持 quarantine，当前类型: {ctype}")

    params = {k: v for k, v in raw.items() if k not in ("type", "severity", "name", "quarantine")}
    name = raw.get("name") or _default_name(index, ctype, params)

    # 类型级校验（缺参/越界/标识符非法在此抛出）
    if ctype == "row_count":
        params["min"] = _int_param(index, params, "min", default=1, min_value=0)
    elif ctype == "null_rate":
        params["column"] = _ident(params.get("column"), f"checks[{index}].column")
        max_pct = params.get("max_pct")
        if not isinstance(max_pct, (int, float)) or isinstance(max_pct, bool) or not 0 <= max_pct <= 100:
            raise ValueError(f"checks[{index}] null_rate.max_pct 须在 [0, 100]: {max_pct!r}")
    elif ctype == "unique":
        columns = params.get("columns")
        if not isinstance(columns, list) or not columns:
            raise ValueError(f"checks[{index}] unique.columns 须为非空列表: {columns!r}")
        params["columns"] = [_ident(c, f"checks[{index}].columns") for c in columns]
    elif ctype == "accepted_values":
        params["column"] = _ident(params.get("column"), f"checks[{index}].column")
        values = params.get("values")
        if not isinstance(values, list) or not values:
            raise ValueError(f"checks[{index}] accepted_values.values 须为非空列表: {values!r}")
    elif ctype == "freshness":
        params["date_column"] = _ident(params.get("date_column"), f"checks[{index}].date_column")
        params["max_lag_days"] = _int_param(index, params, "max_lag_days", default=None, min_value=0)
    elif ctype == "custom_sql":
        sql = params.get("sql")
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError(f"checks[{index}] custom_sql.sql 须为非空字符串")
        expect = params.get("expect")
        if expect not in EXPECTS:
            raise ValueError(f"checks[{index}] custom_sql.expect 须为 {EXPECTS}: {expect!r}")

    return Check(type=ctype, severity=severity, name=name, quarantine=quarantine, params=params)


def _default_name(index: int, ctype: str, params: dict) -> str:
    """未显式命名时按类型生成可读名称。"""
    if ctype in ("null_rate", "accepted_values"):
        return f"{ctype}:{params.get('column')}"
    if ctype == "unique":
        return f"unique:{','.join(params.get('columns') or [])}"
    if ctype == "freshness":
        return f"freshness:{params.get('date_column')}"
    if ctype == "custom_sql":
        return f"custom_sql#{index}"
    return ctype


def _int_param(index: int, params: dict, key: str, default, min_value: int) -> int:
    value = params.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool) or value < min_value:
        raise ValueError(f"checks[{index}] {key} 须为 >= {min_value} 的整数: {value!r}")
    return value


# ---------- schema 漂移 ----------


def _diff_schema(baseline_cols: dict, actual_cols: dict) -> Drift:
    """对比基线与实际列（均为 {列名: data_type}），给出三类差异。"""
    added = sorted(set(actual_cols) - set(baseline_cols))
    dropped = sorted(set(baseline_cols) - set(actual_cols))
    type_changed = {
        col: (baseline_cols[col], actual_cols[col])
        for col in sorted(set(baseline_cols) & set(actual_cols))
        if baseline_cols[col] != actual_cols[col]
    }
    return Drift(added=added, dropped=dropped, type_changed=type_changed)


def _check_schema_drift(
    hook: PostgresHook, suite: Suite, baselines_dir: Path
) -> tuple[Drift | None, CheckResult]:
    """读取基线并与 information_schema 对比；漂移整体算一条 error 级检查。"""
    name = "schema_drift"
    baseline_path = Path(baselines_dir) / f"{suite.baseline}.json"
    if not baseline_path.exists():
        return None, CheckResult(name, "error", False, f"基线文件不存在: {baseline_path}")
    with open(baseline_path, encoding="utf-8") as f:
        baseline_cols = (json.load(f) or {}).get("columns", {})

    schema, table = suite.table.split(".")
    actual_cols = {
        row[0]: row[1]
        for row in hook.get_records(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %(s)s AND table_name = %(t)s",
            parameters={"s": schema, "t": table},
        )
    }
    drift = _diff_schema(baseline_cols, actual_cols)
    # 新增列在 allow_extra_columns 下放行；缺列/类型变化永远算失败
    failed = bool(drift.dropped or drift.type_changed or (drift.added and not suite.allow_extra_columns))
    if not drift.any:
        detail = "与基线一致"
    else:
        detail = f"added={drift.added} dropped={drift.dropped} type_changed={drift.type_changed}"
        if drift.added and suite.allow_extra_columns and not (drift.dropped or drift.type_changed):
            detail += "（新增列已被 allow_extra_columns 放行）"
    return drift, CheckResult(name, "error", not failed, detail)


# ---------- 各类型检查执行 ----------


def _run_check(
    hook: PostgresHook, suite: Suite, check: Check, index: int, run_id: str | None, *, read_only: bool = False
) -> CheckResult:
    """执行单条检查；执行异常不中断套件，转为失败结果（让报告一次出全）。"""
    try:
        passed, detail = _dispatch_check(hook, suite, check)
        quarantined = 0
        if not passed and check.quarantine and not read_only:
            violation_sql = _violation_sql(suite, check)
            quarantined, qtable = _quarantine(hook, suite, check, index, violation_sql, run_id)
            detail += f"；已隔离 {quarantined} 行到 {qtable}"
        return CheckResult(check.name, check.severity, passed, detail, quarantined)
    except Exception as e:
        log.exception("检查执行异常: %s.%s", suite.table, check.name)
        return CheckResult(check.name, check.severity, False, f"检查执行异常: {e}")


def _dispatch_check(hook: PostgresHook, suite: Suite, check: Check) -> tuple[bool, str]:
    p = check.params
    table = suite.table

    if check.type == "row_count":
        cnt = hook.get_first(f"SELECT count(*) FROM {table}")[0]
        return cnt >= p["min"], f"共 {cnt} 行（下限 {p['min']}）"

    if check.type == "null_rate":
        total = hook.get_first(f"SELECT count(*) FROM {table}")[0]
        nulls = hook.get_first(f"SELECT count(*) FROM {table} WHERE {p['column']} IS NULL")[0]
        pct = round(100.0 * nulls / total, 4) if total else 0.0  # 空表记 0%，由 row_count 检查兜底
        return pct <= p["max_pct"], f"{p['column']} 空值率 {pct}%（上限 {p['max_pct']}%，{nulls}/{total} 行）"

    if check.type == "unique":
        cols = ", ".join(p["columns"])
        dup = hook.get_first(
            f"SELECT count(*) FROM (SELECT {cols} FROM {table} GROUP BY {cols} HAVING count(*) > 1) dup"
        )[0]
        return dup == 0, f"({cols}) 重复 {dup} 组"

    if check.type == "accepted_values":
        # NULL 不算违规取值（空值问题归 null_rate 检查负责）
        cnt = hook.get_first(
            f"SELECT count(*) FROM ({_violation_sql(suite, check)}) v",
            parameters=_value_params(p["values"]),
        )[0]
        return cnt == 0, f"{p['column']} 非法取值 {cnt} 行（允许 {p['values']}，NULL 不计）"

    if check.type == "freshness":
        max_val = hook.get_first(f"SELECT max({p['date_column']}) FROM {table}")[0]
        if max_val is None:
            return False, f"{p['date_column']} 无数据（空表）"
        max_date = max_val.date() if isinstance(max_val, datetime) else max_val
        lag = (hook.get_first("SELECT CURRENT_DATE")[0] - max_date).days
        detail = f"max({p['date_column']})={max_date}，滞后 {lag} 天（上限 {p['max_lag_days']}）"
        return lag <= p["max_lag_days"], detail

    if check.type == "custom_sql":
        rows = hook.get_records(p["sql"])
        passed = _check_expect(rows, p["expect"])
        first = rows[0][0] if rows else None
        return passed, f"返回 {len(rows)} 行（首行首列={first!r}），期望 {p['expect']}"

    raise ValueError(f"未实现的检查类型: {check.type}")  # pragma: no cover（load_suite 已拦截）


def _check_expect(rows: list, expect: str) -> bool:
    """custom_sql 的期望判定：empty=无行；non_empty=有行；scalar_zero=首行首列等于 0。"""
    if expect == "empty":
        return len(rows) == 0
    if expect == "non_empty":
        return len(rows) > 0
    if expect == "scalar_zero":
        return bool(rows) and rows[0] and rows[0][0] is not None and rows[0][0] == 0
    raise ValueError(f"未知 expect: {expect!r}")


# ---------- 隔离区 ----------


def _value_params(values: list) -> dict:
    return {f"dq_v{i}": v for i, v in enumerate(values)}


def _violation_sql(suite: Suite, check: Check) -> str:
    """选出违规整行的 SQL（列序与原表一致，供隔离 INSERT/DELETE 复用）。"""
    p = check.params
    if check.type == "accepted_values":
        placeholders = ", ".join(f"%(dq_v{i})s" for i in range(len(p["values"])))
        col = p["column"]
        return f"SELECT * FROM {suite.table} WHERE {col} IS NOT NULL AND {col} NOT IN ({placeholders})"
    # custom_sql：约定 sql 本身即返回原表整行（套件评审时保证）
    if check.type == "custom_sql":
        return p["sql"]
    raise ValueError(f"{check.type} 不支持隔离")


def _quarantine_table_name(suite: Suite, check: Check, index: int) -> str:
    # 表名/列名均已过白名单，拼接安全；index 保证同表多检查不撞名
    return f"{QUARANTINE_SCHEMA}.{suite.table.split('.')[-1]}__{check.type}_{index}"


def _quarantine(
    hook: PostgresHook, suite: Suite, check: Check, index: int, violation_sql: str, run_id: str | None
) -> tuple[int, str]:
    """把违规行隔离到 dwh_quarantine：建表（如不存在）-> INSERT SELECT -> 原表 DELETE。

    全程单事务（hook.run 默认非 autocommit，全部成功才提交）；无违规行时不建表。
    返回 (隔离行数, 隔离表名)。
    """
    cnt = hook.get_first(
        f"SELECT count(*) FROM ({violation_sql}) v",
        parameters=_value_params(check.params.get("values", [])) if check.type == "accepted_values" else None,
    )[0]
    qtable = _quarantine_table_name(suite, check, index)
    if cnt == 0:
        return 0, qtable
    value_params = _value_params(check.params["values"]) if check.type == "accepted_values" else {}
    hook.run(
        [
            # schema 假定由迁移创建，这里兜底（引擎可独立运行）
            f"CREATE SCHEMA IF NOT EXISTS {QUARANTINE_SCHEMA}",
            f"CREATE TABLE IF NOT EXISTS {qtable} (LIKE {suite.table} INCLUDING ALL)",
            f"ALTER TABLE {qtable} "
            "ADD COLUMN IF NOT EXISTS _dq_ts TIMESTAMPTZ, ADD COLUMN IF NOT EXISTS _dq_run_id TEXT",
            f"INSERT INTO {qtable} SELECT v.*, now(), %(dq_run_id)s FROM ({violation_sql}) v",
            # 整行匹配删除（IS NOT DISTINCT FROM 空值安全）；
            # 注意：与原表完全重复的行会一并删除，业务表有主键时无此问题
            f"DELETE FROM {suite.table} t USING ({violation_sql}) v WHERE t IS NOT DISTINCT FROM v",
        ],
        parameters={**value_params, "dq_run_id": run_id or "unknown"},
    )
    return cnt, qtable


# ---------- 指标推送（Pushgateway） ----------


# ---------- 套件执行入口 ----------


def run_suite(
    conn_id: str,
    suite: Suite,
    baselines_dir: str | Path,
    pushgateway_url: str | None = None,
    run_id: str | None = None,
    *,
    hook=None,
    read_only: bool = False,
) -> Report:
    """执行整套检查并汇总成 Report；引擎不抛业务异常，是否失败由 DAG 按 score 决定。"""
    if hook is None:
        raise ValueError("Quality evaluation requires an injected query executor")
    results: list[CheckResult] = []
    drift = None
    if suite.baseline:
        drift, drift_result = _check_schema_drift(hook, suite, Path(baselines_dir))
        results.append(drift_result)
    for i, check in enumerate(suite.checks):
        results.append(_run_check(hook, suite, check, i, run_id, read_only=read_only))
    report = Report(table=suite.table, results=results, drift=drift)
    return report
