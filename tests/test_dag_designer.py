"""dag_designer 渲染层单测：schema 校验、catalog 完整性、YAML 双向转换 round-trip。

纯 Python 测试，不依赖 Airflow 运行时。
"""

from pathlib import Path

import pytest
import yaml
from dag_designer import catalog, convert
from dag_designer.schema import DesignerDag, DesignerEdge, DesignerNode
from pydantic import ValidationError

DEMO_YAML = Path(__file__).resolve().parent.parent / "dags" / "designer" / "demo_visual_etl.yaml"


def _node(
    node_id: str, ntype: str = "sql", params: dict | None = None, x: float = 0, y: float = 0
) -> DesignerNode:
    return DesignerNode(
        id=node_id,
        type=ntype,
        params=params if params is not None else {"sql": "SELECT 1"},
        position={"x": x, "y": y},
    )


def _demo_dag() -> DesignerDag:
    return DesignerDag(
        dag_id="demo_dag",
        description="测试 DAG",
        schedule="0 2 * * *",
        nodes=[
            _node("task_a", params={"sql": "SELECT 1"}, x=10, y=20),
            _node("task_b", "sql_check", {"sql": "SELECT count(*) FROM t", "pass_value": "1"}, x=10, y=120),
            _node("task_c", "notify", {"message": "完成"}, x=10, y=220),
        ],
        edges=[
            DesignerEdge(upstream="task_a", downstream="task_b"),
            DesignerEdge(upstream="task_b", downstream="task_c"),
        ],
    )


def _normalize_edges(dag: DesignerDag) -> DesignerDag:
    """edges 顺序不影响语义（to_dict 按下游聚合成 dependencies），比较前统一排序。"""
    return dag.model_copy(update={"edges": sorted(dag.edges, key=lambda e: (e.downstream, e.upstream))})


# ---------- schema 校验 ----------


def test_valid_model_passes():
    dag = _demo_dag()
    assert dag.retries == 2
    assert dag.retry_delay_seconds == 300
    assert dag.alert_on_failure is True


def test_cycle_detected():
    with pytest.raises(ValidationError, match="环"):
        DesignerDag(
            dag_id="bad_cycle",
            nodes=[_node("a"), _node("b")],
            edges=[
                DesignerEdge(upstream="a", downstream="b"),
                DesignerEdge(upstream="b", downstream="a"),
            ],
        )


def test_dangling_node_detected():
    with pytest.raises(ValidationError, match="悬空节点"):
        DesignerDag(
            dag_id="bad_dangling",
            nodes=[_node("a"), _node("b"), _node("lonely")],
            edges=[DesignerEdge(upstream="a", downstream="b")],
        )


def test_single_node_without_edges_ok():
    dag = DesignerDag(dag_id="single_ok", nodes=[_node("only")], edges=[])
    assert len(dag.nodes) == 1


def test_missing_required_param_detected():
    with pytest.raises(ValidationError, match="缺少必填参数 'sql'"):
        DesignerDag(dag_id="bad_param", nodes=[DesignerNode(id="a", type="sql", params={})])


def test_notify_missing_message_detected():
    with pytest.raises(ValidationError, match="缺少必填参数 'message'"):
        DesignerDag(dag_id="bad_notify", nodes=[DesignerNode(id="n", type="notify", params={})])


def test_invalid_dag_id():
    for bad in ("Demo", "1dag", "dag-id", "dag id"):
        with pytest.raises(ValidationError, match="dag_id"):
            DesignerDag(dag_id=bad, nodes=[_node("a")])


def test_invalid_node_id():
    with pytest.raises(ValidationError, match="节点 id"):
        DesignerDag(dag_id="ok_dag", nodes=[_node("Bad-Node")])


def test_edge_references_unknown_node():
    with pytest.raises(ValidationError, match="不存在的节点"):
        DesignerDag(
            dag_id="bad_edge",
            nodes=[_node("a"), _node("b")],
            edges=[DesignerEdge(upstream="a", downstream="ghost")],
        )


def test_unknown_node_type():
    with pytest.raises(ValidationError, match="类型未知"):
        DesignerDag(dag_id="bad_type", nodes=[_node("a", "python_script")])


# ---------- catalog ----------


def test_catalog_all_types_complete():
    types = {spec["type"] for spec in catalog.list_node_types()}
    assert types == {"sql", "sql_check", "notify", "bash", "http", "sensor_sql", "empty"}
    for spec in catalog.list_node_types():
        assert spec["label"]
        assert spec["operator"].startswith("airflow.")
        assert spec["fields"], f"{spec['type']} 缺少 fields"
        for field in spec["fields"]:
            assert {"name", "label", "kind", "required", "default", "options"} <= set(field)


def test_catalog_every_type_has_advanced_fields():
    for spec in catalog.list_node_types():
        names = [f["name"] for f in spec["fields"]]
        assert "trigger_rule" in names, f"{spec['type']} 缺少 trigger_rule"
        assert "retries" in names, f"{spec['type']} 缺少 retries"
    trigger_rule = next(f for f in catalog.get_node_type("sql")["fields"] if f["name"] == "trigger_rule")
    assert trigger_rule["default"] == "all_success"
    assert trigger_rule["options"] == [
        "all_success",
        "all_done",
        "all_failed",
        "none_failed",
        "one_success",
        "one_failed",
        "none_skipped",
    ]
    # retries 默认留空（None），不进入 field_defaults 合并
    retries = next(f for f in catalog.get_node_type("sql")["fields"] if f["name"] == "retries")
    assert retries["default"] is None
    assert "retries" not in catalog.field_defaults("sql")


def test_catalog_new_type_specs():
    bash = catalog.get_node_type("bash")
    assert bash["operator"] == "airflow.providers.standard.operators.bash.BashOperator"
    assert catalog.required_fields("bash") == ["bash_command"]

    http = catalog.get_node_type("http")
    assert http["operator"] == "airflow.providers.http.operators.http.HttpOperator"
    assert catalog.required_fields("http") == ["endpoint"]
    method = next(f for f in http["fields"] if f["name"] == "method")
    assert method["options"] == ["GET", "POST", "PUT", "PATCH", "DELETE"]
    assert method["default"] == "GET"

    sensor = catalog.get_node_type("sensor_sql")
    assert sensor["operator"] == "airflow.providers.common.sql.sensors.sql.SqlSensor"
    assert catalog.required_fields("sensor_sql") == ["sql"]
    defaults = catalog.field_defaults("sensor_sql")
    assert defaults["conn_id"] == "warehouse"
    assert defaults["poke_interval"] == 60
    assert defaults["timeout"] == 3600
    assert defaults["mode"] == "reschedule"

    empty = catalog.get_node_type("empty")
    assert empty["operator"] == "airflow.providers.standard.operators.empty.EmptyOperator"
    assert catalog.required_fields("empty") == []


def test_catalog_required_fields():
    assert catalog.required_fields("sql") == ["sql"]
    assert catalog.required_fields("sql_check") == ["sql", "pass_value"]
    assert catalog.required_fields("notify") == ["message"]
    assert catalog.required_fields("nonexistent") == []


# ---------- 双向转换 ----------


def test_round_trip_dict():
    dag = _demo_dag()
    back = convert.from_yaml_dict(convert.to_yaml_dict(dag))
    assert _normalize_edges(back) == _normalize_edges(dag)


def test_round_trip_demo_yaml_file():
    dag = convert.load_yaml_file(DEMO_YAML)
    assert dag.dag_id == "demo_visual_etl"
    assert dag.schedule is None  # 仅手动触发
    assert [n.id for n in dag.nodes] == ["build_demo", "check_demo", "notify_done"]
    assert [n.type for n in dag.nodes] == ["sql", "sql_check", "notify"]
    back = convert.from_yaml_text(convert.to_yaml_text(dag))
    assert back == dag


def test_yaml_dict_serializable():
    """生成的 YAML dict 必须可被 yaml.safe_dump 序列化（无自定义对象残留）。"""
    text = yaml.safe_dump(convert.to_yaml_dict(_demo_dag()), allow_unicode=True)
    assert isinstance(text, str)


def test_to_yaml_dict_structure():
    data = convert.to_yaml_dict(_demo_dag())
    cfg = data["demo_dag"]
    assert cfg["default_args"]["on_failure_callback"] == catalog.FAILURE_CALLBACK
    assert cfg["default_args"]["retries"] == 2
    assert cfg["default_args"]["retry_delay"] == 300
    assert cfg["schedule"] == "0 2 * * *"
    # catalog 默认值合并进 task 参数
    assert cfg["tasks"]["task_a"]["conn_id"] == "warehouse"
    assert cfg["tasks"]["task_b"]["dependencies"] == ["task_a"]
    assert cfg["tasks"]["task_c"]["python_callable"] == catalog.NOTIFY_CALLABLE
    assert cfg["tasks"]["task_c"]["op_kwargs"] == {"message": "完成"}
    # designer 私有段保存类型与坐标
    assert cfg["designer"]["nodes"]["task_a"]["type"] == "sql"
    assert cfg["designer"]["nodes"]["task_a"]["position"] == {"x": 10.0, "y": 20.0}


def test_alert_off_omits_callback():
    dag = _demo_dag().model_copy(update={"alert_on_failure": False})
    cfg = convert.to_yaml_dict(dag)["demo_dag"]
    assert "on_failure_callback" not in cfg["default_args"]
    back = convert.from_yaml_dict(convert.to_yaml_dict(dag))
    assert back.alert_on_failure is False


def test_from_yaml_dict_rejects_multi_dag():
    with pytest.raises(ValueError, match="只能包含一个 DAG"):
        convert.from_yaml_dict({"dag_a": {}, "dag_b": {}})


# ---------- 高级字段（trigger_rule / retries）round-trip ----------


def _advanced_dag() -> DesignerDag:
    return DesignerDag(
        dag_id="advanced_dag",
        nodes=[
            _node("task_a", "sql", {"sql": "SELECT 1", "trigger_rule": "one_failed", "retries": 3}),
            _node("task_b", "http", {"endpoint": "https://example.com/hook", "method": "POST"}, x=0, y=100),
            _node(
                "task_c",
                "sensor_sql",
                {"sql": "SELECT 1 FROM t WHERE ok", "poke_interval": 30, "timeout": 600},
                x=0,
                y=200,
            ),
            _node("task_d", "bash", {"bash_command": "echo hi"}, x=0, y=300),
            _node("task_e", "empty", {}, x=0, y=400),
        ],
        edges=[
            DesignerEdge(upstream="task_a", downstream="task_b"),
            DesignerEdge(upstream="task_b", downstream="task_c"),
            DesignerEdge(upstream="task_c", downstream="task_d"),
            DesignerEdge(upstream="task_d", downstream="task_e"),
        ],
    )


def test_round_trip_with_trigger_rule_and_retries():
    dag = _advanced_dag()
    back = convert.from_yaml_dict(convert.to_yaml_dict(dag))
    assert _normalize_edges(back) == _normalize_edges(dag)
    # 非默认 trigger_rule / retries 保留在 params 中并进入 task dict
    task_a = convert.to_yaml_dict(dag)["advanced_dag"]["tasks"]["task_a"]
    assert task_a["trigger_rule"] == "one_failed"
    assert task_a["retries"] == 3
    # 未显式设置时：trigger_rule 用 catalog 默认值补齐，retries 缺省不落盘（继承 DAG 级）
    task_b = convert.to_yaml_dict(dag)["advanced_dag"]["tasks"]["task_b"]
    assert task_b["trigger_rule"] == "all_success"
    assert "retries" not in task_b
    # 各新类型的 operator 与透传参数
    tasks = convert.to_yaml_dict(dag)["advanced_dag"]["tasks"]
    assert tasks["task_b"]["operator"] == "airflow.providers.http.operators.http.HttpOperator"
    assert tasks["task_b"]["method"] == "POST"
    assert tasks["task_c"]["operator"] == "airflow.providers.common.sql.sensors.sql.SqlSensor"
    assert tasks["task_c"]["poke_interval"] == 30
    assert tasks["task_c"]["mode"] == "reschedule"  # catalog 默认值合并
    assert tasks["task_d"]["operator"] == "airflow.providers.standard.operators.bash.BashOperator"
    assert tasks["task_e"]["operator"] == "airflow.providers.standard.operators.empty.EmptyOperator"


def test_default_values_not_persisted_in_params():
    """值等于 catalog 默认值的参数不落盘为显式覆盖（往返后 params 中不保留）。"""
    dag = DesignerDag(
        dag_id="defaults_dag",
        nodes=[
            _node(
                "task_a", "sql", {"sql": "SELECT 1", "trigger_rule": "all_success", "conn_id": "warehouse"}
            ),
            _node("task_b", "http", {"endpoint": "https://example.com", "method": "GET"}, x=0, y=100),
        ],
        edges=[DesignerEdge(upstream="task_a", downstream="task_b")],
    )
    back = convert.from_yaml_dict(convert.to_yaml_dict(dag))
    params_a = next(n for n in back.nodes if n.id == "task_a").params
    params_b = next(n for n in back.nodes if n.id == "task_b").params
    assert "trigger_rule" not in params_a  # 等于默认值 all_success
    assert "conn_id" not in params_a  # 等于默认值 warehouse
    assert "trigger_rule" not in params_b
    assert "method" not in params_b  # 等于默认值 GET
    # 但 YAML task dict 中默认值仍然补齐（operator 运行时参数完整）
    tasks = convert.to_yaml_dict(dag)["defaults_dag"]["tasks"]
    assert tasks["task_a"]["trigger_rule"] == "all_success"
    assert tasks["task_a"]["conn_id"] == "warehouse"
    assert tasks["task_b"]["method"] == "GET"
