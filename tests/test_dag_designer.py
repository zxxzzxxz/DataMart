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
        id=node_id, type=ntype, params=params or {"sql": "SELECT 1"}, position={"x": x, "y": y}
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


def test_catalog_three_types_complete():
    types = {spec["type"] for spec in catalog.list_node_types()}
    assert types == {"sql", "sql_check", "notify"}
    for spec in catalog.list_node_types():
        assert spec["label"]
        assert spec["operator"].startswith("airflow.")
        assert spec["fields"], f"{spec['type']} 缺少 fields"
        for field in spec["fields"]:
            assert {"name", "label", "kind", "required", "default", "options"} <= set(field)


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
