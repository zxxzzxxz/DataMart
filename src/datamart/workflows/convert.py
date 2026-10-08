"""DesignerDag ↔ dag-factory v1 YAML dict 的双向转换。

YAML 文件结构（每文件一个 DAG，顶层 key 即 dag_id）：

    demo_visual_etl:
      default_args: {owner, retries, retry_delay, on_failure_callback?}
      schedule: "0 2 * * *" 或 null
      description: "..."
      designer:                       # 设计器私有段：节点 type/坐标
        nodes: {task_a: {type: sql, position: {x: 100, y: 80}}}
      tasks:
        task_a: {operator, 参数..., dependencies: [...]}

已实测（dag-factory 1.1.0 + Airflow 3.3.2）：
- dag 配置里的未知 key（designer 私有段）会被忽略：DagBuilder.build() 按白名单取 key 构造 DAG；
- on_failure_callback 写字符串路径（alerting.send_failure_alert），dag-factory 经 import_string 解析生效；
- retry_delay 直接写秒数即可（Airflow 3 BaseOperator 的 retry_delay 接受 timedelta | float）；
- PythonOperator 的 python_callable 写字符串路径（alerting._post_webhook）即可；
- schedule 写 cron 字符串或 null（手动触发）。

注意：dag-factory 的顶层保留 key 为 default / task_groups，设计器 YAML 不使用它们。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from datamart.workflows import catalog
from datamart.workflows.schema import DesignerDag, DesignerEdge, DesignerNode, NodePosition

# 设计器私有段 key（dag 配置内；dag-factory 渲染时忽略）
DESIGNER_SECTION = "designer"

_NOTIFY_OPERATOR = "airflow.providers.standard.operators.python.PythonOperator"
_OPERATOR_TO_TYPE = {spec["operator"]: spec["type"] for spec in catalog.list_node_types()}
_OPERATOR_TO_TYPE["airflow.providers.common.sql.sensors.sql.SQLSensor"] = "sensor_sql"


def to_yaml_dict(dag: DesignerDag) -> dict[str, Any]:
    """DesignerDag → dag-factory YAML dict（顶层 key = dag_id）。"""
    default_args: dict[str, Any] = {
        "owner": "designer",
        "retries": dag.retries,
        "retry_delay": dag.retry_delay_seconds,
    }
    if dag.alert_on_failure:
        default_args["on_failure_callback"] = catalog.FAILURE_CALLBACK

    downstream_map: dict[str, list[str]] = {}
    for edge in dag.edges:
        downstream_map.setdefault(edge.downstream, []).append(edge.upstream)

    tasks: dict[str, Any] = {}
    for node in dag.nodes:
        spec = catalog.get_node_type(node.type)
        if spec is None:
            raise ValueError(f"未知节点类型：'{node.type}'")
        # 合并 catalog 默认值，保证 YAML 中的参数是完整的（operator 运行时不缺省）
        params = {**catalog.field_defaults(node.type), **node.params}
        task: dict[str, Any] = {"operator": spec["operator"]}
        if node.type == "notify":
            task["python_callable"] = catalog.NOTIFY_CALLABLE
            task["op_kwargs"] = {"message": params["message"]}
            task.update({key: value for key, value in params.items() if key != "message"})
        else:
            task.update(params)
        deps = downstream_map.get(node.id)
        if deps:
            task["dependencies"] = deps
        tasks[node.id] = task

    dag_cfg: dict[str, Any] = {
        "default_args": default_args,
        "schedule": dag.schedule,
    }
    if dag.description:
        dag_cfg["description"] = dag.description
    dag_cfg[DESIGNER_SECTION] = {
        "nodes": {
            node.id: {"type": node.type, "position": {"x": node.position.x, "y": node.position.y}}
            for node in dag.nodes
        }
    }
    dag_cfg["tasks"] = tasks
    return {dag.dag_id: dag_cfg}


def from_yaml_dict(data: dict[str, Any]) -> DesignerDag:
    """dag-factory YAML dict → DesignerDag（to_yaml_dict 的逆运算，round-trip 无损）。"""
    dag_ids = [k for k in data if k not in ("default", "task_groups")]
    if len(dag_ids) != 1:
        raise ValueError(f"设计器 YAML 必须且只能包含一个 DAG 定义，实际顶层 key：{dag_ids}")
    dag_id = dag_ids[0]
    dag_cfg = data[dag_id] or {}
    default_args = dag_cfg.get("default_args") or {}
    meta_nodes = (dag_cfg.get(DESIGNER_SECTION) or {}).get("nodes") or {}
    tasks = dag_cfg.get("tasks") or {}

    nodes: list[DesignerNode] = []
    edges: list[DesignerEdge] = []
    for task_id, raw_task in tasks.items():
        task_cfg = raw_task or {}
        meta = meta_nodes.get(task_id) or {}
        operator = task_cfg.get("operator", "")
        node_type = meta.get("type") or _OPERATOR_TO_TYPE.get(operator)
        if node_type is None:
            raise ValueError(f"无法推断节点 '{task_id}' 的类型（operator: {operator or '未配置'}）")

        if node_type == "notify":
            raw_params = dict(task_cfg.get("op_kwargs") or {})
            raw_params.update({key: task_cfg[key] for key in ("trigger_rule", "retries") if key in task_cfg})
        else:
            raw_params = {k: v for k, v in task_cfg.items() if k not in ("operator", "dependencies")}
        # 与 catalog 默认值一致的参数不保留在 params 中（与 to_yaml_dict 的默认值合并互逆）
        defaults = catalog.field_defaults(node_type)
        params = {k: v for k, v in raw_params.items() if k not in defaults or defaults[k] != v}

        position = meta.get("position") or {}
        nodes.append(
            DesignerNode(id=task_id, type=node_type, params=params, position=NodePosition(**position))
        )
        for dep in task_cfg.get("dependencies") or []:
            edges.append(DesignerEdge(upstream=dep, downstream=task_id))

    return DesignerDag(
        dag_id=dag_id,
        description=dag_cfg.get("description") or None,
        schedule=_normalize_schedule(dag_cfg.get("schedule")),
        retries=int(default_args.get("retries", 2)),
        retry_delay_seconds=int(default_args.get("retry_delay", 300)),
        alert_on_failure=default_args.get("on_failure_callback") is not None,
        nodes=nodes,
        edges=edges,
    )


def _normalize_schedule(raw: Any) -> str | None:
    """YAML 中的 schedule：null / "None" / 空串 统一视为 None（仅手动触发）。"""
    if raw is None:
        return None
    if isinstance(raw, str) and raw.strip().lower() in ("", "none", "null"):
        return None
    return str(raw)


def to_yaml_text(dag: DesignerDag) -> str:
    return yaml.safe_dump(to_yaml_dict(dag), allow_unicode=True, sort_keys=False, default_flow_style=False)


def from_yaml_text(text: str) -> DesignerDag:
    data = yaml.safe_load(text)
    if not isinstance(data, dict):
        raise ValueError("YAML 内容不是合法的 DAG 定义（顶层应为 mapping）")
    return from_yaml_dict(data)


def dump_yaml_file(dag: DesignerDag, path: str | Path) -> None:
    Path(path).write_text(to_yaml_text(dag), encoding="utf-8")


def load_yaml_file(path: str | Path) -> DesignerDag:
    return from_yaml_text(Path(path).read_text(encoding="utf-8"))
