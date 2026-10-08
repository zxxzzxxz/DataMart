"""Instantiate every advertised node type using the production dag-factory version."""

from datamart.workflows import convert
from datamart.workflows.schema import DesignerDag, DesignerEdge, DesignerNode


def test_all_catalog_nodes_render_as_actual_airflow_operators(tmp_path):
    from dagfactory import load_yaml_dags

    definitions = [
        ("sql", {"sql": "SELECT 1"}),
        ("sql_check", {"sql": "SELECT 1", "pass_value": 1}),
        ("notify", {"message": "done", "trigger_rule": "all_done", "retries": 3}),
        ("bash", {"bash_command": "echo done"}),
        ("http", {"endpoint": "/health", "headers": '{"X-Test":"1"}'}),
        ("sensor_sql", {"sql": "SELECT 1", "mode": "normal"}),
        ("empty", {}),
    ]
    nodes = [
        DesignerNode(id=f"node_{index}", type=kind, params=params)
        for index, (kind, params) in enumerate(definitions)
    ]
    model = DesignerDag(
        dag_id="all_node_contracts",
        nodes=nodes,
        edges=[
            DesignerEdge(upstream=left.id, downstream=right.id)
            for left, right in zip(nodes, nodes[1:], strict=False)
        ],
    )
    convert.dump_yaml_file(model, tmp_path / "nodes.yaml")
    context = {}
    load_yaml_dags(context, dags_folder=str(tmp_path))
    dag = context[model.dag_id]
    assert len(dag.tasks) == len(definitions)
    assert dag.get_task("node_2").trigger_rule == "all_done"
    assert dag.get_task("node_2").retries == 3
    assert dag.get_task("node_4").headers == {"X-Test": "1"}
    assert dag.get_task("node_5").mode == "poke"
