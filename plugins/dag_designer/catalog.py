"""节点类型目录：每种类型声明底层 operator 与参数表单 schema（驱动前端属性表单）。

MVP 三种节点类型；新增类型 = 在 NODE_TYPES 注册一条 + 底层 operator/函数实现，前端零改动。
字段定义结构：name / label / kind(text|textarea|select) / required / default / options。
"""

from typing import Any

NODE_TYPES: list[dict[str, Any]] = [
    {
        "type": "sql",
        "label": "SQL 转换",
        "operator": "airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator",
        "fields": [
            {
                "name": "sql",
                "label": "SQL 语句",
                "kind": "textarea",
                "required": True,
                "default": None,
                "options": None,
            },
            {
                "name": "conn_id",
                "label": "数据库连接",
                "kind": "select",
                "required": False,
                "default": "warehouse",
                "options": ["warehouse"],
            },
        ],
    },
    {
        "type": "sql_check",
        "label": "数据校验",
        "operator": "airflow.providers.common.sql.operators.sql.SQLValueCheckOperator",
        "fields": [
            {
                "name": "sql",
                "label": "校验 SQL（返回单值）",
                "kind": "textarea",
                "required": True,
                "default": None,
                "options": None,
            },
            {
                "name": "pass_value",
                "label": "期望值（查询单值与其相等则通过）",
                "kind": "text",
                "required": True,
                "default": None,
                "options": None,
            },
            {
                "name": "conn_id",
                "label": "数据库连接",
                "kind": "select",
                "required": False,
                "default": "warehouse",
                "options": ["warehouse"],
            },
        ],
    },
    {
        "type": "notify",
        "label": "通知",
        "operator": "airflow.providers.standard.operators.python.PythonOperator",
        "fields": [
            {
                "name": "message",
                "label": "消息内容",
                "kind": "textarea",
                "required": True,
                "default": None,
                "options": None,
            },
        ],
    },
]

_TYPE_INDEX: dict[str, dict[str, Any]] = {spec["type"]: spec for spec in NODE_TYPES}

# notify 节点的固定 python_callable（plugins/alerting.py 的 webhook 函数，签名 _post_webhook(message: str)）
NOTIFY_CALLABLE = "alerting._post_webhook"
# 任务失败告警回调（plugins/alerting.py），写入 default_args.on_failure_callback
FAILURE_CALLBACK = "alerting.send_failure_alert"


def list_node_types() -> list[dict[str, Any]]:
    """返回完整节点类型目录（供 /api/node-types）。"""
    return NODE_TYPES


def get_node_type(type_name: str) -> dict[str, Any] | None:
    return _TYPE_INDEX.get(type_name)


def required_fields(type_name: str) -> list[str]:
    spec = get_node_type(type_name)
    if spec is None:
        return []
    return [f["name"] for f in spec["fields"] if f.get("required")]


def field_defaults(type_name: str) -> dict[str, Any]:
    """返回该类型声明了默认值的可选参数（转换层据此补齐缺省值）。"""
    spec = get_node_type(type_name)
    if spec is None:
        return {}
    return {f["name"]: f["default"] for f in spec["fields"] if f.get("default") is not None}
