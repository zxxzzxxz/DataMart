"""节点类型目录：每种类型声明底层 operator 与参数表单 schema（驱动前端属性表单）。

新增类型 = 在 NODE_TYPES 注册一条 + 底层 operator/函数实现，前端零改动。
字段定义结构：name / label / kind(text|textarea|select|number|checkbox) / required / default /
options / help。
所有类型共享 ADVANCED_FIELDS（trigger_rule / retries）：可选高级参数，
默认值经 field_defaults 合并进 task dict；round-trip 时值等于默认值的参数不保留在 params 中。
"""

from copy import deepcopy
from typing import Any

from datamart.workflows.params import PARAM_MODELS

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
    {
        "type": "bash",
        "label": "Bash 脚本",
        "operator": "airflow.providers.standard.operators.bash.BashOperator",
        "fields": [
            {
                "name": "bash_command",
                "label": "Bash 命令",
                "kind": "textarea",
                "required": True,
                "default": None,
                "options": None,
                "help": "在 Airflow worker 容器内执行",
            },
        ],
    },
    {
        "type": "http",
        "label": "HTTP 请求",
        "operator": "airflow.providers.http.operators.http.HttpOperator",
        "fields": [
            {
                "name": "endpoint",
                "label": "请求地址",
                "kind": "text",
                "required": True,
                "default": None,
                "options": None,
                "help": "建议绝对 URL，相对路径需 http_default 连接",
            },
            {
                "name": "method",
                "label": "请求方法",
                "kind": "select",
                "required": False,
                "default": "GET",
                "options": ["GET", "POST", "PUT", "PATCH", "DELETE"],
            },
            {
                "name": "data",
                "label": "请求体",
                "kind": "textarea",
                "required": False,
                "default": None,
                "options": None,
                "help": "可选，随请求发送的报文内容",
            },
            {
                "name": "headers",
                "label": "请求头",
                "kind": "textarea",
                "required": False,
                "default": None,
                "options": None,
                "help": '可选，JSON 字符串，如 {"Authorization":"Bearer x"}',
            },
        ],
    },
    {
        "type": "sensor_sql",
        "label": "SQL 传感器",
        "operator": "airflow.providers.common.sql.sensors.sql.SqlSensor",
        "fields": [
            {
                "name": "sql",
                "label": "探测 SQL",
                "kind": "textarea",
                "required": True,
                "default": None,
                "options": None,
                "help": "返回空结果集=条件未满足，持续等待",
            },
            {
                "name": "conn_id",
                "label": "数据库连接",
                "kind": "select",
                "required": False,
                "default": "warehouse",
                "options": ["warehouse"],
            },
            {
                "name": "poke_interval",
                "label": "探测间隔（秒）",
                "kind": "number",
                "required": False,
                "default": 60,
                "options": None,
            },
            {
                "name": "timeout",
                "label": "超时时间（秒）",
                "kind": "number",
                "required": False,
                "default": 3600,
                "options": None,
            },
            {
                "name": "mode",
                "label": "调度模式",
                "kind": "select",
                "required": False,
                "default": "reschedule",
                "options": ["poke", "reschedule"],
            },
        ],
    },
    {
        "type": "empty",
        "label": "空任务（汇聚/占位）",
        "operator": "airflow.providers.standard.operators.empty.EmptyOperator",
        "fields": [],
    },
]

# 所有节点类型共享的可选高级字段（追加在每种类型的 fields 末尾）
ADVANCED_FIELDS: list[dict[str, Any]] = [
    {
        "name": "trigger_rule",
        "label": "触发规则",
        "kind": "select",
        "required": False,
        "default": "all_success",
        "options": [
            "all_success",
            "all_done",
            "all_failed",
            "none_failed",
            "one_success",
            "one_failed",
            "none_skipped",
        ],
        "help": "上游任务处于何种状态时本任务可触发",
    },
    {
        "name": "retries",
        "label": "重试次数",
        "kind": "number",
        "required": False,
        "default": None,
        "options": None,
        "help": "0-10；留空则继承 DAG 级设置",
    },
]

for _spec in NODE_TYPES:
    _spec["fields"] = _spec["fields"] + ADVANCED_FIELDS
    _spec["parameter_schema"] = PARAM_MODELS[_spec["type"]].model_json_schema()

_TYPE_INDEX: dict[str, dict[str, Any]] = {spec["type"]: spec for spec in NODE_TYPES}

# notify 节点的固定 python_callable（plugins/alerting.py 的 webhook 函数，签名 _post_webhook(message: str)）
NOTIFY_CALLABLE = "alerting._post_webhook"
# 任务失败告警回调（plugins/alerting.py），写入 default_args.on_failure_callback
FAILURE_CALLBACK = "alerting.send_failure_alert"


def list_node_types() -> list[dict[str, Any]]:
    """返回完整节点类型目录（供 /api/node-types）。"""
    return deepcopy(NODE_TYPES)


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
