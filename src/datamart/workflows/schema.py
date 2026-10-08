"""DesignerDag 数据模型与图校验（pydantic v2，不依赖 Airflow）。"""

from __future__ import annotations

import re
from typing import Any

from croniter import croniter
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

from datamart.workflows import catalog
from datamart.workflows.params import PARAM_MODELS

_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class ValidationIssue(BaseModel):
    """结构化校验错误（供 /api/validate 返回，node_id/field 可为空表示 DAG 级错误）。"""

    node_id: str | None = None
    field: str | None = None
    message: str


class NodePosition(BaseModel):
    x: float = 0.0
    y: float = 0.0


class DesignerNode(BaseModel):
    """画布节点：id 即 task_id，type 对应 catalog 中的节点类型，params 为节点参数。"""

    id: str
    type: str
    params: dict[str, Any] = Field(default_factory=dict)
    position: NodePosition = Field(default_factory=NodePosition)

    @field_validator("id")
    @classmethod
    def _id_format(cls, v: str) -> str:
        if not _ID_RE.fullmatch(v):
            raise ValueError(f"节点 id '{v}' 非法：需匹配 ^[a-z][a-z0-9_]*$（小写字母开头的标识符）")
        return v


class DesignerEdge(BaseModel):
    """依赖连线：upstream -> downstream。"""

    upstream: str
    downstream: str


class DesignerDagBase(BaseModel):
    """字段级校验（格式/类型/默认值），不含图语义校验。

    /api/validate 用它做第一步解析，以便收集结构化的图级错误（collect_issues）。
    """

    dag_id: str
    description: str | None = None
    schedule: str | None = None  # cron 字符串；None 表示仅手动触发
    retries: int = Field(default=2, ge=0, le=10)
    retry_delay_seconds: int = Field(default=300, gt=0)
    alert_on_failure: bool = True
    nodes: list[DesignerNode] = Field(min_length=1)
    edges: list[DesignerEdge] = Field(default_factory=list)

    @field_validator("dag_id")
    @classmethod
    def _dag_id_format(cls, v: str) -> str:
        if not _ID_RE.fullmatch(v):
            raise ValueError(f"dag_id '{v}' 非法：需匹配 ^[a-z][a-z0-9_]*$（小写字母开头的标识符）")
        return v

    @field_validator("schedule")
    @classmethod
    def _schedule(cls, value):
        if value is None or not value.strip():
            return None
        value = value.strip()
        aliases = {
            "@once",
            "@continuous",
            "@hourly",
            "@daily",
            "@midnight",
            "@weekly",
            "@monthly",
            "@yearly",
            "@annually",
        }
        if value not in aliases and (len(value.split()) != 5 or not croniter.is_valid(value)):
            raise ValueError("schedule 必须是有效的五段 cron 或 Airflow 调度别名")
        return value


class DesignerDag(DesignerDagBase):
    """设计器 DAG 定义（唯一事实源为 YAML 文件，本模型是其 JSON 形态）。"""

    @model_validator(mode="after")
    def _check_graph(self) -> DesignerDag:
        issues = collect_issues(self)
        if issues:
            raise ValueError("; ".join(issue.message for issue in issues))
        for node in self.nodes:
            node.params = PARAM_MODELS[node.type].model_validate(node.params).model_dump(exclude_unset=True)
        return self


def collect_issues(dag: DesignerDagBase) -> list[ValidationIssue]:
    """图级校验：重复 id、未知类型、必填参数、边引用、自环、悬空节点、环。"""
    issues: list[ValidationIssue] = []
    node_ids = [n.id for n in dag.nodes]

    seen: set[str] = set()
    for nid in node_ids:
        if nid in seen:
            issues.append(ValidationIssue(node_id=nid, field="id", message=f"节点 id 重复：'{nid}'"))
        seen.add(nid)
    id_set = set(node_ids)

    for node in dag.nodes:
        if catalog.get_node_type(node.type) is None:
            issues.append(
                ValidationIssue(
                    node_id=node.id, field="type", message=f"节点 '{node.id}' 类型未知：'{node.type}'"
                )
            )
            continue
        try:
            PARAM_MODELS[node.type].model_validate(node.params)
        except ValidationError as exc:
            for error in exc.errors():
                name = str(error["loc"][0]) if error["loc"] else "params"
                hint = f"缺少必填参数 '{name}'" if error["type"] == "missing" else error["msg"]
                issues.append(
                    ValidationIssue(
                        node_id=node.id, field=name, message=f"节点 '{node.id}' 参数 '{name}': {hint}"
                    )
                )

    connected: set[str] = set()
    for edge in dag.edges:
        for endpoint in (edge.upstream, edge.downstream):
            if endpoint not in id_set:
                issues.append(ValidationIssue(field="edges", message=f"边引用了不存在的节点：'{endpoint}'"))
        if edge.upstream == edge.downstream:
            issues.append(
                ValidationIssue(
                    node_id=edge.upstream, field="edges", message=f"节点 '{edge.upstream}' 不允许自环"
                )
            )
        connected.update((edge.upstream, edge.downstream))

    if len(dag.nodes) > 1:
        for nid in node_ids:
            if nid not in connected:
                issues.append(ValidationIssue(node_id=nid, message=f"悬空节点：'{nid}' 未与任何节点连线"))

    cycle = _find_cycle(dag)
    if cycle:
        issues.append(ValidationIssue(field="edges", message=f"依赖存在环：{' -> '.join(cycle)}"))
    return issues


def _find_cycle(dag: DesignerDagBase) -> list[str] | None:
    """DFS 三色标记找环，返回环上的节点路径（无环返回 None）。"""
    adj: dict[str, list[str]] = {}
    for edge in dag.edges:
        adj.setdefault(edge.upstream, []).append(edge.downstream)

    white, gray, black = 0, 1, 2
    color = {n.id: white for n in dag.nodes}
    stack: list[str] = []

    def dfs(u: str) -> list[str] | None:
        color[u] = gray
        stack.append(u)
        for v in adj.get(u, []):
            if color.get(v, white) == gray:
                return stack[stack.index(v) :] + [v]
            if color.get(v, white) == white:
                found = dfs(v)
                if found:
                    return found
        stack.pop()
        color[u] = black
        return None

    for node in dag.nodes:
        if color[node.id] == white:
            found = dfs(node.id)
            if found:
                return found
    return None
