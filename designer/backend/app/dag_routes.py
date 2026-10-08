"""Workflow HTTP adapter and external-runtime response mapping."""

from datamart.workflows import catalog
from datamart.workflows.schema import DesignerDag, DesignerDagBase, collect_issues
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ValidationError

from app import airflow_client
from app.contracts import DagDocumentResponse, DagMutationResponse, DesignerDagUpdate, RollbackBody
from app.dependencies import get_airflow_client, get_store, require_role, require_user, resources

router = APIRouter()


@router.get("/api/health")
def health(request: Request):
    return {"ok": True}


@router.get("/api/node-types", dependencies=[Depends(require_user)])
def node_types(
    request: Request,
) -> list:
    return catalog.list_node_types()


@router.post("/api/validate", dependencies=[Depends(require_role("editor"))])
def validate(request: Request, body: dict):
    """dry-run 校验（不落盘）：返回 {"ok": bool, "errors": [{node_id, field, message}]}。

    两步解析：先字段级校验（DesignerDagBase，错误带 loc），通过后跑图级语义校验
    （collect_issues，错误天然带 node_id/field）。
    """
    try:
        shape = DesignerDagBase.model_validate(body)
    except ValidationError as exc:
        return {"ok": False, "errors": [_map_validation_error(body, err) for err in exc.errors()]}
    issues = collect_issues(shape)
    return {"ok": not issues, "errors": [issue.model_dump() for issue in issues]}


def _map_validation_error(body: dict, err: dict) -> dict:
    """pydantic 错误 → {node_id, field, message}；模型级错误的节点信息已含在 message 中。"""
    loc = err.get("loc", ())
    node_id = None
    if loc and loc[0] == "nodes" and len(loc) > 1 and isinstance(loc[1], int):
        nodes = body.get("nodes") if isinstance(body, dict) else None
        if isinstance(nodes, list) and len(nodes) > loc[1] and isinstance(nodes[loc[1]], dict):
            node_id = nodes[loc[1]].get("id")
    field = str(loc[-1]) if loc else None
    message = err.get("msg", "")
    if message.startswith("Value error, "):
        message = message[len("Value error, ") :]
    return {"node_id": node_id, "field": field, "message": message}


def _raise_airflow_http(exc: airflow_client.AirflowUnavailable) -> None:
    raise HTTPException(status_code=503, detail=f"Airflow 不可达：{exc}") from exc


def _raise_airflow_not_found(exc: airflow_client.AirflowNotFound) -> None:
    raise HTTPException(status_code=404, detail=str(exc)) from exc


def _raise_invalid_id(exc: ValueError) -> None:
    raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/api/dags/{dag_id}/runs", dependencies=[Depends(require_user)])
def list_dag_runs(request: Request, dag_id: str, limit: int = Query(10, ge=1, le=100)):
    """Airflow dag run 列表（按 start_date 倒序）。"""
    try:
        runs = get_airflow_client(request).list_runs(dag_id, limit=limit)
    except airflow_client.AirflowNotFound as exc:
        _raise_airflow_not_found(exc)
    except airflow_client.AirflowUnavailable as exc:
        _raise_airflow_http(exc)
    except ValueError as exc:
        _raise_invalid_id(exc)
    return {
        "runs": [
            {
                "run_id": r.get("dag_run_id") or r.get("run_id"),
                "state": r.get("state"),
                "logical_date": r.get("logical_date"),
                "start_date": r.get("start_date"),
                "end_date": r.get("end_date"),
                "conf": r.get("conf"),
            }
            for r in runs
        ]
    }


class TriggerBody(BaseModel):
    conf: dict | None = None


@router.get("/api/dags/{dag_id}/runs/{run_id}/tasks", dependencies=[Depends(require_user)])
def list_dag_run_tasks(request: Request, dag_id: str, run_id: str):
    """指定 dag run 的任务实例列表。"""
    try:
        tasks = get_airflow_client(request).list_task_instances(dag_id, run_id)
    except airflow_client.AirflowNotFound as exc:
        _raise_airflow_not_found(exc)
    except airflow_client.AirflowUnavailable as exc:
        _raise_airflow_http(exc)
    except ValueError as exc:
        _raise_invalid_id(exc)
    return {
        "tasks": [
            {
                "task_id": t.get("task_id"),
                "state": t.get("state"),
                "try_number": t.get("try_number"),
                "duration": t.get("duration"),
            }
            for t in tasks
        ]
    }


@router.get("/api/dags/{dag_id}/versions", dependencies=[Depends(require_user)])
def list_dag_versions(request: Request, dag_id: str):
    """版本列表（时间倒序）；无历史版本返回空列表。"""
    try:
        return {"versions": get_store(request).list_versions(dag_id)}
    except ValueError as exc:
        _raise_invalid_id(exc)


@router.get("/api/dags/{dag_id}/versions/{version_id}", dependencies=[Depends(require_user)])
def get_dag_version(request: Request, dag_id: str, version_id: str):
    """读取单个历史版本的完整 YAML 内容。"""
    try:
        return get_store(request).get_version(dag_id, version_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        _raise_invalid_id(exc)


@router.get("/api/dags", dependencies=[Depends(require_user)])
def list_dags(request: Request):
    return get_store(request).list()


@router.get("/api/dags/{dag_id}", dependencies=[Depends(require_user)], response_model=DagDocumentResponse)
def get_dag(request: Request, dag_id: str):
    return resources(request).dags.load(dag_id)


@router.post(
    "/api/dags",
    status_code=201,
    dependencies=[Depends(require_role("editor"))],
    response_model=DagMutationResponse,
)
def create_dag(request: Request, dag: DesignerDag):
    return resources(request).dags.create(dag)


@router.put(
    "/api/dags/{dag_id}", dependencies=[Depends(require_role("editor"))], response_model=DagMutationResponse
)
def update_dag(request: Request, dag_id: str, body: DesignerDagUpdate):
    return resources(request).dags.update(dag_id, body, body.client_revision, body.client_mtime)


@router.delete("/api/dags/{dag_id}", dependencies=[Depends(require_role("admin"))])
def delete_dag(request: Request, dag_id: str, force: bool = False):
    return resources(request).dags.delete(dag_id, force)


@router.post("/api/dags/{dag_id}/trigger", dependencies=[Depends(require_role("editor"))])
def trigger_dag(request: Request, dag_id: str, body: TriggerBody | None = None):
    return resources(request).dags.trigger(dag_id, body.conf if body else None)


@router.post("/api/dags/{dag_id}/rollback", dependencies=[Depends(require_role("editor"))])
def rollback_dag(request: Request, dag_id: str, body: RollbackBody):
    return resources(request).dags.rollback(dag_id, body.version_id, body.client_revision, body.client_mtime)
