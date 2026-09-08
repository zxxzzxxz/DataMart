"""DAG 设计器后端 API：DAG CRUD / 校验 / 节点类型目录。

- YAML 是唯一事实源：$DAGS_DIR/designer/*.yaml（容器内挂载项目 dags/ 目录）
- 渲染层插件：$PLUGINS_DIR 注入 sys.path 后 import dag_designer（plugins/dag_designer/）
- 认证：除 /api/health 外全部要求 `Authorization: Bearer $DESIGNER_TOKEN`
"""

import os
import sys

# 必须在 import dag_designer 之前注入插件目录
_PLUGINS_DIR = os.environ.get("PLUGINS_DIR", "/data/plugins")
if _PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLUGINS_DIR)

from dag_designer import catalog  # noqa: E402
from dag_designer.schema import DesignerDag, DesignerDagBase, collect_issues  # noqa: E402
from fastapi import Depends, FastAPI, Header, HTTPException  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from pydantic import ValidationError  # noqa: E402

from app.store import DagStore, MtimeConflictError  # noqa: E402

_DAGS_DIR = os.environ.get("DAGS_DIR", "/data/dags")
_TOKEN = os.environ.get("DESIGNER_TOKEN", "designer-dev-token")

app = FastAPI(title="DataMart DAG Designer API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 开发期放开
    allow_methods=["*"],
    allow_headers=["*"],
)
store = DagStore(_DAGS_DIR)


def require_token(authorization: str | None = Header(default=None)) -> None:
    if authorization != f"Bearer {_TOKEN}":
        raise HTTPException(status_code=401, detail="缺少或无效的 Authorization Bearer Token")


@app.get("/api/health")
def health():
    return {"ok": True}


@app.get("/api/node-types", dependencies=[Depends(require_token)])
def node_types() -> list:
    return catalog.list_node_types()


@app.get("/api/dags", dependencies=[Depends(require_token)])
def list_dags() -> list:
    return store.list()


@app.get("/api/dags/{dag_id}", dependencies=[Depends(require_token)])
def get_dag(dag_id: str):
    try:
        dag, mtime = store.load(dag_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"DAG 不存在：{dag_id}") from None
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from None
    return {**dag.model_dump(), "mtime": mtime}


@app.post("/api/dags", status_code=201, dependencies=[Depends(require_token)])
def create_dag(dag: DesignerDag):
    try:
        mtime = store.create(dag)
    except FileExistsError:
        raise HTTPException(status_code=409, detail=f"DAG 已存在：{dag.dag_id}") from None
    return {"ok": True, "dag_id": dag.dag_id, "mtime": mtime}


class DesignerDagUpdate(DesignerDag):
    """PUT 载荷：DAG 本体 + 乐观锁基準 mtime（body 字段，与前端契约一致）。"""

    client_mtime: float | None = None


@app.put("/api/dags/{dag_id}", dependencies=[Depends(require_token)])
def update_dag(dag_id: str, dag: DesignerDagUpdate):
    if dag.dag_id != dag_id:
        raise HTTPException(status_code=422, detail="dag_id 不可修改（body.dag_id 必须与路径一致）")
    try:
        mtime = store.update(dag, client_mtime=dag.client_mtime)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"DAG 不存在：{dag_id}") from None
    except MtimeConflictError:
        raise HTTPException(status_code=409, detail="文件已被他人修改，请刷新后重试（mtime 冲突）") from None
    return {"ok": True, "dag_id": dag_id, "mtime": mtime}


@app.delete("/api/dags/{dag_id}", dependencies=[Depends(require_token)])
def delete_dag(dag_id: str):
    try:
        store.delete(dag_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"DAG 不存在：{dag_id}") from None
    return {"ok": True}


@app.post("/api/validate", dependencies=[Depends(require_token)])
def validate(body: dict):
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
