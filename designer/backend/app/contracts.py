"""HTTP request/response contracts, independent of storage implementations."""

from typing import Literal

from datamart.workflows.schema import DesignerDag
from pydantic import BaseModel, Field, model_validator


class ExpectedVersion(BaseModel):
    client_revision: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    client_mtime: float | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def require_version(self):
        if self.client_revision is None and self.client_mtime is None:
            raise ValueError("更新必须携带 client_revision 或 client_mtime")
        return self


class DesignerDagUpdate(DesignerDag, ExpectedVersion):
    pass


class RollbackBody(ExpectedVersion):
    version_id: str


class DagDocumentResponse(DesignerDag):
    mtime: float
    revision: str


class DagMutationResponse(BaseModel):
    ok: Literal[True] = True
    dag_id: str
    mtime: float
    revision: str


class SqlSuccess(BaseModel):
    ok: Literal[True]
    duration_ms: float


class SqlFailure(BaseModel):
    ok: Literal[False]
    error: str
