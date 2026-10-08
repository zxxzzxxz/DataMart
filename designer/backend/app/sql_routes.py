"""SQL check HTTP adapter."""

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from app.contracts import SqlFailure, SqlSuccess
from app.dependencies import require_role, resources

router = APIRouter()


class CheckSqlBody(BaseModel):
    sql: str
    conn_id: str = "warehouse"


@router.post(
    "/api/check-sql", dependencies=[Depends(require_role("editor"))], response_model=SqlSuccess | SqlFailure
)
def check_sql(request: Request, body: CheckSqlBody):
    return resources(request).sql.check(body.sql, body.conn_id)
