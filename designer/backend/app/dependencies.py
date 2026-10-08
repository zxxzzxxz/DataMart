"""Request dependencies resolve resources owned by the application lifecycle."""

from dataclasses import dataclass
from hmac import compare_digest

from fastapi import Depends, Header, HTTPException, Request

from app import auth
from app.auth import AuthUser
from app.services import DagService, SqlCheckService, UserService


@dataclass
class Resources:
    dags: DagService
    sql: SqlCheckService
    users: UserService


def resources(request: Request) -> Resources:
    return request.app.state.resources


def get_store(request: Request):
    return resources(request).dags.store


def get_airflow_client(request: Request):
    return resources(request).dags.airflow


def resolve_authorization(authorization):
    if not authorization or not authorization.startswith("Bearer "):
        return None
    token = authorization.removeprefix("Bearer ")
    if compare_digest(token.encode(), auth.legacy_token().encode()):
        return AuthUser(username="token-admin", role="admin")
    return auth.verify_token(token)


def require_user(authorization: str | None = Header(default=None)) -> AuthUser:
    user = resolve_authorization(authorization)
    if user is None:
        raise HTTPException(status_code=401, detail="缺少或无效的 Authorization Bearer Token")
    return user


def require_role(min_role):
    def checker(user: AuthUser = Depends(require_user)) -> AuthUser:  # noqa: B008
        if not user.has_at_least(min_role):
            raise HTTPException(status_code=403, detail=f"权限不足：需要 {min_role} 及以上角色")
        return user

    return checker
