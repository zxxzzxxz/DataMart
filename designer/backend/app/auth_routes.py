"""Authentication HTTP adapter."""

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.dependencies import require_role, resources

router = APIRouter()


class LoginBody(BaseModel):
    username: str
    password: str


class UserBody(LoginBody):
    role: str


@router.post("/api/auth/login")
def login(request: Request, body: LoginBody):
    return resources(request).users.login(body.username, body.password)


@router.get("/api/auth/users", dependencies=[Depends(require_role("admin"))])
def list_users(request: Request):
    return resources(request).users.list()


@router.post("/api/auth/users", dependencies=[Depends(require_role("admin"))])
def upsert_user(request: Request, body: UserBody):
    try:
        return resources(request).users.upsert(body.username, body.password, body.role)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.delete("/api/auth/users/{username}", dependencies=[Depends(require_role("admin"))])
def delete_user(request: Request, username: str):
    return resources(request).users.delete(username)
