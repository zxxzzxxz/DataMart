"""FastAPI composition root: configuration, resources, lifecycle and error mapping."""

from contextlib import asynccontextmanager

import psycopg2
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from filelock import Timeout

from app import auth
from app.airflow_client import AirflowClient, AirflowNotFound, AirflowUnavailable
from app.auth_routes import router as auth_router
from app.dag_routes import router as dag_router
from app.dependencies import Resources, resolve_authorization
from app.services import ActiveRunsConflict, DagService, LoginFailed, SqlCheckService, UserService
from app.settings import Settings
from app.sql_routes import router as sql_router
from app.store import DagStore, MtimeConflictError


def create_app(settings=None, *, airflow=None, sql_connect=psycopg2.connect):
    settings = settings or Settings.from_env()
    client = airflow or AirflowClient(
        settings.airflow_url, settings.airflow_username, settings.airflow_password
    )

    @asynccontextmanager
    async def lifespan(_app):
        try:
            yield
        finally:
            close = getattr(client, "close", None)
            if close:
                close()

    application = FastAPI(title="DataMart DAG Designer API", lifespan=lifespan)
    application.state.resources = Resources(
        dags=DagService(DagStore(settings.dags_dir, settings.history_dir), client),
        sql=SqlCheckService(settings.warehouse_dsn, sql_connect),
        users=UserService(),
    )
    application.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

    @application.middleware("http")
    async def audit(request: Request, call_next):
        response = await call_next(request)
        if request.method in {"POST", "PUT", "DELETE"} and request.url.path != "/api/auth/login":
            user = resolve_authorization(request.headers.get("authorization"))
            auth.write_audit(
                user=user.username if user else "-",
                role=user.role if user else "-",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                detail={"dag_id": request.path_params["dag_id"]} if "dag_id" in request.path_params else None,
            )
        return response

    def handler(status):
        async def respond(_request, exc):
            body = {"detail": str(exc)}
            if isinstance(exc, AirflowUnavailable):
                body["detail"] = f"Airflow 不可达：{exc}"
            if isinstance(exc, ActiveRunsConflict):
                body["active_runs"] = exc.count
            return JSONResponse(status_code=status, content=body)

        return respond

    for exception, status in [
        (FileNotFoundError, 404),
        (FileExistsError, 409),
        (MtimeConflictError, 409),
        (ActiveRunsConflict, 409),
        (AirflowNotFound, 404),
        (AirflowUnavailable, 503),
        (Timeout, 503),
        (ValueError, 400),
        (LoginFailed, 401),
    ]:
        application.add_exception_handler(exception, handler(status))
    for router in (auth_router, dag_router, sql_router):
        application.include_router(router)
    return application


app = create_app()
