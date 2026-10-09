"""Application use cases; HTTP routing contains no persistence or SQL orchestration."""

import logging
import time

import psycopg2
from filelock import FileLock

from app import auth
from app.airflow_client import AirflowNotFound, AirflowUnavailable

log = logging.getLogger(__name__)


class LoginFailed(Exception):
    pass


class UserService:
    def login(self, username, password):
        record = next((item for item in auth.load_users() if item.get("username") == username), None)
        if record is None or not auth.verify_password(password, str(record.get("password", ""))):
            auth.write_audit(user=username, role="-", method="POST", path="/api/auth/login", status=401)
            raise LoginFailed("用户名或密码错误")
        role = record.get("role") if record.get("role") in auth.ROLES else "viewer"
        token, expires = auth.issue_token(username, role)
        auth.write_audit(user=username, role=role, method="POST", path="/api/auth/login", status=200)
        return {"token": token, "username": username, "role": role, "expires_at": expires}

    def list(self):
        return [{"username": item.get("username"), "role": item.get("role")} for item in auth.load_users()]

    def upsert(self, username, password, role):
        if role not in auth.ROLES:
            raise ValueError(f"非法角色：{role}")
        path = auth.users_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(path) + ".lock", timeout=10):
            users = auth.load_users()
            record = next((item for item in users if item.get("username") == username), None)
            if record is None:
                record = {"username": username}
                users.append(record)
            record.update(password=auth.hash_password(password), role=role)
            auth.save_users(users)
        return {"ok": True, "username": username, "role": role}

    def delete(self, username):
        path = auth.users_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(path) + ".lock", timeout=10):
            users = auth.load_users()
            remaining = [item for item in users if item.get("username") != username]
            if len(remaining) == len(users):
                raise FileNotFoundError(f"用户不存在：{username}")
            auth.save_users(remaining)
        return {"ok": True}


class ActiveRunsConflict(Exception):
    def __init__(self, count):
        self.count = count
        super().__init__(f"存在 {count} 个进行中的运行（running/queued）")


class DagService:
    def __init__(self, store, airflow):
        self.store = store
        self.airflow = airflow

    def load(self, dag_id):
        document = self.store.load_document(dag_id)
        return {**document.dag.model_dump(), **document.metadata()}

    def create(self, dag):
        document = self.store.create_document(dag)
        return {"ok": True, "dag_id": dag.dag_id, **document.metadata()}

    def update(self, dag_id, dag, client_revision, client_mtime):
        if dag.dag_id != dag_id:
            raise ValueError("dag_id 不可修改（body.dag_id 必须与路径一致）")
        document = self.store.update_document(dag, client_revision=client_revision, client_mtime=client_mtime)
        return {"ok": True, "dag_id": dag_id, **document.metadata()}

    def delete(self, dag_id, force=False):
        if not self.store.exists(dag_id):
            raise FileNotFoundError(dag_id)
        try:
            active = self.airflow.count_active_runs(dag_id)
        except AirflowNotFound:
            active = 0  # A saved draft may not yet be parsed by Airflow.
        except AirflowUnavailable:
            log.warning("Airflow 不可达，按既有删除策略保留存储清理能力", exc_info=True)
            active = 0
        if active and not force:
            raise ActiveRunsConflict(active)
        self.store.delete(dag_id)
        return {"ok": True}

    def trigger(self, dag_id, conf):
        if not self.store.exists(dag_id):
            raise FileNotFoundError(dag_id)
        result = self.airflow.trigger(dag_id, conf)
        return {"run_id": result.get("dag_run_id") or result.get("run_id"), "state": result.get("state")}

    def rollback(self, dag_id, version_id, client_revision, client_mtime):
        document, snapshot_id, restored_from = self.store.restore_document(
            dag_id,
            version_id,
            client_revision=client_revision,
            client_mtime=client_mtime,
        )
        return {"ok": True, "version_id": snapshot_id, "restored_from": restored_from, **document.metadata()}


class SqlCheckService:
    def __init__(self, dsn, connect=psycopg2.connect):
        self.dsn = dsn
        self.connect = connect

    def check(self, statement, conn_id):
        if conn_id != "warehouse":
            raise ValueError(f"不支持的 conn_id：{conn_id}（仅支持 warehouse）")
        statement = statement.strip().rstrip(";").strip()
        if not statement:
            raise ValueError("SQL 不能为空")
        # psycopg2 无参 execute 走简单查询协议：语句内分号会变成堆叠语句被全部执行
        if ";" in statement:
            raise ValueError("仅允许单条语句（检测到语句内分号）")
        try:
            connection = self.connect(self.dsn)
        except psycopg2.Error as exc:
            return {"ok": False, "error": str(exc)[:500]}
        try:
            # 会话级只读：DML/DDL 一律被数据库拒绝，EXPLAIN 不产生任何写副作用
            connection.set_session(readonly=True)
            started = time.perf_counter()
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = '10s'")
                cursor.execute(f"EXPLAIN {statement}")
            return {"ok": True, "duration_ms": round((time.perf_counter() - started) * 1000, 1)}
        except psycopg2.Error as exc:
            return {"ok": False, "error": str(exc)[:500]}
        finally:
            # Even successful EXPLAIN requests never commit their session.
            connection.rollback()
            connection.close()
