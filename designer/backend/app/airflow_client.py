"""Airflow REST API v2 客户端（designer-api 后端专用）。

- base_url 取 env AIRFLOW_API_URL（默认 http://airflow-apiserver:8080），timeout 10s
- 认证：Airflow 3 FAB 令牌，POST {base}/auth/token {username,password} → access_token；
  进程内缓存，API 返回 401 时自动重取令牌并重试一次
- 连接/响应异常统一抛 AirflowUnavailable（端点层转 503 {detail: "Airflow 不可达：..."}）；
  404 抛 AirflowNotFound（端点层转 404）
- 所有方法对 dag_id / run_id 做合法字符校验（防路径注入）
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any
from urllib.parse import quote

import httpx

log = logging.getLogger(__name__)

# Airflow 标识符（dag_id / run_id / task_id）允许字母数字与 _ . -，其余一律拒绝
_ID_RE = re.compile(r"^[A-Za-z0-9_.\-]+$")
_RUN_ID_RE = re.compile(r"[A-Za-z0-9_.:+\-]+")

DEFAULT_BASE_URL = "http://airflow-apiserver:8080"
DEFAULT_TIMEOUT = 10.0


class AirflowUnavailable(Exception):
    """Airflow API 网络不可达或响应异常（端点层转 503）。"""


class AirflowNotFound(Exception):
    """Airflow API 返回 404（端点层转 404）。"""


def _check_id(kind: str, value: str) -> None:
    pattern = _RUN_ID_RE if kind == "run_id" else _ID_RE
    if not isinstance(value, str) or not value or not pattern.fullmatch(value):
        raise ValueError(f"非法 {kind}：{value!r}（仅允许字母、数字、下划线、点、中划线）")


class AirflowClient:
    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.base_url = (base_url or os.environ.get("AIRFLOW_API_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.username = username or os.environ.get("AIRFLOW_API_USERNAME", "admin")
        self.password = password or os.environ.get("AIRFLOW_API_PASSWORD", "admin123")
        self._http = httpx.Client(base_url=self.base_url, timeout=timeout)
        self._token: str | None = None

    def close(self):
        self._http.close()

    # ---------- 认证 ----------

    def _fetch_token(self) -> str:
        try:
            resp = self._http.post("/auth/token", json={"username": self.username, "password": self.password})
        except httpx.HTTPError as exc:
            raise AirflowUnavailable(f"认证请求失败：{exc}") from exc
        # Airflow 3 FAB 创建令牌返回 201 Created（不是 200），任何 2xx 都视为成功
        if resp.status_code >= 400:
            raise AirflowUnavailable(f"认证失败：HTTP {resp.status_code}")
        try:
            token = resp.json().get("access_token")
        except ValueError as exc:
            raise AirflowUnavailable(f"认证响应不是合法 JSON：{exc}") from exc
        if not token:
            raise AirflowUnavailable("认证响应缺少 access_token")
        return str(token)

    def _headers(self, force_refresh: bool = False) -> dict[str, str]:
        if self._token is None or force_refresh:
            self._token = self._fetch_token()
        return {"Authorization": f"Bearer {self._token}"}

    # ---------- 底层请求 ----------

    def _request(self, method: str, path: str, *, params: Any = None, json: Any = None) -> Any:
        try:
            resp = self._http.request(method, path, params=params, json=json, headers=self._headers())
        except httpx.HTTPError as exc:
            raise AirflowUnavailable(str(exc)) from exc
        if resp.status_code == 401:
            log.info("Airflow API 返回 401（token 失效），重新获取令牌后重试一次")
            try:
                resp = self._http.request(
                    method, path, params=params, json=json, headers=self._headers(force_refresh=True)
                )
            except httpx.HTTPError as exc:
                raise AirflowUnavailable(str(exc)) from exc
        if resp.status_code == 404:
            raise AirflowNotFound(f"Airflow API 404：{method} {path}")
        if resp.status_code >= 400:
            raise AirflowUnavailable(f"HTTP {resp.status_code}: {resp.text[:200]}")
        try:
            return resp.json()
        except ValueError as exc:
            raise AirflowUnavailable(f"响应不是合法 JSON：{exc}") from exc

    @staticmethod
    def _unwrap_list(data: Any, key: str) -> list[dict[str, Any]]:
        """Airflow 3 v2 集合端点返回数组；兼容 Airflow 2 的 {key: [...], total_entries} 包装。"""
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and isinstance(data.get(key), list):
            return data[key]
        raise AirflowUnavailable(f"响应结构不符合预期（缺少 {key} 列表）")

    # ---------- 业务方法 ----------

    def list_runs(self, dag_id: str, limit: int = 10) -> list[dict[str, Any]]:
        _check_id("dag_id", dag_id)
        data = self._request(
            "GET",
            f"/api/v2/dags/{dag_id}/dagRuns",
            params={"limit": limit, "order_by": "-start_date"},
        )
        return self._unwrap_list(data, "dag_runs")

    def trigger(self, dag_id: str, conf: dict[str, Any] | None = None) -> dict[str, Any]:
        _check_id("dag_id", dag_id)
        # Airflow 3.3 的 dagRuns 要求显式携带 logical_date 字段（可为 null，实测 3.3.2）
        data = self._request(
            "POST", f"/api/v2/dags/{dag_id}/dagRuns", json={"logical_date": None, "conf": conf or {}}
        )
        if not isinstance(data, dict):
            raise AirflowUnavailable("触发响应不是 JSON 对象")
        return data

    def list_task_instances(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        _check_id("dag_id", dag_id)
        _check_id("run_id", run_id)
        data = self._request(
            "GET", f"/api/v2/dags/{quote(dag_id, safe='')}/dagRuns/{quote(run_id, safe='')}/taskInstances"
        )
        return self._unwrap_list(data, "task_instances")

    def count_active_runs(self, dag_id: str) -> int:
        """running + queued 的 dag run 数量（判断删除/回滚前是否有进行中运行）。"""
        _check_id("dag_id", dag_id)
        data = self._request(
            "GET",
            f"/api/v2/dags/{dag_id}/dagRuns",
            params=[("state", "running"), ("state", "queued"), ("limit", 100)],
        )
        if isinstance(data, dict) and isinstance(data.get("total_entries"), int):
            return data["total_entries"]
        return len(self._unwrap_list(data, "dag_runs"))
