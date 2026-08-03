from __future__ import annotations

import json
import os
import re
import socket
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen


_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
_ENVIRONMENT_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class AirflowOrchestratorError(RuntimeError):
    """Safe, stable failure for an Airflow REST API operation."""

    def __init__(self, message: str, *, code: str, status_code: int = 502) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


@dataclass(frozen=True)
class AirflowHealth:
    dag_id: str
    is_paused: bool


@dataclass(frozen=True)
class AirflowRun:
    external_run_id: str
    state: str
    job_id: str | None
    orchestration_run_id: str | None
    request_id: str | None
    input_job_id: str | None = None
    input_orchestration_run_id: str | None = None


class AirflowOrchestrator:
    """Minimal Airflow 2.3 REST v1 client; DAG conf carries stable IDs only."""

    def __init__(
        self,
        base_url: str,
        dag_id: str,
        *,
        timeout: float = 10.0,
        authorization_env: str | None = None,
        opener: Callable[..., Any] | None = None,
    ) -> None:
        parsed = urlparse(str(base_url).strip().rstrip("/"))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("Airflow URL 必须是有效的 HTTP(S) 地址")
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
            raise ValueError("Airflow 超时必须为正数")
        if authorization_env and not _ENVIRONMENT_NAME.fullmatch(authorization_env):
            raise ValueError("Airflow 鉴权环境变量名无效")
        self.base_url = str(base_url).strip().rstrip("/")
        self.dag_id = self._identifier(dag_id, "DAG ID")
        self.timeout = float(timeout)
        self.authorization_env = authorization_env
        self.opener = opener or urlopen

    def health(self) -> AirflowHealth:
        payload = self._request("GET", self._dag_path())
        actual_dag = self._identifier(payload.get("dag_id") or self.dag_id, "Airflow DAG ID")
        if actual_dag != self.dag_id:
            raise AirflowOrchestratorError("Airflow 返回的 DAG 标识不匹配", code="airflow_invalid_response")
        return AirflowHealth(dag_id=actual_dag, is_paused=bool(payload.get("is_paused", False)))

    def trigger(self, job_id: str, orchestration_run_id: str, request_id: str) -> AirflowRun:
        job = self._identifier(job_id, "任务 ID")
        run = self._identifier(orchestration_run_id, "编排运行 ID")
        request = self._identifier(request_id, "请求 ID")
        external_run_id = self._identifier("logrisk__" + job, "Airflow DAG Run ID")
        conf = {"job_id": job, "orchestration_run_id": run, "request_id": request}
        try:
            payload = self._request("POST", self._dag_path("dagRuns"), {"dag_run_id": external_run_id, "conf": conf})
        except AirflowOrchestratorError as exc:
            if exc.code != "airflow_run_conflict":
                raise
            existing = self.get_run(external_run_id)
            if existing.job_id == job and existing.orchestration_run_id == run and existing.request_id == request:
                return existing
            raise AirflowOrchestratorError("Airflow DAG Run 标识已被其他任务占用", code="airflow_run_conflict", status_code=409) from exc
        result = self._run(payload)
        if result.external_run_id != external_run_id or result.job_id != job or result.orchestration_run_id != run:
            raise AirflowOrchestratorError("Airflow 返回的 DAG Run 信息无效", code="airflow_invalid_response")
        return result

    def trigger_input(self, input_job_id: str, input_orchestration_run_id: str, request_id: str) -> AirflowRun:
        """Trigger upload preprocessing with IDs only; raw file content stays in shared storage."""
        input_job = self._identifier(input_job_id, "输入任务 ID")
        run = self._identifier(input_orchestration_run_id, "输入编排运行 ID")
        request = self._identifier(request_id, "请求 ID")
        external_run_id = self._identifier("logrisk_input__" + input_job, "Airflow DAG Run ID")
        conf = {
            "input_job_id": input_job,
            "input_orchestration_run_id": run,
            "request_id": request,
        }
        try:
            payload = self._request("POST", self._dag_path("dagRuns"), {"dag_run_id": external_run_id, "conf": conf})
        except AirflowOrchestratorError as exc:
            if exc.code != "airflow_run_conflict":
                raise
            existing = self.get_run(external_run_id)
            if (
                existing.input_job_id == input_job
                and existing.input_orchestration_run_id == run
                and existing.request_id == request
            ):
                return existing
            raise AirflowOrchestratorError("Airflow DAG Run 标识已被其他任务占用", code="airflow_run_conflict", status_code=409) from exc
        result = self._run(payload)
        if (
            result.external_run_id != external_run_id
            or result.input_job_id != input_job
            or result.input_orchestration_run_id != run
        ):
            raise AirflowOrchestratorError("Airflow 返回的 DAG Run 信息无效", code="airflow_invalid_response")
        return result

    def get_run(self, external_run_id: str) -> AirflowRun:
        run = self._identifier(external_run_id, "Airflow DAG Run ID")
        return self._run(self._request("GET", self._dag_path("dagRuns", run)))

    def cancel(self, external_run_id: str) -> AirflowRun:
        run = self._identifier(external_run_id, "Airflow DAG Run ID")
        return self._run(self._request("PATCH", self._dag_path("dagRuns", run), {"state": "failed"}))

    def _request(self, method: str, path: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
        data = json.dumps(dict(payload), ensure_ascii=False, separators=(",", ":")).encode("utf-8") if payload is not None else None
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if self.authorization_env:
            authorization = os.environ.get(self.authorization_env)
            if not authorization:
                raise AirflowOrchestratorError("未配置 Airflow 鉴权环境变量", code="airflow_credentials_missing", status_code=503)
            headers["Authorization"] = authorization
        request = Request(self.base_url + path, data=data, headers=headers, method=method)
        try:
            with self.opener(request, timeout=self.timeout) as response:
                raw = response.read()
        except HTTPError as exc:
            raise self._http_error(exc.code, path) from exc
        except (URLError, TimeoutError, socket.timeout, OSError) as exc:
            raise AirflowOrchestratorError("无法连接 Airflow 服务", code="airflow_unavailable", status_code=503) from exc
        try:
            parsed = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AirflowOrchestratorError("Airflow 返回了无效 JSON", code="airflow_invalid_response") from exc
        if not isinstance(parsed, dict):
            raise AirflowOrchestratorError("Airflow 返回了无效响应对象", code="airflow_invalid_response")
        return parsed

    def _dag_path(self, *parts: str) -> str:
        values = ["/api/v1/dags", quote(self.dag_id, safe="")]
        values.extend(quote(str(part), safe="") for part in parts)
        return "/".join(values)

    @staticmethod
    def _identifier(value: Any, label: str) -> str:
        result = str(value or "").strip()
        if not _IDENTIFIER.fullmatch(result):
            raise ValueError(f"{label} 只能包含字母、数字、点、下划线、短横线和冒号")
        return result

    @staticmethod
    def _http_error(status: int, path: str) -> AirflowOrchestratorError:
        if status == 401:
            return AirflowOrchestratorError("Airflow 鉴权失败", code="airflow_auth_failed", status_code=502)
        if status == 403:
            return AirflowOrchestratorError("当前身份无权调用 Airflow", code="airflow_access_denied", status_code=502)
        if status == 404:
            code = "airflow_run_not_found" if "/dagRuns/" in path else "airflow_dag_not_found"
            return AirflowOrchestratorError("Airflow 目标不存在", code=code, status_code=404)
        if status == 409:
            return AirflowOrchestratorError("Airflow DAG Run 已存在", code="airflow_run_conflict", status_code=409)
        return AirflowOrchestratorError("Airflow 请求失败", code="airflow_http_error", status_code=502)

    def _run(self, payload: Mapping[str, Any]) -> AirflowRun:
        conf = payload.get("conf")
        if not isinstance(conf, Mapping):
            raise AirflowOrchestratorError("Airflow DAG Run 缺少安全配置快照", code="airflow_invalid_response")
        try:
            return AirflowRun(
                external_run_id=self._identifier(payload.get("dag_run_id"), "Airflow DAG Run ID"),
                state=self._identifier(payload.get("state"), "Airflow DAG Run 状态"),
                job_id=self._optional_identifier(conf.get("job_id"), "任务 ID"),
                orchestration_run_id=self._optional_identifier(conf.get("orchestration_run_id"), "编排运行 ID"),
                request_id=self._optional_identifier(conf.get("request_id"), "请求 ID"),
                input_job_id=self._optional_identifier(conf.get("input_job_id"), "输入任务 ID"),
                input_orchestration_run_id=self._optional_identifier(
                    conf.get("input_orchestration_run_id"), "输入编排运行 ID"
                ),
            )
        except ValueError as exc:
            raise AirflowOrchestratorError("Airflow DAG Run 响应字段无效", code="airflow_invalid_response") from exc

    @classmethod
    def _optional_identifier(cls, value: Any, label: str) -> str | None:
        return None if value is None else cls._identifier(value, label)
