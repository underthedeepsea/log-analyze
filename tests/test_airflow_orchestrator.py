from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


@dataclass
class FakeAirflow:
    status: int = 200
    post_status: int | None = None
    response: dict[str, object] = field(default_factory=dict)
    last_method: str | None = None
    last_path: str | None = None
    last_json: dict[str, object] | None = None

    def __post_init__(self) -> None:
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args: object) -> None:
                return

            def _reply(self, status: int, payload: dict[str, object]) -> None:
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def _record(self) -> None:
                parent.last_method = self.command
                parent.last_path = self.path
                length = int(self.headers.get("Content-Length") or 0)
                parent.last_json = json.loads(self.rfile.read(length) or b"{}") if length else None

            def do_GET(self) -> None:  # noqa: N802
                self._record()
                if parent.status != 200:
                    self._reply(parent.status, parent.response or {"detail": "sensitive upstream text"})
                    return
                if "/dagRuns/" in self.path:
                    self._reply(200, parent.response or {
                        "dag_run_id": "logrisk__job-1",
                        "state": "running",
                        "conf": {"job_id": "job-1"},
                    })
                else:
                    self._reply(200, parent.response or {"dag_id": "logrisk_analysis", "is_paused": False})

            def do_POST(self) -> None:  # noqa: N802
                self._record()
                status = parent.post_status if parent.post_status is not None else parent.status
                if status != 200:
                    self._reply(status, parent.response or {"detail": "sensitive upstream text"})
                    return
                self._reply(200, parent.response or {
                    "dag_run_id": str((parent.last_json or {}).get("dag_run_id") or ""),
                    "state": "queued",
                    "conf": (parent.last_json or {}).get("conf") or {},
                })

            def do_PATCH(self) -> None:  # noqa: N802
                self._record()
                self._reply(parent.status, parent.response or {
                    "dag_run_id": "logrisk__job-1",
                    "state": "failed",
                    "conf": {"job_id": "job-1"},
                })

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


@pytest.fixture
def fake_airflow() -> FakeAirflow:
    server = FakeAirflow()
    yield server
    server.close()


def test_airflow_trigger_sends_only_sanitized_ids(fake_airflow: FakeAirflow) -> None:
    from logrisk.orchestration.airflow import AirflowOrchestrator

    client = AirflowOrchestrator(fake_airflow.url, "logrisk_analysis", timeout=10)

    result = client.trigger("job-1", "orchestration-1", "request-1")

    assert result.external_run_id == "logrisk__job-1"
    assert fake_airflow.last_json == {
        "dag_run_id": "logrisk__job-1",
        "conf": {"job_id": "job-1", "orchestration_run_id": "orchestration-1", "request_id": "request-1"},
    }


def test_airflow_input_trigger_sends_only_stable_input_ids(fake_airflow: FakeAirflow) -> None:
    from logrisk.orchestration.airflow import AirflowOrchestrator

    client = AirflowOrchestrator(fake_airflow.url, "logrisk_input_preprocess", timeout=10)
    result = client.trigger_input("input_job-1", "input_orchestration-1", "request-1")

    assert result.external_run_id == "logrisk_input__input_job-1"
    assert result.input_job_id == "input_job-1"
    assert fake_airflow.last_json == {
        "dag_run_id": "logrisk_input__input_job-1",
        "conf": {
            "input_job_id": "input_job-1",
            "input_orchestration_run_id": "input_orchestration-1",
            "request_id": "request-1",
        },
    }


def test_airflow_agent_trigger_sends_only_stable_agent_ids(fake_airflow: FakeAirflow) -> None:
    from logrisk.orchestration.airflow import AirflowOrchestrator

    fake_airflow.response = {
        "dag_run_id": "logrisk_agent__run-1",
        "state": "queued",
        "conf": {"agent_run_id": "run-1", "request_id": "request-1"},
    }
    result = AirflowOrchestrator(fake_airflow.url, "logrisk_agent_run").trigger_agent("run-1", "request-1")

    assert result.agent_run_id == "run-1"
    assert fake_airflow.last_json == {
        "dag_run_id": "logrisk_agent__run-1",
        "conf": {"agent_run_id": "run-1", "request_id": "request-1"},
    }


@pytest.mark.parametrize(
    ("status", "code"),
    [(401, "airflow_auth_failed"), (403, "airflow_access_denied"), (404, "airflow_dag_not_found")],
)
def test_airflow_error_is_stable_and_never_includes_upstream_content(fake_airflow: FakeAirflow, status: int, code: str) -> None:
    from logrisk.orchestration.airflow import AirflowOrchestrator, AirflowOrchestratorError

    fake_airflow.status = status
    fake_airflow.response = {"detail": "Authorization: Bearer airflow-secret"}

    with pytest.raises(AirflowOrchestratorError) as raised:
        AirflowOrchestrator(fake_airflow.url, "logrisk_analysis").health()

    assert raised.value.code == code
    assert "airflow-secret" not in str(raised.value)


def test_airflow_cancel_uses_patch_and_exposes_only_run_metadata(fake_airflow: FakeAirflow) -> None:
    from logrisk.orchestration.airflow import AirflowOrchestrator

    result = AirflowOrchestrator(fake_airflow.url, "logrisk_analysis").cancel("logrisk__job-1")

    assert fake_airflow.last_method == "PATCH"
    assert fake_airflow.last_json == {"state": "failed"}
    assert result.state == "failed"


def test_airflow_duplicate_run_is_idempotent_only_for_the_same_safe_ids(fake_airflow: FakeAirflow) -> None:
    from logrisk.orchestration.airflow import AirflowOrchestrator

    fake_airflow.post_status = 409
    fake_airflow.response = {
        "dag_run_id": "logrisk__job-1",
        "state": "running",
        "conf": {"job_id": "job-1", "orchestration_run_id": "orchestration-1", "request_id": "request-1"},
    }

    result = AirflowOrchestrator(fake_airflow.url, "logrisk_analysis").trigger(
        "job-1", "orchestration-1", "request-1"
    )

    assert result.external_run_id == "logrisk__job-1"
    assert result.state == "running"


def test_airflow_rejects_missing_credentials_and_invalid_run_response_without_leaking_values(fake_airflow: FakeAirflow) -> None:
    from logrisk.orchestration.airflow import AirflowOrchestrator, AirflowOrchestratorError

    with pytest.raises(AirflowOrchestratorError, match="未配置") as missing_credentials:
        AirflowOrchestrator(fake_airflow.url, "logrisk_analysis", authorization_env="AIRFLOW_TEST_SECRET").health()
    assert missing_credentials.value.code == "airflow_credentials_missing"

    fake_airflow.response = {"dag_run_id": "logrisk__job-1", "conf": {"job_id": "job-1"}}
    with pytest.raises(AirflowOrchestratorError) as invalid:
        AirflowOrchestrator(fake_airflow.url, "logrisk_analysis").trigger("job-1", "orchestration-1", "request-1")
    assert invalid.value.code == "airflow_invalid_response"
