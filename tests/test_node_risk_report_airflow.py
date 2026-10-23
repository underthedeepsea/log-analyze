from __future__ import annotations

import json
from types import SimpleNamespace
import pytest
from logrisk.orchestration.airflow import AirflowOrchestrator
from logrisk.server_risk.airflow import execute_report
from logrisk.server_risk.contracts import ServerRiskError
from tests.server_risk_fixtures import domain_services, batch, observation, report_request


def test_report_worker_stable_ids_no_migration_no_replay(tmp_path):
    service, now, _, _, fake = domain_services(tmp_path)
    service.observations.submit(batch("seed", observation(1, now["value"])), "fixture-actor")
    run = service.reports.create(report_request("worker"), "fixture-actor", request_id="request-worker")
    container = SimpleNamespace(config=SimpleNamespace(migrate_database=False), server_risk=service)
    assert execute_report({"report_id": run["report_id"], "request_id": "request-worker"}, container=container)["status"] == "completed"
    assert execute_report({"report_id": run["report_id"], "request_id": "request-worker"}, container=container)["status"] == "completed" and len(fake.calls) == 1
    with pytest.raises(ServerRiskError):
        execute_report({"report_id": run["report_id"], "request_id": "other"}, container=container)
    with pytest.raises(ServerRiskError):
        execute_report({"report_id": run["report_id"], "request_id": "request-worker", "model": "qwen"}, container=container)
    container.config.migrate_database = True
    with pytest.raises(ServerRiskError):
        execute_report({"report_id": run["report_id"], "request_id": "request-worker"}, container=container)


def test_report_airflow_conf_is_ids_only_and_dag_is_bounded():
    calls = []
    class Response:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def read(self):
            return json.dumps({"dag_run_id": "logrisk_report__report1", "state": "queued", "conf": {"report_id": "report1", "request_id": "request1"}}).encode()
    def opener(request, timeout):
        calls.append(json.loads(request.data)); return Response()
    run = AirflowOrchestrator("http://127.0.0.1:1234", "logrisk_node_risk_report", opener=opener).trigger_node_risk_report("report1", "request1")
    assert run.report_id == "report1" and calls == [{"dag_run_id": "logrisk_report__report1", "conf": {"report_id": "report1", "request_id": "request1"}}]
    from pathlib import Path
    source = Path("integrations/airflow/dags/logrisk_node_risk_report.py").read_text()
    assert '"retries": 0' in source and 'pool="logrisk_llm_pool"' in source and 'max_active_runs=1' in source and 'seconds=120' in source
