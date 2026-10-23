from __future__ import annotations


def execute_node_risk_report(*, report_id: str, request_id: str) -> dict[str, str]:
    from logrisk.server_risk.airflow import execute_report
    return execute_report({"report_id": report_id, "request_id": request_id})
