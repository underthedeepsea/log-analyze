from __future__ import annotations

from typing import Any

from logrisk.server_risk.contracts import ServerRiskError, identifier, strict_object


def execute_report(conf: dict[str, Any], *, container: Any = None) -> dict[str, str]:
    strict_object(conf, {"report_id", "request_id"}, {"report_id", "request_id"})
    report_id, request_id = identifier(conf["report_id"]), identifier(conf["request_id"])
    if container is None:
        from logrisk_django.service_factory import get_container
        container = get_container()
    if container.config.migrate_database:
        raise ServerRiskError("worker_migration_forbidden", "生产Worker必须使用离线已迁移数据库")
    reports = container.server_risk.reports
    saved = reports.get(report_id)
    if saved["request_id"] != request_id:
        raise ServerRiskError("report_dispatch_scope_mismatch", "请求ID与锁定报告不匹配", 409)
    reports.execute(report_id)
    result = reports.get(report_id)
    return {"report_id": report_id, "status": result["status"], "request_id": request_id}
