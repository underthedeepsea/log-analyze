from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote, urlencode

from logrisk.server_risk.contracts import TOOL_CONTRACTS, tool_arguments


def list_tools() -> list[dict[str, Any]]:
    result = []
    for name, (properties, required, outputs) in TOOL_CONTRACTS.items():
        result.append({"name": name, "description": "LOGRISK 服务器风险：" + name.removeprefix("logrisk_"), "inputSchema": {"type": "object", "additionalProperties": False, "properties": properties, "required": required}, "outputSchema": {"type": "object", "required": outputs, "properties": {key: {} for key in outputs}}, "annotations": {"readOnlyHint": name not in ("logrisk_generate_day_report", "logrisk_cancel_report"), "destructiveHint": False, "idempotentHint": True, "openWorldHint": False}})
    return result


def tool_request(name: str, arguments: dict[str, Any]) -> tuple[str, str, dict[str, Any] | None]:
    args = tool_arguments(name, arguments)
    prefix = "/api/node-risks/" + quote(args.get("cluster", ""), safe="") + "/" + quote(args.get("node_id", ""), safe="")
    if name == "logrisk_find_nodes":
        return "GET", "/api/node-risks/search?" + urlencode(args), None
    if name == "logrisk_daily_series":
        return "GET", prefix + "/daily?" + urlencode({k: args[k] for k in ("from", "to", "timezone")}), None
    if name == "logrisk_day_evidence":
        return "GET", prefix + "/days/" + args["date"] + "?" + urlencode({k: v for k, v in args.items() if k in ("timezone", "cursor", "limit")}), None
    if name == "logrisk_current_assessment":
        return "GET", prefix + "/assessment", None
    if name == "logrisk_generate_day_report":
        return "POST", "/api/node-risk-reports", {"schema_version": "node_risk_report_request_v1", **args}
    if name == "logrisk_get_report":
        return "GET", "/api/node-risk-reports/" + quote(args["report_id"], safe=""), None
    return "POST", "/api/node-risk-reports/" + quote(args["report_id"], safe="") + "/cancel", {"idempotency_key": args["idempotency_key"]}


def tool_result(value: dict[str, Any], *, error: bool = False) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": json.dumps(value, ensure_ascii=False, sort_keys=True)}], "structuredContent": value, "isError": error}
