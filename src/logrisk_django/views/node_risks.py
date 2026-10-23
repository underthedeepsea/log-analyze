from __future__ import annotations

import json
from urllib.parse import quote

from django.http import HttpRequest, HttpResponse, JsonResponse

from logrisk.runtime.identity import RuntimeAccessError
from logrisk.server_risk.contracts import ServerRiskError
from logrisk_django.service_factory import get_config, get_facade, get_identity_resolver
from logrisk_django.views.access import require_django_write_access


def server_risk_api(request: HttpRequest, **_kwargs) -> HttpResponse:
    """Host identity and shared facade only; Web processes never execute reports."""
    try:
        identity = get_identity_resolver().resolve(request)
        if not identity.authenticated or not identity.actor or identity.source not in {"django", "trusted_proxy"}:
            raise RuntimeAccessError("服务器风险入口需要宿主可信身份")
        facade = get_facade()
        if request.method == "GET":
            path = request.path
            if path.endswith("/overview") and "cluster" in _kwargs and "node" in _kwargs:
                # Django route values have already passed WSGI percent decoding.
                # Restore one encoded segment per identity for the shared facade;
                # literal names such as "%41" must never select the host "A".
                path = "/api/node-risks/" + quote(_kwargs["cluster"], safe="") + "/" + quote(_kwargs["node"], safe="") + "/overview"
            result = facade.dispatch_read(path, request.GET, identity)
        elif request.method == "POST":
            if request.path != "/api/node-risk-queries":
                verified = require_django_write_access(request)
                if isinstance(verified, JsonResponse):
                    return verified
                if not get_config().write_roles or not set(get_config().write_roles).intersection(identity.roles):
                    raise RuntimeAccessError("服务器风险写入需要配置且匹配的角色")
            if len(request.body) > 1048576:
                raise ServerRiskError("request_resource_limit", "请求体超过资源上限", 413)
            payload = json.loads(request.body.decode("utf-8") or "{}")
            result = facade.dispatch_server_risk_write(request.path, payload, identity)
        else:
            return JsonResponse({"code": "method_not_allowed", "error": "请求方法不受支持"}, status=405)
        if result is None:
            return JsonResponse({"code": "not_found", "error": "入口不存在"}, status=404)
        response = HttpResponse(result.body, status=result.status) if isinstance(result.body, bytes) else JsonResponse(result.body, status=result.status, json_dumps_params={"ensure_ascii": False})
        for key, value in result.headers.items():
            response[key] = value
        response["Cache-Control"] = "no-store"
        response["X-Request-ID"] = identity.request_id
        return response
    except RuntimeAccessError as exc:
        return JsonResponse({"code": exc.code, "error": str(exc)}, status=403)
    except ServerRiskError as exc:
        return JsonResponse({"code": exc.code, "error": str(exc)}, status=exc.status_code)
    except (ValueError, UnicodeError, TypeError):
        return JsonResponse({"code": "invalid_schema", "error": "请求结构不符合合同"}, status=422)
    except Exception:
        return JsonResponse({"code": "server_risk_unavailable", "error": "服务器风险服务暂时不可用"}, status=503)
