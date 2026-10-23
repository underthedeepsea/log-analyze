from __future__ import annotations

import argparse
import ipaddress
import json
import sys
import threading
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from logrisk import __version__
from logrisk.mcp_tools import list_tools, tool_request, tool_result
from logrisk.server_risk.contracts import DeadlineHTTPResponse, ServerRiskError

PROTOCOL_VERSION = "2025-11-25"
MAX_MESSAGE = 1048576
HTTP_TIMEOUT = 10.0


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise URLError("redirect forbidden")


def api_base(value: str) -> str:
    parsed = urlsplit(value)
    try:
        address = ipaddress.ip_address(parsed.hostname or "")
        if not address.is_loopback or parsed.scheme != "http" or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ("", "/") or parsed.port is None:
            raise ValueError()
    except ValueError as exc:
        raise ValueError("api-base必须为带端口的HTTP loopback literal地址") from exc
    return value.rstrip("/")


def _tool_params(params: dict[str, Any]) -> dict[str, Any]:
    """Remove validated protocol metadata without widening business arguments."""
    if "_meta" in params:
        meta = params["_meta"]
        if not isinstance(meta, dict):
            raise ServerRiskError("invalid_schema", "协议元数据必须为对象")
        if "progressToken" in meta:
            token = meta["progressToken"]
            if isinstance(token, bool) or not isinstance(token, (str, int)):
                raise ServerRiskError("invalid_schema", "进度标识必须为字符串或整数")
    return {key: value for key, value in params.items() if key != "_meta"}


class MCPServer:
    def __init__(self, base: str) -> None:
        self.base = api_base(base)
        self.opener = build_opener(NoRedirect)
        self.initialized = False
        self.ready = False
        self.output_lock = threading.Lock()
        self.active: dict[Any, threading.Event] = {}
        self.active_lock = threading.Lock()

    def emit(self, value: dict[str, Any]) -> None:
        with self.output_lock:
            sys.stdout.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")
            sys.stdout.flush()

    def error(self, request_id: Any, code: int, message: str) -> None:
        self.emit({"jsonrpc": "2.0", "id": request_id, "error": {"code": code, "message": message}})

    def http(self, method: str, path: str, payload: Any, cancelled: threading.Event | None = None) -> tuple[dict[str, Any], bool]:
        body = json.dumps(payload).encode() if payload is not None else None
        request = Request(self.base + path, data=body, method=method, headers={"Content-Type": "application/json", "Accept": "application/json"})
        try:
            with DeadlineHTTPResponse(request, timeout=HTTP_TIMEOUT, limit=MAX_MESSAGE, cancelled=cancelled.is_set if cancelled else None) as response:
                raw = response.read()
                value = json.loads(raw)
                if not isinstance(value, dict):
                    return {"code": "application_invalid_response", "error": "HTTP响应不是对象"}, True
                if response.code >= 300:
                    return {"code": str(value.get("code") or value.get("error_code") or "application_error")[:100], "error": "应用拒绝请求或执行失败"}, True
                return value, False
        except ServerRiskError as exc:
            return {"code": "application_response_limit" if exc.code == "http_response_limit" else "application_unavailable", "error": "HTTP调用已取消或超过资源限制"}, True
        except (ValueError, UnicodeError):
            return {"code": "application_invalid_response", "error": "HTTP响应不是合同JSON"}, True
        except (URLError, TimeoutError, OSError):
            return {"code": "application_unavailable", "error": "LOGRISK HTTP服务不可用"}, True

    def call(self, request_id: Any, params: dict[str, Any], cancelled: threading.Event) -> None:
        response = None
        try:
            params = _tool_params(params)
            if set(params) != {"name", "arguments"} or not isinstance(params["name"], str):
                raise ServerRiskError("invalid_schema", "工具参数结构无效")
            method, path, body = tool_request(params["name"], params["arguments"])
            if cancelled.is_set():
                return
            value, is_error = self.http(method, path, body, cancelled)
            response = {"jsonrpc": "2.0", "id": request_id, "result": tool_result(value, error=is_error)}
        except ServerRiskError as exc:
            response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32602, "message": "工具名称或参数不符合固定Schema", "data": {"code": exc.code}}}
        except Exception:
            response = {"jsonrpc": "2.0", "id": request_id, "error": {"code": -32603, "message": "MCP请求失败"}}
        finally:
            # Release the admission slot before publishing the response. A client
            # may send its next request as soon as it receives this line.
            with self.active_lock:
                self.active.pop(request_id, None)
            if response is not None and not cancelled.is_set():
                self.emit(response)

    def message(self, message: Any) -> None:
        if not isinstance(message, dict) or message.get("jsonrpc") != "2.0" or not isinstance(message.get("method"), str) or set(message) - {"jsonrpc", "id", "method", "params"}:
            self.error(None, -32600, "无效JSON-RPC请求")
            return
        request_id, method = message.get("id"), message["method"]
        if request_id is not None and (isinstance(request_id, bool) or not isinstance(request_id, (str, int))):
            self.error(None, -32600, "无效请求ID")
            return
        params = message.get("params", {})
        if not isinstance(params, dict):
            self.error(request_id, -32602, "参数必须为对象")
            return
        if method == "notifications/cancelled":
            target = params.get("requestId")
            if "id" in message or set(params) - {"requestId", "reason"} or isinstance(target, bool) or not isinstance(target, (str, int)) or ("reason" in params and not isinstance(params["reason"], str)):
                return
            with self.active_lock:
                event = self.active.get(target)
                if event:
                    event.set()
            return
        if method == "initialize":
            if self.initialized or not isinstance(params.get("protocolVersion"), str) or not isinstance(params.get("capabilities"), dict) or not isinstance(params.get("clientInfo"), dict):
                self.error(request_id, -32602, "初始化参数或顺序无效")
                return
            self.initialized = True
            self.emit({"jsonrpc": "2.0", "id": request_id, "result": {"protocolVersion": PROTOCOL_VERSION, "capabilities": {"tools": {}}, "serverInfo": {"name": "logrisk-server-risk", "version": __version__}}})
            return
        if method == "notifications/initialized":
            if self.initialized:
                self.ready = True
            return
        if request_id is None:
            return
        if method == "ping":
            self.emit({"jsonrpc": "2.0", "id": request_id, "result": {}})
        elif not self.ready:
            self.error(request_id, -32002, "尚未完成初始化")
        elif method == "tools/list":
            try:
                listing = _tool_params(params)
                # The fixed catalog fits one page and never issues a nextCursor.
                # Any supplied cursor is therefore unissued, including empty/null.
                if set(listing) - {"cursor"} or "cursor" in listing:
                    raise ServerRiskError("invalid_schema", "固定工具目录没有可继续的游标")
            except ServerRiskError:
                self.error(request_id, -32602, "工具列表参数或游标无效")
            else:
                self.emit({"jsonrpc": "2.0", "id": request_id, "result": {"tools": list_tools()}})
        elif method == "tools/call":
            with self.active_lock:
                if len(self.active) >= 1 or request_id in self.active:
                    self.error(request_id, -32000, "MCP请求并发上限为1")
                    return
                event = threading.Event()
                self.active[request_id] = event
            threading.Thread(target=self.call, args=(request_id, params, event), daemon=True).start()
        else:
            self.error(request_id, -32601, "方法不受支持")

    def serve(self) -> None:
        while True:
            raw = sys.stdin.buffer.readline(MAX_MESSAGE + 2)
            if not raw:
                break
            if len(raw) > MAX_MESSAGE:
                self.error(None, -32600, "消息超过1MiB")
                break
            try:
                self.message(json.loads(raw))
            except (ValueError, UnicodeError):
                self.error(None, -32700, "JSON解析失败")
            except Exception:
                self.error(None, -32600, "无效协议消息")
        with self.active_lock:
            for event in self.active.values():
                event.set()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-base", default="http://127.0.0.1:8080")
    args = parser.parse_args()
    try:
        MCPServer(args.api_base).serve()
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2)


if __name__ == "__main__":
    main()
