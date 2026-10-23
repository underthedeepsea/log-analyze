from __future__ import annotations

import http.client
import multiprocessing
import time
from email.message import Message
from typing import Any
from urllib.parse import urlsplit

from logrisk.server_risk.contracts import ServerRiskError


def _exchange(sender: Any, request: dict[str, Any], limit: int, initializer: Any) -> None:
    """Fresh spawn owns only its HTTP handles, never a database or artifact."""
    connection = response = None
    try:
        if initializer is not None:
            initializer()
        parsed = urlsplit(request["url"])
        cls = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
        connection = cls(parsed.hostname, parsed.port, timeout=request["timeout"])
        connection.connect()
        connection.request(request["method"], (parsed.path or "/") + ("?" + parsed.query if parsed.query else ""), request["body"], request["headers"])
        response = connection.getresponse()
        body = bytearray()
        while True:
            chunk = response.read1(min(65536, limit + 1 - len(body)))
            if not chunk:
                break
            body.extend(chunk)
            if len(body) > limit:
                sender.send(("error", "http_response_limit"))
                return
        sender.send(("response", response.status, list(response.headers.items()), bytes(body)))
    except BaseException:
        # Exception strings can contain credentials or user data.
        try:
            sender.send(("error", "http_transport_failed"))
        except (OSError, EOFError):
            pass
    finally:
        if response is not None:
            response.close()
        if connection is not None:
            connection.close()
        sender.close()


class CancellableHTTPResponse:
    """One owned, killable helper covers DNS/connect/header/body under one budget."""

    def __init__(self, request: Any, *, timeout: float, limit: int, cancelled: Any = None, deadline: float | None = None, initializer: Any = None, lifecycle_hook: Any = None) -> None:
        parsed = urlsplit(request.full_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ServerRiskError("http_transport_invalid", "HTTP目标无效")
        self.deadline = min(deadline if deadline is not None else float("inf"), time.monotonic() + max(0.001, timeout))
        self.cancelled = cancelled or (lambda: False)
        self.process = None
        self.receiver = None
        self.body = b""
        self.closed = False
        self.lifecycle_hook = lifecycle_hook
        context = multiprocessing.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        self.receiver = receiver
        self.process = context.Process(target=_exchange, args=(sender, {"url": request.full_url, "method": request.get_method(), "body": request.data, "headers": dict(request.header_items()), "timeout": max(0.001, timeout)}, limit, initializer), name="node-risk-http-transport")
        try:
            self._check()
            self.process.start()
            sender.close()
            if lifecycle_hook:
                lifecycle_hook("transport_started", self.process.pid)
            while True:
                self._check()
                if receiver.poll(min(0.025, max(0.0, self.deadline - time.monotonic()))):
                    try:
                        result = receiver.recv()
                    except EOFError:
                        raise ServerRiskError("http_transport_failed", "HTTP调用失败") from None
                    self._check()
                    if result[0] != "response":
                        raise ServerRiskError(result[1], "HTTP调用失败或响应超过限制")
                    self.code = result[1]
                    self.headers = Message()
                    for key, value in result[2]:
                        self.headers[key] = value
                    self.body = result[3]
                    if len(self.body) > limit:
                        raise ServerRiskError("http_response_limit", "HTTP响应超过资源上限")
                    break
                if not self.process.is_alive():
                    raise ServerRiskError("http_transport_failed", "HTTP调用失败")
        except BaseException:
            self.close()
            raise
        finally:
            sender.close()
        self.close()

    def _check(self) -> None:
        if self.cancelled():
            raise ServerRiskError("http_cancelled", "HTTP调用已取消")
        if time.monotonic() >= self.deadline:
            raise ServerRiskError("http_deadline_exceeded", "HTTP调用超过期限")

    def read(self, _size: int = -1) -> bytes:
        self._check()
        return self.body if _size < 0 else self.body[:_size]

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        process = self.process
        if process is not None and process.pid is not None:
            if process.is_alive():
                process.terminate()
            process.join(timeout=0.1)
            if process.is_alive():
                process.kill()
                process.join(timeout=0.1)
            if process.is_alive():
                raise ServerRiskError("http_cleanup_failed", "HTTP执行资源未能回收")
            pid = process.pid
            process.close()
            if self.lifecycle_hook:
                self.lifecycle_hook("transport_reaped", pid)
        if self.receiver is not None:
            self.receiver.close()

    def __enter__(self) -> Any:
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()
