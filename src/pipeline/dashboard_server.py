from __future__ import annotations

import argparse
import json
import os
import re
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from logrisk.feature_extractor_ollama import DEFAULT_OLLAMA_URL
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager


MAX_UPLOAD_BYTES = 10 * 1024 * 1024
DEFAULT_MODEL = "qwen3:1.7b"


class DashboardHTTPServer(ThreadingHTTPServer):
    daemon_threads = True


def check_ollama(base_url: str, timeout: float = 2) -> dict[str, Any]:
    try:
        with urlopen(f"{base_url.rstrip('/')}/api/tags", timeout=timeout) as response:
            payload = json.load(response)
        models = [item.get("name") for item in payload.get("models", []) if item.get("name")]
        return {"online": True, "models": models}
    except (OSError, URLError, ValueError, json.JSONDecodeError) as exc:
        return {"online": False, "models": [], "error": str(exc)}


def build_server(
    host: str,
    port: int,
    manager: FeatureJobManager | None = None,
    frontend_path: str | Path | None = None,
    ollama_checker: Callable[[], dict[str, Any]] | None = None,
    default_model: str = DEFAULT_MODEL,
    default_ollama_url: str = DEFAULT_OLLAMA_URL,
    default_timeout: float = 120,
) -> DashboardHTTPServer:
    root = Path(__file__).resolve().parents[2]
    server = DashboardHTTPServer((host, port), DashboardHandler)
    server.manager = manager or FeatureJobManager()  # type: ignore[attr-defined]
    server.frontend_path = Path(frontend_path or root / "frontend" / "index.html")  # type: ignore[attr-defined]
    server.default_model = default_model  # type: ignore[attr-defined]
    server.default_ollama_url = default_ollama_url  # type: ignore[attr-defined]
    server.default_timeout = default_timeout  # type: ignore[attr-defined]
    server.ollama_checker = ollama_checker or (  # type: ignore[attr-defined]
        lambda: check_ollama(default_ollama_url)
    )
    return server


class DashboardHandler(BaseHTTPRequestHandler):
    server: DashboardHTTPServer

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _json(self, status: int, payload: Any, headers: dict[str, str] | None = None) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> Any:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise FeatureJobError("Content-Length 无效") from exc
        if length > MAX_UPLOAD_BYTES:
            raise FeatureJobError("上传内容超过 10 MB")
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise FeatureJobError("请求体不是有效 JSON") from exc

    def _route_parts(self) -> tuple[str, dict[str, list[str]]]:
        parsed = urlparse(self.path)
        return parsed.path, parse_qs(parsed.query)

    def do_GET(self) -> None:
        path, query = self._route_parts()
        try:
            if path == "/":
                self._serve_frontend()
                return
            if path == "/api/config":
                self._json(HTTPStatus.OK, {
                    "default_model": self.server.default_model,  # type: ignore[attr-defined]
                    "default_ollama_url": self.server.default_ollama_url,  # type: ignore[attr-defined]
                    "default_timeout": self.server.default_timeout,  # type: ignore[attr-defined]
                    "max_upload_bytes": MAX_UPLOAD_BYTES,
                })
                return
            if path == "/api/ollama/status":
                self._json(HTTPStatus.OK, self.server.ollama_checker())  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)", path)
            if match:
                self._json(HTTPStatus.OK, self.server.manager.get_job(match.group(1)))  # type: ignore[attr-defined]
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/events", path)
            if match:
                cursor = int(query.get("cursor", ["0"])[0])
                self._serve_events(match.group(1), max(0, cursor))
                return
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
        except FeatureJobError as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except (TypeError, ValueError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_POST(self) -> None:
        path, _ = self._route_parts()
        try:
            payload = self._read_json()
            if path == "/api/jobs":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                job_id = self.server.manager.create_job(  # type: ignore[attr-defined]
                    payload.get("result"),
                    model=payload.get("model") or self.server.default_model,  # type: ignore[attr-defined]
                    min_score=float(payload.get("min_score", 40)),
                    base_url=payload.get("ollama_url") or self.server.default_ollama_url,  # type: ignore[attr-defined]
                    timeout=float(payload.get("timeout", self.server.default_timeout)),  # type: ignore[attr-defined]
                )
                self._json(HTTPStatus.ACCEPTED, {"job_id": job_id})
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/entities/([^/]+)/retry", path)
            if match:
                self.server.manager.retry_entity(match.group(1), match.group(2))  # type: ignore[attr-defined]
                self._json(HTTPStatus.ACCEPTED, {"status": "queued"})
                return
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/export", path)
            if match:
                package = self.server.manager.export_approved(match.group(1))  # type: ignore[attr-defined]
                self._json(
                    HTTPStatus.OK,
                    package,
                    {"Content-Disposition": 'attachment; filename="logrisk-feature-package.json"'},
                )
                return
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
        except FeatureJobError as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except (TypeError, ValueError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_PATCH(self) -> None:
        path, _ = self._route_parts()
        try:
            payload = self._read_json()
            match = re.fullmatch(r"/api/jobs/([a-f0-9]+)/features/([A-Za-z0-9_-]+)", path)
            if not match:
                self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
                return
            feature = self.server.manager.update_feature(match.group(1), match.group(2), payload)  # type: ignore[attr-defined]
            self._json(HTTPStatus.OK, feature)
        except FeatureJobError as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def _serve_frontend(self) -> None:
        path = self.server.frontend_path  # type: ignore[attr-defined]
        if not path.is_file():
            self._json(HTTPStatus.NOT_FOUND, {"error": "前端文件不存在"})
            return
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_events(self, job_id: str, cursor: int) -> None:
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "close")
        self.end_headers()
        while True:
            events, cursor = self.server.manager.wait_for_events(job_id, cursor, timeout=15)  # type: ignore[attr-defined]
            try:
                if not events:
                    self.wfile.write(b": heartbeat\n\n")
                for event in events:
                    data = json.dumps(event, ensure_ascii=False, separators=(",", ":"))
                    self.wfile.write(f"id: {event['sequence']}\nevent: {event['type']}\ndata: {data}\n\n".encode("utf-8"))
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return
            if self.server.manager.get_job(job_id)["status"] in {"completed", "completed_with_errors"}:  # type: ignore[attr-defined]
                return


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="启动日志特征人工审批 Dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--model", default=os.getenv("OLLAMA_MODEL", DEFAULT_MODEL))
    parser.add_argument("--ollama-url", default=os.getenv("OLLAMA_HOST", DEFAULT_OLLAMA_URL))
    parser.add_argument("--ollama-timeout", type=float, default=float(os.getenv("OLLAMA_TIMEOUT", "120")))
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    server = build_server(
        args.host,
        args.port,
        default_model=args.model,
        default_ollama_url=args.ollama_url,
        default_timeout=args.ollama_timeout,
    )
    print(f"Feature review dashboard: http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
