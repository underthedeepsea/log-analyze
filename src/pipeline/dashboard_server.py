from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from logrisk.approved_rules import ApprovedRuleError, ApprovedRuleStore
from logrisk.feature_extractor_ollama import DEFAULT_OLLAMA_URL
from logrisk.feature_jobs import FeatureJobError, FeatureJobManager
from logrisk.input_parser import parse_log_content
from logrisk.processing_metrics import ProcessingMetricsError, ProcessingMetricsStore
from pipeline.manual_import_pipeline import analyze_records


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
    input_analyzer: Callable[[list[dict[str, Any]]], dict[str, Any]] | None = None,
) -> DashboardHTTPServer:
    root = Path(__file__).resolve().parents[2]
    server = DashboardHTTPServer((host, port), DashboardHandler)
    state_root = root / "state"
    server.manager = manager or FeatureJobManager(  # type: ignore[attr-defined]
        rule_store=ApprovedRuleStore(state_root / "approved_rules.json"),
        metrics_store=ProcessingMetricsStore(state_root / "processing_metrics.json"),
    )
    server.frontend_path = Path(frontend_path or root / "frontend" / "dist" / "index.html")  # type: ignore[attr-defined]
    server.default_model = default_model  # type: ignore[attr-defined]
    server.default_ollama_url = default_ollama_url  # type: ignore[attr-defined]
    server.default_timeout = default_timeout  # type: ignore[attr-defined]
    server.ollama_checker = ollama_checker or (  # type: ignore[attr-defined]
        lambda: check_ollama(default_ollama_url)
    )
    analysis_lock = threading.Lock()

    def default_input_analyzer(records: list[dict[str, Any]]) -> dict[str, Any]:
        with analysis_lock:
            return analyze_records(
                records,
                config_path=str(root / "configs" / "drain3_recommended.ini"),
                rules_path=str(root / "configs" / "risk_rules.yaml"),
                state_dir=str(state_root / "dashboard_drain3"),
            )

    server.input_analyzer = input_analyzer or default_input_analyzer  # type: ignore[attr-defined]
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
            if path.startswith("/assets/"):
                self._serve_asset(path)
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
            if path == "/api/rules":
                self._json(HTTPStatus.OK, {"rules": self.server.manager.list_rules()})  # type: ignore[attr-defined]
                return
            if path == "/api/metrics":
                self._json(HTTPStatus.OK, self.server.manager.get_system_metrics())  # type: ignore[attr-defined]
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
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except (TypeError, ValueError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})

    def do_POST(self) -> None:
        path, _ = self._route_parts()
        try:
            payload = self._read_json()
            if path == "/api/inputs/analyze":
                if not isinstance(payload, dict):
                    raise FeatureJobError("请求体必须是 JSON object")
                filename = payload.get("filename")
                content = payload.get("content")
                if not isinstance(filename, str) or not filename.strip():
                    raise FeatureJobError("上传文件名无效")
                if not isinstance(content, str):
                    raise FeatureJobError("上传内容必须是 UTF-8 文本")
                records = parse_log_content(filename.strip(), content)
                result = self.server.input_analyzer(records)  # type: ignore[attr-defined]
                self._json(HTTPStatus.OK, {"result": result})
                return
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
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError) as exc:
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
        except (FeatureJobError, ApprovedRuleError, ProcessingMetricsError) as exc:
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

    def _serve_asset(self, request_path: str) -> None:
        asset_root = (self.server.frontend_path.parent / "assets").resolve()  # type: ignore[attr-defined]
        relative = request_path.removeprefix("/assets/")
        target = (asset_root / relative).resolve()
        try:
            target.relative_to(asset_root)
        except ValueError:
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
            return
        if not target.is_file():
            self._json(HTTPStatus.NOT_FOUND, {"error": "资源不存在"})
            return
        body = target.read_bytes()
        content_type = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
        if content_type.startswith("text/") or content_type in {"application/javascript", "application/json"}:
            content_type += "; charset=utf-8"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
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
