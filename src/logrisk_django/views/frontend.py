from __future__ import annotations

from pathlib import Path

from django.http import FileResponse, Http404, HttpRequest
from django.views.decorators.http import require_GET


def packaged_index_path() -> Path:
    return Path(__file__).resolve().parents[1] / "static" / "logrisk" / "index.html"


@require_GET
def frontend(request: HttpRequest, path: str = "") -> FileResponse:
    """Serve the SPA entrypoint only after explicit API routes have been matched."""
    if path.startswith("api/"):
        raise Http404("资源不存在")
    index = packaged_index_path()
    if not index.is_file():
        raise Http404("LOGRISK 前端静态包不存在")
    return FileResponse(index.open("rb"), content_type="text/html; charset=utf-8")
