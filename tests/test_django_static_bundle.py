from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_django_static_bundle_matches_committed_frontend_dist() -> None:
    source_root = ROOT / "frontend" / "dist"
    package_root = ROOT / "src" / "logrisk_django" / "static" / "logrisk"
    assert source_root.is_dir()
    for source in sorted(source_root.rglob("*")):
        if source.is_file():
            packaged = package_root / source.relative_to(source_root)
            assert packaged.is_file(), f"缺少 Django 静态资源: {packaged.relative_to(ROOT)}"
            assert packaged.read_bytes() == source.read_bytes()


def test_django_spa_fallback_keeps_api_routes_outside_static_namespace() -> None:
    from logrisk_django.urls import urlpatterns

    routes = {str(item.pattern) for item in urlpatterns}
    assert "api/jobs" in routes
    assert "<path:path>" in routes
