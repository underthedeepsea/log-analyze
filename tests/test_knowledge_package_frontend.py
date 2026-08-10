from pathlib import Path


def _frontend_source() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in (Path("frontend/src/app.js"), Path("frontend/src/styles.css"))
    )


def test_knowledge_package_workspace_contract() -> None:
    source = _frontend_source()
    for value in (
        "知识包中心",
        "/knowledge-packages",
        "/api/knowledge-packages",
        "/api/knowledge-packages/uploads",
        "知识包上传、预览与安装",
        "安装前确认",
        "导入候选区",
        "默认禁用",
        "knowledge-packages-page",
    ):
        assert value in source


def test_knowledge_package_bundle_is_synced() -> None:
    for name in ("app.js", "styles.css"):
        source = (Path("frontend/src") / name).read_bytes()
        bundled = (Path("frontend/dist/assets") / ("app.js" if name == "app.js" else "app.css")).read_bytes()
        assert source == bundled
