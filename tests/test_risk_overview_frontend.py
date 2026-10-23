from __future__ import annotations

import json
from pathlib import Path
import shutil
import subprocess

import pytest


def test_overview_runtime_copies_match_and_javascript_is_valid():
    for asset in ("app.js", "runtime-extra.css"):
        source = Path("frontend/src", asset).read_bytes()
        assert source == Path("frontend/dist/assets", asset).read_bytes()
        assert source == Path("src/logrisk_django/static/logrisk/assets", asset).read_bytes()
    node = shutil.which("node")
    if node:
        subprocess.run([node, "--check", "frontend/src/app.js"], check=True, capture_output=True)
        subprocess.run([node, "--check", "scripts/verify_revision005_browser.cjs"], check=True, capture_output=True)


@pytest.fixture(scope="module")
def browser_report(tmp_path_factory):
    node = shutil.which("node")
    if not node:
        pytest.skip("Node is unavailable; headless behavior remains unverified")
    output = tmp_path_factory.mktemp("revision005_frontend")
    result = subprocess.run(
        [node, "scripts/verify_revision005_browser.cjs", f"--output={output}"],
        text=True, capture_output=True, timeout=90,
    )
    # Keep the actual command output and report on failure; no raw source logs used.
    assert result.returncode == 0, f"headless command failed:\n{result.stdout}\n{result.stderr}"
    report = json.loads((output / "report.json").read_text())
    assert report["data_source"] == "synthetic_browser_fixture"
    assert report["status"] == "passed"
    return report


def test_drawer_preserves_scope_focus_page_scroll_and_legacy_paths(browser_report):
    checks = browser_report["checks"]
    assert checks["drawer_preserves_filters_page_scroll_focus_and_traps_keyboard"]
    assert checks["drawer_rejects_stale_host_detail_and_keeps_legacy_paths"]
    assert checks["encoded_host_deep_link"]
    assert checks["legacy_date_uses_same_host_scope_without_catalog"]
    assert checks["drawer_handles_failure_empty_and_rejects_wrong_host"]


def test_overview_filters_pagination_truthful_unknown_and_request_races(browser_report):
    checks = browser_report["checks"]
    assert checks["filters_and_full_result_statistics"]
    assert checks["scoped_server_titles_precede_technical_risk_ids"]
    assert checks["overview_rejects_stale_scope"]
    assert checks["real_empty_and_error_never_fall_back_to_demo"]
    assert checks["unknown_recovery_does_not_become_confirmed_unrecovered"]


def test_beijing_date_shortcuts_and_invalid_ranges(browser_report):
    assert browser_report["checks"]["beijing_dates_independent_of_browser_timezone"]
    assert browser_report["checks"]["invalid_date_range_is_explicit"]


def test_url_historical_scope_initialization_and_safe_normalization(browser_report):
    assert browser_report["checks"]["url_initializes_historical_scope_and_survives_drawer"]
    assert browser_report["checks"]["url_invalid_enums_and_pagination_normalize_safely"]
