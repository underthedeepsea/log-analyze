from __future__ import annotations

import json
from pathlib import Path
import subprocess
import shutil
import re

from logrisk import __version__


def test_runtime_static_and_versions_are_consistent():
    source = Path("frontend/src/app.js").read_bytes()
    assert source == Path("frontend/dist/assets/app.js").read_bytes() == Path("src/logrisk_django/static/logrisk/assets/app.js").read_bytes()
    assert Path("frontend/src/runtime-extra.css").read_bytes() == Path("frontend/dist/assets/runtime-extra.css").read_bytes() == Path("src/logrisk_django/static/logrisk/assets/runtime-extra.css").read_bytes()
    version = re.search(r'^version = "([^"]+)"', Path("pyproject.toml").read_text(), re.M).group(1)
    assert version == __version__ == json.loads(Path("frontend/package.json").read_text())["version"] == json.loads(Path("frontend/package-lock.json").read_text())["version"]
    assert f"当前版本：`{__version__}`" in Path("README.md").read_text() and f"## {__version__} -" in Path("releas.md").read_text()
    node = shutil.which("node")
    if node: subprocess.run([node, "--check", "frontend/src/app.js"], check=True, capture_output=True)


def test_ui_contract_uses_complete_pages_unknown_gaps_fixed_report_scope():
    source = Path("frontend/src/app.js").read_text()
    chunk = source[source.index("  function NodeRiskPage("):source.index("  function SemanticLibraryPage(")]
    for marker in ('current.items.concat(value.items)', 'selected.latest_risk_at', 'server-history-note', 'point.assessed_score_peak != null', 'occurrence_count == null ? "未知"', 'sandbox: ""', 'setQueryResult(null)', 'const q = { cluster: selected.cluster, node_id: selected.node_id, date: day, timezone: zone }', 'currentRequest(request)', 'value.evidence_revision !== page.evidence_revision', 'evidence_expired', '跨日仍未结束的风险'):
        assert marker in chunk
    assert "dangerouslySetInnerHTML" not in chunk
    assert 'date: queryResult' not in chunk
    assert 'latest_risk_at: assessment.latest_risk_at' in source
    prompt = Path("configs/ai_harness.yaml").read_text()
    assert "node_risk_report" in prompt
    # Browser checks are executed separately against the same committed bundle.
