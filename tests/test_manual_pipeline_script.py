from pathlib import Path


def test_manual_pipeline_prefers_project_virtualenv():
    script = Path("scripts/run_manual_pipeline.sh").read_text(encoding="utf-8")

    assert 'PYTHON="$ROOT/.venv/bin/python"' in script
    assert 'if [ ! -x "$PYTHON" ]' in script
    assert 'PYTHON="python3"' in script
    assert '"$PYTHON" -m pipeline.manual_import_pipeline' in script
