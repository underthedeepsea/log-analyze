from pathlib import Path
import subprocess


def test_dashboard_script_supports_process_commands_and_state_files():
    script = Path("scripts/dashboard.sh").read_text(encoding="utf-8")

    for command in ("start", "stop", "restart", "status", "foreground"):
        assert command in script
    assert "dashboard.pid" in script
    assert "dashboard.log" in script
    assert "kill -0" in script
    assert 'exec "$PYTHON" -m pipeline.dashboard_server' in script
    assert "（PID ${pid}）" in script


def test_dashboard_scripts_have_valid_shell_syntax():
    for script in ("scripts/dashboard.sh", "scripts/run_dashboard.sh"):
        subprocess.run(["bash", "-n", script], check=True)


def test_legacy_launcher_delegates_to_foreground_mode():
    script = Path("scripts/run_dashboard.sh").read_text(encoding="utf-8")

    assert "dashboard.sh" in script
    assert "foreground" in script
