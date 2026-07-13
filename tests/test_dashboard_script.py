from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "dashboard.sh"


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def test_dashboard_script_start_status_restart_stop(tmp_path):
    env = os.environ | {
        "DASHBOARD_PORT": str(_free_port()),
        "DASHBOARD_STATE_DIR": str(tmp_path),
        "PYTHONPATH": str(ROOT / "src"),
    }

    try:
        started = subprocess.run(["bash", str(SCRIPT), "start"], cwd=ROOT, env=env, text=True, capture_output=True, timeout=15)
        assert started.returncode == 0, started.stderr
        assert "Dashboard 已启动" in started.stdout

        status = subprocess.run(["bash", str(SCRIPT), "status"], cwd=ROOT, env=env, text=True, capture_output=True, timeout=5)
        assert status.returncode == 0
        assert "Dashboard 运行中" in status.stdout

        restarted = subprocess.run(["bash", str(SCRIPT), "restart"], cwd=ROOT, env=env, text=True, capture_output=True, timeout=20)
        assert restarted.returncode == 0, restarted.stderr
        assert "Dashboard 已停止" in restarted.stdout
        assert "Dashboard 已启动" in restarted.stdout
    finally:
        subprocess.run(["bash", str(SCRIPT), "stop"], cwd=ROOT, env=env, text=True, capture_output=True, timeout=15)

    stopped = subprocess.run(["bash", str(SCRIPT), "status"], cwd=ROOT, env=env, text=True, capture_output=True, timeout=5)
    assert stopped.returncode != 0
    assert not (tmp_path / "dashboard.pid").exists()


def test_dashboard_script_recovers_stale_pid_file(tmp_path):
    (tmp_path / "dashboard.pid").write_text("999999\n", encoding="utf-8")
    env = os.environ | {
        "DASHBOARD_PORT": str(_free_port()),
        "DASHBOARD_STATE_DIR": str(tmp_path),
    }

    status = subprocess.run(["bash", str(SCRIPT), "status"], cwd=ROOT, env=env, text=True, capture_output=True, timeout=5)

    assert status.returncode != 0
    assert not (tmp_path / "dashboard.pid").exists()
