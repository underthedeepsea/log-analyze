#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${DASHBOARD_STATE_DIR:-$ROOT_DIR/state}"
PID_FILE="$STATE_DIR/dashboard.pid"
LOG_FILE="$STATE_DIR/dashboard.log"
PYTHON="$ROOT_DIR/.venv/bin/python"
[[ -x "$PYTHON" ]] || PYTHON="${PYTHON_BIN:-python3}"

mkdir -p "$STATE_DIR"
export PYTHONPATH="${PYTHONPATH:-}:$ROOT_DIR/src"

dashboard_command() {
  exec "$PYTHON" -m pipeline.dashboard_server \
    --host "${DASHBOARD_HOST:-127.0.0.1}" \
    --port "${DASHBOARD_PORT:-8080}" \
    --model "${OLLAMA_MODEL:-qwen3:1.7b}" \
    --ollama-url "${OLLAMA_HOST:-http://127.0.0.1:11434}" \
    --ollama-timeout "${OLLAMA_TIMEOUT:-120}"
}

read_pid() {
  [[ -f "$PID_FILE" ]] && tr -d '[:space:]' < "$PID_FILE"
}

is_running() {
  local pid
  pid="$(read_pid || true)"
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  local command
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"pipeline.dashboard_server"* ]]
}

start_dashboard() {
  if is_running; then
    echo "Dashboard 已运行，PID $(read_pid)"
    return 0
  fi
  rm -f "$PID_FILE"
  cd "$ROOT_DIR"
  nohup "$PYTHON" -m pipeline.dashboard_server \
    --host "${DASHBOARD_HOST:-127.0.0.1}" \
    --port "${DASHBOARD_PORT:-8080}" \
    --model "${OLLAMA_MODEL:-qwen3:1.7b}" \
    --ollama-url "${OLLAMA_HOST:-http://127.0.0.1:11434}" \
    --ollama-timeout "${OLLAMA_TIMEOUT:-120}" >> "$LOG_FILE" 2>&1 &
  local pid=$!
  echo "$pid" > "$PID_FILE"
  sleep 0.4
  if ! is_running; then
    echo "Dashboard 启动失败，请查看 $LOG_FILE" >&2
    rm -f "$PID_FILE"
    return 1
  fi
  echo "Dashboard 已启动：http://${DASHBOARD_HOST:-127.0.0.1}:${DASHBOARD_PORT:-8080}（PID ${pid}）"
}

stop_dashboard() {
  if ! is_running; then
    rm -f "$PID_FILE"
    echo "Dashboard 未运行"
    return 0
  fi
  local pid
  pid="$(read_pid)"
  kill -TERM "$pid"
  for _ in {1..50}; do
    if ! kill -0 "$pid" 2>/dev/null; then
      rm -f "$PID_FILE"
      echo "Dashboard 已停止"
      return 0
    fi
    sleep 0.2
  done
  echo "Dashboard 未能在 10 秒内停止（PID ${pid}）" >&2
  return 1
}

status_dashboard() {
  if is_running; then
    echo "Dashboard 运行中：http://${DASHBOARD_HOST:-127.0.0.1}:${DASHBOARD_PORT:-8080}（PID $(read_pid)）"
    return 0
  fi
  echo "Dashboard 未运行"
  return 1
}

case "${1:-start}" in
  start) start_dashboard ;;
  stop) stop_dashboard ;;
  restart) stop_dashboard; start_dashboard ;;
  status) status_dashboard ;;
  foreground) cd "$ROOT_DIR"; dashboard_command ;;
  *) echo "用法: $0 {start|stop|restart|status|foreground}" >&2; exit 2 ;;
esac
