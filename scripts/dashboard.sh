#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="${DASHBOARD_STATE_DIR:-$ROOT_DIR/state}"
PID_FILE="$STATE_DIR/dashboard.pid"
LOG_FILE="$STATE_DIR/dashboard.log"
PYTHON="$ROOT_DIR/.venv/bin/python"
[[ -x "$PYTHON" ]] || PYTHON="${PYTHON_BIN:-python3}"
HOST="${DASHBOARD_HOST:-127.0.0.1}"
PORT="${DASHBOARD_PORT:-8080}"

mkdir -p "$STATE_DIR"
export PYTHONPATH="${PYTHONPATH:-}:$ROOT_DIR/src"

dashboard_command() {
  exec "$PYTHON" -m pipeline.dashboard_server \
    --host "$HOST" \
    --port "$PORT" \
    --model "${OLLAMA_MODEL:-qwen3:1.7b}" \
    --ollama-url "${OLLAMA_HOST:-http://127.0.0.1:11434}" \
    --ollama-timeout "${OLLAMA_TIMEOUT:-120}"
}

read_pid() {
  [[ -f "$PID_FILE" ]] && tr -d '[:space:]' < "$PID_FILE"
}

is_dashboard_process() {
  local pid="$1" command state
  [[ "$pid" =~ ^[0-9]+$ ]] || return 1
  kill -0 "$pid" 2>/dev/null || return 1
  state="$(ps -p "$pid" -o stat= 2>/dev/null || true)"
  [[ "$state" != *Z* ]] || return 1
  command="$(ps -p "$pid" -o command= 2>/dev/null || true)"
  [[ "$command" == *"pipeline.dashboard_server"* ]]
}

listener_pid() {
  command -v lsof >/dev/null 2>&1 || return 1
  lsof -nP -tiTCP:"$PORT" -sTCP:LISTEN 2>/dev/null | head -n 1
}

is_running() {
  local pid discovered
  pid="$(read_pid || true)"
  if is_dashboard_process "$pid"; then
    return 0
  fi
  rm -f "$PID_FILE"
  discovered="$(listener_pid || true)"
  if is_dashboard_process "$discovered"; then
    echo "$discovered" > "$PID_FILE"
    return 0
  fi
  return 1
}

start_dashboard() {
  if is_running; then
    echo "Dashboard 已运行，PID $(read_pid)"
    return 0
  fi
  rm -f "$PID_FILE"
  local occupied
  occupied="$(listener_pid || true)"
  if [[ -n "$occupied" ]]; then
    echo "Dashboard 启动失败：端口 ${HOST}:${PORT} 已被 PID ${occupied} 占用" >&2
    return 1
  fi
  cd "$ROOT_DIR"
  nohup "$PYTHON" -m pipeline.dashboard_server \
    --host "$HOST" \
    --port "$PORT" \
    --model "${OLLAMA_MODEL:-qwen3:1.7b}" \
    --ollama-url "${OLLAMA_HOST:-http://127.0.0.1:11434}" \
    --ollama-timeout "${OLLAMA_TIMEOUT:-120}" </dev/null >> "$LOG_FILE" 2>&1 &
  local pid=$!
  echo "$pid" > "$PID_FILE"
  for _ in {1..50}; do
    if is_running; then
      echo "Dashboard 已启动：http://${HOST}:${PORT}（PID $(read_pid)）"
      return 0
    fi
    sleep 0.1
  done
  echo "Dashboard 启动失败，请查看 $LOG_FILE" >&2
  rm -f "$PID_FILE"
  return 1
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
    echo "Dashboard 运行中：http://${HOST}:${PORT}（PID $(read_pid)）"
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
  foreground)
    if is_running; then
      echo "Dashboard 已运行，PID $(read_pid)"
      exit 0
    fi
    echo "$$" > "$PID_FILE"
    cd "$ROOT_DIR"
    dashboard_command
    ;;
  *) echo "用法: $0 {start|stop|restart|status|foreground}" >&2; exit 2 ;;
esac
