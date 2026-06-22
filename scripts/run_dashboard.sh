#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"

python3 -m pipeline.dashboard_server \
  --host "${DASHBOARD_HOST:-127.0.0.1}" \
  --port "${DASHBOARD_PORT:-8080}" \
  --model "${OLLAMA_MODEL:-qwen3:1.7b}" \
  --ollama-url "${OLLAMA_HOST:-http://127.0.0.1:11434}" \
  --ollama-timeout "${OLLAMA_TIMEOUT:-120}"
