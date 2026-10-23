#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
exec "$ROOT/.venv/bin/python" -m logrisk.mcp_server --api-base "${LOGRISK_MCP_API_BASE:-http://127.0.0.1:8080}"
