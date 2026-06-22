#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"

mkdir -p output/drain3_state

python3 -m pipeline.manual_import_pipeline \
  --input examples/sample_k8s_logs.jsonl \
  --output-dir output \
  --config configs/drain3_recommended.ini \
  --rules configs/risk_rules.yaml \
  --state-dir output/drain3_state \
  --window-seconds 300

echo
echo "Summary:"
jq '.summary' output/result.json
