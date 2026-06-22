# 日志风险分析系统代码包

本代码包用于在不接 Kafka / ES 的情况下，通过人工导入日志文件验证后端全流程。

## 当前链路

```text
人工导入 JSON/JSONL 日志
  ↓
Normalizer 日志规范化
  ↓
Drain3 模板化
  ↓
窗口聚合
  ↓
风险机器评分
  ↓
Mock RCA
  ↓
输出 result.json
```

## 快速运行

```bash
cd log-risk-analysis-code

python3 -m venv .venv
source .venv/bin/activate

pip install --upgrade pip setuptools wheel
pip install -r requirements.txt

export PYTHONPATH=$(pwd)/src

bash scripts/run_manual_pipeline.sh
```

执行完成后查看：

```bash
jq '.summary' output/result.json
jq '.risk_entities[] | {entity_id, risk_score, risk_level, summary}' output/result.json
jq '.rca_results[] | {risk_entity, root_cause_candidate, confidence}' output/result.json
```

## 手动执行完整命令

```bash
export PYTHONPATH=$(pwd)/src

python3 -m pipeline.manual_import_pipeline \
  --input examples/sample_k8s_logs.jsonl \
  --output-dir output \
  --config configs/drain3_recommended.ini \
  --rules configs/risk_rules.yaml \
  --state-dir output/drain3_state \
  --window-seconds 300 \
  --mock-llm
```

## 输出文件

```text
output/
  normalized_logs.json
  template_events.json
  template_windows.json
  risk_entities.json
  rca_results.json
  result.json
```

## 当前阶段说明

该代码包优先用于验证后端逻辑闭环：

- 不接 Kafka；
- 不接 ES；
- 不接真实 LLM；
- 不接数据库；
- 通过 JSON/JSONL 人工导入日志。

等批处理链路稳定后，再把文件输入替换为 Kafka Consumer。
