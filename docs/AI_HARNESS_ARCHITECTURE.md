# AI Harness Architecture

LOGRISK 在本机完成日志特征识别与人工审批，不实现 RCA。原始日志先经过规范化、Drain3 模板化、窗口聚合和风险评分；只有聚合且脱敏的 Evidence JSON 可以进入 AI Harness。

## Processing Flow

```text
risk_entity
  → approved rule reuse
  → AI cache lookup
  → evidence builder and context budget
  → prompt registry
  → Ollama model client
  → schema validation and output evaluator
  → human approval
  → approved rule with lineage
```

规则命中时直接生成复用特征并跳过 Ollama。Cache 命中时跳过模型调用，但仍重新执行 Schema 校验和 Evaluator，避免缓存结果绕过质量门禁。

## Prompt Version Management

Prompt 正文位于 `prompts/`，注册信息和默认 Prompt 位于 `configs/ai_harness.yaml`。Dashboard 的 Prompt 管理页面支持编辑当前版本并查看历史；保存后的版本历史写入忽略目录中的 `state/prompt_versions.json`。Trace 同时记录 Prompt ID 和内容 Hash，避免同名 Prompt 的不同版本被错误聚合。

## Trace and Evaluation

AI 调用 Trace 追加写入 `state/ai_traces.jsonl`，包括模型、Prompt、Evidence 元数据、响应状态、Schema 和 Evaluator 结果。Trace 不应记录或发送 `samples`、`raw_sample` 与原始日志流。

内置回归测试运行方式：

```bash
OLLAMA_MODEL=qwen3:1.7b .venv/bin/python -m logrisk.ai_eval.runner
```

结果写入 `output/eval_results.json`。Promptfoo 使用真实 `feature_extract_v1` 和本地 Ollama：

```bash
npm ci
OLLAMA_BASE_URL=http://127.0.0.1:11434 npm run promptfoo:eval
```

## Cache and Rule Lineage

AI Cache 默认启用，状态位于 `state/ai_cache.json`。Cache signature 由 Evidence Hash、Prompt Hash、provider、模型和 Thinking 开关组成；设置 `AI_CACHE_ENABLED=0` 可临时禁用。

人工批准的规则写入 `state/approved_rules.json`。Lineage 保存来源 Job、候选特征、Trace、Prompt、provider、模型和 Evidence Hash，使规则可追溯到具体 AI 分析；旧版无 lineage 的规则继续兼容。

## Deferred External Observability

当前不接入 Phoenix、MLflow 或 LangSmith。项目仍是本地、无数据库的人工审批原型，现有 JSONL Trace、Evaluator、Eval Runner 和 Promptfoo 已覆盖当前调试与回归需求。引入外部平台会增加服务部署、数据治理和原始证据外发风险；只有在需要跨团队共享、长期指标查询或分布式 Trace 时再评估接入。
