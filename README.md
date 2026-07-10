# 日志风险特征分析与审批系统

当前版本：`1.14.0`。变更记录见 [`releas.md`](releas.md)。

本项目在本机完成日志规范化、Drain3 模板化、风险评分、规则复用、Ollama 特征识别与人工审批。项目不实现 RCA；原始日志不会直接发送给 Ollama。

```text
JSON / JSONL / TXT / LOG
        ↓
规范化 → Drain3 → 风险评分
        ↓
全局批准规则匹配 ──命中──→ 规则复用（跳过 LLM）
        │未命中
        ↓
AI Cache ──未命中──→ Ollama 特征识别 → 人工审批 → 外部 RCA 专家系统
```

## 环境准备

依赖环境固定放在项目根目录的 `.venv/`，启动脚本会优先使用 `.venv/bin/python`。`.venv/` 已被 Git 忽略，不会提交到仓库；如果目录丢失，按下面命令重建即可。

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

确认依赖可用：

```bash
.venv/bin/python -c "import drain3; print('venv ok')"
```

前端是已提交到 `frontend/dist/` 的纯 React 静态应用，不依赖前端构建工具或 CDN，普通启动不需要 Node.js，也不需要编译。

## 输入格式

命令行和 Dashboard 支持 `result.json`、JSON、JSONL、UTF-8 `.txt` 和 `.log`。纯文本中的每个非空行作为一条日志进入完整分析流程。

```bash
python3 -m pipeline.manual_import_pipeline \
  --input examples/sample_plain_logs.log \
  --output-dir output \
  --config configs/drain3_recommended.ini \
  --rules configs/risk_rules.yaml \
  --state-dir output/drain3_state
```

## 启动与重启

```bash
ollama serve
ollama pull qwen3:1.7b

bash scripts/dashboard.sh start
bash scripts/dashboard.sh status
bash scripts/dashboard.sh restart
bash scripts/dashboard.sh stop
```

访问 [http://127.0.0.1:8080](http://127.0.0.1:8080)。日志写入 `state/dashboard.log`，PID 写入 `state/dashboard.pid`。兼容的前台方式为 `bash scripts/run_dashboard.sh`。可通过 `OLLAMA_MODEL`、`OLLAMA_HOST`、`OLLAMA_TIMEOUT`、`DASHBOARD_HOST` 和 `DASHBOARD_PORT` 覆盖默认配置。

Dashboard 上传 10MB 以内文件时继续使用 inline 分析；超过 10MB 时自动使用 1MB 分片上传到 `state/uploads/`，后端创建异步 input job 并把结果写入 `output/uploads/{input_job_id}/result.json`。默认单文件上限为 500MB，支持 `.log`、`.txt`、`.jsonl`、`.ndjson`、`.gz` 和 Linux `messages` 这类无后缀文本日志。大文件 Drain3 会按“集群 + 节点 + 来源 + 组件”安全分区，并使用最多 CPU 核数的独立进程并行处理；同一分区保持原始顺序。结果摘要包含 `drain3_parallel`、`drain3_worker_count` 与 `drain3_partition_count`；只有一个分区时会自动串行处理，以保持在线模板学习语义。

AI Cache 默认启用，缓存文件为 `state/ai_cache.json`。同一 evidence hash、Prompt hash、provider、模型和 Thinking 开关再次分析时会跳过 Ollama；调试模型或 Prompt 时可用 `AI_CACHE_ENABLED=0 bash scripts/dashboard.sh restart` 临时关闭。

模型画像配置在 `configs/model_profiles.yaml`。Dashboard 的“模型画像”页面展示参数量、上下文窗口、默认 Prompt、Thinking ON/OFF 和 Evidence 预算，并支持复制新增 Profile 后保存回本地 YAML。当前内置 `qwen3.5:4b-mlx`、`qwen3:1.7b`、`qwen3.6:35b-a3b` 和 `deepseek-v4:flash` 四类 Profile；默认启用 `qwen3_1_7b_fast`，默认 Prompt 为 `feature_extract_v3_compact_strict_json_en`。Ollama options 会传入 `think: false`、`temperature: 0` 和输出长度限制，其中 `qwen3.5:4b-mlx` 的 `num_predict` 为 900。Evidence 会按当前 Profile 裁剪模板数、模板字符数、影响实体数和总字符数，裁剪结果记录到 AI Trace，不写入发给模型的 Evidence。新建分析时可选择自动重试 0–3 次，用于处理模型偶发无效 JSON、缺少 `tags` / `selection_reason` 或质量门禁拦截。

## 规则复用与指标

人工批准的特征会原子写入 `state/approved_rules.json`。后续任何集群或节点命中相同模板 Hash、规则类别和特征类型时，会生成“规则复用”特征并跳过 Ollama。建议定期备份该文件。

Dashboard 实时显示 Drain3 压缩量、当日 LLM 关联日志量、处理速度、ETA、规则复用、AI Cache 命中和节省的调用。`state/processing_metrics.json` 按本地日期累计真实进入模型的关联日志量；该数字是模板关联计数，不代表原始日志被发送给模型。

## AI Harness

Dashboard 提供 `/ai-observability`、`/prompts`、`/model-profiles` 和 `/ai-traces` 四个轻量 Harness 页面。AI 分析观测展示任务阶段、模型 Profile、Thinking 状态、Evidence 预算/裁剪、规则生成漏斗、AI Cache 命中、Evaluator 质量门禁、事件流和实体级失败原因；Prompt 管理保留当前版本编辑和历史版本查看；AI Trace 可按 Job、Trace、状态和 Prompt 过滤，并展示每次调用的模型画像和上下文预算。

批准规则会记录 Rule Lineage：来源 `job_id`、`candidate_id`、`trace_id`、Prompt、模型、provider 和 evidence hash 会随规则持久化，便于后续审计规则从哪次 AI 分析沉淀而来。旧版无 lineage 的规则仍可继续匹配和复用。

人工审批页的“特征日志证据”支持逐条选择脱敏模板；切换模板时，标题、摘要、标签和审批备注会同步生成当前证据的审批草稿，仍可由人工编辑后批准。

## AI Eval Runner

M7 新增本地回归评估能力。默认用例位于 `eval_cases/`，覆盖 OOM 驱逐、containerd runtime 失败、磁盘压力、Pod 业务错误和普通 warning 误报控制。运行：

```bash
OLLAMA_MODEL=qwen3:1.7b .venv/bin/python -m logrisk.ai_eval.runner
```

结果写入 `output/eval_results.json`，包含 `pass_rate`、`json_valid_rate`、`schema_valid_rate`、`template_reference_accuracy` 和 `forbidden_claim_count`。自动化测试使用 fake extractor，不要求 CI 启动 Ollama。

## 安全与导出

Ollama 只接收聚合、脱敏后的实体和模板证据，不接收 `samples`、`raw_sample` 或原始日志流。导出包只包含已批准特征和关联风险节点，不包含原始日志、根因、影响或处置建议。Dashboard 默认仅监听 `127.0.0.1`。

## 测试

```bash
pytest -q
bash -n scripts/*.sh
```

HTTP/SSE 测试使用模拟提取器，不要求运行 Ollama；真实验收可使用 `qwen3:1.7b`。
