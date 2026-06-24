# 日志风险特征分析与审批系统

当前版本：`1.2.1`。变更记录见 [`releas.md`](releas.md)。

本项目在本机完成日志规范化、Drain3 模板化、风险评分、规则复用、Ollama 特征识别与人工审批。项目不实现 RCA；原始日志不会直接发送给 Ollama。

```text
JSON / JSONL / TXT / LOG
        ↓
规范化 → Drain3 → 风险评分
        ↓
全局批准规则匹配 ──命中──→ 规则复用（跳过 LLM）
        │未命中
        ↓
Ollama 特征识别 → 人工审批 → 外部 RCA 专家系统
```

## 环境准备

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
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

## 规则复用与指标

人工批准的特征会原子写入 `state/approved_rules.json`。后续任何集群或节点命中相同模板 Hash、规则类别和特征类型时，会生成“规则复用”特征并跳过 Ollama。建议定期备份该文件。

Dashboard 实时显示 Drain3 压缩量、当日 LLM 关联日志量、处理速度、ETA、规则复用次数和节省的调用。`state/processing_metrics.json` 按本地日期累计 LLM 关联日志量；该数字是模板关联计数，不代表原始日志被发送给模型。

## 安全与导出

Ollama 只接收聚合、脱敏后的实体和模板证据，不接收 `samples`、`raw_sample` 或原始日志流。导出包只包含已批准特征和关联风险节点，不包含原始日志、根因、影响或处置建议。Dashboard 默认仅监听 `127.0.0.1`。

## 测试

```bash
pytest -q
bash -n scripts/*.sh
```

HTTP/SSE 测试使用模拟提取器，不要求运行 Ollama；真实验收可使用 `qwen3:1.7b`。
