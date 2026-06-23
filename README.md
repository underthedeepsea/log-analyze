# 日志风险特征分析与审批系统

当前版本：`1.1.1`。变更记录见 [`releas.md`](releas.md)。

本项目通过本地 JSON/JSONL 日志验证以下链路：

```text
日志导入 → 规范化 → Drain3 模板化 → 窗口聚合 → 风险评分 → result.json
                                                              ↓
人工审批台 ← Ollama 关键特征识别 ← 上传 result.json → 导出批准特征包
                                                              ↓
                                                    外部 RCA 专家系统
```

项目不实现 RCA。Ollama 只识别关键日志特征，不生成根因、影响评估或处置建议。

## 环境准备

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 生成风险分析结果

```bash
bash scripts/run_manual_pipeline.sh
```

主要产物位于 `output/`：

- `normalized_logs.json`
- `template_events.json`
- `template_windows.json`
- `risk_entities.json`
- `result.json`

查看摘要：

```bash
jq '.summary' output/result.json
```

## 启动特征审批台

先启动 Ollama 并确认模型存在：

```bash
ollama serve
ollama pull qwen3:1.7b
```

再启动本地 Dashboard：

```bash
bash scripts/run_dashboard.sh
```

浏览器访问 [http://127.0.0.1:8080](http://127.0.0.1:8080)，上传 `output/result.json`。页面会立即显示风险概览，并按风险分串行调用 Ollama；实体状态和候选特征通过 SSE 实时更新，无需等待整批完成。

默认配置可通过环境变量覆盖：

```bash
OLLAMA_MODEL=qwen3:1.7b \
OLLAMA_HOST=http://127.0.0.1:11434 \
DASHBOARD_PORT=8080 \
bash scripts/run_dashboard.sh
```

## 人工审批与导出

每条候选特征都可以编辑标题、摘要、重要程度、标签和审批备注，然后标记为批准或驳回。只有批准项会进入 `logrisk-feature-package-YYYY-MM-DD.json`，供人工导入外部 RCA 专家系统。

发送给 Ollama 和导出的来源模板均不包含 `samples`、`raw_sample` 或原始日志。Dashboard 默认仅监听 `127.0.0.1`，上传数据和审批状态只保存在内存中。

## 测试

```bash
pytest -q
```

HTTP/SSE 测试会临时绑定本机随机端口，不需要真实 Ollama；真实验收可使用 `qwen3:1.7b` 和示例 `result.json`。
