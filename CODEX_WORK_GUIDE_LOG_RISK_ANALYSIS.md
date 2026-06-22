# Codex 工作指引：日志风险特征分析与人工审批

## 1. 项目目标与边界

项目面向 K8s / IaaS 运维日志，通过本地文件验证日志规范化、模板提取、窗口聚合、风险评分和关键特征审批流程。

本项目不实现 RCA。不得生成根因结论、影响评估、处置建议或模拟 RCA。Ollama 只负责从聚合、评分后的证据中识别值得提交给外部 RCA 日志专家的候选特征。

当前阶段不接 Kafka、Elasticsearch、数据库或外部 LLM 服务。外部 RCA 系统仅通过人工下载并导入已批准 JSON 特征包衔接。

## 2. 当前数据流

```text
JSON / JSONL 日志
  ↓
Normalizer
  ↓
Drain3 模板化
  ↓
窗口聚合
  ↓
风险实体评分
  ↓
result.json
  ↓ 人工上传
本地 Ollama 关键特征识别（串行）
  ↓ 实时 SSE
逐特征人工编辑 / 批准 / 驳回
  ↓
通用 JSON 特征包
  ↓ 人工导入
外部 RCA 专家系统
```

## 3. 目录与模块职责

```text
configs/                         Drain3 和风险规则
examples/                        示例输入
frontend/index.html              特征审批单页应用
scripts/run_manual_pipeline.sh   生成 result.json
scripts/run_dashboard.sh         启动本地审批服务
src/logrisk/
  io_utils.py                    JSON / JSONL I/O
  normalizer.py                  日志规范化
  drain_miner.py                 稳定模板提取
  aggregator.py                  时间窗口聚合
  risk_engine.py                 风险评分与 feature_hint
  feature_extractor_ollama.py    Ollama 特征识别
  feature_jobs.py                内存任务、审批与导出
src/pipeline/
  manual_import_pipeline.py      批处理风险分析
  dashboard_server.py            HTTP / SSE 服务
tests/                           pytest 测试
```

## 4. 批处理要求

`manual_import_pipeline.py` 必须保持文件输入和模块化阶段，不得接入 Ollama。运行：

```bash
export PYTHONPATH=$(pwd)/src
python3 -m pipeline.manual_import_pipeline \
  --input examples/sample_k8s_logs.jsonl \
  --output-dir output \
  --config configs/drain3_recommended.ini \
  --rules configs/risk_rules.yaml \
  --state-dir output/drain3_state \
  --window-seconds 300
```

必须生成：

```text
normalized_logs.json
template_events.json
template_windows.json
risk_entities.json
result.json
```

不得生成 `rca_results.json`。

## 5. 特征识别要求

Ollama 默认地址为 `http://127.0.0.1:11434`，默认模型为 `qwen3:1.7b`。每个风险实体单独请求并按风险分串行处理，默认阈值为 `40`。

允许发送：实体、窗口、风险分/等级、组件、严重度、模板文本/Hash、规则类别、计数、首末时间、受影响实体和 `feature_hint`。

禁止发送：`samples`、`raw_sample`、原始日志流或其他未脱敏文本。

模型只能返回候选特征的类别、标题、摘要、重要程度、模板 Hash、组件、标签和入选理由。实体、风险、计数、时间和来源模板必须由服务端绑定，不能信任模型改写。

## 6. 审批与实时进度

Dashboard 必须立即返回任务 ID，并使用 SSE 推送：

```text
job_created
job_started
entity_started
entity_completed
entity_failed
feature_updated
job_completed
```

单实体失败不得阻塞后续实体，并允许人工重试。候选特征状态仅允许 `pending`、`approved`、`rejected`。人工可编辑标题、摘要、重要程度、标签和审批备注，不能修改服务端事实字段。

## 7. 导出规范

通用导出包使用 `schema_version: "1.0"`，包含生成时间、输入摘要、模型元数据、审批统计和 `approved_features`。只允许导出已批准特征，且不得出现原始日志或 RCA 结论字段。

## 8. 安全与运行约束

1. Dashboard 默认只监听 `127.0.0.1`；
2. 上传文件上限 10 MB；
3. 任务和审批状态只保存在内存；
4. 不引入数据库、Kafka、ES 或 npm 构建链；
5. Ollama 连接、超时或 Schema 错误必须显示在对应实体；
6. 不得自动生成、猜测或模拟 RCA 内容。

## 9. 测试与验收

```bash
export PYTHONPATH=$(pwd)/src
pytest -q
bash -n scripts/run_manual_pipeline.sh scripts/run_dashboard.sh
```

必须覆盖：规范化、稳定模板、聚合、风险评分、证据脱敏、特征 Schema、稳定候选 ID、串行任务、失败继续、重试、审批状态、仅批准项导出、HTTP 路由和 SSE。

人工验收：运行批处理，启动 Dashboard，上传 `output/result.json`，确认页面无需等待整批完成即可看到进度和候选特征；完成编辑、批准、驳回与导出，检查导出包仅包含批准且脱敏的特征。
