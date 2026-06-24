# Codex 工作指引：日志风险特征分析与人工审批

## 1. 项目目标与边界

项目面向 K8s / IaaS 运维日志，通过本地文件验证日志规范化、模板提取、窗口聚合、风险评分和关键特征审批流程。

本项目不实现 RCA。不得生成根因结论、影响评估、处置建议或模拟 RCA。Ollama 只负责从聚合、评分后的证据中识别值得提交给外部 RCA 日志专家的候选特征。

当前阶段不接 Kafka、Elasticsearch、数据库或外部 LLM 服务。外部 RCA 系统仅通过人工下载并导入已批准 JSON 特征包衔接。

## 2. 当前数据流

```text
JSON / JSONL / TXT / LOG 日志
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
全局批准规则匹配（命中时跳过 LLM）
  ↓ 未命中
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
frontend/src/                   纯 React 源码
frontend/dist/                  已提交、无需构建的静态运行文件
scripts/run_manual_pipeline.sh   生成 result.json
scripts/run_dashboard.sh         启动本地审批服务
src/logrisk/
  input_parser.py               JSON / JSONL / 纯文本统一解析
  io_utils.py                    文件 I/O
  normalizer.py                  日志规范化
  drain_miner.py                 稳定模板提取
  aggregator.py                  时间窗口聚合
  risk_engine.py                 风险评分与 feature_hint
  feature_extractor_ollama.py    Ollama 特征识别
  approved_rules.py              全局批准规则持久化
  processing_metrics.py          当日 LLM 关联日志量
  feature_jobs.py                任务、规则复用、审批与导出
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

Ollama 默认地址为 `http://127.0.0.1:11434`，默认模型为 `qwen3:1.7b`。每个未命中批准规则的风险实体单独请求并按风险分串行处理，默认阈值为 `40`。规则库使用模板 Hash、类别和特征类型全局匹配，不区分集群或节点。

允许发送：实体、窗口、风险分/等级、组件、严重度、模板文本/Hash、规则类别、计数、首末时间、受影响实体和 `feature_hint`。

禁止发送：`samples`、`raw_sample`、原始日志流或其他未脱敏文本。

模型只能返回候选特征的类别、标题、摘要、重要程度、模板 Hash、组件、标签和入选理由。实体、风险、计数、时间和来源模板必须由服务端绑定，不能信任模型改写。

## 6. 审批与实时进度

Dashboard 必须立即返回任务 ID，并使用 SSE 推送：

```text
job_created
job_started
entity_rule_matched
entity_started
entity_completed
entity_failed
feature_updated
job_completed
```

单实体失败不得阻塞后续实体，并允许人工重试。候选特征状态仅允许 `pending`、`approved`、`rejected`。人工可编辑标题、摘要、重要程度、标签和审批备注，不能修改服务端事实字段。

审批页采用候选特征、证据、编辑器三栏工作区。选择候选特征后，证据栏必须展示其 `source_templates` 中的 Drain3 脱敏模板证据，包括模板、Hash、组件、类别、严重度、计数和时间范围。系统不保存原始日志；原始日志不可用时不得伪造或回查，只展示上述脱敏特征日志，并明确提示数据边界。

## 7. 导出规范

通用导出包使用 `schema_version: "1.0"`，包含生成时间、输入摘要、模型元数据、审批统计和 `approved_features`。只允许导出已批准特征，且不得出现原始日志或 RCA 结论字段。

## 8. 安全与运行约束

1. Dashboard 默认只监听 `127.0.0.1`；
2. 上传文件上限 10 MB；
3. 任务保存在内存；批准规则和当日指标原子写入 `state/`；
4. 不引入数据库、Kafka、ES、Vite、前端 CDN 或运行时构建链；
5. Ollama 连接、超时或 Schema 错误必须显示在对应实体；
6. 不得自动生成、猜测或模拟 RCA 内容。

## 9. 测试与验收

```bash
export PYTHONPATH=$(pwd)/src
pytest -q
bash -n scripts/*.sh
```

必须覆盖：规范化、稳定模板、聚合、风险评分、证据脱敏、特征 Schema、稳定候选 ID、串行任务、失败继续、重试、审批状态、仅批准项导出、HTTP 路由和 SSE。

人工验收：上传 `result.json` 或纯文本日志，确认 Drain3 压缩、分析速度和进度实时更新；批准一条特征后重新分析匹配日志，确认显示“规则复用 / 跳过 LLM”；重启服务后确认规则库和当日指标仍存在；检查导出包仅包含批准且脱敏的特征。

## 10. 版本发布规则

每次代码更新必须同步更新根目录 `releas.md`。版本格式为 `1.<功能版本>.<Bug版本>`：功能发布提升中间位并将最后一位归零；仅修复 Bug 时提升最后一位。不得在没有版本记录的情况下完成代码变更。
