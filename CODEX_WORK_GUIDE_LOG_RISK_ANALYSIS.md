# Codex 工作指引：日志风险分析系统人工导入版后端闭环

## 1. 项目背景

当前项目目标是建设一套面向 K8s / IaaS 运维场景的日志风险分析与 RCA 辅助系统。

现阶段不接 Kafka、不接 ES、不接真实 LLM，先通过人工导入日志文件的方式验证后端全流程。

目标链路：

```text
人工导入 JSON / JSONL 日志文件
  ↓
Normalizer 日志规范化
  ↓
Drain3 模板化
  ↓
窗口聚合
  ↓
风险机器 / 风险实体评分
  ↓
Mock RCA 分析
  ↓
输出 result.json
```

系统后续会接入 Kafka / ES / LLM / UI，但当前阶段的核心任务是先把本地批处理链路跑通，并保证模块边界清晰，方便后续平滑替换为流式输入。

---

## 2. 当前阶段强约束

Codex 在本阶段开发时必须遵守以下约束：

1. **不要接 Kafka**
   - 当前输入来自本地 JSON / JSONL 文件。
   - 不要引入 Kafka Consumer / Producer。
   - 不要引入 Zookeeper / Kafka docker-compose。

2. **不要接 ES**
   - 当前不做 ES 查询、不做 scroll、不做 search_after。
   - 原始日志回查能力后续再做。

3. **不要接真实 LLM**
   - 当前只实现 Mock RCA。
   - 后续真实 LLM Gateway 再单独开发。
   - Mock RCA 需要输出类似真实 RCA 的结构化结果。

4. **不要引入数据库**
   - 当前所有中间结果都输出到 `output/*.json`。
   - 不要引入 Redis / PostgreSQL / ClickHouse / SQLite。

5. **保持模块可替换**
   - 当前是文件输入；
   - 后续 Kafka 输入时，应尽量复用 Normalizer、Drain3 Miner、Aggregator、Risk Engine、RCA Gateway 的核心函数。

6. **不要让 LLM 或 Mock RCA 直接处理原始日志流**
   - RCA 只能处理经过模板化、窗口聚合和风险评分后的证据包。

---

## 3. 目标运行命令

最终必须支持以下命令：

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

也需要支持脚本方式：

```bash
bash scripts/run_manual_pipeline.sh
```

执行完成后，必须生成：

```text
output/
  normalized_logs.json
  template_events.json
  template_windows.json
  risk_entities.json
  rca_results.json
  result.json
```

---

## 4. 输入数据要求

当前阶段推荐使用 JSONL。

示例：

```jsonl
{"timestamp":"2026-06-22T10:01:01+08:00","cluster":"prod-a","node":"node-a","source_type":"syslog","message":"Jun 22 10:01:01 node-a kubelet[2145]: E0622 10:01:01.123456 eviction_manager.go:350] eviction manager: pods ranked for eviction"}
{"timestamp":"2026-06-22T10:01:02+08:00","cluster":"prod-a","node":"node-a","source_type":"syslog","message":"Jun 22 10:01:02 node-a kernel: [88771.123456] Memory cgroup out of memory: Killed process 24811 (java) total-vm:2048000kB"}
{"timestamp":"2026-06-22T10:01:03+08:00","cluster":"prod-a","node":"node-a","source_type":"syslog","message":"Jun 22 10:01:03 node-a containerd[991]: failed to create shim task: OCI runtime create failed"}
{"timestamp":"2026-06-22T10:01:04+08:00","cluster":"prod-a","node":"node-a","namespace":"prod","pod":"pay-api-7d6b4f-xk9p2","container":"pay-api","source_type":"podlog","component":"pay-api","level":"ERROR","message":"connection reset by peer"}
```

输入字段允许不完整，但系统应尽量做容错：

| 字段 | 是否必需 | 说明 |
|---|---|---|
| `timestamp` | 建议必填 | 日志时间 |
| `cluster` | 建议必填 | 集群名，缺省可设为 `default` |
| `node` | 可选 | 节点名，可从 syslog 中解析 |
| `namespace` | 可选 | Pod 日志建议提供 |
| `pod` | 可选 | Pod 日志建议提供 |
| `container` | 可选 | Pod 日志建议提供 |
| `source_type` | 可选 | `syslog` / `podlog` / `k8s-component` |
| `component` | 可选 | kubelet / kernel / containerd / app name |
| `level` / `severity` | 可选 | 日志级别 |
| `message` | 必填 | 原始日志正文 |

---

## 5. 代码目录约定

请保持或调整为以下结构：

```text
log-risk-analysis-code/
  configs/
    drain3_recommended.ini
    drain3_original.ini
    risk_rules.yaml

  examples/
    sample_k8s_logs.jsonl
    sample_logs.json

  output/
    drain3_state/
    normalized_logs.json
    template_events.json
    template_windows.json
    risk_entities.json
    rca_results.json
    result.json

  src/
    logrisk/
      __init__.py
      io_utils.py
      normalizer.py
      drain_miner.py
      aggregator.py
      risk_engine.py
      rca_mock.py

    pipeline/
      __init__.py
      manual_import_pipeline.py

  tests/
    test_normalizer.py
    test_risk_engine.py
    test_pipeline.py

  scripts/
    run_manual_pipeline.sh

  requirements.txt
  README.md
  pyproject.toml
```

---

## 6. 模块开发要求

### 6.1 `io_utils.py`

职责：

1. 读取 JSON；
2. 读取 JSONL；
3. 写 JSON；
4. 写 JSONL。

必须支持：

```python
read_json_or_jsonl(path) -> list[dict]
write_json(path, data) -> None
write_jsonl(path, rows) -> None
```

容错要求：

- 空文件返回空列表；
- JSON 顶层可以是数组；
- JSON 顶层可以是 `{"logs": [...]}` / `{"data": [...]}` / `{"records": [...]}`；
- JSONL 每行必须是 object；
- 字符串日志可以转换为 `{"message": "..."}`。

---

### 6.2 `normalizer.py`

职责：

将原始日志转换为统一结构。

核心函数：

```python
normalize_record(record: dict, default_cluster: str = "default") -> dict
normalize_records(records: list[dict], default_cluster: str = "default") -> list[dict]
```

输出字段必须包含：

```json
{
  "raw_log_id": null,
  "timestamp": "2026-06-22T10:01:01+08:00",
  "cluster": "prod-a",
  "node": "node-a",
  "namespace": null,
  "pod": null,
  "container": null,
  "source_type": "syslog",
  "component": "kubelet",
  "severity": "ERROR",
  "message_core": "eviction manager: pods ranked for eviction",
  "raw_log": "Jun 22 10:01:01 node-a kubelet[2145]: ...",
  "labels": {}
}
```

必须支持的日志格式：

#### syslog

```text
Jun 22 10:01:01 node-a kubelet[2145]: message
Jun 22 10:01:02 node-a kernel: message
Jun 22 10:01:03 node-a containerd[991]: message
```

需要解析：

- node；
- process/component；
- message。

#### K8s klog

```text
E0622 10:01:01.123456 2145 eviction_manager.go:350] eviction manager: pods ranked for eviction
```

需要解析：

- level：`I/W/E/F` → `INFO/WARN/ERROR/FATAL`；
- source file；
- message_core。

其中 `message_core` 应只保留真正的日志正文。

#### Pod 日志

如果输入中已经有：

```json
{
  "namespace": "prod",
  "pod": "pay-api-7d6b4f-xk9p2",
  "container": "pay-api",
  "component": "pay-api",
  "level": "ERROR",
  "message": "connection reset by peer"
}
```

则应保留 metadata，并将：

```text
message_core = "connection reset by peer"
```

---

### 6.3 `drain_miner.py`

职责：

使用 Drain3 对 `message_core` 做模板化。

核心函数：

```python
mine_template_events(records, config_path, state_dir) -> list[dict]
```

必须实现：

1. 按以下维度分片维护 Drain3 Miner：

```text
cluster + source_type + component
```

2. 不要使用 Drain3 自增 `cluster_id` 作为长期主键。

3. 生成稳定模板 ID：

```python
template_hash = sha1(cluster + source_type + component + template)[:16]
```

4. 输出 template event：

```json
{
  "event_id": null,
  "timestamp": "2026-06-22T10:01:01+08:00",
  "cluster": "prod-a",
  "node": "node-a",
  "namespace": null,
  "pod": null,
  "container": null,
  "source_type": "syslog",
  "component": "kubelet",
  "severity": "ERROR",
  "template_hash": "xxxx",
  "template": "eviction manager: pods ranked for eviction",
  "parameters": [],
  "message_core": "eviction manager: pods ranked for eviction",
  "raw_sample": "Jun 22 10:01:01 node-a kubelet[2145]: ...",
  "change_type": "cluster_created"
}
```

5. Drain3 state 文件应保存到：

```text
output/drain3_state/
```

---

### 6.4 `aggregator.py`

职责：

将逐条 template event 聚合成窗口事件。

核心函数：

```python
aggregate_template_events(events, window_seconds=300, max_samples_per_template=3) -> list[dict]
```

聚合维度：

```text
window_start
window_end
cluster
entity_type
entity_id
component
template_hash
```

实体识别优先级：

```text
node > pod > unknown
```

输出字段：

```json
{
  "window_start": "2026-06-22T10:00:00+08:00",
  "window_end": "2026-06-22T10:05:00+08:00",
  "cluster": "prod-a",
  "entity_type": "node",
  "entity_id": "node-a",
  "source_type": "syslog",
  "component": "kubelet",
  "severity": "ERROR",
  "template_hash": "xxxx",
  "template": "eviction manager: pods ranked for eviction",
  "count": 2,
  "first_seen": "2026-06-22T10:01:01+08:00",
  "last_seen": "2026-06-22T10:02:01+08:00",
  "samples": ["..."],
  "affected_namespaces": [],
  "affected_pods": []
}
```

要求：

- 每个模板最多保留 3 条样本；
- 需要记录 affected namespace/pod；
- 需要支持 1m / 5m / 15m 窗口参数。

---

### 6.5 `risk_engine.py`

职责：

根据规则文件对模板窗口评分，并聚合成风险实体。

核心函数：

```python
load_rules(path) -> dict
score_risk_entities(windows, rules) -> list[dict]
```

规则文件：

```text
configs/risk_rules.yaml
```

评分建议：

```text
window_score =
  template_weight
  × severity_weight
  × component_weight
  × count_weight
  / 2
```

风险等级：

```text
score >= 90: critical
score >= 70: high
score >= 40: medium
else: low
```

必须支持的模板规则：

1. kernel OOM；
2. kubelet eviction；
3. containerd shim / OCI runtime failed；
4. disk pressure / image garbage collection；
5. apiserver / etcd timeout。

输出风险实体：

```json
{
  "window_start": "...",
  "window_end": "...",
  "cluster": "prod-a",
  "entity_type": "node",
  "entity_id": "node-a",
  "risk_score": 96,
  "risk_level": "critical",
  "top_templates": [],
  "affected_entities": [],
  "summary": "节点或 cgroup 出现 OOM，优先检查内存水位、异常进程、Pod requests/limits。"
}
```

---

### 6.6 `rca_mock.py`

职责：

在不接真实 LLM 的情况下生成结构化 RCA 结果。

核心函数：

```python
generate_mock_rca(entities, min_score=40) -> list[dict]
```

Mock 判断逻辑：

#### OOM + eviction

如果风险实体中出现：

- `out of memory`；
- `OOM`；
- `eviction`；

输出：

```text
高概率为节点内存压力 / OOM 引发 kubelet 驱逐或业务 Pod 异常
```

#### containerd / OCI runtime failed

输出：

```text
高概率为容器运行时异常导致 Pod 创建或启动失败
```

#### disk pressure

输出：

```text
可能为节点磁盘压力或镜像垃圾回收异常
```

#### 只有业务 Pod 错误

输出：

```text
当前证据不足，可能为业务侧或依赖侧异常
```

输出结构：

```json
{
  "window_start": "...",
  "window_end": "...",
  "cluster": "prod-a",
  "risk_entity": {
    "type": "node",
    "id": "node-a"
  },
  "risk_score": 96,
  "risk_level": "critical",
  "root_cause_candidate": "...",
  "confidence": 0.86,
  "evidence_chain": [
    {
      "time": "...",
      "evidence": "...",
      "interpretation": "..."
    }
  ],
  "impact": "...",
  "suggested_actions": [],
  "need_more_data": []
}
```

---

### 6.7 `manual_import_pipeline.py`

职责：

串联完整链路。

核心函数：

```python
run_pipeline(
    input_path,
    output_dir,
    config_path,
    rules_path,
    state_dir,
    window_seconds=300,
    mock_llm=True,
) -> dict
```

执行顺序：

```python
raw_records = read_json_or_jsonl(input_path)
normalized = normalize_records(raw_records)
template_events = mine_template_events(normalized)
template_windows = aggregate_template_events(template_events)
risk_entities = score_risk_entities(template_windows)
rca_results = generate_mock_rca(risk_entities)
result = merge_all()
```

必须输出：

```text
normalized_logs.json
template_events.json
template_windows.json
risk_entities.json
rca_results.json
result.json
```

`result.json` 必须包含：

```json
{
  "summary": {
    "total_raw_logs": 0,
    "total_normalized_logs": 0,
    "total_template_events": 0,
    "total_template_windows": 0,
    "total_risk_entities": 0,
    "critical_entities": 0,
    "high_entities": 0
  },
  "risk_entities": [],
  "rca_results": [],
  "top_templates": [],
  "debug_files": {}
}
```

---

## 7. 测试要求

至少补充以下测试：

```text
tests/test_normalizer.py
tests/test_risk_engine.py
tests/test_pipeline.py
```

### 7.1 Normalizer 测试

必须覆盖：

1. syslog kubelet；
2. syslog kernel；
3. syslog containerd；
4. klog level 解析；
5. podlog metadata 保留。

### 7.2 Drain3 Miner 测试

必须覆盖：

1. 相同结构不同参数聚合到同一模板；
2. 不同 component 使用不同分片；
3. template_hash 稳定。

### 7.3 Aggregator 测试

必须覆盖：

1. 5 分钟窗口；
2. 同 template_hash count 聚合；
3. samples 最多 3 条；
4. affected_pods 聚合。

### 7.4 Risk Engine 测试

必须覆盖：

1. OOM 得到高风险；
2. eviction 得到高风险；
3. containerd failed 得到高风险；
4. 普通 INFO 得到低风险。

### 7.5 Pipeline 测试

必须覆盖：

1. 使用 `examples/sample_k8s_logs.jsonl`；
2. 能生成所有 output 文件；
3. `result.json.summary.total_raw_logs > 0`；
4. 至少生成一个 high 或 critical 风险实体。

运行测试：

```bash
export PYTHONPATH=$(pwd)/src
pytest -q
```

---

## 8. 验收标准

完成后必须满足以下命令可用：

```bash
bash scripts/run_manual_pipeline.sh
```

执行后：

```bash
jq '.summary' output/result.json
```

应能看到类似：

```json
{
  "total_raw_logs": 10,
  "total_normalized_logs": 10,
  "total_template_events": 10,
  "total_template_windows": 7,
  "total_risk_entities": 3,
  "critical_entities": 1,
  "high_entities": 1
}
```

查看风险实体：

```bash
jq '.risk_entities[] | {entity_id, risk_score, risk_level, summary}' output/result.json
```

查看 RCA：

```bash
jq '.rca_results[] | {risk_entity, root_cause_candidate, confidence}' output/result.json
```

必须能识别：

1. `node-a` 存在 OOM / eviction 风险；
2. `node-c` 可能存在 containerd runtime 风险；
3. RCA 输出包含 evidence_chain 和 suggested_actions。

---

## 9. 禁止事项

当前阶段不要做以下事情：

1. 不要引入 Kafka；
2. 不要引入 ES；
3. 不要引入数据库；
4. 不要接真实 LLM；
5. 不要把所有逻辑写进一个大脚本；
6. 不要让 Mock RCA 直接分析原始日志；
7. 不要使用 Drain3 自增 `cluster_id` 作为长期模板 ID；
8. 不要把 syslog 完整前缀直接作为 Drain3 的主要分析对象；
9. 不要删除中间输出文件，中间文件用于调试和验证。

---

## 10. 后续扩展方向

当前阶段完成后，再进入下一阶段：

### Phase 2：真实 LLM Gateway

- 加入 prompt 模板；
- 输出 JSON schema；
- 增加缓存；
- 增加限流；
- 增加失败降级。

### Phase 3：Kafka 化

将：

```text
read_json_or_jsonl(input_path)
```

替换为：

```text
KafkaConsumer(logs.normalized)
```

其他核心函数保持复用。

### Phase 4：ES 回查

在 RCA 结果中增加：

```text
raw_log_id
es_index
query_condition
```

用于从 UI 回查原始日志。

### Phase 5：前端 UI

展示：

1. 风险机器榜；
2. 异常模板；
3. RCA 时间线；
4. 原始日志样本；
5. 模板治理；
6. 规则沉淀。
