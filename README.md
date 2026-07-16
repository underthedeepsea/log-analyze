# 日志风险特征分析与审批系统

当前版本：`1.20.1`。完整变更记录见 [`releas.md`](releas.md)。

LOGRISK 在本地完成日志规范化、Drain3 模板化、确定性语义增强、风险评分、规则复用、模型特征识别和人工审批。系统只生成可审查、可导出的日志特征，不执行根因分析（RCA），也不会把原始日志直接发送给模型。

## 核心能力

- 支持 JSON、JSONL、纯文本、无后缀 Linux 日志和 Gzip 大文件。
- 使用 Drain3 压缩日志，并保留 HTTP 状态码、errno、exit code、signal、NVIDIA Xid 和 Kubernetes Reason 等关键语义。
- 已批准规则优先匹配，命中后跳过模型调用；未知特征才进入 AI 分析和人工审批。
- 支持本地 Ollama 与 OpenAI-compatible `/v1/chat/completions` 服务。
- 提供 Prompt 版本、模型 Profile、AI Trace、缓存、评测、Drain3 配置和语义词典治理。
- 只导出人工批准的特征及关联风险节点，供外部 RCA 专家系统使用。

```text
日志文件
   ↓
规范化 → Drain3 模板化 → 语义增强 → 风险评分
   ↓
批准规则匹配 ──命中──→ 规则复用（跳过模型）
   │未命中
   ↓
AI Cache → Ollama / OpenAI-compatible 特征识别
   ↓
人工审批 → 批准规则库 → 导出到外部 RCA 系统
```

## 快速开始

### 1. 创建 Python 环境

依赖环境固定放在项目根目录 `.venv/`。该目录已被 Git 忽略，丢失后可随时重建：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 准备模型

使用本地 Ollama 时：

```bash
ollama serve
ollama pull qwen3:1.7b
```

也可以在 Dashboard 的“模型画像”页面配置远端 OpenAI-compatible 服务，无需安装 Ollama。

### 3. 启动 Dashboard

```bash
bash scripts/dashboard.sh start
bash scripts/dashboard.sh status
```

访问 [http://127.0.0.1:8080](http://127.0.0.1:8080)。常用管理命令：

```bash
bash scripts/dashboard.sh restart
bash scripts/dashboard.sh stop
```

启动脚本使用轻量 `nohup` 后台进程，不注册 `launchd` 或其他常驻系统服务。日志位于 `state/dashboard.log`，PID 位于 `state/dashboard.pid`；前台运行可使用 `bash scripts/run_dashboard.sh`。

## 日志输入与处理

Dashboard 和命令行支持：

- `.json`、`.jsonl`、`.ndjson`、`.txt`、`.log`；
- Linux `messages` 等无后缀 UTF-8 文本日志；
- `.gz` 压缩日志；
- 已生成的 `result.json`。

纯文本中的每个非空行都会作为一条日志进入完整分析流程。命令行示例：

```bash
python3 -m pipeline.manual_import_pipeline \
  --input examples/sample_plain_logs.log \
  --output-dir output \
  --config configs/drain3_recommended.ini \
  --rules configs/risk_rules.yaml \
  --state-dir output/drain3_state
```

10MB 以内文件使用 inline 分析；超过 10MB 后自动按 1MB 分片上传并创建异步任务。默认单文件上限为 500MB。大文件采用流式规范化和多进程 Drain3 分区处理，同一“集群 + 节点 + 来源 + 组件”分区保持原始顺序。并行度、Gzip 解压限制和单行大小限制配置在 `configs/runtime.yaml`。

## 模型连接与 Profile

“模型画像”页面将 API 连接和模型 Profile 分开管理：

- 连接保存 Provider、Base URL、超时和 API Key 环境变量名；
- Profile 保存模型名、Prompt、Thinking、Evidence 预算、输出预算和结构化输出模式；
- 新建任务会锁定连接、Profile、模型和 Prompt 快照，重试不会切换 Provider；
- 支持 `json_schema`、`json_object` 和 `prompt_only` 三种输出模式。

远端服务的 Base URL 应包含 `/v1`。API Key 只从环境变量读取，不会写入 SQLite、日志或 AI Trace：

```bash
export REMOTE_LLM_API_KEY='replace-with-secret'
bash scripts/dashboard.sh restart
```

随后在“模型画像 → API 连接”中新建 `openai_compatible` 连接，将 API Key 环境变量填写为 `REMOTE_LLM_API_KEY`，测试成功后绑定模型 Profile。不支持 `response_format` 的服务请选择 `prompt_only`。

仓库内置 `qwen3.5:4b-mlx`、`qwen3:1.7b`、`qwen3.6:35b-a3b` 和 `deepseek-v4:flash` Profile。默认 Profile 为 `qwen3_1_7b_fast`，默认 Prompt 为 `feature_extract_v3_compact_strict_json_en`。`qwen3.5:4b-mlx` 的默认输出预算为 1600 tokens。

分析任务可配置自动重试 0–3 次。缺少字段、JSON 无效或结构不合法时，会使用同一连接和 Profile 重试，不会隐式降级。AI Cache 默认启用；相同 Evidence、Prompt、Provider、模型和 Thinking 配置会复用结果。调试时可临时关闭：

```bash
AI_CACHE_ENABLED=0 bash scripts/dashboard.sh restart
```

## Prompt、观测与审批

Dashboard 提供以下工作区：

- **Prompt 管理**：编辑当前版本并查看不可变历史版本；
- **模型画像**：管理模型连接、上下文预算和结构化输出方式；
- **AI 分析观测**：查看任务阶段、裁剪预算、缓存命中、质量门禁和失败原因；
- **AI 调用追踪**：按任务、Trace、状态和 Prompt 查询实际调用快照；
- **人工审批**：逐条选择脱敏模板证据，编辑标题、摘要、标签和审批备注；
- **批准规则库**：查看规则来源、复用记录和完整 Lineage。

规则 Lineage 包含来源任务、候选特征、Trace、Prompt、模型、Provider 和 Evidence Hash。批准规则命中后会直接生成复用特征并跳过模型调用。导出包只包含已批准特征和关联风险节点。

## Drain3 与语义治理

### Drain3 配置和模板质量

`configs/drain3_recommended.ini` 是只读基线，包含算法参数和脱敏规则。可在“评测中心 → 模板质量”复制候选配置、编辑完整 INI、执行配置校验、关联 Gold Dataset 评测，并人工发布或回滚。

质量中心展示 Grouping F1、Over-merge、Over-split、Singleton、Wildcard 和 Churn，支持模板标注、编辑、忽略、合并、恢复、软删除和版本回滚。发布不会覆盖仓库基线，只影响之后新建的任务；运行中任务继续使用创建时锁定的版本。

### 确定性语义词典

内置词典位于 `configs/semantic_dictionary/`，用于从模板参数中识别有界语义，而不改变 Drain3 聚类结构。当前覆盖 Linux、Kubernetes、NVIDIA GPU 和容器运行时。

“语义词典”页面支持创建候选版本、编辑自定义规则、输入单条日志测试、校验、发布和回滚。内置规则只读，自定义规则用于扩展业务日志语义；发布只影响后续任务。

## SQLite 存储

运行期业务状态默认保存在 `state/logrisk.sqlite3`。启动时自动应用 `database/migrations/` 中的版本化 SQL；`database/schema.yaml` 描述表结构、字段用途和未来 PostgreSQL 类型映射。SQLite 启用外键、WAL、`busy_timeout` 和事务写入。

仓库中的配置和 Prompt 只作为首次启动种子，初始化后以 SQLite 为运行时权威来源。旧 JSON、JSONL、YAML 和 Prompt 历史会按 SHA256 幂等导入。原始上传文件、分片、Drain3 `.bin` 和导出物继续保存在文件系统，SQLite 仅登记元数据。

以下基础种子会提交到仓库：

- `configs/risk_rules.yaml`
- `configs/model_profiles.yaml`
- `configs/drain3_recommended.ini`
- `configs/drain3_profiles/`
- `configs/semantic_dictionary/`
- `prompts/`

本机数据库、WAL/SHM、日志、上传内容和生成结果均被 Git 忽略。

## 部署配置

前端是已提交到 `frontend/dist/` 的纯 React 静态应用，不依赖 Vite、CDN 或运行时编译。前后端可分开部署：默认后端地址在 `frontend/dist/config.js` 设置，页面“系统设置”可保存浏览器级覆盖。

跨域部署时配置允许来源：

```bash
DASHBOARD_CORS_ORIGINS=https://logrisk.example.internal \
  bash scripts/dashboard.sh start
```

常用环境变量包括 `LOGRISK_DB_PATH`、`OLLAMA_MODEL`、`OLLAMA_HOST`、`OLLAMA_TIMEOUT`、`DASHBOARD_HOST` 和 `DASHBOARD_PORT`。Dashboard 默认仅监听 `127.0.0.1`。

## 评测与测试

内置评测用例位于 `eval_cases/canonical/`，覆盖 OOM 驱逐、containerd runtime 失败、磁盘压力、Pod 业务错误和普通 warning 误报控制。

运行 AI Eval Runner：

```bash
OLLAMA_MODEL=qwen3:1.7b .venv/bin/python -m logrisk.ai_eval.runner
```

结果写入 `output/eval_results.json`，包含通过率、JSON/Schema 有效率、模板引用准确率和禁止结论计数。

运行 Promptfoo/Ollama 回归评测：

```bash
npm ci
OLLAMA_BASE_URL=http://127.0.0.1:11434 npm run promptfoo:eval
```

运行自动化测试：

```bash
pytest -q
bash -n scripts/*.sh
```

HTTP、SSE 和 Provider 测试使用模拟提取器或模拟 HTTP 服务，不要求启动 Ollama 或远端模型。

## 安全边界

- 模型只接收聚合、脱敏后的实体和模板 Evidence；
- 禁止发送 `samples`、`raw_sample` 或原始日志流；
- API Key 只从环境变量读取，不进入数据库、Trace、错误消息或前端响应；
- 系统不生成根因、业务影响或处置建议；
- 只有人工批准的特征可以导出到外部 RCA 专家系统。
