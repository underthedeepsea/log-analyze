# 日志风险特征分析与审批系统

<p align="center">
  <img src="frontend/logo/logrisk-app-icon-orange-v2.png" width="112" alt="LOGRISK 应用图标" />
</p>

当前版本：`1.27.0`。完整变更记录见 [`releas.md`](releas.md)。

LOGRISK 在本地完成日志规范化、Drain3 模板化、确定性语义增强、风险评分、规则复用、模型特征识别和人工审批。系统只生成可审查、可导出的日志特征，不执行根因分析（RCA），也不会把原始日志直接发送给模型。

## 核心能力

- 支持 JSON、JSONL、纯文本、无后缀 Linux 日志和 Gzip 大文件。
- 使用 Drain3 压缩日志，并保留 HTTP 状态码、errno、exit code、signal、NVIDIA Xid 和 Kubernetes Reason 等关键语义。
- 大文件支持可恢复的文件 Checkpoint、批次提交和未知模板治理队列；Kafka 仅保留受控内部适配器契约，不连接 Broker。
- 已批准且处于启用状态的规则优先匹配，命中后跳过模型调用；未知特征才进入 AI 分析和人工审批。
- 支持本地 Ollama、OpenAI-compatible `/v1/chat/completions` 服务和可扩展的内部 Token 鉴权 Provider 模板。
- 提供 Prompt 版本、模型 Profile、AI Trace、Observation/Span 链路、缓存、评测、Drain3 配置和语义词典治理。
- 提供统一的评测与基准中心，可比较 Prompt、模型 Profile、失败 Case、趋势及发布门禁。
- 提供服务器风险总览、风险事件台账、可解释评分，以及可编辑、可发布、可回滚的风险语义库。
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

也可以在 Dashboard 的“模型画像”页面配置远端 OpenAI-compatible 服务或内部扩展适配器，无需安装 Ollama。

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

### 可恢复处理与 Kafka 预留接口

“流式处理”工作区会显示大文件任务的来源、Drain3 配置摘要、最后成功 Checkpoint、提交批次和脱敏未知模板队列。文件任务按有界记录批次处理；每个批次在 Drain3 模板化、语义/规则判定后，以单个数据库事务同时提交脱敏摘要、未知模板和字节 Offset。服务重启会将运行中任务标记为中断，只有人工点击“从 Checkpoint 恢复”才会继续。文件身份、内容前缀或 Drain3 配置变化会标记为冲突，不能静默重读或跳过数据。

Kafka 目前不是可用数据源：页面只展示 Topic、Consumer Group、Partition Offset、Bootstrap 环境变量名和 adapter ID 的契约。仓库不包含 Kafka 客户端、Broker 连接、凭据输入或自动消费；内部团队必须在受控代码中显式注册适配器后，才能由后续任务启动器使用。任务状态、审计事件和未知模板不保存原始日志、Kafka Token 或密码。

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

对于内部的 Token、签名或私有 SDK 协议，请选择 `extension` 连接并使用已提交的适配器模板。模板与核心任务链路隔离，连接仅保存适配器 ID、非敏感配置和环境变量名；实际 Token 不会保存或展示。请按 [本地扩展模型 Provider 开发指南](LOCAL_PROVIDER_DEVELOPMENT_GUIDE.md) 在内部环境完成适配。

仓库内置 `qwen3.5:4b-mlx`、`qwen3.5:9b-mlx`、`qwen3:1.7b`、`qwen3.6:35b-a3b` 和 `deepseek-v4:flash` Profile。默认 Profile 仍为 `qwen3_1_7b_fast`，默认 Prompt 为 `feature_extract_v3_compact_strict_json_en`。`qwen3.5:9b-mlx` 使用 262144 tokens 上下文、12000 tokens 推荐输入预算和 2000 tokens 输出预算，默认关闭 Thinking 以提高结构化 JSON 稳定性。

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
- **规则治理**：查看规则健康度、生命周期状态、版本历史、误报反馈、复审队列和完整 Lineage。

人工审批表单默认保留模型返回的特征标题与标签；摘要同时展示模型摘要和当前选中的 Drain3 脱敏模板，便于审查语义判断与确定性日志证据。证据区明确区分“候选特征”和“关联证据模板”，并展示每个模板的 Xid、风险语义、严重级别和独立 Hash。

AI 分析观测使用一个 Observation 汇总一次任务，并以 Span 展示输入、规范化、Drain3、聚合、规则/缓存、Evidence、Prompt、模型、JSON 解析、Schema、Evaluator、候选和审批阶段。页面支持阶段筛选、Span 详情、成功率、P50/P95 延迟及 Token 用量统计；模型 Profile 同时支持可选的每百万输入/输出 Token 单价，缺少 usage 或单价时成本明确显示“不可计算”。历史结果 Replay 不调用模型；“重新调用原模型”必须人工确认，并使用来源 Trace 锁定的 Prompt、Profile、Provider、模型参数和脱敏 Evidence。两种 Replay 都不会写入候选、批准规则或审批记录。

所有启用的特征提取 Prompt 都必须明确声明 `feature_type`、`title`、`summary`、`importance`、`template_hashes`、`components`、`tags` 和 `selection_reason` 八个字段，并要求 `feature_type` 使用 `lowercase_snake_case`。启动时会把仍使用旧六字段契约的内置 Prompt 追加升级为新版本，同时保留原历史；页面保存缺少必填字段的 Prompt 时会返回明确错误。

规则 Lineage 包含来源任务、候选特征、Trace、Prompt、模型、Provider 和 Evidence Hash。只有 `active` 规则参与匹配；命中后会直接生成复用特征并跳过模型调用。导出包只包含已批准特征和关联风险节点。

## 规则生命周期治理

“规则治理”页面将批准规则作为版本化资产管理：

- 生命周期状态包括 `active`、`disabled`、`under_review`、`deprecated` 和 `archived`；
- 健康度展示 7/30 天命中、最后命中、跨集群命中、30 天误报率和下次复审时间；
- 人工反馈支持“命中有效”和“误报”，异常健康度会进入复审队列；
- 状态变更和回滚要求填写原因并校验当前版本，避免并发覆盖；
- 回滚通过追加新版本实现，历史快照和审计事件不会被覆盖。

旧规则首次迁移后默认设为 `active` 和版本 `v1`。兼容接口 `/api/rules` 继续可用；治理接口统一位于 `/api/rule-governance/`。

## Drain3 与语义治理

### Drain3 配置和模板质量

`configs/drain3_recommended.ini` 是只读基线，包含算法参数和脱敏规则。可在“评测中心 → 模板质量”复制候选配置、编辑完整 INI、执行配置校验、关联 Gold Dataset 评测，并人工发布或回滚。

质量中心展示 Grouping F1、Over-merge、Over-split、Singleton、Wildcard 和 Churn，支持模板标注、编辑、忽略、合并、恢复、软删除和版本回滚。发布不会覆盖仓库基线，只影响之后新建的任务；运行中任务继续使用创建时锁定的版本。

### 确定性语义词典

内置词典位于 `configs/semantic_dictionary/`，用于从模板参数中识别有界语义，而不改变 Drain3 聚类结构。当前覆盖 Linux、Kubernetes、NVIDIA GPU 和容器运行时。

“语义词典”页面支持创建候选版本、编辑自定义规则、输入单条日志测试、校验、发布和回滚。内置规则只读，自定义规则用于扩展业务日志语义；发布只影响后续任务。

### 风险语义与服务器风险

“风险语义库”与上述字段词典分工不同：字段词典保留 HTTP 状态码、errno 等 Typed Parameters；风险语义库把日志模式分类为稳定的 `risk_type`，例如 Xid 35 为 `gpu.video_processor_exception`、Xid 79 为 `gpu.fallen_off_bus`。两条日志即使共享同一 Drain3 结构模板，也不会被合并成相同风险含义。

内置风险语义位于 `configs/risk_semantics/builtin.yaml`，首批覆盖 NVIDIA Xid/SXid、Kubernetes Node/Pod 状态、Linux 内存/存储/网络/进程以及容器运行时。页面支持正负样例测试、内置规则覆盖、版本保存、人工发布和回滚。风险语义与节点事件写入统一的 `state/logrisk.sqlite3`，历史事件锁定命中时的语义版本。

“服务器风险”页面区分风险事件数、日志命中次数（`occurrence_count`）和风险类型数，并展示未恢复事件、主要风险及确定性评分贡献。Xid 79 等直接基础设施故障可触发配置化 Hard Override；评分不由 AI 决定，页面中的动作代码也只用于审计和外部流程参考，不会自动执行运维操作。

## 运行期存储：SQLite 与 PostgreSQL

默认使用 `state/logrisk.sqlite3`。启动时应用 `database/migrations/`；SQLite 启用外键、WAL、`busy_timeout` 和短事务写入。`database/schema.yaml` 是双模式数据字典，PostgreSQL 对应迁移保存在 `database/postgres/migrations/`。批准规则、风险语义、节点风险、版本、反馈、Trace、评测和审计记录均使用同一运行数据库。

生产环境可显式改用外部 PostgreSQL，不提供 Docker Compose，也不会自动回退到 SQLite：

```bash
pip install -r requirements-postgres.txt
export LOGRISK_DATABASE_PROVIDER=postgres
export LOGRISK_DATABASE_URL='postgresql://logrisk:replace-with-secret@db.example:5432/logrisk?sslmode=require'
bash scripts/dashboard.sh start
```

`LOGRISK_DATABASE_URL` 优先于页面候选配置；命令行 `--database-provider postgres --database-url ...` 优先于环境变量。页面“系统设置 → PostgreSQL 运行数据库”可保存不含密码的主机、端口、库名、用户、SSL 模式和密码环境变量名，测试连接后仍需重启才会切换。密码、完整 DSN 不会保存到数据库、配置文件、Trace、日志或前端响应。

### 停机迁移到 PostgreSQL

迁移只复制数据库元数据；不会复制原始日志、上传分片、Drain3 `.bin` 或导出文件。先停服务并备份原 SQLite 文件，再执行：

```bash
bash scripts/dashboard.sh stop
.venv/bin/python -m pipeline.database_migrate --source-sqlite state/logrisk.sqlite3 --dry-run
LOGRISK_DATABASE_URL="$LOGRISK_DATABASE_URL" .venv/bin/python -m pipeline.database_migrate \
  --source-sqlite state/logrisk.sqlite3 --execute
LOGRISK_DATABASE_URL="$LOGRISK_DATABASE_URL" .venv/bin/python -m pipeline.database_migrate \
  --source-sqlite state/logrisk.sqlite3 --verify
LOGRISK_DATABASE_PROVIDER=postgres LOGRISK_DATABASE_URL="$LOGRISK_DATABASE_URL" \
  bash scripts/dashboard.sh start
```

`--execute` 在提交前核对每张表的行数、主键集合和规范化摘要，并由 PostgreSQL 外键约束验证关系；任一步失败会回滚目标导入。报告还会检查已登记 Artifact 路径是否存在。若需回滚，停服务、移除 `LOGRISK_DATABASE_PROVIDER` 和 `LOGRISK_DATABASE_URL` 后重新启动，即可继续使用未被修改的原 SQLite 文件。

仓库中的配置和 Prompt 只作为首次启动种子，初始化后以当前 Provider 为运行时权威来源。旧 JSON、JSONL、YAML 和 Prompt 历史会按 SHA256 幂等导入。原始上传文件、分片、Drain3 `.bin` 和导出物继续保存在文件系统，数据库仅登记元数据。

以下基础种子会提交到仓库：

- `configs/risk_rules.yaml`
- `configs/model_profiles.yaml`
- `configs/drain3_recommended.ini`
- `configs/drain3_profiles/`
- `configs/semantic_dictionary/`
- `prompts/`

本机数据库、WAL/SHM、日志、上传内容和生成结果均被 Git 忽略。

## 部署配置

前端是已提交到 `frontend/dist/` 的纯 React 静态应用，不依赖 CDN 或运行时编译，普通启动不需要 Node.js。前后端可分开部署：默认后端地址在 `frontend/dist/config.js` 设置，页面“系统设置”可保存浏览器级覆盖。

跨域部署时配置允许来源：

```bash
DASHBOARD_CORS_ORIGINS=https://logrisk.example.internal \
  bash scripts/dashboard.sh start
```

常用环境变量包括 `LOGRISK_DB_PATH`、`OLLAMA_MODEL`、`OLLAMA_HOST`、`OLLAMA_TIMEOUT`、`DASHBOARD_HOST` 和 `DASHBOARD_PORT`。Dashboard 默认仅监听 `127.0.0.1`。

## 评测与测试

Dashboard 的“评测与基准”工作区统一读取现有 Eval、AI Trace、模型 Profile、Cache、Evaluator 和 Drain3 Quality 数据，并提供：

- Fake Model、历史 Trace 回放和人工确认的真实模型三种运行模式；
- Prompt 对比、模型排行榜、失败 Case 分类和质量趋势；
- 通过率、JSON/Schema 有效率、模板引用准确率、平均/P95 延迟、缓存命中和规则跳过指标；
- 基线与候选版本门禁，结论为 `passed`、`blocked` 或 `manual_review`。
- AI Trace、模型 Profile、Prompt、Drain3 评测、Drain3 模板与 Canonical Case 的统一资产计数。

真实模型评测必须锁定 Suite、Prompt 内容、模型 Profile、Provider 连接、Case 数、超时、重试和调用预算，并在页面二次确认。停用连接或缺少 API Key 环境变量的远端连接不能启动；创建后即使配置发生变化，Run 仍使用已保存快照。Benchmark Gate 只记录决策依据，不会自动发布 Prompt、模型 Profile、Drain3 配置或批准规则。运行状态保存在 SQLite 的 `benchmark_*` 表中，Case 只包含聚合、脱敏 Evidence。

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
