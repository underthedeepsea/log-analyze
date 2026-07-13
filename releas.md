# Release Notes

版本号格式为 `1.<功能版本>.<Bug版本>`：

- 功能发布提升中间位并将 Bug 位归零，例如 `1.1.3 → 1.2.0`；
- 仅修复 Bug 时提升最后一位，例如 `1.2.0 → 1.2.1`；
- 每次代码更新必须同步更新本文件。

## 1.16.1 - 2026-07-13

### Changed

- M10 生产加固改用双层模板标识：实例 Hash 保留集群/节点隔离，跨集群 Fingerprint 用于批准规则复用，并兼容旧版 `template_hash` 规则。
- Feature Job 改为 `state/feature_jobs/<job_id>/snapshot.json` 与 `events.jsonl` 文件持久化，不引入 SQLite；服务重启后恢复已完成任务，将运行中任务标记为 `interrupted`，且不会自动重放模型调用。
- Promptfoo 改为动态评测生产默认 Prompt，补齐严格 8 字段 Schema、模板/组件引用、业务预期、零误报、禁止表达和原始日志泄漏断言。
- M7 与 M8 统一读取 `eval_cases/canonical/`，Promptfoo 用例由确定性脚本生成。
- Drain3 参数提取默认关闭；多进程显式使用 `spawn`，默认最多 4 个 Worker 并保留 1 个 CPU 核。
- 大文件改为流式 Normalize、分区 Spool、Worker 按文件挖掘和增量聚合；增加 GZ 解压量、压缩比和单行字节限制。
- 批准规则库新增来源模型、Prompt、Lineage 状态和详情链路，支持 Rule 与 Trace 双向跳转。
- 新增 PR CI 质量门禁，覆盖 pytest、compileall、Shell、确定性 Eval 和 Promptfoo 用例漂移检查。
- 生产默认 Prompt 明确禁止将模板 Hash 复制为 `feature_type`，优先使用合法异常类别，并强化正常 INFO/驱动注册日志零误报约束。

### Fixed

- 修复旧模板 Hash 包含集群信息，导致相同异常模板无法跨集群复用的问题。
- 修复 Dashboard 重启后内存中的分析任务、候选特征和审批上下文全部丢失的问题。

## 1.16.0 - 2026-07-11

### Added

- 新增 M9 AI Harness 架构文档，集中说明处理链路、Prompt 版本、Trace、Eval、Cache 和 Rule Lineage。
- AI Harness 路线图 M1–M9 全部完成，形成从证据构造、模型调用、质量门禁、缓存、规则追溯到双层回归评测的完整本地链路。

### Changed

- README 当前版本更新为 `1.16.0`，补充本地状态文件位置和架构文档入口。
- AGENTS 增加 AI Harness 文件职责与外部观测平台延后接入约束。

### Decisions

- 暂不接入 Phoenix、MLflow 或 LangSmith；现阶段继续使用本地 JSONL Trace、Evaluator、Eval Runner 和 Promptfoo，避免增加服务部署和证据外发风险。

### Fixed

- 修复手工流水线脚本忽略项目 `.venv`、错误调用系统 Python 导致 Drain3 依赖缺失的问题。

## 1.15.0 - 2026-07-11

### Added

- 新增 M8 Promptfoo 本地 Ollama 回归评测、5 个特征识别用例及 JSON、禁止表达、模板 Hash 断言。
- Promptfoo 动态加载项目真实的 `prompts/feature_extract_v1.md`，并将测试 Evidence JSON 追加到模型请求中。

### Changed

- README 当前版本更新为 `1.15.0`，增加 Promptfoo 开发期评测命令。

## 1.14.0 - 2026-07-07

### Added

- 新增 M7 AI Eval Dataset + Eval Runner：`eval_cases/` 提供 5 个本地 AI 回归用例。
- 新增 `python -m logrisk.ai_eval.runner`，可读取 eval cases 并输出 `output/eval_results.json`。
- Eval 结果新增 `pass_rate`、`json_valid_rate`、`schema_valid_rate`、`template_reference_accuracy` 和 `forbidden_claim_count`。
- 新增 pytest 覆盖 runner 聚合指标、禁止表达、模板引用准确率和默认 eval case 清单。

### Changed

- README 当前版本更新为 `1.14.0`，增加 AI Eval Runner 使用说明。
- 大文件 Drain3 改为按“集群 + 节点 + 来源 + 组件”安全分区；不同分区由多进程并行处理，同一分区严格保留原始日志顺序和独占状态文件。
- 大文件预处理进度新增 Drain3 分区完成数，结果摘要新增并行状态、有效工作进程数和分区数。

## 1.13.0 - 2026-07-06

### Added

- 新增 Rule Lineage：人工批准特征写入规则库时记录来源 `job_id`、`candidate_id`、`trace_id`、Prompt、provider、模型和 evidence hash。
- 导出包中的已批准特征同步包含 lineage，便于外部审计规则来源。

### Changed

- README 当前版本更新为 `1.13.0`。

### Compatibility

- 旧版无 `lineage` 的 `approved_rules.json` 继续可读，规则匹配与复用逻辑不受 lineage 影响。

### Fixed

- 修复人工审批页“特征日志证据”只有第一条模板有选中态、第二条及后续模板不可选中的问题。
- 修复切换不同特征日志证据后，右侧人工审批面板仍显示旧证据信息的问题。
- 修复切换特征日志证据后，特征标题、摘要、标签和审批备注四个输入框不随当前证据更新的问题。

## 1.12.0 - 2026-07-05

### Added

- 新增 AI Cache / Dedup：同一 evidence hash、Prompt hash、provider 和模型组合重复分析时复用 `state/ai_cache.json`。
- 新增 `entity_cache_hit` Job 事件，AI 观测页和事件流展示 Cache 命中。
- Dashboard 指标新增 AI Cache 命中次数、命中日志量和合并后的节省 Ollama 调用收益。
- 新增 `AI_CACHE_ENABLED=0` 开关，便于调试 Prompt 或模型时临时关闭缓存。
- 新增模型画像与上下文预算：`configs/model_profiles.yaml` 支持按模型参数量、上下文窗口、默认 Prompt、Thinking 开关和 Evidence Budget 配置 Ollama 调用。
- 新增“模型画像”页面，展示当前 Profile、Thinking ON/OFF、Context Budget 和最终调用配置预览。
- 模型画像页新增“新增 Profile”和“保存 Profile”，支持复制现有配置并写回本地 `configs/model_profiles.yaml`。
- 内置 Profile 调整为用户指定的 `qwen3.5:4b-mlx`、`qwen3:1.7b`、`qwen3.6:35b-a3b` 和 `deepseek-v4:flash`。
- AI 分析观测和 Trace 详情新增 `model_profile_id`、Thinking 状态、Evidence 预算与裁剪元数据。
- 新增 `feature_extract_v3_compact_strict_json_en` Prompt，并作为默认日志特征识别模板，强化 `tags` 与 `selection_reason` 必填约束。
- 新建分析支持配置自动重试次数，模型单次返回缺字段、无效 JSON 或 Evaluator 拦截时可按配置重试该风险实体。
- 新增 10MB+ 大日志文件上传链路：前端自动分片上传，后端落盘到 `state/uploads/`，并通过异步 input job 生成 `output/uploads/{input_job_id}/result.json`。
- 新增大文件预处理进度展示，显示分片上传进度、后端预处理阶段和已解析记录数。

### Changed

- Cache 命中仍会重新执行 schema 校验和 Evaluator，只跳过模型调用，不跳过质量门禁。
- 当日 LLM 关联日志量只统计真实进入模型的日志量，不把 Cache 命中计入模型调用。
- Ollama 调用会合并 Profile 生成的 options，默认向 Qwen3 小模型传入 `think: false`、`temperature: 0` 和 `num_predict`。
- AI Cache signature 纳入 Thinking 开关，避免 Thinking ON/OFF 结果混用。
- `qwen3.5:4b-mlx` Profile 的 `num_predict` 从 1600 下调到 900，减少小模型冗长输出导致的结构化失败风险。
- 默认 Profile Prompt 切换到 `feature_extract_v3_compact_strict_json_en`，历史 v2 compact/strict 仍保留为可选模板。
- 小文件上传仍走原 `/api/inputs/analyze`，10MB 以上文件改走 `/api/uploads`、`/api/inputs/analyze-upload` 和 `/api/input-jobs/{id}`。
- README 当前版本更新为 `1.12.0`。

### Fixed

- 修复 `dashboard.sh status` 在服务已运行时可能误判“未运行”的问题。
- 兼容 Ollama 返回的 Markdown fenced JSON：仅当 `message.content` 是完整 ```json 包裹的 JSON 对象时剥离外壳，字段校验和 Evaluator 仍照常执行。
- 修复模型偶发遗漏 `tags` / `selection_reason` 时只能整实体失败的问题：任务会先按配置自动重试，最终仍不通过才标记失败。
- 修复 Prompt 详情“关联调用”按 `prompt_id` 误关联历史版本 Trace 的问题；现在只统计当前 Prompt hash 的调用。
- 修复前端上传文件选择器限制后缀的问题；现在可直接选择 Linux `messages`、`syslog` 等无后缀日志文件。

## 1.11.0 - 2026-07-02

### Added

- 新增 M4 Output Evaluator，模型输出通过基础字段校验后继续执行质量门禁。
- Evaluator 拦截不存在的 `template_hash`、`component`、实体引用、非法 `feature_type`、空标题/摘要和 RCA/处置建议越界表达。
- AI Trace 新增 `evaluator_result`，记录 passed、errors、warnings、score 和逐规则结果。
- AI 分析观测新增进入 Evaluator、Evaluator 通过/拦截、证据引用错误、RCA / 建议越界和质量门禁通过率。
- Trace 详情新增 “Evaluator 结果” Tab，人工审批候选特征展示质量门禁通过标识。

### Changed

- Ollama 特征识别在 Evaluator 未通过时终止该实体分析，不把被拦截结果送入人工审批。
- 观测接口的实体状态新增 model、schema、evaluator 状态和拦截原因。
- README 当前版本更新为 `1.11.0`。

## 1.10.0 - 2026-07-02

本次版本包含 `1.8.0` 之后的全部更新：`1.9.0` 与 `1.10.0`。

### Added

- 新增 `AI 分析观测` 页面：`/ai-observability`。
- 左侧工作区新增 `AI 分析观测` 导航入口。
- 新增任务级 AI 分析观测能力：
  - 当前运行任务；
  - 进入 AI 分析实体数；
  - 模型成功率；
  - 候选特征生成数；
  - Schema / Evaluator 拦截数；
  - 正常完成但无特征数量。
- 新增当前任务阶段进度：
  - 任务创建；
  - 实体筛选；
  - 规则复用；
  - Evidence 构造；
  - Prompt 加载；
  - 模型调用；
  - JSON 解析；
  - Schema 校验；
  - Evaluator；
  - 候选特征；
  - 人工审批；
  - 规则沉淀。
- 新增规则生成漏斗，用于解释风险实体到最终规则数量减少的路径。
- 新增最近 AI 事件流，用于查看 Job 分析过程中的关键事件。
- 新增实体级 AI 分析状态表，可区分规则复用、模型调用中、模型失败、解析失败、Schema 失败、Evaluator 拦截、无关键特征、已生成候选、等待审批、已批准和已驳回。
- Prompt 详情新增概览、Prompt 内容、关联调用和版本信息 Tab。
- Trace 详情新增概览、Evidence、Prompt、模型输出和校验结果 Tab。
- AI 调用追踪页新增 `job_id`、`trace_id`、`status` 和 `prompt_id` 过滤。
- 新增 AI 观测接口：
  - `GET /api/ai-harness/observability/summary`；
  - `GET /api/ai-harness/jobs/{job_id}/progress`；
  - `GET /api/ai-harness/jobs/{job_id}/events`；
  - `GET /api/ai-harness/events/recent`。
- 识别队列新增 `查看 AI 观测`。
- AI 观测页支持跳转 AI Trace、人工审批和批准规则库。
- Prompt 详情可查看关联 Trace，Trace 详情可查看对应 Prompt、模型输出和校验结果。

### Changed

- Dashboard 支持直接访问 `/prompts`、`/ai-traces` 和 `/ai-observability`。
- 运行中的 AI 观测页每 2 秒刷新，已完成任务自动停止轮询。
- README 当前版本更新为 `1.10.0`。
- `releas.md` 补齐 `1.9.0` 与 `1.10.0` 记录。

### Fixed

- 修复前端选择 Prompt 后未传入实际 Ollama 特征识别链路的问题。
- Trace 记录新增 `job_id`，识别队列可按 Job 准确查看关联 AI 调用。

### Verified

- `pytest -q`：88 passed。
- 前端 JS 语法检查通过。
- Shell 脚本语法检查通过。
- 浏览器验证 `/ai-observability` 页面可正常打开并展示布局。

## 1.9.0 - 2026-07-02

### Added

- 补齐 M1.5 AI Harness UI：Prompt 详情和 Trace 详情支持分 Tab 查看。
- AI 调用追踪页新增 `job_id`、`trace_id`、`status` 和 `prompt_id` 过滤。
- Dashboard 支持直接访问 `/prompts` 和 `/ai-traces`。

### Fixed

- 修复前端选择 Prompt 后未传入实际 Ollama 特征识别链路的问题。
- Trace 记录新增任务 ID，识别队列可按 Job 准确查看关联 AI 调用。

## 1.8.0 - 2026-07-01

### Added

- 新增 AI Harness Evidence Builder，统一构建发送给模型的脱敏证据包和稳定 evidence hash。
- 新增 Model Client 抽象、本地 Ollama Provider 和测试用 Mock Provider。

### Changed

- Ollama 特征识别改为复用 Evidence Builder 和 Model Client，保留原有提取接口、JSON Schema、低温度和 trace 行为。
- 模型 HTTP 调用从 `feature_extractor_ollama.py` 拆出，后续可继续扩展其他 provider。

## 1.6.0 - 2026-07-01

### Added

- 新增 `feature_extract_v2_compact_en` 和 `feature_extract_v2_strict_en` 两个默认 Prompt 库条目。

### Changed

- 默认特征识别 Prompt 切换为 `feature_extract_v2_compact_en`，备注为 `compact for 小参数模型`。
- `feature_extract_v2_strict_en` 备注为 `for 大参数模型`，供大参数模型场景手动选择。

## 1.5.0 - 2026-07-01

### Added

- Prompt 管理详情新增字段说明、当前版本编辑和保存功能。
- Prompt 保存时自动将旧内容写入 `state/prompt_versions.json`，形成版本历史。
- Prompt 详情新增历史版本列表，可查看历史 hash、保存时间、变更说明和旧内容。

### Fixed

- 修复 Prompt 列表中显示名、Prompt ID 与描述文本间距过小、内容连在一起的问题。

## 1.4.0 - 2026-07-01

### Added

- 新增 Milestone 1 UI：左侧工作区加入“AI 调用追踪”和“Prompt 管理”。
- 新增 `/api/ai-harness/status`、Prompt 列表/详情和 Trace 列表/详情查询接口。
- 首页新建分析区展示分析流程和 Prompt 选择；顶部展示当前 Prompt 与 Trace 状态。
- 识别队列和人工审批页增加 AI Trace 来源与查看入口。

### Changed

- Prompt Registry 支持配置元数据、默认 Prompt 查询和 Prompt 列表。
- Trace Logger 支持 JSONL 查询、筛选、今日摘要和禁用写入。
- 明确忽略内部 Milestone UI 开发说明文件，避免提交到 GitHub。

### Fixed

- 修复 Dashboard 重启脚本后台启动时依赖导出 shell 函数导致进程未稳定拉起的问题。

## 1.3.0 - 2026-07-01

### Added

- 新增轻量 AI Harness 里程碑 1：Prompt Registry、JSONL Trace Logger 和默认配置。
- 将 Ollama 特征识别 system prompt 外置到 `prompts/feature_extract_v1.md`。
- 每次 Ollama 特征识别完成后写入 `state/ai_traces.jsonl`，记录 prompt hash、证据 hash、模型输出、校验结果和耗时。

### Changed

- Trace 写入失败不影响主流程。
- 忽略内部开发路线目录，避免推送到 GitHub。
- README 明确项目 `.venv/` 的创建方式、固定路径和依赖校验命令。

## 1.2.1 - 2026-06-24

### Fixed

- 修复人工审批页隐藏候选特征列表、无法选择审批对象的问题。
- 审批页新增 Drain3 脱敏模板证据，展示模板、Hash、组件、类别、严重度、计数和时间范围。
- 进入审批页时自动选中首条特征，任务快照更新后自动修复失效选择。
- 修复启动脚本在中文 PID 提示中错误扩展变量、导致启动命令异常退出的问题。

## 1.2.0 - 2026-06-23

### Added

- 新增全局批准规则库；命中历史人工批准规则的实体自动生成已批准特征并跳过 Ollama。
- 新增 JSON、JSONL、TXT 和 LOG 统一输入，纯文本按非空行进入规范化、Drain3 和风险评分流程。
- 新增当日 LLM 关联日志量、实时/近 60 秒分析速度、ETA、规则复用量和节省调用统计。
- 新增 `scripts/dashboard.sh start|stop|restart|status` 后台进程管理。

### Changed

- 前端使用纯 React 静态文件重写为白底橙色运维台，不使用 Vite、CDN 或运行时构建。
- 新增 Drain3 实时压缩流、动态进度条、圆形规则收益和只读批准规则库视图。

## 1.1.1 - 2026-06-23

### Fixed

- 将风险实体状态与人工审批合并为同一个全宽工作区，通过“节点与识别状态 / 特征人工审批”选项卡切换，避免审批区域不可见。
- 修复工作区四个导航项无法点击的问题，改为可键盘访问的按钮并支持滚动定位和选项卡切换。

## 1.1.0 - 2026-06-23

### Added

- 新增真实风险节点汇总卡片，展示节点、集群、风险等级、关联日志量和识别状态。
- 新增原始日志量、待分析日志量、已分析日志量、Drain3 压缩减少量、模板窗口数和压缩率指标。
- 新增 Drain3 模板窗口详情，展示组件、模板、计数、时间窗口和关联实体。
- 导出包新增 `approved_risk_nodes`，仅汇总存在批准特征的真实节点。

### Fixed

- 修复“风险实体与识别状态”内容显示不全的问题，增加常驻滚动条和可拖动高度调节。

## 1.0.0 - 2026-06-22

### Added

- 初始日志规范化、Drain3 模板化、窗口聚合和风险评分流程。
- 初始本地 Ollama 关键特征识别、SSE 实时进度、逐特征人工审批和 JSON 导出 Dashboard。
