# Release Notes

版本号格式为 `1.<功能版本>.<Bug版本>`：

- 功能发布提升中间位并将 Bug 位归零，例如 `1.1.3 → 1.2.0`；
- 仅修复 Bug 时提升最后一位，例如 `1.2.0 → 1.2.1`；
- 每次代码更新必须同步更新本文件。

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
