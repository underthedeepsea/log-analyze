# Release Notes

版本号格式为 `1.<功能版本>.<Bug版本>`：

- 功能发布提升中间位并将 Bug 位归零，例如 `1.1.3 → 1.2.0`；
- 仅修复 Bug 时提升最后一位，例如 `1.2.0 → 1.2.1`；
- 每次代码更新必须同步更新本文件。

## 1.36.3 - 2026-09-03

### Fixed

- 新增基于选定模板证据的确定性语义解析器，区分具体根因与通用 wrapper，保守处理混合语义并保持未知问题严格回退。
- 修复审批身份推导读取实体 `top_templates` 造成的语义污染；审批身份由 `semantic_safe` 驱动，同时保留历史 V1 物理 identity 比较兼容。
- 统一审批队列、规则复用与 Candidate 持久化的 `semantic_safe` 门禁，保留具体 OOM 语义并为审核组保存脱敏解析诊断。

## 1.36.2 - 2026-09-03

### Added

- 新增 SQLite/PostgreSQL `0021` forward-only classification migration，以及 `LEGACY_V1`、`VALID_V2`、`MALFORMED_V2` 三态规则分类。
- 完善持久化人工审批工作台：支持全量队列游标、脏草稿保护、真实执行上下文展示和明确的并发冲突提示。

### Fixed

- 修复 Approval Identity V2 对未知或歧义根因的严格回退，并保留历史 V1 `approval_key` 的比较与物理分组兼容。
- 修正多重具体语义关键字、递归根因字段和未知命名空间的收集与校验；严格回退保留来源锚点，并避免将 PodSandbox OOM 误判为 CNI 插件故障。
- 收紧 CNI 关键字上下文，避免通用 network/网络文本或普通 PodSandbox 失败被误判为 CNI IP 耗尽，同时保留 sandbox 网络包装器识别。
- 修复 Worker 旧 Candidate 快照覆盖人工审核状态，并为审核更新增加 SQLite/PostgreSQL 兼容的原子 CAS、完整队列读取和明确的 404/409 错误。
- 修复审核操作的二阶段 CAS 覆盖和部分批准，补强文件存储跨进程 CAS，并阻止同进度旧快照覆盖实时审核字段。
- 修复代表 Candidate CAS 失败后仍写入规则、同身份 Candidate 或 Approval Group，并在规则写入失败时回滚审批状态。
- 修复过期的幂等批准绕过 Candidate 版本校验，并避免重复编辑已复用规则的 Candidate 时重复累计复用次数。
- 修复批准规则 V1/V2 匹配与停用规则替换：仅复用 active 规则，保留 V1 物理身份；停用前身生成可追溯的独立 active replacement，并将规则复用统计限定为模型调用前命中。
- 收紧 V2 语义匹配的 `match_mode` 门禁，并让 template-set 的 feature_type、组件、模板锚点证据缺失或不兼容时拒绝实体复用；canonical template-set 规则使用确定性的严格存储键，避免覆盖语义规则。
- 修复损坏的 `approved_rule_v2` 回退到 V1 匹配路径的问题，以及数据库 reader、legacy import、治理状态变更和 rollback 对规则 identity 的静默覆盖或无条件升级。
- 修复审批批准后的跨任务 pending Candidate 持久化收敛、重启恢复和重复审批幂等，并保证 group reject 不修改已结束 Candidate。
- 修复持久化审批队列在全量 Candidate 聚合后的 logical Group 分页、游标校验、全局统计和代表性证据脱敏。
- 统一 standalone Dashboard 与 Django facade 的审批校验错误映射，补齐 422 validation 响应，并验证无当前 Job 时的 review/queue/export 路由。
- 补强 SQLite/PostgreSQL 共享存储的并发复用计数、终态审批组保护和无事件刷新恢复，避免跨进程并发与 Worker/Reviewer 时序造成状态回退。
- 持久化 Candidate、Job 快照和文件事件写入现在递归剔除原始日志、样例、消息及凭据字段，保持审批、模型和审计边界不泄漏敏感内容。

## 1.36.1 - 2026-09-01

### Fixed

- 修复同一 Drain3 根因因 feature_type、组件 wrapper 或模板集合差异被拆成多个待审批组的问题。
- CNI IP 耗尽现在优先于 generic CNI/PodSandbox wrapper 语义，并兼容历史 V1 审批规则。
- 修复人工审批页面依赖当前浏览器 Job Snapshot，刷新或重新进入后看不到数据库 pending Candidate 的问题。
- 人工审批页改为从持久化全局审批队列加载，并按 canonical problem_code 聚合。
- 修复 `/review` 路由刷新后丢失，以及审批页显示默认模型而不是实际 Candidate 执行模型的问题。
- 批准或驳回一个语义审批组后，同组 pending Candidate 自动收敛。

## 1.36.0 - 2026-09-01

### Added

- 新增可选的 `kafka-python` Kafka 消费适配器和 Dashboard 启动接口；启用后按分区高水位流式消费，每批最多 10,000 条。
- 新增通用增量流水线：先提交脱敏数据库 Checkpoint，再提交 Kafka offset；任务可从已提交分区 offset 恢复，且不持久化 Broker 地址、凭据或原始日志。

### Changed

- 将 Kafka 来源从预留契约接入本地 Dashboard 和流式任务状态，默认仍关闭，避免未显式配置时建立 Broker 连接。

### Fixed

- 修复跨时间窗的同一风险实体重复写入特征任务表导致 SQLite `UNIQUE constraint` 和 HTTP 500；创建特征任务前会合并实体窗口、模板计数和时间范围。

## 1.35.2 - 2026-09-01

### Changed

- 完成 Dashboard 识别工作台的视觉与导航文案更新，保持 React 构建产物和 Django 静态包一致，并统一版本标识为 `1.35.2`。

### Fixed

- 修复 Drain3 风险候选按节点和时间窗重复进入审批的问题：新增稳定 `problem_code`、`approval_key` 与 Approval Group。
- 审批后自动收敛同一问题的 pending Candidate，并在模型调用前复用已批准规则，保留候选发生记录和规则复用审计。

## 1.35.1 - 2026-08-23

### Fixed

- 新增 `0019` SQLite/PostgreSQL 迁移，为 Candidate 反馈建立 `(candidate_id, job_id)` 复合唯一引用，阻止并发跨 Job 重挂载产生不一致反馈；SQLite 重建失败会完整回滚并可修复后重试，同时保留已有反馈历史与幂等重试。

## 1.35.0 - 2026-08-23

### Added

- 新增 M23 Central Continuous Learning 持久化基础：Candidate 人工反馈追加记录与幂等键、Drain Dataset family/revision/hash/lifecycle 元数据，以及标注和复核的 Dataset 摘要引用。
- 新增 SQLite/PostgreSQL 对等 `0018` migration、跨 Provider SQL Repository、旧 Dataset 元数据回填和内容摘要锁定；不引入模型 Worker、自动批准或生产资产发布。

### Security

- Candidate 反馈保持追加式历史，禁止通过幂等重试覆盖原决策；Dataset 记录递归拒绝原始日志、样例、凭据、Token、DSN、鉴权头和 Cookie。

## 1.34.0 - 2026-08-13

### Added

- 新增固定角色 Multi-Agent DAG：证据专家与规则专家可按显式依赖并行执行，特征专家在依赖完成后生成经过确定性 Evaluator 的待审批 Candidate。
- 新增 DAG Compiler、Scheduler、Worker、Retry、全局 ToolCall 预算、超时、取消、幂等、Checkpoint 恢复和只读 Replay；禁止环、未知角色、客户端工具注入、动态 Agent 与递归工作流。
- 新增 SQLite/PostgreSQL 对等 `0017` migration 与 4 张工作流表，持久化工作流定义、Run、节点 Checkpoint 和追加式事件。
- 新增 Dashboard、Django PACAS/RBAC 与 Airflow `logrisk_agent_workflow` 接口；生产调度仅传稳定的工作流 Run/请求 ID，分派失败会持久化为可审计失败状态。
- 新增“AI 工程 → 工作流编排”页面，可组合固定角色、编辑依赖和预算，并查看 DAG 状态、节点日志、事件、Artifact、人工 Gate、暂停、取消与节点重试。
- 工作流 Run 现在会锁定提交时的模型 Profile、连接和 Prompt 哈希，节点重试、恢复与子 Agent 均复用同一运行快照，避免运行期间配置变更造成漂移。
- 创建工作流 Run 时强制校验快照一致性、连接可用性和凭据配置；恢复中断节点会清理旧子 Run 引用，避免使用失效配置或重复挂接执行记录。

### Security

- 工作流默认关闭，只接受聚合脱敏 Evidence；递归拒绝原始日志、样本、凭据、Token、DSN、鉴权头和 Cookie。
- Agent 工作流不能批准、导出、发布规则、执行 RCA/修复或绕过人工审批；不提供任意网络、Shell、文件或数据库工具，也不自动切换模型 Provider。

## 1.33.0 - 2026-08-12

### Added

- 新增可选的受控 Agent 日志智能运行：模型只生成严格结构化、顺序执行的白名单工具计划，并受步骤数、工具成本和超时预算限制。
- 新增 SQLite/PostgreSQL 对等 `0016` migration 与 5 张 Agent 领域表，持久化 Run、步骤、工具调用、脱敏产物和追加式事件，支持恢复、暂停、继续、取消、幂等重试与只读 Replay。
- 新增脱敏 Evidence、批准规则、知识资产、确定性 Evaluator 和待审批 Candidate 五类工具；Candidate 只有通过 Evaluator 后才能登记，且不会自动批准、导出或进入规则库。
- 新增 Dashboard Agent API、Django 4.2 PACAS/RBAC 控制面和 Airflow 2.3.2 `logrisk_agent_run` DAG；生产调度只传递 Run/请求 ID，执行发生在 Celery Worker。
- 新增“AI 工程 → Agent 运行”页面，展示运行状态、预算、计划步骤、ToolCall、Artifact、失败摘要和审计事件，并提供暂停、继续、取消、重试、只读回放与人工审批跳转。
- Agent 服务重启后可恢复排队、规划中与运行中任务；中断步骤会回到待执行状态，同一锁定步骤最多自动重试一次且不会重新规划或切换 Provider。

### Security

- Agent 功能默认关闭；Planner 不接收原始日志，工具注册表递归拒绝 `samples`、`raw_sample`、原始日志、API Key、Token、密码、DSN、鉴权头和 Cookie。
- Agent 无批准、导出、规则发布、RCA 或自动修复能力；Django 写操作继续依赖外部 PACAS/RBAC，Airflow DAG conf/XCom 不携带 Evidence、Prompt 或模型内容。

## 1.32.0 - 2026-08-10

### Added

- 新增知识包中心与 `.logrisk-package.zip` 离线包格式，支持 Drain3 配置、语义词典、特征 Prompt、风险语义、批准规则候选和 Gold Dataset 的 Manifest、SHA256、版本范围与精确依赖校验。
- 新增跨 SQLite/PostgreSQL 的知识包注册表迁移 `0015`，记录包版本、资产状态、导入阶段、受控 Artifact 路径和脱敏审计事件；包内容不写入数据库。
- 新增知识包上传、只读预览、确认安装、资产逐项登记候选、版本退休和审计查询 API，并加入 Dashboard “数据治理 → 知识包中心”工作区与内置示例包下载。
- 新增 `python -m logrisk.knowledge_packages.archive build|validate` 离线命令；Prompt、Drain3、语义、风险语义和 Gold Dataset 资产通过现有候选服务登记，规则候选保留为待人工审查引用。

### Security

- 知识包拒绝 Zip Slip、绝对路径、符号链接、脚本/插件、远程地址、未知扩展名和超限压缩包；Gold Dataset 禁止 `samples`、原始日志、Token、密码、DSN 等敏感字段。
- 安装后的资产默认禁用；登记候选不等于生产发布，不会绕过人工审批或改写当前 Drain3、Prompt、语义词典和规则配置。数据库、Trace、错误与审计只保存摘要和操作元数据。

## 1.31.1 - 2026-08-04

### Fixed

- 修复 PostgreSQL 内置风险语义种子向 `BOOLEAN` 字段写入整数导致应用容器启动失败的问题。
- 修复 `PostgresCursor` 无法被现有 Store 迭代，导致 PostgreSQL 模式恢复任务时启动失败的问题。
- 修复 Airflow 2.3.2 DAG 缺少 `start_date`、动态映射错误传递队列参数，以及默认队列未被 Celery Worker 消费的问题。
- 修复 Drain3/特征批次为空时动态映射跳过下游任务、任务长期停留在排队状态的问题；空任务现在可正常完成并收敛编排状态。

## 1.31.0 - 2026-08-03

### Added

- 新增框架无关的 LOGRISK Application Container；Django、Airflow 与本地 Dashboard 后续可复用同一组数据库、模型、规则、任务、运行时、评测和发布就绪服务，避免复制业务装配逻辑。
- 本地 Dashboard 已改为仅承载 HTTP 路由与静态页面，服务装配迁移至 `logrisk.application`；Airflow Worker 不会触发本地服务启动时的流式任务中断或历史文件导入。
- 新增跨 SQLite/PostgreSQL 的 Airflow 编排运行状态表与乐观并发仓储，持久化调度、心跳、取消、完成和可恢复分派状态；外部编排错误仅保存脱敏摘要，不保存凭据或任务内容。
- 新增受控共享 Artifact 存储：上传完成后以 SHA256 校验、同目录临时文件和原子替换写入 `LOGRISK_SHARED_ROOT`；数据库与任务快照仅保存共享根目录内的相对路径。
- 新增仅依赖标准库的 Airflow 2.3 REST v1 适配器，可安全触发、查询和取消 DAG Run；DAG conf 只传递任务、编排运行和请求 ID，错误与凭据均不会回显或持久化。
- 新增可安装的 Django 4.2.16 App 基础层，提供严格生产配置、可替换 PACAS/RBAC Django User 身份适配和共享服务工厂；不创建 Django Model、迁移或本地账号体系。
- 新增框架无关的核心只读 API Facade；本地 Dashboard 与 Django 共享健康、运行就绪、模型画像、Prompt、规则治理和发布就绪响应逻辑，避免并行维护两套业务查询实现。
- Django 新增受 PACAS/RBAC 身份与角色约束的特征任务提交接口：任务和编排记录先持久化，再触发 Airflow；触发失败会保留可审计的 `dispatch_failed` 状态且不执行本地回退。
- 新增 Airflow 2.3.2 可部署 DAG 与延迟装配的任务入口；DAG 解析不连接数据库、模型或共享目录，Task/XCom 仅传递稳定 ID、状态和计数。
- Django 新增受 PACAS/RBAC 保护的特征审批、受控导出与发布就绪校验接口；这些操作与本地 Dashboard 复用同一 API Facade，并写入仅含操作元数据的运行审计记录。
- 新增 Django 可分发静态包：React 纯静态产物可通过 `sync_django_static.sh` 确定性同步至 `logrisk_django/static/logrisk/`，支持 `collectstatic` 与 API 优先的 SPA 回退。
- Airflow DAG 补齐预处理、Drain3 分区、模板合并、规则复用、模型批次与校验阶段，并按 `logrisk_cpu_pool`/`logrisk_llm_pool` 和 Celery 队列进行基础动态映射；模型实体失败会准确收敛为失败编排状态。
- Django 生产适配层改为不在启动或 Airflow Worker 初始化时自动迁移数据库；新增 `logrisk_check`、`logrisk_migrate` 和 `logrisk_reconcile_dispatch` 管理命令，用于显式检查/迁移与恢复待分派任务。
- 新增 Django/Airflow 生产部署指南、无密配置示例和数据库调用关系说明，覆盖独立数据库、共享目录、Celery 队列、显式迁移、安全边界与回滚流程。
- 新增上传预处理的独立 `logrisk_input_preprocess` DAG、跨 SQLite/PostgreSQL 输入编排状态与 Django 分片上传接口；上传日志仅以受控共享目录路径保存，DAG conf/XCom 只传输入任务、编排运行和请求 ID。Django/Worker 不再把跨进程已排队的特征任务误标为中断，并会从运行时数据库刷新 Airflow 写回的任务状态和事件。
- Django 生产适配补齐模型连接、模型画像、Prompt 版本、规则治理、Retention 与数据库候选配置的受控写接口；所有写操作统一经过 PACAS/RBAC 身份、乐观版本校验和脱敏运行审计。
- 新增规则治理的详情、复审队列、状态变更、反馈、回滚以及 Airflow 编排运行的取消/重试接口；取消和重试只推进持久化状态，不在 Django 进程内启动模型任务。
- 新增风险语义规则的创建、覆盖、编辑、校验、发布、停用、恢复默认、回滚与导入接口，内置规则保持只读，审计事件不保存操作备注或规则正文。
- 新增 Drain3 数据集、标注、评测、候选配置和调参任务的 Django 受控接口，并新增 Benchmark Suite、Run、取消、对比和门禁评估接口，复用现有 SQLite/PostgreSQL Repository。
- 新增 Airflow 运行状态同步接口与输入预处理编排详情接口；Django 只校验稳定的 DAG Run 标识和生命周期状态，使用乐观锁更新本地记录，并将 Airflow 健康状态单独暴露给运行中心。
- 新增 `logrisk_reconcile_runs` 管理命令，支持 `--dry-run` 查询活动 DAG Run 并在停滞或 Worker 异常后安全收敛本地状态；不复制 DAG conf、XCom 或日志内容。

## 1.30.0 - 2026-07-31

### Added

- 新增发布就绪中心：可在 Dashboard 的“系统 → 发布就绪”执行确定性、只读发布前检查，覆盖运行时/数据库迁移、前端静态包、基础配置、默认模型 Profile 与连接、默认 Prompt、Drain3、语义词典、多来源规则和 Benchmark 门禁。
- 新增 SQLite/PostgreSQL 对等 `0012` migration，以及 `release_validations`、`release_validation_checks` 两张表；结果以目标版本和幂等键保存，支持查看最近记录、历史和脱敏诊断。
- 新增 `/api/release-readiness`、`/history`、`/diagnostic` 和受外部身份保护的 `/validate` API；校验不会调用模型、迁移数据库、切换配置或自动发布。
- 新增发布就绪 React 页面，显示总体状态、通过/复核/阻断计数、检查明细、版本输入、校验历史和复制诊断信息入口。

### Security

- 发布校验仅保存脱敏状态和有限元数据；不会记录原始日志、样例、Prompt 正文、模型内容、API Key、Token、密码或 DSN。
- `blocked` 状态阻止“可发布”结论；`warning` 仅提示人工复核，系统不自动绕过或修复任何检查。

## 1.29.0 - 2026-07-30

### Added

- 新增 M17 多来源智能关联：对 kernel、kubelet、containerd、podlog、audit 和 journal 的脱敏模板观察按明确实体、显式层级与时间窗口生成跨来源证据链。
- 新增节点、命名空间、Pod、容器和设备实体路由，支持人工配置的确定性别名；跨集群与缺少可靠实体标识的数据不会被强制关联。
- 新增 SQLite/PostgreSQL 对等 `0011` migration、5 张领域表、追加式规则审计以及摘要、实体、时间线、关联详情和规则 API。
- Dashboard 新增“多来源关联”工作区，展示来源覆盖、实体目录、跨来源时间线和可控关联规则。
- 多来源关联新增可视化规则编辑：可维护规则名称、启停状态、来源组合、时间窗口、最低风险分、最低出现次数和置信度；保存采用版本校验并写入脱敏审计。
- Dashboard 顶栏新增“帮助”入口，后端提供 `/help` 离线管理员手册；手册采用 LOGRISK 应用图标，并补充多来源关联规则的边界、字段说明和操作流程。
- Dashboard 导航归纳为“分析工作台、AI 工程、规则与风险、数据治理、系统”五个可折叠分组；保留所有原有入口、路由和玻璃选中效果，切换页面时自动聚焦当前分组。
- 调整分组导航文字尺寸并移除项目数量标记，提升可读性并减少无关视觉信息。
- 帮助手册新增离线章节搜索，并补齐折叠导航、帮助入口、AI 分析观测与安全 Replay 的使用说明。
- 修复帮助搜索后目录仍显示全部章节的问题；匹配外的目录项现在会明确隐藏。

### Changed

- Drain3 仍只学习 `message_core`；实体路由位于归一化之后，实体键与层级关系随模板事件和聚合窗口透传，不改变模板树、参数或脱敏规则。
- 大文件、Checkpoint 恢复和直接分析完成风险评分后，自动写入脱敏多来源观察并执行幂等关联。

### Security

- 多来源关联不使用 LLM、模糊字符串匹配或 RCA 推断；持久化与 API 均排除原始日志、样例和 `message_core`。

## 1.28.0 - 2026-07-29

### Added

- 新增 Production Runtime 领域：统一的任务目录、存储配额、Retention 预览/执行、运行状态、健康检查、就绪检查和脱敏审计。
- 新增 PACAS/RBAC 外部身份边界：仅信任配置的代理 CIDR 与身份 Header；生产写请求可按角色失败关闭，本机开发继续支持显式 loopback bypass。
- 新增 `runtime_policies`、`runtime_maintenance_runs`、`runtime_quota_snapshots` 和 `runtime_audit_events`，并提供 SQLite/PostgreSQL 对等 `0010` migration 与数据字典记录。
- 新增 Dashboard“运行中心”，展示就绪状态、跨域任务、任务排序、存储用量、Retention 策略、人工确认清理、审计记录与可复制诊断信息。
- 新增离线 HTML《LOGRISK 完整管理员使用手册》及 8 张脱敏界面截图，覆盖日志分析、审批导出、AI 工程、规则与语义、Drain3 评测、流式处理和生产运行操作。

### Changed

- 上传与分析在接受前重新计算存储用量；超过硬限制时返回稳定的 `507 runtime_quota_exceeded`，避免继续写满本机或生产磁盘。
- Retention 只处理受控根目录中的已完成/失败任务产物，跳过运行任务、原始来源和导出物；执行后同步清理对应 Artifact 元数据。
- 所有 Dashboard 写请求追加脱敏通用审计事件；运行策略使用乐观版本校验，冲突返回 `409 runtime_version_conflict`。

### Security

- 不新增本地用户、Bearer Token、密码、会话或第二套 RBAC；不会持久化认证 Header、Cookie、Token、API Key、DSN、模型内容或原始日志。

## 1.27.0 - 2026-07-28

### Added

- 新增 AI Observability 2.0：以 Observation 汇总任务，以 Span 展示输入、Drain3、规则复用、Evidence、Prompt、模型、解析、Schema、Evaluator、候选和审批链路。
- 新增按阶段与状态筛选的时间线、Span 详情、阶段成功率、P50/P95 延迟、模型 Token 用量和可选 Profile 单价成本指标；旧版 AI Trace 保持兼容。
- 新增安全 Replay：历史结果重放不会调用模型；原模型重放必须人工确认并锁定来源 Prompt、Profile、Provider、Evidence 与模型参数。
- 新增 SQLite/PostgreSQL 双模式 Observation、Span、Replay 和 Replay 事件表，并在数据字典中记录用途与 PostgreSQL 映射。

### Security

- Observation 和 Replay 仅保存聚合、脱敏快照，不保存原始日志、模型凭据或数据库密钥。
- Replay 结果与正式候选、规则及审批链路隔离，不会自动写入业务资产，也不会切换或降级模型 Provider。

## 1.26.0 - 2026-07-27

### Added

- 新增文件增量来源契约、文件身份校验、已提交字节 Offset Checkpoint 和有界批次处理；中断任务可从最后成功提交点继续，已提交批次不会重复写入未知模板队列。
- 新增跨 SQLite/PostgreSQL 的流式任务、批次提交、未知模板队列和审计事件数据模型，以及“流式处理”工作区和任务恢复 API。
- 新增 Kafka 消费适配器预留契约：支持显式注册内部 Adapter、Topic、Consumer Group、Partition Offset、Bootstrap 环境变量名和 adapter ID，但默认禁用且不含 Kafka 客户端或 Broker 连接。

### Changed

- 大文件上传任务会创建独立流式状态，持续上报 Checkpoint、批次提交数和处理吞吐；服务重启时遗留运行任务改为中断，需人工恢复。

### Security

- 流式状态、未知模板和审计记录仅保存 Drain3 脱敏模板与聚合统计；拒绝 `raw_sample`、`samples`、`message`、`content` 等原始日志字段。
- 文件身份或 Drain3 配置摘要不一致时任务进入冲突状态；Kafka 凭据、Token、密码和原始消费记录不持久化，也不由 Dashboard 读取。

## 1.25.1 - 2026-07-24

### Added

- 新增受控 `extension` 模型 Provider、显式适配器 allow-list 与可提交的 Token 鉴权模板；内部团队可在独立文件中实现 Token 刷新、签名、私有请求和响应解析。
- 模型画像新增扩展适配器连接配置，支持适配器 ID、逻辑凭据到环境变量名映射、非敏感配置、能力展示和连接测试。
- 新增 [本地扩展模型 Provider 开发指南](LOCAL_PROVIDER_DEVELOPMENT_GUIDE.md)，约束弱模型二次开发的允许范围、禁止范围和验收步骤。

### Security

- 扩展连接仅持久化环境变量名，不保存 Token、密钥、Cookie、签名原料或鉴权响应；扩展调用错误与原始输出会按已配置凭据值脱敏。
- 适配器默认拒绝未经内部补全的真实调用；未注册适配器、缺失凭据映射和敏感扩展配置均会明确失败，不自动回退到其他 Provider。

## 1.25.0 - 2026-07-23

### Added

- 新增 LOGRISK 橙色应用图标资源，并作为浏览器标签页、Edge 快捷方式和移动端图标使用。
- 新增 SQLite/PostgreSQL 双模式运行数据库；SQLite 继续为默认本地 Provider，PostgreSQL 通过显式环境变量或命令行配置启用，且不会自动回退。
- 新增 PostgreSQL 版本化迁移、可选 `psycopg[binary]` 依赖、连接候选配置页面和停机 SQLite→PostgreSQL 元数据迁移工具（`--dry-run`、`--execute`、`--verify`）。

### Changed

- README 顶部展示新的 LOGRISK 应用图标，确保 GitHub 项目首页使用统一品牌标识。
- 首页顶栏改用新的 LOGRISK 应用图标，并移除 `FEATURE REVIEW` 副标题。
- GitHub 发布流程补充标准 Markdown 正文、可读 Release 标题与累积版本说明校验规则。
- 数据访问层统一支持 SQLite qmark 与 PostgreSQL 参数、UTC 时间、JSONB、布尔值、稳定排序和跨库 `ON CONFLICT`；迁移只保留文件路径元数据，不复制原始日志或产物本体。
- PostgreSQL 集成测试改用符合 libpq URI 规范的参数编码，并已在本机隔离 PostgreSQL 容器完成真实 SQLite→PostgreSQL 迁移校验。

## 1.24.2 - 2026-07-22

### Fixed

- 修复侧栏选中项顶部硬白色内阴影造成的断层，改为连续的轻量玻璃渐变过渡。
- 选中菜单文字恢复常规字重，避免出现不必要的粗体效果。
- 降低选中项底色、阴影与内反射强度，保留单色动态高光和透镜动画。

## 1.24.1 - 2026-07-22

### Fixed

- 修复部分屏幕分辨率下左侧功能菜单展示不完整的问题；菜单区域改为独立纵向滚动，并仅在内容溢出时显示橙色滚动条。
- 新增桌面侧栏折叠与窄屏抽屉菜单，窄屏不再横向渲染完整导航；支持遮罩关闭、切换页面自动关闭和无障碍状态说明。
- 移除菜单字符图标与底部系统边界卡片，未选中项保持纯白背景。
- 选中项改为悬浮玻璃质感，使用单色动态高光、平滑透镜放大和柔和阴影；系统启用减少动态效果时自动停用动画。

## 1.24.0 - 2026-07-22

### Added

- 新增统一“评测与基准”工作区，整合现有 Eval、Promptfoo、AI Trace、模型 Profile、Cache、Evaluator 和 Drain3 Quality 指标。
- 新增 Benchmark Suite、Run、Case Result、Gate、Artifact 和追加式审计 SQLite 数据模型；运行状态、进度、失败原因与执行快照可在服务重启后恢复查询。
- 新增 Fake Model、历史 Trace 回放和人工确认的真实模型运行模式；真实运行锁定 Prompt、模型 Profile、Case 数、超时、重试与预算，不自动切换 Provider。
- 新增统一指标归一化、Prompt 对比、模型排行榜、失败 Case 分类、质量趋势与基线/候选 Regression Gate。
- 新增 `/api/benchmark-center/` 领域 API，以及包含加载、空、失败、诊断复制和真实模型二次确认状态的 React 页面。
- 总览新增 AI Trace、模型 Profile、Prompt、Drain3 评测、Drain3 模板和 Canonical Case 的统一资产计数。
- 真实模型 Run 会固化 Prompt 内容、模型 Profile、Provider 连接、超时、重试和调用预算；执行期间配置变更不影响已创建 Run。

### Security

- Benchmark 只读取聚合、脱敏 Evidence 和评测元数据，不保存或展示原始日志；门禁结果不会自动修改 Prompt、模型 Profile、Drain3 配置或批准规则。
- Suite 写入会递归拒绝原始日志和 sample 字段；停用连接或未配置密钥的远端连接不能启动真实模型评测。

## 1.23.1 - 2026-07-21

### Fixed

- 修复人工审批选择 Drain3 证据模板后，模型生成的特征标题和标签被前端通用组件、级别字段覆盖的问题。
- 特征摘要改为组合模型摘要与当前选中的 Drain3 脱敏模板；切换模板时保留模型标题和标签，并更新对应模板证据。
- 审批证据区明确展示一个候选特征与多个 Drain3 证据模板的归并关系，并分别展示 Xid、风险类型、严重级别和模板 Hash，避免把多个证据模板误解为重复特征。

## 1.23.0 - 2026-07-19

### Added

- 新增可编辑风险语义库，内置 NVIDIA Xid/SXid、Kubernetes、Linux 内存/存储/网络/进程和容器运行时语义；Xid 35 与 Xid 79 等共享结构模板的日志可保留不同风险含义。
- 新增内置规则覆盖、用户规则、正负样例测试、版本历史、人工发布、停用、恢复默认、回滚、导入和导出能力；所有治理写操作进入追加式审计记录。
- 新增节点风险事件台账、幂等去重、日聚合和当前快照，明确区分风险事件数、日志命中次数与不同风险类型数。
- 新增确定性节点综合评分、配置化 Hard Override、评分解释、人工确认和恢复操作；AI 不参与生产风险等级判定。
- 新增 `/api/semantics` 与 `/api/node-risks` API，以及“风险语义库”和“服务器风险”React 页面。

### Changed

- 小文件和大文件流水线在 Drain3 模板化后、风险实体聚合前执行风险语义匹配，并将节点风险写入现有统一 SQLite 数据库。
- AI Evidence 可携带有界的风险类型、语义版本和脱敏字段，但排除动作元数据、原始日志和自动处置信息。
- SQLite 数据字典升级到 v5，新增风险语义版本、校验、审计、节点风险事件、日聚合、快照和幂等写入表。

### Security

- 节点风险台账只保存确定性语义、脱敏字段和 Evidence 引用，不保存原始日志；建议动作仅作为审计元数据，不执行自动修复或 RCA。

## 1.22.0 - 2026-07-18

### Added

- 新增批准规则生命周期治理，支持 `active`、`disabled`、`under_review`、`deprecated` 和 `archived` 五种状态；只有启用规则参与特征匹配。
- 新增不可变规则版本、误报/有效反馈、追加式审计事件和乐观版本校验；状态变更与回滚均返回 Request ID、规则 ID 和最新版本。
- 新增规则健康度计算，展示 7/30 天命中、最后命中、跨集群命中、30 天误报率、复审时间、健康分和复审原因。
- 新增 `/api/rule-governance/` 领域 API，覆盖分页列表、详情、复审队列、状态操作、反馈和确认回滚；保留 `/api/rules` 兼容接口。
- 规则治理页面新增资产指标、状态筛选、健康度列表、复审工作台、版本树、回滚确认、审计记录、加载/空/失败状态和诊断信息复制。

### Changed

- SQLite 新增规则治理迁移；既有规则自动迁移为 `active`、版本 `v1`，旧文件导入继续保持幂等。
- 规则复用事件新增任务、实体和集群信息，用于计算跨集群健康指标。
- 数据字典补充规则资产、版本、反馈和审计表及 PostgreSQL 映射说明。

### Security

- 规则治理仅保存脱敏规则、Lineage 和操作元数据，不新增原始日志持久化或模型输入。
- 回滚以追加新版本实现，不覆盖历史记录；并发写入通过 `expected_version` 冲突校验阻止。

## 1.21.0 - 2026-07-16

### Added

- 新增内置 `qwen3.5:9b-mlx` 模型 Profile，使用本机 Ollama 连接、262144 tokens 上下文、12000 tokens 推荐输入预算和 2000 tokens 输出预算。
- 新 Profile 默认使用 `feature_extract_v3_compact_strict_json_en`、JSON Schema 结构化输出并关闭 Thinking，现有默认 Profile 保持为 `qwen3_1_7b_fast`。
- 新增幂等 SQLite 迁移，为已有数据库补充 9B Profile，保留用户当前默认 Profile 和已有同名配置。

### Fixed

- 修复 `feature_extract_v1`、`feature_extract_v2_compact_en` 和 `feature_extract_v2_strict_en` 仍使用六字段示例，导致模型稳定遗漏 `tags` 与 `selection_reason` 的严重输出契约冲突。
- 统一全部内置特征提取 Prompt 为八字段契约，明确 `feature_type` 使用 `lowercase_snake_case`，并移除 v1 整体 Markdown 代码围栏。
- 启动时自动把 SQLite 中仍使用旧契约的当前 Prompt 追加升级到修复后的种子版本，保留全部历史版本；后续保存缺少必填字段的特征 Prompt 会被拒绝。

## 1.20.1 - 2026-07-16

### Fixed

- 修复模型画像页点击“保存 Profile”没有可见反馈的问题；按钮现在显示保存中、成功或错误状态。
- 修复 `recommended_input_tokens` 和 `max_output_tokens` 错误写入 Evidence Budget、顶层配置未更新的问题。
- 修复页面回传的派生 `options.num_predict` 覆盖新输出预算的问题；派生的 Thinking、输出长度和结构化模式不再作为可编辑 options 持久化。
- 将 `qwen3.5:4b-mlx` 默认输出预算从 900 提升至 1600 tokens，降低多特征 JSON 因截断而缺少 `tags` 或 `selection_reason` 的概率。
- 新增 SQLite 迁移，自动清理历史 Profile 中的派生 options，并仅将仍使用 900 默认值的内置 4B Profile 升级为 1600。

### Documentation

- 重构 README 为面向使用和部署的产品说明，删除内部里程碑与 Phase 表述，并按快速开始、日志处理、模型配置、治理、存储、安全和测试重新组织内容。

## 1.20.0 - 2026-07-16

### Added

- 新增 Ollama 与 OpenAI-compatible API 连接管理，支持新增、编辑、启停和连接测试；模型 Profile 通过 `connection_id` 绑定连接。
- 新增 `/v1/chat/completions` Provider，支持 `json_schema`、`json_object` 和 `prompt_only` 三种结构化输出模式，并兼容原始 JSON 与 Markdown fenced JSON。
- 新增 SQLite 运行时数据层、版本化 SQL 迁移和 `database/schema.yaml` 数据字典；任务、Prompt、规则、Trace、缓存、指标、上传元数据、Drain3 治理和语义词典统一持久化。
- 新增旧 JSON、JSONL、YAML 和 Prompt 历史的 SHA256 幂等导入；原始上传、分片、Drain3 状态和导出物继续保留在文件系统。
- 模型画像页面新增 API 连接管理、Provider 状态、连接选择和结构化输出模式配置。

### Changed

- 仓库中的 Prompt、模型 Profile、风险规则、Drain3 基线和内置语义词典改为首次启动种子；初始化后以 SQLite 为运行时权威来源。
- 新建任务锁定连接、模型 Profile、模型和 Prompt 快照；自动重试始终使用同一 Provider，不进行隐式降级。

### Security

- API Key 仅按环境变量名引用，不保存真实密钥到 SQLite、Trace、日志、错误消息或前端响应。
- Ollama 与远端 Provider 均只接收聚合、脱敏 Evidence，继续禁止发送原始日志。

## 1.19.2 - 2026-07-16

### Fixed

- 修复 Ollama Thinking 开关错误放入 `options` 导致 Qwen3.5 忽略 `think=false`、思考耗尽 `num_predict` 且结构化内容为空的问题；Thinking 现在按 `/api/chat` 顶层字段发送。
- 当模型因 Thinking 耗尽输出预算且未返回内容时，记录明确的 `parse_failed` 原因，便于在 AI Trace 中区分普通 JSON 解析失败。

## 1.19.1 - 2026-07-15

### Fixed

- 修复语义词典内容无法选中复制的问题；切换 Linux、Kubernetes、NVIDIA GPU 或容器运行时词典时，语义测试台现在会同步对应组件和示例日志，并清空旧测试结果。

## 1.19.0 - 2026-07-15

### Added

- 新增 M11.5 确定性语义增强层，首期识别 HTTP 状态码、errno、exit code、signal、NVIDIA Xid 和 Kubernetes Reason，并生成 Typed Parameters。
- 新增 Linux、Kubernetes、NVIDIA 和容器运行时四类只读内置语义词典。
- 新增按词典独立演进的文件化候选版本库，支持追加式版本、校验报告、人工发布、活动指针、审计回滚和任务可锁定快照。
- 小文件与大文件流水线支持锁定词典快照，在不改变 Drain3 结构聚类的前提下，将有界语义字段、标签和 Typed Parameters 传递到模板窗口、风险实体与脱敏 Evidence。
- 新增语义词典列表、版本、候选保存、校验、人工发布、回滚、单日志测试和模板语义摘要 API；大文件任务持久化创建时锁定的完整安全快照。
- 质量中心新增“语义词典”页面，提供独立词典状态、只读内置规则、自定义扩展编辑、单日志语义测试、Typed Mask、版本历史、校验、发布和回滚操作。

### Security

- 语义提取不调用 LLM，不生成 RCA 或处置建议，也不额外持久化原始日志。

## 1.18.0 - 2026-07-15

### Added

- 新增文件化 Drain3 配置版本库，以只读 `configs/drain3_recommended.ini` 为系统基线，支持候选配置、完整 INI 快照、参数与脱敏正则校验、发布指针和审计回滚。
- 新增 Drain3 配置治理 API；候选配置必须关联同版本、同 Hash 且通过质量门槛的评测任务，发布与回滚均要求人工确认。
- 普通上传与大文件任务在创建时锁定当前活动配置 ID、版本、Hash 和快照路径，配置切换只影响后续新任务。
- 质量中心新增“Drain3 配置”页面，提供配置版本库、结构化算法参数、脱敏规则列表、INI 原文、基线差异、配置校验、评测关联、人工发布和回滚操作。
- 配置详情支持切换并查看全部历史版本；历史版本保持只读，可作为回滚目标，最新候选版本继续采用追加式保存。

## 1.17.1 - 2026-07-14

### Fixed

- 统一 M11 质量中心与系统设置的字号、间距、按钮、表单、状态标签和卡片样式，修复页面出现重复工作台标题及浏览器原生控件破坏视觉一致性的问题。
- 标注工作台改为可选择的模板队列与审核详情联动布局；可疑模板增加问题分类筛选；配置对比改为字段级参数差异表。
- 模板管理增加搜索、组件和状态筛选，发布管理增加人工治理阶段展示，并补齐窄屏响应式布局。

## 1.17.0 - 2026-07-14

### Added

- 新增 M11 Drain Template Quality Center，支持 Gold Dataset、人工簇标注与复核、有标签/无标签/稳定性/下游/性能指标以及 Profile 对比。
- 新增 kernel、kubelet、containerd、audit、podlog 五类候选 Drain3 Profile，并提供带硬门槛的 Grid Search 排序基础能力。
- 新增版本化 Drain3 模板治理：编辑、忽略、合并、恢复、软删除、历史和回滚均保留追加式审计记录。
- 新增“评测中心 · 模板质量”和“系统设置”页面，覆盖质量概览、标注工作台、可疑模板、配置对比、模板管理和发布管理。
- 前端支持连接任意 HTTP/HTTPS LOGRISK 后端；可通过 `frontend/dist/config.js` 设置部署默认值，或在浏览器中测试并保存覆盖地址。
- Dashboard 新增 `/api/health`、Drain Quality API、可配置 CORS 和跨域预检支持。

### Changed

- Dashboard 分析结果自动登记脱敏 Drain3 模板，并应用当前模板覆盖版本；原始模板和 `template_hash` 始终保留。
- Profile promote/rollback 仅记录人工治理决策，不自动改写生产 Drain3 配置。

### Security

- CORS 默认不向未知来源开放，分离部署必须显式配置 `DASHBOARD_CORS_ORIGINS`。
- 模板治理不保存原始日志；生产有效变更要求人工确认和乐观版本校验。

## 1.16.2 - 2026-07-13

### Fixed

- 修复 Dashboard PID 文件过期后 `status`、`restart` 无法识别实际运行进程的问题。
- 启动时增加端口占用诊断、最长 5 秒存活确认和标准输入重定向，避免误报“已启动”后立即退出。
- 启动脚本继续采用轻量 `nohup` 后台进程，不注册 `launchd` 或其他常驻系统服务，降低对本机环境的影响。
- `foreground` 模式同步登记 PID，确保后续可使用统一脚本查询和停止服务。

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
