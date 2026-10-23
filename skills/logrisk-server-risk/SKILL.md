---
name: logrisk-server-risk
description: 使用 LOGRISK 项目 MCP 查询指定服务器的每日风险与当前判断，并按用户要求生成可离线打开的 Ling HTML 日报。适用于历史风险查询和报告，不用于原始日志分析、RCA 或运维修复。
---

使用前确认七个 `logrisk_` 工具可用。缺少 MCP 时说明相关查询或报告无法执行；不要改读数据库、原始日志或任意文件。

用 `logrisk_find_nodes` 找到明确 cluster/node_id。同名节点必须澄清，不能模糊关联。锁定日期、年份、IANA 时区（默认 Asia/Shanghai）；“8月31日”必须使用用户或页面已选年份并展示归一化日期。

调用 `logrisk_daily_series` 和 `logrisk_day_evidence`，遍历返回的 next_cursor；汇总来自后端完整事实。用 `logrisk_current_assessment` 区分查询日判断与现在。缺覆盖、过期、历史事实、投影未完成均说明未知；acknowledged/manual_closed 不能证明当前健康，error_only 无新错误不能证明恢复。

用户要求报告时调用 `logrisk_generate_day_report`，显式指定 `ling_3_0_tiny_8b_q4km`（或实际匹配 Ling 的用户 Profile）和稳定 idempotency_key。报告由应用的固定 Ling 流程执行，不能使用宿主模型编造报告、提交 HTML 或更换 Qwen。用 `logrisk_get_report` 有限查询 queued/running/终态。报告完成后返回真实 artifact.url；如果工具给出相对URL，保留相对引用并说明依赖当前 LOGRISK 应用地址，不猜测服务器端口。失败说明安全 error.code；不要自动创建新请求或重试模型。用户要求取消时用 `logrisk_cancel_report`，持续调用可能在锁定超时内收束，取消后无可下载成品。

不得追加采集/分析、审批/导出特征、修改规则、运维或外部发布。Skill 不授予权限。v1 MCP 是本地 stdio→loopback HTTP 客户端；生产身份启用而无合格宿主身份桥会拒绝，不能注入 actor/roles 或直接访问业务库绕过。

需要精确字段、错误与示例时阅读 [接口合同](references/contracts.md)。

自动恢复解释：必须原样引用返回的evaluation_mode、as_of、policy_version、hold/observed/remaining_seconds、missing_conditions、evidence_refs、decision_ref和episode_refs。current_assessment固定使用当前时间，不注入客户端clock/as_of。day_evidence为历史事件时间回看；historical clear不能称当前健康。SYNTHETIC仅为合成夹具，真实存量历史缺少可信来源和覆盖时保留unknown。人工确认/恢复仅审计处理元数据，不能变更判断。
