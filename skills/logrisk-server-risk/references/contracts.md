# 项目接口合同

协议固定为 MCP 2025-11-25 stdio，UTF-8 每行一个 JSON-RPC；七工具的 inputSchema/outputSchema 以应用返回的 tools/list 为准。全部输入拒绝额外字段；不能传 SQL、文件路径、API URL、actor、roles、HTML 或模型正文。

| 工具 | 必填参数 | 结果 |
|---|---|---|
| logrisk_find_nodes | 无；可用 q、cluster、cursor、limit | items、next_cursor |
| logrisk_daily_series | cluster、node_id、from、to、timezone | 补齐日期的 items、revision、projection_complete |
| logrisk_day_evidence | cluster、node_id、date、timezone | summary、items、total、next_cursor、quality |
| logrisk_current_assessment | cluster、node_id | present/recovering/clear/unknown、valid_until、reasons |
| logrisk_generate_day_report | cluster、node_id、date、timezone、model_profile_id、idempotency_key | report_id、status、artifact/error |
| logrisk_get_report | report_id | 同一持久 Run |
| logrisk_cancel_report | report_id、idempotency_key | 幂等取消 Run |

查询日期使用 YYYY-MM-DD；范围最多90日，limit默认50且最多100。未知发生次数为 null；known_occurrence_count 只表示已知命中。跨日不可拆聚合为 boundary_ambiguous。分数仅表示真实采样，不倒灌发生日。当前无有效coverage时 unknown；clear 仅指登记观测范围内无当前风险。

示例：查 prod-a / node-01 的 2026-08-31，timezone=Asia/Shanghai；先查日证据并翻页，再查当前状态。要求报告时传 model_profile_id=ling_3_0_tiny_8b_q4km、稳定 idempotency_key；完成后引用 artifact.url，不把 queued 当成功。

稳定错误包括 invalid_date_or_timezone、invalid_cursor、report_disabled、model_profile_mismatch、model_profile_unavailable、report_evidence_validation_failed、report_interrupted、artifact_integrity_error、artifact_path_error、runtime_identity_required。业务错误为 isError=true；协议/Schema错误使用JSON-RPC error。拒绝不落Run、不调用Provider、不写Artifact。

默认不安装到全局技能目录，不修改客户端配置；项目启动示例为 `.venv/bin/python -m logrisk.mcp_server --api-base http://127.0.0.1:8080`。不提供公网/Streamable HTTP MCP 或生产身份网关。

自动恢复解释：必须原样引用返回的evaluation_mode、as_of、policy_version、hold/observed/remaining_seconds、missing_conditions、evidence_refs、decision_ref和episode_refs。current_assessment固定使用当前时间，不注入客户端clock/as_of。day_evidence为历史事件时间回看；historical clear不能称当前健康。SYNTHETIC仅为合成夹具，真实存量历史缺少可信来源和覆盖时保留unknown。人工确认/恢复仅审计处理元数据，不能变更判断。
