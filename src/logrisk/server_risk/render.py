from __future__ import annotations

import base64
import hashlib
from html import escape
from typing import Any

from logrisk import __version__
RENDERER_VERSION = "node-risk-html-v1"
STYLE = "body{font:16px system-ui,sans-serif;max-width:1080px;margin:40px auto;padding:0 24px;color:#272522;background:#faf9f5}h1{font-size:30px}table{border-collapse:collapse;width:100%;font-size:14px}td,th{padding:10px;border-bottom:1px solid #ddd;text-align:left}small{color:#666}.unknown{color:#776e60}code{overflow-wrap:anywhere}"
CSP = "default-src 'none'; style-src 'sha256-" + base64.b64encode(hashlib.sha256(STYLE.encode()).digest()).decode() + "'; img-src data:; base-uri 'none'; form-action 'none'"
PHRASES = {"risk_summary": "记录显示发生过风险。", "repeated_occurrence": "相关对象存在重复命中。", "multiple_objects": "风险涉及多个明确对象。", "historical_only": "这些是历史事实；当前状态缺少有效观测。", "coverage_gap": "采集覆盖证据不足，不能将无记录解释为正常。", "no_recovery_evidence": "没有权威恢复证据。人工处理记录不能证明当前健康。", "observed_recovery": "登记观测支持在相应证据时间恢复。", "hardware_unresolved": "硬件风险尚无匹配的验证结束依据。"}

LABELS = {"historical_context_only": "恢复依据属于历史回看；当前缺少有效观测", "present": "风险存在", "recovering": "恢复观察中", "clear": "观测范围内无当前风险", "unknown": "未知", "complete": "覆盖完整", "partial": "覆盖不完整", "source_coverage_unregistered": "缺少登记的完整采集覆盖", "boundary_ambiguous": "跨日聚合无法精确拆分", "legacy_unverified": "历史记录缺少可验证的实时覆盖", "applicable_checks_unregistered": "尚未登记适用的状态检查", "required_observations_missing": "缺少必要对象的观测", "evidence_expired": "观测已过有效期", "adverse_evidence_expired_or_historical": "异常证据已过期或仅为历史记录", "projection_pending": "证据处理尚未完成", "no_current_evidence": "缺少当前有效观测"}


def label(value: Any) -> str:
    return "未知" if value is None else LABELS.get(str(value), "证据覆盖不足")


def render_html(evidence: dict[str, Any], blocks: list[dict[str, Any]], profile: dict[str, Any]) -> bytes:
    e = lambda value: escape(str(value), quote=True)
    q, summary = evidence["query"], evidence["summary"]
    lines = ["<!doctype html><html lang=\"zh-CN\"><head><meta charset=\"utf-8\">", '<meta http-equiv="Content-Security-Policy" content="' + e(CSP) + '">', "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>服务器风险日报</title><style>" + STYLE + "</style></head><body>", "<h1>服务器风险日报</h1>", "<p>" + e(q["cluster"]) + " / " + e(q["node_id"]) + " · " + e(q["date"]) + " · " + e(q["timezone"]) + "</p>", '<p class="unknown">当日判断：' + e(label(summary["end_of_day_assessment"])) + "；当前判断：" + e(label(evidence["current_assessment"]["status"])) + "</p>", "<p>已知日志命中：" + e(summary["known_occurrence_count"]) + "；完整发生次数：" + e("未知" if summary["occurrence_count"] is None else summary["occurrence_count"]) + "；覆盖：" + e(label(summary["coverage"])) + "</p>", "<h2>事实摘要</h2>"]
    lines.append("<h2>自动判断依据</h2>")
    for title, explanation in (("当日历史回看", evidence.get("historical_assessment", {})), ("当前实时判断", evidence["current_assessment"])):
        lines.append("<h3>" + e(title) + "</h3><p>模式 " + e(explanation.get("evaluation_mode")) + " · 判断时点 " + e(explanation.get("as_of")) + " · 规则版本 " + e(explanation.get("policy_version")) + "</p>")
        for check in explanation.get("checks", []):
            interval = check.get("observed_interval") or {}
            missing = "；".join(label(reason) for reason in check.get("missing_conditions", [])) or "已满足登记条件"
            lines.append("<p>" + e(check["risk_type"]) + " · " + e(label(check["status"])) + " · 观察 " + e(check.get("observed_seconds")) + "/" + e(check.get("hold_seconds")) + " 秒 · 剩余 " + e(check.get("remaining_seconds")) + " 秒</p><p>观测区间 " + e(interval.get("start") or "未开始") + " → " + e(interval.get("end") or "未提供") + " · 缺失条件 " + e(missing) + "</p>")
            lines.append("<details><summary>证据与决定引用</summary><p>决定 <code>" + e(check.get("decision_ref")) + "</code></p>")
            for title, references in (("事实", check.get("evidence_refs", [])), ("轮次", check.get("episode_refs", []))):
                for reference in references:
                    lines.append("<p>" + e(title) + " <code>" + e(reference) + "</code></p>")
            lines.append("</details>")
    for block in blocks:
        lines.append("<p>" + e(PHRASES[block["claim_code"]]) + " <small>" + e(", ".join(block["fact_refs"])) + "</small></p>")
    lines.append("<h2>风险明细</h2><table><thead><tr><th>对象</th><th>风险</th><th>严重度</th><th>发生区间</th><th>已知次数</th></tr></thead><tbody>")
    for fact in evidence["facts"]:
        lines.append("<tr>" + "".join("<td>" + e(value) + "</td>" for value in (fact["object_id"], fact["risk_type"], fact["severity"], fact["first_seen"] + " – " + fact["last_seen"], fact["count"])) + "</tr>")
    lines.append("</tbody></table><h2>跨日仍未结束的风险</h2><p>以下轮次不增加当日发生次数。</p><table><thead><tr><th>对象</th><th>风险</th><th>严重度</th><th>首次异常</th><th>最后异常</th></tr></thead><tbody>")
    for episode in evidence.get("unfinished_episodes", []):
        lines.append("<tr>" + "".join("<td>" + e(value) + "</td>" for value in (episode["object_id"], episode["risk_type"], episode["severity"], episode["first_seen"], episode["last_seen"])) + "</tr>")
    lines.append("</tbody></table><h2>覆盖与口径</h2><p>" + e("；".join(label(reason) for reason in summary["missing_reasons"]) or "登记的覆盖已完成") + "</p><p>按真实事件区间归本地日；跨日聚合不拆分。分数仅使用真实评分样本。处理确认与当前判断独立。未提供实时完整覆盖时状态为未知。</p><h2>证据索引</h2>")
    for fact in evidence["facts"] + evidence.get("unfinished_episodes", []):
        lines.append("<p><code>" + e(fact["fact_ref"]) + "</code> · " + e(fact["provenance"]) + " · " + e(fact["template_text"]) + "</p>")
    lines.append("<p>模型未接收的事实引用：" + e(evidence["omitted_fact_count"]) + "；以上 HTML 保留完整明细。</p><small>" + e(profile["model"]) + " · " + e(__version__) + " · " + RENDERER_VERSION + " · " + e(evidence["generated_as_of"]) + " · digest " + e(evidence["evidence_digest"]) + "</small></body></html>")
    return "".join(lines).encode("utf-8")
