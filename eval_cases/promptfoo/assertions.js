const REQUIRED = ["feature_type", "title", "summary", "importance", "template_hashes", "components", "tags", "selection_reason"];
const IMPORTANCE = new Set(["low", "medium", "high", "critical"]);
function result(pass, reason) { return { pass, score: pass ? 1 : 0, reason }; }
function parsed(output) { try { return JSON.parse(output); } catch (_) { return null; } }
function payload(output) { const value = parsed(output); return value && Object.keys(value).length === 1 && Array.isArray(value.features) ? value : null; }
function expected(context) { return JSON.parse(context.vars.expected_json || "{}"); }
function evidence(context) { return JSON.parse(context.vars.evidence_json || "{}"); }
function features(output) { const value = payload(output); return value && value.features; }

module.exports.validJson = (output) => result(parsed(output) !== null, "输出必须是有效 JSON");
module.exports.schemaValid = (output) => {
  const items = features(output);
  if (!items) return result(false, "顶层必须且只能包含 features 数组");
  const valid = items.every((item) => item && typeof item === "object" && REQUIRED.every((key) => Object.prototype.hasOwnProperty.call(item, key)) &&
    REQUIRED.length === Object.keys(item).length && /^[a-z][a-z0-9_]*$/.test(item.feature_type) &&
    typeof item.title === "string" && item.title.trim() && typeof item.summary === "string" && item.summary.trim() &&
    IMPORTANCE.has(item.importance) && Array.isArray(item.template_hashes) && item.template_hashes.length > 0 &&
    item.template_hashes.every(Boolean) && Array.isArray(item.components) && item.components.length > 0 && item.components.every(Boolean) &&
    Array.isArray(item.tags) && item.tags.length > 0 && item.tags.every((tag) => typeof tag === "string" && tag.trim()) &&
    typeof item.selection_reason === "string" && item.selection_reason.trim());
  return result(Boolean(valid), "每条特征必须严格符合 8 字段 Schema");
};
module.exports.knownHashes = (output, context) => {
  const items = features(output); if (!items) return result(false, "无法解析 features");
  const allowed = new Set((evidence(context).templates || []).map((item) => item.template_hash));
  const valid = items.every((item) => (item.template_hashes || []).every((hash) => allowed.has(hash)));
  return result(valid, "template_hashes 必须来自 Evidence");
};
module.exports.knownComponents = (output, context) => {
  const items = features(output); if (!items) return result(false, "无法解析 features");
  const allowed = new Set((evidence(context).templates || []).map((item) => item.component));
  return result(items.every((item) => (item.components || []).every((component) => allowed.has(component))), "components 必须来自 Evidence");
};
module.exports.expectedFeatureTypes = (output, context) => {
  const items = features(output); if (!items) return result(false, "无法解析 features");
  const actual = items.map((item) => item.feature_type);
  const valid = (expected(context).expected_feature_types || []).every((item) => actual.some((value) => value === item || value.startsWith(item + "_")));
  return result(valid, "缺少预期 feature_type");
};
module.exports.expectedEmptyFeatures = (output, context) => {
  const items = features(output); if (!items) return result(false, "无法解析 features");
  return result(!expected(context).expect_empty_features || items.length === 0, "正常日志必须返回空 features");
};
module.exports.forbiddenClaims = (output, context) => {
  const hits = (expected(context).forbidden_claims || []).filter((word) => output.includes(word));
  return result(hits.length === 0, hits.length ? "包含禁止表达: " + hits.join(", ") : "未包含禁止表达");
};
module.exports.noRawLogLeak = (output) => result(!/(raw_sample|samples|raw_log)/.test(output), "输出不得泄漏原始日志字段");
