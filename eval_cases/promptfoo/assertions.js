function parsed(output) { try { return JSON.parse(output); } catch (_) { return null; } }
function features(output) { const value = parsed(output); return value && Array.isArray(value.features) ? value.features : null; }
module.exports.validJson = (output) => features(output) !== null;
module.exports.knownHashes = (output, context) => {
  const value = features(output); if (value === null) return false;
  const allowed = new Set(JSON.parse(context.vars.allowed_hashes_json || "[]"));
  return value.every((item) => Array.isArray(item.template_hashes) && item.template_hashes.every((hash) => allowed.has(hash)));
};
module.exports.forbiddenClaims = (output, context) => !JSON.parse(context.vars.forbidden_json || "[]").some((word) => output.includes(word));
