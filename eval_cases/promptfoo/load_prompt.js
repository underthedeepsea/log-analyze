const fs = require("fs");
const path = require("path");

function resolvePromptId(configText) {
  const defaults = configText.match(/defaults:\s*\n([\s\S]*?)(?:\n\S|$)/);
  const match = defaults && defaults[1].match(/^\s+feature_extract:\s*([A-Za-z0-9_.-]+)\s*$/m);
  if (!match) throw new Error("configs/ai_harness.yaml 缺少 defaults.feature_extract");
  return process.env.LOGRISK_EVAL_PROMPT_ID || match[1];
}

module.exports = async function ({ vars }) {
  const root = path.resolve(__dirname, "../..");
  const promptId = resolvePromptId(fs.readFileSync(path.join(root, "configs/ai_harness.yaml"), "utf8"));
  const prompt = fs.readFileSync(path.join(root, "prompts", promptId + ".md"), "utf8").trim();
  return prompt + "\n\nEvidence JSON:\n" + vars.evidence_json;
};
module.exports.resolvePromptId = resolvePromptId;
