const fs = require("fs");
const path = require("path");

module.exports = async function ({ vars }) {
  const promptPath = path.resolve(__dirname, "../../prompts/feature_extract_v1.md");
  const prompt = fs.readFileSync(promptPath, "utf8").trim();
  return prompt + "\n\nEvidence JSON:\n" + vars.evidence_json;
};
