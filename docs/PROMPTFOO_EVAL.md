# Promptfoo 本地评测

M8 使用 Promptfoo 对 `feature_extract_v1` 做本地 Ollama 回归评测。它只在开发期运行，不参与 Dashboard 运行时，也不会发送原始日志。

```bash
npm ci
export OLLAMA_BASE_URL=http://127.0.0.1:11434
npm run promptfoo:eval
```

用例位于 `eval_cases/promptfoo/cases.json`，覆盖 OOM、containerd、磁盘压力、应用连接失败和正常日志误报控制。断言检查 JSON、`features`、禁止表达及模板 Hash 是否来自输入证据。

Prompt 由 `eval_cases/promptfoo/feature_extract_v1.js` 动态加载项目真实的 `prompts/feature_extract_v1.md`，避免评测副本与线上 Prompt 漂移。
