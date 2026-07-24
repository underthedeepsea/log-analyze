# 本地扩展模型 Provider 开发指南

本指南用于在内部环境适配带 Token、签名或私有 SDK 的模型服务。该扩展只负责模型连接；LOGRISK 仍只做日志特征识别与人工审批，不做 RCA。

## 修改边界

### 允许修改

- `src/logrisk/ai_harness/providers/extensions/token_auth_template.py`：补全 Token 获取/刷新、签名、私有请求体和响应内容提取。
- 如需新的适配器，可在同一 `extensions/` 目录新增文件，并在 `registry.py` 的字面量 allow-list 中显式注册。
- 连接页面的“扩展适配器”中填写 Base URL、环境变量名映射与非敏感配置。

### 禁止修改

- 不要修改 `feature_extractor_ollama.py`、`feature_jobs.py`、审批、规则导出、Trace、数据库迁移或前端任务流程来适配私有协议。
- 不要把 Token、密钥、Cookie、签名原料或鉴权响应写进 `extension_config`、Profile、SQLite、PostgreSQL、Trace、日志、错误消息或代码。
- 不要让适配器接收原始日志、`samples` 或 `raw_sample`，也不要添加自动回退到其他 Provider。

## 最小适配步骤

1. 在 `TokenAuthTemplateAdapter.descriptor` 中声明适配器 ID、支持的结构化输出模式以及所需逻辑凭据字段。
2. 在模型画像页面新建 `extension` 连接。例如凭据映射为：

```json
{
  "access_token": "INTERNAL_LLM_ACCESS_TOKEN"
}
```

3. 在启动服务的终端设置真实值，而不是写入配置：

```bash
export INTERNAL_LLM_ACCESS_TOKEN='replace-in-internal-shell'
bash scripts/dashboard.sh restart
```

4. 仅在模板的 `build_headers()`、`refresh_token_if_needed()`、`build_request_body()`、`send_request()` 与 `extract_content()` 内补全私有协议。`extract_content()` 必须只返回模型原始文本，首字符应为 `{` 或兼容的 fenced JSON。
5. 保留 `generate_content()` 的输入/输出契约；核心客户端会统一解析 JSON、校验八个特征字段、写入脱敏 Trace 并处理重试。

## 验收清单

- 用假的本地 HTTP 服务覆盖成功响应、Token 缺失、Token 刷新、超时、HTTP 错误和 Markdown JSON。
- 执行 `pytest tests/test_extension_provider.py tests/test_provider_connections.py -v`。
- 在页面保存连接后确认只显示环境变量名和“已配置/未配置”，绝不显示真实 Token。
- 检查任务 Trace、数据库和日志，确认不包含真实 Token 或原始日志。
- 未完成内部适配时，模板应明确报“需在内部环境补全”，不得尝试访问外部服务。

## 提交前检查

执行 `git diff --cached --name-only`，确认不包含 `.env`、`state/`、数据库文件、Trace、上传日志、导出物或任何内部协议文档。不得提交 Token。
