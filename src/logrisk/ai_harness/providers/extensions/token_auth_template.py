from __future__ import annotations

import os
from typing import Any, Mapping

from logrisk.ai_harness.model_client import ModelClientError
from logrisk.ai_harness.providers.extensions.base import ExtensionDescriptor, ExtensionRequest


class TokenAuthTemplateAdapter:
    """A safe, committed template for an internally implemented Token protocol.

    Only this file (or another explicitly registered peer) should contain
    private authentication, request signing, and response protocol code.
    The shipped template deliberately makes no external request.
    """

    descriptor = ExtensionDescriptor(
        adapter_id="token_auth_template",
        display_name="Token 鉴权适配器模板",
        supported_output_modes=("json_schema", "json_object", "prompt_only"),
        credential_fields={"access_token": "访问 Token 环境变量"},
        config_help="仅保存逻辑凭据名对应的环境变量名；Token 实际值不会保存、展示或写入 Trace。",
    )

    def validate_connection(self, connection: Mapping[str, Any]) -> None:
        if str(connection.get("adapter_id") or "") != self.descriptor.adapter_id:
            raise ModelClientError("扩展连接 adapter_id 与 Token 鉴权模板不匹配")
        credentials = connection.get("credential_envs")
        if not isinstance(credentials, Mapping) or not str(credentials.get("access_token") or ""):
            raise ModelClientError("Token 鉴权模板需要 access_token 环境变量映射")

    def check_connection(self, connection: Mapping[str, Any]) -> dict[str, Any]:
        try:
            self.validate_connection(connection)
        except ModelClientError as exc:
            return {"online": False, "models": [], "error": str(exc)}
        env_name = str(dict(connection.get("credential_envs") or {}).get("access_token") or "")
        if not os.environ.get(env_name):
            return {"online": False, "models": [], "error": f"未配置环境变量: {env_name}"}
        return {"online": False, "models": [], "error": "Token 鉴权模板需在内部环境补全后才能测试"}

    # INTERNAL ADAPTATION AREA: keep private Token acquisition and refresh here.
    def _credential(self, name: str, connection: Mapping[str, Any]) -> str:
        env_name = str(dict(connection.get("credential_envs") or {}).get(name) or "")
        value = os.environ.get(env_name)
        if not env_name or not value:
            raise ModelClientError(f"未配置环境变量: {env_name or name}")
        return value

    # INTERNAL ADAPTATION AREA: construct headers without changing core Provider files.
    def build_headers(self, request: ExtensionRequest) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._credential('access_token', request.connection)}"}

    # INTERNAL ADAPTATION AREA: refresh short-lived credentials only in this adapter.
    def refresh_token_if_needed(self, request: ExtensionRequest) -> None:
        return None

    # INTERNAL ADAPTATION AREA: translate the standard request into private payload fields.
    def build_request_body(self, request: ExtensionRequest) -> dict[str, Any]:
        return {"model": request.model, "messages": request.messages}

    # INTERNAL ADAPTATION AREA: perform the private HTTP or SDK call here.
    def send_request(
        self,
        request: ExtensionRequest,
        headers: Mapping[str, str],
        body: Mapping[str, Any],
    ) -> Any:
        raise ModelClientError("Token 鉴权模板需在内部环境补全后才能调用")

    # INTERNAL ADAPTATION AREA: return only the model's text content.
    def extract_content(self, response: Any) -> str:
        if not isinstance(response, str):
            raise ModelClientError("Token 鉴权适配器必须返回模型文本内容")
        return response

    def generate_content(self, request: ExtensionRequest) -> str:
        self.validate_connection(request.connection)
        raise ModelClientError("Token 鉴权模板需在内部环境补全后才能调用")
