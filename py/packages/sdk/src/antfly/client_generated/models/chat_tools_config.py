from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chat_tool_name import ChatToolName
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.fetch_config import FetchConfig
    from ..models.web_search_config import WebSearchConfig


T = TypeVar("T", bound="ChatToolsConfig")


@_attrs_define
class ChatToolsConfig:
    """Configuration for retrieval agent tools.

    If `enabled_tools` is empty/omitted, retrieval agents default to all retrieval tools
    available for the request. Explicit retrieval policies should use semantic_search
    for vector retrieval.

    For models that don't support native tool calling (e.g., Ollama),
    a prompt-based fallback is used with structured output parsing.

        Attributes:
            enabled_tools (list[ChatToolName] | Unset): List of tools to enable. If empty, retrieval agents default to all
                retrieval
                tools available for the request.
                 Example: ['add_filter', 'semantic_search', 'web_search'].
            web_search_config (WebSearchConfig | Unset): A unified configuration for web search providers.

                Each provider has specific configuration requirements. Use the appropriate
                provider-specific config or set common options at the top level.

                **Environment Variables (fallbacks):**
                - EXA_API_KEY
                - SERPER_API_KEY
                - TAVILY_API_KEY
                - BRAVE_API_KEY
                - YOU_API_KEY
                - LINKUP_API_KEY
                - GOOGLE_APPLICATION_CREDENTIALS, GOOGLE_CLOUD_PROJECT, GOOGLE_CLOUD_LOCATION
            web_search_connection (str | Unset): Name of a configured connections.<id> resource with kind web_search.
                Request-level tool options may reduce scope, but cannot expand the
                connection's configured capabilities or policy.
            fetch_config (FetchConfig | Unset): Configuration for URL content fetching.

                Uses Antfly's content fetcher for downloading and processing. Supports:
                - HTTP/HTTPS URLs with security validation
                - HTML pages (extracts readable text via go-readability)
                - PDF files (extracts text)
                - Images (returns as data URIs)
                - Plain text files
                - S3 URLs (requires s3_credentials)

                Security features include:
                - Allowed host whitelist
                - Private IP blocking (SSRF prevention)
                - Download size limits
                - Timeout controls
            max_tool_iterations (int | Unset): Maximum number of tool call iterations per turn.
                Prevents infinite loops in tool execution.
                 Default: 5.
    """

    enabled_tools: list[ChatToolName] | Unset = UNSET
    web_search_config: WebSearchConfig | Unset = UNSET
    web_search_connection: str | Unset = UNSET
    fetch_config: FetchConfig | Unset = UNSET
    max_tool_iterations: int | Unset = 5
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        enabled_tools: list[str] | Unset = UNSET
        if not isinstance(self.enabled_tools, Unset):
            enabled_tools = []
            for enabled_tools_item_data in self.enabled_tools:
                enabled_tools_item = enabled_tools_item_data.value
                enabled_tools.append(enabled_tools_item)

        web_search_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.web_search_config, Unset):
            web_search_config = self.web_search_config.to_dict()

        web_search_connection = self.web_search_connection

        fetch_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.fetch_config, Unset):
            fetch_config = self.fetch_config.to_dict()

        max_tool_iterations = self.max_tool_iterations

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled_tools is not UNSET:
            field_dict["enabled_tools"] = enabled_tools
        if web_search_config is not UNSET:
            field_dict["web_search_config"] = web_search_config
        if web_search_connection is not UNSET:
            field_dict["web_search_connection"] = web_search_connection
        if fetch_config is not UNSET:
            field_dict["fetch_config"] = fetch_config
        if max_tool_iterations is not UNSET:
            field_dict["max_tool_iterations"] = max_tool_iterations

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.fetch_config import FetchConfig
        from ..models.web_search_config import WebSearchConfig

        d = dict(src_dict)
        _enabled_tools = d.pop("enabled_tools", UNSET)
        enabled_tools: list[ChatToolName] | Unset = UNSET
        if _enabled_tools is not UNSET:
            enabled_tools = []
            for enabled_tools_item_data in _enabled_tools:
                enabled_tools_item = ChatToolName(enabled_tools_item_data)

                enabled_tools.append(enabled_tools_item)

        _web_search_config = d.pop("web_search_config", UNSET)
        web_search_config: WebSearchConfig | Unset
        if isinstance(_web_search_config, Unset):
            web_search_config = UNSET
        else:
            web_search_config = WebSearchConfig.from_dict(_web_search_config)

        web_search_connection = d.pop("web_search_connection", UNSET)

        _fetch_config = d.pop("fetch_config", UNSET)
        fetch_config: FetchConfig | Unset
        if isinstance(_fetch_config, Unset):
            fetch_config = UNSET
        else:
            fetch_config = FetchConfig.from_dict(_fetch_config)

        max_tool_iterations = d.pop("max_tool_iterations", UNSET)

        chat_tools_config = cls(
            enabled_tools=enabled_tools,
            web_search_config=web_search_config,
            web_search_connection=web_search_connection,
            fetch_config=fetch_config,
            max_tool_iterations=max_tool_iterations,
        )

        chat_tools_config.additional_properties = d
        return chat_tools_config

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
