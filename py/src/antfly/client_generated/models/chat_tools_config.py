from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

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
    """Configuration for chat agent tools.

    If `enabled_tools` is empty/omitted, defaults to: add_filter, ask_clarification, search.

    For models that don't support native tool calling (e.g., Ollama),
    a prompt-based fallback is used with structured output parsing.

        Attributes:
            enabled_tools (Union[Unset, list[ChatToolName]]): List of tools to enable. If empty, defaults to filter,
                clarification, and search.
                 Example: ['add_filter', 'search', 'websearch'].
            websearch_config (Union[Unset, WebSearchConfig]): A unified configuration for web search providers.

                Each provider has specific configuration requirements. Use the appropriate
                provider-specific config or set common options at the top level.

                **Environment Variables (fallbacks):**
                - GOOGLE_CSE_API_KEY, GOOGLE_CSE_ID
                - BING_SEARCH_API_KEY
                - SERPER_API_KEY
                - TAVILY_API_KEY
                - BRAVE_API_KEY
            fetch_config (Union[Unset, FetchConfig]): Configuration for URL content fetching.

                Uses lib/scraping for downloading and processing. Supports:
                - HTTP/HTTPS URLs with security validation
                - HTML pages (extracts readable text via go-readability)
                - PDF files (extracts text)
                - Images (returns as data URIs)
                - Plain text files
                - S3 URLs (requires s3_credentials)

                Security features (from lib/scraping.ContentSecurityConfig):
                - Allowed host whitelist
                - Private IP blocking (SSRF prevention)
                - Download size limits
                - Timeout controls
            max_tool_iterations (Union[Unset, int]): Maximum number of tool call iterations per turn.
                Prevents infinite loops in tool execution.
                 Default: 5.
    """

    enabled_tools: Union[Unset, list[ChatToolName]] = UNSET
    websearch_config: Union[Unset, "WebSearchConfig"] = UNSET
    fetch_config: Union[Unset, "FetchConfig"] = UNSET
    max_tool_iterations: Union[Unset, int] = 5
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        enabled_tools: Union[Unset, list[str]] = UNSET
        if not isinstance(self.enabled_tools, Unset):
            enabled_tools = []
            for enabled_tools_item_data in self.enabled_tools:
                enabled_tools_item = enabled_tools_item_data.value
                enabled_tools.append(enabled_tools_item)

        websearch_config: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.websearch_config, Unset):
            websearch_config = self.websearch_config.to_dict()

        fetch_config: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.fetch_config, Unset):
            fetch_config = self.fetch_config.to_dict()

        max_tool_iterations = self.max_tool_iterations

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled_tools is not UNSET:
            field_dict["enabled_tools"] = enabled_tools
        if websearch_config is not UNSET:
            field_dict["websearch_config"] = websearch_config
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
        enabled_tools = []
        _enabled_tools = d.pop("enabled_tools", UNSET)
        for enabled_tools_item_data in _enabled_tools or []:
            enabled_tools_item = ChatToolName(enabled_tools_item_data)

            enabled_tools.append(enabled_tools_item)

        _websearch_config = d.pop("websearch_config", UNSET)
        websearch_config: Union[Unset, WebSearchConfig]
        if isinstance(_websearch_config, Unset):
            websearch_config = UNSET
        else:
            websearch_config = WebSearchConfig.from_dict(_websearch_config)

        _fetch_config = d.pop("fetch_config", UNSET)
        fetch_config: Union[Unset, FetchConfig]
        if isinstance(_fetch_config, Unset):
            fetch_config = UNSET
        else:
            fetch_config = FetchConfig.from_dict(_fetch_config)

        max_tool_iterations = d.pop("max_tool_iterations", UNSET)

        chat_tools_config = cls(
            enabled_tools=enabled_tools,
            websearch_config=websearch_config,
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
