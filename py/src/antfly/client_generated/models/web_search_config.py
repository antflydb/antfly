from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.web_search_provider import WebSearchProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="WebSearchConfig")


@_attrs_define
class WebSearchConfig:
    """A unified configuration for web search providers.

    Each provider has specific configuration requirements. Use the appropriate
    provider-specific config or set common options at the top level.

    **Environment Variables (fallbacks):**
    - GOOGLE_CSE_API_KEY, GOOGLE_CSE_ID
    - BING_SEARCH_API_KEY
    - SERPER_API_KEY
    - TAVILY_API_KEY
    - BRAVE_API_KEY

        Attributes:
            provider (WebSearchProvider): The web search provider to use.

                - **google**: Google Custom Search API (requires CSE setup)
                - **bing**: Microsoft Bing Web Search API
                - **serper**: Serper.dev Google Search API (simpler setup)
                - **tavily**: Tavily AI Search API (optimized for RAG)
                - **brave**: Brave Search API
                - **duckduckgo**: DuckDuckGo Instant Answer API (limited, no API key)
            max_results (Union[Unset, int]): Maximum number of search results to return Default: 5.
            timeout_ms (Union[Unset, int]): Request timeout in milliseconds Default: 10000.
            safe_search (Union[Unset, bool]): Enable safe search filtering Default: True.
            language (Union[Unset, str]): Preferred language for results (e.g., 'en', 'es', 'fr') Example: en.
            region (Union[Unset, str]): Preferred region for results (e.g., 'us', 'uk', 'de') Example: us.
    """

    provider: WebSearchProvider
    max_results: Union[Unset, int] = 5
    timeout_ms: Union[Unset, int] = 10000
    safe_search: Union[Unset, bool] = True
    language: Union[Unset, str] = UNSET
    region: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        max_results = self.max_results

        timeout_ms = self.timeout_ms

        safe_search = self.safe_search

        language = self.language

        region = self.region

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if max_results is not UNSET:
            field_dict["max_results"] = max_results
        if timeout_ms is not UNSET:
            field_dict["timeout_ms"] = timeout_ms
        if safe_search is not UNSET:
            field_dict["safe_search"] = safe_search
        if language is not UNSET:
            field_dict["language"] = language
        if region is not UNSET:
            field_dict["region"] = region

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = WebSearchProvider(d.pop("provider"))

        max_results = d.pop("max_results", UNSET)

        timeout_ms = d.pop("timeout_ms", UNSET)

        safe_search = d.pop("safe_search", UNSET)

        language = d.pop("language", UNSET)

        region = d.pop("region", UNSET)

        web_search_config = cls(
            provider=provider,
            max_results=max_results,
            timeout_ms=timeout_ms,
            safe_search=safe_search,
            language=language,
            region=region,
        )

        web_search_config.additional_properties = d
        return web_search_config

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
