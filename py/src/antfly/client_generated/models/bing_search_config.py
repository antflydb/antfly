from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.bing_search_config_freshness import BingSearchConfigFreshness
from ..models.web_search_provider import WebSearchProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="BingSearchConfig")


@_attrs_define
class BingSearchConfig:
    """Configuration for Microsoft Bing Web Search API.

    **Setup:**
    1. Create Azure account at https://portal.azure.com
    2. Create a Bing Search resource
    3. Get API key from resource keys

    **Docs:** https://docs.microsoft.com/en-us/bing/search-apis/bing-web-search/overview

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
            api_key (Union[Unset, str]): Bing Search API key (or set BING_SEARCH_API_KEY env var)
            endpoint (Union[Unset, str]): Bing API endpoint URL Default: 'https://api.bing.microsoft.com/v7.0/search'.
            freshness (Union[Unset, BingSearchConfigFreshness]): Filter results by freshness
    """

    provider: WebSearchProvider
    max_results: Union[Unset, int] = 5
    timeout_ms: Union[Unset, int] = 10000
    safe_search: Union[Unset, bool] = True
    language: Union[Unset, str] = UNSET
    region: Union[Unset, str] = UNSET
    api_key: Union[Unset, str] = UNSET
    endpoint: Union[Unset, str] = "https://api.bing.microsoft.com/v7.0/search"
    freshness: Union[Unset, BingSearchConfigFreshness] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        max_results = self.max_results

        timeout_ms = self.timeout_ms

        safe_search = self.safe_search

        language = self.language

        region = self.region

        api_key = self.api_key

        endpoint = self.endpoint

        freshness: Union[Unset, str] = UNSET
        if not isinstance(self.freshness, Unset):
            freshness = self.freshness.value

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
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if endpoint is not UNSET:
            field_dict["endpoint"] = endpoint
        if freshness is not UNSET:
            field_dict["freshness"] = freshness

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

        api_key = d.pop("api_key", UNSET)

        endpoint = d.pop("endpoint", UNSET)

        _freshness = d.pop("freshness", UNSET)
        freshness: Union[Unset, BingSearchConfigFreshness]
        if isinstance(_freshness, Unset):
            freshness = UNSET
        else:
            freshness = BingSearchConfigFreshness(_freshness)

        bing_search_config = cls(
            provider=provider,
            max_results=max_results,
            timeout_ms=timeout_ms,
            safe_search=safe_search,
            language=language,
            region=region,
            api_key=api_key,
            endpoint=endpoint,
            freshness=freshness,
        )

        bing_search_config.additional_properties = d
        return bing_search_config

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
