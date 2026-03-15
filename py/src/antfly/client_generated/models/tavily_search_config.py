from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.tavily_search_config_search_depth import TavilySearchConfigSearchDepth
from ..models.web_search_provider import WebSearchProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="TavilySearchConfig")


@_attrs_define
class TavilySearchConfig:
    """Configuration for Tavily AI Search API.

    Tavily is optimized for RAG and AI applications, providing
    pre-processed results with summaries and relevance scoring.

    **Setup:**
    1. Sign up at https://tavily.com
    2. Get API key from dashboard

    **Docs:** https://docs.tavily.com

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
            api_key (Union[Unset, str]): Tavily API key (or set TAVILY_API_KEY env var)
            search_depth (Union[Unset, TavilySearchConfigSearchDepth]): Search depth:
                - basic: Fast search with standard results
                - advanced: Deeper search with more comprehensive results
                 Default: TavilySearchConfigSearchDepth.BASIC.
            include_answer (Union[Unset, bool]): Include AI-generated answer summary Default: True.
            include_raw_content (Union[Unset, bool]): Include raw HTML content of pages Default: False.
            include_domains (Union[Unset, list[str]]): Only include results from these domains
            exclude_domains (Union[Unset, list[str]]): Exclude results from these domains
    """

    provider: WebSearchProvider
    max_results: Union[Unset, int] = 5
    timeout_ms: Union[Unset, int] = 10000
    safe_search: Union[Unset, bool] = True
    language: Union[Unset, str] = UNSET
    region: Union[Unset, str] = UNSET
    api_key: Union[Unset, str] = UNSET
    search_depth: Union[Unset, TavilySearchConfigSearchDepth] = TavilySearchConfigSearchDepth.BASIC
    include_answer: Union[Unset, bool] = True
    include_raw_content: Union[Unset, bool] = False
    include_domains: Union[Unset, list[str]] = UNSET
    exclude_domains: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        max_results = self.max_results

        timeout_ms = self.timeout_ms

        safe_search = self.safe_search

        language = self.language

        region = self.region

        api_key = self.api_key

        search_depth: Union[Unset, str] = UNSET
        if not isinstance(self.search_depth, Unset):
            search_depth = self.search_depth.value

        include_answer = self.include_answer

        include_raw_content = self.include_raw_content

        include_domains: Union[Unset, list[str]] = UNSET
        if not isinstance(self.include_domains, Unset):
            include_domains = self.include_domains

        exclude_domains: Union[Unset, list[str]] = UNSET
        if not isinstance(self.exclude_domains, Unset):
            exclude_domains = self.exclude_domains

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
        if search_depth is not UNSET:
            field_dict["search_depth"] = search_depth
        if include_answer is not UNSET:
            field_dict["include_answer"] = include_answer
        if include_raw_content is not UNSET:
            field_dict["include_raw_content"] = include_raw_content
        if include_domains is not UNSET:
            field_dict["include_domains"] = include_domains
        if exclude_domains is not UNSET:
            field_dict["exclude_domains"] = exclude_domains

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

        _search_depth = d.pop("search_depth", UNSET)
        search_depth: Union[Unset, TavilySearchConfigSearchDepth]
        if isinstance(_search_depth, Unset):
            search_depth = UNSET
        else:
            search_depth = TavilySearchConfigSearchDepth(_search_depth)

        include_answer = d.pop("include_answer", UNSET)

        include_raw_content = d.pop("include_raw_content", UNSET)

        include_domains = cast(list[str], d.pop("include_domains", UNSET))

        exclude_domains = cast(list[str], d.pop("exclude_domains", UNSET))

        tavily_search_config = cls(
            provider=provider,
            max_results=max_results,
            timeout_ms=timeout_ms,
            safe_search=safe_search,
            language=language,
            region=region,
            api_key=api_key,
            search_depth=search_depth,
            include_answer=include_answer,
            include_raw_content=include_raw_content,
            include_domains=include_domains,
            exclude_domains=exclude_domains,
        )

        tavily_search_config.additional_properties = d
        return tavily_search_config

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
