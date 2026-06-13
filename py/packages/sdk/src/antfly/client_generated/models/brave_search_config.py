from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.brave_search_config_freshness import BraveSearchConfigFreshness
from ..models.web_search_provider import WebSearchProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="BraveSearchConfig")


@_attrs_define
class BraveSearchConfig:
    """Configuration for Brave Search API.

    Brave Search provides privacy-focused search with its own independent index.

    **Setup:**
    1. Sign up at https://brave.com/search/api/
    2. Get API key from dashboard

    **Docs:** https://api.search.brave.com/app/documentation

        Attributes:
            provider (WebSearchProvider): The web search provider to use.

                - **exa**: Exa neural/semantic web search API
                - **serper**: Serper.dev Google Search API (simpler setup)
                - **tavily**: Tavily AI Search API (optimized for RAG)
                - **brave**: Brave Search API
                - **you**: You.com Search API for agent and research workflows
                - **linkup**: Linkup Search API for web search and content retrieval
                - **vertex**: Google Cloud Agent Search / Vertex AI Search
            api_key (str | Unset): Brave Search API key (or set BRAVE_API_KEY env var)
            endpoint (str | Unset): Provider endpoint override when applicable
            max_results (int | Unset): Maximum number of search results to return Default: 5.
            timeout_ms (int | Unset): Request timeout in milliseconds Default: 10000.
            safe_search (bool | Unset): Enable safe search filtering Default: True.
            language (str | Unset): Preferred language for results (e.g., 'en', 'es', 'fr') Example: en.
            region (str | Unset): Preferred region for results (e.g., 'us', 'uk', 'de') Example: us.
            include_content (bool | Unset): Ask the provider to return extracted page content when supported Default: False.
            include_highlights (bool | Unset): Ask the provider to return highlighted passages when supported Default:
                False.
            freshness (BraveSearchConfigFreshness | Unset): Freshness filter: pd=day, pw=week, pm=month, py=year
            text_decorations (bool | Unset): Include text decorations (bold, italic markers) Default: False.
            spellcheck (bool | Unset): Enable spellcheck suggestions Default: True.
    """

    provider: WebSearchProvider
    api_key: str | Unset = UNSET
    endpoint: str | Unset = UNSET
    max_results: int | Unset = 5
    timeout_ms: int | Unset = 10000
    safe_search: bool | Unset = True
    language: str | Unset = UNSET
    region: str | Unset = UNSET
    include_content: bool | Unset = False
    include_highlights: bool | Unset = False
    freshness: BraveSearchConfigFreshness | Unset = UNSET
    text_decorations: bool | Unset = False
    spellcheck: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        api_key = self.api_key

        endpoint = self.endpoint

        max_results = self.max_results

        timeout_ms = self.timeout_ms

        safe_search = self.safe_search

        language = self.language

        region = self.region

        include_content = self.include_content

        include_highlights = self.include_highlights

        freshness: str | Unset = UNSET
        if not isinstance(self.freshness, Unset):
            freshness = self.freshness.value

        text_decorations = self.text_decorations

        spellcheck = self.spellcheck

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if api_key is not UNSET:
            field_dict["api_key"] = api_key
        if endpoint is not UNSET:
            field_dict["endpoint"] = endpoint
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
        if include_content is not UNSET:
            field_dict["include_content"] = include_content
        if include_highlights is not UNSET:
            field_dict["include_highlights"] = include_highlights
        if freshness is not UNSET:
            field_dict["freshness"] = freshness
        if text_decorations is not UNSET:
            field_dict["text_decorations"] = text_decorations
        if spellcheck is not UNSET:
            field_dict["spellcheck"] = spellcheck

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = WebSearchProvider(d.pop("provider"))

        api_key = d.pop("api_key", UNSET)

        endpoint = d.pop("endpoint", UNSET)

        max_results = d.pop("max_results", UNSET)

        timeout_ms = d.pop("timeout_ms", UNSET)

        safe_search = d.pop("safe_search", UNSET)

        language = d.pop("language", UNSET)

        region = d.pop("region", UNSET)

        include_content = d.pop("include_content", UNSET)

        include_highlights = d.pop("include_highlights", UNSET)

        _freshness = d.pop("freshness", UNSET)
        freshness: BraveSearchConfigFreshness | Unset
        if isinstance(_freshness, Unset):
            freshness = UNSET
        else:
            freshness = BraveSearchConfigFreshness(_freshness)

        text_decorations = d.pop("text_decorations", UNSET)

        spellcheck = d.pop("spellcheck", UNSET)

        brave_search_config = cls(
            provider=provider,
            api_key=api_key,
            endpoint=endpoint,
            max_results=max_results,
            timeout_ms=timeout_ms,
            safe_search=safe_search,
            language=language,
            region=region,
            include_content=include_content,
            include_highlights=include_highlights,
            freshness=freshness,
            text_decorations=text_decorations,
            spellcheck=spellcheck,
        )

        brave_search_config.additional_properties = d
        return brave_search_config

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
