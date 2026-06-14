from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.web_search_provider import WebSearchProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="YouSearchConfig")


@_attrs_define
class YouSearchConfig:
    """Configuration for You.com Search API.

    You.com is useful for agentic search and research-oriented result
    retrieval.

    **Setup:**
    1. Sign up for You.com API access
    2. Get API key from dashboard

    **Docs:** https://api.you.com

        Attributes:
            provider (WebSearchProvider): The web search provider to use.

                - **exa**: Exa neural/semantic web search API
                - **serper**: Serper.dev Google Search API (simpler setup)
                - **tavily**: Tavily AI Search API (optimized for RAG)
                - **brave**: Brave Search API
                - **you**: You.com Search API for agent and research workflows
                - **linkup**: Linkup Search API for web search and content retrieval
                - **vertex**: Google Cloud Agent Search / Vertex AI Search
            api_key (str | Unset): You.com API key (or set YOU_API_KEY env var)
            endpoint (str | Unset): You.com API endpoint URL
            project_id (str | Unset): Google Cloud project ID for provider vertex. Shared Vertex credential field; see
                vertex.yaml#/components/schemas/VertexCredentials. Falls back to GOOGLE_CLOUD_PROJECT.
            location (str | Unset): Google Cloud location for provider vertex. Shared Vertex credential field; see
                vertex.yaml#/components/schemas/VertexCredentials. Falls back to GOOGLE_CLOUD_LOCATION, then global.
            data_store (str | Unset): Agent Search data store ID for provider vertex.
            serving_config (str | Unset): Agent Search serving config ID for provider vertex. Defaults to default_config.
            credentials_path (str | Unset): Service account JSON path for provider vertex. Shared Vertex credential field;
                see vertex.yaml#/components/schemas/VertexCredentials. Falls back to GOOGLE_APPLICATION_CREDENTIALS or ADC.
            max_results (int | Unset): Maximum number of search results to return Default: 5.
            timeout_ms (int | Unset): Request timeout in milliseconds Default: 10000.
            safe_search (bool | Unset): Enable safe search filtering Default: True.
            language (str | Unset): Preferred language for results (e.g., 'en', 'es', 'fr') Example: en.
            region (str | Unset): Preferred region for results (e.g., 'us', 'uk', 'de') Example: us.
            include_content (bool | Unset): Ask the provider to return extracted page content when supported Default: False.
            include_highlights (bool | Unset): Ask the provider to return highlighted passages when supported Default:
                False.
    """

    provider: WebSearchProvider
    api_key: str | Unset = UNSET
    endpoint: str | Unset = UNSET
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    data_store: str | Unset = UNSET
    serving_config: str | Unset = UNSET
    credentials_path: str | Unset = UNSET
    max_results: int | Unset = 5
    timeout_ms: int | Unset = 10000
    safe_search: bool | Unset = True
    language: str | Unset = UNSET
    region: str | Unset = UNSET
    include_content: bool | Unset = False
    include_highlights: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        api_key = self.api_key

        endpoint = self.endpoint

        project_id = self.project_id

        location = self.location

        data_store = self.data_store

        serving_config = self.serving_config

        credentials_path = self.credentials_path

        max_results = self.max_results

        timeout_ms = self.timeout_ms

        safe_search = self.safe_search

        language = self.language

        region = self.region

        include_content = self.include_content

        include_highlights = self.include_highlights

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
        if project_id is not UNSET:
            field_dict["project_id"] = project_id
        if location is not UNSET:
            field_dict["location"] = location
        if data_store is not UNSET:
            field_dict["data_store"] = data_store
        if serving_config is not UNSET:
            field_dict["serving_config"] = serving_config
        if credentials_path is not UNSET:
            field_dict["credentials_path"] = credentials_path
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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = WebSearchProvider(d.pop("provider"))

        api_key = d.pop("api_key", UNSET)

        endpoint = d.pop("endpoint", UNSET)

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        data_store = d.pop("data_store", UNSET)

        serving_config = d.pop("serving_config", UNSET)

        credentials_path = d.pop("credentials_path", UNSET)

        max_results = d.pop("max_results", UNSET)

        timeout_ms = d.pop("timeout_ms", UNSET)

        safe_search = d.pop("safe_search", UNSET)

        language = d.pop("language", UNSET)

        region = d.pop("region", UNSET)

        include_content = d.pop("include_content", UNSET)

        include_highlights = d.pop("include_highlights", UNSET)

        you_search_config = cls(
            provider=provider,
            api_key=api_key,
            endpoint=endpoint,
            project_id=project_id,
            location=location,
            data_store=data_store,
            serving_config=serving_config,
            credentials_path=credentials_path,
            max_results=max_results,
            timeout_ms=timeout_ms,
            safe_search=safe_search,
            language=language,
            region=region,
            include_content=include_content,
            include_highlights=include_highlights,
        )

        you_search_config.additional_properties = d
        return you_search_config

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
