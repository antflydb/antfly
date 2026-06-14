from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.exa_search_config_search_type import ExaSearchConfigSearchType
from ..models.web_search_provider import WebSearchProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExaSearchConfig")


@_attrs_define
class ExaSearchConfig:
    """Configuration for Exa neural/semantic web search.

    Exa is optimized for semantic web search, highlights, and retrieved page
    contents for RAG and agent workflows.

    **Setup:**
    1. Sign up at https://exa.ai
    2. Get API key from dashboard

    **Docs:** https://docs.exa.ai

        Attributes:
            provider (WebSearchProvider): The web search provider to use.

                - **exa**: Exa neural/semantic web search API
                - **serper**: Serper.dev Google Search API (simpler setup)
                - **tavily**: Tavily AI Search API (optimized for RAG)
                - **brave**: Brave Search API
                - **you**: You.com Search API for agent and research workflows
                - **linkup**: Linkup Search API for web search and content retrieval
                - **vertex**: Google Cloud Agent Search / Vertex AI Search
            api_key (str | Unset): Exa API key (or set EXA_API_KEY env var)
            endpoint (str | Unset): Provider endpoint override when applicable
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
            search_type (ExaSearchConfigSearchType | Unset): Search mode to request from Exa Default:
                ExaSearchConfigSearchType.AUTO.
            num_results (int | Unset): Provider-specific result count override
            start_published_date (str | Unset): ISO date/time lower bound for published date filtering
            end_published_date (str | Unset): ISO date/time upper bound for published date filtering
            include_domains (list[str] | Unset): Only include results from these domains
            exclude_domains (list[str] | Unset): Exclude results from these domains
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
    search_type: ExaSearchConfigSearchType | Unset = ExaSearchConfigSearchType.AUTO
    num_results: int | Unset = UNSET
    start_published_date: str | Unset = UNSET
    end_published_date: str | Unset = UNSET
    include_domains: list[str] | Unset = UNSET
    exclude_domains: list[str] | Unset = UNSET
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

        search_type: str | Unset = UNSET
        if not isinstance(self.search_type, Unset):
            search_type = self.search_type.value

        num_results = self.num_results

        start_published_date = self.start_published_date

        end_published_date = self.end_published_date

        include_domains: list[str] | Unset = UNSET
        if not isinstance(self.include_domains, Unset):
            include_domains = self.include_domains

        exclude_domains: list[str] | Unset = UNSET
        if not isinstance(self.exclude_domains, Unset):
            exclude_domains = self.exclude_domains

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
        if search_type is not UNSET:
            field_dict["search_type"] = search_type
        if num_results is not UNSET:
            field_dict["num_results"] = num_results
        if start_published_date is not UNSET:
            field_dict["start_published_date"] = start_published_date
        if end_published_date is not UNSET:
            field_dict["end_published_date"] = end_published_date
        if include_domains is not UNSET:
            field_dict["include_domains"] = include_domains
        if exclude_domains is not UNSET:
            field_dict["exclude_domains"] = exclude_domains

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

        _search_type = d.pop("search_type", UNSET)
        search_type: ExaSearchConfigSearchType | Unset
        if isinstance(_search_type, Unset):
            search_type = UNSET
        else:
            search_type = ExaSearchConfigSearchType(_search_type)

        num_results = d.pop("num_results", UNSET)

        start_published_date = d.pop("start_published_date", UNSET)

        end_published_date = d.pop("end_published_date", UNSET)

        include_domains = cast(list[str], d.pop("include_domains", UNSET))

        exclude_domains = cast(list[str], d.pop("exclude_domains", UNSET))

        exa_search_config = cls(
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
            search_type=search_type,
            num_results=num_results,
            start_published_date=start_published_date,
            end_published_date=end_published_date,
            include_domains=include_domains,
            exclude_domains=exclude_domains,
        )

        exa_search_config.additional_properties = d
        return exa_search_config

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
