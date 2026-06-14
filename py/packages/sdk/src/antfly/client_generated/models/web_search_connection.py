from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="WebSearchConnection")


@_attrs_define
class WebSearchConnection:
    """
    Attributes:
        service (str | Unset): Provider-specific service flavor, such as agent_search for provider vertex.
        max_results (int | Unset): Maximum ranked results this connection is configured to return.
        timeout_ms (int | Unset): Provider request timeout in milliseconds.
        safe_search (bool | Unset): Whether safe-search filtering is requested.
        language (str | Unset): Preferred result language.
        region (str | Unset): Preferred result region.
        include_content (bool | Unset): Whether extracted content is requested when supported.
        include_highlights (bool | Unset): Whether highlighted passages are requested when supported.
        endpoint (str | Unset): Provider endpoint override when configured.
        project_id (str | Unset): Google Cloud project for provider vertex.
        location (str | Unset): Google Cloud location for provider vertex.
        data_store (str | Unset): Agent Search data store ID for provider vertex.
        serving_config (str | Unset): Agent Search serving config ID for provider vertex.
        include_domains (list[str] | Unset): Domain allowlist when configured.
        exclude_domains (list[str] | Unset): Domain denylist when configured.
        configured (bool | Unset): True when required credentials/config are present. Secret values are never returned.
    """

    service: str | Unset = UNSET
    max_results: int | Unset = UNSET
    timeout_ms: int | Unset = UNSET
    safe_search: bool | Unset = UNSET
    language: str | Unset = UNSET
    region: str | Unset = UNSET
    include_content: bool | Unset = UNSET
    include_highlights: bool | Unset = UNSET
    endpoint: str | Unset = UNSET
    project_id: str | Unset = UNSET
    location: str | Unset = UNSET
    data_store: str | Unset = UNSET
    serving_config: str | Unset = UNSET
    include_domains: list[str] | Unset = UNSET
    exclude_domains: list[str] | Unset = UNSET
    configured: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        service = self.service

        max_results = self.max_results

        timeout_ms = self.timeout_ms

        safe_search = self.safe_search

        language = self.language

        region = self.region

        include_content = self.include_content

        include_highlights = self.include_highlights

        endpoint = self.endpoint

        project_id = self.project_id

        location = self.location

        data_store = self.data_store

        serving_config = self.serving_config

        include_domains: list[str] | Unset = UNSET
        if not isinstance(self.include_domains, Unset):
            include_domains = self.include_domains

        exclude_domains: list[str] | Unset = UNSET
        if not isinstance(self.exclude_domains, Unset):
            exclude_domains = self.exclude_domains

        configured = self.configured

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if service is not UNSET:
            field_dict["service"] = service
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
        if include_domains is not UNSET:
            field_dict["include_domains"] = include_domains
        if exclude_domains is not UNSET:
            field_dict["exclude_domains"] = exclude_domains
        if configured is not UNSET:
            field_dict["configured"] = configured

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        service = d.pop("service", UNSET)

        max_results = d.pop("max_results", UNSET)

        timeout_ms = d.pop("timeout_ms", UNSET)

        safe_search = d.pop("safe_search", UNSET)

        language = d.pop("language", UNSET)

        region = d.pop("region", UNSET)

        include_content = d.pop("include_content", UNSET)

        include_highlights = d.pop("include_highlights", UNSET)

        endpoint = d.pop("endpoint", UNSET)

        project_id = d.pop("project_id", UNSET)

        location = d.pop("location", UNSET)

        data_store = d.pop("data_store", UNSET)

        serving_config = d.pop("serving_config", UNSET)

        include_domains = cast(list[str], d.pop("include_domains", UNSET))

        exclude_domains = cast(list[str], d.pop("exclude_domains", UNSET))

        configured = d.pop("configured", UNSET)

        web_search_connection = cls(
            service=service,
            max_results=max_results,
            timeout_ms=timeout_ms,
            safe_search=safe_search,
            language=language,
            region=region,
            include_content=include_content,
            include_highlights=include_highlights,
            endpoint=endpoint,
            project_id=project_id,
            location=location,
            data_store=data_store,
            serving_config=serving_config,
            include_domains=include_domains,
            exclude_domains=exclude_domains,
            configured=configured,
        )

        web_search_connection.additional_properties = d
        return web_search_connection

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
