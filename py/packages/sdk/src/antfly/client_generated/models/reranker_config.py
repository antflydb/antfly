from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.reranker_provider import RerankerProvider
from ..types import UNSET, Unset

T = TypeVar("T", bound="RerankerConfig")


@_attrs_define
class RerankerConfig:
    """A unified configuration for a reranking provider.

    Example:
        {'provider': 'cohere', 'model': 'rerank-v4.0-pro', 'field': 'content'}

    Attributes:
        provider (RerankerProvider): The reranking provider to use.
        field (str | Unset): Field name to extract from documents for reranking.
        template (str | Unset): Handlebars template to render document text for reranking.
        candidate_count (int | Unset): Maximum number of globally highest-ranked retrieval candidates to send to the
            reranker. In distributed deployments each shard retrieves at most this many candidates, the coordinator retains
            the global window, and the provider is called once. Defaults to offset plus the effective final result limit
            and, when supplied explicitly, must be at least that page boundary. Candidates outside this window are not
            returned, but hits.total continues to describe the underlying retrieval match count. The ceiling bounds
            retrieval fan-out, memory, provider latency, and external API cost. The effective window must be at most 1000,
            and providers may impose a lower ceiling; Vertex currently accepts at most 200. Antfly rejects invalid or
            provider-specific windows before retrieval fan-out.
        top_n (int | Unset): Deprecated compatibility override for QueryRequest.limit. When present, this is the final
            page size after reranking and offset is applied after scoring. Prefer QueryRequest.limit. Cannot exceed
            candidate_count when both are present or the selected provider's candidate ceiling; Vertex currently accepts at
            most 200.
    """

    provider: RerankerProvider
    field: str | Unset = UNSET
    template: str | Unset = UNSET
    candidate_count: int | Unset = UNSET
    top_n: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        provider = self.provider.value

        field = self.field

        template = self.template

        candidate_count = self.candidate_count

        top_n = self.top_n

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "provider": provider,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if template is not UNSET:
            field_dict["template"] = template
        if candidate_count is not UNSET:
            field_dict["candidate_count"] = candidate_count
        if top_n is not UNSET:
            field_dict["top_n"] = top_n

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        provider = RerankerProvider(d.pop("provider"))

        field = d.pop("field", UNSET)

        template = d.pop("template", UNSET)

        candidate_count = d.pop("candidate_count", UNSET)

        top_n = d.pop("top_n", UNSET)

        reranker_config = cls(
            provider=provider,
            field=field,
            template=template,
            candidate_count=candidate_count,
            top_n=top_n,
        )

        reranker_config.additional_properties = d
        return reranker_config

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
