from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.enrichment_context_source import EnrichmentContextSource


T = TypeVar("T", bound="EnrichmentConfigContextSources")


@_attrs_define
class EnrichmentConfigContextSources:
    """Named cross-document context consumed by this asset producer, rendered as a canonical `{"context":{<name>:...}}`
    block appended to the producer input and folded into its skip state. Changes to consumed inputs (projected fields,
    query membership, insertion of a first match, deletion, TTL expiry, or a missing optional document becoming present)
    invalidate and rebuild the artifact; unrelated rows and unconsumed fields do not. Only valid on asset enrichments
    whose producer consumes prompt text. Supported by embedded runtimes (single-table and multi-table Lite); distributed
    tables reject the declaration explicitly.

    """

    additional_properties: dict[str, EnrichmentContextSource] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_context_source import EnrichmentContextSource

        d = dict(src_dict)
        enrichment_config_context_sources = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = EnrichmentContextSource.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        enrichment_config_context_sources.additional_properties = additional_properties
        return enrichment_config_context_sources

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> EnrichmentContextSource:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: EnrichmentContextSource) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
