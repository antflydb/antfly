from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.put_artifact_enrichment_response_201_context_sources_additional_property import (
        PutArtifactEnrichmentResponse201ContextSourcesAdditionalProperty,
    )


T = TypeVar("T", bound="PutArtifactEnrichmentResponse201ContextSources")


@_attrs_define
class PutArtifactEnrichmentResponse201ContextSources:
    """ """

    additional_properties: dict[str, PutArtifactEnrichmentResponse201ContextSourcesAdditionalProperty] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.put_artifact_enrichment_response_201_context_sources_additional_property import (
            PutArtifactEnrichmentResponse201ContextSourcesAdditionalProperty,
        )

        d = dict(src_dict)
        put_artifact_enrichment_response_201_context_sources = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = PutArtifactEnrichmentResponse201ContextSourcesAdditionalProperty.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        put_artifact_enrichment_response_201_context_sources.additional_properties = additional_properties
        return put_artifact_enrichment_response_201_context_sources

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> PutArtifactEnrichmentResponse201ContextSourcesAdditionalProperty:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: PutArtifactEnrichmentResponse201ContextSourcesAdditionalProperty) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
