from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.put_artifact_enrichment_response_201_context_sources import (
        PutArtifactEnrichmentResponse201ContextSources,
    )


T = TypeVar("T", bound="PutArtifactEnrichmentResponse201")


@_attrs_define
class PutArtifactEnrichmentResponse201:
    """
    Attributes:
        context_sources (PutArtifactEnrichmentResponse201ContextSources | Unset):
    """

    context_sources: PutArtifactEnrichmentResponse201ContextSources | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        context_sources: dict[str, Any] | Unset = UNSET
        if not isinstance(self.context_sources, Unset):
            context_sources = self.context_sources.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if context_sources is not UNSET:
            field_dict["context_sources"] = context_sources

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.put_artifact_enrichment_response_201_context_sources import (
            PutArtifactEnrichmentResponse201ContextSources,
        )

        d = dict(src_dict)
        _context_sources = d.pop("context_sources", UNSET)
        context_sources: PutArtifactEnrichmentResponse201ContextSources | Unset
        if isinstance(_context_sources, Unset):
            context_sources = UNSET
        else:
            context_sources = PutArtifactEnrichmentResponse201ContextSources.from_dict(_context_sources)

        put_artifact_enrichment_response_201 = cls(
            context_sources=context_sources,
        )

        put_artifact_enrichment_response_201.additional_properties = d
        return put_artifact_enrichment_response_201

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
