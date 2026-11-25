from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.put_artifact_enrichment_response_201_context_sources_additional_property_kind import (
    PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyKind,
)
from ..models.put_artifact_enrichment_response_201_context_sources_additional_property_tracking import (
    PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyTracking,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="PutArtifactEnrichmentResponse201ContextSourcesAdditionalProperty")


@_attrs_define
class PutArtifactEnrichmentResponse201ContextSourcesAdditionalProperty:
    """
    Attributes:
        kind (PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyKind | Unset):
        tracking (PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyTracking | Unset):
        reason (str | Unset):
    """

    kind: PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyKind | Unset = UNSET
    tracking: PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyTracking | Unset = UNSET
    reason: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        tracking: str | Unset = UNSET
        if not isinstance(self.tracking, Unset):
            tracking = self.tracking.value

        reason = self.reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if kind is not UNSET:
            field_dict["kind"] = kind
        if tracking is not UNSET:
            field_dict["tracking"] = tracking
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _kind = d.pop("kind", UNSET)
        kind: PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyKind(_kind)

        _tracking = d.pop("tracking", UNSET)
        tracking: PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyTracking | Unset
        if isinstance(_tracking, Unset):
            tracking = UNSET
        else:
            tracking = PutArtifactEnrichmentResponse201ContextSourcesAdditionalPropertyTracking(_tracking)

        reason = d.pop("reason", UNSET)

        put_artifact_enrichment_response_201_context_sources_additional_property = cls(
            kind=kind,
            tracking=tracking,
            reason=reason,
        )

        put_artifact_enrichment_response_201_context_sources_additional_property.additional_properties = d
        return put_artifact_enrichment_response_201_context_sources_additional_property

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
