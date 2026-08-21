from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.hierarchy_evidence_canonical import HierarchyEvidenceCanonical
    from ..models.hierarchy_evidence_mention import HierarchyEvidenceMention


T = TypeVar("T", bound="HierarchyEvidence")


@_attrs_define
class HierarchyEvidence:
    """
    Attributes:
        local_id (str | Unset):
        decision (str | Unset):
        confidence (float | Unset):
        source_artifact (str | Unset):
        source_artifact_key (str | Unset):
        resolution_artifact (str | Unset):
        resolution_artifact_key (str | Unset):
        resolver (str | Unset):
        resolver_table (str | Unset):
        mention (HierarchyEvidenceMention | Unset):
        canonical (HierarchyEvidenceCanonical | Unset):
    """

    local_id: str | Unset = UNSET
    decision: str | Unset = UNSET
    confidence: float | Unset = UNSET
    source_artifact: str | Unset = UNSET
    source_artifact_key: str | Unset = UNSET
    resolution_artifact: str | Unset = UNSET
    resolution_artifact_key: str | Unset = UNSET
    resolver: str | Unset = UNSET
    resolver_table: str | Unset = UNSET
    mention: HierarchyEvidenceMention | Unset = UNSET
    canonical: HierarchyEvidenceCanonical | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        local_id = self.local_id

        decision = self.decision

        confidence = self.confidence

        source_artifact = self.source_artifact

        source_artifact_key = self.source_artifact_key

        resolution_artifact = self.resolution_artifact

        resolution_artifact_key = self.resolution_artifact_key

        resolver = self.resolver

        resolver_table = self.resolver_table

        mention: dict[str, Any] | Unset = UNSET
        if not isinstance(self.mention, Unset):
            mention = self.mention.to_dict()

        canonical: dict[str, Any] | Unset = UNSET
        if not isinstance(self.canonical, Unset):
            canonical = self.canonical.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if local_id is not UNSET:
            field_dict["local_id"] = local_id
        if decision is not UNSET:
            field_dict["decision"] = decision
        if confidence is not UNSET:
            field_dict["confidence"] = confidence
        if source_artifact is not UNSET:
            field_dict["source_artifact"] = source_artifact
        if source_artifact_key is not UNSET:
            field_dict["source_artifact_key"] = source_artifact_key
        if resolution_artifact is not UNSET:
            field_dict["resolution_artifact"] = resolution_artifact
        if resolution_artifact_key is not UNSET:
            field_dict["resolution_artifact_key"] = resolution_artifact_key
        if resolver is not UNSET:
            field_dict["resolver"] = resolver
        if resolver_table is not UNSET:
            field_dict["resolver_table"] = resolver_table
        if mention is not UNSET:
            field_dict["mention"] = mention
        if canonical is not UNSET:
            field_dict["canonical"] = canonical

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.hierarchy_evidence_canonical import HierarchyEvidenceCanonical
        from ..models.hierarchy_evidence_mention import HierarchyEvidenceMention

        d = dict(src_dict)
        local_id = d.pop("local_id", UNSET)

        decision = d.pop("decision", UNSET)

        confidence = d.pop("confidence", UNSET)

        source_artifact = d.pop("source_artifact", UNSET)

        source_artifact_key = d.pop("source_artifact_key", UNSET)

        resolution_artifact = d.pop("resolution_artifact", UNSET)

        resolution_artifact_key = d.pop("resolution_artifact_key", UNSET)

        resolver = d.pop("resolver", UNSET)

        resolver_table = d.pop("resolver_table", UNSET)

        _mention = d.pop("mention", UNSET)
        mention: HierarchyEvidenceMention | Unset
        if isinstance(_mention, Unset):
            mention = UNSET
        else:
            mention = HierarchyEvidenceMention.from_dict(_mention)

        _canonical = d.pop("canonical", UNSET)
        canonical: HierarchyEvidenceCanonical | Unset
        if isinstance(_canonical, Unset):
            canonical = UNSET
        else:
            canonical = HierarchyEvidenceCanonical.from_dict(_canonical)

        hierarchy_evidence = cls(
            local_id=local_id,
            decision=decision,
            confidence=confidence,
            source_artifact=source_artifact,
            source_artifact_key=source_artifact_key,
            resolution_artifact=resolution_artifact,
            resolution_artifact_key=resolution_artifact_key,
            resolver=resolver,
            resolver_table=resolver_table,
            mention=mention,
            canonical=canonical,
        )

        return hierarchy_evidence
