from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_record_page_items_item_explicit_disposition_type_0_provenance_type_0_evidence_item import (
        MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0EvidenceItem,
    )


T = TypeVar("T", bound="MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0")


@_attrs_define
class MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0:
    """
    Attributes:
        principal (str):
        recorded_at (int):
        evidence (list[MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0EvidenceItem]):
        forgotten (bool | Unset):
    """

    principal: str
    recorded_at: int
    evidence: list[MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0EvidenceItem]
    forgotten: bool | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        principal = self.principal

        recorded_at = self.recorded_at

        evidence = []
        for evidence_item_data in self.evidence:
            evidence_item = evidence_item_data.to_dict()
            evidence.append(evidence_item)

        forgotten = self.forgotten

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "principal": principal,
                "recorded_at": recorded_at,
                "evidence": evidence,
            }
        )
        if forgotten is not UNSET:
            field_dict["forgotten"] = forgotten

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_record_page_items_item_explicit_disposition_type_0_provenance_type_0_evidence_item import (
            MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0EvidenceItem,
        )

        d = dict(src_dict)
        principal = d.pop("principal")

        recorded_at = d.pop("recorded_at")

        evidence = []
        _evidence = d.pop("evidence")
        for evidence_item_data in _evidence:
            evidence_item = MemoryRecordPageItemsItemExplicitDispositionType0ProvenanceType0EvidenceItem.from_dict(
                evidence_item_data
            )

            evidence.append(evidence_item)

        forgotten = d.pop("forgotten", UNSET)

        memory_record_page_items_item_explicit_disposition_type_0_provenance_type_0 = cls(
            principal=principal,
            recorded_at=recorded_at,
            evidence=evidence,
            forgotten=forgotten,
        )

        return memory_record_page_items_item_explicit_disposition_type_0_provenance_type_0
