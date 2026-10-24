from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_correction_lifecycle import MemoryCorrectionLifecycle
from ..models.memory_correction_stability import MemoryCorrectionStability
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_correction_evidence_item import MemoryCorrectionEvidenceItem


T = TypeVar("T", bound="MemoryCorrection")


@_attrs_define
class MemoryCorrection:
    """
    Attributes:
        id (str):
        expected_revision (int):
        lifecycle (MemoryCorrectionLifecycle):
        reason (str):
        stability (MemoryCorrectionStability | Unset):
        evidence (list[MemoryCorrectionEvidenceItem] | Unset):
    """

    id: str
    expected_revision: int
    lifecycle: MemoryCorrectionLifecycle
    reason: str
    stability: MemoryCorrectionStability | Unset = UNSET
    evidence: list[MemoryCorrectionEvidenceItem] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        expected_revision = self.expected_revision

        lifecycle = self.lifecycle.value

        reason = self.reason

        stability: str | Unset = UNSET
        if not isinstance(self.stability, Unset):
            stability = self.stability.value

        evidence: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.evidence, Unset):
            evidence = []
            for evidence_item_data in self.evidence:
                evidence_item = evidence_item_data.to_dict()
                evidence.append(evidence_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "expected_revision": expected_revision,
                "lifecycle": lifecycle,
                "reason": reason,
            }
        )
        if stability is not UNSET:
            field_dict["stability"] = stability
        if evidence is not UNSET:
            field_dict["evidence"] = evidence

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_correction_evidence_item import MemoryCorrectionEvidenceItem

        d = dict(src_dict)
        id = d.pop("id")

        expected_revision = d.pop("expected_revision")

        lifecycle = MemoryCorrectionLifecycle(d.pop("lifecycle"))

        reason = d.pop("reason")

        _stability = d.pop("stability", UNSET)
        stability: MemoryCorrectionStability | Unset
        if isinstance(_stability, Unset):
            stability = UNSET
        else:
            stability = MemoryCorrectionStability(_stability)

        _evidence = d.pop("evidence", UNSET)
        evidence: list[MemoryCorrectionEvidenceItem] | Unset = UNSET
        if _evidence is not UNSET:
            evidence = []
            for evidence_item_data in _evidence:
                evidence_item = MemoryCorrectionEvidenceItem.from_dict(evidence_item_data)

                evidence.append(evidence_item)

        memory_correction = cls(
            id=id,
            expected_revision=expected_revision,
            lifecycle=lifecycle,
            reason=reason,
            stability=stability,
            evidence=evidence,
        )

        return memory_correction
