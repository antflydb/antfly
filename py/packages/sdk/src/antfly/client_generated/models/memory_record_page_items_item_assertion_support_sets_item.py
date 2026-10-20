from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_record_page_items_item_assertion_support_sets_item_evidence_item import (
        MemoryRecordPageItemsItemAssertionSupportSetsItemEvidenceItem,
    )


T = TypeVar("T", bound="MemoryRecordPageItemsItemAssertionSupportSetsItem")


@_attrs_define
class MemoryRecordPageItemsItemAssertionSupportSetsItem:
    """
    Attributes:
        evidence (list[MemoryRecordPageItemsItemAssertionSupportSetsItemEvidenceItem]):
        depends_on (list[str] | Unset):
    """

    evidence: list[MemoryRecordPageItemsItemAssertionSupportSetsItemEvidenceItem]
    depends_on: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        evidence = []
        for evidence_item_data in self.evidence:
            evidence_item = evidence_item_data.to_dict()
            evidence.append(evidence_item)

        depends_on: list[str] | Unset = UNSET
        if not isinstance(self.depends_on, Unset):
            depends_on = self.depends_on

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "evidence": evidence,
            }
        )
        if depends_on is not UNSET:
            field_dict["depends_on"] = depends_on

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_record_page_items_item_assertion_support_sets_item_evidence_item import (
            MemoryRecordPageItemsItemAssertionSupportSetsItemEvidenceItem,
        )

        d = dict(src_dict)
        evidence = []
        _evidence = d.pop("evidence")
        for evidence_item_data in _evidence:
            evidence_item = MemoryRecordPageItemsItemAssertionSupportSetsItemEvidenceItem.from_dict(evidence_item_data)

            evidence.append(evidence_item)

        depends_on = cast(list[str], d.pop("depends_on", UNSET))

        memory_record_page_items_item_assertion_support_sets_item = cls(
            evidence=evidence,
            depends_on=depends_on,
        )

        return memory_record_page_items_item_assertion_support_sets_item
