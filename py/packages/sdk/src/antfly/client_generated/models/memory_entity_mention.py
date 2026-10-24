from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.memory_entity_mention_visibility import MemoryEntityMentionVisibility
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memory_entity_mention_evidence_item import MemoryEntityMentionEvidenceItem


T = TypeVar("T", bound="MemoryEntityMention")


@_attrs_define
class MemoryEntityMention:
    """
    Attributes:
        id (str):
        entity_id (str):
        evidence (list[MemoryEntityMentionEvidenceItem]):
        visibility (MemoryEntityMentionVisibility | Unset):  Default: MemoryEntityMentionVisibility.PRIVATE.
    """

    id: str
    entity_id: str
    evidence: list[MemoryEntityMentionEvidenceItem]
    visibility: MemoryEntityMentionVisibility | Unset = MemoryEntityMentionVisibility.PRIVATE

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        entity_id = self.entity_id

        evidence = []
        for evidence_item_data in self.evidence:
            evidence_item = evidence_item_data.to_dict()
            evidence.append(evidence_item)

        visibility: str | Unset = UNSET
        if not isinstance(self.visibility, Unset):
            visibility = self.visibility.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "entity_id": entity_id,
                "evidence": evidence,
            }
        )
        if visibility is not UNSET:
            field_dict["visibility"] = visibility

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_mention_evidence_item import MemoryEntityMentionEvidenceItem

        d = dict(src_dict)
        id = d.pop("id")

        entity_id = d.pop("entity_id")

        evidence = []
        _evidence = d.pop("evidence")
        for evidence_item_data in _evidence:
            evidence_item = MemoryEntityMentionEvidenceItem.from_dict(evidence_item_data)

            evidence.append(evidence_item)

        _visibility = d.pop("visibility", UNSET)
        visibility: MemoryEntityMentionVisibility | Unset
        if isinstance(_visibility, Unset):
            visibility = UNSET
        else:
            visibility = MemoryEntityMentionVisibility(_visibility)

        memory_entity_mention = cls(
            id=id,
            entity_id=entity_id,
            evidence=evidence,
            visibility=visibility,
        )

        return memory_entity_mention
