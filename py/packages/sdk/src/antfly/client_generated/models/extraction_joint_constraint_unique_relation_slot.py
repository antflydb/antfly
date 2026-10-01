from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_unique_relation_slot_slot import (
    ExtractionJointConstraintUniqueRelationSlotSlot,
)
from ..models.extraction_joint_constraint_unique_relation_slot_type import (
    ExtractionJointConstraintUniqueRelationSlotType,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointConstraintUniqueRelationSlot")


@_attrs_define
class ExtractionJointConstraintUniqueRelationSlot:
    """
    Attributes:
        type_ (ExtractionJointConstraintUniqueRelationSlotType):
        relation (str | Unset):
        slot (ExtractionJointConstraintUniqueRelationSlotSlot | Unset):  Default:
            ExtractionJointConstraintUniqueRelationSlotSlot.HEAD.
    """

    type_: ExtractionJointConstraintUniqueRelationSlotType
    relation: str | Unset = UNSET
    slot: ExtractionJointConstraintUniqueRelationSlotSlot | Unset = ExtractionJointConstraintUniqueRelationSlotSlot.HEAD

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        relation = self.relation

        slot: str | Unset = UNSET
        if not isinstance(self.slot, Unset):
            slot = self.slot.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if relation is not UNSET:
            field_dict["relation"] = relation
        if slot is not UNSET:
            field_dict["slot"] = slot

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionJointConstraintUniqueRelationSlotType(d.pop("type"))

        relation = d.pop("relation", UNSET)

        _slot = d.pop("slot", UNSET)
        slot: ExtractionJointConstraintUniqueRelationSlotSlot | Unset
        if isinstance(_slot, Unset):
            slot = UNSET
        else:
            slot = ExtractionJointConstraintUniqueRelationSlotSlot(_slot)

        extraction_joint_constraint_unique_relation_slot = cls(
            type_=type_,
            relation=relation,
            slot=slot,
        )

        return extraction_joint_constraint_unique_relation_slot
