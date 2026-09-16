from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_max_relations_per_tail_type import (
    ExtractionJointConstraintMaxRelationsPerTailType,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointConstraintMaxRelationsPerTail")


@_attrs_define
class ExtractionJointConstraintMaxRelationsPerTail:
    """
    Attributes:
        type_ (ExtractionJointConstraintMaxRelationsPerTailType):
        limit (int):
        relation (str | Unset):
    """

    type_: ExtractionJointConstraintMaxRelationsPerTailType
    limit: int
    relation: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        limit = self.limit

        relation = self.relation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "limit": limit,
            }
        )
        if relation is not UNSET:
            field_dict["relation"] = relation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionJointConstraintMaxRelationsPerTailType(d.pop("type"))

        limit = d.pop("limit")

        relation = d.pop("relation", UNSET)

        extraction_joint_constraint_max_relations_per_tail = cls(
            type_=type_,
            limit=limit,
            relation=relation,
        )

        return extraction_joint_constraint_max_relations_per_tail
