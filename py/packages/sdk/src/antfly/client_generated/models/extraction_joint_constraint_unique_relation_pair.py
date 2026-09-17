from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_unique_relation_pair_type import (
    ExtractionJointConstraintUniqueRelationPairType,
)
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointConstraintUniqueRelationPair")


@_attrs_define
class ExtractionJointConstraintUniqueRelationPair:
    """
    Attributes:
        type_ (ExtractionJointConstraintUniqueRelationPairType):
        relation (str | Unset):
        directed (bool | Unset):  Default: True.
    """

    type_: ExtractionJointConstraintUniqueRelationPairType
    relation: str | Unset = UNSET
    directed: bool | Unset = True

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        relation = self.relation

        directed = self.directed

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if relation is not UNSET:
            field_dict["relation"] = relation
        if directed is not UNSET:
            field_dict["directed"] = directed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionJointConstraintUniqueRelationPairType(d.pop("type"))

        relation = d.pop("relation", UNSET)

        directed = d.pop("directed", UNSET)

        extraction_joint_constraint_unique_relation_pair = cls(
            type_=type_,
            relation=relation,
            directed=directed,
        )

        return extraction_joint_constraint_unique_relation_pair
