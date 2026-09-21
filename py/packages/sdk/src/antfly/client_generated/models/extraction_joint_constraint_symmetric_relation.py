from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_symmetric_relation_type import ExtractionJointConstraintSymmetricRelationType

T = TypeVar("T", bound="ExtractionJointConstraintSymmetricRelation")


@_attrs_define
class ExtractionJointConstraintSymmetricRelation:
    """
    Attributes:
        type_ (ExtractionJointConstraintSymmetricRelationType):
        relation (str):
    """

    type_: ExtractionJointConstraintSymmetricRelationType
    relation: str

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        relation = self.relation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "relation": relation,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionJointConstraintSymmetricRelationType(d.pop("type"))

        relation = d.pop("relation")

        extraction_joint_constraint_symmetric_relation = cls(
            type_=type_,
            relation=relation,
        )

        return extraction_joint_constraint_symmetric_relation
