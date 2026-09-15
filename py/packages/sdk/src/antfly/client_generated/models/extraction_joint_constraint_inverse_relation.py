from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_inverse_relation_type import ExtractionJointConstraintInverseRelationType

T = TypeVar("T", bound="ExtractionJointConstraintInverseRelation")


@_attrs_define
class ExtractionJointConstraintInverseRelation:
    """
    Attributes:
        type_ (ExtractionJointConstraintInverseRelationType):
        relation (str):
        inverse (str):
    """

    type_: ExtractionJointConstraintInverseRelationType
    relation: str
    inverse: str

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        relation = self.relation

        inverse = self.inverse

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "relation": relation,
                "inverse": inverse,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionJointConstraintInverseRelationType(d.pop("type"))

        relation = d.pop("relation")

        inverse = d.pop("inverse")

        extraction_joint_constraint_inverse_relation = cls(
            type_=type_,
            relation=relation,
            inverse=inverse,
        )

        return extraction_joint_constraint_inverse_relation
