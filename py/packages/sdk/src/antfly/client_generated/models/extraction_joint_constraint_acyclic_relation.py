from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_acyclic_relation_type import ExtractionJointConstraintAcyclicRelationType

T = TypeVar("T", bound="ExtractionJointConstraintAcyclicRelation")


@_attrs_define
class ExtractionJointConstraintAcyclicRelation:
    """
    Attributes:
        type_ (ExtractionJointConstraintAcyclicRelationType):
        relation (str):
    """

    type_: ExtractionJointConstraintAcyclicRelationType
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
        type_ = ExtractionJointConstraintAcyclicRelationType(d.pop("type"))

        relation = d.pop("relation")

        extraction_joint_constraint_acyclic_relation = cls(
            type_=type_,
            relation=relation,
        )

        return extraction_joint_constraint_acyclic_relation
