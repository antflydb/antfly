from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_no_self_loops_type import ExtractionJointConstraintNoSelfLoopsType
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointConstraintNoSelfLoops")


@_attrs_define
class ExtractionJointConstraintNoSelfLoops:
    """
    Attributes:
        type_ (ExtractionJointConstraintNoSelfLoopsType):
        relation (str | Unset):
    """

    type_: ExtractionJointConstraintNoSelfLoopsType
    relation: str | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        relation = self.relation

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if relation is not UNSET:
            field_dict["relation"] = relation

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionJointConstraintNoSelfLoopsType(d.pop("type"))

        relation = d.pop("relation", UNSET)

        extraction_joint_constraint_no_self_loops = cls(
            type_=type_,
            relation=relation,
        )

        return extraction_joint_constraint_no_self_loops
