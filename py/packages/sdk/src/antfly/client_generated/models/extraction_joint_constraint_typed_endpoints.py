from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.extraction_joint_constraint_typed_endpoints_type import ExtractionJointConstraintTypedEndpointsType
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionJointConstraintTypedEndpoints")


@_attrs_define
class ExtractionJointConstraintTypedEndpoints:
    """
    Attributes:
        type_ (ExtractionJointConstraintTypedEndpointsType):
        relation (str | Unset):
        head_types (list[str] | Unset):
        tail_types (list[str] | Unset):
    """

    type_: ExtractionJointConstraintTypedEndpointsType
    relation: str | Unset = UNSET
    head_types: list[str] | Unset = UNSET
    tail_types: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        relation = self.relation

        head_types: list[str] | Unset = UNSET
        if not isinstance(self.head_types, Unset):
            head_types = self.head_types

        tail_types: list[str] | Unset = UNSET
        if not isinstance(self.tail_types, Unset):
            tail_types = self.tail_types

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
            }
        )
        if relation is not UNSET:
            field_dict["relation"] = relation
        if head_types is not UNSET:
            field_dict["head_types"] = head_types
        if tail_types is not UNSET:
            field_dict["tail_types"] = tail_types

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = ExtractionJointConstraintTypedEndpointsType(d.pop("type"))

        relation = d.pop("relation", UNSET)

        head_types = cast(list[str], d.pop("head_types", UNSET))

        tail_types = cast(list[str], d.pop("tail_types", UNSET))

        extraction_joint_constraint_typed_endpoints = cls(
            type_=type_,
            relation=relation,
            head_types=head_types,
            tail_types=tail_types,
        )

        return extraction_joint_constraint_typed_endpoints
