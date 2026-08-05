from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extraction_structure_field_type_1_type import ExtractionStructureFieldType1Type
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtractionStructureFieldType1")


@_attrs_define
class ExtractionStructureFieldType1:
    """
    Attributes:
        type_ (ExtractionStructureFieldType1Type | Unset):
        enum (list[str] | Unset):
    """

    type_: ExtractionStructureFieldType1Type | Unset = UNSET
    enum: list[str] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        enum: list[str] | Unset = UNSET
        if not isinstance(self.enum, Unset):
            enum = self.enum

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if type_ is not UNSET:
            field_dict["type"] = type_
        if enum is not UNSET:
            field_dict["enum"] = enum

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _type_ = d.pop("type", UNSET)
        type_: ExtractionStructureFieldType1Type | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = ExtractionStructureFieldType1Type(_type_)

        enum = cast(list[str], d.pop("enum", UNSET))

        extraction_structure_field_type_1 = cls(
            type_=type_,
            enum=enum,
        )

        extraction_structure_field_type_1.additional_properties = d
        return extraction_structure_field_type_1

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
