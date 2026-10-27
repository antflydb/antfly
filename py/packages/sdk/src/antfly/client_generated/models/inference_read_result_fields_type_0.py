from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="InferenceReadResultFieldsType0")


@_attrs_define
class InferenceReadResultFieldsType0:
    """Structured fields extracted by document understanding models (Donut, Florence-2).
    Fields are flattened with dot notation for nested structures.
    Only present for models that output structured data.

        Example:
            {'menu.nm': 'Coffee', 'menu.price': '$3.50', 'total': '$123.45'}

    """

    additional_properties: dict[str, str] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        inference_read_result_fields_type_0 = cls()

        inference_read_result_fields_type_0.additional_properties = d
        return inference_read_result_fields_type_0

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> str:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: str) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
