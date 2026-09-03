from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.linear_merge_request_records_additional_property import LinearMergeRequestRecordsAdditionalProperty


T = TypeVar("T", bound="LinearMergeRequestRecords")


@_attrs_define
class LinearMergeRequestRecords:
    """Map of resource ID to resource object: {"resource_id_1": {...}, "resource_id_2": {...}}

    Requirements:
    - The server processes keys in lexicographic order
    - Use consistent key naming (e.g., all start with same prefix)

    This format avoids duplicate IDs and matches Antfly's batch write interface.

        Example:
            {'product:001': {'name': 'Laptop', 'price': 999.99}, 'product:002': {'name': 'Mouse', 'price': 29.99},
                'product:003': {'name': 'Keyboard', 'price': 79.99}}

    """

    additional_properties: dict[str, LinearMergeRequestRecordsAdditionalProperty] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.linear_merge_request_records_additional_property import (
            LinearMergeRequestRecordsAdditionalProperty,
        )

        d = dict(src_dict)
        linear_merge_request_records = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = LinearMergeRequestRecordsAdditionalProperty.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        linear_merge_request_records.additional_properties = additional_properties
        return linear_merge_request_records

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> LinearMergeRequestRecordsAdditionalProperty:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: LinearMergeRequestRecordsAdditionalProperty) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
