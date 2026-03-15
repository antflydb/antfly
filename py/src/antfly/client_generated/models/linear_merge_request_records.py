from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="LinearMergeRequestRecords")


@_attrs_define
class LinearMergeRequestRecords:
    """Map of resource ID to resource object: {"resource_id_1": {...}, "resource_id_2": {...}}

    Requirements:
    - Keys must be sorted lexicographically by your client
    - Server will process keys in sorted order
    - Use consistent key naming (e.g., all start with same prefix)

    This format avoids duplicate IDs and matches Antfly's batch write interface.

        Example:
            {'product:001': {'name': 'Laptop', 'price': 999.99}, 'product:002': {'name': 'Mouse', 'price': 29.99},
                'product:003': {'name': 'Keyboard', 'price': 79.99}}

    """

    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        linear_merge_request_records = cls()

        linear_merge_request_records.additional_properties = d
        return linear_merge_request_records

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
