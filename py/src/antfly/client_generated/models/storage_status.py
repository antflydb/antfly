from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="StorageStatus")


@_attrs_define
class StorageStatus:
    """
    Attributes:
        disk_usage (Union[Unset, int]): Disk usage in bytes.
        empty (Union[Unset, bool]): Whether the table has received data.
    """

    disk_usage: Union[Unset, int] = UNSET
    empty: Union[Unset, bool] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        disk_usage = self.disk_usage

        empty = self.empty

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if disk_usage is not UNSET:
            field_dict["disk_usage"] = disk_usage
        if empty is not UNSET:
            field_dict["empty"] = empty

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        disk_usage = d.pop("disk_usage", UNSET)

        empty = d.pop("empty", UNSET)

        storage_status = cls(
            disk_usage=disk_usage,
            empty=empty,
        )

        storage_status.additional_properties = d
        return storage_status

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
