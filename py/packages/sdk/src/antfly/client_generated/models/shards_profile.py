from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ShardsProfile")


@_attrs_define
class ShardsProfile:
    """Shard-level execution statistics.

    Attributes:
        total (int | Unset): Total shards targeted by the query.
        successful (int | Unset): Shards that returned results successfully.
        failed (int | Unset): Shards that failed during execution.
    """

    total: int | Unset = UNSET
    successful: int | Unset = UNSET
    failed: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        total = self.total

        successful = self.successful

        failed = self.failed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if total is not UNSET:
            field_dict["total"] = total
        if successful is not UNSET:
            field_dict["successful"] = successful
        if failed is not UNSET:
            field_dict["failed"] = failed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        total = d.pop("total", UNSET)

        successful = d.pop("successful", UNSET)

        failed = d.pop("failed", UNSET)

        shards_profile = cls(
            total=total,
            successful=successful,
            failed=failed,
        )

        shards_profile.additional_properties = d
        return shards_profile

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
