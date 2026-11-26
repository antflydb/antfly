from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="DependencyImpactRowViaItem")


@_attrs_define
class DependencyImpactRowViaItem:
    """
    Attributes:
        table (str | Unset):
        key (str | Unset):
        artifact (str | Unset):
    """

    table: str | Unset = UNSET
    key: str | Unset = UNSET
    artifact: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        table = self.table

        key = self.key

        artifact = self.artifact

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if table is not UNSET:
            field_dict["table"] = table
        if key is not UNSET:
            field_dict["key"] = key
        if artifact is not UNSET:
            field_dict["artifact"] = artifact

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        table = d.pop("table", UNSET)

        key = d.pop("key", UNSET)

        artifact = d.pop("artifact", UNSET)

        dependency_impact_row_via_item = cls(
            table=table,
            key=key,
            artifact=artifact,
        )

        dependency_impact_row_via_item.additional_properties = d
        return dependency_impact_row_via_item

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
