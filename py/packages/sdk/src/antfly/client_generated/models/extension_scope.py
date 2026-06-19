from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extension_scope_kind import ExtensionScopeKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtensionScope")


@_attrs_define
class ExtensionScope:
    """
    Attributes:
        kind (ExtensionScopeKind):
        table_name (str | Unset): Required when kind is table.
    """

    kind: ExtensionScopeKind
    table_name: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        table_name = self.table_name

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
            }
        )
        if table_name is not UNSET:
            field_dict["table_name"] = table_name

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = ExtensionScopeKind(d.pop("kind"))

        table_name = d.pop("table_name", UNSET)

        extension_scope = cls(
            kind=kind,
            table_name=table_name,
        )

        extension_scope.additional_properties = d
        return extension_scope

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
