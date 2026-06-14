from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.extension_object_kind import ExtensionObjectKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="ExtensionObjectDecl")


@_attrs_define
class ExtensionObjectDecl:
    """
    Attributes:
        kind (ExtensionObjectKind):
        name (str): Path-safe extension or package identifier.
        shape (str | Unset):
        table_name (str | Unset):
        config_json (str | Unset):  Default: '{}'.
    """

    kind: ExtensionObjectKind
    name: str
    shape: str | Unset = UNSET
    table_name: str | Unset = UNSET
    config_json: str | Unset = "{}"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        name = self.name

        shape = self.shape

        table_name = self.table_name

        config_json = self.config_json

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
                "name": name,
            }
        )
        if shape is not UNSET:
            field_dict["shape"] = shape
        if table_name is not UNSET:
            field_dict["table_name"] = table_name
        if config_json is not UNSET:
            field_dict["config_json"] = config_json

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = ExtensionObjectKind(d.pop("kind"))

        name = d.pop("name")

        shape = d.pop("shape", UNSET)

        table_name = d.pop("table_name", UNSET)

        config_json = d.pop("config_json", UNSET)

        extension_object_decl = cls(
            kind=kind,
            name=name,
            shape=shape,
            table_name=table_name,
            config_json=config_json,
        )

        extension_object_decl.additional_properties = d
        return extension_object_decl

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
