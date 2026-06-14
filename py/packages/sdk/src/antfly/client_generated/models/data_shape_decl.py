from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.data_shape_kind import DataShapeKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="DataShapeDecl")


@_attrs_define
class DataShapeDecl:
    """
    Attributes:
        name (str): Path-safe extension or package identifier.
        kind (DataShapeKind):
        version (str | Unset):  Default: '1'.
        schema_json (str | Unset): JSON object encoded as a string until the public shape language is finalized.
            Default: '{}'.
    """

    name: str
    kind: DataShapeKind
    version: str | Unset = "1"
    schema_json: str | Unset = "{}"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind = self.kind.value

        version = self.version

        schema_json = self.schema_json

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "kind": kind,
            }
        )
        if version is not UNSET:
            field_dict["version"] = version
        if schema_json is not UNSET:
            field_dict["schema_json"] = schema_json

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        kind = DataShapeKind(d.pop("kind"))

        version = d.pop("version", UNSET)

        schema_json = d.pop("schema_json", UNSET)

        data_shape_decl = cls(
            name=name,
            kind=kind,
            version=version,
            schema_json=schema_json,
        )

        data_shape_decl.additional_properties = d
        return data_shape_decl

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
