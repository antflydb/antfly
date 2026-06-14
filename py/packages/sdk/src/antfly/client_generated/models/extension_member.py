from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.data_shape_kind import DataShapeKind
from ..models.extension_object_kind import ExtensionObjectKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.extension_scope import ExtensionScope


T = TypeVar("T", bound="ExtensionMember")


@_attrs_define
class ExtensionMember:
    """
    Attributes:
        extension_name (str): Path-safe extension or package identifier.
        scope (ExtensionScope):
        object_kind (ExtensionObjectKind):
        object_name (str): Path-safe extension or package identifier.
        table_name (str | Unset):
        shape_kind (DataShapeKind | Unset):
        shape_name (str | Unset):
        shape_version (str | Unset):
        owner_metadata_json (str | Unset):  Default: '{}'.
    """

    extension_name: str
    scope: ExtensionScope
    object_kind: ExtensionObjectKind
    object_name: str
    table_name: str | Unset = UNSET
    shape_kind: DataShapeKind | Unset = UNSET
    shape_name: str | Unset = UNSET
    shape_version: str | Unset = UNSET
    owner_metadata_json: str | Unset = "{}"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        extension_name = self.extension_name

        scope = self.scope.to_dict()

        object_kind = self.object_kind.value

        object_name = self.object_name

        table_name = self.table_name

        shape_kind: str | Unset = UNSET
        if not isinstance(self.shape_kind, Unset):
            shape_kind = self.shape_kind.value

        shape_name = self.shape_name

        shape_version = self.shape_version

        owner_metadata_json = self.owner_metadata_json

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "extension_name": extension_name,
                "scope": scope,
                "object_kind": object_kind,
                "object_name": object_name,
            }
        )
        if table_name is not UNSET:
            field_dict["table_name"] = table_name
        if shape_kind is not UNSET:
            field_dict["shape_kind"] = shape_kind
        if shape_name is not UNSET:
            field_dict["shape_name"] = shape_name
        if shape_version is not UNSET:
            field_dict["shape_version"] = shape_version
        if owner_metadata_json is not UNSET:
            field_dict["owner_metadata_json"] = owner_metadata_json

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.extension_scope import ExtensionScope

        d = dict(src_dict)
        extension_name = d.pop("extension_name")

        scope = ExtensionScope.from_dict(d.pop("scope"))

        object_kind = ExtensionObjectKind(d.pop("object_kind"))

        object_name = d.pop("object_name")

        table_name = d.pop("table_name", UNSET)

        _shape_kind = d.pop("shape_kind", UNSET)
        shape_kind: DataShapeKind | Unset
        if isinstance(_shape_kind, Unset):
            shape_kind = UNSET
        else:
            shape_kind = DataShapeKind(_shape_kind)

        shape_name = d.pop("shape_name", UNSET)

        shape_version = d.pop("shape_version", UNSET)

        owner_metadata_json = d.pop("owner_metadata_json", UNSET)

        extension_member = cls(
            extension_name=extension_name,
            scope=scope,
            object_kind=object_kind,
            object_name=object_name,
            table_name=table_name,
            shape_kind=shape_kind,
            shape_name=shape_name,
            shape_version=shape_version,
            owner_metadata_json=owner_metadata_json,
        )

        extension_member.additional_properties = d
        return extension_member

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
