from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.permission_type import PermissionType
from ..models.resource_type import ResourceType

T = TypeVar("T", bound="Permission")


@_attrs_define
class Permission:
    """
    Attributes:
        resource (str): Resource name (e.g., table name, target username, or '*' for global). Example: orders_table.
        resource_type (ResourceType): Type of the resource, e.g., table, user, or global ('*'). Example: table.
        type_ (PermissionType): Type of permission. Example: read.
    """

    resource: str
    resource_type: ResourceType
    type_: PermissionType
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource = self.resource

        resource_type = self.resource_type.value

        type_ = self.type_.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "resource": resource,
                "resource_type": resource_type,
                "type": type_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        resource = d.pop("resource")

        resource_type = ResourceType(d.pop("resource_type"))

        type_ = PermissionType(d.pop("type"))

        permission = cls(
            resource=resource,
            resource_type=resource_type,
            type_=type_,
        )

        permission.additional_properties = d
        return permission

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
