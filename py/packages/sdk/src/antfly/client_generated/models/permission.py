from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.permission_type import PermissionType
from ..models.resource_type import ResourceType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.catalog_table_scope import CatalogTableScope


T = TypeVar("T", bound="Permission")


@_attrs_define
class Permission:
    """Specify exactly one of a legacy literal resource or a structured table_target; table_target requires resource_type
    table.

        Attributes:
            resource_type (ResourceType): Type of resource: table, user, inference, or global ('*'). Use inference with
                resource '*' to grant access to unified inference routes. Example: table.
            type_ (PermissionType): Type of permission. Example: read.
            resource (str | Unset): Resource name (e.g., table name, target username, or '*' for all inference operations or
                a global grant). Example: orders_table.
            table_target (CatalogTableScope | Unset): A table or all tables in an explicit namespace. A missing table
                selects the namespace; a table named '*' remains literal.
    """

    resource_type: ResourceType
    type_: PermissionType
    resource: str | Unset = UNSET
    table_target: CatalogTableScope | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resource_type = self.resource_type.value

        type_ = self.type_.value

        resource = self.resource

        table_target: dict[str, Any] | Unset = UNSET
        if not isinstance(self.table_target, Unset):
            table_target = self.table_target.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "resource_type": resource_type,
                "type": type_,
            }
        )
        if resource is not UNSET:
            field_dict["resource"] = resource
        if table_target is not UNSET:
            field_dict["table_target"] = table_target

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.catalog_table_scope import CatalogTableScope

        d = dict(src_dict)
        resource_type = ResourceType(d.pop("resource_type"))

        type_ = PermissionType(d.pop("type"))

        resource = d.pop("resource", UNSET)

        _table_target = d.pop("table_target", UNSET)
        table_target: CatalogTableScope | Unset
        if isinstance(_table_target, Unset):
            table_target = UNSET
        else:
            table_target = CatalogTableScope.from_dict(_table_target)

        permission = cls(
            resource_type=resource_type,
            type_=type_,
            resource=resource,
            table_target=table_target,
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
