from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.create_api_key_request_row_filter_type_0 import CreateApiKeyRequestRowFilterType0
    from ..models.permission import Permission


T = TypeVar("T", bound="CreateApiKeyRequest")


@_attrs_define
class CreateApiKeyRequest:
    """Request to create a new API key.

    Attributes:
        name (str): Human-readable name for the API key. Example: CI pipeline key.
        expires_in (str | Unset): Duration until expiration (e.g., '720h' for 30 days). Empty means never. Example:
            720h.
        permissions (list[Permission] | None | Unset): Optional permission scoping. Each permission must be a subset of
            the creator's permissions.
        row_filter (CreateApiKeyRequestRowFilterType0 | None | Unset): Optional per-table row filter. Keys are table
            names (or '*' for all tables). Values are bleve query JSON objects. API keys inherit the owner's effective row
            filters; key-local filters are applied as additional narrowing.
    """

    name: str
    expires_in: str | Unset = UNSET
    permissions: list[Permission] | None | Unset = UNSET
    row_filter: CreateApiKeyRequestRowFilterType0 | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.create_api_key_request_row_filter_type_0 import CreateApiKeyRequestRowFilterType0

        name = self.name

        expires_in = self.expires_in

        permissions: list[dict[str, Any]] | None | Unset
        if isinstance(self.permissions, Unset):
            permissions = UNSET
        elif isinstance(self.permissions, list):
            permissions = []
            for permissions_type_0_item_data in self.permissions:
                permissions_type_0_item = permissions_type_0_item_data.to_dict()
                permissions.append(permissions_type_0_item)

        else:
            permissions = self.permissions

        row_filter: dict[str, Any] | None | Unset
        if isinstance(self.row_filter, Unset):
            row_filter = UNSET
        elif isinstance(self.row_filter, CreateApiKeyRequestRowFilterType0):
            row_filter = self.row_filter.to_dict()
        else:
            row_filter = self.row_filter

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if expires_in is not UNSET:
            field_dict["expires_in"] = expires_in
        if permissions is not UNSET:
            field_dict["permissions"] = permissions
        if row_filter is not UNSET:
            field_dict["row_filter"] = row_filter

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.create_api_key_request_row_filter_type_0 import CreateApiKeyRequestRowFilterType0
        from ..models.permission import Permission

        d = dict(src_dict)
        name = d.pop("name")

        expires_in = d.pop("expires_in", UNSET)

        def _parse_permissions(data: object) -> list[Permission] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                permissions_type_0 = []
                _permissions_type_0 = data
                for permissions_type_0_item_data in _permissions_type_0:
                    permissions_type_0_item = Permission.from_dict(permissions_type_0_item_data)

                    permissions_type_0.append(permissions_type_0_item)

                return permissions_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[Permission] | None | Unset, data)

        permissions = _parse_permissions(d.pop("permissions", UNSET))

        def _parse_row_filter(data: object) -> CreateApiKeyRequestRowFilterType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                row_filter_type_0 = CreateApiKeyRequestRowFilterType0.from_dict(data)

                return row_filter_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(CreateApiKeyRequestRowFilterType0 | None | Unset, data)

        row_filter = _parse_row_filter(d.pop("row_filter", UNSET))

        create_api_key_request = cls(
            name=name,
            expires_in=expires_in,
            permissions=permissions,
            row_filter=row_filter,
        )

        create_api_key_request.additional_properties = d
        return create_api_key_request

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
