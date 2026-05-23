from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.api_key_row_filter_type_0 import ApiKeyRowFilterType0
    from ..models.permission import Permission


T = TypeVar("T", bound="ApiKeyWithSecret")


@_attrs_define
class ApiKeyWithSecret:
    """API key creation response including the cleartext secret (shown once).

    Attributes:
        key_id (str): Unique identifier for the API key. Example: aBcDeFgHiJkLmNoPqRsT.
        name (str): Human-readable name for the API key. Example: CI pipeline key.
        username (str): Owner of the API key. Example: johndoe.
        created_at (datetime.datetime): When the API key was created.
        key_secret (str): Cleartext secret for the API key. Store securely — it cannot be retrieved again. Example:
            dGhpcyBpcyBhIHNlY3JldA.
        encoded (str): Pre-encoded credential ready for the Authorization header: base64(key_id:key_secret). Example:
            YUJjRGVGZ0hpSmtMbU5vUHFSc1Q6ZEdocGN5QnBjeUJoSUhObFkzSmxkQQ==.
        permissions (list[Permission] | None | Unset): Optional permission scoping. If empty, inherits owner's full
            permissions.
        row_filter (ApiKeyRowFilterType0 | None | Unset): Optional per-table row filter. Keys are table names (or '*'
            for all tables). Values are bleve query JSON objects. API keys inherit the owner's effective row filters; key-
            local filters are applied as additional narrowing.
        expires_at (datetime.datetime | None | Unset): When the API key expires. Null means never.
    """

    key_id: str
    name: str
    username: str
    created_at: datetime.datetime
    key_secret: str
    encoded: str
    permissions: list[Permission] | None | Unset = UNSET
    row_filter: ApiKeyRowFilterType0 | None | Unset = UNSET
    expires_at: datetime.datetime | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.api_key_row_filter_type_0 import ApiKeyRowFilterType0

        key_id = self.key_id

        name = self.name

        username = self.username

        created_at = self.created_at.isoformat()

        key_secret = self.key_secret

        encoded = self.encoded

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
        elif isinstance(self.row_filter, ApiKeyRowFilterType0):
            row_filter = self.row_filter.to_dict()
        else:
            row_filter = self.row_filter

        expires_at: None | str | Unset
        if isinstance(self.expires_at, Unset):
            expires_at = UNSET
        elif isinstance(self.expires_at, datetime.datetime):
            expires_at = self.expires_at.isoformat()
        else:
            expires_at = self.expires_at

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key_id": key_id,
                "name": name,
                "username": username,
                "created_at": created_at,
                "key_secret": key_secret,
                "encoded": encoded,
            }
        )
        if permissions is not UNSET:
            field_dict["permissions"] = permissions
        if row_filter is not UNSET:
            field_dict["row_filter"] = row_filter
        if expires_at is not UNSET:
            field_dict["expires_at"] = expires_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.api_key_row_filter_type_0 import ApiKeyRowFilterType0
        from ..models.permission import Permission

        d = dict(src_dict)
        key_id = d.pop("key_id")

        name = d.pop("name")

        username = d.pop("username")

        created_at = isoparse(d.pop("created_at"))

        key_secret = d.pop("key_secret")

        encoded = d.pop("encoded")

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

        def _parse_row_filter(data: object) -> ApiKeyRowFilterType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                row_filter_type_0 = ApiKeyRowFilterType0.from_dict(data)

                return row_filter_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(ApiKeyRowFilterType0 | None | Unset, data)

        row_filter = _parse_row_filter(d.pop("row_filter", UNSET))

        def _parse_expires_at(data: object) -> datetime.datetime | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, str):
                    raise TypeError()
                expires_at_type_0 = isoparse(data)

                return expires_at_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(datetime.datetime | None | Unset, data)

        expires_at = _parse_expires_at(d.pop("expires_at", UNSET))

        api_key_with_secret = cls(
            key_id=key_id,
            name=name,
            username=username,
            created_at=created_at,
            key_secret=key_secret,
            encoded=encoded,
            permissions=permissions,
            row_filter=row_filter,
            expires_at=expires_at,
        )

        api_key_with_secret.additional_properties = d
        return api_key_with_secret

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
