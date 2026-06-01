from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.get_current_user_response_200_metadata_type_0 import GetCurrentUserResponse200MetadataType0
    from ..models.permission import Permission


T = TypeVar("T", bound="GetCurrentUserResponse200")


@_attrs_define
class GetCurrentUserResponse200:
    """
    Attributes:
        username (str | Unset):  Example: johndoe.
        permissions (list[Permission] | Unset):
        metadata (GetCurrentUserResponse200MetadataType0 | None | Unset):
    """

    username: str | Unset = UNSET
    permissions: list[Permission] | Unset = UNSET
    metadata: GetCurrentUserResponse200MetadataType0 | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.get_current_user_response_200_metadata_type_0 import GetCurrentUserResponse200MetadataType0

        username = self.username

        permissions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.permissions, Unset):
            permissions = []
            for permissions_item_data in self.permissions:
                permissions_item = permissions_item_data.to_dict()
                permissions.append(permissions_item)

        metadata: dict[str, Any] | None | Unset
        if isinstance(self.metadata, Unset):
            metadata = UNSET
        elif isinstance(self.metadata, GetCurrentUserResponse200MetadataType0):
            metadata = self.metadata.to_dict()
        else:
            metadata = self.metadata

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if username is not UNSET:
            field_dict["username"] = username
        if permissions is not UNSET:
            field_dict["permissions"] = permissions
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.get_current_user_response_200_metadata_type_0 import GetCurrentUserResponse200MetadataType0
        from ..models.permission import Permission

        d = dict(src_dict)
        username = d.pop("username", UNSET)

        _permissions = d.pop("permissions", UNSET)
        permissions: list[Permission] | Unset = UNSET
        if _permissions is not UNSET:
            permissions = []
            for permissions_item_data in _permissions:
                permissions_item = Permission.from_dict(permissions_item_data)

                permissions.append(permissions_item)

        def _parse_metadata(data: object) -> GetCurrentUserResponse200MetadataType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                metadata_type_0 = GetCurrentUserResponse200MetadataType0.from_dict(data)

                return metadata_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(GetCurrentUserResponse200MetadataType0 | None | Unset, data)

        metadata = _parse_metadata(d.pop("metadata", UNSET))

        get_current_user_response_200 = cls(
            username=username,
            permissions=permissions,
            metadata=metadata,
        )

        get_current_user_response_200.additional_properties = d
        return get_current_user_response_200

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
