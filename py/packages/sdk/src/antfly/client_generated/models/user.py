from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.user_metadata_type_0 import UserMetadataType0


T = TypeVar("T", bound="User")


@_attrs_define
class User:
    """
    Attributes:
        username (str):  Example: johndoe.
        password_hash (str): Base64 encoded password hash. Exposing this is a security risk. Example: JGFyZ29uMm....
        metadata (None | Unset | UserMetadataType0): Server-side auth metadata available to stored row-filter policies
            through $auth metadata paths. Example: {'tenant_id': 'acme', 'department': 'eng'}.
    """

    username: str
    password_hash: str
    metadata: None | Unset | UserMetadataType0 = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.user_metadata_type_0 import UserMetadataType0

        username = self.username

        password_hash = self.password_hash

        metadata: dict[str, Any] | None | Unset
        if isinstance(self.metadata, Unset):
            metadata = UNSET
        elif isinstance(self.metadata, UserMetadataType0):
            metadata = self.metadata.to_dict()
        else:
            metadata = self.metadata

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "username": username,
                "password_hash": password_hash,
            }
        )
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.user_metadata_type_0 import UserMetadataType0

        d = dict(src_dict)
        username = d.pop("username")

        password_hash = d.pop("password_hash")

        def _parse_metadata(data: object) -> None | Unset | UserMetadataType0:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                metadata_type_0 = UserMetadataType0.from_dict(data)

                return metadata_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | Unset | UserMetadataType0, data)

        metadata = _parse_metadata(d.pop("metadata", UNSET))

        user = cls(
            username=username,
            password_hash=password_hash,
            metadata=metadata,
        )

        user.additional_properties = d
        return user

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
