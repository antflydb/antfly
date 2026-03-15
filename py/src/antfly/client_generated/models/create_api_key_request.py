from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.permission import Permission


T = TypeVar("T", bound="CreateApiKeyRequest")


@_attrs_define
class CreateApiKeyRequest:
    """Request to create a new API key.

    Attributes:
        name (str): Human-readable name for the API key. Example: CI pipeline key.
        expires_in (Union[Unset, str]): Duration until expiration (e.g., '720h' for 30 days). Empty means never.
            Example: 720h.
        permissions (Union[None, Unset, list['Permission']]): Optional permission scoping. Each permission must be a
            subset of the creator's permissions.
    """

    name: str
    expires_in: Union[Unset, str] = UNSET
    permissions: Union[None, Unset, list["Permission"]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        expires_in = self.expires_in

        permissions: Union[None, Unset, list[dict[str, Any]]]
        if isinstance(self.permissions, Unset):
            permissions = UNSET
        elif isinstance(self.permissions, list):
            permissions = []
            for permissions_type_0_item_data in self.permissions:
                permissions_type_0_item = permissions_type_0_item_data.to_dict()
                permissions.append(permissions_type_0_item)

        else:
            permissions = self.permissions

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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.permission import Permission

        d = dict(src_dict)
        name = d.pop("name")

        expires_in = d.pop("expires_in", UNSET)

        def _parse_permissions(data: object) -> Union[None, Unset, list["Permission"]]:
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
            except:  # noqa: E722
                pass
            return cast(Union[None, Unset, list["Permission"]], data)

        permissions = _parse_permissions(d.pop("permissions", UNSET))

        create_api_key_request = cls(
            name=name,
            expires_in=expires_in,
            permissions=permissions,
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
