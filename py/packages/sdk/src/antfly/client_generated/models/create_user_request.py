from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.create_user_request_metadata_type_0 import CreateUserRequestMetadataType0
    from ..models.permission import Permission


T = TypeVar("T", bound="CreateUserRequest")


@_attrs_define
class CreateUserRequest:
    """
    Attributes:
        password (str):  Example: s3cr3tP@sswOrd.
        username (str | Unset): Username for the new user. If provided in the path, this field can be omitted or must
            match the path parameter. Example: johndoe.
        initial_policies (list[Permission] | None | Unset): Optional list of initial permissions for the user.
        metadata (CreateUserRequestMetadataType0 | None | Unset): Auth metadata available to stored row-filter policies.
            Example: {'tenant_id': 'acme'}.
    """

    password: str
    username: str | Unset = UNSET
    initial_policies: list[Permission] | None | Unset = UNSET
    metadata: CreateUserRequestMetadataType0 | None | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.create_user_request_metadata_type_0 import CreateUserRequestMetadataType0

        password = self.password

        username = self.username

        initial_policies: list[dict[str, Any]] | None | Unset
        if isinstance(self.initial_policies, Unset):
            initial_policies = UNSET
        elif isinstance(self.initial_policies, list):
            initial_policies = []
            for initial_policies_type_0_item_data in self.initial_policies:
                initial_policies_type_0_item = initial_policies_type_0_item_data.to_dict()
                initial_policies.append(initial_policies_type_0_item)

        else:
            initial_policies = self.initial_policies

        metadata: dict[str, Any] | None | Unset
        if isinstance(self.metadata, Unset):
            metadata = UNSET
        elif isinstance(self.metadata, CreateUserRequestMetadataType0):
            metadata = self.metadata.to_dict()
        else:
            metadata = self.metadata

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "password": password,
            }
        )
        if username is not UNSET:
            field_dict["username"] = username
        if initial_policies is not UNSET:
            field_dict["initial_policies"] = initial_policies
        if metadata is not UNSET:
            field_dict["metadata"] = metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.create_user_request_metadata_type_0 import CreateUserRequestMetadataType0
        from ..models.permission import Permission

        d = dict(src_dict)
        password = d.pop("password")

        username = d.pop("username", UNSET)

        def _parse_initial_policies(data: object) -> list[Permission] | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                initial_policies_type_0 = []
                _initial_policies_type_0 = data
                for initial_policies_type_0_item_data in _initial_policies_type_0:
                    initial_policies_type_0_item = Permission.from_dict(initial_policies_type_0_item_data)

                    initial_policies_type_0.append(initial_policies_type_0_item)

                return initial_policies_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[Permission] | None | Unset, data)

        initial_policies = _parse_initial_policies(d.pop("initial_policies", UNSET))

        def _parse_metadata(data: object) -> CreateUserRequestMetadataType0 | None | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                metadata_type_0 = CreateUserRequestMetadataType0.from_dict(data)

                return metadata_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(CreateUserRequestMetadataType0 | None | Unset, data)

        metadata = _parse_metadata(d.pop("metadata", UNSET))

        create_user_request = cls(
            password=password,
            username=username,
            initial_policies=initial_policies,
            metadata=metadata,
        )

        create_user_request.additional_properties = d
        return create_user_request

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
