from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="CreateTablespaceRequest")


@_attrs_define
class CreateTablespaceRequest:
    """Tablespace creation request. Placement policy is fail-closed until native placement planning consumes it.

    Attributes:
        location_json (str | Unset): JSON-encoded location descriptor. Defaults to `null`. Example:
            "/var/lib/antfly/fastspace".
        placement_policy_json (str | Unset): JSON-encoded placement policy. Only `{}` is accepted until native placement
            support lands. Example: {}.
    """

    location_json: str | Unset = UNSET
    placement_policy_json: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        location_json = self.location_json

        placement_policy_json = self.placement_policy_json

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if location_json is not UNSET:
            field_dict["location_json"] = location_json
        if placement_policy_json is not UNSET:
            field_dict["placement_policy_json"] = placement_policy_json

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        location_json = d.pop("location_json", UNSET)

        placement_policy_json = d.pop("placement_policy_json", UNSET)

        create_tablespace_request = cls(
            location_json=location_json,
            placement_policy_json=placement_policy_json,
        )

        create_tablespace_request.additional_properties = d
        return create_tablespace_request

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
