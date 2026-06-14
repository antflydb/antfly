from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.drop_extension_request_mode import DropExtensionRequestMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="DropExtensionRequest")


@_attrs_define
class DropExtensionRequest:
    """
    Attributes:
        mode (DropExtensionRequestMode | Unset):  Default: DropExtensionRequestMode.RESTRICT.
        dry_run (bool | Unset):  Default: False.
    """

    mode: DropExtensionRequestMode | Unset = DropExtensionRequestMode.RESTRICT
    dry_run: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        dry_run = self.dry_run

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if mode is not UNSET:
            field_dict["mode"] = mode
        if dry_run is not UNSET:
            field_dict["dry_run"] = dry_run

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        _mode = d.pop("mode", UNSET)
        mode: DropExtensionRequestMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = DropExtensionRequestMode(_mode)

        dry_run = d.pop("dry_run", UNSET)

        drop_extension_request = cls(
            mode=mode,
            dry_run=dry_run,
        )

        drop_extension_request.additional_properties = d
        return drop_extension_request

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
