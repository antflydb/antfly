from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.runtime_decl_mode import RuntimeDeclMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="RuntimeDecl")


@_attrs_define
class RuntimeDecl:
    """
    Attributes:
        name (str): Path-safe extension or package identifier.
        mode (RuntimeDeclMode | Unset):  Default: RuntimeDeclMode.MANIFEST_ONLY.
        artifact (str | Unset):
        config_json (str | Unset):  Default: '{}'.
    """

    name: str
    mode: RuntimeDeclMode | Unset = RuntimeDeclMode.MANIFEST_ONLY
    artifact: str | Unset = UNSET
    config_json: str | Unset = "{}"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        mode: str | Unset = UNSET
        if not isinstance(self.mode, Unset):
            mode = self.mode.value

        artifact = self.artifact

        config_json = self.config_json

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if mode is not UNSET:
            field_dict["mode"] = mode
        if artifact is not UNSET:
            field_dict["artifact"] = artifact
        if config_json is not UNSET:
            field_dict["config_json"] = config_json

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        _mode = d.pop("mode", UNSET)
        mode: RuntimeDeclMode | Unset
        if isinstance(_mode, Unset):
            mode = UNSET
        else:
            mode = RuntimeDeclMode(_mode)

        artifact = d.pop("artifact", UNSET)

        config_json = d.pop("config_json", UNSET)

        runtime_decl = cls(
            name=name,
            mode=mode,
            artifact=artifact,
            config_json=config_json,
        )

        runtime_decl.additional_properties = d
        return runtime_decl

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
