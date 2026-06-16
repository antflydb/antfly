from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.capability import Capability
    from ..models.extension_scope import ExtensionScope


T = TypeVar("T", bound="InstallExtensionRequest")


@_attrs_define
class InstallExtensionRequest:
    """
    Attributes:
        scope (ExtensionScope):
        version (str | Unset):
        config_json (str | Unset):  Default: '{}'.
        grants (list[Capability] | Unset):
        dry_run (bool | Unset):  Default: False.
    """

    scope: ExtensionScope
    version: str | Unset = UNSET
    config_json: str | Unset = "{}"
    grants: list[Capability] | Unset = UNSET
    dry_run: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        scope = self.scope.to_dict()

        version = self.version

        config_json = self.config_json

        grants: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.grants, Unset):
            grants = []
            for grants_item_data in self.grants:
                grants_item = grants_item_data.to_dict()
                grants.append(grants_item)

        dry_run = self.dry_run

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "scope": scope,
            }
        )
        if version is not UNSET:
            field_dict["version"] = version
        if config_json is not UNSET:
            field_dict["config_json"] = config_json
        if grants is not UNSET:
            field_dict["grants"] = grants
        if dry_run is not UNSET:
            field_dict["dry_run"] = dry_run

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.capability import Capability
        from ..models.extension_scope import ExtensionScope

        d = dict(src_dict)
        scope = ExtensionScope.from_dict(d.pop("scope"))

        version = d.pop("version", UNSET)

        config_json = d.pop("config_json", UNSET)

        _grants = d.pop("grants", UNSET)
        grants: list[Capability] | Unset = UNSET
        if _grants is not UNSET:
            grants = []
            for grants_item_data in _grants:
                grants_item = Capability.from_dict(grants_item_data)

                grants.append(grants_item)

        dry_run = d.pop("dry_run", UNSET)

        install_extension_request = cls(
            scope=scope,
            version=version,
            config_json=config_json,
            grants=grants,
            dry_run=dry_run,
        )

        install_extension_request.additional_properties = d
        return install_extension_request

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
