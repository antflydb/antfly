from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.installed_extension_status import InstalledExtensionStatus
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.capability import Capability
    from ..models.extension_scope import ExtensionScope


T = TypeVar("T", bound="InstalledExtension")


@_attrs_define
class InstalledExtension:
    """
    Attributes:
        name (str): Path-safe extension or package identifier.
        package_name (str): Path-safe extension or package identifier.
        package_version (str):
        package_digest (str):
        scope (ExtensionScope):
        status (InstalledExtensionStatus):
        config_json (str | Unset):  Default: '{}'.
        granted_capabilities (list[Capability] | Unset):
        installed_at_epoch_ms (int | Unset):
    """

    name: str
    package_name: str
    package_version: str
    package_digest: str
    scope: ExtensionScope
    status: InstalledExtensionStatus
    config_json: str | Unset = "{}"
    granted_capabilities: list[Capability] | Unset = UNSET
    installed_at_epoch_ms: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        package_name = self.package_name

        package_version = self.package_version

        package_digest = self.package_digest

        scope = self.scope.to_dict()

        status = self.status.value

        config_json = self.config_json

        granted_capabilities: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.granted_capabilities, Unset):
            granted_capabilities = []
            for granted_capabilities_item_data in self.granted_capabilities:
                granted_capabilities_item = granted_capabilities_item_data.to_dict()
                granted_capabilities.append(granted_capabilities_item)

        installed_at_epoch_ms = self.installed_at_epoch_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "package_name": package_name,
                "package_version": package_version,
                "package_digest": package_digest,
                "scope": scope,
                "status": status,
            }
        )
        if config_json is not UNSET:
            field_dict["config_json"] = config_json
        if granted_capabilities is not UNSET:
            field_dict["granted_capabilities"] = granted_capabilities
        if installed_at_epoch_ms is not UNSET:
            field_dict["installed_at_epoch_ms"] = installed_at_epoch_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.capability import Capability
        from ..models.extension_scope import ExtensionScope

        d = dict(src_dict)
        name = d.pop("name")

        package_name = d.pop("package_name")

        package_version = d.pop("package_version")

        package_digest = d.pop("package_digest")

        scope = ExtensionScope.from_dict(d.pop("scope"))

        status = InstalledExtensionStatus(d.pop("status"))

        config_json = d.pop("config_json", UNSET)

        _granted_capabilities = d.pop("granted_capabilities", UNSET)
        granted_capabilities: list[Capability] | Unset = UNSET
        if _granted_capabilities is not UNSET:
            granted_capabilities = []
            for granted_capabilities_item_data in _granted_capabilities:
                granted_capabilities_item = Capability.from_dict(granted_capabilities_item_data)

                granted_capabilities.append(granted_capabilities_item)

        installed_at_epoch_ms = d.pop("installed_at_epoch_ms", UNSET)

        installed_extension = cls(
            name=name,
            package_name=package_name,
            package_version=package_version,
            package_digest=package_digest,
            scope=scope,
            status=status,
            config_json=config_json,
            granted_capabilities=granted_capabilities,
            installed_at_epoch_ms=installed_at_epoch_ms,
        )

        installed_extension.additional_properties = d
        return installed_extension

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
