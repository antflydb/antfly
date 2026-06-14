from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.package_kind import PackageKind
from ..models.package_manifest_manifest_api_version import PackageManifestManifestApiVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.capability import Capability
    from ..models.install_manifest import InstallManifest
    from ..models.package_artifact import PackageArtifact
    from ..models.package_dependency import PackageDependency


T = TypeVar("T", bound="PackageManifest")


@_attrs_define
class PackageManifest:
    """
    Attributes:
        manifest_api_version (PackageManifestManifestApiVersion):
        name (str):
        version (str):
        kind (PackageKind):
        install (InstallManifest):
        description (str | Unset):
        digest (str | Unset):
        trusted (bool | Unset):  Default: False.
        relocatable (bool | Unset):  Default: False.
        capabilities_requested (list[Capability] | Unset):
        dependencies (list[PackageDependency] | Unset):
        artifacts (list[PackageArtifact] | Unset):
    """

    manifest_api_version: PackageManifestManifestApiVersion
    name: str
    version: str
    kind: PackageKind
    install: InstallManifest
    description: str | Unset = UNSET
    digest: str | Unset = UNSET
    trusted: bool | Unset = False
    relocatable: bool | Unset = False
    capabilities_requested: list[Capability] | Unset = UNSET
    dependencies: list[PackageDependency] | Unset = UNSET
    artifacts: list[PackageArtifact] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        manifest_api_version = self.manifest_api_version.value

        name = self.name

        version = self.version

        kind = self.kind.value

        install = self.install.to_dict()

        description = self.description

        digest = self.digest

        trusted = self.trusted

        relocatable = self.relocatable

        capabilities_requested: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.capabilities_requested, Unset):
            capabilities_requested = []
            for capabilities_requested_item_data in self.capabilities_requested:
                capabilities_requested_item = capabilities_requested_item_data.to_dict()
                capabilities_requested.append(capabilities_requested_item)

        dependencies: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.dependencies, Unset):
            dependencies = []
            for dependencies_item_data in self.dependencies:
                dependencies_item = dependencies_item_data.to_dict()
                dependencies.append(dependencies_item)

        artifacts: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.artifacts, Unset):
            artifacts = []
            for artifacts_item_data in self.artifacts:
                artifacts_item = artifacts_item_data.to_dict()
                artifacts.append(artifacts_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "manifest_api_version": manifest_api_version,
                "name": name,
                "version": version,
                "kind": kind,
                "install": install,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if digest is not UNSET:
            field_dict["digest"] = digest
        if trusted is not UNSET:
            field_dict["trusted"] = trusted
        if relocatable is not UNSET:
            field_dict["relocatable"] = relocatable
        if capabilities_requested is not UNSET:
            field_dict["capabilities_requested"] = capabilities_requested
        if dependencies is not UNSET:
            field_dict["dependencies"] = dependencies
        if artifacts is not UNSET:
            field_dict["artifacts"] = artifacts

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.capability import Capability
        from ..models.install_manifest import InstallManifest
        from ..models.package_artifact import PackageArtifact
        from ..models.package_dependency import PackageDependency

        d = dict(src_dict)
        manifest_api_version = PackageManifestManifestApiVersion(d.pop("manifest_api_version"))

        name = d.pop("name")

        version = d.pop("version")

        kind = PackageKind(d.pop("kind"))

        install = InstallManifest.from_dict(d.pop("install"))

        description = d.pop("description", UNSET)

        digest = d.pop("digest", UNSET)

        trusted = d.pop("trusted", UNSET)

        relocatable = d.pop("relocatable", UNSET)

        _capabilities_requested = d.pop("capabilities_requested", UNSET)
        capabilities_requested: list[Capability] | Unset = UNSET
        if _capabilities_requested is not UNSET:
            capabilities_requested = []
            for capabilities_requested_item_data in _capabilities_requested:
                capabilities_requested_item = Capability.from_dict(capabilities_requested_item_data)

                capabilities_requested.append(capabilities_requested_item)

        _dependencies = d.pop("dependencies", UNSET)
        dependencies: list[PackageDependency] | Unset = UNSET
        if _dependencies is not UNSET:
            dependencies = []
            for dependencies_item_data in _dependencies:
                dependencies_item = PackageDependency.from_dict(dependencies_item_data)

                dependencies.append(dependencies_item)

        _artifacts = d.pop("artifacts", UNSET)
        artifacts: list[PackageArtifact] | Unset = UNSET
        if _artifacts is not UNSET:
            artifacts = []
            for artifacts_item_data in _artifacts:
                artifacts_item = PackageArtifact.from_dict(artifacts_item_data)

                artifacts.append(artifacts_item)

        package_manifest = cls(
            manifest_api_version=manifest_api_version,
            name=name,
            version=version,
            kind=kind,
            install=install,
            description=description,
            digest=digest,
            trusted=trusted,
            relocatable=relocatable,
            capabilities_requested=capabilities_requested,
            dependencies=dependencies,
            artifacts=artifacts,
        )

        package_manifest.additional_properties = d
        return package_manifest

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
