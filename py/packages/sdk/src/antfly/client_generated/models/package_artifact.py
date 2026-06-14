from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.package_artifact_kind import PackageArtifactKind
from ..types import UNSET, Unset

T = TypeVar("T", bound="PackageArtifact")


@_attrs_define
class PackageArtifact:
    """
    Attributes:
        kind (PackageArtifactKind):
        path (str):  Example: extension.json.
        digest (str | Unset):  Example: sha256:abc123.
    """

    kind: PackageArtifactKind
    path: str
    digest: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        path = self.path

        digest = self.digest

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "kind": kind,
                "path": path,
            }
        )
        if digest is not UNSET:
            field_dict["digest"] = digest

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        kind = PackageArtifactKind(d.pop("kind"))

        path = d.pop("path")

        digest = d.pop("digest", UNSET)

        package_artifact = cls(
            kind=kind,
            path=path,
            digest=digest,
        )

        package_artifact.additional_properties = d
        return package_artifact

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
