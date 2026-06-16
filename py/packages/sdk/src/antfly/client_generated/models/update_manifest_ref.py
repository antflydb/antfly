from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="UpdateManifestRef")


@_attrs_define
class UpdateManifestRef:
    """
    Attributes:
        from_version (str):  Example: 1.0.0.
        to_version (str):  Example: 1.1.0.
        path (str):  Example: updates/1.0.0-1.1.0.json.
        digest (str | Unset):  Example: sha256:def456.
    """

    from_version: str
    to_version: str
    path: str
    digest: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from_version = self.from_version

        to_version = self.to_version

        path = self.path

        digest = self.digest

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "from_version": from_version,
                "to_version": to_version,
                "path": path,
            }
        )
        if digest is not UNSET:
            field_dict["digest"] = digest

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        from_version = d.pop("from_version")

        to_version = d.pop("to_version")

        path = d.pop("path")

        digest = d.pop("digest", UNSET)

        update_manifest_ref = cls(
            from_version=from_version,
            to_version=to_version,
            path=path,
            digest=digest,
        )

        update_manifest_ref.additional_properties = d
        return update_manifest_ref

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
