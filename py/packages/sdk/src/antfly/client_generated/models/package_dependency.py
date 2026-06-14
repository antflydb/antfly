from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="PackageDependency")


@_attrs_define
class PackageDependency:
    """
    Attributes:
        name (str): Path-safe extension or package identifier.
        version_requirement (str | Unset):  Example: >=1.0.0.
        optional (bool | Unset):  Default: False.
    """

    name: str
    version_requirement: str | Unset = UNSET
    optional: bool | Unset = False
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        version_requirement = self.version_requirement

        optional = self.optional

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if version_requirement is not UNSET:
            field_dict["version_requirement"] = version_requirement
        if optional is not UNSET:
            field_dict["optional"] = optional

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        version_requirement = d.pop("version_requirement", UNSET)

        optional = d.pop("optional", UNSET)

        package_dependency = cls(
            name=name,
            version_requirement=version_requirement,
            optional=optional,
        )

        package_dependency.additional_properties = d
        return package_dependency

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
