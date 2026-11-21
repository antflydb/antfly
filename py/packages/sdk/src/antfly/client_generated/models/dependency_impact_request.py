from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.dependency_impact_request_traversal import DependencyImpactRequestTraversal
from ..types import UNSET, Unset

T = TypeVar("T", bound="DependencyImpactRequest")


@_attrs_define
class DependencyImpactRequest:
    """
    Attributes:
        key (str): Source document key whose dependents are requested.
        revision (str | Unset): Optional hex input revision; only outputs that consumed that revision are returned.
        traversal (DependencyImpactRequestTraversal | Unset): `transitive` also lists same-document artifacts that
            consume each dependent (live candidates with their path). Default: DependencyImpactRequestTraversal.DIRECT.
        max_depth (int | Unset):  Default: 8.
        limit (int | Unset):  Default: 100.
    """

    key: str
    revision: str | Unset = UNSET
    traversal: DependencyImpactRequestTraversal | Unset = DependencyImpactRequestTraversal.DIRECT
    max_depth: int | Unset = 8
    limit: int | Unset = 100
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key = self.key

        revision = self.revision

        traversal: str | Unset = UNSET
        if not isinstance(self.traversal, Unset):
            traversal = self.traversal.value

        max_depth = self.max_depth

        limit = self.limit

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key": key,
            }
        )
        if revision is not UNSET:
            field_dict["revision"] = revision
        if traversal is not UNSET:
            field_dict["traversal"] = traversal
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        key = d.pop("key")

        revision = d.pop("revision", UNSET)

        _traversal = d.pop("traversal", UNSET)
        traversal: DependencyImpactRequestTraversal | Unset
        if isinstance(_traversal, Unset):
            traversal = UNSET
        else:
            traversal = DependencyImpactRequestTraversal(_traversal)

        max_depth = d.pop("max_depth", UNSET)

        limit = d.pop("limit", UNSET)

        dependency_impact_request = cls(
            key=key,
            revision=revision,
            traversal=traversal,
            max_depth=max_depth,
            limit=limit,
        )

        dependency_impact_request.additional_properties = d
        return dependency_impact_request

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
