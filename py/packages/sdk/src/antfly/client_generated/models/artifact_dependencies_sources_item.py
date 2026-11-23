from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.artifact_dependencies_sources_item_kind import ArtifactDependenciesSourcesItemKind
from ..models.artifact_dependencies_sources_item_tracking import ArtifactDependenciesSourcesItemTracking
from ..types import UNSET, Unset

T = TypeVar("T", bound="ArtifactDependenciesSourcesItem")


@_attrs_define
class ArtifactDependenciesSourcesItem:
    """
    Attributes:
        name (str | Unset):
        kind (ArtifactDependenciesSourcesItemKind | Unset):
        tracking (ArtifactDependenciesSourcesItemTracking | Unset):
        ranked (bool | Unset):
    """

    name: str | Unset = UNSET
    kind: ArtifactDependenciesSourcesItemKind | Unset = UNSET
    tracking: ArtifactDependenciesSourcesItemTracking | Unset = UNSET
    ranked: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        kind: str | Unset = UNSET
        if not isinstance(self.kind, Unset):
            kind = self.kind.value

        tracking: str | Unset = UNSET
        if not isinstance(self.tracking, Unset):
            tracking = self.tracking.value

        ranked = self.ranked

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if name is not UNSET:
            field_dict["name"] = name
        if kind is not UNSET:
            field_dict["kind"] = kind
        if tracking is not UNSET:
            field_dict["tracking"] = tracking
        if ranked is not UNSET:
            field_dict["ranked"] = ranked

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name", UNSET)

        _kind = d.pop("kind", UNSET)
        kind: ArtifactDependenciesSourcesItemKind | Unset
        if isinstance(_kind, Unset):
            kind = UNSET
        else:
            kind = ArtifactDependenciesSourcesItemKind(_kind)

        _tracking = d.pop("tracking", UNSET)
        tracking: ArtifactDependenciesSourcesItemTracking | Unset
        if isinstance(_tracking, Unset):
            tracking = UNSET
        else:
            tracking = ArtifactDependenciesSourcesItemTracking(_tracking)

        ranked = d.pop("ranked", UNSET)

        artifact_dependencies_sources_item = cls(
            name=name,
            kind=kind,
            tracking=tracking,
            ranked=ranked,
        )

        artifact_dependencies_sources_item.additional_properties = d
        return artifact_dependencies_sources_item

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
