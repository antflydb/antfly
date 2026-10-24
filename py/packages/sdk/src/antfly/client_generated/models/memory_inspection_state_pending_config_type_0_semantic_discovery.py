from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="MemoryInspectionStatePendingConfigType0SemanticDiscovery")


@_attrs_define
class MemoryInspectionStatePendingConfigType0SemanticDiscovery:
    """
    Attributes:
        enabled (bool | Unset):  Default: False.
        name_threshold (float | Unset):  Default: 0.8.
        view_threshold (float | Unset):  Default: 0.85.
        neighbors (int | Unset):  Default: 32.
    """

    enabled: bool | Unset = False
    name_threshold: float | Unset = 0.8
    view_threshold: float | Unset = 0.85
    neighbors: int | Unset = 32

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        name_threshold = self.name_threshold

        view_threshold = self.view_threshold

        neighbors = self.neighbors

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if name_threshold is not UNSET:
            field_dict["name_threshold"] = name_threshold
        if view_threshold is not UNSET:
            field_dict["view_threshold"] = view_threshold
        if neighbors is not UNSET:
            field_dict["neighbors"] = neighbors

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        name_threshold = d.pop("name_threshold", UNSET)

        view_threshold = d.pop("view_threshold", UNSET)

        neighbors = d.pop("neighbors", UNSET)

        memory_inspection_state_pending_config_type_0_semantic_discovery = cls(
            enabled=enabled,
            name_threshold=name_threshold,
            view_threshold=view_threshold,
            neighbors=neighbors,
        )

        return memory_inspection_state_pending_config_type_0_semantic_discovery
