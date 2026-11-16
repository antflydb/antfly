from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResourcesConfigMemory")


@_attrs_define
class ResourcesConfigMemory:
    """
    Attributes:
        max_bytes (int | Unset): Whole-process memory envelope; zero selects automatic cgroup or host detection.
            Physical capacity always clamps this value.
    """

    max_bytes: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        max_bytes = self.max_bytes

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_bytes is not UNSET:
            field_dict["max_bytes"] = max_bytes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_bytes = d.pop("max_bytes", UNSET)

        resources_config_memory = cls(
            max_bytes=max_bytes,
        )

        return resources_config_memory
