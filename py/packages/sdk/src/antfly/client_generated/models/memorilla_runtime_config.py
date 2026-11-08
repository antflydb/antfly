from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.memorilla_profile_config import MemorillaProfileConfig


T = TypeVar("T", bound="MemorillaRuntimeConfig")


@_attrs_define
class MemorillaRuntimeConfig:
    """
    Attributes:
        enabled (bool | Unset):  Default: False.
        profiles (list[MemorillaProfileConfig] | Unset):
    """

    enabled: bool | Unset = False
    profiles: list[MemorillaProfileConfig] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        profiles: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.profiles, Unset):
            profiles = []
            for profiles_item_data in self.profiles:
                profiles_item = profiles_item_data.to_dict()
                profiles.append(profiles_item)

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if profiles is not UNSET:
            field_dict["profiles"] = profiles

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memorilla_profile_config import MemorillaProfileConfig

        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        _profiles = d.pop("profiles", UNSET)
        profiles: list[MemorillaProfileConfig] | Unset = UNSET
        if _profiles is not UNSET:
            profiles = []
            for profiles_item_data in _profiles:
                profiles_item = MemorillaProfileConfig.from_dict(profiles_item_data)

                profiles.append(profiles_item)

        memorilla_runtime_config = cls(
            enabled=enabled,
            profiles=profiles,
        )

        return memorilla_runtime_config
