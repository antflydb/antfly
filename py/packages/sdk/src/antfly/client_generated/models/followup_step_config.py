from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="FollowupStepConfig")


@_attrs_define
class FollowupStepConfig:
    """Configuration for deterministic follow-up suggestions derived from the
    original query and the standard Antfly follow-up templates.

        Attributes:
            enabled (bool | Unset): Compatibility switch. The step is enabled when this object is present; omit the step to
                disable it.
            count (int | Unset): Number of follow-up questions to generate Default: 3.
    """

    enabled: bool | Unset = UNSET
    count: int | Unset = 3
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        count = self.count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if count is not UNSET:
            field_dict["count"] = count

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        count = d.pop("count", UNSET)

        followup_step_config = cls(
            enabled=enabled,
            count=count,
        )

        followup_step_config.additional_properties = d
        return followup_step_config

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
