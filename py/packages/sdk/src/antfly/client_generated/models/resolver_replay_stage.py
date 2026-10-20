from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ResolverReplayStage")


@_attrs_define
class ResolverReplayStage:
    """
    Attributes:
        enabled (bool | Unset):
        target_sequence (int | Unset):
        applied_sequence (int | Unset):
        catch_up_required (bool | Unset):
        blocked (bool | Unset):
        blocked_reason (str | Unset):
        error_count (int | Unset):
    """

    enabled: bool | Unset = UNSET
    target_sequence: int | Unset = UNSET
    applied_sequence: int | Unset = UNSET
    catch_up_required: bool | Unset = UNSET
    blocked: bool | Unset = UNSET
    blocked_reason: str | Unset = UNSET
    error_count: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        target_sequence = self.target_sequence

        applied_sequence = self.applied_sequence

        catch_up_required = self.catch_up_required

        blocked = self.blocked

        blocked_reason = self.blocked_reason

        error_count = self.error_count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if target_sequence is not UNSET:
            field_dict["target_sequence"] = target_sequence
        if applied_sequence is not UNSET:
            field_dict["applied_sequence"] = applied_sequence
        if catch_up_required is not UNSET:
            field_dict["catch_up_required"] = catch_up_required
        if blocked is not UNSET:
            field_dict["blocked"] = blocked
        if blocked_reason is not UNSET:
            field_dict["blocked_reason"] = blocked_reason
        if error_count is not UNSET:
            field_dict["error_count"] = error_count

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        target_sequence = d.pop("target_sequence", UNSET)

        applied_sequence = d.pop("applied_sequence", UNSET)

        catch_up_required = d.pop("catch_up_required", UNSET)

        blocked = d.pop("blocked", UNSET)

        blocked_reason = d.pop("blocked_reason", UNSET)

        error_count = d.pop("error_count", UNSET)

        resolver_replay_stage = cls(
            enabled=enabled,
            target_sequence=target_sequence,
            applied_sequence=applied_sequence,
            catch_up_required=catch_up_required,
            blocked=blocked,
            blocked_reason=blocked_reason,
            error_count=error_count,
        )

        resolver_replay_stage.additional_properties = d
        return resolver_replay_stage

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
