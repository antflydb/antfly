from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="ReplicationSourceActionHint")


@_attrs_define
class ReplicationSourceActionHint:
    """Action hint for this replication source when remediation is recommended.
    Present only in GET table detail responses.

        Attributes:
            action (str | Unset): Recommended action (e.g., "reseed_exact_cutover").
            reason (str | Unset): Human-readable reason for the recommendation.
            reseed_exact_cutover_path (str | Unset): API path to trigger a reseed exact cutover, if applicable.
    """

    action: str | Unset = UNSET
    reason: str | Unset = UNSET
    reseed_exact_cutover_path: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        action = self.action

        reason = self.reason

        reseed_exact_cutover_path = self.reseed_exact_cutover_path

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if action is not UNSET:
            field_dict["action"] = action
        if reason is not UNSET:
            field_dict["reason"] = reason
        if reseed_exact_cutover_path is not UNSET:
            field_dict["reseed_exact_cutover_path"] = reseed_exact_cutover_path

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        action = d.pop("action", UNSET)

        reason = d.pop("reason", UNSET)

        reseed_exact_cutover_path = d.pop("reseed_exact_cutover_path", UNSET)

        replication_source_action_hint = cls(
            action=action,
            reason=reason,
            reseed_exact_cutover_path=reseed_exact_cutover_path,
        )

        replication_source_action_hint.additional_properties = d
        return replication_source_action_hint

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
