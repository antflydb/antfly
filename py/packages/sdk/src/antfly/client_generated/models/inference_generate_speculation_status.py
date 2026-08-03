from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceGenerateSpeculationStatus")


@_attrs_define
class InferenceGenerateSpeculationStatus:
    """Effective speculative-decoding decision for this completion.

    Attributes:
        policy (str):
        calibration (str):
        decision (str):
        disabled_reason (None | str | Unset):
    """

    policy: str
    calibration: str
    decision: str
    disabled_reason: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        policy = self.policy

        calibration = self.calibration

        decision = self.decision

        disabled_reason: None | str | Unset
        if isinstance(self.disabled_reason, Unset):
            disabled_reason = UNSET
        else:
            disabled_reason = self.disabled_reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "policy": policy,
                "calibration": calibration,
                "decision": decision,
            }
        )
        if disabled_reason is not UNSET:
            field_dict["disabled_reason"] = disabled_reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        policy = d.pop("policy")

        calibration = d.pop("calibration")

        decision = d.pop("decision")

        def _parse_disabled_reason(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        disabled_reason = _parse_disabled_reason(d.pop("disabled_reason", UNSET))

        inference_generate_speculation_status = cls(
            policy=policy,
            calibration=calibration,
            decision=decision,
            disabled_reason=disabled_reason,
        )

        inference_generate_speculation_status.additional_properties = d
        return inference_generate_speculation_status

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
