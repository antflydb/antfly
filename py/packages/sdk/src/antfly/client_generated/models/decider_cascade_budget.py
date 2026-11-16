from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="DeciderCascadeBudget")


@_attrs_define
class DeciderCascadeBudget:
    """
    Attributes:
        max_model_calls (int | Unset): Stage invocations allowed per request. Defaults to the stage count and cannot be
            lower.
        deadline_ms (int | Unset): Narrows the caller's deadline for the whole cascade. Checked before each stage and
            enforced by stage transports.
    """

    max_model_calls: int | Unset = UNSET
    deadline_ms: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        max_model_calls = self.max_model_calls

        deadline_ms = self.deadline_ms

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if max_model_calls is not UNSET:
            field_dict["max_model_calls"] = max_model_calls
        if deadline_ms is not UNSET:
            field_dict["deadline_ms"] = deadline_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        max_model_calls = d.pop("max_model_calls", UNSET)

        deadline_ms = d.pop("deadline_ms", UNSET)

        decider_cascade_budget = cls(
            max_model_calls=max_model_calls,
            deadline_ms=deadline_ms,
        )

        return decider_cascade_budget
