from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.index_repair_status_state import IndexRepairStatusState
from ..types import UNSET, Unset

T = TypeVar("T", bound="IndexRepairStatus")


@_attrs_define
class IndexRepairStatus:
    """Compact user-facing state for an automatic index repair. Detailed diagnostics are available from the admin API and
    metrics.

        Attributes:
            state (IndexRepairStatusState): Stable repair state. Internal state-machine phases are intentionally not exposed
                here.
            action_required (bool): Whether an operator must resume, retry, reconfigure, or drop the affected index.
            reason (str | Unset): Diagnostic reason automation stopped. Present only when action_required is true.
    """

    state: IndexRepairStatusState
    action_required: bool
    reason: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        state = self.state.value

        action_required = self.action_required

        reason = self.reason

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "state": state,
                "action_required": action_required,
            }
        )
        if reason is not UNSET:
            field_dict["reason"] = reason

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        state = IndexRepairStatusState(d.pop("state"))

        action_required = d.pop("action_required")

        reason = d.pop("reason", UNSET)

        index_repair_status = cls(
            state=state,
            action_required=action_required,
            reason=reason,
        )

        index_repair_status.additional_properties = d
        return index_repair_status

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
