from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.research_incomplete_details_reason import ResearchIncompleteDetailsReason
from ..types import UNSET, Unset

T = TypeVar("T", bound="ResearchIncompleteDetails")


@_attrs_define
class ResearchIncompleteDetails:
    """
    Attributes:
        reason (ResearchIncompleteDetailsReason): Why the run stopped:
            - max_rounds: research rounds were exhausted before the reflector was satisfied (the report is still written)
            - max_llm_calls / max_tool_calls: a hard budget was exhausted
            - deadline: the wall-clock budget elapsed
            - no_evidence: researchers found no evidence to write from
            - clarification_required: the planner needs a user decision
            - cancelled: a durable job was cancelled
            - phase_limit: a job advance stopped after its requested number of phases
        message (str | Unset): Human-readable detail.
    """

    reason: ResearchIncompleteDetailsReason
    message: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reason = self.reason.value

        message = self.message

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "reason": reason,
            }
        )
        if message is not UNSET:
            field_dict["message"] = message

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        reason = ResearchIncompleteDetailsReason(d.pop("reason"))

        message = d.pop("message", UNSET)

        research_incomplete_details = cls(
            reason=reason,
            message=message,
        )

        research_incomplete_details.additional_properties = d
        return research_incomplete_details

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
