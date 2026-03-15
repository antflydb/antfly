from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.incomplete_details_reason import IncompleteDetailsReason

T = TypeVar("T", bound="IncompleteDetails")


@_attrs_define
class IncompleteDetails:
    """Explains why the agent stopped before completion. Present when status is "incomplete".

    Attributes:
        reason (IncompleteDetailsReason): Why the agent stopped:
            - max_iterations: Hit the configured max_iterations limit
            - max_tokens: LLM output was truncated
            - no_tools: No tools were available for agentic mode
            - clarification_needed: Agent needs user input to proceed
    """

    reason: IncompleteDetailsReason
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        reason = self.reason.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "reason": reason,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        reason = IncompleteDetailsReason(d.pop("reason"))

        incomplete_details = cls(
            reason=reason,
        )

        incomplete_details.additional_properties = d
        return incomplete_details

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
