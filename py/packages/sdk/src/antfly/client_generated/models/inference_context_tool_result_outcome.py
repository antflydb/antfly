from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.inference_context_tool_result_outcome_disposition import InferenceContextToolResultOutcomeDisposition
from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceContextToolResultOutcome")


@_attrs_define
class InferenceContextToolResultOutcome:
    """
    Attributes:
        tool_call_id (str):
        disposition (InferenceContextToolResultOutcomeDisposition): Raw result retained, represented by attributed
            checkpoint tool_evidence,
            or dropped. During compact, a result mentioned only in the conversation
            summary is conservatively reported as dropped; only matching attributed
            tool_evidence qualifies as summarized. Outcomes do not include per-result
            token counts. Context planning removes whole messages without truncating
            individual messages; the agent layer must compute delivered tokens.
        message_index (int | Unset): Original request message index; omitted for evidence from an earlier checkpoint.
    """

    tool_call_id: str
    disposition: InferenceContextToolResultOutcomeDisposition
    message_index: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        tool_call_id = self.tool_call_id

        disposition = self.disposition.value

        message_index = self.message_index

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "tool_call_id": tool_call_id,
                "disposition": disposition,
            }
        )
        if message_index is not UNSET:
            field_dict["message_index"] = message_index

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        tool_call_id = d.pop("tool_call_id")

        disposition = InferenceContextToolResultOutcomeDisposition(d.pop("disposition"))

        message_index = d.pop("message_index", UNSET)

        inference_context_tool_result_outcome = cls(
            tool_call_id=tool_call_id,
            disposition=disposition,
            message_index=message_index,
        )

        return inference_context_tool_result_outcome
