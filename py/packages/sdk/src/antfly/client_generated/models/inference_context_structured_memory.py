from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.inference_context_tool_evidence import InferenceContextToolEvidence
    from ..models.inference_context_unresolved_tool_call import InferenceContextUnresolvedToolCall


T = TypeVar("T", bound="InferenceContextStructuredMemory")


@_attrs_define
class InferenceContextStructuredMemory:
    """
    Attributes:
        conversation (str): Compacted conversation memory excluding attributed tool evidence.
        tool_evidence (list[InferenceContextToolEvidence]):
        unresolved_tool_calls (list[InferenceContextUnresolvedToolCall]):
    """

    conversation: str
    tool_evidence: list[InferenceContextToolEvidence]
    unresolved_tool_calls: list[InferenceContextUnresolvedToolCall]

    def to_dict(self) -> dict[str, Any]:
        conversation = self.conversation

        tool_evidence = []
        for tool_evidence_item_data in self.tool_evidence:
            tool_evidence_item = tool_evidence_item_data.to_dict()
            tool_evidence.append(tool_evidence_item)

        unresolved_tool_calls = []
        for unresolved_tool_calls_item_data in self.unresolved_tool_calls:
            unresolved_tool_calls_item = unresolved_tool_calls_item_data.to_dict()
            unresolved_tool_calls.append(unresolved_tool_calls_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "conversation": conversation,
                "tool_evidence": tool_evidence,
                "unresolved_tool_calls": unresolved_tool_calls,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_context_tool_evidence import InferenceContextToolEvidence
        from ..models.inference_context_unresolved_tool_call import InferenceContextUnresolvedToolCall

        d = dict(src_dict)
        conversation = d.pop("conversation")

        tool_evidence = []
        _tool_evidence = d.pop("tool_evidence")
        for tool_evidence_item_data in _tool_evidence:
            tool_evidence_item = InferenceContextToolEvidence.from_dict(tool_evidence_item_data)

            tool_evidence.append(tool_evidence_item)

        unresolved_tool_calls = []
        _unresolved_tool_calls = d.pop("unresolved_tool_calls")
        for unresolved_tool_calls_item_data in _unresolved_tool_calls:
            unresolved_tool_calls_item = InferenceContextUnresolvedToolCall.from_dict(unresolved_tool_calls_item_data)

            unresolved_tool_calls.append(unresolved_tool_calls_item)

        inference_context_structured_memory = cls(
            conversation=conversation,
            tool_evidence=tool_evidence,
            unresolved_tool_calls=unresolved_tool_calls,
        )

        return inference_context_structured_memory
