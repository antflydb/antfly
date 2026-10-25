from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="InferenceContextToolEvidence")


@_attrs_define
class InferenceContextToolEvidence:
    """
    Attributes:
        tool_call_id (str):
        tool_name (str):
        facts (str): Compacted evidence attributed to this completed tool call.
    """

    tool_call_id: str
    tool_name: str
    facts: str

    def to_dict(self) -> dict[str, Any]:
        tool_call_id = self.tool_call_id

        tool_name = self.tool_name

        facts = self.facts

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "tool_call_id": tool_call_id,
                "tool_name": tool_name,
                "facts": facts,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        tool_call_id = d.pop("tool_call_id")

        tool_name = d.pop("tool_name")

        facts = d.pop("facts")

        inference_context_tool_evidence = cls(
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            facts=facts,
        )

        return inference_context_tool_evidence
