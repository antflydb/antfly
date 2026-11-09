from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

T = TypeVar("T", bound="InferenceContextToolEvidence")


@_attrs_define
class InferenceContextToolEvidence:
    """
    Attributes:
        tool_call_id (str):
        tool_name (str):
        facts (str): Compacted evidence attributed to this completed tool call.
        source_ids (list[str] | Unset): Exact source identities attributed to this tool result. Optional for legacy
            checkpoints; summaries must preserve server-supplied identities without shortening or substitution.
    """

    tool_call_id: str
    tool_name: str
    facts: str
    source_ids: list[str] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        tool_call_id = self.tool_call_id

        tool_name = self.tool_name

        facts = self.facts

        source_ids: list[str] | Unset = UNSET
        if not isinstance(self.source_ids, Unset):
            source_ids = self.source_ids

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "tool_call_id": tool_call_id,
                "tool_name": tool_name,
                "facts": facts,
            }
        )
        if source_ids is not UNSET:
            field_dict["source_ids"] = source_ids

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        tool_call_id = d.pop("tool_call_id")

        tool_name = d.pop("tool_name")

        facts = d.pop("facts")

        source_ids = cast(list[str], d.pop("source_ids", UNSET))

        inference_context_tool_evidence = cls(
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            facts=facts,
            source_ids=source_ids,
        )

        return inference_context_tool_evidence
