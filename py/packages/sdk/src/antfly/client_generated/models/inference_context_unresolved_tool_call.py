from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="InferenceContextUnresolvedToolCall")


@_attrs_define
class InferenceContextUnresolvedToolCall:
    """
    Attributes:
        tool_call_id (str):
        tool_name (str):
    """

    tool_call_id: str
    tool_name: str

    def to_dict(self) -> dict[str, Any]:
        tool_call_id = self.tool_call_id

        tool_name = self.tool_name

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "tool_call_id": tool_call_id,
                "tool_name": tool_name,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        tool_call_id = d.pop("tool_call_id")

        tool_name = d.pop("tool_name")

        inference_context_unresolved_tool_call = cls(
            tool_call_id=tool_call_id,
            tool_name=tool_name,
        )

        return inference_context_unresolved_tool_call
