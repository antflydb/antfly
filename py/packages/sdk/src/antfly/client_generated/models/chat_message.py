from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.chat_message_role import ChatMessageRole
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chat_tool_call import ChatToolCall
    from ..models.chat_tool_result import ChatToolResult


T = TypeVar("T", bound="ChatMessage")


@_attrs_define
class ChatMessage:
    """A message in the conversation history

    Attributes:
        role (ChatMessageRole): Role of the message sender in the conversation
        content (str): Text content of the message
        tool_calls (list[ChatToolCall] | Unset): Tool calls made by the assistant (only for assistant role)
        tool_results (list[ChatToolResult] | Unset): Results from tool executions (only for tool role)
    """

    role: ChatMessageRole
    content: str
    tool_calls: list[ChatToolCall] | Unset = UNSET
    tool_results: list[ChatToolResult] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        role = self.role.value

        content = self.content

        tool_calls: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tool_calls, Unset):
            tool_calls = []
            for tool_calls_item_data in self.tool_calls:
                tool_calls_item = tool_calls_item_data.to_dict()
                tool_calls.append(tool_calls_item)

        tool_results: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tool_results, Unset):
            tool_results = []
            for tool_results_item_data in self.tool_results:
                tool_results_item = tool_results_item_data.to_dict()
                tool_results.append(tool_results_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "role": role,
                "content": content,
            }
        )
        if tool_calls is not UNSET:
            field_dict["tool_calls"] = tool_calls
        if tool_results is not UNSET:
            field_dict["tool_results"] = tool_results

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chat_tool_call import ChatToolCall
        from ..models.chat_tool_result import ChatToolResult

        d = dict(src_dict)
        role = ChatMessageRole(d.pop("role"))

        content = d.pop("content")

        _tool_calls = d.pop("tool_calls", UNSET)
        tool_calls: list[ChatToolCall] | Unset = UNSET
        if _tool_calls is not UNSET:
            tool_calls = []
            for tool_calls_item_data in _tool_calls:
                tool_calls_item = ChatToolCall.from_dict(tool_calls_item_data)

                tool_calls.append(tool_calls_item)

        _tool_results = d.pop("tool_results", UNSET)
        tool_results: list[ChatToolResult] | Unset = UNSET
        if _tool_results is not UNSET:
            tool_results = []
            for tool_results_item_data in _tool_results:
                tool_results_item = ChatToolResult.from_dict(tool_results_item_data)

                tool_results.append(tool_results_item)

        chat_message = cls(
            role=role,
            content=content,
            tool_calls=tool_calls,
            tool_results=tool_results,
        )

        chat_message.additional_properties = d
        return chat_message

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
