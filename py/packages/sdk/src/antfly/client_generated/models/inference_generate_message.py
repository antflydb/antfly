from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_role import InferenceRole
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_tool_call import InferenceToolCall


T = TypeVar("T", bound="InferenceGenerateMessage")


@_attrs_define
class InferenceGenerateMessage:
    """
    Attributes:
        role (InferenceRole): The role of a message sender in a conversation
        content (None | str | Unset): The generated message content (null when tool_calls is present)
        tool_calls (list[InferenceToolCall] | Unset): Tool calls made by the model (only present when finish_reason is
            tool_calls)
    """

    role: InferenceRole
    content: None | str | Unset = UNSET
    tool_calls: list[InferenceToolCall] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        role = self.role.value

        content: None | str | Unset
        if isinstance(self.content, Unset):
            content = UNSET
        else:
            content = self.content

        tool_calls: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tool_calls, Unset):
            tool_calls = []
            for tool_calls_item_data in self.tool_calls:
                tool_calls_item = tool_calls_item_data.to_dict()
                tool_calls.append(tool_calls_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "role": role,
            }
        )
        if content is not UNSET:
            field_dict["content"] = content
        if tool_calls is not UNSET:
            field_dict["tool_calls"] = tool_calls

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_tool_call import InferenceToolCall

        d = dict(src_dict)
        role = InferenceRole(d.pop("role"))

        def _parse_content(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        content = _parse_content(d.pop("content", UNSET))

        _tool_calls = d.pop("tool_calls", UNSET)
        tool_calls: list[InferenceToolCall] | Unset = UNSET
        if _tool_calls is not UNSET:
            tool_calls = []
            for tool_calls_item_data in _tool_calls:
                tool_calls_item = InferenceToolCall.from_dict(tool_calls_item_data)

                tool_calls.append(tool_calls_item)

        inference_generate_message = cls(
            role=role,
            content=content,
            tool_calls=tool_calls,
        )

        inference_generate_message.additional_properties = d
        return inference_generate_message

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
