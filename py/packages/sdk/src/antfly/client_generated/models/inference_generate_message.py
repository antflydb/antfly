from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_role import InferenceRole
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_generate_message_extra_content import InferenceGenerateMessageExtraContent
    from ..models.tool_call import ToolCall


T = TypeVar("T", bound="InferenceGenerateMessage")


@_attrs_define
class InferenceGenerateMessage:
    """
    Attributes:
        role (InferenceRole): Chat message role, including developer instructions preserved by context management.
        extra_content (InferenceGenerateMessageExtraContent | Unset): Opaque provider message metadata. Preserve
            unchanged when replaying assistant messages; native Anthropic content retains signed thinking blocks.
        content (None | str | Unset): The generated message content (null when tool_calls is present)
        reasoning_content (None | str | Unset): Model reasoning emitted on a private reasoning channel, separate from
            public content
        tool_calls (list[ToolCall] | Unset): Tool calls made by the model (only present when finish_reason is
            tool_calls)
    """

    role: InferenceRole
    extra_content: InferenceGenerateMessageExtraContent | Unset = UNSET
    content: None | str | Unset = UNSET
    reasoning_content: None | str | Unset = UNSET
    tool_calls: list[ToolCall] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        role = self.role.value

        extra_content: dict[str, Any] | Unset = UNSET
        if not isinstance(self.extra_content, Unset):
            extra_content = self.extra_content.to_dict()

        content: None | str | Unset
        if isinstance(self.content, Unset):
            content = UNSET
        else:
            content = self.content

        reasoning_content: None | str | Unset
        if isinstance(self.reasoning_content, Unset):
            reasoning_content = UNSET
        else:
            reasoning_content = self.reasoning_content

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
        if extra_content is not UNSET:
            field_dict["extra_content"] = extra_content
        if content is not UNSET:
            field_dict["content"] = content
        if reasoning_content is not UNSET:
            field_dict["reasoning_content"] = reasoning_content
        if tool_calls is not UNSET:
            field_dict["tool_calls"] = tool_calls

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_generate_message_extra_content import InferenceGenerateMessageExtraContent
        from ..models.tool_call import ToolCall

        d = dict(src_dict)
        role = InferenceRole(d.pop("role"))

        _extra_content = d.pop("extra_content", UNSET)
        extra_content: InferenceGenerateMessageExtraContent | Unset
        if isinstance(_extra_content, Unset):
            extra_content = UNSET
        else:
            extra_content = InferenceGenerateMessageExtraContent.from_dict(_extra_content)

        def _parse_content(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        content = _parse_content(d.pop("content", UNSET))

        def _parse_reasoning_content(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        reasoning_content = _parse_reasoning_content(d.pop("reasoning_content", UNSET))

        _tool_calls = d.pop("tool_calls", UNSET)
        tool_calls: list[ToolCall] | Unset = UNSET
        if _tool_calls is not UNSET:
            tool_calls = []
            for tool_calls_item_data in _tool_calls:
                tool_calls_item = ToolCall.from_dict(tool_calls_item_data)

                tool_calls.append(tool_calls_item)

        inference_generate_message = cls(
            role=role,
            extra_content=extra_content,
            content=content,
            reasoning_content=reasoning_content,
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
