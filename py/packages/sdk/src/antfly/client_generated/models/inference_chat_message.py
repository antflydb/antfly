from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_role import InferenceRole
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.image_url_content_part import ImageURLContentPart
    from ..models.media_content_part import MediaContentPart
    from ..models.text_content_part import TextContentPart
    from ..models.tool_call import ToolCall


T = TypeVar("T", bound="InferenceChatMessage")


@_attrs_define
class InferenceChatMessage:
    """
    Attributes:
        role (InferenceRole): Role of the message sender in a generation/chat conversation
        content (list[ImageURLContentPart | MediaContentPart | TextContentPart] | str | Unset): Message content.
            Supports two formats:
            - Simple string: "Hello, how are you?"
            - Array of content parts: [{"type": "text", "text": "Hello"}]
        tool_calls (list[ToolCall] | Unset): Tool calls made by the assistant (only for role=assistant)
        tool_call_id (str | Unset): ID of the tool call this message is responding to (only for role=tool)
    """

    role: InferenceRole
    content: list[ImageURLContentPart | MediaContentPart | TextContentPart] | str | Unset = UNSET
    tool_calls: list[ToolCall] | Unset = UNSET
    tool_call_id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.text_content_part import TextContentPart

        role = self.role.value

        content: list[dict[str, Any]] | str | Unset
        if isinstance(self.content, Unset):
            content = UNSET
        elif isinstance(self.content, list):
            content = []
            for componentsschemas_chat_message_content_type_1_item_data in self.content:
                componentsschemas_chat_message_content_type_1_item: dict[str, Any]
                if isinstance(componentsschemas_chat_message_content_type_1_item_data, TextContentPart):
                    componentsschemas_chat_message_content_type_1_item = (
                        componentsschemas_chat_message_content_type_1_item_data.to_dict()
                    )
                elif isinstance(componentsschemas_chat_message_content_type_1_item_data, ImageURLContentPart):
                    componentsschemas_chat_message_content_type_1_item = (
                        componentsschemas_chat_message_content_type_1_item_data.to_dict()
                    )
                else:
                    componentsschemas_chat_message_content_type_1_item = (
                        componentsschemas_chat_message_content_type_1_item_data.to_dict()
                    )

                content.append(componentsschemas_chat_message_content_type_1_item)

        else:
            content = self.content

        tool_calls: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.tool_calls, Unset):
            tool_calls = []
            for tool_calls_item_data in self.tool_calls:
                tool_calls_item = tool_calls_item_data.to_dict()
                tool_calls.append(tool_calls_item)

        tool_call_id = self.tool_call_id

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
        if tool_call_id is not UNSET:
            field_dict["tool_call_id"] = tool_call_id

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.image_url_content_part import ImageURLContentPart
        from ..models.media_content_part import MediaContentPart
        from ..models.text_content_part import TextContentPart
        from ..models.tool_call import ToolCall

        d = dict(src_dict)
        role = InferenceRole(d.pop("role"))

        def _parse_content(
            data: object,
        ) -> list[ImageURLContentPart | MediaContentPart | TextContentPart] | str | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                componentsschemas_chat_message_content_type_1 = []
                _componentsschemas_chat_message_content_type_1 = data
                for (
                    componentsschemas_chat_message_content_type_1_item_data
                ) in _componentsschemas_chat_message_content_type_1:

                    def _parse_componentsschemas_chat_message_content_type_1_item(
                        data: object,
                    ) -> ImageURLContentPart | MediaContentPart | TextContentPart:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_content_part_type_0 = TextContentPart.from_dict(data)

                            return componentsschemas_content_part_type_0
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_content_part_type_1 = ImageURLContentPart.from_dict(data)

                            return componentsschemas_content_part_type_1
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_content_part_type_2 = MediaContentPart.from_dict(data)

                        return componentsschemas_content_part_type_2

                    componentsschemas_chat_message_content_type_1_item = (
                        _parse_componentsschemas_chat_message_content_type_1_item(
                            componentsschemas_chat_message_content_type_1_item_data
                        )
                    )

                    componentsschemas_chat_message_content_type_1.append(
                        componentsschemas_chat_message_content_type_1_item
                    )

                return componentsschemas_chat_message_content_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(list[ImageURLContentPart | MediaContentPart | TextContentPart] | str | Unset, data)

        content = _parse_content(d.pop("content", UNSET))

        _tool_calls = d.pop("tool_calls", UNSET)
        tool_calls: list[ToolCall] | Unset = UNSET
        if _tool_calls is not UNSET:
            tool_calls = []
            for tool_calls_item_data in _tool_calls:
                tool_calls_item = ToolCall.from_dict(tool_calls_item_data)

                tool_calls.append(tool_calls_item)

        tool_call_id = d.pop("tool_call_id", UNSET)

        inference_chat_message = cls(
            role=role,
            content=content,
            tool_calls=tool_calls,
            tool_call_id=tool_call_id,
        )

        inference_chat_message.additional_properties = d
        return inference_chat_message

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
