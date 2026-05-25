from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_role import TermiteRole
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.termite_image_url_content_part import TermiteImageURLContentPart
    from ..models.termite_media_content_part import TermiteMediaContentPart
    from ..models.termite_text_content_part import TermiteTextContentPart
    from ..models.termite_tool_call import TermiteToolCall


T = TypeVar("T", bound="TermiteChatMessage")


@_attrs_define
class TermiteChatMessage:
    """
    Attributes:
        role (TermiteRole): The role of a message sender in a conversation
        content (list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str | Unset):
            Message content. Supports two formats:
            - Simple string: "Hello, how are you?"
            - Array of content parts (OpenAI multimodal format): [{"type": "text", "text": "Hello"}]
        tool_calls (list[TermiteToolCall] | Unset): Tool calls made by the assistant (only for role=assistant)
        tool_call_id (str | Unset): ID of the tool call this message is responding to (only for role=tool)
    """

    role: TermiteRole
    content: list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str | Unset = UNSET
    tool_calls: list[TermiteToolCall] | Unset = UNSET
    tool_call_id: str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.termite_image_url_content_part import TermiteImageURLContentPart
        from ..models.termite_text_content_part import TermiteTextContentPart

        role = self.role.value

        content: list[dict[str, Any]] | str | Unset
        if isinstance(self.content, Unset):
            content = UNSET
        elif isinstance(self.content, list):
            content = []
            for componentsschemas_termite_chat_message_content_type_1_item_data in self.content:
                componentsschemas_termite_chat_message_content_type_1_item: dict[str, Any]
                if isinstance(componentsschemas_termite_chat_message_content_type_1_item_data, TermiteTextContentPart):
                    componentsschemas_termite_chat_message_content_type_1_item = (
                        componentsschemas_termite_chat_message_content_type_1_item_data.to_dict()
                    )
                elif isinstance(
                    componentsschemas_termite_chat_message_content_type_1_item_data, TermiteImageURLContentPart
                ):
                    componentsschemas_termite_chat_message_content_type_1_item = (
                        componentsschemas_termite_chat_message_content_type_1_item_data.to_dict()
                    )
                else:
                    componentsschemas_termite_chat_message_content_type_1_item = (
                        componentsschemas_termite_chat_message_content_type_1_item_data.to_dict()
                    )

                content.append(componentsschemas_termite_chat_message_content_type_1_item)

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
        from ..models.termite_image_url_content_part import TermiteImageURLContentPart
        from ..models.termite_media_content_part import TermiteMediaContentPart
        from ..models.termite_text_content_part import TermiteTextContentPart
        from ..models.termite_tool_call import TermiteToolCall

        d = dict(src_dict)
        role = TermiteRole(d.pop("role"))

        def _parse_content(
            data: object,
        ) -> list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, list):
                    raise TypeError()
                componentsschemas_termite_chat_message_content_type_1 = []
                _componentsschemas_termite_chat_message_content_type_1 = data
                for (
                    componentsschemas_termite_chat_message_content_type_1_item_data
                ) in _componentsschemas_termite_chat_message_content_type_1:

                    def _parse_componentsschemas_termite_chat_message_content_type_1_item(
                        data: object,
                    ) -> TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart:
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_termite_content_part_type_0 = TermiteTextContentPart.from_dict(data)

                            return componentsschemas_termite_content_part_type_0
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        try:
                            if not isinstance(data, dict):
                                raise TypeError()
                            componentsschemas_termite_content_part_type_1 = TermiteImageURLContentPart.from_dict(data)

                            return componentsschemas_termite_content_part_type_1
                        except (TypeError, ValueError, AttributeError, KeyError):
                            pass
                        if not isinstance(data, dict):
                            raise TypeError()
                        componentsschemas_termite_content_part_type_2 = TermiteMediaContentPart.from_dict(data)

                        return componentsschemas_termite_content_part_type_2

                    componentsschemas_termite_chat_message_content_type_1_item = (
                        _parse_componentsschemas_termite_chat_message_content_type_1_item(
                            componentsschemas_termite_chat_message_content_type_1_item_data
                        )
                    )

                    componentsschemas_termite_chat_message_content_type_1.append(
                        componentsschemas_termite_chat_message_content_type_1_item
                    )

                return componentsschemas_termite_chat_message_content_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(
                list[TermiteImageURLContentPart | TermiteMediaContentPart | TermiteTextContentPart] | str | Unset, data
            )

        content = _parse_content(d.pop("content", UNSET))

        _tool_calls = d.pop("tool_calls", UNSET)
        tool_calls: list[TermiteToolCall] | Unset = UNSET
        if _tool_calls is not UNSET:
            tool_calls = []
            for tool_calls_item_data in _tool_calls:
                tool_calls_item = TermiteToolCall.from_dict(tool_calls_item_data)

                tool_calls.append(tool_calls_item)

        tool_call_id = d.pop("tool_call_id", UNSET)

        termite_chat_message = cls(
            role=role,
            content=content,
            tool_calls=tool_calls,
            tool_call_id=tool_call_id,
        )

        termite_chat_message.additional_properties = d
        return termite_chat_message

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
