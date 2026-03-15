from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.chat_tool_call_arguments import ChatToolCallArguments


T = TypeVar("T", bound="ChatToolCall")


@_attrs_define
class ChatToolCall:
    """A tool call made by the assistant

    Attributes:
        id (str): Unique identifier for this tool call
        name (str): Name of the tool being called
        arguments (ChatToolCallArguments): Arguments passed to the tool as key-value pairs
    """

    id: str
    name: str
    arguments: "ChatToolCallArguments"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        name = self.name

        arguments = self.arguments.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "name": name,
                "arguments": arguments,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chat_tool_call_arguments import ChatToolCallArguments

        d = dict(src_dict)
        id = d.pop("id")

        name = d.pop("name")

        arguments = ChatToolCallArguments.from_dict(d.pop("arguments"))

        chat_tool_call = cls(
            id=id,
            name=name,
            arguments=arguments,
        )

        chat_tool_call.additional_properties = d
        return chat_tool_call

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
