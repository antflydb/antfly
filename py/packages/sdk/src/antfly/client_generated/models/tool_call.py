from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.tool_call_type import ToolCallType

if TYPE_CHECKING:
    from ..models.tool_call_function import ToolCallFunction


T = TypeVar("T", bound="ToolCall")


@_attrs_define
class ToolCall:
    """OpenAI-compatible assistant tool call.

    Attributes:
        id (str): Tool call identifier.
        type_ (ToolCallType):
        function (ToolCallFunction): The function called by a model tool call.
    """

    id: str
    type_: ToolCallType
    function: ToolCallFunction
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        type_ = self.type_.value

        function = self.function.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "id": id,
                "type": type_,
                "function": function,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.tool_call_function import ToolCallFunction

        d = dict(src_dict)
        id = d.pop("id")

        type_ = ToolCallType(d.pop("type"))

        function = ToolCallFunction.from_dict(d.pop("function"))

        tool_call = cls(
            id=id,
            type_=type_,
            function=function,
        )

        tool_call.additional_properties = d
        return tool_call

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
