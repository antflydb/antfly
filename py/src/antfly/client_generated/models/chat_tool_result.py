from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.chat_tool_result_result import ChatToolResultResult


T = TypeVar("T", bound="ChatToolResult")


@_attrs_define
class ChatToolResult:
    """Result from executing a tool call

    Attributes:
        tool_call_id (str): ID of the tool call this result corresponds to
        result (ChatToolResultResult): Result data from the tool execution
        error (Union[Unset, str]): Error message if tool execution failed
    """

    tool_call_id: str
    result: "ChatToolResultResult"
    error: Union[Unset, str] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        tool_call_id = self.tool_call_id

        result = self.result.to_dict()

        error = self.error

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "tool_call_id": tool_call_id,
                "result": result,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chat_tool_result_result import ChatToolResultResult

        d = dict(src_dict)
        tool_call_id = d.pop("tool_call_id")

        result = ChatToolResultResult.from_dict(d.pop("result"))

        error = d.pop("error", UNSET)

        chat_tool_result = cls(
            tool_call_id=tool_call_id,
            result=result,
            error=error,
        )

        chat_tool_result.additional_properties = d
        return chat_tool_result

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
