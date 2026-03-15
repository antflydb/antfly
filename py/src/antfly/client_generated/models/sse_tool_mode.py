from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.sse_tool_mode_mode import SSEToolModeMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="SSEToolMode")


@_attrs_define
class SSEToolMode:
    """Emitted when the agent selects a tool-calling mode

    Attributes:
        mode (SSEToolModeMode): Tool calling mode selected
        tools_count (Union[Unset, int]): Number of tools available (present for native mode)
    """

    mode: SSEToolModeMode
    tools_count: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        mode = self.mode.value

        tools_count = self.tools_count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "mode": mode,
            }
        )
        if tools_count is not UNSET:
            field_dict["tools_count"] = tools_count

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        mode = SSEToolModeMode(d.pop("mode"))

        tools_count = d.pop("tools_count", UNSET)

        sse_tool_mode = cls(
            mode=mode,
            tools_count=tools_count,
        )

        sse_tool_mode.additional_properties = d
        return sse_tool_mode

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
