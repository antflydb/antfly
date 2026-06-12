from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_role import InferenceRole
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_tool_call_delta import InferenceToolCallDelta


T = TypeVar("T", bound="InferenceGenerateDelta")


@_attrs_define
class InferenceGenerateDelta:
    """Delta content for streaming

    Attributes:
        role (InferenceRole | Unset): Role of the message sender in a generation/chat conversation
        content (None | str | Unset): Token content delta
        tool_calls (list[InferenceToolCallDelta] | Unset): Tool call deltas for streaming tool calls
    """

    role: InferenceRole | Unset = UNSET
    content: None | str | Unset = UNSET
    tool_calls: list[InferenceToolCallDelta] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        role: str | Unset = UNSET
        if not isinstance(self.role, Unset):
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
        field_dict.update({})
        if role is not UNSET:
            field_dict["role"] = role
        if content is not UNSET:
            field_dict["content"] = content
        if tool_calls is not UNSET:
            field_dict["tool_calls"] = tool_calls

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_tool_call_delta import InferenceToolCallDelta

        d = dict(src_dict)
        _role = d.pop("role", UNSET)
        role: InferenceRole | Unset
        if isinstance(_role, Unset):
            role = UNSET
        else:
            role = InferenceRole(_role)

        def _parse_content(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        content = _parse_content(d.pop("content", UNSET))

        _tool_calls = d.pop("tool_calls", UNSET)
        tool_calls: list[InferenceToolCallDelta] | Unset = UNSET
        if _tool_calls is not UNSET:
            tool_calls = []
            for tool_calls_item_data in _tool_calls:
                tool_calls_item = InferenceToolCallDelta.from_dict(tool_calls_item_data)

                tool_calls.append(tool_calls_item)

        inference_generate_delta = cls(
            role=role,
            content=content,
            tool_calls=tool_calls,
        )

        inference_generate_delta.additional_properties = d
        return inference_generate_delta

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
