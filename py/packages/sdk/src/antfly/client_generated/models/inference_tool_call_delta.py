from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_tool_call_delta_type import InferenceToolCallDeltaType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.inference_tool_call_function_delta import InferenceToolCallFunctionDelta


T = TypeVar("T", bound="InferenceToolCallDelta")


@_attrs_define
class InferenceToolCallDelta:
    """Incremental tool call data for streaming

    Attributes:
        index (int | Unset): Index of the tool call in the array
        id (str | Unset): Unique identifier (only in first delta for this index)
        type_ (InferenceToolCallDeltaType | Unset): The type of tool call (only in first delta)
        function (InferenceToolCallFunctionDelta | Unset): Incremental function call data for streaming
    """

    index: int | Unset = UNSET
    id: str | Unset = UNSET
    type_: InferenceToolCallDeltaType | Unset = UNSET
    function: InferenceToolCallFunctionDelta | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        id = self.id

        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        function: dict[str, Any] | Unset = UNSET
        if not isinstance(self.function, Unset):
            function = self.function.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if index is not UNSET:
            field_dict["index"] = index
        if id is not UNSET:
            field_dict["id"] = id
        if type_ is not UNSET:
            field_dict["type"] = type_
        if function is not UNSET:
            field_dict["function"] = function

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_tool_call_function_delta import InferenceToolCallFunctionDelta

        d = dict(src_dict)
        index = d.pop("index", UNSET)

        id = d.pop("id", UNSET)

        _type_ = d.pop("type", UNSET)
        type_: InferenceToolCallDeltaType | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = InferenceToolCallDeltaType(_type_)

        _function = d.pop("function", UNSET)
        function: InferenceToolCallFunctionDelta | Unset
        if isinstance(_function, Unset):
            function = UNSET
        else:
            function = InferenceToolCallFunctionDelta.from_dict(_function)

        inference_tool_call_delta = cls(
            index=index,
            id=id,
            type_=type_,
            function=function,
        )

        inference_tool_call_delta.additional_properties = d
        return inference_tool_call_delta

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
