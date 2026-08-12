from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.inference_tool_choice_type_1_type import InferenceToolChoiceType1Type

if TYPE_CHECKING:
    from ..models.inference_tool_choice_function import InferenceToolChoiceFunction


T = TypeVar("T", bound="InferenceToolChoiceType1")


@_attrs_define
class InferenceToolChoiceType1:
    """Force a specific function to be called

    Attributes:
        type_ (InferenceToolChoiceType1Type):
        function (InferenceToolChoiceFunction):
    """

    type_: InferenceToolChoiceType1Type
    function: InferenceToolChoiceFunction
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        function = self.function.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": type_,
                "function": function,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.inference_tool_choice_function import InferenceToolChoiceFunction

        d = dict(src_dict)
        type_ = InferenceToolChoiceType1Type(d.pop("type"))

        function = InferenceToolChoiceFunction.from_dict(d.pop("function"))

        inference_tool_choice_type_1 = cls(
            type_=type_,
            function=function,
        )

        inference_tool_choice_type_1.additional_properties = d
        return inference_tool_choice_type_1

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
