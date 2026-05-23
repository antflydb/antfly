from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.termite_tool_type import TermiteToolType

if TYPE_CHECKING:
    from ..models.termite_function_definition import TermiteFunctionDefinition


T = TypeVar("T", bound="TermiteTool")


@_attrs_define
class TermiteTool:
    """A tool (function) that the model can call

    Attributes:
        type_ (TermiteToolType): The type of tool (currently only "function" is supported)
        function (TermiteFunctionDefinition): Definition of a function that can be called by the model
    """

    type_: TermiteToolType
    function: TermiteFunctionDefinition
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
        from ..models.termite_function_definition import TermiteFunctionDefinition

        d = dict(src_dict)
        type_ = TermiteToolType(d.pop("type"))

        function = TermiteFunctionDefinition.from_dict(d.pop("function"))

        termite_tool = cls(
            type_=type_,
            function=function,
        )

        termite_tool.additional_properties = d
        return termite_tool

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
