from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_result_node import GraphResultNode


T = TypeVar("T", bound="PatternMatchBindings")


@_attrs_define
class PatternMatchBindings:
    """Map of alias to matched node"""

    additional_properties: dict[str, "GraphResultNode"] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_result_node import GraphResultNode

        d = dict(src_dict)
        pattern_match_bindings = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            additional_property = GraphResultNode.from_dict(prop_dict)

            additional_properties[prop_name] = additional_property

        pattern_match_bindings.additional_properties = additional_properties
        return pattern_match_bindings

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> "GraphResultNode":
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: "GraphResultNode") -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
