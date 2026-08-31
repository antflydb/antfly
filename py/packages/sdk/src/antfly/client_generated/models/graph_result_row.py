from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_binding_node import GraphBindingNode


T = TypeVar("T", bound="GraphResultRow")


@_attrs_define
class GraphResultRow:
    """ """

    additional_properties: dict[str, GraphBindingNode | None] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_binding_node import GraphBindingNode

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, GraphBindingNode):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_binding_node import GraphBindingNode

        d = dict(src_dict)
        graph_result_row = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(data: object) -> GraphBindingNode | None:
                if data is None:
                    return data
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_result_binding_type_0 = GraphBindingNode.from_dict(data)

                    return componentsschemas_graph_result_binding_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                return cast(GraphBindingNode | None, data)

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        graph_result_row.additional_properties = additional_properties
        return graph_result_row

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> GraphBindingNode | None:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: GraphBindingNode | None) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
