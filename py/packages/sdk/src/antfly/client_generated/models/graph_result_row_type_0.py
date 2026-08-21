from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_result_node import GraphResultNode


T = TypeVar("T", bound="GraphResultRowType0")


@_attrs_define
class GraphResultRowType0:
    """ """

    additional_properties: dict[str, GraphResultNode | None] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_result_node import GraphResultNode

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, GraphResultNode):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_result_node import GraphResultNode

        d = dict(src_dict)
        graph_result_row_type_0 = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(data: object) -> GraphResultNode | None:
                if data is None:
                    return data
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_result_binding_type_0 = GraphResultNode.from_dict(data)

                    return componentsschemas_graph_result_binding_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                return cast(GraphResultNode | None, data)

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        graph_result_row_type_0.additional_properties = additional_properties
        return graph_result_row_type_0

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> GraphResultNode | None:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: GraphResultNode | None) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
