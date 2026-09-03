from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_alias_count_aggregate import GraphAliasCountAggregate
    from ..models.graph_row_count_aggregate import GraphRowCountAggregate


T = TypeVar("T", bound="GraphAggregatesReturnAggregates")


@_attrs_define
class GraphAggregatesReturnAggregates:
    """Keys are GraphIdentifiers naming aggregate results."""

    additional_properties: dict[str, GraphAliasCountAggregate | GraphRowCountAggregate] = _attrs_field(
        init=False, factory=dict
    )

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_row_count_aggregate import GraphRowCountAggregate

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, GraphRowCountAggregate):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_alias_count_aggregate import GraphAliasCountAggregate
        from ..models.graph_row_count_aggregate import GraphRowCountAggregate

        d = dict(src_dict)
        graph_aggregates_return_aggregates = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(data: object) -> GraphAliasCountAggregate | GraphRowCountAggregate:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_count_aggregate_type_0 = GraphRowCountAggregate.from_dict(data)

                    return componentsschemas_graph_count_aggregate_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_count_aggregate_type_1 = GraphAliasCountAggregate.from_dict(data)

                return componentsschemas_graph_count_aggregate_type_1

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        graph_aggregates_return_aggregates.additional_properties = additional_properties
        return graph_aggregates_return_aggregates

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> GraphAliasCountAggregate | GraphRowCountAggregate:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: GraphAliasCountAggregate | GraphRowCountAggregate) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
