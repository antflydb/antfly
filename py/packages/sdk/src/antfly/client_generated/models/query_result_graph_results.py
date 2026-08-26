from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_aggregates_result import GraphAggregatesResult
    from ..models.graph_bindings_result import GraphBindingsResult
    from ..models.graph_nodes_result import GraphNodesResult
    from ..models.legacy_graph_query_result import LegacyGraphQueryResult


T = TypeVar("T", bound="QueryResultGraphResults")


@_attrs_define
class QueryResultGraphResults:
    """Results from canonical graph_queries or deprecated graph_searches."""

    additional_properties: dict[
        str, GraphAggregatesResult | GraphBindingsResult | GraphNodesResult | LegacyGraphQueryResult
    ] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_aggregates_result import GraphAggregatesResult
        from ..models.graph_bindings_result import GraphBindingsResult
        from ..models.graph_nodes_result import GraphNodesResult

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, GraphBindingsResult):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, GraphAggregatesResult):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, GraphNodesResult):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_aggregates_result import GraphAggregatesResult
        from ..models.graph_bindings_result import GraphBindingsResult
        from ..models.graph_nodes_result import GraphNodesResult
        from ..models.legacy_graph_query_result import LegacyGraphQueryResult

        d = dict(src_dict)
        query_result_graph_results = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(
                data: object,
            ) -> GraphAggregatesResult | GraphBindingsResult | GraphNodesResult | LegacyGraphQueryResult:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_query_result_type_0 = GraphBindingsResult.from_dict(data)

                    return componentsschemas_graph_query_result_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_query_result_type_1 = GraphAggregatesResult.from_dict(data)

                    return componentsschemas_graph_query_result_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_query_result_type_2 = GraphNodesResult.from_dict(data)

                    return componentsschemas_graph_query_result_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_result_type_1 = LegacyGraphQueryResult.from_dict(data)

                return componentsschemas_graph_result_type_1

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        query_result_graph_results.additional_properties = additional_properties
        return query_result_graph_results

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(
        self, key: str
    ) -> GraphAggregatesResult | GraphBindingsResult | GraphNodesResult | LegacyGraphQueryResult:
        return self.additional_properties[key]

    def __setitem__(
        self, key: str, value: GraphAggregatesResult | GraphBindingsResult | GraphNodesResult | LegacyGraphQueryResult
    ) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
