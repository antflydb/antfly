from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.graph_k_shortest_paths_query import GraphKShortestPathsQuery
    from ..models.graph_match_query import GraphMatchQuery
    from ..models.graph_shortest_path_query import GraphShortestPathQuery
    from ..models.graph_traverse_query import GraphTraverseQuery


T = TypeVar("T", bound="QueryRequestGraphQueries")


@_attrs_define
class QueryRequestGraphQueries:
    """Declarative graph matching, traversal, and path queries. A nested node
    `filter` is a typed, non-scoring stored-document predicate. It shares
    familiar scalar syntax with document queries but deliberately excludes
    analyzer-backed and index-only clauses. A request may contain at most
    64 named graph operations, of which at most 8 may be named `match`
    operations. Operation names must be 1-128 Unicode characters and
    must not begin with `$`, have leading or trailing spaces, contain
    non-ASCII whitespace, or contain Unicode control or format code
    points. Ordinary internal ASCII spaces are allowed. `$` is reserved
    for result namespaces.
    Put multiple counts over one pattern in the same `match` return
    object so they share one complete anchor scan.

    """

    additional_properties: dict[
        str, GraphKShortestPathsQuery | GraphMatchQuery | GraphShortestPathQuery | GraphTraverseQuery
    ] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_match_query import GraphMatchQuery
        from ..models.graph_shortest_path_query import GraphShortestPathQuery
        from ..models.graph_traverse_query import GraphTraverseQuery

        field_dict: dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():
            if isinstance(prop, GraphMatchQuery):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, GraphTraverseQuery):
                field_dict[prop_name] = prop.to_dict()
            elif isinstance(prop, GraphShortestPathQuery):
                field_dict[prop_name] = prop.to_dict()
            else:
                field_dict[prop_name] = prop.to_dict()

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_k_shortest_paths_query import GraphKShortestPathsQuery
        from ..models.graph_match_query import GraphMatchQuery
        from ..models.graph_shortest_path_query import GraphShortestPathQuery
        from ..models.graph_traverse_query import GraphTraverseQuery

        d = dict(src_dict)
        query_request_graph_queries = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(
                data: object,
            ) -> GraphKShortestPathsQuery | GraphMatchQuery | GraphShortestPathQuery | GraphTraverseQuery:
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_query_type_0 = GraphMatchQuery.from_dict(data)

                    return componentsschemas_graph_query_type_0
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_query_type_1 = GraphTraverseQuery.from_dict(data)

                    return componentsschemas_graph_query_type_1
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                try:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_query_type_2 = GraphShortestPathQuery.from_dict(data)

                    return componentsschemas_graph_query_type_2
                except (TypeError, ValueError, AttributeError, KeyError):
                    pass
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_query_type_3 = GraphKShortestPathsQuery.from_dict(data)

                return componentsschemas_graph_query_type_3

            additional_property = _parse_additional_property(prop_dict)

            additional_properties[prop_name] = additional_property

        query_request_graph_queries.additional_properties = additional_properties
        return query_request_graph_queries

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(
        self, key: str
    ) -> GraphKShortestPathsQuery | GraphMatchQuery | GraphShortestPathQuery | GraphTraverseQuery:
        return self.additional_properties[key]

    def __setitem__(
        self, key: str, value: GraphKShortestPathsQuery | GraphMatchQuery | GraphShortestPathQuery | GraphTraverseQuery
    ) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
