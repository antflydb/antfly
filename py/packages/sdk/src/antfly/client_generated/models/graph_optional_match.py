from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_match_edge import GraphMatchEdge
    from ..models.graph_optional_match_nodes import GraphOptionalMatchNodes
    from ..models.graph_where_and import GraphWhereAnd
    from ..models.graph_where_not_equal import GraphWhereNotEqual
    from ..models.graph_where_not_exists import GraphWhereNotExists


T = TypeVar("T", bound="GraphOptionalMatch")


@_attrs_define
class GraphOptionalMatch:
    """
    Attributes:
        edges (list[GraphMatchEdge]):
        nodes (GraphOptionalMatchNodes | Unset): Aliases are limited to 128 Unicode code points and cannot have leading
            or trailing whitespace.
        where (GraphWhereAnd | GraphWhereNotEqual | GraphWhereNotExists | Unset):
    """

    edges: list[GraphMatchEdge]
    nodes: GraphOptionalMatchNodes | Unset = UNSET
    where: GraphWhereAnd | GraphWhereNotEqual | GraphWhereNotExists | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_where_and import GraphWhereAnd
        from ..models.graph_where_not_equal import GraphWhereNotEqual

        edges = []
        for edges_item_data in self.edges:
            edges_item = edges_item_data.to_dict()
            edges.append(edges_item)

        nodes: dict[str, Any] | Unset = UNSET
        if not isinstance(self.nodes, Unset):
            nodes = self.nodes.to_dict()

        where: dict[str, Any] | Unset
        if isinstance(self.where, Unset):
            where = UNSET
        elif isinstance(self.where, GraphWhereAnd):
            where = self.where.to_dict()
        elif isinstance(self.where, GraphWhereNotEqual):
            where = self.where.to_dict()
        else:
            where = self.where.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "edges": edges,
            }
        )
        if nodes is not UNSET:
            field_dict["nodes"] = nodes
        if where is not UNSET:
            field_dict["where"] = where

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_match_edge import GraphMatchEdge
        from ..models.graph_optional_match_nodes import GraphOptionalMatchNodes
        from ..models.graph_where_and import GraphWhereAnd
        from ..models.graph_where_not_equal import GraphWhereNotEqual
        from ..models.graph_where_not_exists import GraphWhereNotExists

        d = dict(src_dict)
        edges = []
        _edges = d.pop("edges")
        for edges_item_data in _edges:
            edges_item = GraphMatchEdge.from_dict(edges_item_data)

            edges.append(edges_item)

        _nodes = d.pop("nodes", UNSET)
        nodes: GraphOptionalMatchNodes | Unset
        if isinstance(_nodes, Unset):
            nodes = UNSET
        else:
            nodes = GraphOptionalMatchNodes.from_dict(_nodes)

        def _parse_where(data: object) -> GraphWhereAnd | GraphWhereNotEqual | GraphWhereNotExists | Unset:
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_where_expression_type_0 = GraphWhereAnd.from_dict(data)

                return componentsschemas_graph_where_expression_type_0
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                componentsschemas_graph_where_expression_type_1 = GraphWhereNotEqual.from_dict(data)

                return componentsschemas_graph_where_expression_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            if not isinstance(data, dict):
                raise TypeError()
            componentsschemas_graph_where_expression_type_2 = GraphWhereNotExists.from_dict(data)

            return componentsschemas_graph_where_expression_type_2

        where = _parse_where(d.pop("where", UNSET))

        graph_optional_match = cls(
            edges=edges,
            nodes=nodes,
            where=where,
        )

        return graph_optional_match
