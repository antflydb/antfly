from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_match_edge import GraphMatchEdge
    from ..models.graph_match_nodes import GraphMatchNodes
    from ..models.graph_optional_match import GraphOptionalMatch
    from ..models.graph_where_and import GraphWhereAnd
    from ..models.graph_where_not_equal import GraphWhereNotEqual
    from ..models.graph_where_not_exists import GraphWhereNotExists


T = TypeVar("T", bound="GraphMatch")


@_attrs_define
class GraphMatch:
    """
    Attributes:
        anchor (str): Alias enumerated from the query table as the source relation. Every other alias is reached through
            graph edges and may resolve to a table-qualified target identity. An `ids` filter, or a disjunction made only of
            `ids` filters, uses the table's primary identity access path and needs no secondary index. Stored-field
            predicates and row-level authorization filters on this alias must have native index coverage so Antfly can
            enumerate the complete relation in `_id` order; otherwise the request fails with
            `graph_anchor_filter_requires_index`.
        nodes (GraphMatchNodes): Aliases are limited to 128 Unicode code points and cannot have leading or trailing
            whitespace.
        edges (list[GraphMatchEdge]):
        where (GraphWhereAnd | GraphWhereNotEqual | GraphWhereNotExists | Unset):
        optional (list[GraphOptionalMatch] | Unset):
    """

    anchor: str
    nodes: GraphMatchNodes
    edges: list[GraphMatchEdge]
    where: GraphWhereAnd | GraphWhereNotEqual | GraphWhereNotExists | Unset = UNSET
    optional: list[GraphOptionalMatch] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_where_and import GraphWhereAnd
        from ..models.graph_where_not_equal import GraphWhereNotEqual

        anchor = self.anchor

        nodes = self.nodes.to_dict()

        edges = []
        for edges_item_data in self.edges:
            edges_item = edges_item_data.to_dict()
            edges.append(edges_item)

        where: dict[str, Any] | Unset
        if isinstance(self.where, Unset):
            where = UNSET
        elif isinstance(self.where, GraphWhereAnd):
            where = self.where.to_dict()
        elif isinstance(self.where, GraphWhereNotEqual):
            where = self.where.to_dict()
        else:
            where = self.where.to_dict()

        optional: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.optional, Unset):
            optional = []
            for optional_item_data in self.optional:
                optional_item = optional_item_data.to_dict()
                optional.append(optional_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "anchor": anchor,
                "nodes": nodes,
                "edges": edges,
            }
        )
        if where is not UNSET:
            field_dict["where"] = where
        if optional is not UNSET:
            field_dict["optional"] = optional

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_match_edge import GraphMatchEdge
        from ..models.graph_match_nodes import GraphMatchNodes
        from ..models.graph_optional_match import GraphOptionalMatch
        from ..models.graph_where_and import GraphWhereAnd
        from ..models.graph_where_not_equal import GraphWhereNotEqual
        from ..models.graph_where_not_exists import GraphWhereNotExists

        d = dict(src_dict)
        anchor = d.pop("anchor")

        nodes = GraphMatchNodes.from_dict(d.pop("nodes"))

        edges = []
        _edges = d.pop("edges")
        for edges_item_data in _edges:
            edges_item = GraphMatchEdge.from_dict(edges_item_data)

            edges.append(edges_item)

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

        _optional = d.pop("optional", UNSET)
        optional: list[GraphOptionalMatch] | Unset = UNSET
        if _optional is not UNSET:
            optional = []
            for optional_item_data in _optional:
                optional_item = GraphOptionalMatch.from_dict(optional_item_data)

                optional.append(optional_item)

        graph_match = cls(
            anchor=anchor,
            nodes=nodes,
            edges=edges,
            where=where,
            optional=optional,
        )

        return graph_match
