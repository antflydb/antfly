from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_query_type import GraphQueryType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_query_result_aggregates import GraphQueryResultAggregates
    from ..models.graph_query_stats import GraphQueryStats
    from ..models.graph_result_node import GraphResultNode
    from ..models.graph_result_row_type_0 import GraphResultRowType0
    from ..models.path import Path
    from ..models.pattern_match import PatternMatch


T = TypeVar("T", bound="GraphQueryResult")


@_attrs_define
class GraphQueryResult:
    """Results of a graph query

    Attributes:
        type_ (GraphQueryType | Unset): Deprecated discriminator used by LegacyGraphQuery.
        nodes (list[GraphResultNode] | Unset): Result nodes
        paths (list[Path] | Unset): Result paths (for pathfinding queries)
        matches (list[PatternMatch] | Unset): Deprecated graph_searches pattern results; use rows for graph_queries.
        total (int | Unset): Deprecated graph_searches result count; use stats or a named count aggregate.
        rows (list[GraphResultRowType0] | Unset):
        aggregates (GraphQueryResultAggregates | Unset):
        stats (GraphQueryStats | Unset):
        took (int | Unset): Query execution time
    """

    type_: GraphQueryType | Unset = UNSET
    nodes: list[GraphResultNode] | Unset = UNSET
    paths: list[Path] | Unset = UNSET
    matches: list[PatternMatch] | Unset = UNSET
    total: int | Unset = UNSET
    rows: list[GraphResultRowType0] | Unset = UNSET
    aggregates: GraphQueryResultAggregates | Unset = UNSET
    stats: GraphQueryStats | Unset = UNSET
    took: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.graph_result_row_type_0 import GraphResultRowType0

        type_: str | Unset = UNSET
        if not isinstance(self.type_, Unset):
            type_ = self.type_.value

        nodes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.nodes, Unset):
            nodes = []
            for nodes_item_data in self.nodes:
                nodes_item = nodes_item_data.to_dict()
                nodes.append(nodes_item)

        paths: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.paths, Unset):
            paths = []
            for paths_item_data in self.paths:
                paths_item = paths_item_data.to_dict()
                paths.append(paths_item)

        matches: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.matches, Unset):
            matches = []
            for matches_item_data in self.matches:
                matches_item = matches_item_data.to_dict()
                matches.append(matches_item)

        total = self.total

        rows: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.rows, Unset):
            rows = []
            for rows_item_data in self.rows:
                rows_item: dict[str, Any]
                if isinstance(rows_item_data, GraphResultRowType0):
                    rows_item = rows_item_data.to_dict()

                rows.append(rows_item)

        aggregates: dict[str, Any] | Unset = UNSET
        if not isinstance(self.aggregates, Unset):
            aggregates = self.aggregates.to_dict()

        stats: dict[str, Any] | Unset = UNSET
        if not isinstance(self.stats, Unset):
            stats = self.stats.to_dict()

        took = self.took

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if type_ is not UNSET:
            field_dict["type"] = type_
        if nodes is not UNSET:
            field_dict["nodes"] = nodes
        if paths is not UNSET:
            field_dict["paths"] = paths
        if matches is not UNSET:
            field_dict["matches"] = matches
        if total is not UNSET:
            field_dict["total"] = total
        if rows is not UNSET:
            field_dict["rows"] = rows
        if aggregates is not UNSET:
            field_dict["aggregates"] = aggregates
        if stats is not UNSET:
            field_dict["stats"] = stats
        if took is not UNSET:
            field_dict["took"] = took

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_query_result_aggregates import GraphQueryResultAggregates
        from ..models.graph_query_stats import GraphQueryStats
        from ..models.graph_result_node import GraphResultNode
        from ..models.graph_result_row_type_0 import GraphResultRowType0
        from ..models.path import Path
        from ..models.pattern_match import PatternMatch

        d = dict(src_dict)
        _type_ = d.pop("type", UNSET)
        type_: GraphQueryType | Unset
        if isinstance(_type_, Unset):
            type_ = UNSET
        else:
            type_ = GraphQueryType(_type_)

        _nodes = d.pop("nodes", UNSET)
        nodes: list[GraphResultNode] | Unset = UNSET
        if _nodes is not UNSET:
            nodes = []
            for nodes_item_data in _nodes:
                nodes_item = GraphResultNode.from_dict(nodes_item_data)

                nodes.append(nodes_item)

        _paths = d.pop("paths", UNSET)
        paths: list[Path] | Unset = UNSET
        if _paths is not UNSET:
            paths = []
            for paths_item_data in _paths:
                paths_item = Path.from_dict(paths_item_data)

                paths.append(paths_item)

        _matches = d.pop("matches", UNSET)
        matches: list[PatternMatch] | Unset = UNSET
        if _matches is not UNSET:
            matches = []
            for matches_item_data in _matches:
                matches_item = PatternMatch.from_dict(matches_item_data)

                matches.append(matches_item)

        total = d.pop("total", UNSET)

        _rows = d.pop("rows", UNSET)
        rows: list[GraphResultRowType0] | Unset = UNSET
        if _rows is not UNSET:
            rows = []
            for rows_item_data in _rows:

                def _parse_rows_item(data: object) -> GraphResultRowType0:
                    if not isinstance(data, dict):
                        raise TypeError()
                    componentsschemas_graph_result_row_type_0 = GraphResultRowType0.from_dict(data)

                    return componentsschemas_graph_result_row_type_0

                rows_item = _parse_rows_item(rows_item_data)

                rows.append(rows_item)

        _aggregates = d.pop("aggregates", UNSET)
        aggregates: GraphQueryResultAggregates | Unset
        if isinstance(_aggregates, Unset):
            aggregates = UNSET
        else:
            aggregates = GraphQueryResultAggregates.from_dict(_aggregates)

        _stats = d.pop("stats", UNSET)
        stats: GraphQueryStats | Unset
        if isinstance(_stats, Unset):
            stats = UNSET
        else:
            stats = GraphQueryStats.from_dict(_stats)

        took = d.pop("took", UNSET)

        graph_query_result = cls(
            type_=type_,
            nodes=nodes,
            paths=paths,
            matches=matches,
            total=total,
            rows=rows,
            aggregates=aggregates,
            stats=stats,
            took=took,
        )

        graph_query_result.additional_properties = d
        return graph_query_result

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
