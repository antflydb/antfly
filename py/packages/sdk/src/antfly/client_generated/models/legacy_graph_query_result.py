from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_query_type import GraphQueryType
from ..models.legacy_graph_query_result_kind import LegacyGraphQueryResultKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_result_node import GraphResultNode
    from ..models.path import Path
    from ..models.pattern_match import PatternMatch


T = TypeVar("T", bound="LegacyGraphQueryResult")


@_attrs_define
class LegacyGraphQueryResult:
    """Deprecated graph_searches response envelope.

    Attributes:
        kind (LegacyGraphQueryResultKind): Stable discriminator for the deprecated graph result shape.
        type_ (GraphQueryType): Deprecated discriminator used by LegacyGraphQuery.
        nodes (list[GraphResultNode]): Result nodes.
        paths (list[Path]): Result paths.
        total (int): Deprecated graph_searches result count; use stats or a named count aggregate.
        took (int): Query execution time
        matches (list[PatternMatch] | Unset): Deprecated graph_searches pattern results; use rows for graph_queries.
    """

    kind: LegacyGraphQueryResultKind
    type_: GraphQueryType
    nodes: list[GraphResultNode]
    paths: list[Path]
    total: int
    took: int
    matches: list[PatternMatch] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        type_ = self.type_.value

        nodes = []
        for nodes_item_data in self.nodes:
            nodes_item = nodes_item_data.to_dict()
            nodes.append(nodes_item)

        paths = []
        for paths_item_data in self.paths:
            paths_item = paths_item_data.to_dict()
            paths.append(paths_item)

        total = self.total

        took = self.took

        matches: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.matches, Unset):
            matches = []
            for matches_item_data in self.matches:
                matches_item = matches_item_data.to_dict()
                matches.append(matches_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "type": type_,
                "nodes": nodes,
                "paths": paths,
                "total": total,
                "took": took,
            }
        )
        if matches is not UNSET:
            field_dict["matches"] = matches

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_result_node import GraphResultNode
        from ..models.path import Path
        from ..models.pattern_match import PatternMatch

        d = dict(src_dict)
        kind = LegacyGraphQueryResultKind(d.pop("kind"))

        type_ = GraphQueryType(d.pop("type"))

        nodes = []
        _nodes = d.pop("nodes")
        for nodes_item_data in _nodes:
            nodes_item = GraphResultNode.from_dict(nodes_item_data)

            nodes.append(nodes_item)

        paths = []
        _paths = d.pop("paths")
        for paths_item_data in _paths:
            paths_item = Path.from_dict(paths_item_data)

            paths.append(paths_item)

        total = d.pop("total")

        took = d.pop("took")

        _matches = d.pop("matches", UNSET)
        matches: list[PatternMatch] | Unset = UNSET
        if _matches is not UNSET:
            matches = []
            for matches_item_data in _matches:
                matches_item = PatternMatch.from_dict(matches_item_data)

                matches.append(matches_item)

        legacy_graph_query_result = cls(
            kind=kind,
            type_=type_,
            nodes=nodes,
            paths=paths,
            total=total,
            took=took,
            matches=matches,
        )

        return legacy_graph_query_result
