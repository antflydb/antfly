from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_query_stats import GraphQueryStats
    from ..models.graph_result_node import GraphResultNode
    from ..models.path import Path


T = TypeVar("T", bound="GraphNodesResult")


@_attrs_define
class GraphNodesResult:
    """Nodes and any materialized paths from a canonical traversal or path query.

    Attributes:
        nodes (list[GraphResultNode]): Result nodes.
        paths (list[Path]): Materialized result paths; empty when paths were not requested or produced.
        stats (GraphQueryStats):
        took (int): Query execution time.
    """

    nodes: list[GraphResultNode]
    paths: list[Path]
    stats: GraphQueryStats
    took: int

    def to_dict(self) -> dict[str, Any]:
        nodes = []
        for nodes_item_data in self.nodes:
            nodes_item = nodes_item_data.to_dict()
            nodes.append(nodes_item)

        paths = []
        for paths_item_data in self.paths:
            paths_item = paths_item_data.to_dict()
            paths.append(paths_item)

        stats = self.stats.to_dict()

        took = self.took

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "nodes": nodes,
                "paths": paths,
                "stats": stats,
                "took": took,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_query_stats import GraphQueryStats
        from ..models.graph_result_node import GraphResultNode
        from ..models.path import Path

        d = dict(src_dict)
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

        stats = GraphQueryStats.from_dict(d.pop("stats"))

        took = d.pop("took")

        graph_nodes_result = cls(
            nodes=nodes,
            paths=paths,
            stats=stats,
            took=took,
        )

        return graph_nodes_result
