from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_nodes_result_kind import GraphNodesResultKind

if TYPE_CHECKING:
    from ..models.graph_path import GraphPath
    from ..models.graph_query_stats import GraphQueryStats
    from ..models.graph_result_node import GraphResultNode


T = TypeVar("T", bound="GraphNodesResult")


@_attrs_define
class GraphNodesResult:
    """Composable result nodes and any materialized paths from a canonical traversal or path query.

    Attributes:
        kind (GraphNodesResultKind): Stable discriminator for the graph result shape.
        nodes (list[GraphResultNode]): Traversal result nodes. Path operations emit one terminal result node per
            returned path; inspect paths[].nodes for complete path membership.
        paths (list[GraphPath]): Materialized result paths; empty when paths were not requested or produced.
        stats (GraphQueryStats):
        took (int): Query execution time.
    """

    kind: GraphNodesResultKind
    nodes: list[GraphResultNode]
    paths: list[GraphPath]
    stats: GraphQueryStats
    took: int

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

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
                "kind": kind,
                "nodes": nodes,
                "paths": paths,
                "stats": stats,
                "took": took,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_path import GraphPath
        from ..models.graph_query_stats import GraphQueryStats
        from ..models.graph_result_node import GraphResultNode

        d = dict(src_dict)
        kind = GraphNodesResultKind(d.pop("kind"))

        nodes = []
        _nodes = d.pop("nodes")
        for nodes_item_data in _nodes:
            nodes_item = GraphResultNode.from_dict(nodes_item_data)

            nodes.append(nodes_item)

        paths = []
        _paths = d.pop("paths")
        for paths_item_data in _paths:
            paths_item = GraphPath.from_dict(paths_item_data)

            paths.append(paths_item)

        stats = GraphQueryStats.from_dict(d.pop("stats"))

        took = d.pop("took")

        graph_nodes_result = cls(
            kind=kind,
            nodes=nodes,
            paths=paths,
            stats=stats,
            took=took,
        )

        return graph_nodes_result
