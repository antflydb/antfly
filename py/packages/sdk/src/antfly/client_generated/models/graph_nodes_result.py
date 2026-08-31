from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_nodes_result_kind import GraphNodesResultKind

if TYPE_CHECKING:
    from ..models.graph_result_node import GraphResultNode
    from ..models.graph_result_stats import GraphResultStats


T = TypeVar("T", bound="GraphNodesResult")


@_attrs_define
class GraphNodesResult:
    """Composable results from a canonical traversal query.

    Attributes:
        kind (GraphNodesResultKind): Stable discriminator for the graph result shape.
        nodes (list[GraphResultNode]): Traversal result nodes; requested paths are stored on each node.
        stats (GraphResultStats): Completion statistics for a bounded graph result.
    """

    kind: GraphNodesResultKind
    nodes: list[GraphResultNode]
    stats: GraphResultStats

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        nodes = []
        for nodes_item_data in self.nodes:
            nodes_item = nodes_item_data.to_dict()
            nodes.append(nodes_item)

        stats = self.stats.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "nodes": nodes,
                "stats": stats,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_result_node import GraphResultNode
        from ..models.graph_result_stats import GraphResultStats

        d = dict(src_dict)
        kind = GraphNodesResultKind(d.pop("kind"))

        nodes = []
        _nodes = d.pop("nodes")
        for nodes_item_data in _nodes:
            nodes_item = GraphResultNode.from_dict(nodes_item_data)

            nodes.append(nodes_item)

        stats = GraphResultStats.from_dict(d.pop("stats"))

        graph_nodes_result = cls(
            kind=kind,
            nodes=nodes,
            stats=stats,
        )

        return graph_nodes_result
