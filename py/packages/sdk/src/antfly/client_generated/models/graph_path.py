from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_path_edge import GraphPathEdge
    from ..models.graph_path_endpoint import GraphPathEndpoint


T = TypeVar("T", bound="GraphPath")


@_attrs_define
class GraphPath:
    """An ordered canonical graph path with table-qualified node identities.

    Attributes:
        nodes (list[GraphPathEndpoint]): Ordered node identities. Table is omitted for nodes in the query table.
        edges (list[GraphPathEdge]): Ordered edges; edges[i] traverses from nodes[i] to nodes[i + 1].
        total_weight (float): Sum of raw edge weights along the path. Path ordering still follows the selected weight
            mode.
        length (int):
    """

    nodes: list[GraphPathEndpoint]
    edges: list[GraphPathEdge]
    total_weight: float
    length: int

    def to_dict(self) -> dict[str, Any]:
        nodes = []
        for nodes_item_data in self.nodes:
            nodes_item = nodes_item_data.to_dict()
            nodes.append(nodes_item)

        edges = []
        for edges_item_data in self.edges:
            edges_item = edges_item_data.to_dict()
            edges.append(edges_item)

        total_weight = self.total_weight

        length = self.length

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "nodes": nodes,
                "edges": edges,
                "total_weight": total_weight,
                "length": length,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_path_edge import GraphPathEdge
        from ..models.graph_path_endpoint import GraphPathEndpoint

        d = dict(src_dict)
        nodes = []
        _nodes = d.pop("nodes")
        for nodes_item_data in _nodes:
            nodes_item = GraphPathEndpoint.from_dict(nodes_item_data)

            nodes.append(nodes_item)

        edges = []
        _edges = d.pop("edges")
        for edges_item_data in _edges:
            edges_item = GraphPathEdge.from_dict(edges_item_data)

            edges.append(edges_item)

        total_weight = d.pop("total_weight")

        length = d.pop("length")

        graph_path = cls(
            nodes=nodes,
            edges=edges,
            total_weight=total_weight,
            length=length,
        )

        return graph_path
