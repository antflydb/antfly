from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_path_objective import GraphPathObjective

if TYPE_CHECKING:
    from ..models.graph_path_edge import GraphPathEdge
    from ..models.graph_path_endpoint import GraphPathEndpoint


T = TypeVar("T", bound="GraphPath")


@_attrs_define
class GraphPath:
    """An ordered canonical graph path with table-qualified node identities and a self-describing ranking score.

    Attributes:
        nodes (list[GraphPathEndpoint]): Ordered node identities. Table is omitted for nodes in the query table.
        edges (list[GraphPathEdge]): Ordered edges; edges[i] traverses from nodes[i] to nodes[i + 1].
        objective (GraphPathObjective): Objective used to rank graph paths:
            - min_hops: Minimize the number of edges.
            - min_weight_sum: Minimize the sum of finite non-negative edge weights.
            - max_weight_product: Maximize the product of edge weights, requiring every traversed weight to be in [0,1].
        weight_sum (float): Sum of raw edge weights along the path, independent of the selected ranking objective.
        objective_value (float): The user-facing value optimized by objective; edge count for min_hops, weight_sum for
            min_weight_sum, and the raw edge-weight product for max_weight_product.
        length (int):
    """

    nodes: list[GraphPathEndpoint]
    edges: list[GraphPathEdge]
    objective: GraphPathObjective
    weight_sum: float
    objective_value: float
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

        objective = self.objective.value

        weight_sum = self.weight_sum

        objective_value = self.objective_value

        length = self.length

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "nodes": nodes,
                "edges": edges,
                "objective": objective,
                "weight_sum": weight_sum,
                "objective_value": objective_value,
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

        objective = GraphPathObjective(d.pop("objective"))

        weight_sum = d.pop("weight_sum")

        objective_value = d.pop("objective_value")

        length = d.pop("length")

        graph_path = cls(
            nodes=nodes,
            edges=edges,
            objective=objective,
            weight_sum=weight_sum,
            objective_value=objective_value,
            length=length,
        )

        return graph_path
