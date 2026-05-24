from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.edge_direction import EdgeDirection
from ..models.path_weight_mode import PathWeightMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_query_params_algorithm_params import GraphQueryParamsAlgorithmParams
    from ..models.node_filter import NodeFilter


T = TypeVar("T", bound="GraphQueryParams")


@_attrs_define
class GraphQueryParams:
    """Parameters for graph traversal and pathfinding

    Attributes:
        edge_types (list[str] | Unset): Filter by edge types
        direction (EdgeDirection | Unset): Direction of edges to query:
            - out: Outgoing edges from the node
            - in: Incoming edges to the node
            - both: Both outgoing and incoming edges
        max_depth (int | Unset): Maximum traversal depth
        min_weight (float | Unset): Minimum edge weight
        max_weight (float | Unset): Maximum edge weight
        max_results (int | Unset): Maximum number of results (traversal)
        deduplicate_nodes (bool | Unset): Remove duplicate nodes (traversal)
        include_paths (bool | Unset): Include path information (traversal)
        weight_mode (PathWeightMode | Unset): Path weighting algorithm for pathfinding:
            - min_hops: Minimize number of edges
            - min_weight: Minimize sum of edge weights
            - max_weight: Maximize product of edge weights
        k (int | Unset): Number of paths to find (k-shortest-paths)
        node_filter (NodeFilter | Unset): Filter nodes during graph traversal using existing query primitives
        algorithm (str | Unset): Graph algorithm to run (e.g., 'pagerank', 'betweenness')
        algorithm_params (GraphQueryParamsAlgorithmParams | Unset): Parameters for the graph algorithm
    """

    edge_types: list[str] | Unset = UNSET
    direction: EdgeDirection | Unset = UNSET
    max_depth: int | Unset = UNSET
    min_weight: float | Unset = UNSET
    max_weight: float | Unset = UNSET
    max_results: int | Unset = UNSET
    deduplicate_nodes: bool | Unset = UNSET
    include_paths: bool | Unset = UNSET
    weight_mode: PathWeightMode | Unset = UNSET
    k: int | Unset = UNSET
    node_filter: NodeFilter | Unset = UNSET
    algorithm: str | Unset = UNSET
    algorithm_params: GraphQueryParamsAlgorithmParams | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        max_depth = self.max_depth

        min_weight = self.min_weight

        max_weight = self.max_weight

        max_results = self.max_results

        deduplicate_nodes = self.deduplicate_nodes

        include_paths = self.include_paths

        weight_mode: str | Unset = UNSET
        if not isinstance(self.weight_mode, Unset):
            weight_mode = self.weight_mode.value

        k = self.k

        node_filter: dict[str, Any] | Unset = UNSET
        if not isinstance(self.node_filter, Unset):
            node_filter = self.node_filter.to_dict()

        algorithm = self.algorithm

        algorithm_params: dict[str, Any] | Unset = UNSET
        if not isinstance(self.algorithm_params, Unset):
            algorithm_params = self.algorithm_params.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if direction is not UNSET:
            field_dict["direction"] = direction
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if max_weight is not UNSET:
            field_dict["max_weight"] = max_weight
        if max_results is not UNSET:
            field_dict["max_results"] = max_results
        if deduplicate_nodes is not UNSET:
            field_dict["deduplicate_nodes"] = deduplicate_nodes
        if include_paths is not UNSET:
            field_dict["include_paths"] = include_paths
        if weight_mode is not UNSET:
            field_dict["weight_mode"] = weight_mode
        if k is not UNSET:
            field_dict["k"] = k
        if node_filter is not UNSET:
            field_dict["node_filter"] = node_filter
        if algorithm is not UNSET:
            field_dict["algorithm"] = algorithm
        if algorithm_params is not UNSET:
            field_dict["algorithm_params"] = algorithm_params

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_query_params_algorithm_params import GraphQueryParamsAlgorithmParams
        from ..models.node_filter import NodeFilter

        d = dict(src_dict)
        edge_types = cast(list[str], d.pop("edge_types", UNSET))

        _direction = d.pop("direction", UNSET)
        direction: EdgeDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EdgeDirection(_direction)

        max_depth = d.pop("max_depth", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        max_weight = d.pop("max_weight", UNSET)

        max_results = d.pop("max_results", UNSET)

        deduplicate_nodes = d.pop("deduplicate_nodes", UNSET)

        include_paths = d.pop("include_paths", UNSET)

        _weight_mode = d.pop("weight_mode", UNSET)
        weight_mode: PathWeightMode | Unset
        if isinstance(_weight_mode, Unset):
            weight_mode = UNSET
        else:
            weight_mode = PathWeightMode(_weight_mode)

        k = d.pop("k", UNSET)

        _node_filter = d.pop("node_filter", UNSET)
        node_filter: NodeFilter | Unset
        if isinstance(_node_filter, Unset):
            node_filter = UNSET
        else:
            node_filter = NodeFilter.from_dict(_node_filter)

        algorithm = d.pop("algorithm", UNSET)

        _algorithm_params = d.pop("algorithm_params", UNSET)
        algorithm_params: GraphQueryParamsAlgorithmParams | Unset
        if isinstance(_algorithm_params, Unset):
            algorithm_params = UNSET
        else:
            algorithm_params = GraphQueryParamsAlgorithmParams.from_dict(_algorithm_params)

        graph_query_params = cls(
            edge_types=edge_types,
            direction=direction,
            max_depth=max_depth,
            min_weight=min_weight,
            max_weight=max_weight,
            max_results=max_results,
            deduplicate_nodes=deduplicate_nodes,
            include_paths=include_paths,
            weight_mode=weight_mode,
            k=k,
            node_filter=node_filter,
            algorithm=algorithm,
            algorithm_params=algorithm_params,
        )

        graph_query_params.additional_properties = d
        return graph_query_params

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
