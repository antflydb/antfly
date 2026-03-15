from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union, cast

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
        edge_types (Union[Unset, list[str]]): Filter by edge types
        direction (Union[Unset, EdgeDirection]): Direction of edges to query:
            - out: Outgoing edges from the node
            - in: Incoming edges to the node
            - both: Both outgoing and incoming edges
        max_depth (Union[Unset, int]): Maximum traversal depth
        min_weight (Union[Unset, float]): Minimum edge weight
        max_weight (Union[Unset, float]): Maximum edge weight
        max_results (Union[Unset, int]): Maximum number of results (traversal)
        deduplicate_nodes (Union[Unset, bool]): Remove duplicate nodes (traversal)
        include_paths (Union[Unset, bool]): Include path information (traversal)
        weight_mode (Union[Unset, PathWeightMode]): Path weighting algorithm for pathfinding:
            - min_hops: Minimize number of edges
            - min_weight: Minimize sum of edge weights
            - max_weight: Maximize product of edge weights
        k (Union[Unset, int]): Number of paths to find (k-shortest-paths)
        node_filter (Union[Unset, NodeFilter]): Filter nodes during graph traversal using existing query primitives
        algorithm (Union[Unset, str]): Graph algorithm to run (e.g., 'pagerank', 'betweenness')
        algorithm_params (Union[Unset, GraphQueryParamsAlgorithmParams]): Parameters for the graph algorithm
    """

    edge_types: Union[Unset, list[str]] = UNSET
    direction: Union[Unset, EdgeDirection] = UNSET
    max_depth: Union[Unset, int] = UNSET
    min_weight: Union[Unset, float] = UNSET
    max_weight: Union[Unset, float] = UNSET
    max_results: Union[Unset, int] = UNSET
    deduplicate_nodes: Union[Unset, bool] = UNSET
    include_paths: Union[Unset, bool] = UNSET
    weight_mode: Union[Unset, PathWeightMode] = UNSET
    k: Union[Unset, int] = UNSET
    node_filter: Union[Unset, "NodeFilter"] = UNSET
    algorithm: Union[Unset, str] = UNSET
    algorithm_params: Union[Unset, "GraphQueryParamsAlgorithmParams"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        edge_types: Union[Unset, list[str]] = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        direction: Union[Unset, str] = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        max_depth = self.max_depth

        min_weight = self.min_weight

        max_weight = self.max_weight

        max_results = self.max_results

        deduplicate_nodes = self.deduplicate_nodes

        include_paths = self.include_paths

        weight_mode: Union[Unset, str] = UNSET
        if not isinstance(self.weight_mode, Unset):
            weight_mode = self.weight_mode.value

        k = self.k

        node_filter: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.node_filter, Unset):
            node_filter = self.node_filter.to_dict()

        algorithm = self.algorithm

        algorithm_params: Union[Unset, dict[str, Any]] = UNSET
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
        direction: Union[Unset, EdgeDirection]
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
        weight_mode: Union[Unset, PathWeightMode]
        if isinstance(_weight_mode, Unset):
            weight_mode = UNSET
        else:
            weight_mode = PathWeightMode(_weight_mode)

        k = d.pop("k", UNSET)

        _node_filter = d.pop("node_filter", UNSET)
        node_filter: Union[Unset, NodeFilter]
        if isinstance(_node_filter, Unset):
            node_filter = UNSET
        else:
            node_filter = NodeFilter.from_dict(_node_filter)

        algorithm = d.pop("algorithm", UNSET)

        _algorithm_params = d.pop("algorithm_params", UNSET)
        algorithm_params: Union[Unset, GraphQueryParamsAlgorithmParams]
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
