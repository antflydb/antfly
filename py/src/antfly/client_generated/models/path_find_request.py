from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.edge_direction import EdgeDirection
from ..models.path_find_weight_mode import PathFindWeightMode
from ..types import UNSET, Unset

T = TypeVar("T", bound="PathFindRequest")


@_attrs_define
class PathFindRequest:
    """
    Attributes:
        source (str): Source node key (base64-encoded)
        target (str): Target node key (base64-encoded)
        edge_types (Union[Unset, list[str]]): Filter by specific edge types
        max_depth (Union[Unset, int]):  Default: 10.
        weight_mode (Union[Unset, PathFindWeightMode]): Algorithm for path finding:
            - min_hops: Shortest path by hop count (breadth-first search, ignores weights)
            - max_weight: Path with maximum product of edge weights (strongest connection chain)
            - min_weight: Path with minimum sum of edge weights (lowest cost route)
        k (Union[Unset, int]):  Default: 1.
        min_weight (Union[Unset, float]):
        max_weight (Union[Unset, float]):
        direction (Union[Unset, EdgeDirection]): Direction of edges to query:
            - out: Outgoing edges from the node
            - in: Incoming edges to the node
            - both: Both outgoing and incoming edges
    """

    source: str
    target: str
    edge_types: Union[Unset, list[str]] = UNSET
    max_depth: Union[Unset, int] = 10
    weight_mode: Union[Unset, PathFindWeightMode] = UNSET
    k: Union[Unset, int] = 1
    min_weight: Union[Unset, float] = UNSET
    max_weight: Union[Unset, float] = UNSET
    direction: Union[Unset, EdgeDirection] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        source = self.source

        target = self.target

        edge_types: Union[Unset, list[str]] = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        max_depth = self.max_depth

        weight_mode: Union[Unset, str] = UNSET
        if not isinstance(self.weight_mode, Unset):
            weight_mode = self.weight_mode.value

        k = self.k

        min_weight = self.min_weight

        max_weight = self.max_weight

        direction: Union[Unset, str] = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "source": source,
                "target": target,
            }
        )
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth
        if weight_mode is not UNSET:
            field_dict["weight_mode"] = weight_mode
        if k is not UNSET:
            field_dict["k"] = k
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if max_weight is not UNSET:
            field_dict["max_weight"] = max_weight
        if direction is not UNSET:
            field_dict["direction"] = direction

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        source = d.pop("source")

        target = d.pop("target")

        edge_types = cast(list[str], d.pop("edge_types", UNSET))

        max_depth = d.pop("max_depth", UNSET)

        _weight_mode = d.pop("weight_mode", UNSET)
        weight_mode: Union[Unset, PathFindWeightMode]
        if isinstance(_weight_mode, Unset):
            weight_mode = UNSET
        else:
            weight_mode = PathFindWeightMode(_weight_mode)

        k = d.pop("k", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        max_weight = d.pop("max_weight", UNSET)

        _direction = d.pop("direction", UNSET)
        direction: Union[Unset, EdgeDirection]
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EdgeDirection(_direction)

        path_find_request = cls(
            source=source,
            target=target,
            edge_types=edge_types,
            max_depth=max_depth,
            weight_mode=weight_mode,
            k=k,
            min_weight=min_weight,
            max_weight=max_weight,
            direction=direction,
        )

        path_find_request.additional_properties = d
        return path_find_request

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
