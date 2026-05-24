from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.edge_direction import EdgeDirection
from ..types import UNSET, Unset

T = TypeVar("T", bound="TraversalRules")


@_attrs_define
class TraversalRules:
    """Rules for graph traversal

    Attributes:
        edge_types (list[str] | Unset): Filter edges by type (empty = all types)
        min_weight (float | Unset): Minimum edge weight filter Default: 0.0.
        max_weight (float | Unset): Maximum edge weight filter Default: 1.0.
        direction (EdgeDirection | Unset): Direction of edges to query:
            - out: Outgoing edges from the node
            - in: Incoming edges to the node
            - both: Both outgoing and incoming edges
        max_depth (int | Unset): Maximum traversal depth (0 = unlimited) Default: 3.
        max_results (int | Unset): Maximum results to return (0 = unlimited) Default: 100.
        include_paths (bool | Unset): Include path information in results Default: False.
        deduplicate_nodes (bool | Unset): Visit each node only once Default: True.
    """

    edge_types: list[str] | Unset = UNSET
    min_weight: float | Unset = 0.0
    max_weight: float | Unset = 1.0
    direction: EdgeDirection | Unset = UNSET
    max_depth: int | Unset = 3
    max_results: int | Unset = 100
    include_paths: bool | Unset = False
    deduplicate_nodes: bool | Unset = True
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        min_weight = self.min_weight

        max_weight = self.max_weight

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        max_depth = self.max_depth

        max_results = self.max_results

        include_paths = self.include_paths

        deduplicate_nodes = self.deduplicate_nodes

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if max_weight is not UNSET:
            field_dict["max_weight"] = max_weight
        if direction is not UNSET:
            field_dict["direction"] = direction
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth
        if max_results is not UNSET:
            field_dict["max_results"] = max_results
        if include_paths is not UNSET:
            field_dict["include_paths"] = include_paths
        if deduplicate_nodes is not UNSET:
            field_dict["deduplicate_nodes"] = deduplicate_nodes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        edge_types = cast(list[str], d.pop("edge_types", UNSET))

        min_weight = d.pop("min_weight", UNSET)

        max_weight = d.pop("max_weight", UNSET)

        _direction = d.pop("direction", UNSET)
        direction: EdgeDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EdgeDirection(_direction)

        max_depth = d.pop("max_depth", UNSET)

        max_results = d.pop("max_results", UNSET)

        include_paths = d.pop("include_paths", UNSET)

        deduplicate_nodes = d.pop("deduplicate_nodes", UNSET)

        traversal_rules = cls(
            edge_types=edge_types,
            min_weight=min_weight,
            max_weight=max_weight,
            direction=direction,
            max_depth=max_depth,
            max_results=max_results,
            include_paths=include_paths,
            deduplicate_nodes=deduplicate_nodes,
        )

        traversal_rules.additional_properties = d
        return traversal_rules

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
