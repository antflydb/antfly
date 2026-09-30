from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.edge_direction import EdgeDirection
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_relationship_filter import GraphRelationshipFilter


T = TypeVar("T", bound="PatternEdgeStep")


@_attrs_define
class PatternEdgeStep:
    """Deprecated linear graph_searches pattern edge.

    Attributes:
        edge_filter (GraphRelationshipFilter | Unset): AND predicates applied to every relationship before neighbor
            admission, path ranking, and match counting. Missing or null properties fail comparisons, including ne; use
            explicit null operators. Maximum 64 predicates and 64 KiB of predicate fields and values. Time intervals have
            inclusive lower and exclusive upper bounds. Missing/null valid-time bounds are open; known_at requires a
            created_at value. Invalid timestamp properties never match.
        types (list[str] | Unset): Empty or omitted matches every edge type; otherwise at most 64 unique types totaling
            at most 64 KiB.
        direction (EdgeDirection | Unset): Direction of edges to query:
            - out: Outgoing edges from the node
            - in: Incoming edges to the node
            - both: Both outgoing and incoming edges
        min_hops (int | Unset):  Default: 1.
        max_hops (int | Unset):  Default: 1.
        min_weight (float | Unset):
        max_weight (float | Unset):
    """

    edge_filter: GraphRelationshipFilter | Unset = UNSET
    types: list[str] | Unset = UNSET
    direction: EdgeDirection | Unset = UNSET
    min_hops: int | Unset = 1
    max_hops: int | Unset = 1
    min_weight: float | Unset = UNSET
    max_weight: float | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        edge_filter: dict[str, Any] | Unset = UNSET
        if not isinstance(self.edge_filter, Unset):
            edge_filter = self.edge_filter.to_dict()

        types: list[str] | Unset = UNSET
        if not isinstance(self.types, Unset):
            types = self.types

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        min_hops = self.min_hops

        max_hops = self.max_hops

        min_weight = self.min_weight

        max_weight = self.max_weight

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if edge_filter is not UNSET:
            field_dict["edge_filter"] = edge_filter
        if types is not UNSET:
            field_dict["types"] = types
        if direction is not UNSET:
            field_dict["direction"] = direction
        if min_hops is not UNSET:
            field_dict["min_hops"] = min_hops
        if max_hops is not UNSET:
            field_dict["max_hops"] = max_hops
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if max_weight is not UNSET:
            field_dict["max_weight"] = max_weight

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_relationship_filter import GraphRelationshipFilter

        d = dict(src_dict)
        _edge_filter = d.pop("edge_filter", UNSET)
        edge_filter: GraphRelationshipFilter | Unset
        if isinstance(_edge_filter, Unset):
            edge_filter = UNSET
        else:
            edge_filter = GraphRelationshipFilter.from_dict(_edge_filter)

        types = cast(list[str], d.pop("types", UNSET))

        _direction = d.pop("direction", UNSET)
        direction: EdgeDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EdgeDirection(_direction)

        min_hops = d.pop("min_hops", UNSET)

        max_hops = d.pop("max_hops", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        max_weight = d.pop("max_weight", UNSET)

        pattern_edge_step = cls(
            edge_filter=edge_filter,
            types=types,
            direction=direction,
            min_hops=min_hops,
            max_hops=max_hops,
            min_weight=min_weight,
            max_weight=max_weight,
        )

        pattern_edge_step.additional_properties = d
        return pattern_edge_step

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
