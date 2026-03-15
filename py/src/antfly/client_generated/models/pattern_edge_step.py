from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.edge_direction import EdgeDirection
from ..types import UNSET, Unset

T = TypeVar("T", bound="PatternEdgeStep")


@_attrs_define
class PatternEdgeStep:
    """Edge constraints in a pattern step

    Attributes:
        types (Union[Unset, list[str]]): Edge types to traverse (empty = any)
        direction (Union[Unset, EdgeDirection]): Direction of edges to query:
            - out: Outgoing edges from the node
            - in: Incoming edges to the node
            - both: Both outgoing and incoming edges
        min_hops (Union[Unset, int]): Minimum number of hops (1 = direct edge) Default: 1.
        max_hops (Union[Unset, int]): Maximum number of hops (>1 = variable-length path) Default: 1.
        min_weight (Union[Unset, float]): Minimum edge weight filter
        max_weight (Union[Unset, float]): Maximum edge weight filter
    """

    types: Union[Unset, list[str]] = UNSET
    direction: Union[Unset, EdgeDirection] = UNSET
    min_hops: Union[Unset, int] = 1
    max_hops: Union[Unset, int] = 1
    min_weight: Union[Unset, float] = UNSET
    max_weight: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        types: Union[Unset, list[str]] = UNSET
        if not isinstance(self.types, Unset):
            types = self.types

        direction: Union[Unset, str] = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        min_hops = self.min_hops

        max_hops = self.max_hops

        min_weight = self.min_weight

        max_weight = self.max_weight

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
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
        d = dict(src_dict)
        types = cast(list[str], d.pop("types", UNSET))

        _direction = d.pop("direction", UNSET)
        direction: Union[Unset, EdgeDirection]
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EdgeDirection(_direction)

        min_hops = d.pop("min_hops", UNSET)

        max_hops = d.pop("max_hops", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        max_weight = d.pop("max_weight", UNSET)

        pattern_edge_step = cls(
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
