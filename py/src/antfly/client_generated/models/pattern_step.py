from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.node_filter import NodeFilter
    from ..models.pattern_edge_step import PatternEdgeStep


T = TypeVar("T", bound="PatternStep")


@_attrs_define
class PatternStep:
    """A step in a graph pattern query

    Attributes:
        alias (Union[Unset, str]): Name for this node (reuse alias for cycle detection)
        node_filter (Union[Unset, NodeFilter]): Filter nodes during graph traversal using existing query primitives
        edge (Union[Unset, PatternEdgeStep]): Edge constraints in a pattern step
    """

    alias: Union[Unset, str] = UNSET
    node_filter: Union[Unset, "NodeFilter"] = UNSET
    edge: Union[Unset, "PatternEdgeStep"] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        alias = self.alias

        node_filter: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.node_filter, Unset):
            node_filter = self.node_filter.to_dict()

        edge: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.edge, Unset):
            edge = self.edge.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if alias is not UNSET:
            field_dict["alias"] = alias
        if node_filter is not UNSET:
            field_dict["node_filter"] = node_filter
        if edge is not UNSET:
            field_dict["edge"] = edge

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.node_filter import NodeFilter
        from ..models.pattern_edge_step import PatternEdgeStep

        d = dict(src_dict)
        alias = d.pop("alias", UNSET)

        _node_filter = d.pop("node_filter", UNSET)
        node_filter: Union[Unset, NodeFilter]
        if isinstance(_node_filter, Unset):
            node_filter = UNSET
        else:
            node_filter = NodeFilter.from_dict(_node_filter)

        _edge = d.pop("edge", UNSET)
        edge: Union[Unset, PatternEdgeStep]
        if isinstance(_edge, Unset):
            edge = UNSET
        else:
            edge = PatternEdgeStep.from_dict(_edge)

        pattern_step = cls(
            alias=alias,
            node_filter=node_filter,
            edge=edge,
        )

        pattern_step.additional_properties = d
        return pattern_step

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
