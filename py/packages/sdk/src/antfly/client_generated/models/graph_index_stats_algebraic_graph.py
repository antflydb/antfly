from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_index_stats_algebraic_graph_traversal import GraphIndexStatsAlgebraicGraphTraversal


T = TypeVar("T", bound="GraphIndexStatsAlgebraicGraph")


@_attrs_define
class GraphIndexStatsAlgebraicGraph:
    """Algebraic graph execution health for bounded semiring traversal.

    Attributes:
        traversal (GraphIndexStatsAlgebraicGraphTraversal | Unset):
    """

    traversal: GraphIndexStatsAlgebraicGraphTraversal | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        traversal: dict[str, Any] | Unset = UNSET
        if not isinstance(self.traversal, Unset):
            traversal = self.traversal.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if traversal is not UNSET:
            field_dict["traversal"] = traversal

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_index_stats_algebraic_graph_traversal import GraphIndexStatsAlgebraicGraphTraversal

        d = dict(src_dict)
        _traversal = d.pop("traversal", UNSET)
        traversal: GraphIndexStatsAlgebraicGraphTraversal | Unset
        if isinstance(_traversal, Unset):
            traversal = UNSET
        else:
            traversal = GraphIndexStatsAlgebraicGraphTraversal.from_dict(_traversal)

        graph_index_stats_algebraic_graph = cls(
            traversal=traversal,
        )

        graph_index_stats_algebraic_graph.additional_properties = d
        return graph_index_stats_algebraic_graph

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
