from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphIndexStatsAlgebraicGraphTraversal")


@_attrs_define
class GraphIndexStatsAlgebraicGraphTraversal:
    """
    Attributes:
        attempted (Union[Unset, int]):
        proven (Union[Unset, int]):
        rejected (Union[Unset, int]):
        fallback (Union[Unset, int]):
        result_nodes (Union[Unset, int]):
    """

    attempted: Union[Unset, int] = UNSET
    proven: Union[Unset, int] = UNSET
    rejected: Union[Unset, int] = UNSET
    fallback: Union[Unset, int] = UNSET
    result_nodes: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        attempted = self.attempted

        proven = self.proven

        rejected = self.rejected

        fallback = self.fallback

        result_nodes = self.result_nodes

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if attempted is not UNSET:
            field_dict["attempted"] = attempted
        if proven is not UNSET:
            field_dict["proven"] = proven
        if rejected is not UNSET:
            field_dict["rejected"] = rejected
        if fallback is not UNSET:
            field_dict["fallback"] = fallback
        if result_nodes is not UNSET:
            field_dict["result_nodes"] = result_nodes

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        attempted = d.pop("attempted", UNSET)

        proven = d.pop("proven", UNSET)

        rejected = d.pop("rejected", UNSET)

        fallback = d.pop("fallback", UNSET)

        result_nodes = d.pop("result_nodes", UNSET)

        graph_index_stats_algebraic_graph_traversal = cls(
            attempted=attempted,
            proven=proven,
            rejected=rejected,
            fallback=fallback,
            result_nodes=result_nodes,
        )

        graph_index_stats_algebraic_graph_traversal.additional_properties = d
        return graph_index_stats_algebraic_graph_traversal

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
