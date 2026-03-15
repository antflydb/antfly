from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_index_stats_edge_types import GraphIndexStatsEdgeTypes


T = TypeVar("T", bound="GraphIndexStats")


@_attrs_define
class GraphIndexStats:
    """Statistics for graph index

    Attributes:
        error (Union[Unset, str]): Error message if stats could not be retrieved
        total_edges (Union[Unset, int]): Total number of edges in the graph
        edge_types (Union[Unset, GraphIndexStatsEdgeTypes]): Count of edges per edge type
        rebuilding (Union[Unset, bool]): Whether the index is currently rebuilding
        backfill_progress (Union[Unset, float]): Rebuild progress as a ratio from 0.0 to 1.0
        backfill_items_processed (Union[Unset, int]): Number of edges indexed during current rebuild
    """

    error: Union[Unset, str] = UNSET
    total_edges: Union[Unset, int] = UNSET
    edge_types: Union[Unset, "GraphIndexStatsEdgeTypes"] = UNSET
    rebuilding: Union[Unset, bool] = UNSET
    backfill_progress: Union[Unset, float] = UNSET
    backfill_items_processed: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        error = self.error

        total_edges = self.total_edges

        edge_types: Union[Unset, dict[str, Any]] = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types.to_dict()

        rebuilding = self.rebuilding

        backfill_progress = self.backfill_progress

        backfill_items_processed = self.backfill_items_processed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if error is not UNSET:
            field_dict["error"] = error
        if total_edges is not UNSET:
            field_dict["total_edges"] = total_edges
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if rebuilding is not UNSET:
            field_dict["rebuilding"] = rebuilding
        if backfill_progress is not UNSET:
            field_dict["backfill_progress"] = backfill_progress
        if backfill_items_processed is not UNSET:
            field_dict["backfill_items_processed"] = backfill_items_processed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_index_stats_edge_types import GraphIndexStatsEdgeTypes

        d = dict(src_dict)
        error = d.pop("error", UNSET)

        total_edges = d.pop("total_edges", UNSET)

        _edge_types = d.pop("edge_types", UNSET)
        edge_types: Union[Unset, GraphIndexStatsEdgeTypes]
        if isinstance(_edge_types, Unset):
            edge_types = UNSET
        else:
            edge_types = GraphIndexStatsEdgeTypes.from_dict(_edge_types)

        rebuilding = d.pop("rebuilding", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_items_processed = d.pop("backfill_items_processed", UNSET)

        graph_index_stats = cls(
            error=error,
            total_edges=total_edges,
            edge_types=edge_types,
            rebuilding=rebuilding,
            backfill_progress=backfill_progress,
            backfill_items_processed=backfill_items_processed,
        )

        graph_index_stats.additional_properties = d
        return graph_index_stats

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
