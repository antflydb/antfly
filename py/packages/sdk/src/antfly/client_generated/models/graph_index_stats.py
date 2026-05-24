from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_index_stats_index_type import GraphIndexStatsIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_index_stats_algebraic_graph import GraphIndexStatsAlgebraicGraph
    from ..models.graph_index_stats_edge_types import GraphIndexStatsEdgeTypes


T = TypeVar("T", bound="GraphIndexStats")


@_attrs_define
class GraphIndexStats:
    """Statistics for graph index

    Attributes:
        index_type (GraphIndexStatsIndexType): Discriminator for the index stats variant.
        error (str | Unset): Error message if stats could not be retrieved
        total_edges (int | Unset): Total number of edges in the graph
        edge_types (GraphIndexStatsEdgeTypes | Unset): Count of edges per edge type
        rebuilding (bool | Unset): Whether the index is currently rebuilding
        backfill_progress (float | Unset): Rebuild progress as a ratio from 0.0 to 1.0
        backfill_items_processed (int | Unset): Number of edges indexed during current rebuild
        algebraic_graph (GraphIndexStatsAlgebraicGraph | Unset): Algebraic graph execution health for bounded semiring
            traversal.
    """

    index_type: GraphIndexStatsIndexType
    error: str | Unset = UNSET
    total_edges: int | Unset = UNSET
    edge_types: GraphIndexStatsEdgeTypes | Unset = UNSET
    rebuilding: bool | Unset = UNSET
    backfill_progress: float | Unset = UNSET
    backfill_items_processed: int | Unset = UNSET
    algebraic_graph: GraphIndexStatsAlgebraicGraph | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        error = self.error

        total_edges = self.total_edges

        edge_types: dict[str, Any] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types.to_dict()

        rebuilding = self.rebuilding

        backfill_progress = self.backfill_progress

        backfill_items_processed = self.backfill_items_processed

        algebraic_graph: dict[str, Any] | Unset = UNSET
        if not isinstance(self.algebraic_graph, Unset):
            algebraic_graph = self.algebraic_graph.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_type": index_type,
            }
        )
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
        if algebraic_graph is not UNSET:
            field_dict["algebraic_graph"] = algebraic_graph

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_index_stats_algebraic_graph import GraphIndexStatsAlgebraicGraph
        from ..models.graph_index_stats_edge_types import GraphIndexStatsEdgeTypes

        d = dict(src_dict)
        index_type = GraphIndexStatsIndexType(d.pop("index_type"))

        error = d.pop("error", UNSET)

        total_edges = d.pop("total_edges", UNSET)

        _edge_types = d.pop("edge_types", UNSET)
        edge_types: GraphIndexStatsEdgeTypes | Unset
        if isinstance(_edge_types, Unset):
            edge_types = UNSET
        else:
            edge_types = GraphIndexStatsEdgeTypes.from_dict(_edge_types)

        rebuilding = d.pop("rebuilding", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_items_processed = d.pop("backfill_items_processed", UNSET)

        _algebraic_graph = d.pop("algebraic_graph", UNSET)
        algebraic_graph: GraphIndexStatsAlgebraicGraph | Unset
        if isinstance(_algebraic_graph, Unset):
            algebraic_graph = UNSET
        else:
            algebraic_graph = GraphIndexStatsAlgebraicGraph.from_dict(_algebraic_graph)

        graph_index_stats = cls(
            index_type=index_type,
            error=error,
            total_edges=total_edges,
            edge_types=edge_types,
            rebuilding=rebuilding,
            backfill_progress=backfill_progress,
            backfill_items_processed=backfill_items_processed,
            algebraic_graph=algebraic_graph,
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
