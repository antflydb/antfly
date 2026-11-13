from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.enrichment_context_graph_direction import EnrichmentContextGraphDirection
from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentContextGraph")


@_attrs_define
class EnrichmentContextGraph:
    """Bounded breadth-first traversal over explicitly written edges of a graph index. Field- and artifact-derived
    (generated) edges are never traversed by live context. Members render with depth, edge type, and direction.

        Attributes:
            graph_index (str):
            table (str | Unset):
            start_from (str | Unset): JSON pointer to the start node key; omitted means the dependent row itself.
            edge_types (list[str] | Unset):
            direction (EnrichmentContextGraphDirection | Unset):  Default: EnrichmentContextGraphDirection.OUT.
            depth (int | Unset):  Default: 1.
            max_fanout (int | Unset): Maximum edges per visited node; exceeding it fails explicitly. Default: 100.
    """

    graph_index: str
    table: str | Unset = UNSET
    start_from: str | Unset = UNSET
    edge_types: list[str] | Unset = UNSET
    direction: EnrichmentContextGraphDirection | Unset = EnrichmentContextGraphDirection.OUT
    depth: int | Unset = 1
    max_fanout: int | Unset = 100

    def to_dict(self) -> dict[str, Any]:
        graph_index = self.graph_index

        table = self.table

        start_from = self.start_from

        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        depth = self.depth

        max_fanout = self.max_fanout

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "graph_index": graph_index,
            }
        )
        if table is not UNSET:
            field_dict["table"] = table
        if start_from is not UNSET:
            field_dict["start_from"] = start_from
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if direction is not UNSET:
            field_dict["direction"] = direction
        if depth is not UNSET:
            field_dict["depth"] = depth
        if max_fanout is not UNSET:
            field_dict["max_fanout"] = max_fanout

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        graph_index = d.pop("graph_index")

        table = d.pop("table", UNSET)

        start_from = d.pop("start_from", UNSET)

        edge_types = cast(list[str], d.pop("edge_types", UNSET))

        _direction = d.pop("direction", UNSET)
        direction: EnrichmentContextGraphDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EnrichmentContextGraphDirection(_direction)

        depth = d.pop("depth", UNSET)

        max_fanout = d.pop("max_fanout", UNSET)

        enrichment_context_graph = cls(
            graph_index=graph_index,
            table=table,
            start_from=start_from,
            edge_types=edge_types,
            direction=direction,
            depth=depth,
            max_fanout=max_fanout,
        )

        return enrichment_context_graph
