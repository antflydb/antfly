from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.enrichment_context_graph_direction import EnrichmentContextGraphDirection
from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentContextGraph")


@_attrs_define
class EnrichmentContextGraph:
    """Bounded breadth-first traversal over the edges of a graph index. By default only explicitly written edges are
    traversed; `include_generated` also follows field-derived and enrichment-generated edges, whose changes invalidate
    dependents when their edge artifacts are written or removed. Generated edges must not depend on the consuming
    enrichment. Members render with depth, edge type, and direction.

        Attributes:
            graph_index (str):
            table (str | Unset):
            start_from (str | Unset): JSON pointer to the start node key; omitted means the dependent row itself.
            edge_types (list[str] | Unset):
            direction (EnrichmentContextGraphDirection | Unset):  Default: EnrichmentContextGraphDirection.OUT.
            depth (int | Unset):  Default: 1.
            max_fanout (int | Unset): Maximum edges per visited node; exceeding it fails explicitly. Default: 100.
            include_generated (bool | Unset): Also traverse field-derived and enrichment-generated edges. Default: False.
    """

    graph_index: str
    table: str | Unset = UNSET
    start_from: str | Unset = UNSET
    edge_types: list[str] | Unset = UNSET
    direction: EnrichmentContextGraphDirection | Unset = EnrichmentContextGraphDirection.OUT
    depth: int | Unset = 1
    max_fanout: int | Unset = 100
    include_generated: bool | Unset = False

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

        include_generated = self.include_generated

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
        if include_generated is not UNSET:
            field_dict["include_generated"] = include_generated

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

        include_generated = d.pop("include_generated", UNSET)

        enrichment_context_graph = cls(
            graph_index=graph_index,
            table=table,
            start_from=start_from,
            edge_types=edge_types,
            direction=direction,
            depth=depth,
            max_fanout=max_fanout,
            include_generated=include_generated,
        )

        return enrichment_context_graph
