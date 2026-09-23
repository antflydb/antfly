from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.enrichment_neighbor_context_config_direction import EnrichmentNeighborContextConfigDirection
from ..types import UNSET, Unset

T = TypeVar("T", bound="EnrichmentNeighborContextConfig")


@_attrs_define
class EnrichmentNeighborContextConfig:
    """Bounded sample of the document's same-shard graph neighbors appended to an asset producer's rendered input as a
    compact JSON block ({"neighbors":[{"edge_type":...,"direction":...,"target":...,"weight":...}]}), ordered by edge
    type then target key. A conceptualizer enrichment on an entities table can thereby ground its abstractions in
    adjacent facts ("started_by -> John Andrew Rice"). The sampled block participates in the producer's skip state, so a
    changed adjacency re-runs the producer.

        Attributes:
            graph_index (str): Name of a graph index on the same table whose local state is sampled. Validated at admission;
                cross-shard neighbors are not sampled.
            edge_types (list[str] | Unset): Edge types to sample. Empty admits every edge type.
            direction (EnrichmentNeighborContextConfigDirection | Unset): Adjacency orientation to sample relative to the
                document. Default: EnrichmentNeighborContextConfigDirection.BOTH.
            limit (int | Unset): Maximum neighbors rendered into the producer input, applied after deterministic ordering.
                Default: 8.
    """

    graph_index: str
    edge_types: list[str] | Unset = UNSET
    direction: EnrichmentNeighborContextConfigDirection | Unset = EnrichmentNeighborContextConfigDirection.BOTH
    limit: int | Unset = 8

    def to_dict(self) -> dict[str, Any]:
        graph_index = self.graph_index

        edge_types: list[str] | Unset = UNSET
        if not isinstance(self.edge_types, Unset):
            edge_types = self.edge_types

        direction: str | Unset = UNSET
        if not isinstance(self.direction, Unset):
            direction = self.direction.value

        limit = self.limit

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "graph_index": graph_index,
            }
        )
        if edge_types is not UNSET:
            field_dict["edge_types"] = edge_types
        if direction is not UNSET:
            field_dict["direction"] = direction
        if limit is not UNSET:
            field_dict["limit"] = limit

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        graph_index = d.pop("graph_index")

        edge_types = cast(list[str], d.pop("edge_types", UNSET))

        _direction = d.pop("direction", UNSET)
        direction: EnrichmentNeighborContextConfigDirection | Unset
        if isinstance(_direction, Unset):
            direction = UNSET
        else:
            direction = EnrichmentNeighborContextConfigDirection(_direction)

        limit = d.pop("limit", UNSET)

        enrichment_neighbor_context_config = cls(
            graph_index=graph_index,
            edge_types=edge_types,
            direction=direction,
            limit=limit,
        )

        return enrichment_neighbor_context_config
