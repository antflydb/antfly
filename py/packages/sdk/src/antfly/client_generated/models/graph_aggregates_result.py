from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_aggregates_result_kind import GraphAggregatesResultKind

if TYPE_CHECKING:
    from ..models.graph_aggregates_result_aggregates import GraphAggregatesResultAggregates
    from ..models.graph_exact_result_stats import GraphExactResultStats


T = TypeVar("T", bound="GraphAggregatesResult")


@_attrs_define
class GraphAggregatesResult:
    """Complete exact aggregates from a canonical graph MATCH query.

    Attributes:
        kind (GraphAggregatesResultKind): Stable discriminator for the graph result shape.
        aggregates (GraphAggregatesResultAggregates): Keys are the GraphIdentifiers selected by the corresponding
            aggregate return projection.
        stats (GraphExactResultStats): Completion statistics for a graph result that is exact or fails without producing
            a result.
    """

    kind: GraphAggregatesResultKind
    aggregates: GraphAggregatesResultAggregates
    stats: GraphExactResultStats

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        aggregates = self.aggregates.to_dict()

        stats = self.stats.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "aggregates": aggregates,
                "stats": stats,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_aggregates_result_aggregates import GraphAggregatesResultAggregates
        from ..models.graph_exact_result_stats import GraphExactResultStats

        d = dict(src_dict)
        kind = GraphAggregatesResultKind(d.pop("kind"))

        aggregates = GraphAggregatesResultAggregates.from_dict(d.pop("aggregates"))

        stats = GraphExactResultStats.from_dict(d.pop("stats"))

        graph_aggregates_result = cls(
            kind=kind,
            aggregates=aggregates,
            stats=stats,
        )

        return graph_aggregates_result
