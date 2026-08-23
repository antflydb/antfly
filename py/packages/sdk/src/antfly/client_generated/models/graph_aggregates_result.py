from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_aggregates_result_aggregates import GraphAggregatesResultAggregates
    from ..models.graph_query_stats import GraphQueryStats


T = TypeVar("T", bound="GraphAggregatesResult")


@_attrs_define
class GraphAggregatesResult:
    """Complete exact aggregates from a canonical graph MATCH query.

    Attributes:
        aggregates (GraphAggregatesResultAggregates):
        stats (GraphQueryStats):
        took (int): Query execution time.
    """

    aggregates: GraphAggregatesResultAggregates
    stats: GraphQueryStats
    took: int

    def to_dict(self) -> dict[str, Any]:
        aggregates = self.aggregates.to_dict()

        stats = self.stats.to_dict()

        took = self.took

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "aggregates": aggregates,
                "stats": stats,
                "took": took,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_aggregates_result_aggregates import GraphAggregatesResultAggregates
        from ..models.graph_query_stats import GraphQueryStats

        d = dict(src_dict)
        aggregates = GraphAggregatesResultAggregates.from_dict(d.pop("aggregates"))

        stats = GraphQueryStats.from_dict(d.pop("stats"))

        took = d.pop("took")

        graph_aggregates_result = cls(
            aggregates=aggregates,
            stats=stats,
            took=took,
        )

        return graph_aggregates_result
