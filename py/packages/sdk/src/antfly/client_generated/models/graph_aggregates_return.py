from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_aggregates_return_aggregates import GraphAggregatesReturnAggregates


T = TypeVar("T", bound="GraphAggregatesReturn")


@_attrs_define
class GraphAggregatesReturn:
    """
    Attributes:
        aggregates (GraphAggregatesReturnAggregates):
    """

    aggregates: GraphAggregatesReturnAggregates

    def to_dict(self) -> dict[str, Any]:
        aggregates = self.aggregates.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "aggregates": aggregates,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_aggregates_return_aggregates import GraphAggregatesReturnAggregates

        d = dict(src_dict)
        aggregates = GraphAggregatesReturnAggregates.from_dict(d.pop("aggregates"))

        graph_aggregates_return = cls(
            aggregates=aggregates,
        )

        return graph_aggregates_return
