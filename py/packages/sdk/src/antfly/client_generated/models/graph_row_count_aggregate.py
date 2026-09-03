from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_row_count_target import GraphRowCountTarget

T = TypeVar("T", bound="GraphRowCountAggregate")


@_attrs_define
class GraphRowCountAggregate:
    """
    Attributes:
        count (GraphRowCountTarget): Count every complete graph binding, including a binding retained by an unmatched
            optional group through null extension.
    """

    count: GraphRowCountTarget

    def to_dict(self) -> dict[str, Any]:
        count = self.count.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "count": count,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        count = GraphRowCountTarget(d.pop("count"))

        graph_row_count_aggregate = cls(
            count=count,
        )

        return graph_row_count_aggregate
