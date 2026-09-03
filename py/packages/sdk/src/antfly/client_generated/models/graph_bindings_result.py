from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_bindings_result_kind import GraphBindingsResultKind

if TYPE_CHECKING:
    from ..models.graph_result_row import GraphResultRow
    from ..models.graph_result_stats import GraphResultStats


T = TypeVar("T", bound="GraphBindingsResult")


@_attrs_define
class GraphBindingsResult:
    """A deterministic bounded prefix of projected bindings from a canonical graph MATCH query. Inspect stats.truncated to
    determine whether enumeration was exhaustive.

        Attributes:
            kind (GraphBindingsResultKind): Stable discriminator for the graph result shape.
            rows (list[GraphResultRow]):
            stats (GraphResultStats): Completion statistics for a bounded graph result.
    """

    kind: GraphBindingsResultKind
    rows: list[GraphResultRow]
    stats: GraphResultStats

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        rows = []
        for rows_item_data in self.rows:
            rows_item = rows_item_data.to_dict()
            rows.append(rows_item)

        stats = self.stats.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "rows": rows,
                "stats": stats,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_result_row import GraphResultRow
        from ..models.graph_result_stats import GraphResultStats

        d = dict(src_dict)
        kind = GraphBindingsResultKind(d.pop("kind"))

        rows = []
        _rows = d.pop("rows")
        for rows_item_data in _rows:
            rows_item = GraphResultRow.from_dict(rows_item_data)

            rows.append(rows_item)

        stats = GraphResultStats.from_dict(d.pop("stats"))

        graph_bindings_result = cls(
            kind=kind,
            rows=rows,
            stats=stats,
        )

        return graph_bindings_result
