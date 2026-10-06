from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_bindings_result_kind import GraphBindingsResultKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_bindings_result_computed_item import GraphBindingsResultComputedItem
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
            computed (list[GraphBindingsResultComputedItem] | Unset): Evaluated values parallel to returned rows, when
                decision evaluation was requested.
    """

    kind: GraphBindingsResultKind
    rows: list[GraphResultRow]
    stats: GraphResultStats
    computed: list[GraphBindingsResultComputedItem] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        kind = self.kind.value

        rows = []
        for rows_item_data in self.rows:
            rows_item = rows_item_data.to_dict()
            rows.append(rows_item)

        stats = self.stats.to_dict()

        computed: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.computed, Unset):
            computed = []
            for computed_item_data in self.computed:
                computed_item = computed_item_data.to_dict()
                computed.append(computed_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "kind": kind,
                "rows": rows,
                "stats": stats,
            }
        )
        if computed is not UNSET:
            field_dict["computed"] = computed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_bindings_result_computed_item import GraphBindingsResultComputedItem
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

        _computed = d.pop("computed", UNSET)
        computed: list[GraphBindingsResultComputedItem] | Unset = UNSET
        if _computed is not UNSET:
            computed = []
            for computed_item_data in _computed:
                computed_item = GraphBindingsResultComputedItem.from_dict(computed_item_data)

                computed.append(computed_item)

        graph_bindings_result = cls(
            kind=kind,
            rows=rows,
            stats=stats,
            computed=computed,
        )

        return graph_bindings_result
