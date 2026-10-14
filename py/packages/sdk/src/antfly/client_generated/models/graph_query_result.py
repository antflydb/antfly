from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_query_result_wire_version import GraphQueryResultWireVersion
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_query_result_plan_item import GraphQueryResultPlanItem
    from ..models.graph_query_stats import GraphQueryStats
    from ..models.graph_wire_value import GraphWireValue
    from ..models.graph_write_receipt import GraphWriteReceipt


T = TypeVar("T", bound="GraphQueryResult")


@_attrs_define
class GraphQueryResult:
    """
    Attributes:
        wire_version (GraphQueryResultWireVersion):
        columns (list[str]):
        rows (list[list[GraphWireValue]]):
        plan (list[GraphQueryResultPlanItem]):
        stats (GraphQueryStats):
        graph_incarnation (str):
        graph_revision (str):
        complete (bool):
        commit (GraphWriteReceipt | Unset):
        read_only (bool | Unset): Whether the compiled statement has no mutation clauses; EXPLAIN also reports the
            underlying statement capability.
    """

    wire_version: GraphQueryResultWireVersion
    columns: list[str]
    rows: list[list[GraphWireValue]]
    plan: list[GraphQueryResultPlanItem]
    stats: GraphQueryStats
    graph_incarnation: str
    graph_revision: str
    complete: bool
    commit: GraphWriteReceipt | Unset = UNSET
    read_only: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        wire_version = self.wire_version.value

        columns = self.columns

        rows = []
        for rows_item_data in self.rows:
            rows_item = []
            for rows_item_item_data in rows_item_data:
                rows_item_item = rows_item_item_data.to_dict()
                rows_item.append(rows_item_item)

            rows.append(rows_item)

        plan = []
        for plan_item_data in self.plan:
            plan_item = plan_item_data.to_dict()
            plan.append(plan_item)

        stats = self.stats.to_dict()

        graph_incarnation = self.graph_incarnation

        graph_revision = self.graph_revision

        complete = self.complete

        commit: dict[str, Any] | Unset = UNSET
        if not isinstance(self.commit, Unset):
            commit = self.commit.to_dict()

        read_only = self.read_only

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "wire_version": wire_version,
                "columns": columns,
                "rows": rows,
                "plan": plan,
                "stats": stats,
                "graph_incarnation": graph_incarnation,
                "graph_revision": graph_revision,
                "complete": complete,
            }
        )
        if commit is not UNSET:
            field_dict["commit"] = commit
        if read_only is not UNSET:
            field_dict["read_only"] = read_only

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_query_result_plan_item import GraphQueryResultPlanItem
        from ..models.graph_query_stats import GraphQueryStats
        from ..models.graph_wire_value import GraphWireValue
        from ..models.graph_write_receipt import GraphWriteReceipt

        d = dict(src_dict)
        wire_version = GraphQueryResultWireVersion(d.pop("wire_version"))

        columns = cast(list[str], d.pop("columns"))

        rows = []
        _rows = d.pop("rows")
        for rows_item_data in _rows:
            rows_item = []
            _rows_item = rows_item_data
            for rows_item_item_data in _rows_item:
                rows_item_item = GraphWireValue.from_dict(rows_item_item_data)

                rows_item.append(rows_item_item)

            rows.append(rows_item)

        plan = []
        _plan = d.pop("plan")
        for plan_item_data in _plan:
            plan_item = GraphQueryResultPlanItem.from_dict(plan_item_data)

            plan.append(plan_item)

        stats = GraphQueryStats.from_dict(d.pop("stats"))

        graph_incarnation = d.pop("graph_incarnation")

        graph_revision = d.pop("graph_revision")

        complete = d.pop("complete")

        _commit = d.pop("commit", UNSET)
        commit: GraphWriteReceipt | Unset
        if isinstance(_commit, Unset):
            commit = UNSET
        else:
            commit = GraphWriteReceipt.from_dict(_commit)

        read_only = d.pop("read_only", UNSET)

        graph_query_result = cls(
            wire_version=wire_version,
            columns=columns,
            rows=rows,
            plan=plan,
            stats=stats,
            graph_incarnation=graph_incarnation,
            graph_revision=graph_revision,
            complete=complete,
            commit=commit,
            read_only=read_only,
        )

        graph_query_result.additional_properties = d
        return graph_query_result

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
