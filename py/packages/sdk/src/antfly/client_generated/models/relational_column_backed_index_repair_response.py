from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.relational_column_backed_index_repair_range_result import RelationalColumnBackedIndexRepairRangeResult
    from ..models.relational_column_backed_index_repair_report import RelationalColumnBackedIndexRepairReport


T = TypeVar("T", bound="RelationalColumnBackedIndexRepairResponse")


@_attrs_define
class RelationalColumnBackedIndexRepairResponse:
    """
    Attributes:
        complete (bool):
        ranges_scanned (int):
        ranges_repaired (int):
        ranges_missing (int):
        report (RelationalColumnBackedIndexRepairReport):
        groups (list[RelationalColumnBackedIndexRepairRangeResult]):
    """

    complete: bool
    ranges_scanned: int
    ranges_repaired: int
    ranges_missing: int
    report: RelationalColumnBackedIndexRepairReport
    groups: list[RelationalColumnBackedIndexRepairRangeResult]

    def to_dict(self) -> dict[str, Any]:
        complete = self.complete

        ranges_scanned = self.ranges_scanned

        ranges_repaired = self.ranges_repaired

        ranges_missing = self.ranges_missing

        report = self.report.to_dict()

        groups = []
        for groups_item_data in self.groups:
            groups_item = groups_item_data.to_dict()
            groups.append(groups_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "complete": complete,
                "ranges_scanned": ranges_scanned,
                "ranges_repaired": ranges_repaired,
                "ranges_missing": ranges_missing,
                "report": report,
                "groups": groups,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_column_backed_index_repair_range_result import (
            RelationalColumnBackedIndexRepairRangeResult,
        )
        from ..models.relational_column_backed_index_repair_report import RelationalColumnBackedIndexRepairReport

        d = dict(src_dict)
        complete = d.pop("complete")

        ranges_scanned = d.pop("ranges_scanned")

        ranges_repaired = d.pop("ranges_repaired")

        ranges_missing = d.pop("ranges_missing")

        report = RelationalColumnBackedIndexRepairReport.from_dict(d.pop("report"))

        groups = []
        _groups = d.pop("groups")
        for groups_item_data in _groups:
            groups_item = RelationalColumnBackedIndexRepairRangeResult.from_dict(groups_item_data)

            groups.append(groups_item)

        relational_column_backed_index_repair_response = cls(
            complete=complete,
            ranges_scanned=ranges_scanned,
            ranges_repaired=ranges_repaired,
            ranges_missing=ranges_missing,
            report=report,
            groups=groups,
        )

        return relational_column_backed_index_repair_response
