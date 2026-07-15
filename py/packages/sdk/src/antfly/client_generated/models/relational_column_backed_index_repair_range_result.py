from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.relational_column_backed_index_repair_report import RelationalColumnBackedIndexRepairReport


T = TypeVar("T", bound="RelationalColumnBackedIndexRepairRangeResult")


@_attrs_define
class RelationalColumnBackedIndexRepairRangeResult:
    """
    Attributes:
        group_id (int):
        table_id (int):
        range_id (int):
        lower_doc_key (str):
        upper_doc_key (str):
        repaired (bool):
        report (RelationalColumnBackedIndexRepairReport):
    """

    group_id: int
    table_id: int
    range_id: int
    lower_doc_key: str
    upper_doc_key: str
    repaired: bool
    report: RelationalColumnBackedIndexRepairReport

    def to_dict(self) -> dict[str, Any]:
        group_id = self.group_id

        table_id = self.table_id

        range_id = self.range_id

        lower_doc_key = self.lower_doc_key

        upper_doc_key = self.upper_doc_key

        repaired = self.repaired

        report = self.report.to_dict()

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "group_id": group_id,
                "table_id": table_id,
                "range_id": range_id,
                "lower_doc_key": lower_doc_key,
                "upper_doc_key": upper_doc_key,
                "repaired": repaired,
                "report": report,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_column_backed_index_repair_report import RelationalColumnBackedIndexRepairReport

        d = dict(src_dict)
        group_id = d.pop("group_id")

        table_id = d.pop("table_id")

        range_id = d.pop("range_id")

        lower_doc_key = d.pop("lower_doc_key")

        upper_doc_key = d.pop("upper_doc_key")

        repaired = d.pop("repaired")

        report = RelationalColumnBackedIndexRepairReport.from_dict(d.pop("report"))

        relational_column_backed_index_repair_range_result = cls(
            group_id=group_id,
            table_id=table_id,
            range_id=range_id,
            lower_doc_key=lower_doc_key,
            upper_doc_key=upper_doc_key,
            repaired=repaired,
            report=report,
        )

        return relational_column_backed_index_repair_range_result
