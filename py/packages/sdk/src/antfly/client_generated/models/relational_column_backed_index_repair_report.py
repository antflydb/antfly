from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

T = TypeVar("T", bound="RelationalColumnBackedIndexRepairReport")


@_attrs_define
class RelationalColumnBackedIndexRepairReport:
    """
    Attributes:
        scanned_rows (int):
        indexed_rows (int):
        deleted_orphan_entries (int):
        written_entries (int):
    """

    scanned_rows: int
    indexed_rows: int
    deleted_orphan_entries: int
    written_entries: int

    def to_dict(self) -> dict[str, Any]:
        scanned_rows = self.scanned_rows

        indexed_rows = self.indexed_rows

        deleted_orphan_entries = self.deleted_orphan_entries

        written_entries = self.written_entries

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "scanned_rows": scanned_rows,
                "indexed_rows": indexed_rows,
                "deleted_orphan_entries": deleted_orphan_entries,
                "written_entries": written_entries,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        scanned_rows = d.pop("scanned_rows")

        indexed_rows = d.pop("indexed_rows")

        deleted_orphan_entries = d.pop("deleted_orphan_entries")

        written_entries = d.pop("written_entries")

        relational_column_backed_index_repair_report = cls(
            scanned_rows=scanned_rows,
            indexed_rows=indexed_rows,
            deleted_orphan_entries=deleted_orphan_entries,
            written_entries=written_entries,
        )

        return relational_column_backed_index_repair_report
