from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="RelationalIndexRepairReport")


@_attrs_define
class RelationalIndexRepairReport:
    """Counters from relational column-backed index repair. These counters describe disposable derived-index artifacts
    rebuilt from authoritative packed rows.

        Attributes:
            scanned_rows (int | Unset):
            indexed_rows (int | Unset):
            deleted_orphan_entries (int | Unset):
            written_entries (int | Unset):
    """

    scanned_rows: int | Unset = UNSET
    indexed_rows: int | Unset = UNSET
    deleted_orphan_entries: int | Unset = UNSET
    written_entries: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        scanned_rows = self.scanned_rows

        indexed_rows = self.indexed_rows

        deleted_orphan_entries = self.deleted_orphan_entries

        written_entries = self.written_entries

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if scanned_rows is not UNSET:
            field_dict["scanned_rows"] = scanned_rows
        if indexed_rows is not UNSET:
            field_dict["indexed_rows"] = indexed_rows
        if deleted_orphan_entries is not UNSET:
            field_dict["deleted_orphan_entries"] = deleted_orphan_entries
        if written_entries is not UNSET:
            field_dict["written_entries"] = written_entries

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        scanned_rows = d.pop("scanned_rows", UNSET)

        indexed_rows = d.pop("indexed_rows", UNSET)

        deleted_orphan_entries = d.pop("deleted_orphan_entries", UNSET)

        written_entries = d.pop("written_entries", UNSET)

        relational_index_repair_report = cls(
            scanned_rows=scanned_rows,
            indexed_rows=indexed_rows,
            deleted_orphan_entries=deleted_orphan_entries,
            written_entries=written_entries,
        )

        relational_index_repair_report.additional_properties = d
        return relational_index_repair_report

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
