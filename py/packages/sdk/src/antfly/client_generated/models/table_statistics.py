from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.table_statistics_field_stats import TableStatisticsFieldStats


T = TypeVar("T", bound="TableStatistics")


@_attrs_define
class TableStatistics:
    """Statistics about a table used for query planning.

    Attributes:
        row_count (int | Unset): Approximate number of rows in the table.
        size_bytes (int | Unset): Approximate size of the table in bytes.
        field_stats (TableStatisticsFieldStats | Unset): Per-field statistics for query optimization.
        last_updated (datetime.datetime | Unset): When these statistics were last computed.
    """

    row_count: int | Unset = UNSET
    size_bytes: int | Unset = UNSET
    field_stats: TableStatisticsFieldStats | Unset = UNSET
    last_updated: datetime.datetime | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        row_count = self.row_count

        size_bytes = self.size_bytes

        field_stats: dict[str, Any] | Unset = UNSET
        if not isinstance(self.field_stats, Unset):
            field_stats = self.field_stats.to_dict()

        last_updated: str | Unset = UNSET
        if not isinstance(self.last_updated, Unset):
            last_updated = self.last_updated.isoformat()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if row_count is not UNSET:
            field_dict["row_count"] = row_count
        if size_bytes is not UNSET:
            field_dict["size_bytes"] = size_bytes
        if field_stats is not UNSET:
            field_dict["field_stats"] = field_stats
        if last_updated is not UNSET:
            field_dict["last_updated"] = last_updated

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.table_statistics_field_stats import TableStatisticsFieldStats

        d = dict(src_dict)
        row_count = d.pop("row_count", UNSET)

        size_bytes = d.pop("size_bytes", UNSET)

        _field_stats = d.pop("field_stats", UNSET)
        field_stats: TableStatisticsFieldStats | Unset
        if isinstance(_field_stats, Unset):
            field_stats = UNSET
        else:
            field_stats = TableStatisticsFieldStats.from_dict(_field_stats)

        _last_updated = d.pop("last_updated", UNSET)
        last_updated: datetime.datetime | Unset
        if isinstance(_last_updated, Unset):
            last_updated = UNSET
        else:
            last_updated = isoparse(_last_updated)

        table_statistics = cls(
            row_count=row_count,
            size_bytes=size_bytes,
            field_stats=field_stats,
            last_updated=last_updated,
        )

        table_statistics.additional_properties = d
        return table_statistics

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
