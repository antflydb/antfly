from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.full_text_index_stats_index_type import FullTextIndexStatsIndexType
from ..types import UNSET, Unset

T = TypeVar("T", bound="FullTextIndexStats")


@_attrs_define
class FullTextIndexStats:
    """
    Attributes:
        index_type (FullTextIndexStatsIndexType): Discriminator for the index stats variant.
        error (str | Unset): Error message if stats could not be retrieved
        total_indexed (int | Unset): Number of documents in the index
        disk_usage (int | Unset): Size of the index in bytes
        rebuilding (bool | Unset): Whether the index is currently rebuilding
        backfill_progress (float | Unset): Progress of ongoing rebuild as fraction [0.0, 1.0]
        backfill_items_processed (int | Unset): Number of documents indexed during current rebuild
    """

    index_type: FullTextIndexStatsIndexType
    error: str | Unset = UNSET
    total_indexed: int | Unset = UNSET
    disk_usage: int | Unset = UNSET
    rebuilding: bool | Unset = UNSET
    backfill_progress: float | Unset = UNSET
    backfill_items_processed: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        error = self.error

        total_indexed = self.total_indexed

        disk_usage = self.disk_usage

        rebuilding = self.rebuilding

        backfill_progress = self.backfill_progress

        backfill_items_processed = self.backfill_items_processed

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_type": index_type,
            }
        )
        if error is not UNSET:
            field_dict["error"] = error
        if total_indexed is not UNSET:
            field_dict["total_indexed"] = total_indexed
        if disk_usage is not UNSET:
            field_dict["disk_usage"] = disk_usage
        if rebuilding is not UNSET:
            field_dict["rebuilding"] = rebuilding
        if backfill_progress is not UNSET:
            field_dict["backfill_progress"] = backfill_progress
        if backfill_items_processed is not UNSET:
            field_dict["backfill_items_processed"] = backfill_items_processed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index_type = FullTextIndexStatsIndexType(d.pop("index_type"))

        error = d.pop("error", UNSET)

        total_indexed = d.pop("total_indexed", UNSET)

        disk_usage = d.pop("disk_usage", UNSET)

        rebuilding = d.pop("rebuilding", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_items_processed = d.pop("backfill_items_processed", UNSET)

        full_text_index_stats = cls(
            index_type=index_type,
            error=error,
            total_indexed=total_indexed,
            disk_usage=disk_usage,
            rebuilding=rebuilding,
            backfill_progress=backfill_progress,
            backfill_items_processed=backfill_items_processed,
        )

        full_text_index_stats.additional_properties = d
        return full_text_index_stats

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
