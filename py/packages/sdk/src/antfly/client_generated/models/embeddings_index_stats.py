from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.embeddings_index_stats_index_type import EmbeddingsIndexStatsIndexType
from ..types import UNSET, Unset

T = TypeVar("T", bound="EmbeddingsIndexStats")


@_attrs_define
class EmbeddingsIndexStats:
    """Statistics for an embeddings index (dense or sparse)

    Attributes:
        index_type (EmbeddingsIndexStatsIndexType): Discriminator for the index stats variant.
        error (str | Unset): Error message if stats could not be retrieved
        total_indexed (int | Unset): Number of vectors/documents in the index
        disk_usage (int | Unset): Size of the index in bytes
        total_nodes (int | Unset): Total number of nodes in the index (dense only)
        total_terms (int | Unset): Number of unique terms in the inverted index (sparse only)
        rebuilding (bool | Unset): Whether the index enricher is currently backfilling
        wal_backlog (int | Unset): Number of documents pending enrichment in the WAL
        backfill_progress (float | Unset): Backfill progress as a ratio from 0.0 to 1.0
        backfill_items_processed (int | Unset): Total items processed during backfill
    """

    index_type: EmbeddingsIndexStatsIndexType
    error: str | Unset = UNSET
    total_indexed: int | Unset = UNSET
    disk_usage: int | Unset = UNSET
    total_nodes: int | Unset = UNSET
    total_terms: int | Unset = UNSET
    rebuilding: bool | Unset = UNSET
    wal_backlog: int | Unset = UNSET
    backfill_progress: float | Unset = UNSET
    backfill_items_processed: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        error = self.error

        total_indexed = self.total_indexed

        disk_usage = self.disk_usage

        total_nodes = self.total_nodes

        total_terms = self.total_terms

        rebuilding = self.rebuilding

        wal_backlog = self.wal_backlog

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
        if total_nodes is not UNSET:
            field_dict["total_nodes"] = total_nodes
        if total_terms is not UNSET:
            field_dict["total_terms"] = total_terms
        if rebuilding is not UNSET:
            field_dict["rebuilding"] = rebuilding
        if wal_backlog is not UNSET:
            field_dict["wal_backlog"] = wal_backlog
        if backfill_progress is not UNSET:
            field_dict["backfill_progress"] = backfill_progress
        if backfill_items_processed is not UNSET:
            field_dict["backfill_items_processed"] = backfill_items_processed

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index_type = EmbeddingsIndexStatsIndexType(d.pop("index_type"))

        error = d.pop("error", UNSET)

        total_indexed = d.pop("total_indexed", UNSET)

        disk_usage = d.pop("disk_usage", UNSET)

        total_nodes = d.pop("total_nodes", UNSET)

        total_terms = d.pop("total_terms", UNSET)

        rebuilding = d.pop("rebuilding", UNSET)

        wal_backlog = d.pop("wal_backlog", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_items_processed = d.pop("backfill_items_processed", UNSET)

        embeddings_index_stats = cls(
            index_type=index_type,
            error=error,
            total_indexed=total_indexed,
            disk_usage=disk_usage,
            total_nodes=total_nodes,
            total_terms=total_terms,
            rebuilding=rebuilding,
            wal_backlog=wal_backlog,
            backfill_progress=backfill_progress,
            backfill_items_processed=backfill_items_processed,
        )

        embeddings_index_stats.additional_properties = d
        return embeddings_index_stats

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
