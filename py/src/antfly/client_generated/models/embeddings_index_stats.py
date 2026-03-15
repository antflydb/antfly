from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="EmbeddingsIndexStats")


@_attrs_define
class EmbeddingsIndexStats:
    """Statistics for an embeddings index (dense or sparse)

    Attributes:
        error (Union[Unset, str]): Error message if stats could not be retrieved
        total_indexed (Union[Unset, int]): Number of vectors/documents in the index
        disk_usage (Union[Unset, int]): Size of the index in bytes
        total_nodes (Union[Unset, int]): Total number of nodes in the index (dense only)
        total_terms (Union[Unset, int]): Number of unique terms in the inverted index (sparse only)
        rebuilding (Union[Unset, bool]): Whether the index enricher is currently backfilling
        wal_backlog (Union[Unset, int]): Number of documents pending enrichment in the WAL
        backfill_progress (Union[Unset, float]): Backfill progress as a ratio from 0.0 to 1.0
        backfill_items_processed (Union[Unset, int]): Total items processed during backfill
    """

    error: Union[Unset, str] = UNSET
    total_indexed: Union[Unset, int] = UNSET
    disk_usage: Union[Unset, int] = UNSET
    total_nodes: Union[Unset, int] = UNSET
    total_terms: Union[Unset, int] = UNSET
    rebuilding: Union[Unset, bool] = UNSET
    wal_backlog: Union[Unset, int] = UNSET
    backfill_progress: Union[Unset, float] = UNSET
    backfill_items_processed: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
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
        field_dict.update({})
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
