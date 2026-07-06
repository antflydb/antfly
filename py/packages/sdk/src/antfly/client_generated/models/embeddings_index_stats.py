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
        projection_checkpoint_status (str | Unset): Durable projection checkpoint status: clean, rebuilding, degraded,
            or repair_required.
        projection_checkpoint_applied_sequence (int | Unset): Highest derived-log sequence covered by the durable
            projection checkpoint.
        projection_checkpoint_generation (int | Unset): Projection generation associated with the durable checkpoint.
        projection_checkpoint_config_hash (int | Unset): Projection configuration identity associated with the durable
            checkpoint.
        checkpoint_replay_tail_sequence_count (int | Unset): Number of derived-log sequences after the durable
            checkpoint that still need replay.
        repair_scan_issue_count (int | Unset): Repair issues found by explicit repair-scan accounting for this
            projection.
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
    projection_checkpoint_status: str | Unset = UNSET
    projection_checkpoint_applied_sequence: int | Unset = UNSET
    projection_checkpoint_generation: int | Unset = UNSET
    projection_checkpoint_config_hash: int | Unset = UNSET
    checkpoint_replay_tail_sequence_count: int | Unset = UNSET
    repair_scan_issue_count: int | Unset = UNSET
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

        projection_checkpoint_status = self.projection_checkpoint_status

        projection_checkpoint_applied_sequence = self.projection_checkpoint_applied_sequence

        projection_checkpoint_generation = self.projection_checkpoint_generation

        projection_checkpoint_config_hash = self.projection_checkpoint_config_hash

        checkpoint_replay_tail_sequence_count = self.checkpoint_replay_tail_sequence_count

        repair_scan_issue_count = self.repair_scan_issue_count

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
        if projection_checkpoint_status is not UNSET:
            field_dict["projection_checkpoint_status"] = projection_checkpoint_status
        if projection_checkpoint_applied_sequence is not UNSET:
            field_dict["projection_checkpoint_applied_sequence"] = projection_checkpoint_applied_sequence
        if projection_checkpoint_generation is not UNSET:
            field_dict["projection_checkpoint_generation"] = projection_checkpoint_generation
        if projection_checkpoint_config_hash is not UNSET:
            field_dict["projection_checkpoint_config_hash"] = projection_checkpoint_config_hash
        if checkpoint_replay_tail_sequence_count is not UNSET:
            field_dict["checkpoint_replay_tail_sequence_count"] = checkpoint_replay_tail_sequence_count
        if repair_scan_issue_count is not UNSET:
            field_dict["repair_scan_issue_count"] = repair_scan_issue_count

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

        projection_checkpoint_status = d.pop("projection_checkpoint_status", UNSET)

        projection_checkpoint_applied_sequence = d.pop("projection_checkpoint_applied_sequence", UNSET)

        projection_checkpoint_generation = d.pop("projection_checkpoint_generation", UNSET)

        projection_checkpoint_config_hash = d.pop("projection_checkpoint_config_hash", UNSET)

        checkpoint_replay_tail_sequence_count = d.pop("checkpoint_replay_tail_sequence_count", UNSET)

        repair_scan_issue_count = d.pop("repair_scan_issue_count", UNSET)

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
            projection_checkpoint_status=projection_checkpoint_status,
            projection_checkpoint_applied_sequence=projection_checkpoint_applied_sequence,
            projection_checkpoint_generation=projection_checkpoint_generation,
            projection_checkpoint_config_hash=projection_checkpoint_config_hash,
            checkpoint_replay_tail_sequence_count=checkpoint_replay_tail_sequence_count,
            repair_scan_issue_count=repair_scan_issue_count,
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
