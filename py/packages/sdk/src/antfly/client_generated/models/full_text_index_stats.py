from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.full_text_index_stats_index_type import FullTextIndexStatsIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.full_text_index_stats_async_indexing import FullTextIndexStatsAsyncIndexing
    from ..models.full_text_index_stats_promotion import FullTextIndexStatsPromotion
    from ..models.full_text_index_stats_resolution import FullTextIndexStatsResolution
    from ..models.full_text_index_stats_text_merge import FullTextIndexStatsTextMerge
    from ..models.index_readiness_status import IndexReadinessStatus
    from ..models.index_repair_status import IndexRepairStatus


T = TypeVar("T", bound="FullTextIndexStats")


@_attrs_define
class FullTextIndexStats:
    """
    Attributes:
        index_type (FullTextIndexStatsIndexType): Discriminator for the index stats variant.
        readiness (IndexReadinessStatus | Unset):
        error (str | Unset): Error message if stats could not be retrieved
        total_indexed (int | Unset): Number of documents in the index
        disk_usage (int | Unset): Size of the index in bytes
        rebuilding (bool | Unset): Whether the index is currently rebuilding
        repair (IndexRepairStatus | Unset): Compact user-facing state for an automatic index repair. Detailed
            diagnostics are available from the admin API and metrics.
        backfill_active (bool | Unset): Whether the index is actively rebuilding, replaying, or catching up.
        backfill_progress (float | Unset): Progress of ongoing rebuild as fraction [0.0, 1.0]
        backfill_items_processed (int | Unset): Number of documents indexed during current rebuild
        backfill_state (str | Unset): Operational readiness state such as ready, running, retrying, degraded, or failed.
        doc_count (int | Unset): Number of documents visible to the index.
        term_count (int | Unset): Number of indexed terms when available.
        replay_applied_sequence (int | Unset): Highest replay sequence applied to the index runtime.
        replay_target_sequence (int | Unset): Replay sequence the index runtime must reach to be current.
        replay_catch_up_required (bool | Unset): Whether replay must catch up before the index is fully current.
        runtime_present (bool | Unset):
        runtime_fresh (bool | Unset):
        runtime_source (str | Unset):
        runtime_freshness (str | Unset):
        catch_up_active (bool | Unset):
        catch_up_phase (str | Unset):
        catch_up_applied_sequence (int | Unset):
        catch_up_target_sequence (int | Unset):
        text_merge (FullTextIndexStatsTextMerge | Unset): Full-text merge runtime diagnostics.
        async_indexing (FullTextIndexStatsAsyncIndexing | Unset): Asynchronous indexer runtime diagnostics.
        projection_checkpoint_status (str | Unset): Durable projection checkpoint status: clean, rebuilding, degraded,
            or repair_required.
        projection_checkpoint_applied_sequence (int | Unset): Highest derived-log sequence covered by the durable
            projection checkpoint.
        projection_checkpoint_generation (int | Unset): Projection generation associated with the durable checkpoint.
        projection_checkpoint_config_fingerprint (str | Unset): Projection configuration identity associated with the
            durable checkpoint.
        checkpoint_replay_tail_sequence_count (int | Unset): Number of derived-log sequences after the durable
            checkpoint that still need replay.
        repair_scan_issue_count (int | Unset): Repair issues found by explicit repair-scan accounting for this
            projection.
        edge_count (int | Unset):
        node_count (int | Unset):
        repair_degraded (bool | Unset):
        repair_issue_count (int | Unset):
        repair_summary_ready (bool | Unset):
        repair_issue_count_estimated (bool | Unset):
        expected_groups (int | Unset):
        reported_groups (int | Unset):
        fresh_groups (int | Unset):
        stale_groups (int | Unset):
        missing_groups (int | Unset):
        unknown_remote_groups (int | Unset):
        resolution (FullTextIndexStatsResolution | Unset): Artifact resolution replay diagnostics.
        promotion (FullTextIndexStatsPromotion | Unset): Artifact promotion replay diagnostics.
    """

    index_type: FullTextIndexStatsIndexType
    readiness: IndexReadinessStatus | Unset = UNSET
    error: str | Unset = UNSET
    total_indexed: int | Unset = UNSET
    disk_usage: int | Unset = UNSET
    rebuilding: bool | Unset = UNSET
    repair: IndexRepairStatus | Unset = UNSET
    backfill_active: bool | Unset = UNSET
    backfill_progress: float | Unset = UNSET
    backfill_items_processed: int | Unset = UNSET
    backfill_state: str | Unset = UNSET
    doc_count: int | Unset = UNSET
    term_count: int | Unset = UNSET
    replay_applied_sequence: int | Unset = UNSET
    replay_target_sequence: int | Unset = UNSET
    replay_catch_up_required: bool | Unset = UNSET
    runtime_present: bool | Unset = UNSET
    runtime_fresh: bool | Unset = UNSET
    runtime_source: str | Unset = UNSET
    runtime_freshness: str | Unset = UNSET
    catch_up_active: bool | Unset = UNSET
    catch_up_phase: str | Unset = UNSET
    catch_up_applied_sequence: int | Unset = UNSET
    catch_up_target_sequence: int | Unset = UNSET
    text_merge: FullTextIndexStatsTextMerge | Unset = UNSET
    async_indexing: FullTextIndexStatsAsyncIndexing | Unset = UNSET
    projection_checkpoint_status: str | Unset = UNSET
    projection_checkpoint_applied_sequence: int | Unset = UNSET
    projection_checkpoint_generation: int | Unset = UNSET
    projection_checkpoint_config_fingerprint: str | Unset = UNSET
    checkpoint_replay_tail_sequence_count: int | Unset = UNSET
    repair_scan_issue_count: int | Unset = UNSET
    edge_count: int | Unset = UNSET
    node_count: int | Unset = UNSET
    repair_degraded: bool | Unset = UNSET
    repair_issue_count: int | Unset = UNSET
    repair_summary_ready: bool | Unset = UNSET
    repair_issue_count_estimated: bool | Unset = UNSET
    expected_groups: int | Unset = UNSET
    reported_groups: int | Unset = UNSET
    fresh_groups: int | Unset = UNSET
    stale_groups: int | Unset = UNSET
    missing_groups: int | Unset = UNSET
    unknown_remote_groups: int | Unset = UNSET
    resolution: FullTextIndexStatsResolution | Unset = UNSET
    promotion: FullTextIndexStatsPromotion | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        readiness: dict[str, Any] | Unset = UNSET
        if not isinstance(self.readiness, Unset):
            readiness = self.readiness.to_dict()

        error = self.error

        total_indexed = self.total_indexed

        disk_usage = self.disk_usage

        rebuilding = self.rebuilding

        repair: dict[str, Any] | Unset = UNSET
        if not isinstance(self.repair, Unset):
            repair = self.repair.to_dict()

        backfill_active = self.backfill_active

        backfill_progress = self.backfill_progress

        backfill_items_processed = self.backfill_items_processed

        backfill_state = self.backfill_state

        doc_count = self.doc_count

        term_count = self.term_count

        replay_applied_sequence = self.replay_applied_sequence

        replay_target_sequence = self.replay_target_sequence

        replay_catch_up_required = self.replay_catch_up_required

        runtime_present = self.runtime_present

        runtime_fresh = self.runtime_fresh

        runtime_source = self.runtime_source

        runtime_freshness = self.runtime_freshness

        catch_up_active = self.catch_up_active

        catch_up_phase = self.catch_up_phase

        catch_up_applied_sequence = self.catch_up_applied_sequence

        catch_up_target_sequence = self.catch_up_target_sequence

        text_merge: dict[str, Any] | Unset = UNSET
        if not isinstance(self.text_merge, Unset):
            text_merge = self.text_merge.to_dict()

        async_indexing: dict[str, Any] | Unset = UNSET
        if not isinstance(self.async_indexing, Unset):
            async_indexing = self.async_indexing.to_dict()

        projection_checkpoint_status = self.projection_checkpoint_status

        projection_checkpoint_applied_sequence = self.projection_checkpoint_applied_sequence

        projection_checkpoint_generation = self.projection_checkpoint_generation

        projection_checkpoint_config_fingerprint = self.projection_checkpoint_config_fingerprint

        checkpoint_replay_tail_sequence_count = self.checkpoint_replay_tail_sequence_count

        repair_scan_issue_count = self.repair_scan_issue_count

        edge_count = self.edge_count

        node_count = self.node_count

        repair_degraded = self.repair_degraded

        repair_issue_count = self.repair_issue_count

        repair_summary_ready = self.repair_summary_ready

        repair_issue_count_estimated = self.repair_issue_count_estimated

        expected_groups = self.expected_groups

        reported_groups = self.reported_groups

        fresh_groups = self.fresh_groups

        stale_groups = self.stale_groups

        missing_groups = self.missing_groups

        unknown_remote_groups = self.unknown_remote_groups

        resolution: dict[str, Any] | Unset = UNSET
        if not isinstance(self.resolution, Unset):
            resolution = self.resolution.to_dict()

        promotion: dict[str, Any] | Unset = UNSET
        if not isinstance(self.promotion, Unset):
            promotion = self.promotion.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index_type": index_type,
            }
        )
        if readiness is not UNSET:
            field_dict["readiness"] = readiness
        if error is not UNSET:
            field_dict["error"] = error
        if total_indexed is not UNSET:
            field_dict["total_indexed"] = total_indexed
        if disk_usage is not UNSET:
            field_dict["disk_usage"] = disk_usage
        if rebuilding is not UNSET:
            field_dict["rebuilding"] = rebuilding
        if repair is not UNSET:
            field_dict["repair"] = repair
        if backfill_active is not UNSET:
            field_dict["backfill_active"] = backfill_active
        if backfill_progress is not UNSET:
            field_dict["backfill_progress"] = backfill_progress
        if backfill_items_processed is not UNSET:
            field_dict["backfill_items_processed"] = backfill_items_processed
        if backfill_state is not UNSET:
            field_dict["backfill_state"] = backfill_state
        if doc_count is not UNSET:
            field_dict["doc_count"] = doc_count
        if term_count is not UNSET:
            field_dict["term_count"] = term_count
        if replay_applied_sequence is not UNSET:
            field_dict["replay_applied_sequence"] = replay_applied_sequence
        if replay_target_sequence is not UNSET:
            field_dict["replay_target_sequence"] = replay_target_sequence
        if replay_catch_up_required is not UNSET:
            field_dict["replay_catch_up_required"] = replay_catch_up_required
        if runtime_present is not UNSET:
            field_dict["runtime_present"] = runtime_present
        if runtime_fresh is not UNSET:
            field_dict["runtime_fresh"] = runtime_fresh
        if runtime_source is not UNSET:
            field_dict["runtime_source"] = runtime_source
        if runtime_freshness is not UNSET:
            field_dict["runtime_freshness"] = runtime_freshness
        if catch_up_active is not UNSET:
            field_dict["catch_up_active"] = catch_up_active
        if catch_up_phase is not UNSET:
            field_dict["catch_up_phase"] = catch_up_phase
        if catch_up_applied_sequence is not UNSET:
            field_dict["catch_up_applied_sequence"] = catch_up_applied_sequence
        if catch_up_target_sequence is not UNSET:
            field_dict["catch_up_target_sequence"] = catch_up_target_sequence
        if text_merge is not UNSET:
            field_dict["text_merge"] = text_merge
        if async_indexing is not UNSET:
            field_dict["async_indexing"] = async_indexing
        if projection_checkpoint_status is not UNSET:
            field_dict["projection_checkpoint_status"] = projection_checkpoint_status
        if projection_checkpoint_applied_sequence is not UNSET:
            field_dict["projection_checkpoint_applied_sequence"] = projection_checkpoint_applied_sequence
        if projection_checkpoint_generation is not UNSET:
            field_dict["projection_checkpoint_generation"] = projection_checkpoint_generation
        if projection_checkpoint_config_fingerprint is not UNSET:
            field_dict["projection_checkpoint_config_fingerprint"] = projection_checkpoint_config_fingerprint
        if checkpoint_replay_tail_sequence_count is not UNSET:
            field_dict["checkpoint_replay_tail_sequence_count"] = checkpoint_replay_tail_sequence_count
        if repair_scan_issue_count is not UNSET:
            field_dict["repair_scan_issue_count"] = repair_scan_issue_count
        if edge_count is not UNSET:
            field_dict["edge_count"] = edge_count
        if node_count is not UNSET:
            field_dict["node_count"] = node_count
        if repair_degraded is not UNSET:
            field_dict["repair_degraded"] = repair_degraded
        if repair_issue_count is not UNSET:
            field_dict["repair_issue_count"] = repair_issue_count
        if repair_summary_ready is not UNSET:
            field_dict["repair_summary_ready"] = repair_summary_ready
        if repair_issue_count_estimated is not UNSET:
            field_dict["repair_issue_count_estimated"] = repair_issue_count_estimated
        if expected_groups is not UNSET:
            field_dict["expected_groups"] = expected_groups
        if reported_groups is not UNSET:
            field_dict["reported_groups"] = reported_groups
        if fresh_groups is not UNSET:
            field_dict["fresh_groups"] = fresh_groups
        if stale_groups is not UNSET:
            field_dict["stale_groups"] = stale_groups
        if missing_groups is not UNSET:
            field_dict["missing_groups"] = missing_groups
        if unknown_remote_groups is not UNSET:
            field_dict["unknown_remote_groups"] = unknown_remote_groups
        if resolution is not UNSET:
            field_dict["resolution"] = resolution
        if promotion is not UNSET:
            field_dict["promotion"] = promotion

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.full_text_index_stats_async_indexing import FullTextIndexStatsAsyncIndexing
        from ..models.full_text_index_stats_promotion import FullTextIndexStatsPromotion
        from ..models.full_text_index_stats_resolution import FullTextIndexStatsResolution
        from ..models.full_text_index_stats_text_merge import FullTextIndexStatsTextMerge
        from ..models.index_readiness_status import IndexReadinessStatus
        from ..models.index_repair_status import IndexRepairStatus

        d = dict(src_dict)
        index_type = FullTextIndexStatsIndexType(d.pop("index_type"))

        _readiness = d.pop("readiness", UNSET)
        readiness: IndexReadinessStatus | Unset
        if isinstance(_readiness, Unset):
            readiness = UNSET
        else:
            readiness = IndexReadinessStatus.from_dict(_readiness)

        error = d.pop("error", UNSET)

        total_indexed = d.pop("total_indexed", UNSET)

        disk_usage = d.pop("disk_usage", UNSET)

        rebuilding = d.pop("rebuilding", UNSET)

        _repair = d.pop("repair", UNSET)
        repair: IndexRepairStatus | Unset
        if isinstance(_repair, Unset):
            repair = UNSET
        else:
            repair = IndexRepairStatus.from_dict(_repair)

        backfill_active = d.pop("backfill_active", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_items_processed = d.pop("backfill_items_processed", UNSET)

        backfill_state = d.pop("backfill_state", UNSET)

        doc_count = d.pop("doc_count", UNSET)

        term_count = d.pop("term_count", UNSET)

        replay_applied_sequence = d.pop("replay_applied_sequence", UNSET)

        replay_target_sequence = d.pop("replay_target_sequence", UNSET)

        replay_catch_up_required = d.pop("replay_catch_up_required", UNSET)

        runtime_present = d.pop("runtime_present", UNSET)

        runtime_fresh = d.pop("runtime_fresh", UNSET)

        runtime_source = d.pop("runtime_source", UNSET)

        runtime_freshness = d.pop("runtime_freshness", UNSET)

        catch_up_active = d.pop("catch_up_active", UNSET)

        catch_up_phase = d.pop("catch_up_phase", UNSET)

        catch_up_applied_sequence = d.pop("catch_up_applied_sequence", UNSET)

        catch_up_target_sequence = d.pop("catch_up_target_sequence", UNSET)

        _text_merge = d.pop("text_merge", UNSET)
        text_merge: FullTextIndexStatsTextMerge | Unset
        if isinstance(_text_merge, Unset):
            text_merge = UNSET
        else:
            text_merge = FullTextIndexStatsTextMerge.from_dict(_text_merge)

        _async_indexing = d.pop("async_indexing", UNSET)
        async_indexing: FullTextIndexStatsAsyncIndexing | Unset
        if isinstance(_async_indexing, Unset):
            async_indexing = UNSET
        else:
            async_indexing = FullTextIndexStatsAsyncIndexing.from_dict(_async_indexing)

        projection_checkpoint_status = d.pop("projection_checkpoint_status", UNSET)

        projection_checkpoint_applied_sequence = d.pop("projection_checkpoint_applied_sequence", UNSET)

        projection_checkpoint_generation = d.pop("projection_checkpoint_generation", UNSET)

        projection_checkpoint_config_fingerprint = d.pop("projection_checkpoint_config_fingerprint", UNSET)

        checkpoint_replay_tail_sequence_count = d.pop("checkpoint_replay_tail_sequence_count", UNSET)

        repair_scan_issue_count = d.pop("repair_scan_issue_count", UNSET)

        edge_count = d.pop("edge_count", UNSET)

        node_count = d.pop("node_count", UNSET)

        repair_degraded = d.pop("repair_degraded", UNSET)

        repair_issue_count = d.pop("repair_issue_count", UNSET)

        repair_summary_ready = d.pop("repair_summary_ready", UNSET)

        repair_issue_count_estimated = d.pop("repair_issue_count_estimated", UNSET)

        expected_groups = d.pop("expected_groups", UNSET)

        reported_groups = d.pop("reported_groups", UNSET)

        fresh_groups = d.pop("fresh_groups", UNSET)

        stale_groups = d.pop("stale_groups", UNSET)

        missing_groups = d.pop("missing_groups", UNSET)

        unknown_remote_groups = d.pop("unknown_remote_groups", UNSET)

        _resolution = d.pop("resolution", UNSET)
        resolution: FullTextIndexStatsResolution | Unset
        if isinstance(_resolution, Unset):
            resolution = UNSET
        else:
            resolution = FullTextIndexStatsResolution.from_dict(_resolution)

        _promotion = d.pop("promotion", UNSET)
        promotion: FullTextIndexStatsPromotion | Unset
        if isinstance(_promotion, Unset):
            promotion = UNSET
        else:
            promotion = FullTextIndexStatsPromotion.from_dict(_promotion)

        full_text_index_stats = cls(
            index_type=index_type,
            readiness=readiness,
            error=error,
            total_indexed=total_indexed,
            disk_usage=disk_usage,
            rebuilding=rebuilding,
            repair=repair,
            backfill_active=backfill_active,
            backfill_progress=backfill_progress,
            backfill_items_processed=backfill_items_processed,
            backfill_state=backfill_state,
            doc_count=doc_count,
            term_count=term_count,
            replay_applied_sequence=replay_applied_sequence,
            replay_target_sequence=replay_target_sequence,
            replay_catch_up_required=replay_catch_up_required,
            runtime_present=runtime_present,
            runtime_fresh=runtime_fresh,
            runtime_source=runtime_source,
            runtime_freshness=runtime_freshness,
            catch_up_active=catch_up_active,
            catch_up_phase=catch_up_phase,
            catch_up_applied_sequence=catch_up_applied_sequence,
            catch_up_target_sequence=catch_up_target_sequence,
            text_merge=text_merge,
            async_indexing=async_indexing,
            projection_checkpoint_status=projection_checkpoint_status,
            projection_checkpoint_applied_sequence=projection_checkpoint_applied_sequence,
            projection_checkpoint_generation=projection_checkpoint_generation,
            projection_checkpoint_config_fingerprint=projection_checkpoint_config_fingerprint,
            checkpoint_replay_tail_sequence_count=checkpoint_replay_tail_sequence_count,
            repair_scan_issue_count=repair_scan_issue_count,
            edge_count=edge_count,
            node_count=node_count,
            repair_degraded=repair_degraded,
            repair_issue_count=repair_issue_count,
            repair_summary_ready=repair_summary_ready,
            repair_issue_count_estimated=repair_issue_count_estimated,
            expected_groups=expected_groups,
            reported_groups=reported_groups,
            fresh_groups=fresh_groups,
            stale_groups=stale_groups,
            missing_groups=missing_groups,
            unknown_remote_groups=unknown_remote_groups,
            resolution=resolution,
            promotion=promotion,
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
