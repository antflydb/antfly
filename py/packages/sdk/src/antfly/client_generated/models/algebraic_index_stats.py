from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.algebraic_index_stats_index_type import AlgebraicIndexStatsIndexType
from ..models.algebraic_index_stats_planner_last_decision import AlgebraicIndexStatsPlannerLastDecision
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.algebraic_index_stats_async_indexing import AlgebraicIndexStatsAsyncIndexing
    from ..models.algebraic_index_stats_promotion import AlgebraicIndexStatsPromotion
    from ..models.algebraic_index_stats_resolution import AlgebraicIndexStatsResolution
    from ..models.algebraic_index_stats_resolver_replay import AlgebraicIndexStatsResolverReplay
    from ..models.algebraic_index_stats_source_artifact import AlgebraicIndexStatsSourceArtifact
    from ..models.algebraic_index_stats_source_artifacts_item import AlgebraicIndexStatsSourceArtifactsItem


T = TypeVar("T", bound="AlgebraicIndexStats")


@_attrs_define
class AlgebraicIndexStats:
    """Compact public statistics for an algebraic sidecar index. Detailed runtime, adaptive, and materialization records
    remain internal diagnostics.

        Attributes:
            index_type (AlgebraicIndexStatsIndexType): Discriminator for the index stats variant.
            error (str | Unset): Error message if stats could not be retrieved
            total_indexed (int | Unset): Number of documents reflected in the algebraic sidecar
            disk_usage (int | Unset): Size of the index in bytes
            rebuilding (bool | Unset): Whether the sidecar is currently rebuilding
            backfill_active (bool | Unset): Whether the sidecar is actively rebuilding, replaying, or catching up.
            backfill_progress (float | Unset): Backfill progress as a ratio from 0.0 to 1.0
            backfill_items_processed (int | Unset): Number of documents processed during current backfill
            backfill_state (str | Unset): Operational readiness state such as ready, running, retrying, or failed.
            doc_count (int | Unset): Number of documents visible to the sidecar.
            term_count (int | Unset):
            replay_applied_sequence (int | Unset):
            replay_target_sequence (int | Unset):
            replay_catch_up_required (bool | Unset):
            runtime_present (bool | Unset):
            runtime_fresh (bool | Unset):
            runtime_source (str | Unset):
            runtime_freshness (str | Unset):
            catch_up_active (bool | Unset):
            catch_up_phase (str | Unset):
            catch_up_applied_sequence (int | Unset):
            catch_up_target_sequence (int | Unset):
            async_indexing (AlgebraicIndexStatsAsyncIndexing | Unset):
            healthy (bool | Unset):
            parse_error_count (int | Unset):
            schema_version (int | Unset):
            capability_lifecycle_status (str | Unset): Schema-derived algebraic capability lifecycle, for example current,
                stale, or rebuild_required.
            planner_selected (int | Unset):
            planner_fallback_count (int | Unset):
            planner_last_decision (AlgebraicIndexStatsPlannerLastDecision | Unset):
            planner_last_fallback_reason (str | Unset):
            planner_last_estimated_scan_rows (int | Unset): Latest algebraic planner scan-row estimate for the last selected
                or fallback decision.
            planner_last_estimated_result_buckets (int | Unset): Latest algebraic planner result-bucket estimate for the
                last selected or fallback decision.
            planner_lifecycle_ready (bool | Unset):
            planner_lifecycle_blocking_reason (str | Unset):
            adaptive_progress_count (int | Unset):
            recommendation_count (int | Unset): Number of currently recommended algebraic shapes.
            adaptive_backfilling_count (int | Unset):
            adaptive_ready_count (int | Unset):
            adaptive_stale_count (int | Unset):
            adaptive_cleanup_recommended_count (int | Unset):
            last_error_reason (str | Unset):
            active_progress_lifecycle (str | Unset):
            active_progress_rows_processed (int | Unset):
            active_progress_target_rows (int | Unset):
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
            source_artifact (AlgebraicIndexStatsSourceArtifact | Unset): Source artifact stream used to materialize graph
                edges.
            source_artifacts (list[AlgebraicIndexStatsSourceArtifactsItem] | Unset): All source artifact streams used to
                materialize graph edges, in configured order.
            resolver_replay (AlgebraicIndexStatsResolverReplay | Unset): Graph resolver replay diagnostics.
            resolution (AlgebraicIndexStatsResolution | Unset): Artifact resolution replay diagnostics.
            promotion (AlgebraicIndexStatsPromotion | Unset): Artifact promotion replay diagnostics.
    """

    index_type: AlgebraicIndexStatsIndexType
    error: str | Unset = UNSET
    total_indexed: int | Unset = UNSET
    disk_usage: int | Unset = UNSET
    rebuilding: bool | Unset = UNSET
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
    async_indexing: AlgebraicIndexStatsAsyncIndexing | Unset = UNSET
    healthy: bool | Unset = UNSET
    parse_error_count: int | Unset = UNSET
    schema_version: int | Unset = UNSET
    capability_lifecycle_status: str | Unset = UNSET
    planner_selected: int | Unset = UNSET
    planner_fallback_count: int | Unset = UNSET
    planner_last_decision: AlgebraicIndexStatsPlannerLastDecision | Unset = UNSET
    planner_last_fallback_reason: str | Unset = UNSET
    planner_last_estimated_scan_rows: int | Unset = UNSET
    planner_last_estimated_result_buckets: int | Unset = UNSET
    planner_lifecycle_ready: bool | Unset = UNSET
    planner_lifecycle_blocking_reason: str | Unset = UNSET
    adaptive_progress_count: int | Unset = UNSET
    recommendation_count: int | Unset = UNSET
    adaptive_backfilling_count: int | Unset = UNSET
    adaptive_ready_count: int | Unset = UNSET
    adaptive_stale_count: int | Unset = UNSET
    adaptive_cleanup_recommended_count: int | Unset = UNSET
    last_error_reason: str | Unset = UNSET
    active_progress_lifecycle: str | Unset = UNSET
    active_progress_rows_processed: int | Unset = UNSET
    active_progress_target_rows: int | Unset = UNSET
    projection_checkpoint_status: str | Unset = UNSET
    projection_checkpoint_applied_sequence: int | Unset = UNSET
    projection_checkpoint_generation: int | Unset = UNSET
    projection_checkpoint_config_hash: int | Unset = UNSET
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
    source_artifact: AlgebraicIndexStatsSourceArtifact | Unset = UNSET
    source_artifacts: list[AlgebraicIndexStatsSourceArtifactsItem] | Unset = UNSET
    resolver_replay: AlgebraicIndexStatsResolverReplay | Unset = UNSET
    resolution: AlgebraicIndexStatsResolution | Unset = UNSET
    promotion: AlgebraicIndexStatsPromotion | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        error = self.error

        total_indexed = self.total_indexed

        disk_usage = self.disk_usage

        rebuilding = self.rebuilding

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

        async_indexing: dict[str, Any] | Unset = UNSET
        if not isinstance(self.async_indexing, Unset):
            async_indexing = self.async_indexing.to_dict()

        healthy = self.healthy

        parse_error_count = self.parse_error_count

        schema_version = self.schema_version

        capability_lifecycle_status = self.capability_lifecycle_status

        planner_selected = self.planner_selected

        planner_fallback_count = self.planner_fallback_count

        planner_last_decision: str | Unset = UNSET
        if not isinstance(self.planner_last_decision, Unset):
            planner_last_decision = self.planner_last_decision.value

        planner_last_fallback_reason = self.planner_last_fallback_reason

        planner_last_estimated_scan_rows = self.planner_last_estimated_scan_rows

        planner_last_estimated_result_buckets = self.planner_last_estimated_result_buckets

        planner_lifecycle_ready = self.planner_lifecycle_ready

        planner_lifecycle_blocking_reason = self.planner_lifecycle_blocking_reason

        adaptive_progress_count = self.adaptive_progress_count

        recommendation_count = self.recommendation_count

        adaptive_backfilling_count = self.adaptive_backfilling_count

        adaptive_ready_count = self.adaptive_ready_count

        adaptive_stale_count = self.adaptive_stale_count

        adaptive_cleanup_recommended_count = self.adaptive_cleanup_recommended_count

        last_error_reason = self.last_error_reason

        active_progress_lifecycle = self.active_progress_lifecycle

        active_progress_rows_processed = self.active_progress_rows_processed

        active_progress_target_rows = self.active_progress_target_rows

        projection_checkpoint_status = self.projection_checkpoint_status

        projection_checkpoint_applied_sequence = self.projection_checkpoint_applied_sequence

        projection_checkpoint_generation = self.projection_checkpoint_generation

        projection_checkpoint_config_hash = self.projection_checkpoint_config_hash

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

        source_artifact: dict[str, Any] | Unset = UNSET
        if not isinstance(self.source_artifact, Unset):
            source_artifact = self.source_artifact.to_dict()

        source_artifacts: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.source_artifacts, Unset):
            source_artifacts = []
            for source_artifacts_item_data in self.source_artifacts:
                source_artifacts_item = source_artifacts_item_data.to_dict()
                source_artifacts.append(source_artifacts_item)

        resolver_replay: dict[str, Any] | Unset = UNSET
        if not isinstance(self.resolver_replay, Unset):
            resolver_replay = self.resolver_replay.to_dict()

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
        if error is not UNSET:
            field_dict["error"] = error
        if total_indexed is not UNSET:
            field_dict["total_indexed"] = total_indexed
        if disk_usage is not UNSET:
            field_dict["disk_usage"] = disk_usage
        if rebuilding is not UNSET:
            field_dict["rebuilding"] = rebuilding
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
        if async_indexing is not UNSET:
            field_dict["async_indexing"] = async_indexing
        if healthy is not UNSET:
            field_dict["healthy"] = healthy
        if parse_error_count is not UNSET:
            field_dict["parse_error_count"] = parse_error_count
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version
        if capability_lifecycle_status is not UNSET:
            field_dict["capability_lifecycle_status"] = capability_lifecycle_status
        if planner_selected is not UNSET:
            field_dict["planner_selected"] = planner_selected
        if planner_fallback_count is not UNSET:
            field_dict["planner_fallback_count"] = planner_fallback_count
        if planner_last_decision is not UNSET:
            field_dict["planner_last_decision"] = planner_last_decision
        if planner_last_fallback_reason is not UNSET:
            field_dict["planner_last_fallback_reason"] = planner_last_fallback_reason
        if planner_last_estimated_scan_rows is not UNSET:
            field_dict["planner_last_estimated_scan_rows"] = planner_last_estimated_scan_rows
        if planner_last_estimated_result_buckets is not UNSET:
            field_dict["planner_last_estimated_result_buckets"] = planner_last_estimated_result_buckets
        if planner_lifecycle_ready is not UNSET:
            field_dict["planner_lifecycle_ready"] = planner_lifecycle_ready
        if planner_lifecycle_blocking_reason is not UNSET:
            field_dict["planner_lifecycle_blocking_reason"] = planner_lifecycle_blocking_reason
        if adaptive_progress_count is not UNSET:
            field_dict["adaptive_progress_count"] = adaptive_progress_count
        if recommendation_count is not UNSET:
            field_dict["recommendation_count"] = recommendation_count
        if adaptive_backfilling_count is not UNSET:
            field_dict["adaptive_backfilling_count"] = adaptive_backfilling_count
        if adaptive_ready_count is not UNSET:
            field_dict["adaptive_ready_count"] = adaptive_ready_count
        if adaptive_stale_count is not UNSET:
            field_dict["adaptive_stale_count"] = adaptive_stale_count
        if adaptive_cleanup_recommended_count is not UNSET:
            field_dict["adaptive_cleanup_recommended_count"] = adaptive_cleanup_recommended_count
        if last_error_reason is not UNSET:
            field_dict["last_error_reason"] = last_error_reason
        if active_progress_lifecycle is not UNSET:
            field_dict["active_progress_lifecycle"] = active_progress_lifecycle
        if active_progress_rows_processed is not UNSET:
            field_dict["active_progress_rows_processed"] = active_progress_rows_processed
        if active_progress_target_rows is not UNSET:
            field_dict["active_progress_target_rows"] = active_progress_target_rows
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
        if source_artifact is not UNSET:
            field_dict["source_artifact"] = source_artifact
        if source_artifacts is not UNSET:
            field_dict["source_artifacts"] = source_artifacts
        if resolver_replay is not UNSET:
            field_dict["resolver_replay"] = resolver_replay
        if resolution is not UNSET:
            field_dict["resolution"] = resolution
        if promotion is not UNSET:
            field_dict["promotion"] = promotion

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.algebraic_index_stats_async_indexing import AlgebraicIndexStatsAsyncIndexing
        from ..models.algebraic_index_stats_promotion import AlgebraicIndexStatsPromotion
        from ..models.algebraic_index_stats_resolution import AlgebraicIndexStatsResolution
        from ..models.algebraic_index_stats_resolver_replay import AlgebraicIndexStatsResolverReplay
        from ..models.algebraic_index_stats_source_artifact import AlgebraicIndexStatsSourceArtifact
        from ..models.algebraic_index_stats_source_artifacts_item import AlgebraicIndexStatsSourceArtifactsItem

        d = dict(src_dict)
        index_type = AlgebraicIndexStatsIndexType(d.pop("index_type"))

        error = d.pop("error", UNSET)

        total_indexed = d.pop("total_indexed", UNSET)

        disk_usage = d.pop("disk_usage", UNSET)

        rebuilding = d.pop("rebuilding", UNSET)

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

        _async_indexing = d.pop("async_indexing", UNSET)
        async_indexing: AlgebraicIndexStatsAsyncIndexing | Unset
        if isinstance(_async_indexing, Unset):
            async_indexing = UNSET
        else:
            async_indexing = AlgebraicIndexStatsAsyncIndexing.from_dict(_async_indexing)

        healthy = d.pop("healthy", UNSET)

        parse_error_count = d.pop("parse_error_count", UNSET)

        schema_version = d.pop("schema_version", UNSET)

        capability_lifecycle_status = d.pop("capability_lifecycle_status", UNSET)

        planner_selected = d.pop("planner_selected", UNSET)

        planner_fallback_count = d.pop("planner_fallback_count", UNSET)

        _planner_last_decision = d.pop("planner_last_decision", UNSET)
        planner_last_decision: AlgebraicIndexStatsPlannerLastDecision | Unset
        if isinstance(_planner_last_decision, Unset):
            planner_last_decision = UNSET
        else:
            planner_last_decision = AlgebraicIndexStatsPlannerLastDecision(_planner_last_decision)

        planner_last_fallback_reason = d.pop("planner_last_fallback_reason", UNSET)

        planner_last_estimated_scan_rows = d.pop("planner_last_estimated_scan_rows", UNSET)

        planner_last_estimated_result_buckets = d.pop("planner_last_estimated_result_buckets", UNSET)

        planner_lifecycle_ready = d.pop("planner_lifecycle_ready", UNSET)

        planner_lifecycle_blocking_reason = d.pop("planner_lifecycle_blocking_reason", UNSET)

        adaptive_progress_count = d.pop("adaptive_progress_count", UNSET)

        recommendation_count = d.pop("recommendation_count", UNSET)

        adaptive_backfilling_count = d.pop("adaptive_backfilling_count", UNSET)

        adaptive_ready_count = d.pop("adaptive_ready_count", UNSET)

        adaptive_stale_count = d.pop("adaptive_stale_count", UNSET)

        adaptive_cleanup_recommended_count = d.pop("adaptive_cleanup_recommended_count", UNSET)

        last_error_reason = d.pop("last_error_reason", UNSET)

        active_progress_lifecycle = d.pop("active_progress_lifecycle", UNSET)

        active_progress_rows_processed = d.pop("active_progress_rows_processed", UNSET)

        active_progress_target_rows = d.pop("active_progress_target_rows", UNSET)

        projection_checkpoint_status = d.pop("projection_checkpoint_status", UNSET)

        projection_checkpoint_applied_sequence = d.pop("projection_checkpoint_applied_sequence", UNSET)

        projection_checkpoint_generation = d.pop("projection_checkpoint_generation", UNSET)

        projection_checkpoint_config_hash = d.pop("projection_checkpoint_config_hash", UNSET)

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

        _source_artifact = d.pop("source_artifact", UNSET)
        source_artifact: AlgebraicIndexStatsSourceArtifact | Unset
        if isinstance(_source_artifact, Unset):
            source_artifact = UNSET
        else:
            source_artifact = AlgebraicIndexStatsSourceArtifact.from_dict(_source_artifact)

        _source_artifacts = d.pop("source_artifacts", UNSET)
        source_artifacts: list[AlgebraicIndexStatsSourceArtifactsItem] | Unset = UNSET
        if _source_artifacts is not UNSET:
            source_artifacts = []
            for source_artifacts_item_data in _source_artifacts:
                source_artifacts_item = AlgebraicIndexStatsSourceArtifactsItem.from_dict(source_artifacts_item_data)

                source_artifacts.append(source_artifacts_item)

        _resolver_replay = d.pop("resolver_replay", UNSET)
        resolver_replay: AlgebraicIndexStatsResolverReplay | Unset
        if isinstance(_resolver_replay, Unset):
            resolver_replay = UNSET
        else:
            resolver_replay = AlgebraicIndexStatsResolverReplay.from_dict(_resolver_replay)

        _resolution = d.pop("resolution", UNSET)
        resolution: AlgebraicIndexStatsResolution | Unset
        if isinstance(_resolution, Unset):
            resolution = UNSET
        else:
            resolution = AlgebraicIndexStatsResolution.from_dict(_resolution)

        _promotion = d.pop("promotion", UNSET)
        promotion: AlgebraicIndexStatsPromotion | Unset
        if isinstance(_promotion, Unset):
            promotion = UNSET
        else:
            promotion = AlgebraicIndexStatsPromotion.from_dict(_promotion)

        algebraic_index_stats = cls(
            index_type=index_type,
            error=error,
            total_indexed=total_indexed,
            disk_usage=disk_usage,
            rebuilding=rebuilding,
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
            async_indexing=async_indexing,
            healthy=healthy,
            parse_error_count=parse_error_count,
            schema_version=schema_version,
            capability_lifecycle_status=capability_lifecycle_status,
            planner_selected=planner_selected,
            planner_fallback_count=planner_fallback_count,
            planner_last_decision=planner_last_decision,
            planner_last_fallback_reason=planner_last_fallback_reason,
            planner_last_estimated_scan_rows=planner_last_estimated_scan_rows,
            planner_last_estimated_result_buckets=planner_last_estimated_result_buckets,
            planner_lifecycle_ready=planner_lifecycle_ready,
            planner_lifecycle_blocking_reason=planner_lifecycle_blocking_reason,
            adaptive_progress_count=adaptive_progress_count,
            recommendation_count=recommendation_count,
            adaptive_backfilling_count=adaptive_backfilling_count,
            adaptive_ready_count=adaptive_ready_count,
            adaptive_stale_count=adaptive_stale_count,
            adaptive_cleanup_recommended_count=adaptive_cleanup_recommended_count,
            last_error_reason=last_error_reason,
            active_progress_lifecycle=active_progress_lifecycle,
            active_progress_rows_processed=active_progress_rows_processed,
            active_progress_target_rows=active_progress_target_rows,
            projection_checkpoint_status=projection_checkpoint_status,
            projection_checkpoint_applied_sequence=projection_checkpoint_applied_sequence,
            projection_checkpoint_generation=projection_checkpoint_generation,
            projection_checkpoint_config_hash=projection_checkpoint_config_hash,
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
            source_artifact=source_artifact,
            source_artifacts=source_artifacts,
            resolver_replay=resolver_replay,
            resolution=resolution,
            promotion=promotion,
        )

        algebraic_index_stats.additional_properties = d
        return algebraic_index_stats

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
