from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.algebraic_index_stats_index_type import AlgebraicIndexStatsIndexType
from ..models.algebraic_index_stats_planner_last_decision import AlgebraicIndexStatsPlannerLastDecision
from ..types import UNSET, Unset

T = TypeVar("T", bound="AlgebraicIndexStats")


@_attrs_define
class AlgebraicIndexStats:
    """Compact public statistics for an algebraic sidecar index. Detailed runtime, adaptive, and materialization records
    remain internal diagnostics.

        Attributes:
            index_type (AlgebraicIndexStatsIndexType): Discriminator for the index stats variant.
            error (Union[Unset, str]): Error message if stats could not be retrieved
            total_indexed (Union[Unset, int]): Number of documents reflected in the algebraic sidecar
            disk_usage (Union[Unset, int]): Size of the index in bytes
            rebuilding (Union[Unset, bool]): Whether the sidecar is currently rebuilding
            backfill_progress (Union[Unset, float]): Backfill progress as a ratio from 0.0 to 1.0
            backfill_items_processed (Union[Unset, int]): Number of documents processed during current backfill
            healthy (Union[Unset, bool]):
            parse_error_count (Union[Unset, int]):
            schema_version (Union[Unset, int]):
            capability_lifecycle_status (Union[Unset, str]): Schema-derived algebraic capability lifecycle, for example
                current, stale, or rebuild_required.
            planner_selected (Union[Unset, int]):
            planner_fallback_count (Union[Unset, int]):
            planner_last_decision (Union[Unset, AlgebraicIndexStatsPlannerLastDecision]):
            planner_last_fallback_reason (Union[Unset, str]):
            planner_last_estimated_scan_rows (Union[Unset, int]): Latest algebraic planner scan-row estimate for the last
                selected or fallback decision.
            planner_last_estimated_result_buckets (Union[Unset, int]): Latest algebraic planner result-bucket estimate for
                the last selected or fallback decision.
            planner_lifecycle_ready (Union[Unset, bool]):
            planner_lifecycle_blocking_reason (Union[Unset, str]):
            adaptive_progress_count (Union[Unset, int]):
            recommendation_count (Union[Unset, int]): Number of currently recommended algebraic shapes.
            adaptive_backfilling_count (Union[Unset, int]):
            adaptive_ready_count (Union[Unset, int]):
            adaptive_stale_count (Union[Unset, int]):
            adaptive_cleanup_recommended_count (Union[Unset, int]):
            last_error_reason (Union[Unset, str]):
            active_progress_lifecycle (Union[Unset, str]):
            active_progress_rows_processed (Union[Unset, int]):
            active_progress_target_rows (Union[Unset, int]):
    """

    index_type: AlgebraicIndexStatsIndexType
    error: Union[Unset, str] = UNSET
    total_indexed: Union[Unset, int] = UNSET
    disk_usage: Union[Unset, int] = UNSET
    rebuilding: Union[Unset, bool] = UNSET
    backfill_progress: Union[Unset, float] = UNSET
    backfill_items_processed: Union[Unset, int] = UNSET
    healthy: Union[Unset, bool] = UNSET
    parse_error_count: Union[Unset, int] = UNSET
    schema_version: Union[Unset, int] = UNSET
    capability_lifecycle_status: Union[Unset, str] = UNSET
    planner_selected: Union[Unset, int] = UNSET
    planner_fallback_count: Union[Unset, int] = UNSET
    planner_last_decision: Union[Unset, AlgebraicIndexStatsPlannerLastDecision] = UNSET
    planner_last_fallback_reason: Union[Unset, str] = UNSET
    planner_last_estimated_scan_rows: Union[Unset, int] = UNSET
    planner_last_estimated_result_buckets: Union[Unset, int] = UNSET
    planner_lifecycle_ready: Union[Unset, bool] = UNSET
    planner_lifecycle_blocking_reason: Union[Unset, str] = UNSET
    adaptive_progress_count: Union[Unset, int] = UNSET
    recommendation_count: Union[Unset, int] = UNSET
    adaptive_backfilling_count: Union[Unset, int] = UNSET
    adaptive_ready_count: Union[Unset, int] = UNSET
    adaptive_stale_count: Union[Unset, int] = UNSET
    adaptive_cleanup_recommended_count: Union[Unset, int] = UNSET
    last_error_reason: Union[Unset, str] = UNSET
    active_progress_lifecycle: Union[Unset, str] = UNSET
    active_progress_rows_processed: Union[Unset, int] = UNSET
    active_progress_target_rows: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index_type = self.index_type.value

        error = self.error

        total_indexed = self.total_indexed

        disk_usage = self.disk_usage

        rebuilding = self.rebuilding

        backfill_progress = self.backfill_progress

        backfill_items_processed = self.backfill_items_processed

        healthy = self.healthy

        parse_error_count = self.parse_error_count

        schema_version = self.schema_version

        capability_lifecycle_status = self.capability_lifecycle_status

        planner_selected = self.planner_selected

        planner_fallback_count = self.planner_fallback_count

        planner_last_decision: Union[Unset, str] = UNSET
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

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index_type = AlgebraicIndexStatsIndexType(d.pop("index_type"))

        error = d.pop("error", UNSET)

        total_indexed = d.pop("total_indexed", UNSET)

        disk_usage = d.pop("disk_usage", UNSET)

        rebuilding = d.pop("rebuilding", UNSET)

        backfill_progress = d.pop("backfill_progress", UNSET)

        backfill_items_processed = d.pop("backfill_items_processed", UNSET)

        healthy = d.pop("healthy", UNSET)

        parse_error_count = d.pop("parse_error_count", UNSET)

        schema_version = d.pop("schema_version", UNSET)

        capability_lifecycle_status = d.pop("capability_lifecycle_status", UNSET)

        planner_selected = d.pop("planner_selected", UNSET)

        planner_fallback_count = d.pop("planner_fallback_count", UNSET)

        _planner_last_decision = d.pop("planner_last_decision", UNSET)
        planner_last_decision: Union[Unset, AlgebraicIndexStatsPlannerLastDecision]
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

        algebraic_index_stats = cls(
            index_type=index_type,
            error=error,
            total_indexed=total_indexed,
            disk_usage=disk_usage,
            rebuilding=rebuilding,
            backfill_progress=backfill_progress,
            backfill_items_processed=backfill_items_processed,
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
