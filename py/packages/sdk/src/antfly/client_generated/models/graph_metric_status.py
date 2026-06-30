from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_status_phase import GraphMetricStatusPhase
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_metric_build_page_status import GraphMetricBuildPageStatus
    from ..models.graph_metric_edge_filter_status import GraphMetricEdgeFilterStatus
    from ..models.graph_metric_event import GraphMetricEvent


T = TypeVar("T", bound="GraphMetricStatus")


@_attrs_define
class GraphMetricStatus:
    """
    Attributes:
        state (str):
        phase (GraphMetricStatusPhase):
        build_queued (bool): Whether a local or distributed build is queued after the currently published or building
            generation.
        published_generation (int):
        edge_generation (int):
        target_edge_generation (int):
        progress (float): Build progress for the target edge generation, from 0.0 to 1.0
        converged (bool):
        iterations_completed (int):
        delta (float):
        computed_at_ms (int):
        edge_filter (GraphMetricEdgeFilterStatus | Unset):
        metadata_version (int | Unset): Version of the published graph metric metadata schema.
        maintenance_paused (bool | Unset):
        queued_generation (int | Unset): Pending edge generation waiting to build, or 0 when no build is queued.
        building_generation (int | Unset): Edge generation currently held by an active build lease, or 0 when idle.
        build_job_id (int | Unset): Durable identifier for the active graph metric build job, or 0 when idle.
        build_started_at_ms (int | Unset): Unix epoch milliseconds when the active graph metric build started, or 0 when
            idle.
        build_iteration (int | Unset): Iteration number reported by the active build lease, or 0 when idle or not
            iterative.
        build_lease_expires_at_ms (int | Unset): Unix epoch milliseconds when the active build lease expires, or 0 when
            idle.
        build_worker_id (str | Unset): Worker id that owns the active build lease. Local builds use `local`.
        build_cursor (str | Unset): Opaque resumable cursor for the active build phase. Empty or omitted when idle or
            when the phase has no cursor.
        build_completed_units (int | Unset): Completed work units for the active graph metric build, or 0 when idle or
            unknown.
        build_total_units (int | Unset): Estimated total work units for the active graph metric build, or 0 when idle or
            unknown.
        build_pages (list[GraphMetricBuildPageStatus] | Unset): Active leased or failed build pages for the current
            build phase, capped and ordered by durable page key.
        build_pages_truncated (bool | Unset): Whether build_pages was capped before every active page could be included.
        retry_count (int | Unset): Number of consecutive failed build attempts for the current target generation, or 0
            when no failure applies.
        last_error (str | Unset): Last build error for the current failed target generation.
        last_event (GraphMetricEvent | Unset):
        recent_events (list[GraphMetricEvent] | Unset): Recent graph metric events, newest first.
    """

    state: str
    phase: GraphMetricStatusPhase
    build_queued: bool
    published_generation: int
    edge_generation: int
    target_edge_generation: int
    progress: float
    converged: bool
    iterations_completed: int
    delta: float
    computed_at_ms: int
    edge_filter: GraphMetricEdgeFilterStatus | Unset = UNSET
    metadata_version: int | Unset = UNSET
    maintenance_paused: bool | Unset = UNSET
    queued_generation: int | Unset = UNSET
    building_generation: int | Unset = UNSET
    build_job_id: int | Unset = UNSET
    build_started_at_ms: int | Unset = UNSET
    build_iteration: int | Unset = UNSET
    build_lease_expires_at_ms: int | Unset = UNSET
    build_worker_id: str | Unset = UNSET
    build_cursor: str | Unset = UNSET
    build_completed_units: int | Unset = UNSET
    build_total_units: int | Unset = UNSET
    build_pages: list[GraphMetricBuildPageStatus] | Unset = UNSET
    build_pages_truncated: bool | Unset = UNSET
    retry_count: int | Unset = UNSET
    last_error: str | Unset = UNSET
    last_event: GraphMetricEvent | Unset = UNSET
    recent_events: list[GraphMetricEvent] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        state = self.state

        phase = self.phase.value

        build_queued = self.build_queued

        published_generation = self.published_generation

        edge_generation = self.edge_generation

        target_edge_generation = self.target_edge_generation

        progress = self.progress

        converged = self.converged

        iterations_completed = self.iterations_completed

        delta = self.delta

        computed_at_ms = self.computed_at_ms

        edge_filter: dict[str, Any] | Unset = UNSET
        if not isinstance(self.edge_filter, Unset):
            edge_filter = self.edge_filter.to_dict()

        metadata_version = self.metadata_version

        maintenance_paused = self.maintenance_paused

        queued_generation = self.queued_generation

        building_generation = self.building_generation

        build_job_id = self.build_job_id

        build_started_at_ms = self.build_started_at_ms

        build_iteration = self.build_iteration

        build_lease_expires_at_ms = self.build_lease_expires_at_ms

        build_worker_id = self.build_worker_id

        build_cursor = self.build_cursor

        build_completed_units = self.build_completed_units

        build_total_units = self.build_total_units

        build_pages: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.build_pages, Unset):
            build_pages = []
            for build_pages_item_data in self.build_pages:
                build_pages_item = build_pages_item_data.to_dict()
                build_pages.append(build_pages_item)

        build_pages_truncated = self.build_pages_truncated

        retry_count = self.retry_count

        last_error = self.last_error

        last_event: dict[str, Any] | Unset = UNSET
        if not isinstance(self.last_event, Unset):
            last_event = self.last_event.to_dict()

        recent_events: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.recent_events, Unset):
            recent_events = []
            for recent_events_item_data in self.recent_events:
                recent_events_item = recent_events_item_data.to_dict()
                recent_events.append(recent_events_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "state": state,
                "phase": phase,
                "build_queued": build_queued,
                "published_generation": published_generation,
                "edge_generation": edge_generation,
                "target_edge_generation": target_edge_generation,
                "progress": progress,
                "converged": converged,
                "iterations_completed": iterations_completed,
                "delta": delta,
                "computed_at_ms": computed_at_ms,
            }
        )
        if edge_filter is not UNSET:
            field_dict["edge_filter"] = edge_filter
        if metadata_version is not UNSET:
            field_dict["metadata_version"] = metadata_version
        if maintenance_paused is not UNSET:
            field_dict["maintenance_paused"] = maintenance_paused
        if queued_generation is not UNSET:
            field_dict["queued_generation"] = queued_generation
        if building_generation is not UNSET:
            field_dict["building_generation"] = building_generation
        if build_job_id is not UNSET:
            field_dict["build_job_id"] = build_job_id
        if build_started_at_ms is not UNSET:
            field_dict["build_started_at_ms"] = build_started_at_ms
        if build_iteration is not UNSET:
            field_dict["build_iteration"] = build_iteration
        if build_lease_expires_at_ms is not UNSET:
            field_dict["build_lease_expires_at_ms"] = build_lease_expires_at_ms
        if build_worker_id is not UNSET:
            field_dict["build_worker_id"] = build_worker_id
        if build_cursor is not UNSET:
            field_dict["build_cursor"] = build_cursor
        if build_completed_units is not UNSET:
            field_dict["build_completed_units"] = build_completed_units
        if build_total_units is not UNSET:
            field_dict["build_total_units"] = build_total_units
        if build_pages is not UNSET:
            field_dict["build_pages"] = build_pages
        if build_pages_truncated is not UNSET:
            field_dict["build_pages_truncated"] = build_pages_truncated
        if retry_count is not UNSET:
            field_dict["retry_count"] = retry_count
        if last_error is not UNSET:
            field_dict["last_error"] = last_error
        if last_event is not UNSET:
            field_dict["last_event"] = last_event
        if recent_events is not UNSET:
            field_dict["recent_events"] = recent_events

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_metric_build_page_status import GraphMetricBuildPageStatus
        from ..models.graph_metric_edge_filter_status import GraphMetricEdgeFilterStatus
        from ..models.graph_metric_event import GraphMetricEvent

        d = dict(src_dict)
        state = d.pop("state")

        phase = GraphMetricStatusPhase(d.pop("phase"))

        build_queued = d.pop("build_queued")

        published_generation = d.pop("published_generation")

        edge_generation = d.pop("edge_generation")

        target_edge_generation = d.pop("target_edge_generation")

        progress = d.pop("progress")

        converged = d.pop("converged")

        iterations_completed = d.pop("iterations_completed")

        delta = d.pop("delta")

        computed_at_ms = d.pop("computed_at_ms")

        _edge_filter = d.pop("edge_filter", UNSET)
        edge_filter: GraphMetricEdgeFilterStatus | Unset
        if isinstance(_edge_filter, Unset):
            edge_filter = UNSET
        else:
            edge_filter = GraphMetricEdgeFilterStatus.from_dict(_edge_filter)

        metadata_version = d.pop("metadata_version", UNSET)

        maintenance_paused = d.pop("maintenance_paused", UNSET)

        queued_generation = d.pop("queued_generation", UNSET)

        building_generation = d.pop("building_generation", UNSET)

        build_job_id = d.pop("build_job_id", UNSET)

        build_started_at_ms = d.pop("build_started_at_ms", UNSET)

        build_iteration = d.pop("build_iteration", UNSET)

        build_lease_expires_at_ms = d.pop("build_lease_expires_at_ms", UNSET)

        build_worker_id = d.pop("build_worker_id", UNSET)

        build_cursor = d.pop("build_cursor", UNSET)

        build_completed_units = d.pop("build_completed_units", UNSET)

        build_total_units = d.pop("build_total_units", UNSET)

        _build_pages = d.pop("build_pages", UNSET)
        build_pages: list[GraphMetricBuildPageStatus] | Unset = UNSET
        if _build_pages is not UNSET:
            build_pages = []
            for build_pages_item_data in _build_pages:
                build_pages_item = GraphMetricBuildPageStatus.from_dict(build_pages_item_data)

                build_pages.append(build_pages_item)

        build_pages_truncated = d.pop("build_pages_truncated", UNSET)

        retry_count = d.pop("retry_count", UNSET)

        last_error = d.pop("last_error", UNSET)

        _last_event = d.pop("last_event", UNSET)
        last_event: GraphMetricEvent | Unset
        if isinstance(_last_event, Unset):
            last_event = UNSET
        else:
            last_event = GraphMetricEvent.from_dict(_last_event)

        _recent_events = d.pop("recent_events", UNSET)
        recent_events: list[GraphMetricEvent] | Unset = UNSET
        if _recent_events is not UNSET:
            recent_events = []
            for recent_events_item_data in _recent_events:
                recent_events_item = GraphMetricEvent.from_dict(recent_events_item_data)

                recent_events.append(recent_events_item)

        graph_metric_status = cls(
            state=state,
            phase=phase,
            build_queued=build_queued,
            published_generation=published_generation,
            edge_generation=edge_generation,
            target_edge_generation=target_edge_generation,
            progress=progress,
            converged=converged,
            iterations_completed=iterations_completed,
            delta=delta,
            computed_at_ms=computed_at_ms,
            edge_filter=edge_filter,
            metadata_version=metadata_version,
            maintenance_paused=maintenance_paused,
            queued_generation=queued_generation,
            building_generation=building_generation,
            build_job_id=build_job_id,
            build_started_at_ms=build_started_at_ms,
            build_iteration=build_iteration,
            build_lease_expires_at_ms=build_lease_expires_at_ms,
            build_worker_id=build_worker_id,
            build_cursor=build_cursor,
            build_completed_units=build_completed_units,
            build_total_units=build_total_units,
            build_pages=build_pages,
            build_pages_truncated=build_pages_truncated,
            retry_count=retry_count,
            last_error=last_error,
            last_event=last_event,
            recent_events=recent_events,
        )

        graph_metric_status.additional_properties = d
        return graph_metric_status

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
