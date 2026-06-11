from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.graph_metric_runtime_stats_role import GraphMetricRuntimeStatsRole
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphMetricRuntimeStats")


@_attrs_define
class GraphMetricRuntimeStats:
    """Summarized graph metric maintenance runtime state. Identity fields are stable hashes, not raw process or owner
    identifiers.

        Attributes:
            enabled (bool | Unset):
            role (GraphMetricRuntimeStatsRole | Unset):
            runtime_id_hash (int | Unset):
            owner_id_hash (int | Unset):
            lease_key_hash (int | Unset):
            worker_id_hash (int | Unset):
            worker_count (int | Unset):
            lease_owned (bool | Unset):
            has_lease (bool | Unset):
            acquisition_count (int | Unset):
            takeover_count (int | Unset):
            lease_acquire_failures (int | Unset):
            lost_leases (int | Unset):
            last_acquired_ms (int | Unset):
            started (bool | Unset):
            shutdown (bool | Unset):
            notified (bool | Unset):
            ticks_started (int | Unset):
            ticks_completed (int | Unset):
            durable_progress_ticks (int | Unset):
            idle_ticks (int | Unset):
            error_ticks (int | Unset):
            last_error_name (str | Unset):
            total_metrics_scanned (int | Unset):
            total_active_builds (int | Unset):
            total_builds_started (int | Unset):
            total_worker_steps (int | Unset):
            total_coordinator_steps (int | Unset):
            total_pages_claimed (int | Unset):
            total_pages_completed (int | Unset):
            total_phases_advanced (int | Unset):
            total_published (int | Unset):
            total_failed_builds (int | Unset):
            last_metrics_scanned (int | Unset):
            last_active_builds (int | Unset):
            last_builds_started (int | Unset):
            last_worker_steps (int | Unset):
            last_coordinator_steps (int | Unset):
            last_pages_claimed (int | Unset):
            last_pages_completed (int | Unset):
            last_phases_advanced (int | Unset):
            last_published (int | Unset):
            last_failed_builds (int | Unset):
            last_budget_exhausted (bool | Unset):
    """

    enabled: bool | Unset = UNSET
    role: GraphMetricRuntimeStatsRole | Unset = UNSET
    runtime_id_hash: int | Unset = UNSET
    owner_id_hash: int | Unset = UNSET
    lease_key_hash: int | Unset = UNSET
    worker_id_hash: int | Unset = UNSET
    worker_count: int | Unset = UNSET
    lease_owned: bool | Unset = UNSET
    has_lease: bool | Unset = UNSET
    acquisition_count: int | Unset = UNSET
    takeover_count: int | Unset = UNSET
    lease_acquire_failures: int | Unset = UNSET
    lost_leases: int | Unset = UNSET
    last_acquired_ms: int | Unset = UNSET
    started: bool | Unset = UNSET
    shutdown: bool | Unset = UNSET
    notified: bool | Unset = UNSET
    ticks_started: int | Unset = UNSET
    ticks_completed: int | Unset = UNSET
    durable_progress_ticks: int | Unset = UNSET
    idle_ticks: int | Unset = UNSET
    error_ticks: int | Unset = UNSET
    last_error_name: str | Unset = UNSET
    total_metrics_scanned: int | Unset = UNSET
    total_active_builds: int | Unset = UNSET
    total_builds_started: int | Unset = UNSET
    total_worker_steps: int | Unset = UNSET
    total_coordinator_steps: int | Unset = UNSET
    total_pages_claimed: int | Unset = UNSET
    total_pages_completed: int | Unset = UNSET
    total_phases_advanced: int | Unset = UNSET
    total_published: int | Unset = UNSET
    total_failed_builds: int | Unset = UNSET
    last_metrics_scanned: int | Unset = UNSET
    last_active_builds: int | Unset = UNSET
    last_builds_started: int | Unset = UNSET
    last_worker_steps: int | Unset = UNSET
    last_coordinator_steps: int | Unset = UNSET
    last_pages_claimed: int | Unset = UNSET
    last_pages_completed: int | Unset = UNSET
    last_phases_advanced: int | Unset = UNSET
    last_published: int | Unset = UNSET
    last_failed_builds: int | Unset = UNSET
    last_budget_exhausted: bool | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        enabled = self.enabled

        role: str | Unset = UNSET
        if not isinstance(self.role, Unset):
            role = self.role.value

        runtime_id_hash = self.runtime_id_hash

        owner_id_hash = self.owner_id_hash

        lease_key_hash = self.lease_key_hash

        worker_id_hash = self.worker_id_hash

        worker_count = self.worker_count

        lease_owned = self.lease_owned

        has_lease = self.has_lease

        acquisition_count = self.acquisition_count

        takeover_count = self.takeover_count

        lease_acquire_failures = self.lease_acquire_failures

        lost_leases = self.lost_leases

        last_acquired_ms = self.last_acquired_ms

        started = self.started

        shutdown = self.shutdown

        notified = self.notified

        ticks_started = self.ticks_started

        ticks_completed = self.ticks_completed

        durable_progress_ticks = self.durable_progress_ticks

        idle_ticks = self.idle_ticks

        error_ticks = self.error_ticks

        last_error_name = self.last_error_name

        total_metrics_scanned = self.total_metrics_scanned

        total_active_builds = self.total_active_builds

        total_builds_started = self.total_builds_started

        total_worker_steps = self.total_worker_steps

        total_coordinator_steps = self.total_coordinator_steps

        total_pages_claimed = self.total_pages_claimed

        total_pages_completed = self.total_pages_completed

        total_phases_advanced = self.total_phases_advanced

        total_published = self.total_published

        total_failed_builds = self.total_failed_builds

        last_metrics_scanned = self.last_metrics_scanned

        last_active_builds = self.last_active_builds

        last_builds_started = self.last_builds_started

        last_worker_steps = self.last_worker_steps

        last_coordinator_steps = self.last_coordinator_steps

        last_pages_claimed = self.last_pages_claimed

        last_pages_completed = self.last_pages_completed

        last_phases_advanced = self.last_phases_advanced

        last_published = self.last_published

        last_failed_builds = self.last_failed_builds

        last_budget_exhausted = self.last_budget_exhausted

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if enabled is not UNSET:
            field_dict["enabled"] = enabled
        if role is not UNSET:
            field_dict["role"] = role
        if runtime_id_hash is not UNSET:
            field_dict["runtime_id_hash"] = runtime_id_hash
        if owner_id_hash is not UNSET:
            field_dict["owner_id_hash"] = owner_id_hash
        if lease_key_hash is not UNSET:
            field_dict["lease_key_hash"] = lease_key_hash
        if worker_id_hash is not UNSET:
            field_dict["worker_id_hash"] = worker_id_hash
        if worker_count is not UNSET:
            field_dict["worker_count"] = worker_count
        if lease_owned is not UNSET:
            field_dict["lease_owned"] = lease_owned
        if has_lease is not UNSET:
            field_dict["has_lease"] = has_lease
        if acquisition_count is not UNSET:
            field_dict["acquisition_count"] = acquisition_count
        if takeover_count is not UNSET:
            field_dict["takeover_count"] = takeover_count
        if lease_acquire_failures is not UNSET:
            field_dict["lease_acquire_failures"] = lease_acquire_failures
        if lost_leases is not UNSET:
            field_dict["lost_leases"] = lost_leases
        if last_acquired_ms is not UNSET:
            field_dict["last_acquired_ms"] = last_acquired_ms
        if started is not UNSET:
            field_dict["started"] = started
        if shutdown is not UNSET:
            field_dict["shutdown"] = shutdown
        if notified is not UNSET:
            field_dict["notified"] = notified
        if ticks_started is not UNSET:
            field_dict["ticks_started"] = ticks_started
        if ticks_completed is not UNSET:
            field_dict["ticks_completed"] = ticks_completed
        if durable_progress_ticks is not UNSET:
            field_dict["durable_progress_ticks"] = durable_progress_ticks
        if idle_ticks is not UNSET:
            field_dict["idle_ticks"] = idle_ticks
        if error_ticks is not UNSET:
            field_dict["error_ticks"] = error_ticks
        if last_error_name is not UNSET:
            field_dict["last_error_name"] = last_error_name
        if total_metrics_scanned is not UNSET:
            field_dict["total_metrics_scanned"] = total_metrics_scanned
        if total_active_builds is not UNSET:
            field_dict["total_active_builds"] = total_active_builds
        if total_builds_started is not UNSET:
            field_dict["total_builds_started"] = total_builds_started
        if total_worker_steps is not UNSET:
            field_dict["total_worker_steps"] = total_worker_steps
        if total_coordinator_steps is not UNSET:
            field_dict["total_coordinator_steps"] = total_coordinator_steps
        if total_pages_claimed is not UNSET:
            field_dict["total_pages_claimed"] = total_pages_claimed
        if total_pages_completed is not UNSET:
            field_dict["total_pages_completed"] = total_pages_completed
        if total_phases_advanced is not UNSET:
            field_dict["total_phases_advanced"] = total_phases_advanced
        if total_published is not UNSET:
            field_dict["total_published"] = total_published
        if total_failed_builds is not UNSET:
            field_dict["total_failed_builds"] = total_failed_builds
        if last_metrics_scanned is not UNSET:
            field_dict["last_metrics_scanned"] = last_metrics_scanned
        if last_active_builds is not UNSET:
            field_dict["last_active_builds"] = last_active_builds
        if last_builds_started is not UNSET:
            field_dict["last_builds_started"] = last_builds_started
        if last_worker_steps is not UNSET:
            field_dict["last_worker_steps"] = last_worker_steps
        if last_coordinator_steps is not UNSET:
            field_dict["last_coordinator_steps"] = last_coordinator_steps
        if last_pages_claimed is not UNSET:
            field_dict["last_pages_claimed"] = last_pages_claimed
        if last_pages_completed is not UNSET:
            field_dict["last_pages_completed"] = last_pages_completed
        if last_phases_advanced is not UNSET:
            field_dict["last_phases_advanced"] = last_phases_advanced
        if last_published is not UNSET:
            field_dict["last_published"] = last_published
        if last_failed_builds is not UNSET:
            field_dict["last_failed_builds"] = last_failed_builds
        if last_budget_exhausted is not UNSET:
            field_dict["last_budget_exhausted"] = last_budget_exhausted

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        enabled = d.pop("enabled", UNSET)

        _role = d.pop("role", UNSET)
        role: GraphMetricRuntimeStatsRole | Unset
        if isinstance(_role, Unset):
            role = UNSET
        else:
            role = GraphMetricRuntimeStatsRole(_role)

        runtime_id_hash = d.pop("runtime_id_hash", UNSET)

        owner_id_hash = d.pop("owner_id_hash", UNSET)

        lease_key_hash = d.pop("lease_key_hash", UNSET)

        worker_id_hash = d.pop("worker_id_hash", UNSET)

        worker_count = d.pop("worker_count", UNSET)

        lease_owned = d.pop("lease_owned", UNSET)

        has_lease = d.pop("has_lease", UNSET)

        acquisition_count = d.pop("acquisition_count", UNSET)

        takeover_count = d.pop("takeover_count", UNSET)

        lease_acquire_failures = d.pop("lease_acquire_failures", UNSET)

        lost_leases = d.pop("lost_leases", UNSET)

        last_acquired_ms = d.pop("last_acquired_ms", UNSET)

        started = d.pop("started", UNSET)

        shutdown = d.pop("shutdown", UNSET)

        notified = d.pop("notified", UNSET)

        ticks_started = d.pop("ticks_started", UNSET)

        ticks_completed = d.pop("ticks_completed", UNSET)

        durable_progress_ticks = d.pop("durable_progress_ticks", UNSET)

        idle_ticks = d.pop("idle_ticks", UNSET)

        error_ticks = d.pop("error_ticks", UNSET)

        last_error_name = d.pop("last_error_name", UNSET)

        total_metrics_scanned = d.pop("total_metrics_scanned", UNSET)

        total_active_builds = d.pop("total_active_builds", UNSET)

        total_builds_started = d.pop("total_builds_started", UNSET)

        total_worker_steps = d.pop("total_worker_steps", UNSET)

        total_coordinator_steps = d.pop("total_coordinator_steps", UNSET)

        total_pages_claimed = d.pop("total_pages_claimed", UNSET)

        total_pages_completed = d.pop("total_pages_completed", UNSET)

        total_phases_advanced = d.pop("total_phases_advanced", UNSET)

        total_published = d.pop("total_published", UNSET)

        total_failed_builds = d.pop("total_failed_builds", UNSET)

        last_metrics_scanned = d.pop("last_metrics_scanned", UNSET)

        last_active_builds = d.pop("last_active_builds", UNSET)

        last_builds_started = d.pop("last_builds_started", UNSET)

        last_worker_steps = d.pop("last_worker_steps", UNSET)

        last_coordinator_steps = d.pop("last_coordinator_steps", UNSET)

        last_pages_claimed = d.pop("last_pages_claimed", UNSET)

        last_pages_completed = d.pop("last_pages_completed", UNSET)

        last_phases_advanced = d.pop("last_phases_advanced", UNSET)

        last_published = d.pop("last_published", UNSET)

        last_failed_builds = d.pop("last_failed_builds", UNSET)

        last_budget_exhausted = d.pop("last_budget_exhausted", UNSET)

        graph_metric_runtime_stats = cls(
            enabled=enabled,
            role=role,
            runtime_id_hash=runtime_id_hash,
            owner_id_hash=owner_id_hash,
            lease_key_hash=lease_key_hash,
            worker_id_hash=worker_id_hash,
            worker_count=worker_count,
            lease_owned=lease_owned,
            has_lease=has_lease,
            acquisition_count=acquisition_count,
            takeover_count=takeover_count,
            lease_acquire_failures=lease_acquire_failures,
            lost_leases=lost_leases,
            last_acquired_ms=last_acquired_ms,
            started=started,
            shutdown=shutdown,
            notified=notified,
            ticks_started=ticks_started,
            ticks_completed=ticks_completed,
            durable_progress_ticks=durable_progress_ticks,
            idle_ticks=idle_ticks,
            error_ticks=error_ticks,
            last_error_name=last_error_name,
            total_metrics_scanned=total_metrics_scanned,
            total_active_builds=total_active_builds,
            total_builds_started=total_builds_started,
            total_worker_steps=total_worker_steps,
            total_coordinator_steps=total_coordinator_steps,
            total_pages_claimed=total_pages_claimed,
            total_pages_completed=total_pages_completed,
            total_phases_advanced=total_phases_advanced,
            total_published=total_published,
            total_failed_builds=total_failed_builds,
            last_metrics_scanned=last_metrics_scanned,
            last_active_builds=last_active_builds,
            last_builds_started=last_builds_started,
            last_worker_steps=last_worker_steps,
            last_coordinator_steps=last_coordinator_steps,
            last_pages_claimed=last_pages_claimed,
            last_pages_completed=last_pages_completed,
            last_phases_advanced=last_phases_advanced,
            last_published=last_published,
            last_failed_builds=last_failed_builds,
            last_budget_exhausted=last_budget_exhausted,
        )

        graph_metric_runtime_stats.additional_properties = d
        return graph_metric_runtime_stats

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
