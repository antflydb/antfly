from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="LsmStorageStatus")


@_attrs_define
class LsmStorageStatus:
    """Compact LSM backend operational status. Detailed low-level counters are available through metrics.

    Attributes:
        run_count (int | Unset):
        run_bytes (int | Unset):
        l0_run_count (int | Unset):
        l0_bytes (int | Unset):
        lower_level_run_count (int | Unset):
        lower_level_bytes (int | Unset):
        max_level (int | Unset):
        compactable_l0_run_count (int | Unset):
        overlapping_l0_run_count (int | Unset):
        soft_limit_l0_run_count (int | Unset):
        hard_limit_l0_run_count (int | Unset):
        write_stall_l0_run_debt (int | Unset):
        soft_limit_l0_bytes (int | Unset):
        hard_limit_l0_bytes (int | Unset):
        write_stall_l0_byte_debt (int | Unset):
        level_overflow_run_count (int | Unset):
        level_overflow_bytes (int | Unset):
        obsolete_path_count (int | Unset):
        obsolete_paths_pinned_by_readers (int | Unset):
        obsolete_paths_pinned_by_versions (int | Unset):
        obsolete_paths_waiting_for_retry (int | Unset):
        obsolete_paths_reclaimable (int | Unset):
        obsolete_delete_failures (int | Unset):
        obsolete_delete_retries (int | Unset):
        current_manifest_bytes (int | Unset):
        mutable_entry_count (int | Unset):
        mutable_bytes (int | Unset):
        immutable_memtable_count (int | Unset):
        immutable_entry_count (int | Unset):
        immutable_bytes (int | Unset):
        mutable_snapshot_clone_count (int | Unset):
        mutable_snapshot_clone_bytes (int | Unset):
        mutable_snapshot_clone_peak_bytes (int | Unset):
        read_snapshot_mutable_rotation_count (int | Unset):
        read_snapshot_mutable_rotation_bytes (int | Unset):
        wal_retained_bytes (int | Unset):
        wal_checkpoint_pending (bool | Unset): Whether WAL checkpoint maintenance is pending.
        wal_pressure_blocked (bool | Unset): Whether WAL hard-limit admission is currently blocked.
        wal_checkpoint_retry_reason (str | Unset): Representative reason for the earliest pending WAL checkpoint retry.
        wal_checkpoint_retry_attempts (int | Unset): Consecutive failures for the representative WAL checkpoint retry.
        wal_checkpoint_retry_delay_ns (int | Unset): Nanoseconds until the earliest WAL checkpoint retry; zero means due
            now.
        active_immutable_logical_bytes (int | Unset): Logical bytes in immutable memtables awaiting run publication.
        unpublished_wal_logical_bytes (int | Unset): Logical bytes in runs awaiting durable manifest publication.
        unpublished_wal_max_batch_logical_bytes (int | Unset): Largest logical batch awaiting durable manifest
            publication.
        compaction_backlog_bytes (int | Unset):
        active_readers (int | Unset):
        active_readers_bound_read_txn (int | Unset):
        active_readers_namespace_read_txn (int | Unset):
        active_readers_probe_txn (int | Unset):
        active_readers_current_scan (int | Unset):
        active_readers_write_txn (int | Unset):
        active_readers_compaction (int | Unset):
        active_readers_other (int | Unset):
        obsolete_paths_pinned_by_reader_bound_read_txn (int | Unset):
        obsolete_paths_pinned_by_reader_namespace_read_txn (int | Unset):
        obsolete_paths_pinned_by_reader_probe_txn (int | Unset):
        obsolete_paths_pinned_by_reader_current_scan (int | Unset):
        obsolete_paths_pinned_by_reader_write_txn (int | Unset):
        obsolete_paths_pinned_by_reader_compaction (int | Unset):
        obsolete_paths_pinned_by_reader_other (int | Unset):
        active_bulk_ingest_batches (int | Unset):
        manifest_dirty (bool | Unset):
        obsolete_manifest_dirty (bool | Unset):
        maintenance_score (int | Unset):
        maintenance_debt_hint (int | Unset):
        flush_count (int | Unset):
        flush_output_run_count (int | Unset):
        flush_output_bytes (int | Unset):
        sorted_ingest_run_count (int | Unset):
        sorted_ingest_bytes (int | Unset):
        manifest_write_count (int | Unset):
        manifest_bytes (int | Unset):
        write_pressure_event_count (int | Unset):
        write_pressure_compaction_count (int | Unset):
        write_pressure_compaction_step_count (int | Unset):
        write_pressure_overload_count (int | Unset):
        write_pressure_overload_l0_run_debt (int | Unset):
        immutable_rotation_count (int | Unset):
        immutable_flush_count (int | Unset):
        direct_bulk_ingest_attempt_count (int | Unset):
        direct_bulk_ingest_success_count (int | Unset):
        direct_bulk_ingest_entry_count (int | Unset):
        bulk_append_attempt_count (int | Unset):
        bulk_append_entry_count (int | Unset):
        bulk_append_direct_success_count (int | Unset):
        bulk_append_direct_entry_count (int | Unset):
        bulk_append_fallback_backend_pending_count (int | Unset):
        bulk_append_fallback_below_threshold_count (int | Unset):
        bulk_append_fallback_duplicate_key_count (int | Unset):
        bulk_append_fallback_to_mutable_entry_count (int | Unset):
        direct_bulk_ingest_direct_entry_count (int | Unset):
        direct_bulk_ingest_fallback_unsupported_count (int | Unset):
        direct_bulk_ingest_fallback_backend_mutable_count (int | Unset):
        direct_bulk_ingest_fallback_below_threshold_count (int | Unset):
    """

    run_count: int | Unset = UNSET
    run_bytes: int | Unset = UNSET
    l0_run_count: int | Unset = UNSET
    l0_bytes: int | Unset = UNSET
    lower_level_run_count: int | Unset = UNSET
    lower_level_bytes: int | Unset = UNSET
    max_level: int | Unset = UNSET
    compactable_l0_run_count: int | Unset = UNSET
    overlapping_l0_run_count: int | Unset = UNSET
    soft_limit_l0_run_count: int | Unset = UNSET
    hard_limit_l0_run_count: int | Unset = UNSET
    write_stall_l0_run_debt: int | Unset = UNSET
    soft_limit_l0_bytes: int | Unset = UNSET
    hard_limit_l0_bytes: int | Unset = UNSET
    write_stall_l0_byte_debt: int | Unset = UNSET
    level_overflow_run_count: int | Unset = UNSET
    level_overflow_bytes: int | Unset = UNSET
    obsolete_path_count: int | Unset = UNSET
    obsolete_paths_pinned_by_readers: int | Unset = UNSET
    obsolete_paths_pinned_by_versions: int | Unset = UNSET
    obsolete_paths_waiting_for_retry: int | Unset = UNSET
    obsolete_paths_reclaimable: int | Unset = UNSET
    obsolete_delete_failures: int | Unset = UNSET
    obsolete_delete_retries: int | Unset = UNSET
    current_manifest_bytes: int | Unset = UNSET
    mutable_entry_count: int | Unset = UNSET
    mutable_bytes: int | Unset = UNSET
    immutable_memtable_count: int | Unset = UNSET
    immutable_entry_count: int | Unset = UNSET
    immutable_bytes: int | Unset = UNSET
    mutable_snapshot_clone_count: int | Unset = UNSET
    mutable_snapshot_clone_bytes: int | Unset = UNSET
    mutable_snapshot_clone_peak_bytes: int | Unset = UNSET
    read_snapshot_mutable_rotation_count: int | Unset = UNSET
    read_snapshot_mutable_rotation_bytes: int | Unset = UNSET
    wal_retained_bytes: int | Unset = UNSET
    wal_checkpoint_pending: bool | Unset = UNSET
    wal_pressure_blocked: bool | Unset = UNSET
    wal_checkpoint_retry_reason: str | Unset = UNSET
    wal_checkpoint_retry_attempts: int | Unset = UNSET
    wal_checkpoint_retry_delay_ns: int | Unset = UNSET
    active_immutable_logical_bytes: int | Unset = UNSET
    unpublished_wal_logical_bytes: int | Unset = UNSET
    unpublished_wal_max_batch_logical_bytes: int | Unset = UNSET
    compaction_backlog_bytes: int | Unset = UNSET
    active_readers: int | Unset = UNSET
    active_readers_bound_read_txn: int | Unset = UNSET
    active_readers_namespace_read_txn: int | Unset = UNSET
    active_readers_probe_txn: int | Unset = UNSET
    active_readers_current_scan: int | Unset = UNSET
    active_readers_write_txn: int | Unset = UNSET
    active_readers_compaction: int | Unset = UNSET
    active_readers_other: int | Unset = UNSET
    obsolete_paths_pinned_by_reader_bound_read_txn: int | Unset = UNSET
    obsolete_paths_pinned_by_reader_namespace_read_txn: int | Unset = UNSET
    obsolete_paths_pinned_by_reader_probe_txn: int | Unset = UNSET
    obsolete_paths_pinned_by_reader_current_scan: int | Unset = UNSET
    obsolete_paths_pinned_by_reader_write_txn: int | Unset = UNSET
    obsolete_paths_pinned_by_reader_compaction: int | Unset = UNSET
    obsolete_paths_pinned_by_reader_other: int | Unset = UNSET
    active_bulk_ingest_batches: int | Unset = UNSET
    manifest_dirty: bool | Unset = UNSET
    obsolete_manifest_dirty: bool | Unset = UNSET
    maintenance_score: int | Unset = UNSET
    maintenance_debt_hint: int | Unset = UNSET
    flush_count: int | Unset = UNSET
    flush_output_run_count: int | Unset = UNSET
    flush_output_bytes: int | Unset = UNSET
    sorted_ingest_run_count: int | Unset = UNSET
    sorted_ingest_bytes: int | Unset = UNSET
    manifest_write_count: int | Unset = UNSET
    manifest_bytes: int | Unset = UNSET
    write_pressure_event_count: int | Unset = UNSET
    write_pressure_compaction_count: int | Unset = UNSET
    write_pressure_compaction_step_count: int | Unset = UNSET
    write_pressure_overload_count: int | Unset = UNSET
    write_pressure_overload_l0_run_debt: int | Unset = UNSET
    immutable_rotation_count: int | Unset = UNSET
    immutable_flush_count: int | Unset = UNSET
    direct_bulk_ingest_attempt_count: int | Unset = UNSET
    direct_bulk_ingest_success_count: int | Unset = UNSET
    direct_bulk_ingest_entry_count: int | Unset = UNSET
    bulk_append_attempt_count: int | Unset = UNSET
    bulk_append_entry_count: int | Unset = UNSET
    bulk_append_direct_success_count: int | Unset = UNSET
    bulk_append_direct_entry_count: int | Unset = UNSET
    bulk_append_fallback_backend_pending_count: int | Unset = UNSET
    bulk_append_fallback_below_threshold_count: int | Unset = UNSET
    bulk_append_fallback_duplicate_key_count: int | Unset = UNSET
    bulk_append_fallback_to_mutable_entry_count: int | Unset = UNSET
    direct_bulk_ingest_direct_entry_count: int | Unset = UNSET
    direct_bulk_ingest_fallback_unsupported_count: int | Unset = UNSET
    direct_bulk_ingest_fallback_backend_mutable_count: int | Unset = UNSET
    direct_bulk_ingest_fallback_below_threshold_count: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        run_count = self.run_count

        run_bytes = self.run_bytes

        l0_run_count = self.l0_run_count

        l0_bytes = self.l0_bytes

        lower_level_run_count = self.lower_level_run_count

        lower_level_bytes = self.lower_level_bytes

        max_level = self.max_level

        compactable_l0_run_count = self.compactable_l0_run_count

        overlapping_l0_run_count = self.overlapping_l0_run_count

        soft_limit_l0_run_count = self.soft_limit_l0_run_count

        hard_limit_l0_run_count = self.hard_limit_l0_run_count

        write_stall_l0_run_debt = self.write_stall_l0_run_debt

        soft_limit_l0_bytes = self.soft_limit_l0_bytes

        hard_limit_l0_bytes = self.hard_limit_l0_bytes

        write_stall_l0_byte_debt = self.write_stall_l0_byte_debt

        level_overflow_run_count = self.level_overflow_run_count

        level_overflow_bytes = self.level_overflow_bytes

        obsolete_path_count = self.obsolete_path_count

        obsolete_paths_pinned_by_readers = self.obsolete_paths_pinned_by_readers

        obsolete_paths_pinned_by_versions = self.obsolete_paths_pinned_by_versions

        obsolete_paths_waiting_for_retry = self.obsolete_paths_waiting_for_retry

        obsolete_paths_reclaimable = self.obsolete_paths_reclaimable

        obsolete_delete_failures = self.obsolete_delete_failures

        obsolete_delete_retries = self.obsolete_delete_retries

        current_manifest_bytes = self.current_manifest_bytes

        mutable_entry_count = self.mutable_entry_count

        mutable_bytes = self.mutable_bytes

        immutable_memtable_count = self.immutable_memtable_count

        immutable_entry_count = self.immutable_entry_count

        immutable_bytes = self.immutable_bytes

        mutable_snapshot_clone_count = self.mutable_snapshot_clone_count

        mutable_snapshot_clone_bytes = self.mutable_snapshot_clone_bytes

        mutable_snapshot_clone_peak_bytes = self.mutable_snapshot_clone_peak_bytes

        read_snapshot_mutable_rotation_count = self.read_snapshot_mutable_rotation_count

        read_snapshot_mutable_rotation_bytes = self.read_snapshot_mutable_rotation_bytes

        wal_retained_bytes = self.wal_retained_bytes

        wal_checkpoint_pending = self.wal_checkpoint_pending

        wal_pressure_blocked = self.wal_pressure_blocked

        wal_checkpoint_retry_reason = self.wal_checkpoint_retry_reason

        wal_checkpoint_retry_attempts = self.wal_checkpoint_retry_attempts

        wal_checkpoint_retry_delay_ns = self.wal_checkpoint_retry_delay_ns

        active_immutable_logical_bytes = self.active_immutable_logical_bytes

        unpublished_wal_logical_bytes = self.unpublished_wal_logical_bytes

        unpublished_wal_max_batch_logical_bytes = self.unpublished_wal_max_batch_logical_bytes

        compaction_backlog_bytes = self.compaction_backlog_bytes

        active_readers = self.active_readers

        active_readers_bound_read_txn = self.active_readers_bound_read_txn

        active_readers_namespace_read_txn = self.active_readers_namespace_read_txn

        active_readers_probe_txn = self.active_readers_probe_txn

        active_readers_current_scan = self.active_readers_current_scan

        active_readers_write_txn = self.active_readers_write_txn

        active_readers_compaction = self.active_readers_compaction

        active_readers_other = self.active_readers_other

        obsolete_paths_pinned_by_reader_bound_read_txn = self.obsolete_paths_pinned_by_reader_bound_read_txn

        obsolete_paths_pinned_by_reader_namespace_read_txn = self.obsolete_paths_pinned_by_reader_namespace_read_txn

        obsolete_paths_pinned_by_reader_probe_txn = self.obsolete_paths_pinned_by_reader_probe_txn

        obsolete_paths_pinned_by_reader_current_scan = self.obsolete_paths_pinned_by_reader_current_scan

        obsolete_paths_pinned_by_reader_write_txn = self.obsolete_paths_pinned_by_reader_write_txn

        obsolete_paths_pinned_by_reader_compaction = self.obsolete_paths_pinned_by_reader_compaction

        obsolete_paths_pinned_by_reader_other = self.obsolete_paths_pinned_by_reader_other

        active_bulk_ingest_batches = self.active_bulk_ingest_batches

        manifest_dirty = self.manifest_dirty

        obsolete_manifest_dirty = self.obsolete_manifest_dirty

        maintenance_score = self.maintenance_score

        maintenance_debt_hint = self.maintenance_debt_hint

        flush_count = self.flush_count

        flush_output_run_count = self.flush_output_run_count

        flush_output_bytes = self.flush_output_bytes

        sorted_ingest_run_count = self.sorted_ingest_run_count

        sorted_ingest_bytes = self.sorted_ingest_bytes

        manifest_write_count = self.manifest_write_count

        manifest_bytes = self.manifest_bytes

        write_pressure_event_count = self.write_pressure_event_count

        write_pressure_compaction_count = self.write_pressure_compaction_count

        write_pressure_compaction_step_count = self.write_pressure_compaction_step_count

        write_pressure_overload_count = self.write_pressure_overload_count

        write_pressure_overload_l0_run_debt = self.write_pressure_overload_l0_run_debt

        immutable_rotation_count = self.immutable_rotation_count

        immutable_flush_count = self.immutable_flush_count

        direct_bulk_ingest_attempt_count = self.direct_bulk_ingest_attempt_count

        direct_bulk_ingest_success_count = self.direct_bulk_ingest_success_count

        direct_bulk_ingest_entry_count = self.direct_bulk_ingest_entry_count

        bulk_append_attempt_count = self.bulk_append_attempt_count

        bulk_append_entry_count = self.bulk_append_entry_count

        bulk_append_direct_success_count = self.bulk_append_direct_success_count

        bulk_append_direct_entry_count = self.bulk_append_direct_entry_count

        bulk_append_fallback_backend_pending_count = self.bulk_append_fallback_backend_pending_count

        bulk_append_fallback_below_threshold_count = self.bulk_append_fallback_below_threshold_count

        bulk_append_fallback_duplicate_key_count = self.bulk_append_fallback_duplicate_key_count

        bulk_append_fallback_to_mutable_entry_count = self.bulk_append_fallback_to_mutable_entry_count

        direct_bulk_ingest_direct_entry_count = self.direct_bulk_ingest_direct_entry_count

        direct_bulk_ingest_fallback_unsupported_count = self.direct_bulk_ingest_fallback_unsupported_count

        direct_bulk_ingest_fallback_backend_mutable_count = self.direct_bulk_ingest_fallback_backend_mutable_count

        direct_bulk_ingest_fallback_below_threshold_count = self.direct_bulk_ingest_fallback_below_threshold_count

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if run_count is not UNSET:
            field_dict["run_count"] = run_count
        if run_bytes is not UNSET:
            field_dict["run_bytes"] = run_bytes
        if l0_run_count is not UNSET:
            field_dict["l0_run_count"] = l0_run_count
        if l0_bytes is not UNSET:
            field_dict["l0_bytes"] = l0_bytes
        if lower_level_run_count is not UNSET:
            field_dict["lower_level_run_count"] = lower_level_run_count
        if lower_level_bytes is not UNSET:
            field_dict["lower_level_bytes"] = lower_level_bytes
        if max_level is not UNSET:
            field_dict["max_level"] = max_level
        if compactable_l0_run_count is not UNSET:
            field_dict["compactable_l0_run_count"] = compactable_l0_run_count
        if overlapping_l0_run_count is not UNSET:
            field_dict["overlapping_l0_run_count"] = overlapping_l0_run_count
        if soft_limit_l0_run_count is not UNSET:
            field_dict["soft_limit_l0_run_count"] = soft_limit_l0_run_count
        if hard_limit_l0_run_count is not UNSET:
            field_dict["hard_limit_l0_run_count"] = hard_limit_l0_run_count
        if write_stall_l0_run_debt is not UNSET:
            field_dict["write_stall_l0_run_debt"] = write_stall_l0_run_debt
        if soft_limit_l0_bytes is not UNSET:
            field_dict["soft_limit_l0_bytes"] = soft_limit_l0_bytes
        if hard_limit_l0_bytes is not UNSET:
            field_dict["hard_limit_l0_bytes"] = hard_limit_l0_bytes
        if write_stall_l0_byte_debt is not UNSET:
            field_dict["write_stall_l0_byte_debt"] = write_stall_l0_byte_debt
        if level_overflow_run_count is not UNSET:
            field_dict["level_overflow_run_count"] = level_overflow_run_count
        if level_overflow_bytes is not UNSET:
            field_dict["level_overflow_bytes"] = level_overflow_bytes
        if obsolete_path_count is not UNSET:
            field_dict["obsolete_path_count"] = obsolete_path_count
        if obsolete_paths_pinned_by_readers is not UNSET:
            field_dict["obsolete_paths_pinned_by_readers"] = obsolete_paths_pinned_by_readers
        if obsolete_paths_pinned_by_versions is not UNSET:
            field_dict["obsolete_paths_pinned_by_versions"] = obsolete_paths_pinned_by_versions
        if obsolete_paths_waiting_for_retry is not UNSET:
            field_dict["obsolete_paths_waiting_for_retry"] = obsolete_paths_waiting_for_retry
        if obsolete_paths_reclaimable is not UNSET:
            field_dict["obsolete_paths_reclaimable"] = obsolete_paths_reclaimable
        if obsolete_delete_failures is not UNSET:
            field_dict["obsolete_delete_failures"] = obsolete_delete_failures
        if obsolete_delete_retries is not UNSET:
            field_dict["obsolete_delete_retries"] = obsolete_delete_retries
        if current_manifest_bytes is not UNSET:
            field_dict["current_manifest_bytes"] = current_manifest_bytes
        if mutable_entry_count is not UNSET:
            field_dict["mutable_entry_count"] = mutable_entry_count
        if mutable_bytes is not UNSET:
            field_dict["mutable_bytes"] = mutable_bytes
        if immutable_memtable_count is not UNSET:
            field_dict["immutable_memtable_count"] = immutable_memtable_count
        if immutable_entry_count is not UNSET:
            field_dict["immutable_entry_count"] = immutable_entry_count
        if immutable_bytes is not UNSET:
            field_dict["immutable_bytes"] = immutable_bytes
        if mutable_snapshot_clone_count is not UNSET:
            field_dict["mutable_snapshot_clone_count"] = mutable_snapshot_clone_count
        if mutable_snapshot_clone_bytes is not UNSET:
            field_dict["mutable_snapshot_clone_bytes"] = mutable_snapshot_clone_bytes
        if mutable_snapshot_clone_peak_bytes is not UNSET:
            field_dict["mutable_snapshot_clone_peak_bytes"] = mutable_snapshot_clone_peak_bytes
        if read_snapshot_mutable_rotation_count is not UNSET:
            field_dict["read_snapshot_mutable_rotation_count"] = read_snapshot_mutable_rotation_count
        if read_snapshot_mutable_rotation_bytes is not UNSET:
            field_dict["read_snapshot_mutable_rotation_bytes"] = read_snapshot_mutable_rotation_bytes
        if wal_retained_bytes is not UNSET:
            field_dict["wal_retained_bytes"] = wal_retained_bytes
        if wal_checkpoint_pending is not UNSET:
            field_dict["wal_checkpoint_pending"] = wal_checkpoint_pending
        if wal_pressure_blocked is not UNSET:
            field_dict["wal_pressure_blocked"] = wal_pressure_blocked
        if wal_checkpoint_retry_reason is not UNSET:
            field_dict["wal_checkpoint_retry_reason"] = wal_checkpoint_retry_reason
        if wal_checkpoint_retry_attempts is not UNSET:
            field_dict["wal_checkpoint_retry_attempts"] = wal_checkpoint_retry_attempts
        if wal_checkpoint_retry_delay_ns is not UNSET:
            field_dict["wal_checkpoint_retry_delay_ns"] = wal_checkpoint_retry_delay_ns
        if active_immutable_logical_bytes is not UNSET:
            field_dict["active_immutable_logical_bytes"] = active_immutable_logical_bytes
        if unpublished_wal_logical_bytes is not UNSET:
            field_dict["unpublished_wal_logical_bytes"] = unpublished_wal_logical_bytes
        if unpublished_wal_max_batch_logical_bytes is not UNSET:
            field_dict["unpublished_wal_max_batch_logical_bytes"] = unpublished_wal_max_batch_logical_bytes
        if compaction_backlog_bytes is not UNSET:
            field_dict["compaction_backlog_bytes"] = compaction_backlog_bytes
        if active_readers is not UNSET:
            field_dict["active_readers"] = active_readers
        if active_readers_bound_read_txn is not UNSET:
            field_dict["active_readers_bound_read_txn"] = active_readers_bound_read_txn
        if active_readers_namespace_read_txn is not UNSET:
            field_dict["active_readers_namespace_read_txn"] = active_readers_namespace_read_txn
        if active_readers_probe_txn is not UNSET:
            field_dict["active_readers_probe_txn"] = active_readers_probe_txn
        if active_readers_current_scan is not UNSET:
            field_dict["active_readers_current_scan"] = active_readers_current_scan
        if active_readers_write_txn is not UNSET:
            field_dict["active_readers_write_txn"] = active_readers_write_txn
        if active_readers_compaction is not UNSET:
            field_dict["active_readers_compaction"] = active_readers_compaction
        if active_readers_other is not UNSET:
            field_dict["active_readers_other"] = active_readers_other
        if obsolete_paths_pinned_by_reader_bound_read_txn is not UNSET:
            field_dict["obsolete_paths_pinned_by_reader_bound_read_txn"] = (
                obsolete_paths_pinned_by_reader_bound_read_txn
            )
        if obsolete_paths_pinned_by_reader_namespace_read_txn is not UNSET:
            field_dict["obsolete_paths_pinned_by_reader_namespace_read_txn"] = (
                obsolete_paths_pinned_by_reader_namespace_read_txn
            )
        if obsolete_paths_pinned_by_reader_probe_txn is not UNSET:
            field_dict["obsolete_paths_pinned_by_reader_probe_txn"] = obsolete_paths_pinned_by_reader_probe_txn
        if obsolete_paths_pinned_by_reader_current_scan is not UNSET:
            field_dict["obsolete_paths_pinned_by_reader_current_scan"] = obsolete_paths_pinned_by_reader_current_scan
        if obsolete_paths_pinned_by_reader_write_txn is not UNSET:
            field_dict["obsolete_paths_pinned_by_reader_write_txn"] = obsolete_paths_pinned_by_reader_write_txn
        if obsolete_paths_pinned_by_reader_compaction is not UNSET:
            field_dict["obsolete_paths_pinned_by_reader_compaction"] = obsolete_paths_pinned_by_reader_compaction
        if obsolete_paths_pinned_by_reader_other is not UNSET:
            field_dict["obsolete_paths_pinned_by_reader_other"] = obsolete_paths_pinned_by_reader_other
        if active_bulk_ingest_batches is not UNSET:
            field_dict["active_bulk_ingest_batches"] = active_bulk_ingest_batches
        if manifest_dirty is not UNSET:
            field_dict["manifest_dirty"] = manifest_dirty
        if obsolete_manifest_dirty is not UNSET:
            field_dict["obsolete_manifest_dirty"] = obsolete_manifest_dirty
        if maintenance_score is not UNSET:
            field_dict["maintenance_score"] = maintenance_score
        if maintenance_debt_hint is not UNSET:
            field_dict["maintenance_debt_hint"] = maintenance_debt_hint
        if flush_count is not UNSET:
            field_dict["flush_count"] = flush_count
        if flush_output_run_count is not UNSET:
            field_dict["flush_output_run_count"] = flush_output_run_count
        if flush_output_bytes is not UNSET:
            field_dict["flush_output_bytes"] = flush_output_bytes
        if sorted_ingest_run_count is not UNSET:
            field_dict["sorted_ingest_run_count"] = sorted_ingest_run_count
        if sorted_ingest_bytes is not UNSET:
            field_dict["sorted_ingest_bytes"] = sorted_ingest_bytes
        if manifest_write_count is not UNSET:
            field_dict["manifest_write_count"] = manifest_write_count
        if manifest_bytes is not UNSET:
            field_dict["manifest_bytes"] = manifest_bytes
        if write_pressure_event_count is not UNSET:
            field_dict["write_pressure_event_count"] = write_pressure_event_count
        if write_pressure_compaction_count is not UNSET:
            field_dict["write_pressure_compaction_count"] = write_pressure_compaction_count
        if write_pressure_compaction_step_count is not UNSET:
            field_dict["write_pressure_compaction_step_count"] = write_pressure_compaction_step_count
        if write_pressure_overload_count is not UNSET:
            field_dict["write_pressure_overload_count"] = write_pressure_overload_count
        if write_pressure_overload_l0_run_debt is not UNSET:
            field_dict["write_pressure_overload_l0_run_debt"] = write_pressure_overload_l0_run_debt
        if immutable_rotation_count is not UNSET:
            field_dict["immutable_rotation_count"] = immutable_rotation_count
        if immutable_flush_count is not UNSET:
            field_dict["immutable_flush_count"] = immutable_flush_count
        if direct_bulk_ingest_attempt_count is not UNSET:
            field_dict["direct_bulk_ingest_attempt_count"] = direct_bulk_ingest_attempt_count
        if direct_bulk_ingest_success_count is not UNSET:
            field_dict["direct_bulk_ingest_success_count"] = direct_bulk_ingest_success_count
        if direct_bulk_ingest_entry_count is not UNSET:
            field_dict["direct_bulk_ingest_entry_count"] = direct_bulk_ingest_entry_count
        if bulk_append_attempt_count is not UNSET:
            field_dict["bulk_append_attempt_count"] = bulk_append_attempt_count
        if bulk_append_entry_count is not UNSET:
            field_dict["bulk_append_entry_count"] = bulk_append_entry_count
        if bulk_append_direct_success_count is not UNSET:
            field_dict["bulk_append_direct_success_count"] = bulk_append_direct_success_count
        if bulk_append_direct_entry_count is not UNSET:
            field_dict["bulk_append_direct_entry_count"] = bulk_append_direct_entry_count
        if bulk_append_fallback_backend_pending_count is not UNSET:
            field_dict["bulk_append_fallback_backend_pending_count"] = bulk_append_fallback_backend_pending_count
        if bulk_append_fallback_below_threshold_count is not UNSET:
            field_dict["bulk_append_fallback_below_threshold_count"] = bulk_append_fallback_below_threshold_count
        if bulk_append_fallback_duplicate_key_count is not UNSET:
            field_dict["bulk_append_fallback_duplicate_key_count"] = bulk_append_fallback_duplicate_key_count
        if bulk_append_fallback_to_mutable_entry_count is not UNSET:
            field_dict["bulk_append_fallback_to_mutable_entry_count"] = bulk_append_fallback_to_mutable_entry_count
        if direct_bulk_ingest_direct_entry_count is not UNSET:
            field_dict["direct_bulk_ingest_direct_entry_count"] = direct_bulk_ingest_direct_entry_count
        if direct_bulk_ingest_fallback_unsupported_count is not UNSET:
            field_dict["direct_bulk_ingest_fallback_unsupported_count"] = direct_bulk_ingest_fallback_unsupported_count
        if direct_bulk_ingest_fallback_backend_mutable_count is not UNSET:
            field_dict["direct_bulk_ingest_fallback_backend_mutable_count"] = (
                direct_bulk_ingest_fallback_backend_mutable_count
            )
        if direct_bulk_ingest_fallback_below_threshold_count is not UNSET:
            field_dict["direct_bulk_ingest_fallback_below_threshold_count"] = (
                direct_bulk_ingest_fallback_below_threshold_count
            )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        run_count = d.pop("run_count", UNSET)

        run_bytes = d.pop("run_bytes", UNSET)

        l0_run_count = d.pop("l0_run_count", UNSET)

        l0_bytes = d.pop("l0_bytes", UNSET)

        lower_level_run_count = d.pop("lower_level_run_count", UNSET)

        lower_level_bytes = d.pop("lower_level_bytes", UNSET)

        max_level = d.pop("max_level", UNSET)

        compactable_l0_run_count = d.pop("compactable_l0_run_count", UNSET)

        overlapping_l0_run_count = d.pop("overlapping_l0_run_count", UNSET)

        soft_limit_l0_run_count = d.pop("soft_limit_l0_run_count", UNSET)

        hard_limit_l0_run_count = d.pop("hard_limit_l0_run_count", UNSET)

        write_stall_l0_run_debt = d.pop("write_stall_l0_run_debt", UNSET)

        soft_limit_l0_bytes = d.pop("soft_limit_l0_bytes", UNSET)

        hard_limit_l0_bytes = d.pop("hard_limit_l0_bytes", UNSET)

        write_stall_l0_byte_debt = d.pop("write_stall_l0_byte_debt", UNSET)

        level_overflow_run_count = d.pop("level_overflow_run_count", UNSET)

        level_overflow_bytes = d.pop("level_overflow_bytes", UNSET)

        obsolete_path_count = d.pop("obsolete_path_count", UNSET)

        obsolete_paths_pinned_by_readers = d.pop("obsolete_paths_pinned_by_readers", UNSET)

        obsolete_paths_pinned_by_versions = d.pop("obsolete_paths_pinned_by_versions", UNSET)

        obsolete_paths_waiting_for_retry = d.pop("obsolete_paths_waiting_for_retry", UNSET)

        obsolete_paths_reclaimable = d.pop("obsolete_paths_reclaimable", UNSET)

        obsolete_delete_failures = d.pop("obsolete_delete_failures", UNSET)

        obsolete_delete_retries = d.pop("obsolete_delete_retries", UNSET)

        current_manifest_bytes = d.pop("current_manifest_bytes", UNSET)

        mutable_entry_count = d.pop("mutable_entry_count", UNSET)

        mutable_bytes = d.pop("mutable_bytes", UNSET)

        immutable_memtable_count = d.pop("immutable_memtable_count", UNSET)

        immutable_entry_count = d.pop("immutable_entry_count", UNSET)

        immutable_bytes = d.pop("immutable_bytes", UNSET)

        mutable_snapshot_clone_count = d.pop("mutable_snapshot_clone_count", UNSET)

        mutable_snapshot_clone_bytes = d.pop("mutable_snapshot_clone_bytes", UNSET)

        mutable_snapshot_clone_peak_bytes = d.pop("mutable_snapshot_clone_peak_bytes", UNSET)

        read_snapshot_mutable_rotation_count = d.pop("read_snapshot_mutable_rotation_count", UNSET)

        read_snapshot_mutable_rotation_bytes = d.pop("read_snapshot_mutable_rotation_bytes", UNSET)

        wal_retained_bytes = d.pop("wal_retained_bytes", UNSET)

        wal_checkpoint_pending = d.pop("wal_checkpoint_pending", UNSET)

        wal_pressure_blocked = d.pop("wal_pressure_blocked", UNSET)

        wal_checkpoint_retry_reason = d.pop("wal_checkpoint_retry_reason", UNSET)

        wal_checkpoint_retry_attempts = d.pop("wal_checkpoint_retry_attempts", UNSET)

        wal_checkpoint_retry_delay_ns = d.pop("wal_checkpoint_retry_delay_ns", UNSET)

        active_immutable_logical_bytes = d.pop("active_immutable_logical_bytes", UNSET)

        unpublished_wal_logical_bytes = d.pop("unpublished_wal_logical_bytes", UNSET)

        unpublished_wal_max_batch_logical_bytes = d.pop("unpublished_wal_max_batch_logical_bytes", UNSET)

        compaction_backlog_bytes = d.pop("compaction_backlog_bytes", UNSET)

        active_readers = d.pop("active_readers", UNSET)

        active_readers_bound_read_txn = d.pop("active_readers_bound_read_txn", UNSET)

        active_readers_namespace_read_txn = d.pop("active_readers_namespace_read_txn", UNSET)

        active_readers_probe_txn = d.pop("active_readers_probe_txn", UNSET)

        active_readers_current_scan = d.pop("active_readers_current_scan", UNSET)

        active_readers_write_txn = d.pop("active_readers_write_txn", UNSET)

        active_readers_compaction = d.pop("active_readers_compaction", UNSET)

        active_readers_other = d.pop("active_readers_other", UNSET)

        obsolete_paths_pinned_by_reader_bound_read_txn = d.pop("obsolete_paths_pinned_by_reader_bound_read_txn", UNSET)

        obsolete_paths_pinned_by_reader_namespace_read_txn = d.pop(
            "obsolete_paths_pinned_by_reader_namespace_read_txn", UNSET
        )

        obsolete_paths_pinned_by_reader_probe_txn = d.pop("obsolete_paths_pinned_by_reader_probe_txn", UNSET)

        obsolete_paths_pinned_by_reader_current_scan = d.pop("obsolete_paths_pinned_by_reader_current_scan", UNSET)

        obsolete_paths_pinned_by_reader_write_txn = d.pop("obsolete_paths_pinned_by_reader_write_txn", UNSET)

        obsolete_paths_pinned_by_reader_compaction = d.pop("obsolete_paths_pinned_by_reader_compaction", UNSET)

        obsolete_paths_pinned_by_reader_other = d.pop("obsolete_paths_pinned_by_reader_other", UNSET)

        active_bulk_ingest_batches = d.pop("active_bulk_ingest_batches", UNSET)

        manifest_dirty = d.pop("manifest_dirty", UNSET)

        obsolete_manifest_dirty = d.pop("obsolete_manifest_dirty", UNSET)

        maintenance_score = d.pop("maintenance_score", UNSET)

        maintenance_debt_hint = d.pop("maintenance_debt_hint", UNSET)

        flush_count = d.pop("flush_count", UNSET)

        flush_output_run_count = d.pop("flush_output_run_count", UNSET)

        flush_output_bytes = d.pop("flush_output_bytes", UNSET)

        sorted_ingest_run_count = d.pop("sorted_ingest_run_count", UNSET)

        sorted_ingest_bytes = d.pop("sorted_ingest_bytes", UNSET)

        manifest_write_count = d.pop("manifest_write_count", UNSET)

        manifest_bytes = d.pop("manifest_bytes", UNSET)

        write_pressure_event_count = d.pop("write_pressure_event_count", UNSET)

        write_pressure_compaction_count = d.pop("write_pressure_compaction_count", UNSET)

        write_pressure_compaction_step_count = d.pop("write_pressure_compaction_step_count", UNSET)

        write_pressure_overload_count = d.pop("write_pressure_overload_count", UNSET)

        write_pressure_overload_l0_run_debt = d.pop("write_pressure_overload_l0_run_debt", UNSET)

        immutable_rotation_count = d.pop("immutable_rotation_count", UNSET)

        immutable_flush_count = d.pop("immutable_flush_count", UNSET)

        direct_bulk_ingest_attempt_count = d.pop("direct_bulk_ingest_attempt_count", UNSET)

        direct_bulk_ingest_success_count = d.pop("direct_bulk_ingest_success_count", UNSET)

        direct_bulk_ingest_entry_count = d.pop("direct_bulk_ingest_entry_count", UNSET)

        bulk_append_attempt_count = d.pop("bulk_append_attempt_count", UNSET)

        bulk_append_entry_count = d.pop("bulk_append_entry_count", UNSET)

        bulk_append_direct_success_count = d.pop("bulk_append_direct_success_count", UNSET)

        bulk_append_direct_entry_count = d.pop("bulk_append_direct_entry_count", UNSET)

        bulk_append_fallback_backend_pending_count = d.pop("bulk_append_fallback_backend_pending_count", UNSET)

        bulk_append_fallback_below_threshold_count = d.pop("bulk_append_fallback_below_threshold_count", UNSET)

        bulk_append_fallback_duplicate_key_count = d.pop("bulk_append_fallback_duplicate_key_count", UNSET)

        bulk_append_fallback_to_mutable_entry_count = d.pop("bulk_append_fallback_to_mutable_entry_count", UNSET)

        direct_bulk_ingest_direct_entry_count = d.pop("direct_bulk_ingest_direct_entry_count", UNSET)

        direct_bulk_ingest_fallback_unsupported_count = d.pop("direct_bulk_ingest_fallback_unsupported_count", UNSET)

        direct_bulk_ingest_fallback_backend_mutable_count = d.pop(
            "direct_bulk_ingest_fallback_backend_mutable_count", UNSET
        )

        direct_bulk_ingest_fallback_below_threshold_count = d.pop(
            "direct_bulk_ingest_fallback_below_threshold_count", UNSET
        )

        lsm_storage_status = cls(
            run_count=run_count,
            run_bytes=run_bytes,
            l0_run_count=l0_run_count,
            l0_bytes=l0_bytes,
            lower_level_run_count=lower_level_run_count,
            lower_level_bytes=lower_level_bytes,
            max_level=max_level,
            compactable_l0_run_count=compactable_l0_run_count,
            overlapping_l0_run_count=overlapping_l0_run_count,
            soft_limit_l0_run_count=soft_limit_l0_run_count,
            hard_limit_l0_run_count=hard_limit_l0_run_count,
            write_stall_l0_run_debt=write_stall_l0_run_debt,
            soft_limit_l0_bytes=soft_limit_l0_bytes,
            hard_limit_l0_bytes=hard_limit_l0_bytes,
            write_stall_l0_byte_debt=write_stall_l0_byte_debt,
            level_overflow_run_count=level_overflow_run_count,
            level_overflow_bytes=level_overflow_bytes,
            obsolete_path_count=obsolete_path_count,
            obsolete_paths_pinned_by_readers=obsolete_paths_pinned_by_readers,
            obsolete_paths_pinned_by_versions=obsolete_paths_pinned_by_versions,
            obsolete_paths_waiting_for_retry=obsolete_paths_waiting_for_retry,
            obsolete_paths_reclaimable=obsolete_paths_reclaimable,
            obsolete_delete_failures=obsolete_delete_failures,
            obsolete_delete_retries=obsolete_delete_retries,
            current_manifest_bytes=current_manifest_bytes,
            mutable_entry_count=mutable_entry_count,
            mutable_bytes=mutable_bytes,
            immutable_memtable_count=immutable_memtable_count,
            immutable_entry_count=immutable_entry_count,
            immutable_bytes=immutable_bytes,
            mutable_snapshot_clone_count=mutable_snapshot_clone_count,
            mutable_snapshot_clone_bytes=mutable_snapshot_clone_bytes,
            mutable_snapshot_clone_peak_bytes=mutable_snapshot_clone_peak_bytes,
            read_snapshot_mutable_rotation_count=read_snapshot_mutable_rotation_count,
            read_snapshot_mutable_rotation_bytes=read_snapshot_mutable_rotation_bytes,
            wal_retained_bytes=wal_retained_bytes,
            wal_checkpoint_pending=wal_checkpoint_pending,
            wal_pressure_blocked=wal_pressure_blocked,
            wal_checkpoint_retry_reason=wal_checkpoint_retry_reason,
            wal_checkpoint_retry_attempts=wal_checkpoint_retry_attempts,
            wal_checkpoint_retry_delay_ns=wal_checkpoint_retry_delay_ns,
            active_immutable_logical_bytes=active_immutable_logical_bytes,
            unpublished_wal_logical_bytes=unpublished_wal_logical_bytes,
            unpublished_wal_max_batch_logical_bytes=unpublished_wal_max_batch_logical_bytes,
            compaction_backlog_bytes=compaction_backlog_bytes,
            active_readers=active_readers,
            active_readers_bound_read_txn=active_readers_bound_read_txn,
            active_readers_namespace_read_txn=active_readers_namespace_read_txn,
            active_readers_probe_txn=active_readers_probe_txn,
            active_readers_current_scan=active_readers_current_scan,
            active_readers_write_txn=active_readers_write_txn,
            active_readers_compaction=active_readers_compaction,
            active_readers_other=active_readers_other,
            obsolete_paths_pinned_by_reader_bound_read_txn=obsolete_paths_pinned_by_reader_bound_read_txn,
            obsolete_paths_pinned_by_reader_namespace_read_txn=obsolete_paths_pinned_by_reader_namespace_read_txn,
            obsolete_paths_pinned_by_reader_probe_txn=obsolete_paths_pinned_by_reader_probe_txn,
            obsolete_paths_pinned_by_reader_current_scan=obsolete_paths_pinned_by_reader_current_scan,
            obsolete_paths_pinned_by_reader_write_txn=obsolete_paths_pinned_by_reader_write_txn,
            obsolete_paths_pinned_by_reader_compaction=obsolete_paths_pinned_by_reader_compaction,
            obsolete_paths_pinned_by_reader_other=obsolete_paths_pinned_by_reader_other,
            active_bulk_ingest_batches=active_bulk_ingest_batches,
            manifest_dirty=manifest_dirty,
            obsolete_manifest_dirty=obsolete_manifest_dirty,
            maintenance_score=maintenance_score,
            maintenance_debt_hint=maintenance_debt_hint,
            flush_count=flush_count,
            flush_output_run_count=flush_output_run_count,
            flush_output_bytes=flush_output_bytes,
            sorted_ingest_run_count=sorted_ingest_run_count,
            sorted_ingest_bytes=sorted_ingest_bytes,
            manifest_write_count=manifest_write_count,
            manifest_bytes=manifest_bytes,
            write_pressure_event_count=write_pressure_event_count,
            write_pressure_compaction_count=write_pressure_compaction_count,
            write_pressure_compaction_step_count=write_pressure_compaction_step_count,
            write_pressure_overload_count=write_pressure_overload_count,
            write_pressure_overload_l0_run_debt=write_pressure_overload_l0_run_debt,
            immutable_rotation_count=immutable_rotation_count,
            immutable_flush_count=immutable_flush_count,
            direct_bulk_ingest_attempt_count=direct_bulk_ingest_attempt_count,
            direct_bulk_ingest_success_count=direct_bulk_ingest_success_count,
            direct_bulk_ingest_entry_count=direct_bulk_ingest_entry_count,
            bulk_append_attempt_count=bulk_append_attempt_count,
            bulk_append_entry_count=bulk_append_entry_count,
            bulk_append_direct_success_count=bulk_append_direct_success_count,
            bulk_append_direct_entry_count=bulk_append_direct_entry_count,
            bulk_append_fallback_backend_pending_count=bulk_append_fallback_backend_pending_count,
            bulk_append_fallback_below_threshold_count=bulk_append_fallback_below_threshold_count,
            bulk_append_fallback_duplicate_key_count=bulk_append_fallback_duplicate_key_count,
            bulk_append_fallback_to_mutable_entry_count=bulk_append_fallback_to_mutable_entry_count,
            direct_bulk_ingest_direct_entry_count=direct_bulk_ingest_direct_entry_count,
            direct_bulk_ingest_fallback_unsupported_count=direct_bulk_ingest_fallback_unsupported_count,
            direct_bulk_ingest_fallback_backend_mutable_count=direct_bulk_ingest_fallback_backend_mutable_count,
            direct_bulk_ingest_fallback_below_threshold_count=direct_bulk_ingest_fallback_below_threshold_count,
        )

        lsm_storage_status.additional_properties = d
        return lsm_storage_status

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
