from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="VectorSourceStorageStatus")


@_attrs_define
class VectorSourceStorageStatus:
    """Source vector payload counters and the last completed reclamation observation. Counters reset on process reopen.

    Attributes:
        directory_publications (int | Unset):
        directory_publication_deferrals (int | Unset):
        outer_db_batch_lock_wait_ns (int | Unset):
        collection_mark_budget_yields (int | Unset): Mark steps that yielded after their elapsed time budget.
        collection_mark_busy_deferrals (int | Unset): Concurrent scan attempts deferred to the active snapshot scanner.
        collection_mark_outside_lock_ns (int | Unset): Snapshot marking time spent outside the source writer lock in
            nanoseconds.
        collection_mark_merge_ns (int | Unset): Time merging bounded scan discoveries and concurrent preparation tails
            under the source writer lock.
        collection_mark_max_merge_ns (int | Unset): Longest scan tail merge under the source writer lock in nanoseconds.
        collection_mark_steps (int | Unset):
        collection_mark_rows (int | Unset):
        collection_mark_max_step_rows (int | Unset):
        collection_mark_max_step_ns (int | Unset):
        collection_plan_ns (int | Unset):
        collection_apply_visits_avoided (int | Unset): Incomplete scan turns advanced without entering DB apply;
            metadata maintenance retains its own cadence.
        inventory_rows_scanned (int | Unset): Segment entries visited during full physical inventory scans or
            incremental updates; excludes WAL entries.
        catalog_metadata_bytes_shared (int | Unset): Logical immutable catalog bytes shared instead of copied by
            successful source snapshot and WAL-successor allocations; not resident bytes.
        catalog_metadata_bytes_copied (int | Unset): Immutable catalog array bytes copied by successful source snapshot
            and WAL-successor allocations; excludes object and root-path allocations.
        inventory_updates (int | Unset): Full physical inventory scans and incremental occurrence-cache updates; this
            cache is not ownership authority.
        inventory_update_ns (int | Unset): Time spent on full physical inventory scans or incremental occurrence-cache
            updates.
        collection_locked_ns (int | Unset): Total time inside the source writer lock for collection entry points;
            includes nested stages.
        collection_max_locked_ns (int | Unset): Longest collection entry point hold of the source writer lock.
        collection_setup_ns (int | Unset): Collection setup time, including primary sync, checkpoint and snapshot
            acquisition.
        collection_max_setup_ns (int | Unset): Longest collection setup.
        collection_max_plan_ns (int | Unset): Longest locked collection planning step.
        collection_copy_ns (int | Unset): Locked collection copy time excluding final publication.
        collection_max_copy_ns (int | Unset): Longest locked collection copy step.
        collection_publish_ns (int | Unset): Locked publication time including directory refresh, inventory, receipts
            and reclamation.
        collection_max_publish_ns (int | Unset): Longest locked publication step.
        collection_active_scan_turns (int | Unset): Active scan turns scheduled using the experimental wall-time duty
            policy.
        collection_active_scan_pause_ns (int | Unset): Requested pause time under the active scan policy; not measured
            CPU time.
        collection_rescued_payloads (int | Unset): Protection requests merged into marking, including repeated requests
            across turns.
        deduplicated_reappend_payloads (int | Unset): Durable payload preparations protected by marking without another
            append.
        deduplicated_reappend_bytes (int | Unset): Raw vector bytes avoided by protecting existing durable payloads
            during marking.
        directory_bytes_written (int | Unset):
        directory_entries (int | Unset):
        directory_hits (int | Unset):
        directory_misses (int | Unset):
        source_segments (int | Unset):
        ownership_index_collections (int | Unset):
        ownership_index_entries_scanned (int | Unset):
        prepare_requests (int | Unset):
        prepare_lock_wait_ns (int | Unset):
        decode_outside_lock_ns (int | Unset):
        snapshot_read_ns (int | Unset):
        cache_reclaimed_bytes (int | Unset):
        retired_ann_references_skipped (int | Unset):
        retained_payloads (int | Unset):
        retained_payload_bytes (int | Unset):
        unreferenced_payload_bytes_at_collection (int | Unset):
        checkpoint_bytes_read (int | Unset):
        checkpoint_bytes_written (int | Unset):
        heap_bytes (int | Unset): Allocator-backed source-store state charged to the shared resource manager, excluding
            mmap pages and request-owned buffers.
        location_cache_hits (int | Unset):
        location_cache_misses (int | Unset):
        location_cache_bytes (int | Unset):
        source_shards (int | Unset):
        collection_steps (int | Unset):
        collection_pending_bytes (int | Unset):
        collection_mark_ns (int | Unset):
        checkpoint_receipt_hits (int | Unset):
        checkpoint_inventory_restores (int | Unset):
        checkpoint_receipt_bytes_written (int | Unset):
        prepare_batches (int | Unset):
        preparation_ns (int | Unset):
        durable_append_ns (int | Unset):
        checkpoint_ns (int | Unset):
        prepared_payloads (int | Unset):
        prepared_payload_bytes (int | Unset):
        wal_bytes_written (int | Unset):
        active_sessions (int | Unset):
        resolved_payloads (int | Unset):
        resolved_bytes (int | Unset):
        active_wal_bytes (int | Unset):
        immutable_block_bytes (int | Unset):
        live_payloads_at_collection (int | Unset):
        live_payload_bytes_at_collection (int | Unset):
        collections (int | Unset):
        collection_deferrals (int | Unset):
        collection_bytes_read (int | Unset):
        collection_bytes_written (int | Unset):
        unresolved_primary_commits (int | Unset):
    """

    directory_publications: int | Unset = UNSET
    directory_publication_deferrals: int | Unset = UNSET
    outer_db_batch_lock_wait_ns: int | Unset = UNSET
    collection_mark_budget_yields: int | Unset = UNSET
    collection_mark_busy_deferrals: int | Unset = UNSET
    collection_mark_outside_lock_ns: int | Unset = UNSET
    collection_mark_merge_ns: int | Unset = UNSET
    collection_mark_max_merge_ns: int | Unset = UNSET
    collection_mark_steps: int | Unset = UNSET
    collection_mark_rows: int | Unset = UNSET
    collection_mark_max_step_rows: int | Unset = UNSET
    collection_mark_max_step_ns: int | Unset = UNSET
    collection_plan_ns: int | Unset = UNSET
    collection_apply_visits_avoided: int | Unset = UNSET
    inventory_rows_scanned: int | Unset = UNSET
    catalog_metadata_bytes_shared: int | Unset = UNSET
    catalog_metadata_bytes_copied: int | Unset = UNSET
    inventory_updates: int | Unset = UNSET
    inventory_update_ns: int | Unset = UNSET
    collection_locked_ns: int | Unset = UNSET
    collection_max_locked_ns: int | Unset = UNSET
    collection_setup_ns: int | Unset = UNSET
    collection_max_setup_ns: int | Unset = UNSET
    collection_max_plan_ns: int | Unset = UNSET
    collection_copy_ns: int | Unset = UNSET
    collection_max_copy_ns: int | Unset = UNSET
    collection_publish_ns: int | Unset = UNSET
    collection_max_publish_ns: int | Unset = UNSET
    collection_active_scan_turns: int | Unset = UNSET
    collection_active_scan_pause_ns: int | Unset = UNSET
    collection_rescued_payloads: int | Unset = UNSET
    deduplicated_reappend_payloads: int | Unset = UNSET
    deduplicated_reappend_bytes: int | Unset = UNSET
    directory_bytes_written: int | Unset = UNSET
    directory_entries: int | Unset = UNSET
    directory_hits: int | Unset = UNSET
    directory_misses: int | Unset = UNSET
    source_segments: int | Unset = UNSET
    ownership_index_collections: int | Unset = UNSET
    ownership_index_entries_scanned: int | Unset = UNSET
    prepare_requests: int | Unset = UNSET
    prepare_lock_wait_ns: int | Unset = UNSET
    decode_outside_lock_ns: int | Unset = UNSET
    snapshot_read_ns: int | Unset = UNSET
    cache_reclaimed_bytes: int | Unset = UNSET
    retired_ann_references_skipped: int | Unset = UNSET
    retained_payloads: int | Unset = UNSET
    retained_payload_bytes: int | Unset = UNSET
    unreferenced_payload_bytes_at_collection: int | Unset = UNSET
    checkpoint_bytes_read: int | Unset = UNSET
    checkpoint_bytes_written: int | Unset = UNSET
    heap_bytes: int | Unset = UNSET
    location_cache_hits: int | Unset = UNSET
    location_cache_misses: int | Unset = UNSET
    location_cache_bytes: int | Unset = UNSET
    source_shards: int | Unset = UNSET
    collection_steps: int | Unset = UNSET
    collection_pending_bytes: int | Unset = UNSET
    collection_mark_ns: int | Unset = UNSET
    checkpoint_receipt_hits: int | Unset = UNSET
    checkpoint_inventory_restores: int | Unset = UNSET
    checkpoint_receipt_bytes_written: int | Unset = UNSET
    prepare_batches: int | Unset = UNSET
    preparation_ns: int | Unset = UNSET
    durable_append_ns: int | Unset = UNSET
    checkpoint_ns: int | Unset = UNSET
    prepared_payloads: int | Unset = UNSET
    prepared_payload_bytes: int | Unset = UNSET
    wal_bytes_written: int | Unset = UNSET
    active_sessions: int | Unset = UNSET
    resolved_payloads: int | Unset = UNSET
    resolved_bytes: int | Unset = UNSET
    active_wal_bytes: int | Unset = UNSET
    immutable_block_bytes: int | Unset = UNSET
    live_payloads_at_collection: int | Unset = UNSET
    live_payload_bytes_at_collection: int | Unset = UNSET
    collections: int | Unset = UNSET
    collection_deferrals: int | Unset = UNSET
    collection_bytes_read: int | Unset = UNSET
    collection_bytes_written: int | Unset = UNSET
    unresolved_primary_commits: int | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        directory_publications = self.directory_publications

        directory_publication_deferrals = self.directory_publication_deferrals

        outer_db_batch_lock_wait_ns = self.outer_db_batch_lock_wait_ns

        collection_mark_budget_yields = self.collection_mark_budget_yields

        collection_mark_busy_deferrals = self.collection_mark_busy_deferrals

        collection_mark_outside_lock_ns = self.collection_mark_outside_lock_ns

        collection_mark_merge_ns = self.collection_mark_merge_ns

        collection_mark_max_merge_ns = self.collection_mark_max_merge_ns

        collection_mark_steps = self.collection_mark_steps

        collection_mark_rows = self.collection_mark_rows

        collection_mark_max_step_rows = self.collection_mark_max_step_rows

        collection_mark_max_step_ns = self.collection_mark_max_step_ns

        collection_plan_ns = self.collection_plan_ns

        collection_apply_visits_avoided = self.collection_apply_visits_avoided

        inventory_rows_scanned = self.inventory_rows_scanned

        catalog_metadata_bytes_shared = self.catalog_metadata_bytes_shared

        catalog_metadata_bytes_copied = self.catalog_metadata_bytes_copied

        inventory_updates = self.inventory_updates

        inventory_update_ns = self.inventory_update_ns

        collection_locked_ns = self.collection_locked_ns

        collection_max_locked_ns = self.collection_max_locked_ns

        collection_setup_ns = self.collection_setup_ns

        collection_max_setup_ns = self.collection_max_setup_ns

        collection_max_plan_ns = self.collection_max_plan_ns

        collection_copy_ns = self.collection_copy_ns

        collection_max_copy_ns = self.collection_max_copy_ns

        collection_publish_ns = self.collection_publish_ns

        collection_max_publish_ns = self.collection_max_publish_ns

        collection_active_scan_turns = self.collection_active_scan_turns

        collection_active_scan_pause_ns = self.collection_active_scan_pause_ns

        collection_rescued_payloads = self.collection_rescued_payloads

        deduplicated_reappend_payloads = self.deduplicated_reappend_payloads

        deduplicated_reappend_bytes = self.deduplicated_reappend_bytes

        directory_bytes_written = self.directory_bytes_written

        directory_entries = self.directory_entries

        directory_hits = self.directory_hits

        directory_misses = self.directory_misses

        source_segments = self.source_segments

        ownership_index_collections = self.ownership_index_collections

        ownership_index_entries_scanned = self.ownership_index_entries_scanned

        prepare_requests = self.prepare_requests

        prepare_lock_wait_ns = self.prepare_lock_wait_ns

        decode_outside_lock_ns = self.decode_outside_lock_ns

        snapshot_read_ns = self.snapshot_read_ns

        cache_reclaimed_bytes = self.cache_reclaimed_bytes

        retired_ann_references_skipped = self.retired_ann_references_skipped

        retained_payloads = self.retained_payloads

        retained_payload_bytes = self.retained_payload_bytes

        unreferenced_payload_bytes_at_collection = self.unreferenced_payload_bytes_at_collection

        checkpoint_bytes_read = self.checkpoint_bytes_read

        checkpoint_bytes_written = self.checkpoint_bytes_written

        heap_bytes = self.heap_bytes

        location_cache_hits = self.location_cache_hits

        location_cache_misses = self.location_cache_misses

        location_cache_bytes = self.location_cache_bytes

        source_shards = self.source_shards

        collection_steps = self.collection_steps

        collection_pending_bytes = self.collection_pending_bytes

        collection_mark_ns = self.collection_mark_ns

        checkpoint_receipt_hits = self.checkpoint_receipt_hits

        checkpoint_inventory_restores = self.checkpoint_inventory_restores

        checkpoint_receipt_bytes_written = self.checkpoint_receipt_bytes_written

        prepare_batches = self.prepare_batches

        preparation_ns = self.preparation_ns

        durable_append_ns = self.durable_append_ns

        checkpoint_ns = self.checkpoint_ns

        prepared_payloads = self.prepared_payloads

        prepared_payload_bytes = self.prepared_payload_bytes

        wal_bytes_written = self.wal_bytes_written

        active_sessions = self.active_sessions

        resolved_payloads = self.resolved_payloads

        resolved_bytes = self.resolved_bytes

        active_wal_bytes = self.active_wal_bytes

        immutable_block_bytes = self.immutable_block_bytes

        live_payloads_at_collection = self.live_payloads_at_collection

        live_payload_bytes_at_collection = self.live_payload_bytes_at_collection

        collections = self.collections

        collection_deferrals = self.collection_deferrals

        collection_bytes_read = self.collection_bytes_read

        collection_bytes_written = self.collection_bytes_written

        unresolved_primary_commits = self.unresolved_primary_commits

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if directory_publications is not UNSET:
            field_dict["directory_publications"] = directory_publications
        if directory_publication_deferrals is not UNSET:
            field_dict["directory_publication_deferrals"] = directory_publication_deferrals
        if outer_db_batch_lock_wait_ns is not UNSET:
            field_dict["outer_db_batch_lock_wait_ns"] = outer_db_batch_lock_wait_ns
        if collection_mark_budget_yields is not UNSET:
            field_dict["collection_mark_budget_yields"] = collection_mark_budget_yields
        if collection_mark_busy_deferrals is not UNSET:
            field_dict["collection_mark_busy_deferrals"] = collection_mark_busy_deferrals
        if collection_mark_outside_lock_ns is not UNSET:
            field_dict["collection_mark_outside_lock_ns"] = collection_mark_outside_lock_ns
        if collection_mark_merge_ns is not UNSET:
            field_dict["collection_mark_merge_ns"] = collection_mark_merge_ns
        if collection_mark_max_merge_ns is not UNSET:
            field_dict["collection_mark_max_merge_ns"] = collection_mark_max_merge_ns
        if collection_mark_steps is not UNSET:
            field_dict["collection_mark_steps"] = collection_mark_steps
        if collection_mark_rows is not UNSET:
            field_dict["collection_mark_rows"] = collection_mark_rows
        if collection_mark_max_step_rows is not UNSET:
            field_dict["collection_mark_max_step_rows"] = collection_mark_max_step_rows
        if collection_mark_max_step_ns is not UNSET:
            field_dict["collection_mark_max_step_ns"] = collection_mark_max_step_ns
        if collection_plan_ns is not UNSET:
            field_dict["collection_plan_ns"] = collection_plan_ns
        if collection_apply_visits_avoided is not UNSET:
            field_dict["collection_apply_visits_avoided"] = collection_apply_visits_avoided
        if inventory_rows_scanned is not UNSET:
            field_dict["inventory_rows_scanned"] = inventory_rows_scanned
        if catalog_metadata_bytes_shared is not UNSET:
            field_dict["catalog_metadata_bytes_shared"] = catalog_metadata_bytes_shared
        if catalog_metadata_bytes_copied is not UNSET:
            field_dict["catalog_metadata_bytes_copied"] = catalog_metadata_bytes_copied
        if inventory_updates is not UNSET:
            field_dict["inventory_updates"] = inventory_updates
        if inventory_update_ns is not UNSET:
            field_dict["inventory_update_ns"] = inventory_update_ns
        if collection_locked_ns is not UNSET:
            field_dict["collection_locked_ns"] = collection_locked_ns
        if collection_max_locked_ns is not UNSET:
            field_dict["collection_max_locked_ns"] = collection_max_locked_ns
        if collection_setup_ns is not UNSET:
            field_dict["collection_setup_ns"] = collection_setup_ns
        if collection_max_setup_ns is not UNSET:
            field_dict["collection_max_setup_ns"] = collection_max_setup_ns
        if collection_max_plan_ns is not UNSET:
            field_dict["collection_max_plan_ns"] = collection_max_plan_ns
        if collection_copy_ns is not UNSET:
            field_dict["collection_copy_ns"] = collection_copy_ns
        if collection_max_copy_ns is not UNSET:
            field_dict["collection_max_copy_ns"] = collection_max_copy_ns
        if collection_publish_ns is not UNSET:
            field_dict["collection_publish_ns"] = collection_publish_ns
        if collection_max_publish_ns is not UNSET:
            field_dict["collection_max_publish_ns"] = collection_max_publish_ns
        if collection_active_scan_turns is not UNSET:
            field_dict["collection_active_scan_turns"] = collection_active_scan_turns
        if collection_active_scan_pause_ns is not UNSET:
            field_dict["collection_active_scan_pause_ns"] = collection_active_scan_pause_ns
        if collection_rescued_payloads is not UNSET:
            field_dict["collection_rescued_payloads"] = collection_rescued_payloads
        if deduplicated_reappend_payloads is not UNSET:
            field_dict["deduplicated_reappend_payloads"] = deduplicated_reappend_payloads
        if deduplicated_reappend_bytes is not UNSET:
            field_dict["deduplicated_reappend_bytes"] = deduplicated_reappend_bytes
        if directory_bytes_written is not UNSET:
            field_dict["directory_bytes_written"] = directory_bytes_written
        if directory_entries is not UNSET:
            field_dict["directory_entries"] = directory_entries
        if directory_hits is not UNSET:
            field_dict["directory_hits"] = directory_hits
        if directory_misses is not UNSET:
            field_dict["directory_misses"] = directory_misses
        if source_segments is not UNSET:
            field_dict["source_segments"] = source_segments
        if ownership_index_collections is not UNSET:
            field_dict["ownership_index_collections"] = ownership_index_collections
        if ownership_index_entries_scanned is not UNSET:
            field_dict["ownership_index_entries_scanned"] = ownership_index_entries_scanned
        if prepare_requests is not UNSET:
            field_dict["prepare_requests"] = prepare_requests
        if prepare_lock_wait_ns is not UNSET:
            field_dict["prepare_lock_wait_ns"] = prepare_lock_wait_ns
        if decode_outside_lock_ns is not UNSET:
            field_dict["decode_outside_lock_ns"] = decode_outside_lock_ns
        if snapshot_read_ns is not UNSET:
            field_dict["snapshot_read_ns"] = snapshot_read_ns
        if cache_reclaimed_bytes is not UNSET:
            field_dict["cache_reclaimed_bytes"] = cache_reclaimed_bytes
        if retired_ann_references_skipped is not UNSET:
            field_dict["retired_ann_references_skipped"] = retired_ann_references_skipped
        if retained_payloads is not UNSET:
            field_dict["retained_payloads"] = retained_payloads
        if retained_payload_bytes is not UNSET:
            field_dict["retained_payload_bytes"] = retained_payload_bytes
        if unreferenced_payload_bytes_at_collection is not UNSET:
            field_dict["unreferenced_payload_bytes_at_collection"] = unreferenced_payload_bytes_at_collection
        if checkpoint_bytes_read is not UNSET:
            field_dict["checkpoint_bytes_read"] = checkpoint_bytes_read
        if checkpoint_bytes_written is not UNSET:
            field_dict["checkpoint_bytes_written"] = checkpoint_bytes_written
        if heap_bytes is not UNSET:
            field_dict["heap_bytes"] = heap_bytes
        if location_cache_hits is not UNSET:
            field_dict["location_cache_hits"] = location_cache_hits
        if location_cache_misses is not UNSET:
            field_dict["location_cache_misses"] = location_cache_misses
        if location_cache_bytes is not UNSET:
            field_dict["location_cache_bytes"] = location_cache_bytes
        if source_shards is not UNSET:
            field_dict["source_shards"] = source_shards
        if collection_steps is not UNSET:
            field_dict["collection_steps"] = collection_steps
        if collection_pending_bytes is not UNSET:
            field_dict["collection_pending_bytes"] = collection_pending_bytes
        if collection_mark_ns is not UNSET:
            field_dict["collection_mark_ns"] = collection_mark_ns
        if checkpoint_receipt_hits is not UNSET:
            field_dict["checkpoint_receipt_hits"] = checkpoint_receipt_hits
        if checkpoint_inventory_restores is not UNSET:
            field_dict["checkpoint_inventory_restores"] = checkpoint_inventory_restores
        if checkpoint_receipt_bytes_written is not UNSET:
            field_dict["checkpoint_receipt_bytes_written"] = checkpoint_receipt_bytes_written
        if prepare_batches is not UNSET:
            field_dict["prepare_batches"] = prepare_batches
        if preparation_ns is not UNSET:
            field_dict["preparation_ns"] = preparation_ns
        if durable_append_ns is not UNSET:
            field_dict["durable_append_ns"] = durable_append_ns
        if checkpoint_ns is not UNSET:
            field_dict["checkpoint_ns"] = checkpoint_ns
        if prepared_payloads is not UNSET:
            field_dict["prepared_payloads"] = prepared_payloads
        if prepared_payload_bytes is not UNSET:
            field_dict["prepared_payload_bytes"] = prepared_payload_bytes
        if wal_bytes_written is not UNSET:
            field_dict["wal_bytes_written"] = wal_bytes_written
        if active_sessions is not UNSET:
            field_dict["active_sessions"] = active_sessions
        if resolved_payloads is not UNSET:
            field_dict["resolved_payloads"] = resolved_payloads
        if resolved_bytes is not UNSET:
            field_dict["resolved_bytes"] = resolved_bytes
        if active_wal_bytes is not UNSET:
            field_dict["active_wal_bytes"] = active_wal_bytes
        if immutable_block_bytes is not UNSET:
            field_dict["immutable_block_bytes"] = immutable_block_bytes
        if live_payloads_at_collection is not UNSET:
            field_dict["live_payloads_at_collection"] = live_payloads_at_collection
        if live_payload_bytes_at_collection is not UNSET:
            field_dict["live_payload_bytes_at_collection"] = live_payload_bytes_at_collection
        if collections is not UNSET:
            field_dict["collections"] = collections
        if collection_deferrals is not UNSET:
            field_dict["collection_deferrals"] = collection_deferrals
        if collection_bytes_read is not UNSET:
            field_dict["collection_bytes_read"] = collection_bytes_read
        if collection_bytes_written is not UNSET:
            field_dict["collection_bytes_written"] = collection_bytes_written
        if unresolved_primary_commits is not UNSET:
            field_dict["unresolved_primary_commits"] = unresolved_primary_commits

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        directory_publications = d.pop("directory_publications", UNSET)

        directory_publication_deferrals = d.pop("directory_publication_deferrals", UNSET)

        outer_db_batch_lock_wait_ns = d.pop("outer_db_batch_lock_wait_ns", UNSET)

        collection_mark_budget_yields = d.pop("collection_mark_budget_yields", UNSET)

        collection_mark_busy_deferrals = d.pop("collection_mark_busy_deferrals", UNSET)

        collection_mark_outside_lock_ns = d.pop("collection_mark_outside_lock_ns", UNSET)

        collection_mark_merge_ns = d.pop("collection_mark_merge_ns", UNSET)

        collection_mark_max_merge_ns = d.pop("collection_mark_max_merge_ns", UNSET)

        collection_mark_steps = d.pop("collection_mark_steps", UNSET)

        collection_mark_rows = d.pop("collection_mark_rows", UNSET)

        collection_mark_max_step_rows = d.pop("collection_mark_max_step_rows", UNSET)

        collection_mark_max_step_ns = d.pop("collection_mark_max_step_ns", UNSET)

        collection_plan_ns = d.pop("collection_plan_ns", UNSET)

        collection_apply_visits_avoided = d.pop("collection_apply_visits_avoided", UNSET)

        inventory_rows_scanned = d.pop("inventory_rows_scanned", UNSET)

        catalog_metadata_bytes_shared = d.pop("catalog_metadata_bytes_shared", UNSET)

        catalog_metadata_bytes_copied = d.pop("catalog_metadata_bytes_copied", UNSET)

        inventory_updates = d.pop("inventory_updates", UNSET)

        inventory_update_ns = d.pop("inventory_update_ns", UNSET)

        collection_locked_ns = d.pop("collection_locked_ns", UNSET)

        collection_max_locked_ns = d.pop("collection_max_locked_ns", UNSET)

        collection_setup_ns = d.pop("collection_setup_ns", UNSET)

        collection_max_setup_ns = d.pop("collection_max_setup_ns", UNSET)

        collection_max_plan_ns = d.pop("collection_max_plan_ns", UNSET)

        collection_copy_ns = d.pop("collection_copy_ns", UNSET)

        collection_max_copy_ns = d.pop("collection_max_copy_ns", UNSET)

        collection_publish_ns = d.pop("collection_publish_ns", UNSET)

        collection_max_publish_ns = d.pop("collection_max_publish_ns", UNSET)

        collection_active_scan_turns = d.pop("collection_active_scan_turns", UNSET)

        collection_active_scan_pause_ns = d.pop("collection_active_scan_pause_ns", UNSET)

        collection_rescued_payloads = d.pop("collection_rescued_payloads", UNSET)

        deduplicated_reappend_payloads = d.pop("deduplicated_reappend_payloads", UNSET)

        deduplicated_reappend_bytes = d.pop("deduplicated_reappend_bytes", UNSET)

        directory_bytes_written = d.pop("directory_bytes_written", UNSET)

        directory_entries = d.pop("directory_entries", UNSET)

        directory_hits = d.pop("directory_hits", UNSET)

        directory_misses = d.pop("directory_misses", UNSET)

        source_segments = d.pop("source_segments", UNSET)

        ownership_index_collections = d.pop("ownership_index_collections", UNSET)

        ownership_index_entries_scanned = d.pop("ownership_index_entries_scanned", UNSET)

        prepare_requests = d.pop("prepare_requests", UNSET)

        prepare_lock_wait_ns = d.pop("prepare_lock_wait_ns", UNSET)

        decode_outside_lock_ns = d.pop("decode_outside_lock_ns", UNSET)

        snapshot_read_ns = d.pop("snapshot_read_ns", UNSET)

        cache_reclaimed_bytes = d.pop("cache_reclaimed_bytes", UNSET)

        retired_ann_references_skipped = d.pop("retired_ann_references_skipped", UNSET)

        retained_payloads = d.pop("retained_payloads", UNSET)

        retained_payload_bytes = d.pop("retained_payload_bytes", UNSET)

        unreferenced_payload_bytes_at_collection = d.pop("unreferenced_payload_bytes_at_collection", UNSET)

        checkpoint_bytes_read = d.pop("checkpoint_bytes_read", UNSET)

        checkpoint_bytes_written = d.pop("checkpoint_bytes_written", UNSET)

        heap_bytes = d.pop("heap_bytes", UNSET)

        location_cache_hits = d.pop("location_cache_hits", UNSET)

        location_cache_misses = d.pop("location_cache_misses", UNSET)

        location_cache_bytes = d.pop("location_cache_bytes", UNSET)

        source_shards = d.pop("source_shards", UNSET)

        collection_steps = d.pop("collection_steps", UNSET)

        collection_pending_bytes = d.pop("collection_pending_bytes", UNSET)

        collection_mark_ns = d.pop("collection_mark_ns", UNSET)

        checkpoint_receipt_hits = d.pop("checkpoint_receipt_hits", UNSET)

        checkpoint_inventory_restores = d.pop("checkpoint_inventory_restores", UNSET)

        checkpoint_receipt_bytes_written = d.pop("checkpoint_receipt_bytes_written", UNSET)

        prepare_batches = d.pop("prepare_batches", UNSET)

        preparation_ns = d.pop("preparation_ns", UNSET)

        durable_append_ns = d.pop("durable_append_ns", UNSET)

        checkpoint_ns = d.pop("checkpoint_ns", UNSET)

        prepared_payloads = d.pop("prepared_payloads", UNSET)

        prepared_payload_bytes = d.pop("prepared_payload_bytes", UNSET)

        wal_bytes_written = d.pop("wal_bytes_written", UNSET)

        active_sessions = d.pop("active_sessions", UNSET)

        resolved_payloads = d.pop("resolved_payloads", UNSET)

        resolved_bytes = d.pop("resolved_bytes", UNSET)

        active_wal_bytes = d.pop("active_wal_bytes", UNSET)

        immutable_block_bytes = d.pop("immutable_block_bytes", UNSET)

        live_payloads_at_collection = d.pop("live_payloads_at_collection", UNSET)

        live_payload_bytes_at_collection = d.pop("live_payload_bytes_at_collection", UNSET)

        collections = d.pop("collections", UNSET)

        collection_deferrals = d.pop("collection_deferrals", UNSET)

        collection_bytes_read = d.pop("collection_bytes_read", UNSET)

        collection_bytes_written = d.pop("collection_bytes_written", UNSET)

        unresolved_primary_commits = d.pop("unresolved_primary_commits", UNSET)

        vector_source_storage_status = cls(
            directory_publications=directory_publications,
            directory_publication_deferrals=directory_publication_deferrals,
            outer_db_batch_lock_wait_ns=outer_db_batch_lock_wait_ns,
            collection_mark_budget_yields=collection_mark_budget_yields,
            collection_mark_busy_deferrals=collection_mark_busy_deferrals,
            collection_mark_outside_lock_ns=collection_mark_outside_lock_ns,
            collection_mark_merge_ns=collection_mark_merge_ns,
            collection_mark_max_merge_ns=collection_mark_max_merge_ns,
            collection_mark_steps=collection_mark_steps,
            collection_mark_rows=collection_mark_rows,
            collection_mark_max_step_rows=collection_mark_max_step_rows,
            collection_mark_max_step_ns=collection_mark_max_step_ns,
            collection_plan_ns=collection_plan_ns,
            collection_apply_visits_avoided=collection_apply_visits_avoided,
            inventory_rows_scanned=inventory_rows_scanned,
            catalog_metadata_bytes_shared=catalog_metadata_bytes_shared,
            catalog_metadata_bytes_copied=catalog_metadata_bytes_copied,
            inventory_updates=inventory_updates,
            inventory_update_ns=inventory_update_ns,
            collection_locked_ns=collection_locked_ns,
            collection_max_locked_ns=collection_max_locked_ns,
            collection_setup_ns=collection_setup_ns,
            collection_max_setup_ns=collection_max_setup_ns,
            collection_max_plan_ns=collection_max_plan_ns,
            collection_copy_ns=collection_copy_ns,
            collection_max_copy_ns=collection_max_copy_ns,
            collection_publish_ns=collection_publish_ns,
            collection_max_publish_ns=collection_max_publish_ns,
            collection_active_scan_turns=collection_active_scan_turns,
            collection_active_scan_pause_ns=collection_active_scan_pause_ns,
            collection_rescued_payloads=collection_rescued_payloads,
            deduplicated_reappend_payloads=deduplicated_reappend_payloads,
            deduplicated_reappend_bytes=deduplicated_reappend_bytes,
            directory_bytes_written=directory_bytes_written,
            directory_entries=directory_entries,
            directory_hits=directory_hits,
            directory_misses=directory_misses,
            source_segments=source_segments,
            ownership_index_collections=ownership_index_collections,
            ownership_index_entries_scanned=ownership_index_entries_scanned,
            prepare_requests=prepare_requests,
            prepare_lock_wait_ns=prepare_lock_wait_ns,
            decode_outside_lock_ns=decode_outside_lock_ns,
            snapshot_read_ns=snapshot_read_ns,
            cache_reclaimed_bytes=cache_reclaimed_bytes,
            retired_ann_references_skipped=retired_ann_references_skipped,
            retained_payloads=retained_payloads,
            retained_payload_bytes=retained_payload_bytes,
            unreferenced_payload_bytes_at_collection=unreferenced_payload_bytes_at_collection,
            checkpoint_bytes_read=checkpoint_bytes_read,
            checkpoint_bytes_written=checkpoint_bytes_written,
            heap_bytes=heap_bytes,
            location_cache_hits=location_cache_hits,
            location_cache_misses=location_cache_misses,
            location_cache_bytes=location_cache_bytes,
            source_shards=source_shards,
            collection_steps=collection_steps,
            collection_pending_bytes=collection_pending_bytes,
            collection_mark_ns=collection_mark_ns,
            checkpoint_receipt_hits=checkpoint_receipt_hits,
            checkpoint_inventory_restores=checkpoint_inventory_restores,
            checkpoint_receipt_bytes_written=checkpoint_receipt_bytes_written,
            prepare_batches=prepare_batches,
            preparation_ns=preparation_ns,
            durable_append_ns=durable_append_ns,
            checkpoint_ns=checkpoint_ns,
            prepared_payloads=prepared_payloads,
            prepared_payload_bytes=prepared_payload_bytes,
            wal_bytes_written=wal_bytes_written,
            active_sessions=active_sessions,
            resolved_payloads=resolved_payloads,
            resolved_bytes=resolved_bytes,
            active_wal_bytes=active_wal_bytes,
            immutable_block_bytes=immutable_block_bytes,
            live_payloads_at_collection=live_payloads_at_collection,
            live_payload_bytes_at_collection=live_payload_bytes_at_collection,
            collections=collections,
            collection_deferrals=collection_deferrals,
            collection_bytes_read=collection_bytes_read,
            collection_bytes_written=collection_bytes_written,
            unresolved_primary_commits=unresolved_primary_commits,
        )

        vector_source_storage_status.additional_properties = d
        return vector_source_storage_status

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
