// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const antfly = @import("../root.zig");
const fs_paths = @import("../common/fs_paths.zig");
const group_ids = @import("../common/group_ids.zig");
const build_options = @import("build_options");
const raft_engine = @import("raft_engine");
const platform = @import("antfly_platform");
const tracing = @import("../tracing/mod.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const platform_time = @import("../platform/time.zig");

const setup_io_thread_stack_size = 1 * 1024 * 1024;
const metadata_raft_retained_entries = 1024;
const metadata_raft_compaction_min_interval_entries = 512;
const metadata_raft_election_max_ticks = 60;
const metadata_bootstrap_campaign_retry_min_interval_ns: u64 = 500 * std.time.ns_per_ms;
const trusted_principal_secret_key = "antfly.trusted_principal.secret";
const trusted_principal_issuer_key = "antfly.trusted_principal.issuer";

fn metadataRaftRuntimeConfig() raft_engine.runtime.RuntimeConfig {
    return .{
        .max_tick_batch = 32,
        .max_pending_outbound_messages = 4096,
        .max_pending_outbound_bytes = 16 * 1024 * 1024,
        .max_transport_messages_per_round = 64,
        .max_transport_bytes_per_round = 512 * 1024,
        .max_pending_apply_tasks = 1024,
        .max_pending_apply_bytes = 16 * 1024 * 1024,
        .max_apply_tasks_per_round = 16,
        .applied_log_retained_entries = metadata_raft_retained_entries,
        .applied_log_compaction_min_interval_entries = metadata_raft_compaction_min_interval_entries,
        .applied_log_compaction_single_node_only = false,
    };
}

fn metadataWalReplicaStateConfig() antfly.raft.storage.WalReplicaStateConfig {
    return .{
        .compaction_retained_entries = metadata_raft_retained_entries,
        .compaction_min_interval_entries = metadata_raft_compaction_min_interval_entries,
        .compaction_single_node_only = false,
    };
}

const CliConfig = struct {
    config_path: ?[]const u8 = null,
    raft_host: ?[]const u8 = null,
    raft_port: ?u16 = null,
    api_host: ?[]const u8 = null,
    api_port: ?u16 = null,
    cluster_json: ?[]const u8 = null,
    join: bool = false,
    health_enabled: ?bool = null,
    health_port: ?u16 = null,
    tick_ms: ?u64 = null,
    local_node_id: ?u64 = null,
    data_dir: ?[]const u8 = null,
    replica_root_dir: ?[]const u8 = null,
    replica_catalog_path: ?[]const u8 = null,
    snapshot_root_dir: ?[]const u8 = null,
    extension_package_store_dir: ?[]const u8 = null,
    secret_store_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    auth_enabled: ?bool = null,
    help: bool = false,

    fn deinit(self: *CliConfig, alloc: std.mem.Allocator) void {
        self.secret_store_paths.deinit(alloc);
        self.* = undefined;
    }
};

const Factory = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,
    metadata_group_id: u64,
    metadata_peer_node_ids: []u64 = &.{},

    fn deinit(self: *@This()) void {
        if (self.metadata_peer_node_ids.len > 0) self.alloc.free(self.metadata_peer_node_ids);
        self.* = undefined;
    }

    fn iface(self: *@This()) antfly.raft.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn buildDescriptor(ptr: *anyopaque, record: antfly.raft.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const peer_source = if (record.group_id == self.metadata_group_id and self.metadata_peer_node_ids.len > 0)
            self.metadata_peer_node_ids
        else
            &[_]u64{record.local_node_id};
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, peer_source);
        errdefer self.alloc.free(peers);
        var bootstrap = try antfly.raft.catalog.runtimeBootstrapFromRecord(self.alloc, record);
        errdefer antfly.raft.catalog.freeRuntimeBootstrap(self.alloc, &bootstrap);
        return .{
            .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = .{
                    .id = record.local_node_id,
                    .group_id = record.group_id,
                    .peers = peers,
                    .election_tick = 30,
                    .heartbeat_tick = 1,
                    .pre_vote = true,
                    .check_quorum = true,
                    .random_seed = antfly.raft.stableRandomSeed(record.group_id, record.local_node_id),
                },
                .storage = self.store.storage(),
            },
            .bootstrap = bootstrap,
        };
    }

    fn freeDescriptor(_: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        antfly.raft.catalog.freeRuntimeBootstrap(alloc, &desc.bootstrap);
        alloc.free(desc.group.raft_config.peers);
    }
};

const ResolvedPaths = struct {
    replica_root_dir: []u8,
    replica_catalog_path: []u8,
    snapshot_root_dir: []u8,
    auth_store_root_dir: []u8,
    extension_package_store_dir: []u8,

    fn deinit(self: ResolvedPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.replica_root_dir);
        alloc.free(self.replica_catalog_path);
        alloc.free(self.snapshot_root_dir);
        alloc.free(self.auth_store_root_dir);
        alloc.free(self.extension_package_store_dir);
    }
};

/// Backs the metadata server's health/metrics endpoints. Exposes local raft
/// host metrics and managed-service metrics as Prometheus text, and reports
/// readiness from a cached, constant-time probe flag. Shared by the standalone
/// metadata runtime and the swarm runtime so both expose the same metric set.
pub const HealthSource = struct {
    server: *Server,

    pub fn readiness(self: *HealthSource) antfly.common.health_server.ReadinessChecker {
        return .{
            .ptr = self,
            .vtable = &.{ .check = checkReady },
        };
    }

    pub fn metricsWriter(self: *HealthSource) antfly.common.health_server.MetricsWriter {
        return .{
            .ptr = self,
            .vtable = &.{ .write_metrics = writeMetrics },
        };
    }

    fn checkReady(ptr: *anyopaque) bool {
        const self: *HealthSource = @ptrCast(@alignCast(ptr));
        return self.server.metadataHttpService().probeReady();
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *HealthSource = @ptrCast(@alignCast(ptr));
        const svc = self.server.metadataHttpService();
        const host_metrics = svc.raft.host.http_host.metricsSnapshot();
        const svc_metrics = svc.metrics();
        const memory = svc.memoryDiagnostics();
        const raft_storage = self.server.metadataRaftStorageDiagnostics();

        const append = antfly.common.health_server.appendPromMetric;

        try append(writer, "antfly_raft_hosted_groups", "gauge", "Number of raft groups hosted on this node", @intCast(host_metrics.hosted_groups));
        try append(writer, "antfly_raft_reconcile_rounds_total", "counter", "Total number of reconcile rounds", @intCast(host_metrics.reconcile_rounds));
        try append(writer, "antfly_raft_ensure_replica_calls_total", "counter", "Total ensure_replica calls", @intCast(host_metrics.ensure_replica_calls));
        try append(writer, "antfly_raft_remove_replica_calls_total", "counter", "Total remove_replica calls", @intCast(host_metrics.remove_replica_calls));
        try append(writer, "antfly_raft_runtime_rounds_total", "counter", "Total raft runtime rounds", @intCast(host_metrics.runtime_rounds));
        try append(writer, "antfly_raft_runtime_ticked_groups_total", "counter", "Total raft groups ticked by the runtime", @intCast(host_metrics.runtime_ticked_groups));
        try append(writer, "antfly_raft_runtime_processed_groups_total", "counter", "Total raft ready groups processed by the runtime", @intCast(host_metrics.runtime_processed_groups));
        try append(writer, "antfly_raft_runtime_transport_message_sends_total", "counter", "Total raft transport messages flushed by the runtime", @intCast(host_metrics.runtime_transport_message_sends));
        try append(writer, "antfly_raft_runtime_pending_outbound_messages", "gauge", "Pending outbound raft messages inside the runtime", @intCast(host_metrics.runtime_pending_outbound_messages));
        try append(writer, "antfly_raft_runtime_pending_outbound_bytes", "gauge", "Approximate pending outbound raft bytes inside the runtime", @intCast(host_metrics.runtime_pending_outbound_bytes));
        try append(writer, "antfly_raft_runtime_pending_apply_tasks", "gauge", "Pending raft apply tasks inside the runtime", @intCast(host_metrics.runtime_pending_apply_tasks));
        try append(writer, "antfly_raft_runtime_pending_apply_bytes", "gauge", "Approximate pending raft apply bytes inside the runtime", @intCast(host_metrics.runtime_pending_apply_bytes));
        try append(writer, "antfly_raft_runtime_transport_queue_denials_total", "counter", "Total raft ready denials from outbound transport queue pressure", @intCast(host_metrics.runtime_transport_queue_denials));
        try append(writer, "antfly_raft_runtime_apply_queue_denials_total", "counter", "Total raft ready denials from apply queue pressure", @intCast(host_metrics.runtime_apply_queue_denials));
        try append(writer, "antfly_raft_backup_bootstrap_attempts_total", "counter", "Total backup bootstrap attempts", @intCast(host_metrics.backup_bootstrap_attempts));
        try append(writer, "antfly_raft_backup_bootstrap_failures_total", "counter", "Total backup bootstrap failures", @intCast(host_metrics.backup_bootstrap_failures));
        try append(writer, "antfly_raft_backup_bootstrap_successes_total", "counter", "Total backup bootstrap successes", @intCast(host_metrics.backup_bootstrap_successes));
        try append(writer, "antfly_raft_backup_bootstrap_durability_pending_total", "counter", "Total backup bootstraps awaiting generation durability confirmation", @intCast(host_metrics.backup_bootstrap_durability_pending));
        try append(writer, "antfly_raft_async_send_enqueued_total", "counter", "Total raft frames enqueued for async HTTP send", host_metrics.async_send_enqueued);
        try append(writer, "antfly_raft_async_send_failed_total", "counter", "Total async raft HTTP send failures before retry or drop", host_metrics.async_send_failed);
        try append(writer, "antfly_raft_async_send_retried_total", "counter", "Total async raft HTTP frames requeued for retry", host_metrics.async_send_retried);
        try append(writer, "antfly_raft_async_send_dropped_total", "counter", "Total async raft HTTP frames dropped after retry or queue limits", host_metrics.async_send_dropped);
        try append(writer, "antfly_raft_async_send_queue_full_total", "counter", "Total async raft HTTP global queue-full events", host_metrics.async_send_queue_full);
        try append(writer, "antfly_raft_async_send_peer_queue_full_total", "counter", "Total async raft HTTP per-peer queue-full events", host_metrics.async_send_peer_queue_full);
        try append(writer, "antfly_raft_async_send_pending", "gauge", "Pending async raft HTTP frames", @intCast(host_metrics.async_send_pending));

        try append(writer, "antfly_service_queued_updates", "gauge", "Pending metadata updates waiting to apply", @intCast(svc_metrics.queued_updates));
        try append(writer, "antfly_service_applied_updates_total", "counter", "Total applied metadata updates", @intCast(svc_metrics.applied_updates));
        try append(writer, "antfly_service_sync_rounds_total", "counter", "Total metadata sync rounds", @intCast(svc_metrics.sync_rounds));
        try append(writer, "antfly_service_read_lease_requests_total", "counter", "Total readable-lease requests", @intCast(svc_metrics.read_lease_requests));
        try append(writer, "antfly_service_split_transitions_queued", "gauge", "Queued split transitions", @intCast(svc_metrics.queued_split_transitions));
        try append(writer, "antfly_service_split_transitions_completed_total", "counter", "Completed split transitions", @intCast(svc_metrics.completed_split_transitions));
        try append(writer, "antfly_service_merge_transitions_queued", "gauge", "Queued merge transitions", @intCast(svc_metrics.queued_merge_transitions));
        try append(writer, "antfly_service_merge_transitions_completed_total", "counter", "Completed merge transitions", @intCast(svc_metrics.completed_merge_transitions));

        try append(writer, "antfly_process_memory_available", "gauge", "Whether process memory metrics are available on this platform", if (memory.process.available) 1 else 0);
        if (memory.process.available) {
            try append(writer, "antfly_process_resident_bytes", "gauge", "Process resident bytes reported by the operating system", memory.process.resident_bytes);
            try append(writer, "antfly_process_anonymous_bytes", "gauge", "Process anonymous resident bytes reported by the operating system", memory.process.anonymous_bytes);
            try append(writer, "antfly_process_private_dirty_bytes", "gauge", "Process private dirty bytes reported by the operating system", memory.process.private_dirty_bytes);
            try append(writer, "antfly_process_footprint_bytes", "gauge", "Process physical footprint bytes reported by the operating system", memory.process.footprint_bytes);
            try append(writer, "antfly_process_wired_bytes", "gauge", "Process wired bytes reported by the operating system", memory.process.wired_bytes);
            try append(writer, "antfly_process_pageins_total", "counter", "Process page-ins reported by the operating system", memory.process.pageins);
            try append(writer, "antfly_process_malloc_available", "gauge", "Whether process malloc zone metrics are available on this platform", if (memory.process.malloc_available) 1 else 0);
            if (memory.process.malloc_available) {
                try append(writer, "antfly_process_malloc_allocated_bytes", "gauge", "Live bytes allocated across process malloc zones", memory.process.malloc_allocated_bytes);
                try append(writer, "antfly_process_malloc_zone_bytes", "gauge", "Bytes reserved by process malloc zones", memory.process.malloc_zone_bytes);
            }
        }

        try append(writer, "antfly_metadata_projected_core_snapshot_cached", "gauge", "Whether the metadata projected-core snapshot cache currently has a snapshot", if (memory.projected_core_snapshot.cached) 1 else 0);
        try append(writer, "antfly_metadata_projected_core_snapshot_estimated_bytes", "gauge", "Approximate retained bytes in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.estimated_bytes));
        try append(writer, "antfly_metadata_projected_core_snapshot_tables", "gauge", "Tables retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.tables));
        try append(writer, "antfly_metadata_projected_core_snapshot_ranges", "gauge", "Ranges retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.ranges));
        try append(writer, "antfly_metadata_projected_core_snapshot_stores", "gauge", "Stores retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.stores));
        try append(writer, "antfly_metadata_projected_core_snapshot_store_group_statuses", "gauge", "Store group-status records retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.store_group_statuses));
        try append(writer, "antfly_metadata_projected_core_snapshot_store_runtime_statuses", "gauge", "Store runtime-status records retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.store_runtime_statuses));
        try append(writer, "antfly_metadata_projected_core_snapshot_placement_intents", "gauge", "Placement intents retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.placement_intents));
        try append(writer, "antfly_metadata_projected_core_snapshot_schema_progresses", "gauge", "Schema-progress records retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.schema_progresses));
        try append(writer, "antfly_metadata_projected_core_snapshot_restore_progresses", "gauge", "Restore-progress records retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.restore_progresses));
        try append(writer, "antfly_metadata_projected_core_snapshot_replication_source_statuses", "gauge", "Replication source statuses retained in the metadata projected-core snapshot cache", @intCast(memory.projected_core_snapshot.replication_source_statuses));

        try append(writer, "antfly_metadata_raft_memory_storage_groups", "gauge", "Raft groups counted in metadata raft MemoryStorage diagnostics", @intCast(raft_storage.groups));
        try append(writer, "antfly_metadata_raft_memory_storage_entries", "gauge", "Raft log entries retained by metadata raft MemoryStorage", @intCast(raft_storage.entries));
        try append(writer, "antfly_metadata_raft_memory_storage_entry_capacity", "gauge", "Raft log entry capacity retained by metadata raft MemoryStorage", @intCast(raft_storage.entry_capacity));
        try append(writer, "antfly_metadata_raft_memory_storage_entry_payload_bytes", "gauge", "Raft log entry payload bytes retained by metadata raft MemoryStorage", @intCast(raft_storage.entry_payload_bytes));
        try append(writer, "antfly_metadata_raft_memory_storage_estimated_bytes", "gauge", "Approximate bytes retained by metadata raft MemoryStorage", @intCast(raft_storage.estimated_bytes));
        try append(writer, "antfly_metadata_raft_memory_storage_max_entries_per_group", "gauge", "Maximum raft log entries retained by any metadata raft group MemoryStorage", @intCast(raft_storage.max_entries_per_group));
        try append(writer, "antfly_metadata_raft_memory_storage_min_first_index", "gauge", "Minimum first raft log index retained by metadata raft MemoryStorage", raft_storage.min_first_index);
        try append(writer, "antfly_metadata_raft_memory_storage_max_last_index", "gauge", "Maximum last raft log index retained by metadata raft MemoryStorage", raft_storage.max_last_index);
        try append(writer, "antfly_metadata_raft_memory_storage_max_snapshot_index", "gauge", "Maximum snapshot index retained by metadata raft MemoryStorage", raft_storage.max_snapshot_index);
        try append(writer, "antfly_metadata_raft_memory_storage_compactions_total", "counter", "Total metadata raft MemoryStorage compactions reported by the replica state provider", raft_storage.storage_compactions);

        try append(writer, "antfly_metadata_json_response_calls_total", "counter", "Metadata JSON responses allocated by the metadata HTTP server", memory.json_response.calls);
        try append(writer, "antfly_metadata_json_response_bytes_total", "counter", "Total JSON response body bytes allocated by the metadata HTTP server", memory.json_response.bytes_total);
        try append(writer, "antfly_metadata_json_response_peak_bytes", "gauge", "Largest JSON response body allocated by the metadata HTTP server", memory.json_response.peak_bytes);

        try append(writer, "antfly_metadata_projected_store_lsm_mutable_bytes", "gauge", "Mutable in-memory LSM bytes retained by the metadata projected store", memory.projected_store_lsm.mutable_bytes);
        try append(writer, "antfly_metadata_projected_store_lsm_immutable_bytes", "gauge", "Immutable in-memory LSM bytes retained by the metadata projected store", memory.projected_store_lsm.immutable_bytes);
        try append(writer, "antfly_metadata_projected_store_lsm_run_bytes", "gauge", "Table-run bytes retained by the metadata projected store LSM", memory.projected_store_lsm.total_run_bytes);
        try append(writer, "antfly_metadata_projected_store_lsm_wal_retained_bytes", "gauge", "WAL bytes retained by the metadata projected store LSM", memory.projected_store_lsm.wal_retained_bytes);
        try append(writer, "antfly_metadata_projected_store_lsm_wal_retained_segments", "gauge", "WAL segments retained by the metadata projected store LSM", memory.projected_store_lsm.wal_retained_segments);
        try append(writer, "antfly_metadata_projected_store_lsm_active_readers", "gauge", "Active readers pinning metadata projected store LSM state", memory.projected_store_lsm.active_readers);
        try append(writer, "antfly_metadata_projected_store_lsm_obsolete_paths", "gauge", "Obsolete LSM paths retained by the metadata projected store", memory.projected_store_lsm.obsolete_paths);
        try append(writer, "antfly_metadata_projected_store_lsm_bulk_clone_active_bytes", "gauge", "Bulk-ingest current-scan clone bytes retained by the metadata projected store LSM", memory.projected_store_lsm.bulk_ingest_current_scan_clone_active_bytes);

        try append(writer, "antfly_metadata_hosted_write_cache_present", "gauge", "Whether a hosted metadata write DB cache exists for this replica root", if (memory.hosted_write_cache.present) 1 else 0);
        try append(writer, "antfly_metadata_hosted_write_cache_roots", "gauge", "Hosted metadata write DB cache roots registered in this process", memory.hosted_write_cache.cached_roots);
        try append(writer, "antfly_metadata_hosted_write_cache_entries", "gauge", "Live hosted metadata write DB cache entries", memory.hosted_write_cache.cached_entries);
        try append(writer, "antfly_metadata_hosted_write_cache_retired_entries", "gauge", "Retired hosted metadata write DB cache entries waiting for leases to release", memory.hosted_write_cache.retired_entries);
        try append(writer, "antfly_metadata_hosted_write_cache_table_metadata_entries", "gauge", "Table metadata entries retained by the hosted metadata write DB cache", memory.hosted_write_cache.table_metadata_entries);
        try append(writer, "antfly_metadata_hosted_write_cache_active_leases", "gauge", "Active leases held on live hosted metadata write DB cache entries", memory.hosted_write_cache.active_leases);
        try append(writer, "antfly_metadata_hosted_write_cache_retired_active_leases", "gauge", "Active leases keeping retired hosted metadata write DB cache entries alive", memory.hosted_write_cache.retired_active_leases);
        try append(writer, "antfly_metadata_hosted_write_cache_active_bulk_sessions", "gauge", "Explicit bulk-ingest sessions retained by the hosted metadata write DB cache", memory.hosted_write_cache.active_bulk_sessions);
        try append(writer, "antfly_metadata_hosted_write_cache_bulk_ingest_open_entries", "gauge", "Hosted metadata write DB cache entries with open bulk-ingest sessions", memory.hosted_write_cache.bulk_ingest_open_entries);
        try append(writer, "antfly_metadata_hosted_write_cache_auto_bulk_open_entries", "gauge", "Hosted metadata write DB cache entries with open auto bulk-ingest sessions", memory.hosted_write_cache.auto_bulk_ingest_open_entries);
        try append(writer, "antfly_metadata_hosted_write_cache_auto_bulk_finish_requested_entries", "gauge", "Hosted metadata write DB cache auto bulk-ingest entries that requested finish", memory.hosted_write_cache.auto_bulk_ingest_finish_requested_entries);
        try append(writer, "antfly_metadata_hosted_write_cache_lsm_mutable_bytes", "gauge", "Mutable in-memory LSM bytes retained by hosted metadata write DB cache entries", memory.hosted_write_cache.lsm_mutable_bytes);
        try append(writer, "antfly_metadata_hosted_write_cache_lsm_immutable_bytes", "gauge", "Immutable in-memory LSM bytes retained by hosted metadata write DB cache entries", memory.hosted_write_cache.lsm_immutable_bytes);
        try append(writer, "antfly_metadata_hosted_write_cache_lsm_run_bytes", "gauge", "Table-run bytes retained by hosted metadata write DB cache entries", memory.hosted_write_cache.lsm_total_run_bytes);
        try append(writer, "antfly_metadata_hosted_write_cache_lsm_wal_retained_bytes", "gauge", "WAL bytes retained by hosted metadata write DB cache entries", memory.hosted_write_cache.lsm_wal_retained_bytes);
        try append(writer, "antfly_metadata_hosted_write_cache_lsm_wal_retained_segments", "gauge", "WAL segments retained by hosted metadata write DB cache entries", memory.hosted_write_cache.lsm_wal_retained_segments);
        try append(writer, "antfly_metadata_hosted_write_cache_lsm_active_readers", "gauge", "Active readers pinning hosted metadata write DB cache LSM state", memory.hosted_write_cache.lsm_active_readers);
        try append(writer, "antfly_metadata_hosted_write_cache_lsm_obsolete_paths", "gauge", "Obsolete LSM paths retained by hosted metadata write DB cache entries", memory.hosted_write_cache.lsm_obsolete_paths);
        try append(writer, "antfly_metadata_hosted_write_cache_lsm_bulk_clone_active_bytes", "gauge", "Bulk-ingest current-scan clone bytes retained by hosted metadata write DB cache entries", memory.hosted_write_cache.lsm_bulk_ingest_current_scan_clone_active_bytes);
    }
};

pub const ListenerConfig = struct {
    bind_host: []const u8,
    bind_port: u16,
};

pub const MetadataClusterPeer = struct {
    node_id: u64,
    raft_url: []const u8,
};

pub const ServerConfig = struct {
    local_node_id: u64 = 1,
    metadata_group_id: u64 = group_ids.main_metadata_group_id,
    metadata_cluster_peers: []const MetadataClusterPeer = &.{},
    replica_root_dir: []const u8,
    replica_catalog_path: []const u8,
    snapshot_root_dir: []const u8,
    observe_local_replica_root: bool = true,
    replica_state_backend: antfly.raft.ReplicaStateBackend = .wal,
    bind_host: []const u8 = "127.0.0.1",
    bind_port: u16 = 0,
    admin_bind_host: []const u8 = "127.0.0.1",
    admin_bind_port: u16 = 0,
    reconciler_config: antfly.metadata.reconciler.Reconciler.Config = .{},
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    secret_store: ?*antfly.common.secrets.FileStore = null,
    api_server_cfg: antfly.public_api.http_server.ApiHttpServerConfig = .{},
};

const MetadataRaftStorageDiagnostics = struct {
    groups: usize = 0,
    entries: usize = 0,
    entry_capacity: usize = 0,
    entry_payload_bytes: usize = 0,
    estimated_bytes: usize = 0,
    max_entries_per_group: usize = 0,
    min_first_index: raft_engine.core.types.Index = 0,
    max_last_index: raft_engine.core.types.Index = 0,
    max_snapshot_index: raft_engine.core.types.Index = 0,
    storage_compactions: u64 = 0,
};

const MetadataRaftStorageDiagnosticsCache = struct {
    groups: std.atomic.Value(usize) = .init(0),
    entries: std.atomic.Value(usize) = .init(0),
    entry_capacity: std.atomic.Value(usize) = .init(0),
    entry_payload_bytes: std.atomic.Value(usize) = .init(0),
    estimated_bytes: std.atomic.Value(usize) = .init(0),
    max_entries_per_group: std.atomic.Value(usize) = .init(0),
    min_first_index: std.atomic.Value(raft_engine.core.types.Index) = .init(0),
    max_last_index: std.atomic.Value(raft_engine.core.types.Index) = .init(0),
    max_snapshot_index: std.atomic.Value(raft_engine.core.types.Index) = .init(0),
    storage_compactions: std.atomic.Value(u64) = .init(0),

    fn store(self: *MetadataRaftStorageDiagnosticsCache, value: MetadataRaftStorageDiagnostics) void {
        self.groups.store(value.groups, .monotonic);
        self.entries.store(value.entries, .monotonic);
        self.entry_capacity.store(value.entry_capacity, .monotonic);
        self.entry_payload_bytes.store(value.entry_payload_bytes, .monotonic);
        self.estimated_bytes.store(value.estimated_bytes, .monotonic);
        self.max_entries_per_group.store(value.max_entries_per_group, .monotonic);
        self.min_first_index.store(value.min_first_index, .monotonic);
        self.max_last_index.store(value.max_last_index, .monotonic);
        self.max_snapshot_index.store(value.max_snapshot_index, .monotonic);
        self.storage_compactions.store(value.storage_compactions, .monotonic);
    }

    fn load(self: *const MetadataRaftStorageDiagnosticsCache) MetadataRaftStorageDiagnostics {
        return .{
            .groups = self.groups.load(.monotonic),
            .entries = self.entries.load(.monotonic),
            .entry_capacity = self.entry_capacity.load(.monotonic),
            .entry_payload_bytes = self.entry_payload_bytes.load(.monotonic),
            .estimated_bytes = self.estimated_bytes.load(.monotonic),
            .max_entries_per_group = self.max_entries_per_group.load(.monotonic),
            .min_first_index = self.min_first_index.load(.monotonic),
            .max_last_index = self.max_last_index.load(.monotonic),
            .max_snapshot_index = self.max_snapshot_index.load(.monotonic),
            .storage_compactions = self.storage_compactions.load(.monotonic),
        };
    }
};

pub const Server = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,
    factory: *Factory,
    server: antfly.metadata_server.MetadataServer,
    replica_root_dir: []u8,
    replica_catalog_path: []u8,
    snapshot_root_dir: []u8,
    bind_host: []u8,
    admin_bind_host: []u8,
    raft_storage_diagnostics: MetadataRaftStorageDiagnosticsCache = .{},

    pub fn init(alloc: std.mem.Allocator, cfg: ServerConfig) !Server {
        var result: Server = undefined;
        result.alloc = alloc;
        result.raft_storage_diagnostics = .{};
        result.store = try alloc.create(raft_engine.core.MemoryStorage);
        errdefer alloc.destroy(result.store);
        result.store.* = raft_engine.core.MemoryStorage.init(alloc);
        errdefer result.store.deinit();
        result.factory = try alloc.create(Factory);
        errdefer alloc.destroy(result.factory);
        result.factory.* = .{
            .alloc = alloc,
            .store = result.store,
            .metadata_group_id = cfg.metadata_group_id,
            .metadata_peer_node_ids = try allocMetadataPeerNodeIds(alloc, cfg.local_node_id, cfg.metadata_cluster_peers),
        };
        errdefer result.factory.deinit();
        result.replica_root_dir = try alloc.dupe(u8, cfg.replica_root_dir);
        errdefer alloc.free(result.replica_root_dir);
        result.replica_catalog_path = try alloc.dupe(u8, cfg.replica_catalog_path);
        errdefer alloc.free(result.replica_catalog_path);
        result.snapshot_root_dir = try alloc.dupe(u8, cfg.snapshot_root_dir);
        errdefer alloc.free(result.snapshot_root_dir);
        result.bind_host = try alloc.dupe(u8, cfg.bind_host);
        errdefer alloc.free(result.bind_host);
        result.admin_bind_host = try alloc.dupe(u8, cfg.admin_bind_host);
        errdefer alloc.free(result.admin_bind_host);
        const service_cfg = antfly.metadata_service.MetadataServiceConfig{
            .observe_local_replica_root = cfg.observe_local_replica_root,
            .backend_runtime = cfg.backend_runtime,
            .secret_store = cfg.secret_store,
        };
        result.server = try antfly.metadata_server.MetadataServer.init(alloc, .{
            .http = .{
                .http = .{
                    .host = .{
                        .local_node_id = cfg.local_node_id,
                        .metadata_group_id = cfg.metadata_group_id,
                        .runtime = metadataRaftRuntimeConfig(),
                        .replica_root_dir = result.replica_root_dir,
                        .replica_catalog_path = result.replica_catalog_path,
                        .replica_state_backend = cfg.replica_state_backend,
                        .trace_logger = if (build_options.with_tla) tracing.stderrRaftTraceLogger() else null,
                    },
                    .listener = antfly.raft.httpListenerConfig(result.bind_host, cfg.bind_port),
                    .transport = .{
                        .snapshot = .{
                            .root_dir = result.snapshot_root_dir,
                        },
                    },
                },
                .wal_replica_state = metadataWalReplicaStateConfig(),
            },
            .admin_listener = .{
                .bind_host = result.admin_bind_host,
                .bind_port = cfg.admin_bind_port,
                .serve_in_connection_threads = true,
                .max_connection_threads = 32,
            },
            .service = service_cfg,
            .api_server_cfg = cfg.api_server_cfg,
            .reconciler_config = cfg.reconciler_config,
        }, .{
            .http = .{
                .http = .{
                    .http = .{
                        .host = .{
                            .descriptor_factory = result.factory.iface(),
                        },
                    },
                },
            },
        });
        errdefer result.server.deinit();
        return result;
    }

    pub fn deinit(self: *Server) void {
        self.server.deinit();
        self.alloc.free(self.admin_bind_host);
        self.alloc.free(self.bind_host);
        self.alloc.free(self.snapshot_root_dir);
        self.alloc.free(self.replica_catalog_path);
        self.alloc.free(self.replica_root_dir);
        self.factory.deinit();
        self.store.deinit();
        self.alloc.destroy(self.factory);
        self.alloc.destroy(self.store);
        self.* = undefined;
    }

    pub fn start(self: *Server) !void {
        try self.server.start();
    }

    pub fn bootstrapCluster(
        self: *Server,
        metadata_group_id: u64,
        local_node_id: u64,
        cluster_peers: []const MetadataClusterPeer,
    ) !void {
        if (cluster_peers.len == 0) return try self.bootstrapLocal(metadata_group_id, local_node_id);

        const local_index = indexOfClusterPeer(cluster_peers, local_node_id) orelse return error.MissingLocalMetadataPeer;
        var peer_node_ids = try self.alloc.alloc(u64, cluster_peers.len - 1);
        defer self.alloc.free(peer_node_ids);
        var peer_index: usize = 0;
        for (cluster_peers) |peer| {
            if (peer.node_id == local_node_id) continue;
            peer_node_ids[peer_index] = peer.node_id;
            peer_index += 1;
        }

        const local_record: antfly.raft.catalog.ReplicaRecord = .{
            .group_id = metadata_group_id,
            .replica_id = @as(u64, @intCast(local_index + 1)),
            .local_node_id = local_node_id,
            .bootstrap_mode = .empty,
        };
        if (self.server.svc.raft.host.status(metadata_group_id) == .absent) {
            _ = try self.server.svc.ensureMetadataReplica(local_record);
        }
        var route_endpoints = try self.alloc.alloc([1]antfly.raft.PeerEndpoint, cluster_peers.len);
        defer self.alloc.free(route_endpoints);
        for (cluster_peers, 0..) |peer, index| {
            route_endpoints[index][0] = .{
                .protocol = .http,
                .address = peer.raft_url,
                .metadata = "",
            };
            _ = try self.server.svc.raft.host.http_host.upsertResolvedPeerEndpoints(
                metadata_group_id,
                peer.node_id,
                route_endpoints[index][0..],
            );
        }
        var updates = std.ArrayListUnmanaged(antfly.raft.MetadataUpdate).empty;
        defer updates.deinit(self.alloc);
        try updates.append(self.alloc, .{
            .replica_intent = .{
                .upsert = .{
                    .record = local_record,
                    .peer_node_ids = peer_node_ids,
                },
            },
        });
        for (cluster_peers, 0..) |peer, index| {
            try updates.append(self.alloc, .{
                .peer_route = .{
                    .upsert = .{
                        .group_id = metadata_group_id,
                        .node_id = peer.node_id,
                        .endpoints = route_endpoints[index][0..],
                    },
                },
            });
        }

        try self.server.svc.raft.submitBatch(updates.items);
        _ = try self.server.svc.syncPending();
    }

    pub fn bootstrapLocal(self: *Server, metadata_group_id: u64, local_node_id: u64) !void {
        if (self.server.svc.raft.host.status(metadata_group_id) == .absent) {
            _ = try self.server.svc.ensureMetadataReplica(.{
                .group_id = metadata_group_id,
                .replica_id = 1,
                .local_node_id = local_node_id,
                .bootstrap_mode = .empty,
            });
        }
        try self.server.campaignMetadataGroup();
        const observe_local_replica_root = self.server.svc.observe_local_replica_root;
        self.server.svc.observe_local_replica_root = false;
        defer self.server.svc.observe_local_replica_root = observe_local_replica_root;
        try self.server.runRound();
        self.refreshMetadataRaftStorageDiagnostics();
        _ = try self.server.svc.ensureMetadataReplica(.{
            .group_id = metadata_group_id,
            .replica_id = 1,
            .local_node_id = local_node_id,
            .bootstrap_mode = .persisted,
        });
    }

    pub fn runRound(self: *Server) !void {
        try self.server.runRound();
        self.refreshMetadataRaftStorageDiagnostics();
    }

    pub fn runCdcRound(self: *Server) !void {
        try self.server.runCdcRound();
    }

    pub fn campaignMetadataGroupIfBootstrapLeaderless(self: *Server, metadata_group_id: u64, local_node_id: u64) !bool {
        const raft_status = self.server.svc.raft.host.http_host.host.raftStatus(metadata_group_id);
        if (!metadataRaftStatusShouldBootstrapCampaign(raft_status, local_node_id)) return false;
        try self.server.campaignMetadataGroup();
        return true;
    }

    pub fn status(self: *Server) !antfly.metadata_api.MetadataStatus {
        return try self.server.status();
    }

    pub fn setLocalReplicaRootReconcileHook(self: *Server, hook: ?antfly.metadata_service.LocalReplicaRootReconcileHook) void {
        self.server.setLocalReplicaRootReconcileHook(hook);
    }

    pub fn setLocalReplicaRootReconcilePermitHook(self: *Server, hook: ?antfly.metadata_service.LocalReplicaRootReconcilePermitHook) void {
        self.server.setLocalReplicaRootReconcilePermitHook(hook);
    }

    pub fn setCdcWriteSource(self: *Server, source: antfly.public_api.TableWriteSource) void {
        self.server.setCdcWriteSource(source);
    }

    pub fn metadataHttpService(self: *Server) *antfly.metadata_service.MetadataHttpService {
        return self.server.svc;
    }

    pub fn metadataRaftStorageDiagnostics(self: *Server) MetadataRaftStorageDiagnostics {
        return self.raft_storage_diagnostics.load();
    }

    fn refreshMetadataRaftStorageDiagnostics(self: *Server) void {
        self.raft_storage_diagnostics.store(self.collectMetadataRaftStorageDiagnostics());
    }

    fn collectMetadataRaftStorageDiagnostics(self: *Server) MetadataRaftStorageDiagnostics {
        if (self.server.svc.raft.host.owned_wal_replica_provider) |provider| {
            const stats = provider.diagnostics();
            return .{
                .groups = stats.groups,
                .entries = stats.entries,
                .entry_capacity = stats.entry_capacity,
                .entry_payload_bytes = stats.entry_payload_bytes,
                .estimated_bytes = stats.estimated_bytes,
                .max_entries_per_group = stats.max_entries_per_group,
                .min_first_index = stats.min_first_index,
                .max_last_index = stats.max_last_index,
                .max_snapshot_index = stats.max_snapshot_index,
                .storage_compactions = stats.storage_compactions,
            };
        }
        const storage = self.store.diagnostics();
        return .{
            .groups = if (storage.entries > 0 or storage.snapshot_index > 0) 1 else 0,
            .entries = storage.entries,
            .entry_capacity = storage.entry_capacity,
            .entry_payload_bytes = storage.entry_payload_bytes,
            .estimated_bytes = storage.estimated_bytes,
            .max_entries_per_group = storage.entries,
            .min_first_index = storage.first_index,
            .max_last_index = storage.last_index,
            .max_snapshot_index = storage.snapshot_index,
        };
    }

    pub fn baseUri(self: *Server, alloc: std.mem.Allocator) ![]u8 {
        return try self.server.baseUri(alloc);
    }

    pub fn adminBaseUri(self: *Server, alloc: std.mem.Allocator) ![]u8 {
        return try self.server.adminBaseUri(alloc);
    }
};

fn indexOfClusterPeer(cluster_peers: []const MetadataClusterPeer, node_id: u64) ?usize {
    for (cluster_peers, 0..) |peer, index| {
        if (peer.node_id == node_id) return index;
    }
    return null;
}

fn metadataClusterPreferredCampaigner(cluster_peers: []const MetadataClusterPeer, local_node_id: u64) bool {
    if (cluster_peers.len == 0) return true;
    var min_node_id = cluster_peers[0].node_id;
    for (cluster_peers[1..]) |peer| {
        if (peer.node_id < min_node_id) min_node_id = peer.node_id;
    }
    return local_node_id == min_node_id;
}

fn metadataRaftStatusIsVoter(status: raft_engine.core.Status, local_node_id: u64) bool {
    for (status.conf_state.voters) |node_id| {
        if (node_id == local_node_id) return true;
    }
    return false;
}

fn metadataRaftStatusShouldBootstrapCampaign(status: ?raft_engine.core.Status, local_node_id: u64) bool {
    const raft_status = status orelse return false;
    if (raft_status.soft.leader_id != null) return false;
    switch (raft_status.soft.role) {
        .follower, .pre_candidate, .candidate => {},
        .leader => return false,
    }
    return metadataRaftStatusIsVoter(raft_status, local_node_id);
}

fn metadataBootstrapCampaignRetryIntervalNs(tick_ms: u64) u64 {
    return @max(
        metadata_bootstrap_campaign_retry_min_interval_ns,
        tick_ms * std.time.ns_per_ms * metadata_raft_election_max_ticks * 2,
    );
}

fn allocMetadataPeerNodeIds(
    alloc: std.mem.Allocator,
    local_node_id: u64,
    cluster_peers: []const MetadataClusterPeer,
) ![]u64 {
    if (cluster_peers.len == 0) return &.{};
    if (indexOfClusterPeer(cluster_peers, local_node_id) == null) return error.MissingLocalMetadataPeer;
    var out = try alloc.alloc(u64, cluster_peers.len);
    errdefer alloc.free(out);
    for (cluster_peers, 0..) |peer, index| out[index] = peer.node_id;
    std.mem.sort(u64, out, {}, comptime std.sort.asc(u64));
    return out;
}

pub fn run(init: std.process.Init) !void {
    const alloc = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly_metadata";
    return try runFromIterator(init, argv0, &args);
}

pub fn runFromIterator(
    init: std.process.Init,
    argv0: []const u8,
    args: *std.process.Args.Iterator,
) !void {
    const alloc = init.gpa;
    var cli = try parseCli(alloc, args);
    defer cli.deinit(alloc);
    if (cli.help) {
        printUsage(argv0);
        return;
    }

    var secret_store: antfly.common.secrets.FileStore = undefined;
    var secret_store_initialized = false;
    defer if (secret_store_initialized) secret_store.deinit();

    if (cli.secret_store_paths.items.len > 0) {
        secret_store = try initLayeredSecretStore(alloc, cli.secret_store_paths.items);
        secret_store_initialized = true;
    }

    var loaded_config: ?antfly.common.config.Config = if (cli.config_path) |config_path|
        try antfly.common.config.loadFromPathWithSecrets(
            alloc,
            config_path,
            if (secret_store_initialized) &secret_store else null,
        )
    else
        null;
    defer if (loaded_config) |*cfg| cfg.deinit();

    const data_dir = try resolveLocalBaseDir(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer alloc.free(data_dir);
    try antfly.common.data_format.ensureCompatible(alloc, data_dir);

    const resolved = try resolvePaths(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer resolved.deinit(alloc);
    const auth_enabled = resolveAuthEnabled(cli, if (loaded_config) |*cfg| cfg else null);
    const trusted_principal_secret = try resolveTrustedPrincipalSecret(
        alloc,
        if (secret_store_initialized) &secret_store else null,
    );
    defer if (trusted_principal_secret) |value| alloc.free(value);
    const effective_auth_enabled = auth_enabled or trusted_principal_secret != null;

    var setup_io = std.Io.Threaded.init(alloc, .{ .stack_size = setup_io_thread_stack_size });
    defer setup_io.deinit();
    try ensureDirPath(setup_io.io(), resolved.replica_root_dir);
    try ensureParent(setup_io.io(), resolved.replica_catalog_path);
    try ensureDirPath(setup_io.io(), resolved.snapshot_root_dir);
    try fs_paths.createDirPathPortable(setup_io.io(), resolved.auth_store_root_dir);

    var active_audio_runtime = try antfly.common.audio_runtime.ActiveRuntime.init(
        alloc,
        init.io,
        if (loaded_config) |*cfg| cfg else null,
    );
    defer active_audio_runtime.deinit();

    var auth_backend: ?antfly.lsm_backend.BackendHandle = null;
    var auth_runtime: ?antfly.storage_backend_erased.NamespaceStore = null;
    var auth_user_store: ?antfly.usermgr.StorageUserStore = null;
    var auth_casbin_store: ?antfly.usermgr.StorageCasbinAdapter = null;
    var user_manager: ?antfly.usermgr.UserManager = null;
    if (auth_enabled) {
        auth_backend = try antfly.lsm_backend.BackendHandle.open(alloc, resolved.auth_store_root_dir, .{});
        errdefer if (auth_backend) |*backend| backend.close();
        auth_runtime = try auth_backend.?.backend.runtimeNamespaceStore(alloc);
        errdefer if (auth_runtime) |*runtime| runtime.deinit();
        auth_user_store = antfly.usermgr.StorageUserStore.init(alloc, auth_runtime.?);
        auth_casbin_store = antfly.usermgr.StorageCasbinAdapter.init(alloc, auth_runtime.?);
        user_manager = try antfly.usermgr.UserManager.init(
            alloc,
            auth_user_store.?.iface(),
            try antfly.usermgr.initDefaultEnforcer(alloc, auth_casbin_store.?.iface()),
        );
        errdefer if (user_manager) |*manager| manager.deinit();
        try antfly.usermgr.ensureDefaultAdminUser(&user_manager.?);
    }
    defer if (user_manager) |*manager| manager.deinit();
    defer if (auth_runtime) |*runtime| runtime.deinit();
    defer if (auth_backend) |*backend| backend.close();

    const trusted_principal_issuer = try resolveTrustedPrincipalIssuer(
        alloc,
        if (secret_store_initialized) &secret_store else null,
    );
    defer if (trusted_principal_issuer) |value| alloc.free(value);

    const local_node_id = cli.local_node_id orelse 1;
    const metadata_group_id = group_ids.main_metadata_group_id;
    const cluster_peers = try resolveMetadataClusterPeers(alloc, cli.cluster_json, if (loaded_config) |*cfg| cfg else null);
    defer freeMetadataClusterPeers(alloc, cluster_peers);
    if (cli.join) return error.UnsupportedMetadataJoin;
    const listener = resolveRaftListener(cli, if (loaded_config) |*cfg| cfg else null);
    const admin_listener = resolveAdminListener(cli, if (loaded_config) |*cfg| cfg else null, local_node_id, listener.bind_host);

    var server = try Server.init(alloc, .{
        .local_node_id = local_node_id,
        .metadata_group_id = metadata_group_id,
        .metadata_cluster_peers = cluster_peers,
        .replica_root_dir = resolved.replica_root_dir,
        .replica_catalog_path = resolved.replica_catalog_path,
        .snapshot_root_dir = resolved.snapshot_root_dir,
        .bind_host = listener.bind_host,
        .bind_port = listener.bind_port,
        .admin_bind_host = admin_listener.bind_host,
        .admin_bind_port = admin_listener.bind_port,
        .reconciler_config = shardAllocationReconcilerConfig(if (loaded_config) |*cfg| cfg else null),
        .secret_store = if (secret_store_initialized) &secret_store else null,
        .api_server_cfg = .{
            .auth_enabled = effective_auth_enabled,
            .trusted_principal_secret = trusted_principal_secret,
            .trusted_principal_issuer = trusted_principal_issuer,
            .user_manager = if (user_manager) |*manager| manager else null,
            .secret_store = if (secret_store_initialized) &secret_store else null,
            .remote_content = if (loaded_config) |*cfg| if (cfg.remote_content) |*remote_content| remote_content else null else null,
            .inference_api_key = if (loaded_config) |*cfg| if (cfg.inference.api_key) |value| value else null else null,
            .extension_package_store_dir = resolved.extension_package_store_dir,
            .node_config = if (loaded_config) |*cfg| cfg else null,
        },
    });
    defer server.deinit();
    try server.start();
    try server.bootstrapCluster(metadata_group_id, local_node_id, cluster_peers);
    const synced_extension_packages = try server.server.svc.syncExtensionPackageStore(setup_io.io(), resolved.extension_package_store_dir);
    if (synced_extension_packages > 0) {
        std.log.info("metadata synced extension package store path={s} packages={d}", .{ resolved.extension_package_store_dir, synced_extension_packages });
    }

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);
    std.debug.print("metadata raft api listening on {s}\n", .{base_uri});

    const admin_uri = try server.adminBaseUri(alloc);
    defer alloc.free(admin_uri);
    std.debug.print("metadata admin api listening on {s}\n", .{admin_uri});

    var metadata_health = HealthSource{ .server = &server };
    const health_enabled = cli.health_enabled orelse if (loaded_config) |*cfg| cfg.health_enabled else true;
    const health_port = if (health_enabled)
        cli.health_port orelse if (loaded_config) |*cfg| cfg.health_port else antfly.common.config.default_health_port
    else
        null;
    const health_server = try antfly.common.health_server.HealthServer.startIfConfigured(
        alloc,
        "metadata",
        health_port,
        metadata_health.readiness(),
        metadata_health.metricsWriter(),
    );
    defer if (health_server) |hs| hs.deinit();

    const tick_ms = cli.tick_ms orelse 100;
    var req = std.posix.timespec{
        .sec = @intCast(tick_ms / std.time.ms_per_s),
        .nsec = @intCast((tick_ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    const preferred_bootstrap_campaigner = metadataClusterPreferredCampaigner(cluster_peers, local_node_id);
    const bootstrap_campaign_retry_interval_ns = metadataBootstrapCampaignRetryIntervalNs(tick_ms);
    var last_bootstrap_campaign_retry_ns = platform_time.monotonicNs();
    while (true) {
        if (preferred_bootstrap_campaigner) {
            const now_ns = platform_time.monotonicNs();
            if (last_bootstrap_campaign_retry_ns == 0 or
                now_ns -| last_bootstrap_campaign_retry_ns >= bootstrap_campaign_retry_interval_ns)
            {
                if (try server.campaignMetadataGroupIfBootstrapLeaderless(metadata_group_id, local_node_id)) {
                    std.log.warn("metadata bootstrap campaign retry node_id={}", .{local_node_id});
                    last_bootstrap_campaign_retry_ns = now_ns;
                }
            }
        }
        const run_round_start_ns = platform_time.monotonicNs();
        try server.runRound();
        const run_round_elapsed_ns = platform_time.monotonicNs() -| run_round_start_ns;
        if (run_round_elapsed_ns > antfly.metadata_service.metadata_run_round_slow_threshold_ns) {
            std.log.warn("metadata runRound slow elapsed_ms={d}", .{@divTrunc(run_round_elapsed_ns, std.time.ns_per_ms)});
        }
        const cdc_round_start_ns = platform_time.monotonicNs();
        try server.runCdcRound();
        const cdc_round_elapsed_ns = platform_time.monotonicNs() -| cdc_round_start_ns;
        if (cdc_round_elapsed_ns > std.time.ns_per_s) {
            std.log.warn("metadata runCdcRound slow elapsed_ms={d}", .{@divTrunc(cdc_round_elapsed_ns, std.time.ns_per_ms)});
        }
        const err = std.posix.errno(std.posix.system.nanosleep(&req, &req));
        switch (err) {
            .SUCCESS => {},
            .INTR => continue,
            else => return std.posix.unexpectedErrno(err),
        }
    }
}

fn parseCli(alloc: std.mem.Allocator, args: *std.process.Args.Iterator) !CliConfig {
    var cfg = CliConfig{};
    errdefer cfg.deinit(alloc);
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            cfg.help = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--config")) {
            cfg.config_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--id")) {
            cfg.local_node_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--raft-host")) {
            cfg.raft_host = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--raft-port")) {
            cfg.raft_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--api-host")) {
            cfg.api_host = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--api-port")) {
            cfg.api_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--cluster")) {
            cfg.cluster_json = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--join")) {
            cfg.join = true;
            continue;
        }
        if (std.mem.eql(u8, arg, "--health-port")) {
            cfg.health_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--health")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.health_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--health=")) {
            cfg.health_enabled = parseBoolFlag(arg["--health=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--tick-ms")) {
            cfg.tick_ms = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--data-dir")) {
            cfg.data_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--replica-root-dir")) {
            cfg.replica_root_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--replica-catalog-path")) {
            cfg.replica_catalog_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--snapshot-root-dir")) {
            cfg.snapshot_root_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--extension-package-store")) {
            cfg.extension_package_store_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--secret-store-path")) {
            try cfg.secret_store_paths.append(alloc, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--auth")) {
            cfg.auth_enabled = parseBoolFlag(args.next() orelse return error.InvalidArguments) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--auth=")) {
            cfg.auth_enabled = parseBoolFlag(arg["--auth=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        return error.InvalidArguments;
    }
    return cfg;
}

fn initLayeredSecretStore(
    alloc: std.mem.Allocator,
    raw_paths: []const []const u8,
) !antfly.common.secrets.FileStore {
    var normalized_paths: std.ArrayListUnmanaged([]const u8) = .empty;
    defer {
        for (normalized_paths.items) |path| alloc.free(path);
        normalized_paths.deinit(alloc);
    }
    for (raw_paths) |raw_path| {
        const normalized_path = try normalizeResolvedPathAlloc(alloc, raw_path);
        errdefer alloc.free(normalized_path);
        try normalized_paths.append(alloc, normalized_path);
    }
    return try antfly.common.secrets.FileStore.initLayered(alloc, normalized_paths.items);
}

fn resolveLocalBaseDir(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) ![]u8 {
    if (cli.data_dir) |path| return try normalizeResolvedPathAlloc(alloc, path);
    return try antfly.common.config.resolveLocalBaseDir(alloc, cfg);
}

fn resolvePaths(alloc: std.mem.Allocator, cli: CliConfig, cfg: ?*const antfly.common.config.Config) !ResolvedPaths {
    const local_base = try resolveLocalBaseDir(alloc, cli, cfg);
    defer alloc.free(local_base);
    const metadata_base = try std.fmt.allocPrint(alloc, "{s}/metadata", .{local_base});
    defer alloc.free(metadata_base);

    const replica_root_dir = if (cli.replica_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/replicas", .{metadata_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_root_dir);
    const replica_catalog_path = if (cli.replica_catalog_path) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/catalog.txt", .{metadata_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_catalog_path);
    const snapshot_root_dir = if (cli.snapshot_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{metadata_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(snapshot_root_dir);
    const auth_store_root_dir = blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/auth", .{metadata_base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(auth_store_root_dir);
    const extension_package_store_dir = try resolveExtensionPackageStoreDir(alloc, cli.extension_package_store_dir, local_base);
    errdefer alloc.free(extension_package_store_dir);

    return .{
        .replica_root_dir = replica_root_dir,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root_dir,
        .auth_store_root_dir = auth_store_root_dir,
        .extension_package_store_dir = extension_package_store_dir,
    };
}

fn resolveExtensionPackageStoreDir(
    alloc: std.mem.Allocator,
    cli_path: ?[]const u8,
    local_base: []const u8,
) ![]u8 {
    const env_var_z = try alloc.dupeZ(u8, antfly.extensions.wasmtime_runtime.package_store_env);
    defer alloc.free(env_var_z);
    return try resolveExtensionPackageStoreDirWithEnv(
        alloc,
        cli_path,
        local_base,
        platform.env.getenvSlice(env_var_z),
    );
}

fn resolveExtensionPackageStoreDirWithEnv(
    alloc: std.mem.Allocator,
    cli_path: ?[]const u8,
    local_base: []const u8,
    env_path: ?[]const u8,
) ![]u8 {
    if (cli_path) |path| return try normalizeResolvedPathAlloc(alloc, path);
    if (env_path) |path| {
        if (std.mem.trim(u8, path, " \t\r\n").len > 0) {
            return try normalizeResolvedPathAlloc(alloc, path);
        }
    }

    const raw = try std.fmt.allocPrint(alloc, "{s}/extensions", .{local_base});
    defer alloc.free(raw);
    return try normalizeResolvedPathAlloc(alloc, raw);
}

fn normalizeResolvedPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) return try alloc.dupe(u8, path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const resolved_z = std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), path, alloc) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => null,
        else => return err,
    };
    if (resolved_z) |resolved| return resolved[0..resolved.len];

    return try alloc.dupe(u8, path);
}

pub fn resolveListener(bind_host: ?[]const u8, bind_port: ?u16, cfg: ?*const antfly.common.config.Config) ListenerConfig {
    _ = cfg;
    if (bind_host != null or bind_port != null) {
        return .{
            .bind_host = bind_host orelse "127.0.0.1",
            .bind_port = bind_port orelse 0,
        };
    }
    return .{ .bind_host = "127.0.0.1", .bind_port = 0 };
}

fn resolveRaftListener(cli: CliConfig, cfg: ?*const antfly.common.config.Config) ListenerConfig {
    if (cli.raft_host != null or cli.raft_port != null) {
        return .{
            .bind_host = cli.raft_host orelse "127.0.0.1",
            .bind_port = cli.raft_port orelse 0,
        };
    }
    if (cfg) |loaded| {
        if (cli.local_node_id) |node_id| {
            if (metadataClusterPeerUrl(loaded, node_id)) |url| {
                return parseHostPort(url) catch .{ .bind_host = "127.0.0.1", .bind_port = 0 };
            }
        }
    }
    return resolveListener(null, null, cfg);
}

fn resolveAdminListener(
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
    local_node_id: u64,
    fallback_host: []const u8,
) ListenerConfig {
    if (cli.api_host != null or cli.api_port != null) {
        return .{
            .bind_host = cli.api_host orelse fallback_host,
            .bind_port = cli.api_port orelse 0,
        };
    }
    if (cfg) |loaded| {
        if (metadataOrchestrationPeerUrl(loaded, local_node_id)) |url| {
            return parseHostPort(url) catch .{ .bind_host = fallback_host, .bind_port = 0 };
        }
    }
    return .{ .bind_host = fallback_host, .bind_port = 0 };
}

fn shardAllocationReconcilerConfig(cfg: ?*const antfly.common.config.Config) antfly.metadata.reconciler.Reconciler.Config {
    if (cfg) |loaded| {
        return .{
            .max_shard_size_bytes = loaded.shard_allocation.max_shard_size_bytes,
            .min_shard_size_bytes = loaded.shard_allocation.min_shard_size_bytes,
            .min_shards_per_table = loaded.shard_allocation.min_shards_per_table,
            .max_shards_per_table = loaded.shard_allocation.max_shards_per_table,
            .disable_shard_alloc = loaded.shard_allocation.disable_shard_alloc,
            .auto_range_transition_per_table_limit = loaded.shard_allocation.auto_range_transition_per_table_limit,
            .auto_range_transition_cluster_limit = loaded.shard_allocation.auto_range_transition_cluster_limit,
            .shard_cooldown_millis = loaded.shard_allocation.shard_cooldown_millis,
            .min_shard_merge_age_millis = loaded.shard_allocation.min_shard_merge_age_millis,
        };
    }
    return .{};
}

pub fn parseHostPort(base_uri: []const u8) !ListenerConfig {
    const scheme_pos = std.mem.indexOf(u8, base_uri, "://") orelse return error.InvalidArguments;
    const host_port = base_uri[scheme_pos + 3 ..];
    const path_pos = std.mem.indexOfScalar(u8, host_port, '/');
    const authority = if (path_pos) |pos| host_port[0..pos] else host_port;
    const colon_pos = std.mem.lastIndexOfScalar(u8, authority, ':') orelse return error.InvalidArguments;
    const host = authority[0..colon_pos];
    const port = try std.fmt.parseInt(u16, authority[colon_pos + 1 ..], 10);
    if (host.len == 0) return error.InvalidArguments;
    return .{ .bind_host = host, .bind_port = port };
}

fn resolveMetadataClusterPeers(
    alloc: std.mem.Allocator,
    cluster_json: ?[]const u8,
    cfg: ?*const antfly.common.config.Config,
) ![]MetadataClusterPeer {
    if (cluster_json) |raw| return try parseMetadataClusterJson(alloc, raw);
    if (cfg) |loaded| return try metadataClusterPeersFromConfig(alloc, loaded);
    return &.{};
}

pub fn metadataClusterPeersFromConfig(
    alloc: std.mem.Allocator,
    cfg: *const antfly.common.config.Config,
) ![]MetadataClusterPeer {
    if (cfg.metadata.raft_urls.len == 0) return &.{};
    var out = try alloc.alloc(MetadataClusterPeer, cfg.metadata.raft_urls.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |peer| alloc.free(peer.raft_url);
        alloc.free(out);
    }
    for (cfg.metadata.raft_urls, 0..) |entry, index| {
        out[index] = .{
            .node_id = entry.node_id,
            .raft_url = try alloc.dupe(u8, entry.url),
        };
        initialized += 1;
    }
    return out;
}

pub fn metadataClusterPeerUrl(
    cfg: *const antfly.common.config.Config,
    node_id: u64,
) ?[]const u8 {
    for (cfg.metadata.raft_urls) |entry| {
        if (entry.node_id == node_id) return entry.url;
    }
    return null;
}

pub fn metadataOrchestrationPeerUrl(
    cfg: *const antfly.common.config.Config,
    node_id: u64,
) ?[]const u8 {
    for (cfg.metadata.orchestration_urls) |entry| {
        if (entry.node_id == node_id) return entry.url;
    }
    return null;
}

pub fn parseMetadataClusterJson(alloc: std.mem.Allocator, raw: []const u8) ![]MetadataClusterPeer {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    const object = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidArguments,
    };
    if (object.count() == 0) return &.{};

    var out = try alloc.alloc(MetadataClusterPeer, object.count());
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |peer| alloc.free(peer.raft_url);
        alloc.free(out);
    }

    var it = object.iterator();
    while (it.next()) |entry| {
        const url = switch (entry.value_ptr.*) {
            .string => |value| value,
            else => return error.InvalidArguments,
        };
        out[initialized] = .{
            .node_id = try parseMetadataNodeId(entry.key_ptr.*),
            .raft_url = try alloc.dupe(u8, url),
        };
        initialized += 1;
    }
    return out;
}

fn parseMetadataNodeId(raw: []const u8) !u64 {
    return std.fmt.parseInt(u64, raw, 10) catch
        std.fmt.parseInt(u64, raw, 16) catch
        error.InvalidArguments;
}

pub fn freeMetadataClusterPeers(alloc: std.mem.Allocator, peers: []MetadataClusterPeer) void {
    for (peers) |peer| alloc.free(peer.raft_url);
    if (peers.len > 0) alloc.free(peers);
}

fn ensureDirPath(io: std.Io, dir_path: []const u8) !void {
    try fs_paths.createDirPathPortable(io, dir_path);
}

fn ensureParent(io: std.Io, file_path: []const u8) !void {
    if (std.fs.path.dirname(file_path)) |parent| {
        var dir = std.Io.Dir.cwd().openDir(io, parent, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                try fs_paths.createDirPathPortable(io, parent);
                return;
            },
            else => return err,
        };
        dir.close(io);
    }
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\Usage: {s} [options]
        \\
        \\Options:
        \\  --config <path>                Common Antfly config file
        \\  --id <node-id>                 Metadata node id (default: 1)
        \\  --raft-host <host>             Metadata raft bind host (default: 127.0.0.1)
        \\  --raft-port <port>             Metadata raft bind port (default: 0)
        \\  --api-host <host>              Metadata admin API bind host (default: raft host)
        \\  --api-port <port>              Metadata admin API bind port (default: 0)
        \\  --cluster <json>               Metadata raft peer URLs, e.g. {{"1":"http://127.0.0.1:9017"}}
        \\  --join                         Join an existing metadata cluster (not yet supported)
        \\  --health <true|false>          Enable health/metrics server (default: true)
        \\  --health-port <port>           Dedicated health/metrics bind port (default: 4200)
        \\  --tick-ms <ms>                 Sleep interval while serving (default: 25)
        \\  --data-dir <path>              Local storage root for metadata data
        \\  --replica-root-dir <path>      Replica root directory
        \\  --replica-catalog-path <path>  Replica catalog file path
        \\  --snapshot-root-dir <path>     Snapshot root directory
        \\  --extension-package-store <path>
        \\                                 Extension package store directory
        \\  --secret-store-path <path>     Antfly secrets.json file path; repeat for fallback layers
        \\  --auth <true|false>            Enable public API auth on metadata (default: config)
        \\  -h, --help                     Show this help
        \\
    , .{argv0});
}

fn parseBoolFlag(raw: []const u8) ?bool {
    if (std.mem.eql(u8, raw, "true")) return true;
    if (std.mem.eql(u8, raw, "false")) return false;
    return null;
}

fn resolveAuthEnabled(cli: CliConfig, cfg: ?*const antfly.common.config.Config) bool {
    if (cli.auth_enabled) |value| return value;
    if (cfg) |loaded| return loaded.auth_enabled;
    return false;
}

fn resolveTrustedPrincipalSecret(
    alloc: std.mem.Allocator,
    secret_store: ?*antfly.common.secrets.FileStore,
) !?[]u8 {
    return try resolveMetadataRuntimeSecretValue(alloc, secret_store, trusted_principal_secret_key);
}

fn resolveTrustedPrincipalIssuer(
    alloc: std.mem.Allocator,
    secret_store: ?*antfly.common.secrets.FileStore,
) !?[]u8 {
    return try resolveMetadataRuntimeSecretValue(alloc, secret_store, trusted_principal_issuer_key);
}

fn resolveMetadataRuntimeSecretValue(
    alloc: std.mem.Allocator,
    secret_store: ?*antfly.common.secrets.FileStore,
    key: []const u8,
) !?[]u8 {
    if (secret_store) |store| {
        if (try store.getOwned(alloc, key)) |value| {
            if (std.mem.trim(u8, value, " \t\r\n").len > 0) return value;
            alloc.free(value);
            return null;
        }
        return null;
    }

    const env_var = try antfly.common.secrets.envVarForKey(alloc, key);
    defer alloc.free(env_var);
    const env_var_z = try alloc.dupeZ(u8, env_var);
    defer alloc.free(env_var_z);
    if (platform.env.getenvSlice(env_var_z)) |value| {
        const raw = try alloc.dupe(u8, value);
        if (std.mem.trim(u8, raw, " \t\r\n").len > 0) return raw;
        alloc.free(raw);
    }
    return null;
}

fn canonicalizeMetadataRuntimeValue(alloc: std.mem.Allocator, value: []u8) !?[]u8 {
    const trimmed = std.mem.trim(u8, value, " \t\r\n");
    if (trimmed.len == 0) {
        alloc.free(value);
        return null;
    }
    if (trimmed.len == value.len) return value;
    errdefer alloc.free(value);
    const canonical = try alloc.dupe(u8, trimmed);
    alloc.free(value);
    return canonical;
}

test "metadata runtime module compiles" {
    _ = run;
    _ = runFromIterator;
}

test "metadata runtime cli accepts secret and extension package store paths" {
    var argv = [_][*:0]const u8{
        "--secret-store-path",
        "/run/antfly/secrets/secrets.json",
        "--extension-package-store",
        "/opt/antfly/extensions",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", cfg.secret_store_paths.items[0]);
    try std.testing.expectEqualStrings("/opt/antfly/extensions", cfg.extension_package_store_dir.?);
}

test "metadata runtime cli accepts layered secret store paths" {
    var argv = [_][*:0]const u8{
        "--secret-store-path",
        "/run/antfly/user-secrets/secrets.json",
        "--secret-store-path",
        "/run/antfly/system-secrets/secrets.json",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), cfg.secret_store_paths.items.len);
    try std.testing.expectEqualStrings("/run/antfly/user-secrets/secrets.json", cfg.secret_store_paths.items[0]);
    try std.testing.expectEqualStrings("/run/antfly/system-secrets/secrets.json", cfg.secret_store_paths.items[1]);
}

test "metadata runtime cli accepts auth flag" {
    var argv = [_][*:0]const u8{
        "--auth=true",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqual(true, cfg.auth_enabled.?);
}

test "metadata runtime preserves trusted principal auth material bytes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-trusted-principal-secrets.json", .{tmp.sub_path});
    defer alloc.free(store_path);

    var secret_store = try antfly.common.secrets.FileStore.init(alloc, store_path);
    defer secret_store.deinit();

    var stored_secret = try secret_store.put(alloc, trusted_principal_secret_key, " shared-secret \n");
    defer stored_secret.deinit(alloc);
    var stored_issuer = try secret_store.put(alloc, trusted_principal_issuer_key, "\ttrusted-upstream ");
    defer stored_issuer.deinit(alloc);

    const secret = try resolveTrustedPrincipalSecret(alloc, &secret_store);
    defer if (secret) |value| alloc.free(value);
    const issuer = try resolveTrustedPrincipalIssuer(alloc, &secret_store);
    defer if (issuer) |value| alloc.free(value);

    try std.testing.expectEqualStrings(" shared-secret \n", secret.?);
    try std.testing.expectEqualStrings("\ttrusted-upstream ", issuer.?);
}

fn expectMetricPresent(output: []const u8, name: []const u8) !void {
    const help = try std.fmt.allocPrint(std.testing.allocator, "# HELP {s}", .{name});
    defer std.testing.allocator.free(help);
    try std.testing.expect(std.mem.indexOf(u8, output, help) != null);
}

fn metadataMemorySoakEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_METADATA_MEMORY_SOAK", false);
}

fn metadataMemorySoakEnvUsize(comptime name: [*:0]const u8, default: usize) usize {
    return platform.env.getenvUsize(name) orelse default;
}

fn printMetadataMemorySoakSample(label: []const u8, round: usize, memory: anytype, raft_storage: anytype) void {
    std.debug.print(
        "metadata-memory-soak {s} round={} process_available={} rss={} anon={} private_dirty={} footprint={} malloc_available={} malloc_allocated={} malloc_zone={} projected_cached={} projected_bytes={} projected_tables={} projected_ranges={} projected_stores={} projected_runtime_statuses={} projected_replication_statuses={} raft_groups={} raft_entries={} raft_capacity={} raft_payload_bytes={} raft_estimated_bytes={} raft_max_entries_per_group={} raft_min_first_index={} raft_max_last_index={} raft_max_snapshot_index={} raft_compactions={} ",
        .{
            label,
            round,
            memory.process.available,
            memory.process.resident_bytes,
            memory.process.anonymous_bytes,
            memory.process.private_dirty_bytes,
            memory.process.footprint_bytes,
            memory.process.malloc_available,
            memory.process.malloc_allocated_bytes,
            memory.process.malloc_zone_bytes,
            memory.projected_core_snapshot.cached,
            memory.projected_core_snapshot.estimated_bytes,
            memory.projected_core_snapshot.tables,
            memory.projected_core_snapshot.ranges,
            memory.projected_core_snapshot.stores,
            memory.projected_core_snapshot.store_runtime_statuses,
            memory.projected_core_snapshot.replication_source_statuses,
            raft_storage.groups,
            raft_storage.entries,
            raft_storage.entry_capacity,
            raft_storage.entry_payload_bytes,
            raft_storage.estimated_bytes,
            raft_storage.max_entries_per_group,
            raft_storage.min_first_index,
            raft_storage.max_last_index,
            raft_storage.max_snapshot_index,
            raft_storage.storage_compactions,
        },
    );
    std.debug.print(
        "projected_lsm_mutable={} projected_lsm_immutable={} projected_lsm_runs={} projected_lsm_wal={} json_calls={} json_peak={} json_total={} hosted_cache_present={} hosted_cache_entries={} hosted_retired={} hosted_lsm_wal={}\n",
        .{
            memory.projected_store_lsm.mutable_bytes,
            memory.projected_store_lsm.immutable_bytes,
            memory.projected_store_lsm.total_run_bytes,
            memory.projected_store_lsm.wal_retained_bytes,
            memory.json_response.calls,
            memory.json_response.peak_bytes,
            memory.json_response.bytes_total,
            memory.hosted_write_cache.present,
            memory.hosted_write_cache.cached_entries,
            memory.hosted_write_cache.retired_entries,
            memory.hosted_write_cache.lsm_wal_retained_bytes,
        },
    );
}

test "metadata runtime server uses wal replica state backend by default" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-default-wal/replicas", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-default-wal/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-default-wal/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var server = try Server.init(std.testing.allocator, .{
        .replica_root_dir = replica_root,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root,
    });
    defer server.deinit();

    try std.testing.expect(server.server.svc.raft.host.owned_wal_replica_provider != null);
    try std.testing.expect(server.server.svc.raft.host.owned_file_replica_provider == null);
}

test "metadata runtime enables bounded raft storage compaction for multi-node groups" {
    const runtime_cfg = metadataRaftRuntimeConfig();
    try std.testing.expectEqual(@as(u64, metadata_raft_retained_entries), runtime_cfg.applied_log_retained_entries);
    try std.testing.expectEqual(@as(u64, metadata_raft_compaction_min_interval_entries), runtime_cfg.applied_log_compaction_min_interval_entries);
    try std.testing.expect(!runtime_cfg.applied_log_compaction_single_node_only);

    const wal_cfg = metadataWalReplicaStateConfig();
    try std.testing.expectEqual(@as(u64, metadata_raft_retained_entries), wal_cfg.compaction_retained_entries);
    try std.testing.expectEqual(@as(u64, metadata_raft_compaction_min_interval_entries), wal_cfg.compaction_min_interval_entries);
    try std.testing.expect(!wal_cfg.compaction_single_node_only);
}

test "metadata runtime chooses one preferred bootstrap campaigner" {
    const peers = [_]MetadataClusterPeer{
        .{ .node_id = 3, .raft_url = "http://127.0.0.1:3003" },
        .{ .node_id = 1, .raft_url = "http://127.0.0.1:3001" },
        .{ .node_id = 2, .raft_url = "http://127.0.0.1:3002" },
    };

    try std.testing.expect(metadataClusterPreferredCampaigner(&.{}, 7));
    try std.testing.expect(metadataClusterPreferredCampaigner(&peers, 1));
    try std.testing.expect(!metadataClusterPreferredCampaigner(&peers, 2));
    try std.testing.expect(!metadataClusterPreferredCampaigner(&peers, 3));
}

test "metadata runtime retries bootstrap campaign only for leaderless voters" {
    var voters = [_]u64{ 1, 2, 3 };
    var status = raft_engine.core.Status{
        .id = 1,
        .group_id = group_ids.main_metadata_group_id,
        .soft = .{ .leader_id = null, .role = .follower },
        .hard = .{},
        .conf_state = .{ .voters = voters[0..] },
    };

    try std.testing.expect(metadataRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.role = .pre_candidate;
    try std.testing.expect(metadataRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.role = .candidate;
    try std.testing.expect(metadataRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.role = .leader;
    status.soft.leader_id = 1;
    try std.testing.expect(!metadataRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.role = .follower;
    status.soft.leader_id = 2;
    try std.testing.expect(!metadataRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.leader_id = null;
    try std.testing.expect(!metadataRaftStatusShouldBootstrapCampaign(status, 4));
}

test "metadata runtime scales bootstrap campaign retry interval with tick" {
    try std.testing.expectEqual(
        @as(u64, 600 * std.time.ns_per_ms),
        metadataBootstrapCampaignRetryIntervalNs(5),
    );
    try std.testing.expectEqual(
        @as(u64, 12 * std.time.ns_per_s),
        metadataBootstrapCampaignRetryIntervalNs(100),
    );
}

test "metadata runtime serves raft and admin listener requests on threaded io connections" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-admin-threaded-listener/replicas", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-admin-threaded-listener/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-admin-threaded-listener/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var server = try Server.init(std.testing.allocator, .{
        .replica_root_dir = replica_root,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root,
    });
    defer server.deinit();

    const admin_listener = server.server.owned_admin_listener orelse return error.MissingMetadataAdminListener;
    const raft_listener = server.server.svc.raft.host.http_host.listener;
    try std.testing.expect(raft_listener.cfg.serve_in_connection_threads);
    try std.testing.expectEqual(antfly.raft.default_http_listener_max_connection_threads, raft_listener.cfg.max_connection_threads);
    try std.testing.expect(admin_listener.cfg.serve_in_connection_threads);
    try std.testing.expectEqual(@as(u32, 32), admin_listener.cfg.max_connection_threads);
    try std.testing.expect(server.metadataHttpService().apiIoImpl() != null);
}

test "metadata runtime metrics expose memory ownership buckets" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-metrics/replicas", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-metrics/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-metrics/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var server = try Server.init(std.testing.allocator, .{
        .replica_root_dir = replica_root,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root,
    });
    defer server.deinit();
    try server.start();
    try server.bootstrapLocal(group_ids.main_metadata_group_id, 1);

    if (server.server.owned_admin_http_server) |admin| {
        var resp = try admin.handle(.{ .method = .GET, .uri = antfly.metadata.http_routes.Routes.status });
        defer resp.deinit(std.testing.allocator);
    }

    var health = HealthSource{ .server = &server };
    var buf: [128 * 1024]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buf);
    try health.metricsWriter().writeMetrics(&writer);
    const output = writer.buffered();

    try expectMetricPresent(output, "antfly_process_memory_available");
    try expectMetricPresent(output, "antfly_metadata_projected_core_snapshot_estimated_bytes");
    try expectMetricPresent(output, "antfly_metadata_projected_store_lsm_wal_retained_bytes");
    try expectMetricPresent(output, "antfly_metadata_json_response_calls_total");
    try expectMetricPresent(output, "antfly_metadata_raft_memory_storage_estimated_bytes");
    try expectMetricPresent(output, "antfly_metadata_hosted_write_cache_entries");
    try expectMetricPresent(output, "antfly_metadata_hosted_write_cache_lsm_wal_retained_bytes");
}

test "metadata runtime memory soak diagnostic" {
    if (!metadataMemorySoakEnabled()) return error.SkipZigTest;

    const soak_alloc = std.heap.page_allocator;
    const rounds = metadataMemorySoakEnvUsize("ANTFLY_METADATA_MEMORY_SOAK_ROUNDS", 2000);
    const sample_every = @max(@as(usize, 1), metadataMemorySoakEnvUsize("ANTFLY_METADATA_MEMORY_SOAK_SAMPLE_EVERY", 100));
    const table_count = @max(@as(usize, 1), metadataMemorySoakEnvUsize("ANTFLY_METADATA_MEMORY_SOAK_TABLES", 4));
    const ranges_per_table = @max(@as(usize, 1), metadataMemorySoakEnvUsize("ANTFLY_METADATA_MEMORY_SOAK_RANGES_PER_TABLE", 8));

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(soak_alloc, ".zig-cache/tmp/{s}/metadata-memory-soak/replicas", .{tmp.sub_path});
    defer soak_alloc.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(soak_alloc, ".zig-cache/tmp/{s}/metadata-memory-soak/catalog.txt", .{tmp.sub_path});
    defer soak_alloc.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(soak_alloc, ".zig-cache/tmp/{s}/metadata-memory-soak/snapshots", .{tmp.sub_path});
    defer soak_alloc.free(snapshot_root);

    var server = try Server.init(soak_alloc, .{
        .local_node_id = 1,
        .metadata_group_id = group_ids.main_metadata_group_id,
        .replica_root_dir = replica_root,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root,
        .observe_local_replica_root = false,
    });
    defer server.deinit();
    try server.start();
    try server.bootstrapLocal(group_ids.main_metadata_group_id, 1);

    const svc = server.metadataHttpService();
    try svc.registerNode(.{ .node_id = 1, .role = "metadata" });
    try svc.registerNode(.{ .node_id = 2, .role = "data" });
    try svc.registerNode(.{ .node_id = 3, .role = "data" });
    try svc.upsertStore(.{ .store_id = 31, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024 * 1024 * 1024, .available_bytes = 900 * 1024 * 1024 });
    try svc.upsertStore(.{ .store_id = 32, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024 * 1024 * 1024, .available_bytes = 850 * 1024 * 1024 });
    try svc.upsertStore(.{ .store_id = 33, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024 * 1024 * 1024, .available_bytes = 800 * 1024 * 1024 });

    var table_idx: usize = 0;
    while (table_idx < table_count) : (table_idx += 1) {
        const table_id = 1000 + @as(u64, @intCast(table_idx));
        const table_name = try std.fmt.allocPrint(soak_alloc, "docs_{}", .{table_idx});
        defer soak_alloc.free(table_name);
        try svc.upsertTable(.{
            .table_id = table_id,
            .name = table_name,
            .desired_replica_count = 3,
            .min_ranges = @intCast(ranges_per_table),
        });
        var range_idx: usize = 0;
        while (range_idx < ranges_per_table) : (range_idx += 1) {
            const start_key = try std.fmt.allocPrint(soak_alloc, "doc:{d:0>4}:a", .{range_idx});
            defer soak_alloc.free(start_key);
            const end_key = try std.fmt.allocPrint(soak_alloc, "doc:{d:0>4}:z", .{range_idx});
            defer soak_alloc.free(end_key);
            try svc.upsertRange(.{
                .group_id = table_id * 1000 + @as(u64, @intCast(range_idx + 1)),
                .range_id = @intCast(range_idx + 1),
                .table_id = table_id,
                .start_key = start_key,
                .end_key = end_key,
            });
        }
        try svc.upsertReplicationSourceStatus(.{
            .table_id = table_id,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = table_name,
            .cutover_mode = "exported_snapshot",
            .slot_name = "antfly_metadata_memory_soak_slot",
            .publication_name = "antfly_metadata_memory_soak_publication",
            .phase = "streaming",
            .checkpoint = "lsn:0/16B6B10",
            .stream_checkpoint = "lsn:0/16B6B10",
            .lag_records = @intCast(table_idx),
            .updated_at_ms = 1000 + @as(u64, @intCast(table_idx)),
        });
    }
    try server.runRound();

    const admin = server.server.owned_admin_http_server orelse return error.MissingMetadataAdminServer;
    printMetadataMemorySoakSample("baseline", 0, svc.memoryDiagnostics(), server.metadataRaftStorageDiagnostics());

    var round: usize = 0;
    while (round < rounds) : (round += 1) {
        const round_u64: u64 = @intCast(round);
        const available = (700 * 1024 * 1024) + ((round_u64 % 200) * 1024);
        try svc.reportStoreStatus(.{
            .store_id = 31,
            .live = true,
            .health_class = "healthy",
            .capacity_bytes = 1024 * 1024 * 1024,
            .available_bytes = available,
            .lease_pressure = @intCast(round % 100),
            .read_load = @intCast((round * 3) % 1000),
            .write_load = @intCast((round * 7) % 1000),
        });
        try svc.upsertReplicationSourceStatus(.{
            .table_id = 1000,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "docs_0",
            .cutover_mode = "exported_snapshot",
            .slot_name = "antfly_metadata_memory_soak_slot",
            .publication_name = "antfly_metadata_memory_soak_publication",
            .phase = if (round % 17 == 0) "streaming_failed" else "streaming",
            .checkpoint = "lsn:0/16B6B10",
            .stream_checkpoint = "lsn:0/16B6B10",
            .last_error = if (round % 17 == 0) "synthetic intermittent timeout" else "",
            .failure_class = if (round % 17 == 0) "retryable" else "",
            .lag_records = round_u64 % 1024,
            .lag_millis = round_u64 % 4096,
            .consecutive_failures = if (round % 17 == 0) 1 else 0,
            .last_source_commit_at_ms = 2000 + round_u64,
            .last_success_at_ms = 3000 + round_u64,
            .last_change_applied_at_ms = 4000 + round_u64,
            .updated_at_ms = 5000 + round_u64,
        });
        try server.runRound();

        var status_resp = try admin.handle(.{ .method = .GET, .uri = antfly.metadata.http_routes.Routes.status });
        defer status_resp.deinit(server.alloc);
        var snapshot_resp = try admin.handle(.{ .method = .GET, .uri = antfly.metadata.http_routes.Routes.admin_snapshot });
        defer snapshot_resp.deinit(server.alloc);

        if ((round + 1) % sample_every == 0) {
            printMetadataMemorySoakSample("sample", round + 1, svc.memoryDiagnostics(), server.metadataRaftStorageDiagnostics());
        }
    }
    printMetadataMemorySoakSample("final", rounds, svc.memoryDiagnostics(), server.metadataRaftStorageDiagnostics());
}

test "metadata runtime prefers common config raft url for local id when cli bind is absent" {
    const alloc = std.testing.allocator;
    const raft_urls = try alloc.alloc(antfly.common.config.Config.MetadataConfig.NodeUrl, 1);
    raft_urls[0] = .{ .node_id = 7, .url = try alloc.dupe(u8, "http://127.0.0.1:7011") };
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .metadata = .{
            .raft_urls = raft_urls,
        },
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "antflydb"),
        },
    };
    defer cfg.deinit();

    const resolved = resolveRaftListener(.{ .local_node_id = 7 }, &cfg);
    try std.testing.expectEqualStrings("127.0.0.1", resolved.bind_host);
    try std.testing.expectEqual(@as(u16, 7011), resolved.bind_port);
}

test "metadata runtime resolves paths from common storage base dir" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .storage = .{
            .local_base_dir = try alloc.dupe(u8, "/tmp/antflydb"),
        },
    };
    defer cfg.deinit();

    const resolved = try resolvePaths(alloc, .{}, &cfg);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/tmp/antflydb/metadata/replicas", resolved.replica_root_dir);
    try std.testing.expectEqualStrings("/tmp/antflydb/metadata/catalog.txt", resolved.replica_catalog_path);
    try std.testing.expectEqualStrings("/tmp/antflydb/metadata/snapshots", resolved.snapshot_root_dir);
    try std.testing.expectEqualStrings("/tmp/antflydb/metadata/auth", resolved.auth_store_root_dir);
    try std.testing.expectEqualStrings("/tmp/antflydb/extensions", resolved.extension_package_store_dir);
}

test "metadata runtime resolves explicit extension package store path" {
    const alloc = std.testing.allocator;
    const resolved = try resolvePaths(alloc, .{
        .data_dir = "/tmp/antflydb",
        .extension_package_store_dir = "/opt/antfly/extension-store",
    }, null);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/opt/antfly/extension-store", resolved.extension_package_store_dir);
}

test "metadata runtime resolves extension package store env before local default" {
    const alloc = std.testing.allocator;

    const env_resolved = try resolveExtensionPackageStoreDirWithEnv(alloc, null, "/tmp/antflydb", "/antfly-extension-env");
    defer alloc.free(env_resolved);
    try std.testing.expectEqualStrings("/antfly-extension-env", env_resolved);

    const cli_resolved = try resolveExtensionPackageStoreDirWithEnv(alloc, "/antfly-cli-extensions", "/tmp/antflydb", "/antfly-extension-env");
    defer alloc.free(cli_resolved);
    try std.testing.expectEqualStrings("/antfly-cli-extensions", cli_resolved);
}

test "metadata runtime derives reconciler config from common shard allocation settings" {
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(std.testing.allocator),
        .transcribers = antfly.transcribing.Registry.init(std.testing.allocator),
        .readers = antfly.readers.Registry.init(std.testing.allocator),
        .text_to_speech = antfly.synthesizing.Registry.init(std.testing.allocator),
        .shard_allocation = .{
            .max_shard_size_bytes = 2048,
            .min_shard_size_bytes = 512,
            .min_shards_per_table = 2,
            .max_shards_per_table = 9,
            .disable_shard_alloc = true,
            .auto_range_transition_per_table_limit = 3,
            .auto_range_transition_cluster_limit = 5,
            .shard_cooldown_millis = 90000,
            .min_shard_merge_age_millis = 180000,
        },
    };
    defer cfg.deinit();

    const derived = shardAllocationReconcilerConfig(&cfg);
    try std.testing.expectEqual(@as(u64, 2048), derived.max_shard_size_bytes);
    try std.testing.expectEqual(@as(u64, 512), derived.min_shard_size_bytes);
    try std.testing.expectEqual(@as(u32, 2), derived.min_shards_per_table);
    try std.testing.expectEqual(@as(u32, 9), derived.max_shards_per_table);
    try std.testing.expect(derived.disable_shard_alloc);
    try std.testing.expectEqual(@as(u32, 3), derived.auto_range_transition_per_table_limit);
    try std.testing.expectEqual(@as(u32, 5), derived.auto_range_transition_cluster_limit);
    try std.testing.expectEqual(@as(u64, 90000), derived.shard_cooldown_millis);
    try std.testing.expectEqual(@as(u64, 180000), derived.min_shard_merge_age_millis);
}

test "metadata runtime preserves projected tables across restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-restart/replicas", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-restart/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-restart/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    {
        var server = try Server.init(std.testing.allocator, .{
            .local_node_id = 1,
            .metadata_group_id = group_ids.main_metadata_group_id,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
            .snapshot_root_dir = snapshot_root,
        });
        defer server.deinit();
        try server.start();
        try server.bootstrapLocal(group_ids.main_metadata_group_id, 1);

        try server.metadataHttpService().upsertTable(.{
            .table_id = 77,
            .name = "docs",
        });

        var rounds: usize = 0;
        while (rounds < 8) : (rounds += 1) try server.runRound();

        var snapshot = try server.metadataHttpService().adminSnapshot();
        defer server.metadataHttpService().freeAdminSnapshot(&snapshot);
        try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
        try std.testing.expectEqualStrings("docs", snapshot.tables[0].name);
    }

    {
        var server = try Server.init(std.testing.allocator, .{
            .local_node_id = 1,
            .metadata_group_id = group_ids.main_metadata_group_id,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
            .snapshot_root_dir = snapshot_root,
        });
        defer server.deinit();
        try server.start();
        try server.bootstrapLocal(group_ids.main_metadata_group_id, 1);

        var rounds: usize = 0;
        while (rounds < 8) : (rounds += 1) try server.runRound();

        var snapshot = try server.metadataHttpService().adminSnapshot();
        defer server.metadataHttpService().freeAdminSnapshot(&snapshot);
        try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
        try std.testing.expectEqualStrings("docs", snapshot.tables[0].name);
    }
}

test "metadata runtime bootstrapLocal skips local replica-root reconcile on the bootstrap round" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-bootstrap-skip/replicas", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-bootstrap-skip/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-runtime-bootstrap-skip/snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    const HookCtx = struct {
        runs: usize = 0,

        fn run(ptr: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.runs += 1;
        }
    };

    var server = try Server.init(std.testing.allocator, .{
        .local_node_id = 1,
        .metadata_group_id = group_ids.main_metadata_group_id,
        .replica_root_dir = replica_root,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root,
        .observe_local_replica_root = true,
    });
    defer server.deinit();
    try server.start();

    var hook_ctx = HookCtx{};
    server.setLocalReplicaRootReconcileHook(.{
        .ptr = &hook_ctx,
        .vtable = &.{ .run = HookCtx.run },
    });

    try server.bootstrapLocal(group_ids.main_metadata_group_id, 1);
    try std.testing.expectEqual(@as(usize, 0), hook_ctx.runs);

    var rounds: usize = 0;
    while (hook_ctx.runs == 0 and rounds < 8) : (rounds += 1) {
        try server.runRound();
    }
    try std.testing.expect(hook_ctx.runs > 0);
}
