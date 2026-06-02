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
const indexes_api = @import("../api/indexes.zig");
const json_helpers = @import("../api/json_helpers.zig");
const fs_paths = @import("../common/fs_paths.zig");
const runtime_status = @import("../api/runtime_status.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const lsm_backend_mod = @import("../storage/lsm_backend.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const change_journal_mod = @import("../storage/db/derived/change_journal.zig");
const doc_identity = @import("../storage/db/doc_identity.zig");
const data_raft_batch = @import("raft_batch.zig");
const platform_clock = @import("../platform/clock.zig");
const process_memory_mod = @import("../platform/process_memory.zig");
const platform_time = @import("../platform/time.zig");
const raft_engine = @import("raft_engine");

const health_metrics = antfly.common.health_server;
const setup_io_thread_stack_size = 1 * 1024 * 1024;
const local_group_status_cache_ttl_ms: u64 = 60 * std.time.ms_per_s;
const store_status_report_interval_ticks: usize = 40;
const store_status_heartbeat_interval_ms: u64 = 30 * std.time.ms_per_s;
const metadata_head_cache_ttl_ms: u64 = std.time.ms_per_s;
const metadata_snapshot_cache_ttl_ms: u64 = std.time.ms_per_s;
const provision_head_poll_startup_interval_ms: u64 = std.time.ms_per_s;
const provision_head_poll_interval_ms: u64 = 5 * std.time.ms_per_s;
const runtime_status_refresh_interval_ms: u64 = std.time.ms_per_s;
const runtime_status_refresh_max_db_opens_per_run: usize = 16;
const runtime_status_disk_usage_refresh_interval_ns: u64 = 30 * std.time.ns_per_s;
const auto_bulk_finish_poll_interval_ms: u64 = 250;
const provisioned_startup_catch_up_interval_ms: u64 = std.time.ms_per_s;
const data_raft_batch_leader_wait_ns: u64 = 25 * std.time.ns_per_s;
const data_raft_batch_leader_retry_sleep_ns: u64 = 50 * std.time.ns_per_ms;
const data_raft_metadata_resync_interval_ns: u64 = 500 * std.time.ns_per_ms;
const data_raft_campaign_retry_interval_ns: u64 = 500 * std.time.ns_per_ms;
const data_raft_metadata_sync_interval_ms: u64 = 250;

const CliConfig = struct {
    config_path: ?[]const u8 = null,
    bind_host: ?[]const u8 = null,
    bind_port: ?u16 = null,
    health_enabled: ?bool = null,
    health_port: ?u16 = null,
    raft_bind_host: ?[]const u8 = null,
    raft_bind_port: ?u16 = null,
    auth_enabled: ?bool = null,
    metadata_apis: std.ArrayListUnmanaged([]const u8) = .empty,
    node_id: ?u64 = null,
    store_id: ?u64 = null,
    store_role: ?[]const u8 = null,
    failure_domain: ?[]const u8 = null,
    tick_ms: ?u64 = null,
    data_dir: ?[]const u8 = null,
    replica_root_dir: ?[]const u8 = null,
    replica_catalog_path: ?[]const u8 = null,
    snapshot_root_dir: ?[]const u8 = null,
    secret_store_path: ?[]const u8 = null,
    help: bool = false,

    fn deinit(self: *CliConfig, alloc: std.mem.Allocator) void {
        self.metadata_apis.deinit(alloc);
        self.* = undefined;
    }
};

const ResolvedPaths = struct {
    replica_root_dir: []u8,
    replica_catalog_path: []u8,
    snapshot_root_dir: []u8,
    auth_store_root_dir: []u8,

    fn deinit(self: ResolvedPaths, alloc: std.mem.Allocator) void {
        alloc.free(self.replica_root_dir);
        alloc.free(self.replica_catalog_path);
        alloc.free(self.snapshot_root_dir);
        alloc.free(self.auth_store_root_dir);
    }
};

const ResolvedMetadataApiUrls = struct {
    urls: []const []const u8,

    fn deinit(self: ResolvedMetadataApiUrls, alloc: std.mem.Allocator) void {
        if (self.urls.len > 0) alloc.free(self.urls);
    }
};

fn publicApiListenerConfig(bind_host: []const u8, bind_port: u16) antfly.raft.transport.StdHttpListenerConfig {
    return .{
        .bind_host = bind_host,
        .bind_port = bind_port,
        .max_request_bytes = antfly.public_api.http_server.public_api_max_request_body_bytes,
    };
}

test "data public API listener uses public API request body limit" {
    const cfg = publicApiListenerConfig("127.0.0.1", 8080);
    try std.testing.expectEqual(antfly.public_api.http_server.public_api_max_request_body_bytes, cfg.max_request_bytes);
}

const DataDescriptorFactory = struct {
    alloc: std.mem.Allocator,
    fallback_store: *raft_engine.core.MemoryStorage,
    peer_sets: std.AutoHashMapUnmanaged(u64, []u64) = .empty,
    group_stores: std.AutoHashMapUnmanaged(u64, *raft_engine.core.MemoryStorage) = .empty,

    fn init(alloc: std.mem.Allocator, fallback_store: *raft_engine.core.MemoryStorage) DataDescriptorFactory {
        return .{ .alloc = alloc, .fallback_store = fallback_store };
    }

    fn deinit(self: *DataDescriptorFactory) void {
        var store_it = self.group_stores.valueIterator();
        while (store_it.next()) |store| {
            store.*.deinit();
            self.alloc.destroy(store.*);
        }
        self.group_stores.deinit(self.alloc);
        var it = self.peer_sets.valueIterator();
        while (it.next()) |peers| self.alloc.free(peers.*);
        self.peer_sets.deinit(self.alloc);
        self.* = undefined;
    }

    fn iface(self: *DataDescriptorFactory) antfly.raft.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn replacePeerSets(self: *DataDescriptorFactory, intents: []const antfly.raft.PlacementIntent) !void {
        var next = std.AutoHashMapUnmanaged(u64, []u64).empty;
        errdefer {
            var it = next.valueIterator();
            while (it.next()) |peers| self.alloc.free(peers.*);
            next.deinit(self.alloc);
        }

        for (intents) |intent| {
            const peers = try self.normalizedPeers(intent.record.local_node_id, intent.peer_node_ids);
            const existing = try next.fetchPut(self.alloc, intent.record.group_id, peers);
            if (existing) |old| self.alloc.free(old.value);
        }

        var old_it = self.peer_sets.valueIterator();
        while (old_it.next()) |peers| self.alloc.free(peers.*);
        self.peer_sets.deinit(self.alloc);
        self.peer_sets = next;
    }

    fn normalizedPeers(self: *DataDescriptorFactory, local_node_id: u64, peer_node_ids: []const u64) ![]u64 {
        var peers = std.ArrayListUnmanaged(u64).empty;
        errdefer peers.deinit(self.alloc);

        try appendUniqueNodeId(self.alloc, &peers, local_node_id);
        for (peer_node_ids) |node_id| try appendUniqueNodeId(self.alloc, &peers, node_id);
        std.mem.sort(u64, peers.items, {}, comptime std.sort.asc(u64));
        return try peers.toOwnedSlice(self.alloc);
    }

    fn storageForGroup(self: *DataDescriptorFactory, group_id: u64) !*raft_engine.core.MemoryStorage {
        if (self.group_stores.get(group_id)) |store| return store;
        const store = try self.alloc.create(raft_engine.core.MemoryStorage);
        errdefer self.alloc.destroy(store);
        store.* = raft_engine.core.MemoryStorage.init(self.alloc);
        try self.group_stores.put(self.alloc, group_id, store);
        return store;
    }

    fn buildDescriptor(ptr: *anyopaque, record: antfly.raft.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *DataDescriptorFactory = @ptrCast(@alignCast(ptr));
        const peer_source = self.peer_sets.get(record.group_id) orelse &[_]u64{record.local_node_id};
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, peer_source);
        errdefer self.alloc.free(peers);
        const store = try self.storageForGroup(record.group_id);
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
                    .election_tick = 5,
                    .heartbeat_tick = 1,
                    .pre_vote = true,
                    .check_quorum = true,
                    .random_seed = antfly.raft.stableRandomSeed(record.group_id, record.local_node_id),
                },
                .storage = store.storage(),
            },
            .bootstrap = bootstrap,
        };
    }

    fn freeDescriptor(_: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        antfly.raft.catalog.freeRuntimeBootstrap(alloc, &desc.bootstrap);
        alloc.free(desc.group.raft_config.peers);
    }
};

const RaftTableApplyStateMachine = struct {
    alloc: std.mem.Allocator,
    write_source: antfly.public_api.ProvisionedTableWriteSource,
    write_cache: antfly.public_api.ProvisionedTableWriteCache,
    applied_mutex: std.atomic.Mutex = .unlocked,
    applied_indexes: std.AutoHashMapUnmanaged(u64, u64) = .empty,

    fn init(
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        catalog: antfly.public_api.table_catalog.CatalogSource,
        backend_runtime: ?*backend_runtime_mod.BackendRuntime,
    ) RaftTableApplyStateMachine {
        var write_source = antfly.public_api.ProvisionedTableWriteSource.init(replica_root_dir, catalog);
        write_source.backend_runtime = backend_runtime;
        return .{
            .alloc = alloc,
            .write_source = write_source,
            .write_cache = antfly.public_api.ProvisionedTableWriteCache.init(alloc),
        };
    }

    fn deinit(self: *RaftTableApplyStateMachine) void {
        self.write_source.deinit();
        self.write_cache.deinit();
        self.applied_indexes.deinit(self.alloc);
        self.* = undefined;
    }

    fn attachProvisionedStorage(
        self: *RaftTableApplyStateMachine,
        storage: *antfly.public_api.ProvisionedGroupStorage,
    ) void {
        // Apply/write replay should not populate the shared query-side LSM
        // block cache. This keeps indexing memory separate from read caching.
        self.write_cache.lsm_cache = null;
        self.write_cache.hbc_cache = &storage.hbc_cache;
        self.write_cache.resource_manager = &storage.resource_manager;
        self.write_cache.backend_runtime = storage.backend_runtime;
        self.write_cache.antfly_provider = self.write_source.antfly_provider;
        self.write_cache.secret_store = self.write_source.secret_store;
        self.write_cache.remote_content = self.write_source.remote_content;
        self.write_source.read_cache = &storage.read_cache;
        self.write_source.write_cache = &self.write_cache;
        self.write_source.runtime_status_cache = &storage.runtime_status_cache;
        self.write_source.group_lsm_generation = storage.groupLsmGenerationSource();
        if (storage.backend_runtime) |runtime| self.write_source.backend_runtime = runtime;
    }

    fn appliedIndex(self: *RaftTableApplyStateMachine, group_id: u64) u64 {
        lockAtomic(&self.applied_mutex);
        defer self.applied_mutex.unlock();
        return self.applied_indexes.get(group_id) orelse 0;
    }

    fn setAppliedIndex(self: *RaftTableApplyStateMachine, group_id: u64, index: u64) !void {
        lockAtomic(&self.applied_mutex);
        defer self.applied_mutex.unlock();
        const existing = self.applied_indexes.get(group_id) orelse 0;
        if (index > existing) try self.applied_indexes.put(self.alloc, group_id, index);
    }

    fn stateMachine(self: *RaftTableApplyStateMachine) raft_engine.runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .apply_ready = applyReady,
            },
        };
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        committed_entries: []const raft_engine.core.Entry,
        _: []const raft_engine.core.ReadState,
    ) !void {
        const self: *RaftTableApplyStateMachine = @ptrCast(@alignCast(ptr));
        var last_index: u64 = 0;
        for (committed_entries) |entry| {
            if (entry.index > last_index) last_index = entry.index;
            if (entry.entry_type != .normal) continue;
            if (!std.mem.startsWith(u8, entry.data, "{\"table\"")) continue;
            var decoded = try data_raft_batch.decode(self.alloc, entry.data);
            defer decoded.deinit(self.alloc);
            _ = try self.write_source.applyReplicatedBatchGroupLocal(
                self.alloc,
                group_id,
                decoded.table_name,
                decoded.batch.req,
            );
        }
        if (last_index > 0) try self.setAppliedIndex(group_id, last_index);
    }
};

/// Backs the standalone data server's health/metrics endpoints. The data
/// server has no local raft, so readiness is delegated to its remote
/// metadata status source (which mirrors the existing `/readyz` logic on
/// the main API port) and metrics export a minimal up gauge.
pub const HealthSource = struct {
    data_server: *DataServer,

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
        _ = self.data_server.status_source.status() catch return false;
        return true;
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *HealthSource = @ptrCast(@alignCast(ptr));
        const runtime_summary = self.data_server.provisioned_storage.runtime_status_cache.summary();
        const read_cache_stats = self.data_server.provisioned_storage.read_cache.cacheStats();
        const write_cache_stats = self.data_server.provisioned_storage.write_cache.cacheStats();
        const auto_bulk_stats = self.data_server.write_source.autoBulkIngestStatsBestEffort();
        const fanout_metrics = antfly.public_api.table_reads.parallelFanoutMetricsSnapshot();
        const graph_fanout_metrics = antfly.public_api.distributed_graph.graphFanoutMetricsSnapshot();
        const api_request_stats = if (self.data_server.http_server) |*http_server|
            http_server.requestStats()
        else
            antfly.public_api.ApiHttpServer.RequestStats{};
        try antfly.common.health_server.appendPromMetric(
            writer,
            "antfly_data_server_up",
            "gauge",
            "Whether the data server process is running (1 = yes)",
            1,
        );
        try health_metrics.appendPromMetric(writer, "antfly_data_api_requests_total", "counter", "Requests handled by the local API server process", api_request_stats.request_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_api_first_request_elapsed_ms", "gauge", "Milliseconds from API server initialization until the first handled request", api_request_stats.first_request_elapsed_ms);
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_tables", "gauge", "Tables currently represented in the cached local runtime-status snapshot", runtime_summary.table_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_groups", "gauge", "Local provisioned groups represented in the cached local runtime-status snapshot", runtime_summary.group_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_indexes", "gauge", "Indexes represented in the cached local runtime-status snapshot", runtime_summary.index_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_active", "gauge", "Whether the runtime-status refresh worker is currently active", if (self.data_server.runtime_status_refresh_active.load(.acquire)) 1 else 0);
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_started_total", "counter", "Runtime-status refresh runs started", self.data_server.runtime_status_refresh_started.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_completed_total", "counter", "Runtime-status refresh runs completed", self.data_server.runtime_status_refresh_completed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_failed_total", "counter", "Runtime-status refresh runs that exited early with an error", self.data_server.runtime_status_refresh_failed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_last_table_count", "gauge", "Tables present in the most recent runtime-status refresh snapshot", self.data_server.runtime_status_refresh_last_table_count.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_last_group_count", "gauge", "Local groups present in the most recent runtime-status refresh snapshot", self.data_server.runtime_status_refresh_last_group_count.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_last_db_opens", "gauge", "DB opens performed by the most recent runtime-status refresh run", self.data_server.runtime_status_refresh_last_db_opens.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_last_skipped_db_opens", "gauge", "DB opens skipped by the most recent runtime-status refresh run because the refresh budget was exhausted", self.data_server.runtime_status_refresh_last_skipped_db_opens.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_last_placeholder_group_count", "gauge", "Placeholder group statuses published by the most recent runtime-status refresh run", self.data_server.runtime_status_refresh_last_placeholder_group_count.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_runtime_status_refresh_last_duration_ns", "gauge", "Duration of the most recent runtime-status refresh run in monotonic nanoseconds", self.data_server.runtime_status_refresh_last_duration_ns.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_root_refresh_active", "gauge", "Whether provisioned replica-root refresh is currently active", if (self.data_server.provisioned_root_refresh_active.load(.acquire)) 1 else 0);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_root_refresh_dirty", "gauge", "Whether provisioned replica-root refresh has pending work", if (self.data_server.provisioned_root_refresh_dirty.load(.acquire)) 1 else 0);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_root_refresh_started_total", "counter", "Provisioned replica-root refresh runs started", self.data_server.provisioned_root_refresh_started.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_root_refresh_completed_total", "counter", "Provisioned replica-root refresh runs completed", self.data_server.provisioned_root_refresh_completed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_root_refresh_failed_total", "counter", "Provisioned replica-root refresh runs failed", self.data_server.provisioned_root_refresh_failed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_root_refresh_last_duration_ns", "gauge", "Duration of the most recent provisioned replica-root refresh run in monotonic nanoseconds", self.data_server.provisioned_root_refresh_last_duration_ns.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_finish_active", "gauge", "Whether an auto bulk-ingest finish run is currently active", if (self.data_server.auto_bulk_finish_active.load(.acquire)) 1 else 0);
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_finish_started_total", "counter", "Auto bulk-ingest finish runs started", self.data_server.auto_bulk_finish_started.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_finish_completed_total", "counter", "Auto bulk-ingest finish runs completed", self.data_server.auto_bulk_finish_completed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_finish_failed_total", "counter", "Auto bulk-ingest finish runs failed", self.data_server.auto_bulk_finish_failed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_finish_lock_deferred_total", "counter", "Auto bulk-ingest finish runs deferred because the writer cache lock was busy", self.data_server.auto_bulk_finish_lock_deferred.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_finish_last_duration_ns", "gauge", "Duration of the most recent auto bulk-ingest finish run in monotonic nanoseconds", self.data_server.auto_bulk_finish_last_duration_ns.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_cached_entries", "gauge", "Cached writer DB entries visible to auto bulk-ingest finish", auto_bulk_stats.cached_entries);
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_open_entries", "gauge", "Cached writer DB entries with an open auto bulk-ingest session", auto_bulk_stats.open_entries);
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_active_leases", "gauge", "Active cached writer leases blocking auto bulk-ingest finish", auto_bulk_stats.active_leases);
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_finish_requested_entries", "gauge", "Auto bulk-ingest sessions that reached the max-window finish threshold", auto_bulk_stats.finish_requested_entries);
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_idle_expired_entries", "gauge", "Auto bulk-ingest sessions idle long enough for background finish", auto_bulk_stats.idle_expired_entries);
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_active_table_sessions", "gauge", "Explicit table bulk-ingest sessions tracked by the writer cache", auto_bulk_stats.active_bulk_sessions);
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_total_ops", "gauge", "Operations accumulated across open auto bulk-ingest sessions", auto_bulk_stats.total_ops);
        try health_metrics.appendPromMetric(writer, "antfly_data_auto_bulk_oldest_idle_ns", "gauge", "Longest idle age among open auto bulk-ingest sessions", auto_bulk_stats.oldest_idle_ns);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_query_fanout_total", "counter", "Parallel shard query fanout runs executed via std.Io", fanout_metrics.query_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_query_fanout_ns_total", "counter", "Total monotonic nanoseconds spent in parallel shard query fanout", fanout_metrics.query_parallel_ns_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_query_fanout_planned_parallel_total", "counter", "Shard query fanout requests that the planner chose to execute in parallel", fanout_metrics.query_planned_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_query_fanout_planned_sequential_total", "counter", "Shard query fanout requests that the planner chose to execute sequentially", fanout_metrics.query_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_query_fanout_planned_width_total", "counter", "Sum of planner-selected shard query fanout widths", fanout_metrics.query_planned_width_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_query_fanout_planned_width_count", "counter", "Number of shard query fanout requests contributing to the planned width total", fanout_metrics.query_planned_parallel_total + fanout_metrics.query_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_query_fanout_async_limit", "gauge", "Configured std.Io async limit for the dedicated query fanout runtime", @intFromEnum(self.data_server.query_async_limit));
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_text_stats_fanout_total", "counter", "Parallel distributed text-stats fanout runs executed via std.Io", fanout_metrics.text_stats_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_text_stats_fanout_ns_total", "counter", "Total monotonic nanoseconds spent in parallel distributed text-stats fanout", fanout_metrics.text_stats_parallel_ns_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_text_stats_fanout_planned_parallel_total", "counter", "Distributed text-stats fanout requests that the planner chose to execute in parallel", fanout_metrics.text_stats_planned_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_text_stats_fanout_planned_sequential_total", "counter", "Distributed text-stats fanout requests that the planner chose to execute sequentially", fanout_metrics.text_stats_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_text_stats_fanout_planned_width_total", "counter", "Sum of planner-selected distributed text-stats fanout widths", fanout_metrics.text_stats_planned_width_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_text_stats_fanout_planned_width_count", "counter", "Number of distributed text-stats fanout requests contributing to the planned width total", fanout_metrics.text_stats_planned_parallel_total + fanout_metrics.text_stats_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_preflight_fanout_total", "counter", "Parallel shard preflight fanout runs executed via std.Io", fanout_metrics.preflight_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_preflight_fanout_ns_total", "counter", "Total monotonic nanoseconds spent in parallel shard preflight fanout", fanout_metrics.preflight_parallel_ns_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_preflight_fanout_planned_parallel_total", "counter", "Shard preflight fanout requests that the planner chose to execute in parallel", fanout_metrics.preflight_planned_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_preflight_fanout_planned_sequential_total", "counter", "Shard preflight fanout requests that the planner chose to execute sequentially", fanout_metrics.preflight_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_preflight_fanout_planned_width_total", "counter", "Sum of planner-selected shard preflight fanout widths", fanout_metrics.preflight_planned_width_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_preflight_fanout_planned_width_count", "counter", "Number of shard preflight fanout requests contributing to the planned width total", fanout_metrics.preflight_planned_parallel_total + fanout_metrics.preflight_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_expand_fanout_total", "counter", "Parallel graph expand fanout runs executed via std.Io", graph_fanout_metrics.expand_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_expand_fanout_ns_total", "counter", "Total monotonic nanoseconds spent in parallel graph expand fanout", graph_fanout_metrics.expand_parallel_ns_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_expand_fanout_planned_parallel_total", "counter", "Graph expand requests that the planner chose to execute in parallel", graph_fanout_metrics.expand_planned_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_expand_fanout_planned_sequential_total", "counter", "Graph expand requests that the planner chose to execute sequentially", graph_fanout_metrics.expand_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_expand_fanout_planned_width_total", "counter", "Sum of planner-selected graph expand fanout widths", graph_fanout_metrics.expand_planned_width_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_expand_fanout_planned_width_count", "counter", "Number of graph expand requests contributing to the planned width total", graph_fanout_metrics.expand_planned_parallel_total + graph_fanout_metrics.expand_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_hydrate_fanout_total", "counter", "Parallel graph hydrate fanout runs executed via std.Io", graph_fanout_metrics.hydrate_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_hydrate_fanout_ns_total", "counter", "Total monotonic nanoseconds spent in parallel graph hydrate fanout", graph_fanout_metrics.hydrate_parallel_ns_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_hydrate_fanout_planned_parallel_total", "counter", "Graph hydrate requests that the planner chose to execute in parallel", graph_fanout_metrics.hydrate_planned_parallel_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_hydrate_fanout_planned_sequential_total", "counter", "Graph hydrate requests that the planner chose to execute sequentially", graph_fanout_metrics.hydrate_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_hydrate_fanout_planned_width_total", "counter", "Sum of planner-selected graph hydrate fanout widths", graph_fanout_metrics.hydrate_planned_width_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_parallel_graph_hydrate_fanout_planned_width_count", "counter", "Number of graph hydrate requests contributing to the planned width total", graph_fanout_metrics.hydrate_planned_parallel_total + graph_fanout_metrics.hydrate_planned_sequential_total);
        try health_metrics.appendPromMetric(writer, "antfly_data_replay_debt_tables", "gauge", "Cached local tables with at least one index still behind replay", runtime_summary.tables_with_replay_debt);
        try health_metrics.appendPromMetric(writer, "antfly_data_replay_debt_groups", "gauge", "Cached local groups with at least one index still behind replay", runtime_summary.groups_with_replay_debt);
        try health_metrics.appendPromMetric(writer, "antfly_data_replay_debt_indexes", "gauge", "Cached local indexes that still report replay catch-up debt", runtime_summary.indexes_with_replay_debt);
        try health_metrics.appendPromMetric(writer, "antfly_data_replay_debt_sequences", "gauge", "Total cached replay backlog measured as target minus applied sequence across local indexes", runtime_summary.outstanding_replay_sequences);
        try health_metrics.appendPromMetric(writer, "antfly_data_replay_debt_max_index_sequences", "gauge", "Largest cached replay backlog for any single local index", runtime_summary.max_index_replay_backlog);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_warmup_active", "gauge", "Whether the provisioned cache warmup worker is currently active", if (self.data_server.provisioned_warmup_active.load(.acquire)) 1 else 0);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_warmup_started_total", "counter", "Provisioned cache warmup runs started", self.data_server.provisioned_warmup_started.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_warmup_completed_total", "counter", "Provisioned cache warmup runs completed", self.data_server.provisioned_warmup_completed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_warmup_failed_total", "counter", "Provisioned cache warmup runs that exited early with an error", self.data_server.provisioned_warmup_failed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_warmup_last_group_count", "gauge", "Provisioned table groups warmed by the most recent warmup run", self.data_server.provisioned_warmup_last_group_count.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_warmup_last_duration_ns", "gauge", "Duration of the most recent provisioned cache warmup run in monotonic nanoseconds", self.data_server.provisioned_warmup_last_duration_ns.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_active", "gauge", "Whether the provisioned startup catch-up worker is currently active", if (self.data_server.provisioned_startup_catch_up_active.load(.acquire)) 1 else 0);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_started_total", "counter", "Provisioned startup catch-up runs started", self.data_server.provisioned_startup_catch_up_started.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_completed_total", "counter", "Provisioned startup catch-up runs completed", self.data_server.provisioned_startup_catch_up_completed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_failed_total", "counter", "Provisioned startup catch-up runs that exited early with an error", self.data_server.provisioned_startup_catch_up_failed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_last_group_count", "gauge", "Provisioned table groups examined by the most recent startup catch-up run", self.data_server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_last_groups_with_debt", "gauge", "Provisioned table groups that still had replay debt when the most recent startup catch-up run examined them", self.data_server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_last_groups_cleared", "gauge", "Provisioned table groups whose replay debt was cleared by the most recent startup catch-up run", self.data_server.provisioned_startup_catch_up_last_groups_cleared.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_last_busy_groups", "gauge", "Provisioned table groups deferred by the most recent startup catch-up run because foreground work held the writer lock", self.data_server.provisioned_startup_catch_up_last_busy_groups.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_last_duration_ns", "gauge", "Duration of the most recent provisioned startup catch-up run in monotonic nanoseconds", self.data_server.provisioned_startup_catch_up_last_duration_ns.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_read_cache_hits_total", "counter", "Provisioned read-cache hits served from already-open local table DBs", read_cache_stats.hit_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_read_cache_misses_total", "counter", "Provisioned read-cache opens that had to open a local table DB", read_cache_stats.miss_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_write_cache_hits_total", "counter", "Provisioned write-cache hits served from already-open local table DBs", write_cache_stats.hit_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_write_cache_misses_total", "counter", "Provisioned write-cache opens that had to open a local table DB", write_cache_stats.miss_count);
        try writeResourceMetrics(writer, &self.data_server.provisioned_storage.resource_manager);
        try writeLsmCacheMetrics(writer, self.data_server.provisioned_storage.lsm_cache.snapshotStats());
        try writeLsmNativeStorageMetrics(writer, self.data_server.write_source.lsmNativeStorageStatsBestEffort());
        try writeFullTextMemoryMetrics(writer, self.data_server.write_source.textMemoryAttributionStatsBestEffort());
        try writeProcessMemoryMetrics(writer, process_memory_mod.snapshot());
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_score", "gauge", "Maximum cached table LSM maintenance pressure score", self.data_server.write_source.lsmMaintenanceScoreBestEffort());
        try health_metrics.appendPromMetric(writer, "antfly_lsm_cached_write_dbs", "gauge", "Cached writable table DBs with local LSM state", @intCast(self.data_server.write_source.cachedWriteDbCountBestEffort()));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_active", "gauge", "Whether the data server LSM maintenance background worker is currently active", if (self.data_server.lsm_maintenance_active.load(.acquire)) 1 else 0);
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_started_total", "counter", "Data server LSM maintenance background worker wake cycles started", self.data_server.lsm_maintenance_started.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_completed_total", "counter", "Data server LSM maintenance background worker wake cycles completed with no immediate work remaining", self.data_server.lsm_maintenance_completed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_failed_total", "counter", "Data server LSM maintenance background worker wake cycles that observed an error", self.data_server.lsm_maintenance_failed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_capacity_denied_total", "counter", "Data server LSM maintenance background wake cycles denied by resource capacity", self.data_server.lsm_maintenance_capacity_denied.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_bulk_deferred_total", "counter", "Data server LSM maintenance background wake cycles deferred behind active bulk ingest", self.data_server.lsm_maintenance_bulk_deferred.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_lock_deferred_total", "counter", "Data server LSM maintenance background wake cycles deferred behind foreground locks", self.data_server.lsm_maintenance_lock_deferred.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_next_eligible_ns", "gauge", "Monotonic timestamp when background LSM maintenance can next run", self.data_server.lsm_maintenance_next_eligible_ns.load(.monotonic));
        try writeLsmMaintenanceMetrics(writer, self.data_server.write_source.lsmMaintenanceStatsBestEffort());
        try writeLsmWriteMetrics(writer, self.data_server.write_source.lsmWriteStatsBestEffort());
        try writeTextMergeMetrics(writer, self.data_server.write_source.textMergeStatsBestEffort());
        try writeAsyncIndexingMetrics(writer, self.data_server.write_source.asyncIndexingStatsBestEffort());
        try antfly.db.query_metrics.writePrometheus(writer);
    }
};

fn writeLsmMaintenanceMetrics(writer: *std.Io.Writer, stats: lsm_backend_mod.Backend.MaintenanceStats) !void {
    try health_metrics.appendPromMetric(writer, "antfly_lsm_mutable_entries", "gauge", "Cached write LSM active mutable memtable entries", stats.mutable_entries);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_mutable_bytes", "gauge", "Cached write LSM active mutable memtable estimated bytes", stats.mutable_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_immutable_memtables", "gauge", "Cached write LSM immutable memtables waiting to flush", stats.immutable_memtables);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_immutable_entries", "gauge", "Cached write LSM immutable memtable entries waiting to flush", stats.immutable_entries);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_immutable_bytes", "gauge", "Cached write LSM immutable memtable estimated bytes waiting to flush", stats.immutable_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_total_runs", "gauge", "Cached write LSM active run count", stats.total_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_total_run_bytes", "gauge", "Cached write LSM active run bytes on disk", stats.total_run_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_mutable_snapshot_clone_calls_total", "counter", "LSM mutable snapshot clone calls issued by snapshot reads", stats.mutable_snapshot_clone_calls);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_mutable_snapshot_clone_bytes_total", "counter", "Total bytes cloned into LSM mutable snapshot reads", stats.mutable_snapshot_clone_bytes_total);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_mutable_snapshot_clone_peak_bytes", "gauge", "Peak bytes cloned for a single LSM mutable snapshot read", stats.mutable_snapshot_clone_peak_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_read_snapshot_mutable_rotations_total", "counter", "LSM mutable memtable rotations triggered to serve broad read snapshots without cloning", stats.read_snapshot_mutable_rotations);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_read_snapshot_mutable_rotation_bytes_total", "counter", "Mutable memtable bytes rotated into immutable state for broad read snapshots", stats.read_snapshot_mutable_rotation_bytes_total);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_read_snapshot_mutable_rotation_peak_bytes", "gauge", "Peak mutable memtable bytes rotated for one broad read snapshot", stats.read_snapshot_mutable_rotation_peak_bytes);
    try health_metrics.appendPromMetricHeader(writer, "antfly_lsm_mutable_snapshot_clone_reason_calls_total", "counter", "LSM mutable snapshot clone calls by reader class");
    for (stats.mutable_snapshot_clone_by_reason, 0..) |reason_stats, i| {
        const reason: lsm_backend_mod.MutableSnapshotReason = @enumFromInt(i);
        const labels = [_]health_metrics.PromLabel{.{ .name = "reason", .value = lsm_backend_mod.mutableSnapshotReasonName(reason) }};
        try health_metrics.appendPromSampleLabeled(writer, "antfly_lsm_mutable_snapshot_clone_reason_calls_total", &labels, reason_stats.calls);
    }
    try health_metrics.appendPromMetricHeader(writer, "antfly_lsm_mutable_snapshot_clone_reason_bytes_total", "counter", "Total bytes cloned into LSM mutable snapshot reads by reader class");
    for (stats.mutable_snapshot_clone_by_reason, 0..) |reason_stats, i| {
        const reason: lsm_backend_mod.MutableSnapshotReason = @enumFromInt(i);
        const labels = [_]health_metrics.PromLabel{.{ .name = "reason", .value = lsm_backend_mod.mutableSnapshotReasonName(reason) }};
        try health_metrics.appendPromSampleLabeled(writer, "antfly_lsm_mutable_snapshot_clone_reason_bytes_total", &labels, reason_stats.bytes_total);
    }
    try health_metrics.appendPromMetricHeader(writer, "antfly_lsm_mutable_snapshot_clone_reason_peak_bytes", "gauge", "Peak bytes cloned for a single LSM mutable snapshot read by reader class");
    for (stats.mutable_snapshot_clone_by_reason, 0..) |reason_stats, i| {
        const reason: lsm_backend_mod.MutableSnapshotReason = @enumFromInt(i);
        const labels = [_]health_metrics.PromLabel{.{ .name = "reason", .value = lsm_backend_mod.mutableSnapshotReasonName(reason) }};
        try health_metrics.appendPromSampleLabeled(writer, "antfly_lsm_mutable_snapshot_clone_reason_peak_bytes", &labels, reason_stats.peak_bytes);
    }
    try health_metrics.appendPromMetric(writer, "antfly_lsm_total_run_logical_entry_bytes", "gauge", "Cached write LSM logical table entry bytes", stats.total_run_logical_entry_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_total_run_physical_entry_bytes", "gauge", "Cached write LSM physical table entry bytes after block compression", stats.total_run_physical_entry_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_total_run_compressed_blocks", "gauge", "Cached write LSM compressed table blocks", stats.total_run_compressed_blocks);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_total_run_raw_blocks", "gauge", "Cached write LSM raw table blocks", stats.total_run_raw_blocks);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_l0_runs", "gauge", "Cached write LSM level-zero run count", stats.l0_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_l0_bytes", "gauge", "Cached write LSM level-zero run bytes", stats.l0_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_overlapping_l0_runs", "gauge", "Largest cached write LSM overlapping level-zero run pressure", stats.overlapping_l0_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compactable_l0_runs", "gauge", "Cached write LSM level-zero runs over compaction threshold", stats.compactable_l0_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_soft_limit_l0_runs", "gauge", "Configured cached write LSM soft level-zero run pressure threshold", stats.soft_limit_l0_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_hard_limit_l0_runs", "gauge", "Configured cached write LSM hard level-zero run pressure threshold", stats.hard_limit_l0_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_soft_limit_l0_bytes", "gauge", "Configured cached write LSM soft level-zero byte pressure threshold", stats.soft_limit_l0_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_hard_limit_l0_bytes", "gauge", "Configured cached write LSM hard level-zero byte pressure threshold", stats.hard_limit_l0_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_lower_level_runs", "gauge", "Cached write LSM lower-level run count", stats.lower_level_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_lower_level_bytes", "gauge", "Cached write LSM lower-level run bytes", stats.lower_level_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_max_level", "gauge", "Highest cached write LSM lower level currently populated", stats.max_level);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_level_overflow_runs", "gauge", "Cached write LSM lower-level runs over configured level targets", stats.level_overflow_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_level_overflow_bytes", "gauge", "Cached write LSM lower-level bytes over configured level targets", stats.level_overflow_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_paths", "gauge", "Cached write LSM obsolete table paths waiting for cleanup", stats.obsolete_paths);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_active_readers", "gauge", "Cached write LSM readers currently retaining run or memtable snapshots", stats.active_readers);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_active_bulk_ingest_batches", "gauge", "Cached write LSM active bulk-ingest session batches", stats.active_bulk_ingest_batches);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_retained_segments", "gauge", "Cached write LSM WAL segments still retained for replay", stats.wal_retained_segments);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_retained_bytes", "gauge", "Cached write LSM WAL bytes still retained for replay", stats.wal_retained_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_checkpoint_oldest_retained_segment", "gauge", "Oldest cached write LSM WAL segment still needed by the durable checkpoint", stats.wal_checkpoint_oldest_retained_segment);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_checkpoint_covered_through_segment", "gauge", "Last cached write LSM WAL segment durably covered by the checkpoint", stats.wal_checkpoint_covered_through_segment);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_checkpoint_current_segment", "gauge", "Current cached write LSM WAL segment", stats.wal_checkpoint_current_segment);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_checkpoint_lag_segments", "gauge", "Sealed cached write LSM WAL segments retained before the active segment", stats.wal_checkpoint_lag_segments);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_replay_retained_segments", "gauge", "Cached write LSM dedicated replay WAL segments still retained", stats.wal_replay_retained_segments);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_replay_retained_bytes", "gauge", "Cached write LSM dedicated replay WAL bytes still retained", stats.wal_replay_retained_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_replay_current_segment", "gauge", "Current cached write LSM dedicated replay WAL segment", stats.wal_replay_current_segment);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_manifest_dirty", "gauge", "Whether cached write LSM manifests have unflushed changes", if (stats.manifest_dirty) 1 else 0);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_manifest_dirty", "gauge", "Whether cached write LSM obsolete-file manifests have unflushed changes", if (stats.obsolete_manifest_dirty) 1 else 0);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_active_jobs", "gauge", "Cached write LSM compaction scheduler active jobs", stats.compaction_scheduler_active_jobs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_in_flight_input_bytes", "gauge", "Cached write LSM compaction scheduler in-flight input bytes", stats.compaction_scheduler_in_flight_input_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_active_oldest_age_ns", "gauge", "Oldest active cached write LSM compaction scheduler job age in nanoseconds", stats.compaction_scheduler_active_oldest_age_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_grants_total", "counter", "Cached write LSM compaction scheduler grants", stats.compaction_scheduler_grants);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_completions_total", "counter", "Cached write LSM compaction scheduler completions", stats.compaction_scheduler_completions);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_denied_capacity_total", "counter", "Cached write LSM compaction scheduler capacity denials", stats.compaction_scheduler_denied_capacity);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_denied_resource_pressure_total", "counter", "Cached write LSM compaction scheduler resource-pressure denials", stats.compaction_scheduler_denied_resource_pressure);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_oversized_grants_total", "counter", "Cached write LSM compaction scheduler oversized single-job grants", stats.compaction_scheduler_oversized_grants);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_remembered_pending", "gauge", "Cached write LSM remembered compaction candidates pending retry", stats.compaction_scheduler_remembered_pending);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_remembered_pending_runs", "gauge", "Cached write LSM input runs in remembered compaction candidates pending retry", stats.compaction_scheduler_remembered_pending_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_remembered_pending_bytes", "gauge", "Cached write LSM input bytes in remembered compaction candidates pending retry", stats.compaction_scheduler_remembered_pending_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_remembered_candidates_total", "counter", "Cached write LSM compaction candidates remembered after denied scheduling", stats.compaction_scheduler_remembered_candidates);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_remembered_retries_total", "counter", "Cached write LSM remembered compaction retry attempts", stats.compaction_scheduler_remembered_retries);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_remembered_hits_total", "counter", "Cached write LSM remembered compactions that were executed", stats.compaction_scheduler_remembered_hits);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_remembered_stale_total", "counter", "Cached write LSM remembered compactions invalidated by run-set changes", stats.compaction_scheduler_remembered_stale);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_conflict_denials_total", "counter", "Cached write LSM remembered compaction retries denied by active scheduler conflicts", stats.compaction_scheduler_conflict_denials);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_background_io_budget_bytes", "gauge", "Configured cached write LSM per-maintenance-step background IO byte budget", stats.background_io_budget_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_background_io_reserved_bytes_total", "counter", "Cached write LSM background IO bytes reserved by admitted maintenance jobs", stats.background_io_reserved_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_background_io_denied_jobs_total", "counter", "Cached write LSM maintenance jobs denied by background IO admission", stats.background_io_denied_jobs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_background_io_oversized_jobs_total", "counter", "Cached write LSM maintenance jobs admitted as oversized single jobs", stats.background_io_oversized_jobs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_backend_lock_waits_total", "counter", "Cached write LSM backend lock waits", stats.backend_lock_waits);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_backend_lock_wait_ns_total", "counter", "Nanoseconds spent waiting on cached write LSM backend locks", stats.backend_lock_wait_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_backend_lock_max_wait_ns", "gauge", "Maximum cached write LSM backend lock wait in nanoseconds", stats.backend_lock_max_wait_ns);
}

fn writeLsmWriteMetrics(writer: *std.Io.Writer, stats: lsm_backend_mod.Backend.WriteStats) !void {
    try health_metrics.appendPromMetric(writer, "antfly_lsm_flushes_total", "counter", "Cached write LSM mutable flushes", stats.flushes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_flush_input_entries_total", "counter", "Entries consumed by cached write LSM mutable flushes", stats.flush_input_entries);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_flush_output_runs_total", "counter", "Runs produced by cached write LSM mutable flushes", stats.flush_output_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_flush_output_bytes_total", "counter", "Run bytes produced by cached write LSM mutable flushes", stats.flush_output_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_flush_ns_total", "counter", "Nanoseconds spent in cached write LSM mutable flushes", stats.flush_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_table_file_writes_total", "counter", "Cached write LSM table files written", stats.table_file_writes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_table_file_bytes_total", "counter", "Cached write LSM table file bytes written", stats.table_file_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_table_file_logical_entry_bytes_total", "counter", "Logical entry bytes written into cached write LSM table files", stats.table_file_logical_entry_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_table_file_physical_entry_bytes_total", "counter", "Physical entry bytes written into cached write LSM table files after block compression", stats.table_file_physical_entry_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_table_file_raw_blocks_total", "counter", "Raw cached write LSM table blocks written", stats.table_file_raw_blocks);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_table_file_compressed_blocks_total", "counter", "Compressed cached write LSM table blocks written", stats.table_file_compressed_blocks);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_table_file_compression_codec_mask", "gauge", "Bit mask of cached write LSM table compression codecs observed in writes", stats.table_file_compression_codec_mask);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_sorted_ingest_runs_total", "counter", "Runs published through cached write LSM sorted ingest", stats.sorted_ingest_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_sorted_ingest_bytes_total", "counter", "Run bytes published through cached write LSM sorted ingest", stats.sorted_ingest_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_sorted_ingest_ns_total", "counter", "Nanoseconds spent in cached write LSM sorted ingest", stats.sorted_ingest_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_ns_total", "counter", "Nanoseconds spent compacting cached write LSM runs", stats.compaction_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_manifest_writes_total", "counter", "Cached write LSM manifest writes", stats.manifest_writes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_manifest_bytes_total", "counter", "Cached write LSM manifest bytes written", stats.manifest_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_manifest_ns_total", "counter", "Nanoseconds spent writing cached write LSM manifests", stats.manifest_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_events_total", "counter", "Cached write LSM foreground write-pressure events", stats.write_pressure_events);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_compactions_total", "counter", "Cached write LSM foreground write-pressure compactions", stats.write_pressure_compactions);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_compaction_steps_total", "counter", "Cached write LSM foreground write-pressure compaction steps", stats.write_pressure_compaction_steps);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_overloads_total", "counter", "Cached write LSM write-pressure events that remained above hard limits after the foreground budget", stats.write_pressure_overloads);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_rejections_total", "counter", "Cached write LSM writes rejected after write-pressure overload", stats.write_pressure_rejections);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_ns_total", "counter", "Nanoseconds spent in cached write LSM foreground write-pressure compactions", stats.write_pressure_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_pressure_flushes_total", "counter", "Cached write LSM foreground WAL-pressure flushes", stats.wal_pressure_flushes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_pressure_ns_total", "counter", "Nanoseconds spent in cached write LSM foreground WAL-pressure flushes", stats.wal_pressure_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_append_records_total", "counter", "Cached write LSM WAL records appended", stats.wal_append_records);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_append_entries_total", "counter", "Cached write LSM WAL entries appended", stats.wal_append_entries);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_append_bytes_total", "counter", "Cached write LSM WAL bytes appended", stats.wal_append_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_append_ns_total", "counter", "Nanoseconds spent appending cached write LSM WAL records", stats.wal_append_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_sync_records_total", "counter", "Cached write LSM WAL records synced", stats.wal_sync_records);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_sync_ns_total", "counter", "Nanoseconds spent syncing cached write LSM WAL records", stats.wal_sync_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_replay_records_total", "counter", "Cached write LSM WAL records replayed", stats.wal_replay_records);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_replay_entries_total", "counter", "Cached write LSM WAL entries replayed", stats.wal_replay_entries);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_replay_bytes_total", "counter", "Cached write LSM WAL bytes replayed", stats.wal_replay_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_replay_ns_total", "counter", "Nanoseconds spent replaying cached write LSM WAL records", stats.wal_replay_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_replay_truncated_tail_bytes_total", "counter", "Truncated cached write LSM WAL tail bytes ignored during replay", stats.wal_replay_truncated_tail_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_resets_total", "counter", "Cached write LSM WAL reset operations", stats.wal_resets);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_wal_reset_ns_total", "counter", "Nanoseconds spent resetting cached write LSM WAL files", stats.wal_reset_ns);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_immutable_rotations_total", "counter", "Cached write LSM mutable-to-immutable rotations", stats.immutable_rotations);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_immutable_flushes_total", "counter", "Cached write LSM immutable memtable flushes", stats.immutable_flushes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_immutable_flush_entries_total", "counter", "Entries flushed from cached write LSM immutable memtables", stats.immutable_flush_entries);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_immutable_flush_ns_total", "counter", "Nanoseconds spent flushing cached write LSM immutable memtables", stats.immutable_flush_ns);
}

fn writeFullTextMemoryMetrics(writer: *std.Io.Writer, stats: antfly.db.TextMemoryAttributionStats) !void {
    try health_metrics.appendPromMetric(writer, "antfly_full_text_indexes", "gauge", "Cached write full-text index count", stats.text_indexes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_segments", "gauge", "Cached write full-text segment count", stats.text_segments);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_segment_bytes", "gauge", "Cached write full-text segment bytes", stats.text_segment_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_mmap_segment_bytes", "gauge", "Cached write full-text segment bytes backed by mmap files", stats.text_mmap_segment_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_heap_segment_bytes", "gauge", "Cached write full-text segment bytes retained on the heap", stats.text_heap_segment_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_max_segment_bytes", "gauge", "Largest cached write full-text segment byte size", stats.text_max_segment_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_stored_fields_bytes", "gauge", "Full-text stored-field section bytes across cached write DBs", stats.stored_fields_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_bytes", "gauge", "Full-text inverted section bytes across cached write DBs", stats.inverted_text_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_header_bytes", "gauge", "Full-text inverted section header bytes", stats.inverted_header_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_norm_bytes", "gauge", "Full-text inverted per-document norm table bytes", stats.inverted_norm_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_term_dict_bytes", "gauge", "Full-text inverted term dictionary bytes", stats.inverted_term_dict_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_term_block_bytes", "gauge", "Full-text inverted term dictionary block bytes", stats.inverted_term_block_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_term_index_bytes", "gauge", "Full-text inverted term dictionary block index bytes", stats.inverted_term_index_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_fst_bytes", "gauge", "Full-text inverted term dictionary FST bytes", stats.inverted_fst_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_bloom_bytes", "gauge", "Full-text inverted bloom filter bytes", stats.inverted_bloom_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_postings_bytes", "gauge", "Full-text inverted postings bytes", stats.inverted_postings_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_postings_header_bytes", "gauge", "Full-text inverted postings header bytes", stats.inverted_postings_header_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_block_max_bytes", "gauge", "Full-text inverted block-max metadata bytes", stats.inverted_block_max_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_chunk_meta_bytes", "gauge", "Full-text inverted postings chunk metadata bytes", stats.inverted_chunk_meta_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_postings_payload_bytes", "gauge", "Full-text inverted packed postings payload bytes", stats.inverted_postings_payload_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_positions_bytes", "gauge", "Full-text inverted positions payload bytes", stats.inverted_positions_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_skip_bytes", "gauge", "Full-text inverted skip metadata bytes", stats.inverted_skip_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_one_hit_terms", "gauge", "Full-text inverted one-hit term count", stats.inverted_one_hit_terms);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_inverted_postings_terms", "gauge", "Full-text inverted multi-hit postings term count", stats.inverted_postings_terms);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_typed_doc_values_bytes", "gauge", "Full-text typed doc-value bytes", stats.typed_doc_values_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_doc_ordinals_bytes", "gauge", "Full-text doc ordinal bytes", stats.doc_ordinals_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_section_index_bytes", "gauge", "Full-text section index bytes", stats.section_index_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_configured_lmdb_main_map_bytes", "gauge", "Configured LMDB main map bytes for full-text indexes", stats.configured_lmdb_main_map_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_full_text_configured_lmdb_wal_map_bytes", "gauge", "Configured LMDB WAL map bytes for full-text indexes", stats.configured_lmdb_wal_map_bytes);
}

fn writeTextMergeMetrics(writer: *std.Io.Writer, stats: antfly.db.types.TextMergeStats) !void {
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_enabled", "gauge", "Whether text merge scheduling is enabled for cached write DBs", if (stats.enabled) 1 else 0);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_pending_indexes", "gauge", "Cached write full-text indexes with pending merge debt", stats.pending_indexes);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_pending_segments", "gauge", "Cached write full-text segments in pending merge debt", stats.pending_segments);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_pending_bytes", "gauge", "Cached write full-text segment bytes in pending merge debt", stats.pending_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_pending_heap_bytes", "gauge", "Heap-backed full-text segment bytes in pending merge debt", stats.pending_heap_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_pending_mmap_bytes", "gauge", "Mmap-backed full-text segment bytes in pending merge debt", stats.pending_mmap_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_in_flight_merges", "gauge", "Cached write full-text merges currently in flight", stats.in_flight_merges);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_in_flight_segments", "gauge", "Cached write full-text source segments currently in flight", stats.in_flight_segments);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_completed_total", "counter", "Cached write full-text merges completed", stats.completed_merges);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_skipped_stale_total", "counter", "Cached write full-text merges skipped because the candidate became stale", stats.skipped_stale_merges);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_failed_total", "counter", "Cached write full-text merges that failed", stats.failed_merges);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_input_segments_total", "counter", "Source full-text segments consumed by completed merges", stats.merge_input_segments_total);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_input_bytes_total", "counter", "Source full-text segment bytes consumed by completed merges", stats.merge_input_bytes_total);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_output_segments_total", "counter", "Output full-text segments published by completed merges", stats.merge_output_segments_total);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_output_bytes_total", "counter", "Output full-text segment bytes published by completed merges", stats.merge_output_bytes_total);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_last_input_segments", "gauge", "Source full-text segments consumed by the last completed merge", stats.last_merge_input_segments);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_last_input_bytes", "gauge", "Source full-text segment bytes consumed by the last completed merge", stats.last_merge_input_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_last_output_segments", "gauge", "Output full-text segments published by the last completed merge", stats.last_merge_output_segments);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_last_output_bytes", "gauge", "Output full-text segment bytes published by the last completed merge", stats.last_merge_output_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_quarantined_merges", "gauge", "Cached write full-text merge candidates currently quarantined after failure", stats.quarantined_merges);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_quarantined_segments", "gauge", "Cached write full-text source segments currently quarantined after failure", stats.quarantined_segments);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_retry_after_ns", "gauge", "Latest monotonic retry-after timestamp for cached write full-text merge work", stats.retry_after_ns);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_deferred_for_pressure_total", "counter", "Cached write full-text merge attempts deferred for resource pressure", stats.deferred_for_pressure);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_backpressure_events_total", "counter", "Cached write full-text merge backpressure events", stats.backpressure_events);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_backpressure_ns_total", "counter", "Nanoseconds spent under full-text merge backpressure", stats.backpressure_ns);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_max_pending_segments", "gauge", "Maximum pending full-text segments observed by merge scheduling", stats.max_pending_segments);
    try health_metrics.appendPromMetric(writer, "antfly_text_merge_max_pending_bytes", "gauge", "Maximum pending full-text segment bytes observed by merge scheduling", stats.max_pending_bytes);
}

const AsyncMutexMetricField = enum {
    lock_calls,
    contended_calls,
    max_waiters,
    spin_loops,
    yield_loops,
    sleep_loops,
    wait_ns,
    max_wait_ns,
    hold_ns,
    max_hold_ns,
};

fn writeAsyncIndexingMetrics(writer: *std.Io.Writer, stats: antfly.db.types.AsyncIndexingStats) !void {
    try writeAsyncMutexMetricFamily(writer, stats, .lock_calls, "antfly_async_index_mutex_lock_calls_total", "counter", "Async indexing mutex lock attempts");
    try writeAsyncMutexMetricFamily(writer, stats, .contended_calls, "antfly_async_index_mutex_contended_calls_total", "counter", "Async indexing mutex lock attempts that encountered contention");
    try writeAsyncMutexMetricFamily(writer, stats, .max_waiters, "antfly_async_index_mutex_max_waiters", "gauge", "Maximum concurrent waiters observed for each async indexing mutex");
    try writeAsyncMutexMetricFamily(writer, stats, .spin_loops, "antfly_async_index_mutex_spin_loops_total", "counter", "Async indexing mutex backoff spin-loop iterations");
    try writeAsyncMutexMetricFamily(writer, stats, .yield_loops, "antfly_async_index_mutex_yield_loops_total", "counter", "Async indexing mutex backoff yield iterations");
    try writeAsyncMutexMetricFamily(writer, stats, .sleep_loops, "antfly_async_index_mutex_sleep_loops_total", "counter", "Async indexing mutex backoff sleep iterations");
    try writeAsyncMutexMetricFamily(writer, stats, .wait_ns, "antfly_async_index_mutex_wait_ns_total", "counter", "Total nanoseconds spent waiting on async indexing mutexes");
    try writeAsyncMutexMetricFamily(writer, stats, .max_wait_ns, "antfly_async_index_mutex_max_wait_ns", "gauge", "Maximum observed mutex wait in nanoseconds");
    try writeAsyncMutexMetricFamily(writer, stats, .hold_ns, "antfly_async_index_mutex_hold_ns_total", "counter", "Total nanoseconds spent holding async indexing mutexes");
    try writeAsyncMutexMetricFamily(writer, stats, .max_hold_ns, "antfly_async_index_mutex_max_hold_ns", "gauge", "Maximum observed mutex hold in nanoseconds");

    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_note_calls_total", "counter", "Applied-sequence note calls queued by async indexing", stats.applied_sequence.note_calls);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_forced_flush_calls_total", "counter", "Forced applied-sequence flush calls", stats.applied_sequence.forced_flush_calls);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_skipped_flush_calls_total", "counter", "Applied-sequence calls that skipped flush due to coalescing", stats.applied_sequence.skipped_flush_calls);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_flush_calls_total", "counter", "Applied-sequence flush executions", stats.applied_sequence.flush_calls);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_flushed_indexes_total", "counter", "Index watermark updates written by applied-sequence flushes", stats.applied_sequence.flushed_indexes);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_sync_ns_total", "counter", "Nanoseconds spent syncing indexes before applied-sequence persistence", stats.applied_sequence.sync_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_save_ns_total", "counter", "Nanoseconds spent writing applied-sequence state", stats.applied_sequence.save_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_flush_ns_total", "counter", "Nanoseconds spent across applied-sequence flushes", stats.applied_sequence.flush_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_applied_sequence_max_flush_ns", "gauge", "Maximum observed applied-sequence flush duration in nanoseconds", stats.applied_sequence.max_flush_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_active", "gauge", "Whether startup catch-up is actively opening or catching up a local index", if (stats.startup.active) 1 else 0);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_retained_segments", "gauge", "Retained WAL segments reported by the active startup catch-up snapshot", stats.startup.wal_retained_segments);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_retained_bytes", "gauge", "Retained WAL bytes reported by the active startup catch-up snapshot", stats.startup.wal_retained_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_checkpoint_oldest_retained_segment", "gauge", "Oldest WAL segment still retained by the active startup catch-up checkpoint snapshot", stats.startup.wal_checkpoint_oldest_retained_segment);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_checkpoint_covered_through_segment", "gauge", "Last WAL segment covered by the active startup catch-up checkpoint snapshot", stats.startup.wal_checkpoint_covered_through_segment);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_checkpoint_current_segment", "gauge", "Current WAL segment in the active startup catch-up checkpoint snapshot", stats.startup.wal_checkpoint_current_segment);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_checkpoint_lag_segments", "gauge", "Sealed WAL segments retained before the active segment in the startup catch-up snapshot", stats.startup.wal_checkpoint_lag_segments);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_replay_retained_segments", "gauge", "Dedicated replay WAL segments retained by the active startup catch-up snapshot", stats.startup.wal_replay_retained_segments);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_replay_retained_bytes", "gauge", "Dedicated replay WAL bytes retained by the active startup catch-up snapshot", stats.startup.wal_replay_retained_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_replay_current_segment", "gauge", "Current dedicated replay WAL segment in the startup catch-up snapshot", stats.startup.wal_replay_current_segment);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_configured_indexes", "gauge", "Configured indexes on the table currently being opened or caught up", stats.startup.configured_indexes);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_opened_indexes", "gauge", "Configured indexes already opened for the active startup catch-up table", stats.startup.opened_indexes);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_db_open_ns", "gauge", "Observed DB.open duration for the active startup catch-up table", stats.startup.db_open_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_load_indexes_ns", "gauge", "Observed index-load duration inside DB.open for the active startup catch-up table", stats.startup.load_indexes_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_stores", "gauge", "LSM-backed stores observed during startup index open", stats.startup.lsm_open_stores);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_completed", "gauge", "LSM-backed stores that completed startup open", stats.startup.lsm_open_completed);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_failed", "gauge", "LSM-backed stores that failed startup open", stats.startup.lsm_open_failed);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_total_ns", "gauge", "Summed LSM open duration across startup stores", stats.startup.lsm_open_total_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_initializing_storage_ns", "gauge", "Summed LSM storage initialization duration during startup open", stats.startup.lsm_open_initializing_storage_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_manifest_ns", "gauge", "Summed LSM manifest load duration during startup open", stats.startup.lsm_open_manifest_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_ensuring_dirs_ns", "gauge", "Summed LSM directory creation duration during startup open", stats.startup.lsm_open_ensuring_dirs_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_wal_replay_ns", "gauge", "Summed LSM WAL replay duration during startup open", stats.startup.lsm_open_wal_replay_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_mounting_runs_ns", "gauge", "Summed LSM run mounting duration during startup open", stats.startup.lsm_open_mounting_runs_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_loaded_runs", "gauge", "LSM runs loaded during startup open", stats.startup.lsm_open_loaded_runs);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_obsolete_paths", "gauge", "LSM obsolete paths loaded during startup open", stats.startup.lsm_open_obsolete_paths);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_mutable_entries_after_replay", "gauge", "LSM mutable entries after startup WAL replay", stats.startup.lsm_open_mutable_entries_after_replay);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_immutable_memtables_after_replay", "gauge", "LSM immutable memtables after startup WAL replay", stats.startup.lsm_open_immutable_memtables_after_replay);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_replay_records", "gauge", "Observed LSM WAL replay records during startup index open", stats.startup.wal_replay_records);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_replay_entries", "gauge", "Observed LSM WAL replay entries during startup index open", stats.startup.wal_replay_entries);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_replay_bytes", "gauge", "Observed LSM WAL replay bytes during startup index open", stats.startup.wal_replay_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_replay_ns", "gauge", "Observed LSM WAL replay nanoseconds during startup index open", stats.startup.wal_replay_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_wal_replay_truncated_tail_bytes", "gauge", "Observed truncated WAL tail bytes during startup index open", stats.startup.wal_replay_truncated_tail_bytes);
    try health_metrics.appendPromMetricHeader(writer, "antfly_async_index_startup_phase", "gauge", "One-hot startup catch-up phase, labeled by current phase");
    inline for ([_]antfly.db.types.StartupCatchUpPhase{ .idle, .opening_db, .artifact_rebuild, .startup_catch_up }) |phase| {
        try health_metrics.appendPromSampleLabeled(writer, "antfly_async_index_startup_phase", &.{
            .{ .name = "phase", .value = switch (phase) {
                .idle => "idle",
                .opening_db => "opening_db",
                .artifact_rebuild => "artifact_rebuild",
                .startup_catch_up => "startup_catch_up",
            } },
        }, if (stats.startup.phase == phase) 1 else 0);
    }

    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_begin_calls_total", "counter", "Dense catch-up session begin calls", stats.dense_catch_up.begin_calls);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_finish_calls_total", "counter", "Dense catch-up session successful finishes", stats.dense_catch_up.finish_calls);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_abort_calls_total", "counter", "Dense catch-up session aborts", stats.dense_catch_up.abort_calls);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_active", "gauge", "Whether a dense catch-up session is actively replaying", if (stats.dense_catch_up.active) 1 else 0);
    try health_metrics.appendPromMetricHeader(writer, "antfly_async_index_dense_catch_up_phase", "gauge", "One-hot dense catch-up phase");
    inline for ([_]antfly.db.types.DenseCatchUpStats.Phase{ .idle, .replay, .bulk_finish, .bulk_split, .bulk_publish, .applied_sequence_flush }) |phase| {
        try health_metrics.appendPromSampleLabeled(writer, "antfly_async_index_dense_catch_up_phase", &.{
            .{ .name = "phase", .value = switch (phase) {
                .idle => "idle",
                .replay => "replay",
                .bulk_finish => "bulk_finish",
                .bulk_split => "bulk_split",
                .bulk_publish => "bulk_publish",
                .applied_sequence_flush => "applied_sequence_flush",
            } },
        }, if (stats.dense_catch_up.phase == phase) 1 else 0);
    }
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_current_sequence", "gauge", "Current sequence reached by the active dense catch-up replay", stats.dense_catch_up.current_sequence);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_current_target_sequence", "gauge", "Current replay target sequence for the active dense catch-up replay", stats.dense_catch_up.current_target_sequence);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_current_scanned_entries", "gauge", "Cumulative replay records scanned in the active dense catch-up session", stats.dense_catch_up.current_scanned_entries);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_current_applied_entries", "gauge", "Cumulative replay batches applied in the active dense catch-up session", stats.dense_catch_up.current_applied_entries);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_replay_scan_batches", "gauge", "Replay scan batches opened in the active dense catch-up session", stats.dense_catch_up.replay_scan_batches);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_replay_hint_filter_skips", "gauge", "Replay records skipped by hint filtering in the active dense catch-up session", stats.dense_catch_up.replay_hint_filter_skips);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_progress_updates_total", "counter", "Dense catch-up in-chunk progress updates published", stats.dense_catch_up.progress_updates);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_bulk_finish_windows_total", "counter", "HBC bulk-finish publish windows completed during dense catch-up", stats.dense_catch_up.bulk_finish_windows);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_bulk_finish_split_steps_total", "counter", "HBC deferred leaf split steps completed during dense catch-up bulk finish", stats.dense_catch_up.bulk_finish_split_steps);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_bulk_finish_deferred_leaf_splits", "gauge", "Deferred HBC leaf splits still pending in the active dense catch-up bulk finish", stats.dense_catch_up.bulk_finish_deferred_leaf_splits);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_bulk_finish_current_window", "gauge", "Current HBC bulk-finish publish window", stats.dense_catch_up.bulk_finish_current_window);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_bulk_finish_current_window_split_steps", "gauge", "HBC split steps in the current bulk-finish publish window", stats.dense_catch_up.bulk_finish_current_window_split_steps);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_bulk_finish_current_window_ns", "gauge", "Nanoseconds spent in the current HBC bulk-finish publish window", stats.dense_catch_up.bulk_finish_current_window_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_bulk_finish_max_window_ns", "gauge", "Maximum observed HBC bulk-finish publish window duration", stats.dense_catch_up.bulk_finish_max_window_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_finish_ns_total", "counter", "Nanoseconds spent finishing dense catch-up sessions", stats.dense_catch_up.finish_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_max_finish_ns", "gauge", "Maximum observed dense catch-up finish duration in nanoseconds", stats.dense_catch_up.max_finish_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_finalize_ns_total", "counter", "Nanoseconds spent in HBC bulk-ingest finalize during dense catch-up", stats.dense_catch_up.finalize_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_max_finalize_ns", "gauge", "Maximum observed HBC finalize duration in nanoseconds", stats.dense_catch_up.max_finalize_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_maintenance_calls_total", "counter", "Dense catch-up LSM maintenance call count", stats.dense_catch_up.maintenance_calls);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_maintenance_steps_total", "counter", "Dense catch-up LSM maintenance steps executed", stats.dense_catch_up.maintenance_steps);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_maintenance_ns_total", "counter", "Nanoseconds spent in dense catch-up LSM maintenance", stats.dense_catch_up.maintenance_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_max_maintenance_ns", "gauge", "Maximum observed dense catch-up maintenance duration in nanoseconds", stats.dense_catch_up.max_maintenance_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_manifest_writes_total", "counter", "Manifest writes observed during dense catch-up finish", stats.dense_catch_up.manifest_writes);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_manifest_ns_total", "counter", "Manifest write nanoseconds observed during dense catch-up finish", stats.dense_catch_up.manifest_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_write_pressure_compactions_total", "counter", "Write-pressure compactions observed during dense catch-up finish", stats.dense_catch_up.write_pressure_compactions);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_dense_catch_up_write_pressure_ns_total", "counter", "Write-pressure compaction nanoseconds observed during dense catch-up finish", stats.dense_catch_up.write_pressure_ns);
}

fn writeAsyncMutexMetricFamily(
    writer: *std.Io.Writer,
    stats: antfly.db.types.AsyncIndexingStats,
    field: AsyncMutexMetricField,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
) !void {
    try health_metrics.appendPromMetricHeader(writer, name, metric_type, help);
    try appendAsyncMutexSample(writer, name, "apply", stats.apply_mutex, field);
    try appendAsyncMutexSample(writer, name, "applied_sequence", stats.applied_sequence_mutex, field);
    try appendAsyncMutexSample(writer, name, "dense_finish", stats.dense_finish_mutex, field);
}

fn appendAsyncMutexSample(
    writer: *std.Io.Writer,
    name: []const u8,
    mutex_name: []const u8,
    stats: antfly.db.types.DBMutexStats,
    field: AsyncMutexMetricField,
) !void {
    try health_metrics.appendPromSampleLabeled(writer, name, &.{
        .{ .name = "mutex", .value = mutex_name },
    }, asyncMutexMetricValue(stats, field));
}

fn asyncMutexMetricValue(stats: antfly.db.types.DBMutexStats, field: AsyncMutexMetricField) u64 {
    return switch (field) {
        .lock_calls => stats.lock_calls,
        .contended_calls => stats.contended_calls,
        .max_waiters => stats.max_waiters,
        .spin_loops => stats.spin_loops,
        .yield_loops => stats.yield_loops,
        .sleep_loops => stats.sleep_loops,
        .wait_ns => stats.wait_ns,
        .max_wait_ns => stats.max_wait_ns,
        .hold_ns => stats.hold_ns,
        .max_hold_ns => stats.max_hold_ns,
    };
}

fn writeResourceMetrics(writer: *std.Io.Writer, manager: *resource_manager_mod.ResourceManager) !void {
    const snapshot = manager.snapshot();
    try writeResourceMetricFamily(writer, snapshot, .used_bytes, "antfly_resource_used_bytes", "gauge", "Resource slice bytes currently accounted");
    try writeResourceMetricFamily(writer, snapshot, .peak_bytes, "antfly_resource_peak_bytes", "gauge", "Resource slice peak bytes accounted");
    try writeResourceMetricFamily(writer, snapshot, .soft_limit_bytes, "antfly_resource_soft_limit_bytes", "gauge", "Resource slice soft limit in bytes");
    try writeResourceMetricFamily(writer, snapshot, .hard_limit_bytes, "antfly_resource_hard_limit_bytes", "gauge", "Resource slice hard limit in bytes");
    try writeResourceMetricFamily(writer, snapshot, .soft_limit_events, "antfly_resource_soft_limit_events_total", "counter", "Resource slice soft-limit events");
    try writeResourceMetricFamily(writer, snapshot, .hard_limit_rejections, "antfly_resource_hard_limit_rejections_total", "counter", "Resource slice hard-limit rejections");
    try writeResourceMetricFamily(writer, snapshot, .pressure, "antfly_resource_pressure", "gauge", "Resource slice pressure state, 0 normal, 1 soft, 2 hard");
}

const ResourceMetricField = enum {
    used_bytes,
    peak_bytes,
    soft_limit_bytes,
    hard_limit_bytes,
    soft_limit_events,
    hard_limit_rejections,
    pressure,
};

fn writeResourceMetricFamily(
    writer: *std.Io.Writer,
    snapshot: resource_manager_mod.Stats,
    field: ResourceMetricField,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
) !void {
    try health_metrics.appendPromMetricHeader(writer, name, metric_type, help);
    inline for (.{
        resource_manager_mod.Slice.lsm_block_table_cache,
        resource_manager_mod.Slice.lsm_compaction_work,
        resource_manager_mod.Slice.lsm_table_builder_working_set,
        resource_manager_mod.Slice.lsm_in_memory_state,
        resource_manager_mod.Slice.lsm_wal_write_working_set,
        resource_manager_mod.Slice.hbc_node_metadata_cache,
        resource_manager_mod.Slice.dense_search_working_set,
        resource_manager_mod.Slice.dense_apply_working_set,
        resource_manager_mod.Slice.dense_routing_working_set,
        resource_manager_mod.Slice.derived_replay_window,
        resource_manager_mod.Slice.full_text_pending_segments,
        resource_manager_mod.Slice.full_text_build_working_set,
        resource_manager_mod.Slice.derived_backlog,
        resource_manager_mod.Slice.text_merge_buffers,
        resource_manager_mod.Slice.algebraic_tensor_accumulators,
    }) |slice| {
        const stats = snapshot.slices[@intFromEnum(slice)];
        try health_metrics.appendPromSampleLabeled(writer, name, &.{
            .{ .name = "slice", .value = slice.name() },
        }, resourceMetricValue(stats, field));
    }
}

fn resourceMetricValue(stats: resource_manager_mod.SliceStats, field: ResourceMetricField) u64 {
    return switch (field) {
        .used_bytes => stats.used_bytes,
        .peak_bytes => stats.peak_bytes,
        .soft_limit_bytes => stats.soft_limit_bytes,
        .hard_limit_bytes => stats.hard_limit_bytes,
        .soft_limit_events => stats.soft_limit_events,
        .hard_limit_rejections => stats.hard_limit_rejections,
        .pressure => pressureValue(stats.pressure),
    };
}

fn writeLsmCacheMetrics(writer: *std.Io.Writer, stats: lsm_backend_mod.CacheStats) !void {
    try health_metrics.appendPromMetric(writer, "antfly_lsm_cache_used_bytes", "gauge", "Shared LSM cache bytes currently resident", @intCast(stats.used_bytes));
    try health_metrics.appendPromMetric(writer, "antfly_lsm_cache_entries", "gauge", "Shared LSM cache entry count", @intCast(stats.entry_count));
    try writeLsmCacheKindMetricFamily(writer, stats, .hits, "antfly_lsm_cache_hits_total", "counter", "Shared LSM cache hits");
    try writeLsmCacheKindMetricFamily(writer, stats, .misses, "antfly_lsm_cache_misses_total", "counter", "Shared LSM cache misses");
    try writeLsmCacheKindMetricFamily(writer, stats, .inserts, "antfly_lsm_cache_inserts_total", "counter", "Shared LSM cache inserts");
    try writeLsmCacheKindMetricFamily(writer, stats, .evictions, "antfly_lsm_cache_evictions_total", "counter", "Shared LSM cache evictions");
    try writeLsmCacheKindMetricFamily(writer, stats, .invalidations, "antfly_lsm_cache_invalidations_total", "counter", "Shared LSM cache invalidations");
    try writeLsmCacheKindMetricFamily(writer, stats, .waits, "antfly_lsm_cache_waits_total", "counter", "Shared LSM cache pending-load waits");
}

fn writeLsmNativeStorageMetrics(writer: *std.Io.Writer, stats: ?lsm_backend_mod.NativeStorageStats) !void {
    const value = stats orelse lsm_backend_mod.NativeStorageStats{};
    try health_metrics.appendPromMetric(writer, "antfly_lsm_native_fd_cache_entries", "gauge", "Native LSM storage file descriptors currently retained in the storage IO cache", @intCast(value.fd_cache_entries));
    try health_metrics.appendPromMetric(writer, "antfly_lsm_native_fd_cache_capacity", "gauge", "Maximum native LSM storage file descriptors retained in the storage IO cache", @intCast(value.fd_cache_capacity));
}

fn writeProcessMemoryMetrics(writer: *std.Io.Writer, stats: process_memory_mod.Stats) !void {
    try health_metrics.appendPromMetric(writer, "antfly_process_memory_available", "gauge", "Whether process memory metrics are available on this platform", if (stats.available) 1 else 0);
    if (!stats.available) return;
    try health_metrics.appendPromMetric(writer, "antfly_process_resident_bytes", "gauge", "Process resident bytes reported by the operating system", stats.resident_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_process_footprint_bytes", "gauge", "Process physical footprint bytes reported by the operating system", stats.footprint_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_process_wired_bytes", "gauge", "Process wired bytes reported by the operating system", stats.wired_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_process_pageins_total", "counter", "Process page-ins reported by the operating system", stats.pageins);
    try health_metrics.appendPromMetric(writer, "antfly_process_malloc_available", "gauge", "Whether process malloc zone metrics are available on this platform", if (stats.malloc_available) 1 else 0);
    if (!stats.malloc_available) return;
    try health_metrics.appendPromMetric(writer, "antfly_process_malloc_allocated_bytes", "gauge", "Live bytes allocated across process malloc zones", stats.malloc_allocated_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_process_malloc_zone_bytes", "gauge", "Bytes reserved by process malloc zones", stats.malloc_zone_bytes);
}

const LsmCacheMetricField = enum {
    hits,
    misses,
    inserts,
    evictions,
    invalidations,
    waits,
};

fn writeLsmCacheKindMetricFamily(
    writer: *std.Io.Writer,
    stats: lsm_backend_mod.CacheStats,
    field: LsmCacheMetricField,
    name: []const u8,
    metric_type: []const u8,
    help: []const u8,
) !void {
    try health_metrics.appendPromMetricHeader(writer, name, metric_type, help);
    try appendLsmCacheKindSample(writer, name, "run_state", stats.run_state, field);
    try appendLsmCacheKindSample(writer, name, "run_table_raw", stats.run_table_raw, field);
    try appendLsmCacheKindSample(writer, name, "run_table_index", stats.run_table_index, field);
    try appendLsmCacheKindSample(writer, name, "run_table_block", stats.run_table_block, field);
    try appendLsmCacheKindSample(writer, name, "run_table_physical_block", stats.run_table_physical_block, field);
}

fn appendLsmCacheKindSample(
    writer: *std.Io.Writer,
    name: []const u8,
    kind: []const u8,
    stats: lsm_backend_mod.CacheKindStats,
    field: LsmCacheMetricField,
) !void {
    try health_metrics.appendPromSampleLabeled(writer, name, &.{
        .{ .name = "kind", .value = kind },
    }, lsmCacheMetricValue(stats, field));
}

fn lsmCacheMetricValue(stats: lsm_backend_mod.CacheKindStats, field: LsmCacheMetricField) u64 {
    return switch (field) {
        .hits => stats.hits,
        .misses => stats.misses,
        .inserts => stats.inserts,
        .evictions => stats.evictions,
        .invalidations => stats.invalidations,
        .waits => stats.waits,
    };
}

fn pressureValue(pressure: resource_manager_mod.Pressure) u64 {
    return switch (pressure) {
        .normal => 0,
        .soft => 1,
        .hard => 2,
    };
}

const LocalGroupStatusCache = struct {
    valid: bool = false,
    fingerprint: u64 = 0,
    generation: u64 = 0,
    collected_at_ms: u64 = 0,
    group_statuses: []antfly.metadata.table_manager.GroupStatusReport = &.{},

    fn clear(self: *LocalGroupStatusCache, alloc: std.mem.Allocator) void {
        if (self.group_statuses.len > 0) antfly.metadata.table_manager.freeGroupStatuses(alloc, self.group_statuses);
        self.* = .{};
    }
};

const LocalSplitKeyCache = struct {
    entries: std.ArrayListUnmanaged(Entry) = .empty,

    const Entry = struct {
        group_id: u64,
        lsm_root_generation: u64,
        change_generation: u64,
        split_key: ?[]u8 = null,

        fn deinit(self: *Entry, alloc: std.mem.Allocator) void {
            if (self.split_key) |key| alloc.free(key);
            self.* = undefined;
        }
    };

    fn deinit(self: *LocalSplitKeyCache, alloc: std.mem.Allocator) void {
        self.clear(alloc);
        self.entries.deinit(alloc);
        self.* = .{};
    }

    fn clear(self: *LocalSplitKeyCache, alloc: std.mem.Allocator) void {
        for (self.entries.items) |*entry| entry.deinit(alloc);
        self.entries.clearRetainingCapacity();
    }

    fn snapshot(
        self: *const LocalSplitKeyCache,
        alloc: std.mem.Allocator,
        group_id: u64,
        lsm_root_generation: u64,
        change_generation: u64,
    ) !?SplitKeySnapshot {
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id) continue;
            if (entry.lsm_root_generation != lsm_root_generation) continue;
            if (entry.change_generation != change_generation) continue;
            return .{
                .split_key = if (entry.split_key) |key| try alloc.dupe(u8, key) else null,
            };
        }
        return null;
    }

    fn put(
        self: *LocalSplitKeyCache,
        alloc: std.mem.Allocator,
        group_id: u64,
        lsm_root_generation: u64,
        change_generation: u64,
        split_key: ?[]const u8,
    ) !void {
        const owned_key = if (split_key) |key| try alloc.dupe(u8, key) else null;
        errdefer if (owned_key) |key| alloc.free(key);
        for (self.entries.items) |*entry| {
            if (entry.group_id != group_id) continue;
            if (entry.split_key) |old_key| alloc.free(old_key);
            entry.* = .{
                .group_id = group_id,
                .lsm_root_generation = lsm_root_generation,
                .change_generation = change_generation,
                .split_key = owned_key,
            };
            return;
        }
        try self.entries.append(alloc, .{
            .group_id = group_id,
            .lsm_root_generation = lsm_root_generation,
            .change_generation = change_generation,
            .split_key = owned_key,
        });
    }

    const SplitKeySnapshot = struct {
        split_key: ?[]u8,

        fn deinit(self: *SplitKeySnapshot, alloc: std.mem.Allocator) void {
            if (self.split_key) |key| alloc.free(key);
            self.* = undefined;
        }
    };
};

const CachedSplitKey = union(enum) {
    key: []u8,
    missing,
};

const StoreStatusHeartbeatCache = struct {
    live: bool = true,
    health_class: []const u8 = "healthy",
    owns_health_class: bool = false,
    capacity_bytes: u64 = 0,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    active_backfills: u32 = 0,
    backfill_progress_millis: u16 = 1000,
    group_statuses: []antfly.metadata.table_manager.GroupStatusReport = &.{},
    runtime_statuses: []antfly.metadata.table_manager.RuntimeGroupStatusReport = &.{},

    fn clear(self: *StoreStatusHeartbeatCache, alloc: std.mem.Allocator) void {
        if (self.group_statuses.len > 0) antfly.metadata.table_manager.freeGroupStatuses(alloc, self.group_statuses);
        if (self.runtime_statuses.len > 0) antfly.metadata.table_manager.freeRuntimeGroupStatusReports(alloc, self.runtime_statuses);
        if (self.owns_health_class) alloc.free(self.health_class);
        self.* = .{};
    }
};

const RuntimeStatusDiskUsageCacheEntry = struct {
    disk_bytes: u64 = 0,
    checked_at_ns: u64 = 0,
};

const OwnedLocalGroupStatusRefresh = struct {
    alloc: std.mem.Allocator,
    server: *DataServer,
    generation: u64,
    fingerprint: u64,
    replica_root_dir: []u8,
    group_ids: []u64,
    tables: []antfly.metadata.table_manager.TableRecord,
    ranges: []antfly.metadata.table_manager.RangeRecord,
    stores: []antfly.metadata.table_manager.StoreRecord,
    merged_group_statuses: []antfly.metadata.reconciler.MergedGroupStatus,
    split_transitions: []antfly.metadata.transition_state.SplitTransitionRecord,
    merge_transitions: []antfly.metadata.transition_state.MergeTransitionRecord,
    split_observations: []antfly.metadata.transition_state.SplitObservationRecord,
    merge_observations: []antfly.metadata.transition_state.MergeObservationRecord,
    inferred_group_leadership_source: ?OwnedInferredSnapshotLeadershipSource = null,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,

    fn init(
        alloc: std.mem.Allocator,
        server: *DataServer,
        generation: u64,
        fingerprint: u64,
        replica_root_dir: []const u8,
        group_ids: []const u64,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        stores: []const antfly.metadata.table_manager.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        split_transitions: []const antfly.metadata.transition_state.SplitTransitionRecord,
        merge_transitions: []const antfly.metadata.transition_state.MergeTransitionRecord,
        split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
        merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
        inferred_group_leadership: ?InferredSnapshotLeadershipConfig,
        group_leadership_source: ?GroupLeadershipSource,
        group_membership_source: ?GroupMembershipSource,
    ) !@This() {
        const owned_replica_root_dir = try alloc.dupe(u8, replica_root_dir);
        errdefer alloc.free(owned_replica_root_dir);
        const owned_group_ids = try alloc.dupe(u64, group_ids);
        errdefer alloc.free(owned_group_ids);
        const owned_tables = try cloneTablesOwned(alloc, tables);
        errdefer {
            for (owned_tables) |record| antfly.metadata.table_manager.freeTable(alloc, record);
            alloc.free(owned_tables);
        }
        const owned_ranges = try cloneRangesOwned(alloc, ranges);
        errdefer {
            for (owned_ranges) |record| antfly.metadata.table_manager.freeRange(alloc, record);
            alloc.free(owned_ranges);
        }
        const owned_stores = try cloneStoresOwned(alloc, stores);
        errdefer {
            for (owned_stores) |record| antfly.metadata.table_manager.freeStore(alloc, record);
            alloc.free(owned_stores);
        }
        const owned_merged_group_statuses = try cloneMergedGroupStatusesOwned(alloc, merged_group_statuses);
        errdefer alloc.free(owned_merged_group_statuses);
        const owned_split_transitions = try cloneSplitTransitionsOwned(alloc, split_transitions);
        errdefer {
            for (owned_split_transitions) |record| antfly.metadata.table_manager.freeSplitTransitionRecord(alloc, record);
            alloc.free(owned_split_transitions);
        }
        const owned_merge_transitions = try cloneMergeTransitionsOwned(alloc, merge_transitions);
        errdefer {
            for (owned_merge_transitions) |record| antfly.metadata.table_manager.freeMergeTransitionRecord(alloc, record);
            alloc.free(owned_merge_transitions);
        }
        const owned_split_observations = try cloneSplitObservationsOwned(alloc, split_observations);
        errdefer if (owned_split_observations.len > 0) alloc.free(owned_split_observations);
        const owned_merge_observations = try cloneMergeObservationsOwned(alloc, merge_observations);
        errdefer if (owned_merge_observations.len > 0) alloc.free(owned_merge_observations);
        const owned_inferred_group_leadership = if (inferred_group_leadership) |cfg|
            try OwnedInferredSnapshotLeadershipSource.init(
                alloc,
                cfg.local_node_id,
                cfg.local_store_id,
                owned_stores,
                owned_merged_group_statuses,
                cfg.placement_intents,
            )
        else
            null;
        errdefer if (owned_inferred_group_leadership) |*source| source.deinit(alloc);

        return .{
            .alloc = alloc,
            .server = server,
            .generation = generation,
            .fingerprint = fingerprint,
            .replica_root_dir = owned_replica_root_dir,
            .group_ids = owned_group_ids,
            .tables = owned_tables,
            .ranges = owned_ranges,
            .stores = owned_stores,
            .merged_group_statuses = owned_merged_group_statuses,
            .split_transitions = owned_split_transitions,
            .merge_transitions = owned_merge_transitions,
            .split_observations = owned_split_observations,
            .merge_observations = owned_merge_observations,
            .inferred_group_leadership_source = owned_inferred_group_leadership,
            .group_leadership_source = group_leadership_source,
            .group_membership_source = group_membership_source,
        };
    }

    fn effectiveGroupLeadershipSource(self: *@This()) ?GroupLeadershipSource {
        if (self.inferred_group_leadership_source) |*source| return source.iface();
        return self.group_leadership_source;
    }

    fn deinit(self: *@This()) void {
        self.alloc.free(self.replica_root_dir);
        self.alloc.free(self.group_ids);
        for (self.tables) |record| antfly.metadata.table_manager.freeTable(self.alloc, record);
        self.alloc.free(self.tables);
        for (self.ranges) |record| antfly.metadata.table_manager.freeRange(self.alloc, record);
        self.alloc.free(self.ranges);
        for (self.stores) |record| antfly.metadata.table_manager.freeStore(self.alloc, record);
        self.alloc.free(self.stores);
        self.alloc.free(self.merged_group_statuses);
        for (self.split_transitions) |record| antfly.metadata.table_manager.freeSplitTransitionRecord(self.alloc, record);
        self.alloc.free(self.split_transitions);
        for (self.merge_transitions) |record| antfly.metadata.table_manager.freeMergeTransitionRecord(self.alloc, record);
        self.alloc.free(self.merge_transitions);
        if (self.split_observations.len > 0) self.alloc.free(self.split_observations);
        if (self.merge_observations.len > 0) self.alloc.free(self.merge_observations);
        if (self.inferred_group_leadership_source) |*source| source.deinit(self.alloc);
        self.* = undefined;
    }
};

const InferredSnapshotLeadershipConfig = struct {
    local_node_id: u64,
    local_store_id: u64,
    placement_intents: []const antfly.raft.reconciler.PlacementIntent,
};

const OwnedInferredSnapshotLeadershipSource = struct {
    local_node_id: u64,
    local_store_id: u64,
    stores: []const antfly.metadata.StoreRecord,
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    placement_intents: []antfly.raft.reconciler.PlacementIntent,

    fn init(
        alloc: std.mem.Allocator,
        local_node_id: u64,
        local_store_id: u64,
        stores: []const antfly.metadata.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        placement_intents: []const antfly.raft.reconciler.PlacementIntent,
    ) !@This() {
        return .{
            .local_node_id = local_node_id,
            .local_store_id = local_store_id,
            .stores = stores,
            .merged_group_statuses = merged_group_statuses,
            .placement_intents = try clonePlacementIntentsOwned(alloc, placement_intents),
        };
    }

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.placement_intents) |intent| if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
        alloc.free(self.placement_intents);
        self.* = undefined;
    }

    fn iface(self: *const @This()) GroupLeadershipSource {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .is_local_leader = isLocalLeader,
            },
        };
    }

    fn isLocalLeader(ptr: *anyopaque, group_id: u64) bool {
        const self: *const @This() = @ptrCast(@alignCast(ptr));
        var source = InferredSnapshotLeadershipSource.init(
            self.local_node_id,
            self.local_store_id,
            self.stores,
            self.merged_group_statuses,
            self.placement_intents,
        );
        return source.iface().isLocalLeader(group_id);
    }
};

pub const DataServerConfig = struct {
    bind_host: []const u8 = "127.0.0.1",
    bind_port: u16 = 0,
    raft_bind_host: []const u8 = "127.0.0.1",
    raft_bind_port: u16 = 0,
    enable_data_raft: bool = true,
    data_raft_state_backend: antfly.raft.ReplicaStateBackend = .wal,
    replica_root_dir: []const u8,
    replica_catalog_path: ?[]const u8 = null,
    snapshot_root_dir: ?[]const u8 = null,
    store_registration: ?StoreRegistrationConfig = null,
    group_leadership_source: ?GroupLeadershipSource = null,
    group_membership_source: ?GroupMembershipSource = null,
    query_async_limit: std.Io.Limit = .limited(8),
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    api_server_cfg: antfly.public_api.http_server.ApiHttpServerConfig = .{},
};

pub const StoreRegistrationConfig = struct {
    node_id: u64,
    store_id: u64,
    api_url: []const u8 = "",
    raft_url: []const u8 = "",
    role: []const u8 = "data",
    failure_domain: []const u8 = "",
};

pub const GroupLeadershipSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        is_local_leader: *const fn (ptr: *anyopaque, group_id: u64) bool,
    };

    pub fn isLocalLeader(self: GroupLeadershipSource, group_id: u64) bool {
        return self.vtable.is_local_leader(self.ptr, group_id);
    }

    pub fn fromManagedHostService(service: *antfly.raft.ManagedHostService) GroupLeadershipSource {
        return .{
            .ptr = service,
            .vtable = &.{
                .is_local_leader = struct {
                    fn isLocalLeader(ptr: *anyopaque, group_id: u64) bool {
                        const svc: *antfly.raft.ManagedHostService = @ptrCast(@alignCast(ptr));
                        return svc.host.host.isLocalLeader(group_id);
                    }
                }.isLocalLeader,
            },
        };
    }

    pub fn fromManagedHttpHostService(service: *antfly.raft.ManagedHttpHostService) GroupLeadershipSource {
        return .{
            .ptr = service,
            .vtable = &.{
                .is_local_leader = struct {
                    fn isLocalLeader(ptr: *anyopaque, group_id: u64) bool {
                        const svc: *antfly.raft.ManagedHttpHostService = @ptrCast(@alignCast(ptr));
                        return svc.host.http_host.host.isLocalLeader(group_id);
                    }
                }.isLocalLeader,
            },
        };
    }
};

pub const GroupMembership = struct {
    local_voter: bool = false,
    voter_count: u16 = 0,
    joint_consensus: bool = false,
};

pub const GroupMembershipSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        membership: *const fn (ptr: *anyopaque, group_id: u64) GroupMembership,
    };

    pub fn membership(self: GroupMembershipSource, group_id: u64) GroupMembership {
        return self.vtable.membership(self.ptr, group_id);
    }

    pub fn fromManagedHostService(service: *antfly.raft.ManagedHostService) GroupMembershipSource {
        return .{
            .ptr = service,
            .vtable = &.{
                .membership = struct {
                    fn membership(ptr: *anyopaque, group_id: u64) GroupMembership {
                        const svc: *antfly.raft.ManagedHostService = @ptrCast(@alignCast(ptr));
                        const raft_status = svc.host.host.raftStatus(group_id) orelse return .{};
                        var local_voter = false;
                        for (raft_status.conf_state.voters) |node_id| {
                            if (node_id == svc.host.host.cfg.local_node_id) {
                                local_voter = true;
                                break;
                            }
                        }
                        return .{
                            .local_voter = local_voter,
                            .voter_count = @intCast(raft_status.conf_state.voters.len),
                            .joint_consensus = raft_status.conf_state.voters_outgoing.len > 0,
                        };
                    }
                }.membership,
            },
        };
    }

    pub fn fromManagedHttpHostService(service: *antfly.raft.ManagedHttpHostService) GroupMembershipSource {
        return .{
            .ptr = service,
            .vtable = &.{
                .membership = struct {
                    fn membership(ptr: *anyopaque, group_id: u64) GroupMembership {
                        const svc: *antfly.raft.ManagedHttpHostService = @ptrCast(@alignCast(ptr));
                        const raft_status = svc.host.http_host.host.raftStatus(group_id) orelse return .{};
                        var local_voter = false;
                        for (raft_status.conf_state.voters) |node_id| {
                            if (node_id == svc.host.http_host.host.cfg.local_node_id) {
                                local_voter = true;
                                break;
                            }
                        }
                        return .{
                            .local_voter = local_voter,
                            .voter_count = @intCast(raft_status.conf_state.voters.len),
                            .joint_consensus = raft_status.conf_state.voters_outgoing.len > 0,
                        };
                    }
                }.membership,
            },
        };
    }
};

const InferredSnapshotLeadershipSource = struct {
    local_node_id: u64,
    local_store_id: u64,
    stores: []const antfly.metadata.StoreRecord,
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    placement_intents: []const antfly.raft.reconciler.PlacementIntent,

    fn init(
        local_node_id: u64,
        local_store_id: u64,
        stores: []const antfly.metadata.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        placement_intents: []const antfly.raft.reconciler.PlacementIntent,
    ) InferredSnapshotLeadershipSource {
        return .{
            .local_node_id = local_node_id,
            .local_store_id = local_store_id,
            .stores = stores,
            .merged_group_statuses = merged_group_statuses,
            .placement_intents = placement_intents,
        };
    }

    fn iface(self: *const InferredSnapshotLeadershipSource) GroupLeadershipSource {
        return .{
            .ptr = @constCast(self),
            .vtable = &.{
                .is_local_leader = isLocalLeader,
            },
        };
    }

    fn isLocalLeader(ptr: *anyopaque, group_id: u64) bool {
        const self: *const InferredSnapshotLeadershipSource = @ptrCast(@alignCast(ptr));
        if (self.explicitLeaderStoreId(group_id)) |leader_store_id| {
            return leader_store_id == self.local_store_id;
        }
        if (self.stores.len == 1 and self.stores[0].store_id == self.local_store_id) return true;

        var placement_count: usize = 0;
        var local_match = false;
        for (self.placement_intents) |intent| {
            if (intent.record.group_id != group_id) continue;
            placement_count += 1;
            if (intent.record.local_node_id == self.local_node_id or intent.store_id == self.local_store_id) {
                local_match = true;
            }
        }
        return placement_count == 1 and local_match;
    }

    fn explicitLeaderStoreId(self: *const InferredSnapshotLeadershipSource, group_id: u64) ?u64 {
        if (findMergedSnapshotGroupStatus(self.merged_group_statuses, group_id)) |status| {
            if (status.leader_known) return status.leader_store_id;
            return null;
        }
        var leader_store_id: ?u64 = null;
        for (self.stores) |store| {
            if (!store.live) continue;
            if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
            for (store.group_statuses) |group_status| {
                if (group_status.group_id != group_id) continue;
                if (!group_status.local_leader) continue;
                if (leader_store_id == null) {
                    leader_store_id = store.store_id;
                    continue;
                }
                if (leader_store_id.? != store.store_id) return null;
            }
        }
        return leader_store_id;
    }
};

pub const DataServer = struct {
    alloc: std.mem.Allocator,
    remote_metadata: ?*RemoteMetadataSource = null,
    metadata_service: ?*antfly.metadata_service.MetadataService = null,
    metadata_http_service: ?*antfly.metadata_service.MetadataHttpService = null,
    data_raft: ?*antfly.raft.ManagedHttpHostService = null,
    data_raft_mutex: std.atomic.Mutex = .unlocked,
    data_raft_factory: ?*DataDescriptorFactory = null,
    data_raft_store: ?*raft_engine.core.MemoryStorage = null,
    data_raft_apply: ?*RaftTableApplyStateMachine = null,
    data_raft_base_uri: ?[]u8 = null,
    metadata_local_providers_registered: bool = false,
    store_registration: ?StoreRegistrationConfig = null,
    store_registration_confirmed: bool = false,
    group_leadership_source: ?GroupLeadershipSource = null,
    group_membership_source: ?GroupMembershipSource = null,
    local_transition_runtime: ?antfly.raft.TransitionRuntime = null,
    store_status_ticks: usize = 0,
    store_status_dirty: bool = true,
    last_store_status_report_at_ms: u64 = 0,
    last_data_raft_metadata_sync_at_ms: u64 = 0,
    last_data_raft_placement_fingerprint: ?u64 = null,
    provision_ticks: usize = 0,
    last_provision_fingerprint: ?u64 = null,
    last_provision_metadata_epoch: ?u64 = null,
    last_provision_head_check_at_ms: u64 = 0,
    provisioned_root_refresh_mutex: std.atomic.Mutex = .unlocked,
    provisioned_root_refresh_thread: ?std.Thread = null,
    provisioned_root_refresh_active: std.atomic.Value(bool) = .init(false),
    provisioned_root_refresh_dirty: std.atomic.Value(bool) = .init(true),
    provisioned_root_refresh_started: std.atomic.Value(u64) = .init(0),
    provisioned_root_refresh_completed: std.atomic.Value(u64) = .init(0),
    provisioned_root_refresh_failed: std.atomic.Value(u64) = .init(0),
    provisioned_root_refresh_last_run_at_ms: std.atomic.Value(u64) = .init(0),
    provisioned_root_refresh_last_duration_ns: std.atomic.Value(u64) = .init(0),
    local_group_status_generation: std.atomic.Value(u64) = .init(1),
    local_group_status_cache_mutex: std.atomic.Mutex = .unlocked,
    local_group_status_cache: LocalGroupStatusCache = .{},
    local_group_status_refresh_mutex: std.atomic.Mutex = .unlocked,
    local_group_status_refresh_thread: ?std.Thread = null,
    local_group_status_refresh_active: std.atomic.Value(bool) = .init(false),
    runtime_status_refresh_mutex: std.atomic.Mutex = .unlocked,
    runtime_status_refresh_thread: ?std.Thread = null,
    runtime_status_refresh_active: std.atomic.Value(bool) = .init(false),
    runtime_status_refresh_started: std.atomic.Value(u64) = .init(0),
    runtime_status_refresh_completed: std.atomic.Value(u64) = .init(0),
    runtime_status_refresh_failed: std.atomic.Value(u64) = .init(0),
    runtime_status_refresh_last_table_count: std.atomic.Value(u64) = .init(0),
    runtime_status_refresh_last_group_count: std.atomic.Value(u64) = .init(0),
    runtime_status_refresh_last_db_opens: std.atomic.Value(u64) = .init(0),
    runtime_status_refresh_last_skipped_db_opens: std.atomic.Value(u64) = .init(0),
    runtime_status_refresh_last_placeholder_group_count: std.atomic.Value(u64) = .init(0),
    runtime_status_refresh_last_duration_ns: std.atomic.Value(u64) = .init(0),
    local_split_key_generation: std.atomic.Value(u64) = .init(1),
    local_split_key_cache_mutex: std.atomic.Mutex = .unlocked,
    local_split_key_cache: LocalSplitKeyCache = .{},
    auto_bulk_finish_mutex: std.atomic.Mutex = .unlocked,
    auto_bulk_finish_io: ?std.Io.Threaded = null,
    auto_bulk_finish_future: ?std.Io.Future(void) = null,
    auto_bulk_finish_stop: std.atomic.Value(bool) = .init(false),
    auto_bulk_finish_active: std.atomic.Value(bool) = .init(false),
    auto_bulk_finish_started: std.atomic.Value(u64) = .init(0),
    auto_bulk_finish_completed: std.atomic.Value(u64) = .init(0),
    auto_bulk_finish_failed: std.atomic.Value(u64) = .init(0),
    auto_bulk_finish_lock_deferred: std.atomic.Value(u64) = .init(0),
    auto_bulk_finish_last_duration_ns: std.atomic.Value(u64) = .init(0),
    auto_bulk_finish_last_run_at_ms: std.atomic.Value(u64) = .init(0),
    provisioned_warmup_mutex: std.atomic.Mutex = .unlocked,
    provisioned_warmup_thread: ?std.Thread = null,
    provisioned_warmup_active: std.atomic.Value(bool) = .init(false),
    provisioned_warmup_started: std.atomic.Value(u64) = .init(0),
    provisioned_warmup_completed: std.atomic.Value(u64) = .init(0),
    provisioned_warmup_failed: std.atomic.Value(u64) = .init(0),
    provisioned_warmup_last_group_count: std.atomic.Value(u64) = .init(0),
    provisioned_warmup_last_duration_ns: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_mutex: std.atomic.Mutex = .unlocked,
    provisioned_startup_catch_up_thread: ?std.Thread = null,
    provisioned_startup_catch_up_active: std.atomic.Value(bool) = .init(false),
    provisioned_startup_catch_up_target_mutex: std.atomic.Mutex = .unlocked,
    provisioned_startup_catch_up_target_group_id: u64 = 0,
    provisioned_startup_catch_up_target_table_name: ?[]u8 = null,
    provisioned_startup_catch_up_dirty: std.atomic.Value(bool) = .init(true),
    provisioned_startup_catch_up_started: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_completed: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_failed: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_last_group_count: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_last_groups_with_debt: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_last_groups_cleared: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_last_busy_groups: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_last_duration_ns: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_last_run_at_ms: std.atomic.Value(u64) = .init(0),
    runtime_status_dirty: std.atomic.Value(bool) = .init(true),
    runtime_status_last_refresh_at_ms: std.atomic.Value(u64) = .init(0),
    runtime_status_disk_usage_cache_mutex: std.atomic.Mutex = .unlocked,
    runtime_status_disk_usage_cache: std.AutoHashMapUnmanaged(u64, RuntimeStatusDiskUsageCacheEntry) = .empty,
    store_status_cache_mutex: std.atomic.Mutex = .unlocked,
    store_status_heartbeat_cache: StoreStatusHeartbeatCache = .{},
    provisioned_storage: antfly.public_api.ProvisionedGroupStorage,
    read_source: antfly.public_api.ProvisionedTableReadSource,
    write_source: antfly.public_api.ProvisionedTableWriteSource,
    status_source: antfly.public_api.http_server.StatusSource,
    http_server: ?antfly.public_api.ApiHttpServer = null,
    api_server_cfg: antfly.public_api.http_server.ApiHttpServerConfig,
    query_async_limit: std.Io.Limit,
    backend_runtime_mutex: std.atomic.Mutex = .unlocked,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    owned_backend_runtime: ?backend_runtime_mod.BackendRuntimeHandle = null,
    listener_cfg: antfly.raft.transport.std_http_listener.StdHttpListenerConfig,
    listener: ?antfly.raft.transport.std_http_listener.StdHttpListener = null,
    query_io_impl: ?std.Io.Threaded = null,
    lsm_maintenance_thread: ?std.Thread = null,
    lsm_maintenance_stop: std.atomic.Value(bool) = .init(false),
    lsm_maintenance_wake: std.atomic.Value(bool) = .init(false),
    lsm_maintenance_active: std.atomic.Value(bool) = .init(false),
    lsm_maintenance_started: std.atomic.Value(u64) = .init(0),
    lsm_maintenance_completed: std.atomic.Value(u64) = .init(0),
    lsm_maintenance_failed: std.atomic.Value(u64) = .init(0),
    lsm_maintenance_capacity_denied: std.atomic.Value(u64) = .init(0),
    lsm_maintenance_bulk_deferred: std.atomic.Value(u64) = .init(0),
    lsm_maintenance_lock_deferred: std.atomic.Value(u64) = .init(0),
    lsm_maintenance_next_eligible_ns: std.atomic.Value(u64) = .init(0),

    const lsm_maintenance_worker_idle_sleep_ns = 250 * std.time.ns_per_ms;
    const lsm_maintenance_worker_retry_sleep_ns = 100 * std.time.ns_per_ms;
    const lsm_maintenance_worker_bulk_defer_ns = 500 * std.time.ns_per_ms;
    const lsm_maintenance_worker_pressure_defer_ns = 500 * std.time.ns_per_ms;
    const lsm_maintenance_worker_max_steps_per_wake = 8;

    const ProvisionedWarmupStats = struct {
        warmed_group_count: u64 = 0,
        duration_ns: u64 = 0,
    };

    const ProvisionedStartupCatchUpStats = struct {
        group_count: u64 = 0,
        groups_with_debt: u64 = 0,
        groups_cleared: u64 = 0,
        busy_groups: u64 = 0,
        debt_remaining: bool = false,
        duration_ns: u64 = 0,
    };

    const StartupCatchUpGroupDisposition = enum {
        attempt,
        skip_nonlocal,
        retry_unknown,
    };

    const RuntimeStatusRefreshStats = struct {
        table_count: u64 = 0,
        group_count: u64 = 0,
        db_opens: u64 = 0,
        skipped_db_opens: u64 = 0,
        placeholder_group_count: u64 = 0,
        duration_ns: u64 = 0,
    };

    const RuntimeStatusRefreshBudget = struct {
        max_db_opens: usize,
        db_opens: usize = 0,
        skipped_db_opens: usize = 0,
        placeholder_group_count: usize = 0,

        fn canOpenDb(self: *const @This()) bool {
            return self.db_opens < self.max_db_opens;
        }

        fn recordOpen(self: *@This()) void {
            self.db_opens += 1;
        }

        fn recordSkippedOpen(self: *@This()) void {
            self.skipped_db_opens += 1;
        }

        fn recordPlaceholder(self: *@This()) void {
            self.placeholder_group_count += 1;
        }
    };

    const ActiveStartupCatchUpTarget = struct {
        group_id: u64,
        table_name: []u8,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
            self.* = undefined;
        }
    };
    const lsm_maintenance_background_reservation_bytes: u64 = 32 * 1024 * 1024;

    pub fn initFromLocalMetadataSources(
        alloc: std.mem.Allocator,
        cfg: DataServerConfig,
        catalog: antfly.public_api.table_catalog.CatalogSource,
        status_source: antfly.public_api.http_server.StatusSource,
    ) DataServer {
        return .{
            .alloc = alloc,
            .store_registration = cfg.store_registration,
            .group_leadership_source = cfg.group_leadership_source,
            .group_membership_source = cfg.group_membership_source,
            .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
            .read_source = antfly.public_api.ProvisionedTableReadSource.init(
                cfg.replica_root_dir,
                catalog,
                antfly.raft.read_gate.noopReadableLeaseRequester(),
            ),
            .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
                cfg.replica_root_dir,
                catalog,
            ),
            .status_source = status_source,
            .api_server_cfg = cfg.api_server_cfg,
            .query_async_limit = cfg.query_async_limit,
            .backend_runtime = cfg.backend_runtime,
            .listener_cfg = publicApiListenerConfig(cfg.bind_host, cfg.bind_port),
        };
    }

    pub fn initFromMetadataService(
        alloc: std.mem.Allocator,
        cfg: DataServerConfig,
        svc: *antfly.metadata_service.MetadataService,
    ) DataServer {
        return .{
            .alloc = alloc,
            .metadata_service = svc,
            .store_registration = cfg.store_registration,
            .group_leadership_source = cfg.group_leadership_source orelse GroupLeadershipSource.fromManagedHostService(&svc.raft),
            .group_membership_source = cfg.group_membership_source orelse GroupMembershipSource.fromManagedHostService(&svc.raft),
            .local_transition_runtime = svc.raft.local_transition_runtime,
            .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
            .read_source = antfly.public_api.ProvisionedTableReadSource.init(
                cfg.replica_root_dir,
                antfly.public_api.table_catalog.CatalogSource.fromMetadataService(svc),
                antfly.raft.read_gate.noopReadableLeaseRequester(),
            ),
            .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
                cfg.replica_root_dir,
                antfly.public_api.table_catalog.CatalogSource.fromMetadataService(svc),
            ),
            .status_source = antfly.public_api.http_server.StatusSource.fromMetadataService(svc),
            .api_server_cfg = cfg.api_server_cfg,
            .query_async_limit = cfg.query_async_limit,
            .backend_runtime = cfg.backend_runtime,
            .listener_cfg = publicApiListenerConfig(cfg.bind_host, cfg.bind_port),
        };
    }

    pub fn initFromMetadataHttpService(
        alloc: std.mem.Allocator,
        cfg: DataServerConfig,
        svc: *antfly.metadata_service.MetadataHttpService,
    ) DataServer {
        return .{
            .alloc = alloc,
            .metadata_http_service = svc,
            .store_registration = cfg.store_registration,
            .group_leadership_source = cfg.group_leadership_source orelse GroupLeadershipSource.fromManagedHttpHostService(&svc.raft),
            .group_membership_source = cfg.group_membership_source orelse GroupMembershipSource.fromManagedHttpHostService(&svc.raft),
            .local_transition_runtime = svc.raft.local_transition_runtime,
            .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
            .read_source = antfly.public_api.ProvisionedTableReadSource.init(
                cfg.replica_root_dir,
                antfly.public_api.table_catalog.CatalogSource.fromMetadataHttpService(svc),
                antfly.raft.read_gate.noopReadableLeaseRequester(),
            ),
            .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
                cfg.replica_root_dir,
                antfly.public_api.table_catalog.CatalogSource.fromMetadataHttpService(svc),
            ),
            .status_source = antfly.public_api.http_server.StatusSource.fromMetadataHttpService(svc),
            .api_server_cfg = cfg.api_server_cfg,
            .query_async_limit = cfg.query_async_limit,
            .backend_runtime = cfg.backend_runtime,
            .listener_cfg = publicApiListenerConfig(cfg.bind_host, cfg.bind_port),
        };
    }

    /// Initialize the ApiHttpServer without creating a listener.
    /// Call this when you want to use the API server with an external
    /// httpx.Server instead of the built-in StdHttpListener.
    pub fn initApiServer(self: *DataServer) void {
        if (self.http_server != null) return;
        var api_server_cfg = self.api_server_cfg;
        api_server_cfg.shard_ops = self.localShardOperationAdapter();
        api_server_cfg.shard_db_adapter = self.localShardDbAdapter();
        api_server_cfg.backend_runtime = self.backend_runtime;
        if (self.query_io_impl == null) {
            self.query_io_impl = std.Io.Threaded.init(self.alloc, .{
                .async_limit = self.query_async_limit,
            });
        }
        _ = self.read_source.withIo(&self.query_io_impl.?);
        _ = self.read_source.withSecretStore(api_server_cfg.secret_store);
        _ = self.write_source.withSecretStore(api_server_cfg.secret_store);
        _ = self.read_source.withRemoteContent(api_server_cfg.remote_content);
        _ = self.write_source.withRemoteContent(api_server_cfg.remote_content);
        if (self.backend_runtime) |runtime| {
            self.provisioned_storage.attachBackendRuntime(runtime, &self.read_source, &self.write_source);
        }
        self.provisioned_storage.attachSources(&self.read_source, &self.write_source);
        if (self.data_raft_apply) |apply_sm| {
            apply_sm.attachProvisionedStorage(&self.provisioned_storage);
            _ = apply_sm.write_source.withSecretStore(api_server_cfg.secret_store);
            apply_sm.write_cache.secret_store = api_server_cfg.secret_store;
            _ = apply_sm.write_source.withRemoteContent(api_server_cfg.remote_content);
            apply_sm.write_cache.remote_content = api_server_cfg.remote_content;
            apply_sm.write_source.setLocalChangeHook(self.localChangeHook());
        }
        self.read_source.primary_lookup_db = self.localPrimaryLookupDbSource();
        self.write_source.setLocalChangeHook(self.localChangeHook());
        _ = self.write_source.withRaftBatcher(if (self.data_raft != null) self.localRaftBatcher() else null);
        self.http_server = antfly.public_api.ApiHttpServer.init(
            self.alloc,
            api_server_cfg,
            self.status_source,
            self.read_source.source(),
            self.write_source.source(),
        );
        self.http_server.?.antfly_provider = self.read_source.antfly_provider;
    }

    pub fn setAntflyProvider(
        self: *DataServer,
        provider: ?antfly.inference.managed_embedder.AntflyProvider,
    ) void {
        _ = self.read_source.withAntflyProvider(provider);
        _ = self.write_source.withAntflyProvider(provider);
        if (self.data_raft_apply) |apply_sm| {
            _ = apply_sm.write_source.withAntflyProvider(provider);
            apply_sm.write_cache.antfly_provider = provider;
        }
        if (self.http_server) |*server| server.antfly_provider = provider;
    }

    pub fn start(self: *DataServer) !void {
        _ = try self.ensureBackendRuntime();
        self.registerMetadataLocalProviders();
        self.initApiServer();
        if (self.data_raft) |raft| {
            try raft.start();
            self.data_raft_base_uri = try raft.baseUri(self.alloc);
        }
        if (self.listener == null) {
            self.listener = antfly.raft.transport.std_http_listener.StdHttpListener.init(
                self.alloc,
                self.listener_cfg,
                self.http_server.?.executor(),
            );
        }
        self.listener.?.setStreamingExecutor(self.http_server.?.streamingExecutor());
        try self.listener.?.start();
        if (self.store_registration != null) {
            self.store_status_dirty = true;
            self.registerNodeIfConfigured() catch |err| switch (err) {
                error.HttpConnectionClosing,
                error.ConnectionResetByPeer,
                error.ConnectionRefused,
                error.BrokenPipe,
                error.EndOfStream,
                error.UnexpectedHttpStatus,
                error.NotListening,
                => std.log.warn("data node registration deferred err={}", .{err}),
                else => return err,
            };
        }
        self.requestRuntimeStatusRefresh() catch |err| switch (err) {
            error.ThreadQuotaExceeded,
            error.SystemResources,
            => std.log.warn("runtime status refresh start deferred err={}", .{err}),
            else => return err,
        };
        self.requestProvisionedStartupCatchUp() catch |err| switch (err) {
            error.ThreadQuotaExceeded,
            error.SystemResources,
            => std.log.warn("provisioned startup catch-up start deferred err={}", .{err}),
            else => return err,
        };
    }

    pub fn runRound(self: *DataServer) !void {
        if (self.data_raft) |raft| {
            lockAtomic(&self.data_raft_mutex);
            defer self.data_raft_mutex.unlock();
            try raft.runRound();
        }
        self.requestAutoBulkFinishBackground() catch |err| {
            std.log.warn("auto bulk ingest finish start deferred err={}", .{err});
        };
        self.requestLsmMaintenanceBackground() catch try self.runLsmMaintenanceForegroundRound();
        if (self.data_raft != null and self.remote_metadata != null) {
            const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
            if (self.last_data_raft_metadata_sync_at_ms == 0 or
                now_ms -| self.last_data_raft_metadata_sync_at_ms >= data_raft_metadata_sync_interval_ms)
            {
                self.last_data_raft_metadata_sync_at_ms = now_ms;
                self.syncDataRaftFromRemoteMetadata() catch |err| {
                    std.log.warn("data raft metadata sync failed err={}", .{err});
                };
            }
        }
        if (self.remote_metadata != null and self.store_registration != null) {
            if (!self.store_registration_confirmed) {
                self.registerNodeIfConfigured() catch |register_err| switch (register_err) {
                    error.HttpConnectionClosing,
                    error.ConnectionResetByPeer,
                    error.ConnectionRefused,
                    error.BrokenPipe,
                    error.EndOfStream,
                    error.UnexpectedHttpStatus,
                    error.NotListening,
                    => {},
                    else => return register_err,
                };
            }
            self.store_status_ticks += 1;
            const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
            const due_store_status_heartbeat = self.last_store_status_report_at_ms == 0 or
                now_ms -| self.last_store_status_report_at_ms >= store_status_heartbeat_interval_ms;
            const due_full_store_status = self.localGroupStatusCacheStale(now_ms);
            const due_data_raft_status_refresh = self.data_raft != null;
            if (self.store_status_ticks >= store_status_report_interval_ticks and
                (self.store_status_dirty or due_store_status_heartbeat or due_full_store_status or due_data_raft_status_refresh))
            {
                self.store_status_ticks = 0;
                const result = if (self.store_status_dirty or due_full_store_status or due_data_raft_status_refresh)
                    self.reportStoreStatus()
                else
                    self.reportStoreStatusHeartbeat();
                result catch |err| switch (err) {
                    // Split runtime can briefly observe placement before the
                    // local replica root is fully provisioned on disk.
                    error.FileNotFound,
                    error.UnknownGroup,
                    error.LmdbUnexpected,
                    error.Corrupted,
                    error.HttpConnectionClosing,
                    error.ConnectionResetByPeer,
                    error.ConnectionRefused,
                    error.BrokenPipe,
                    error.EndOfStream,
                    error.UnexpectedHttpStatus,
                    => {},
                    error.UnknownStore => {
                        self.store_registration_confirmed = false;
                        self.registerNodeIfConfigured() catch |register_err| switch (register_err) {
                            error.HttpConnectionClosing,
                            error.ConnectionResetByPeer,
                            error.ConnectionRefused,
                            error.BrokenPipe,
                            error.EndOfStream,
                            error.UnexpectedHttpStatus,
                            => {},
                            else => return register_err,
                        };
                    },
                    else => return err,
                };
            }

            self.provision_ticks += 1;
            if (self.provision_ticks >= 4) {
                self.provision_ticks = 0;
                self.maybeRequestProvisionedRootRefresh() catch |err| switch (err) {
                    // Local split-runtime provisioning can race with active writes.
                    // Treat those as transient and retry on the next provision tick.
                    error.WriterLocked,
                    error.FileNotFound,
                    error.UnknownGroup,
                    error.LmdbUnexpected,
                    error.Corrupted,
                    error.HttpConnectionClosing,
                    error.ConnectionResetByPeer,
                    error.ConnectionRefused,
                    error.BrokenPipe,
                    error.EndOfStream,
                    => {},
                    else => return err,
                };
            }
        }
        try self.maybeRequestRuntimeStatusRefresh();
        try self.maybeRequestProvisionedStartupCatchUp();
    }

    pub fn deinit(self: *DataServer) void {
        self.unregisterMetadataLocalProviders();
        self.stopLsmMaintenanceBackground();
        self.joinProvisionedRootRefreshThread();
        self.joinLocalGroupStatusRefreshThread();
        self.joinRuntimeStatusRefreshThread();
        self.joinAutoBulkFinishTask();
        self.joinProvisionedWarmupThread();
        self.joinProvisionedStartupCatchUpThread();
        self.clearProvisionedStartupCatchUpTarget();
        if (self.listener) |*listener| listener.deinit();
        if (self.http_server) |*http_server| http_server.deinit();
        if (self.data_raft) |raft| {
            raft.stop();
            raft.deinit();
            self.alloc.destroy(raft);
        }
        if (self.data_raft_factory) |factory| {
            factory.deinit();
            self.alloc.destroy(factory);
        }
        if (self.data_raft_apply) |apply_sm| {
            apply_sm.deinit();
            self.alloc.destroy(apply_sm);
        }
        if (self.data_raft_store) |store| {
            store.deinit();
            self.alloc.destroy(store);
        }
        if (self.data_raft_base_uri) |uri| self.alloc.free(uri);
        self.local_split_key_cache.deinit(self.alloc);
        self.local_group_status_cache.clear(self.alloc);
        self.runtime_status_disk_usage_cache.deinit(self.alloc);
        self.store_status_heartbeat_cache.clear(self.alloc);
        self.write_source.deinit();
        self.provisioned_storage.deinit();
        if (self.owned_backend_runtime) |*runtime| runtime.deinit();
        if (self.remote_metadata) |remote_metadata| {
            remote_metadata.deinit();
            self.alloc.destroy(remote_metadata);
        }
        if (self.query_io_impl) |*io_impl| io_impl.deinit();
        self.listener = null;
        self.http_server = null;
        self.data_raft = null;
        self.data_raft_factory = null;
        self.data_raft_apply = null;
        self.data_raft_store = null;
        self.data_raft_base_uri = null;
        self.remote_metadata = null;
        self.owned_backend_runtime = null;
        self.backend_runtime = null;
        self.query_io_impl = null;
    }

    fn ensureBackendRuntime(self: *DataServer) !*backend_runtime_mod.BackendRuntime {
        lockAtomic(&self.backend_runtime_mutex);
        defer self.backend_runtime_mutex.unlock();
        if (self.backend_runtime == null) {
            self.owned_backend_runtime = try backend_runtime_mod.BackendRuntimeHandle.init(self.alloc, .{});
            self.backend_runtime = self.owned_backend_runtime.?.ptr();
        }
        if (self.backend_runtime) |ptr| {
            self.provisioned_storage.attachBackendRuntime(ptr, &self.read_source, &self.write_source);
            if (self.data_raft_apply) |apply_sm| {
                apply_sm.write_source.backend_runtime = ptr;
                apply_sm.write_cache.backend_runtime = ptr;
            }
            return ptr;
        }
        unreachable;
    }

    fn runLsmMaintenanceForegroundRound(self: *DataServer) !void {
        _ = self.write_source.runLsmMaintenanceRound() catch |err| switch (err) {
            error.ReadOnly,
            error.FileNotFound,
            error.LmdbUnexpected,
            error.Corrupted,
            => {},
            else => return err,
        };
    }

    fn requestLsmMaintenanceBackground(self: *DataServer) !void {
        const now_ns = platform_time.monotonicNs();
        if (now_ns < self.lsm_maintenance_next_eligible_ns.load(.monotonic)) return;
        if (self.resourcePressureDefersBackgroundMaintenance()) {
            self.deferLsmMaintenance(now_ns, lsm_maintenance_worker_pressure_defer_ns);
            _ = self.lsm_maintenance_capacity_denied.fetchAdd(1, .monotonic);
            return;
        }
        if (self.write_source.lsmMaintenanceScoreBestEffort() == 0) return;
        self.lsm_maintenance_wake.store(true, .release);
        if (self.lsm_maintenance_thread == null) {
            self.lsm_maintenance_stop.store(false, .release);
            self.lsm_maintenance_thread = try std.Thread.spawn(.{}, lsmMaintenanceWorkerMain, .{self});
        }
    }

    fn stopLsmMaintenanceBackground(self: *DataServer) void {
        self.lsm_maintenance_stop.store(true, .release);
        self.lsm_maintenance_wake.store(true, .release);
        if (self.lsm_maintenance_thread) |thread| {
            thread.join();
            self.lsm_maintenance_thread = null;
        }
        self.lsm_maintenance_active.store(false, .release);
    }

    fn resourcePressureDefersBackgroundMaintenance(self: *DataServer) bool {
        return self.provisioned_storage.resource_manager.sliceStats(.lsm_compaction_work).pressure == .hard;
    }

    fn deferLsmMaintenance(self: *DataServer, now_ns: u64, delay_ns: u64) void {
        self.lsm_maintenance_next_eligible_ns.store(now_ns +| delay_ns, .release);
    }

    fn lsmMaintenanceWorkerMain(self: *DataServer) void {
        while (!self.lsm_maintenance_stop.load(.acquire)) {
            const woke = self.lsm_maintenance_wake.swap(false, .acq_rel);
            const now_ns = platform_time.monotonicNs();
            if (now_ns < self.lsm_maintenance_next_eligible_ns.load(.monotonic)) {
                sleepLsmMaintenanceWorker();
                continue;
            }
            if (!woke and self.write_source.lsmMaintenanceScoreBestEffort() == 0) {
                sleepLsmMaintenanceWorker();
                continue;
            }
            if (self.resourcePressureDefersBackgroundMaintenance()) {
                self.deferLsmMaintenance(now_ns, lsm_maintenance_worker_pressure_defer_ns);
                _ = self.lsm_maintenance_capacity_denied.fetchAdd(1, .monotonic);
                sleepLsmMaintenanceWorker();
                continue;
            }

            var reservation = self.provisioned_storage.resource_manager.reserve(.lsm_compaction_work, lsm_maintenance_background_reservation_bytes) catch {
                self.deferLsmMaintenance(now_ns, lsm_maintenance_worker_pressure_defer_ns);
                _ = self.lsm_maintenance_capacity_denied.fetchAdd(1, .monotonic);
                sleepLsmMaintenanceWorker();
                continue;
            };
            defer reservation.release();

            self.lsm_maintenance_active.store(true, .release);
            _ = self.lsm_maintenance_started.fetchAdd(1, .monotonic);
            var completed = false;
            var steps: usize = 0;
            while (steps < lsm_maintenance_worker_max_steps_per_wake and !self.lsm_maintenance_stop.load(.acquire)) : (steps += 1) {
                const did_work = self.write_source.runLsmMaintenanceRoundBestEffort() catch |err| {
                    switch (err) {
                        error.ReadOnly,
                        error.FileNotFound,
                        error.LmdbUnexpected,
                        error.Corrupted,
                        => {},
                        else => std.log.warn("lsm maintenance background round failed: {}", .{err}),
                    }
                    _ = self.lsm_maintenance_failed.fetchAdd(1, .monotonic);
                    break;
                };
                if (!did_work) {
                    if (self.write_source.lsmMaintenanceScoreBestEffort() > 0) {
                        _ = self.lsm_maintenance_lock_deferred.fetchAdd(1, .monotonic);
                        self.deferLsmMaintenance(platform_time.monotonicNs(), lsm_maintenance_worker_retry_sleep_ns);
                    }
                    completed = true;
                    break;
                }
            }
            if (steps >= lsm_maintenance_worker_max_steps_per_wake and self.write_source.lsmMaintenanceScoreBestEffort() > 0) {
                self.deferLsmMaintenance(platform_time.monotonicNs(), lsm_maintenance_worker_retry_sleep_ns);
            }
            if (completed) _ = self.lsm_maintenance_completed.fetchAdd(1, .monotonic);
            self.lsm_maintenance_active.store(false, .release);
        }
        self.lsm_maintenance_active.store(false, .release);
    }

    fn sleepLsmMaintenanceWorker() void {
        var req = std.posix.timespec{
            .sec = @intCast(@divTrunc(lsm_maintenance_worker_idle_sleep_ns, std.time.ns_per_s)),
            .nsec = @intCast(@mod(lsm_maintenance_worker_idle_sleep_ns, std.time.ns_per_s)),
        };
        while (true) {
            const err = std.posix.errno(std.posix.system.nanosleep(&req, &req));
            switch (err) {
                .SUCCESS => return,
                .INTR => continue,
                else => {
                    std.Thread.yield() catch {};
                    return;
                },
            }
        }
    }

    pub fn baseUri(self: *DataServer, alloc: std.mem.Allocator) ![]u8 {
        const listener = self.listener orelse return error.NotListening;
        return try listener.baseUri(alloc);
    }

    fn localShardOperationAdapter(self: *DataServer) antfly.raft.ShardOperationAdapter {
        return .{
            .ptr = self,
            .vtable = &.{
                .observe_split = localObserveSplit,
                .observe_merge = localObserveMerge,
                .prepare_split_source = localPrepareSplitSource,
                .start_split_source = localStartSplitSource,
                .bootstrap_split_destination = localBootstrapSplitDestination,
                .catch_up_split_destination = localCatchUpSplitDestination,
                .finalize_split_source = localFinalizeSplitSource,
                .rollback_split = localRollbackSplit,
                .accept_merge_receiver = localAcceptMergeReceiver,
                .catch_up_merge_receiver = localCatchUpMergeReceiver,
                .finalize_merge = localFinalizeMerge,
                .rollback_merge = localRollbackMerge,
            },
        };
    }

    pub fn localShardDbAdapter(self: *DataServer) antfly.metadata.ShardDbAdapter {
        return .{
            .ptr = self,
            .vtable = &.{
                .fetch_median_key = localFetchMedianKey,
                .schema_index_ready = localSchemaIndexReady,
            },
        };
    }

    fn localRaftBatcher(self: *DataServer) antfly.public_api.table_writes.RaftBatcher {
        return .{
            .ptr = self,
            .vtable = &.{
                .batch_group = localRaftBatchGroup,
                .batch_group_local = localRaftBatchGroupLocal,
            },
        };
    }

    fn localPrimaryLookupDbSource(self: *DataServer) antfly.public_api.table_reads.PrimaryLookupDbSource {
        return .{
            .ptr = self,
            .lease_group = localPrimaryLookupDbLeaseGroup,
        };
    }

    fn localPrimaryLookupDbLeaseGroup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        lsm_root_generation: u64,
    ) !?antfly.public_api.table_reads.PrimaryLookupDbLease {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.data_raft_apply) |apply_sm| {
            const apply_source = apply_sm.write_source.primaryLookupDbSource();
            if (try apply_source.leaseGroup(alloc, table_name, group_id, lsm_root_generation)) |lease| {
                return lease;
            }
        }
        const write_source = self.write_source.primaryLookupDbSource();
        return try write_source.leaseGroup(alloc, table_name, group_id, lsm_root_generation);
    }

    fn localRaftBatchGroup(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: antfly.db.types.BatchRequest,
    ) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        try self.proposeRaftBatchGroup(alloc, group_id, table_name, req, true);
    }

    fn localRaftBatchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: antfly.db.types.BatchRequest,
    ) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        try self.proposeRaftBatchGroup(alloc, group_id, table_name, req, true);
    }

    fn proposeRaftBatchGroup(
        self: *DataServer,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: antfly.db.types.BatchRequest,
        allow_remote_forward: bool,
    ) !void {
        const raft = self.data_raft orelse return error.UnsupportedOperation;
        try self.syncDataRaftFromRemoteMetadata();
        const deadline_ns = platform_time.monotonicNs() + data_raft_batch_leader_wait_ns;
        var last_metadata_sync_ns = platform_time.monotonicNs();
        var last_local_campaign_ns: u64 = 0;
        while (true) {
            var target_index: ?u64 = null;
            var leader_node_id: ?u64 = null;
            var local_node_id: u64 = 0;
            var local_status_missing = false;

            {
                lockAtomic(&self.data_raft_mutex);
                defer self.data_raft_mutex.unlock();

                local_node_id = raft.host.http_host.host.cfg.local_node_id;
                if (raft.host.http_host.host.isLocalLeader(group_id)) {
                    const encoded = try data_raft_batch.encode(alloc, table_name, req);
                    defer alloc.free(encoded);
                    try raft.host.http_host.propose(group_id, encoded);
                    target_index = if (raft.host.http_host.host.raftStatus(group_id)) |status|
                        status.last_index
                    else
                        return error.UnknownGroup;
                } else {
                    const status = raft.host.http_host.host.raftStatus(group_id);
                    local_status_missing = status == null;
                    leader_node_id = if (status) |raft_status| raft_status.soft.leader_id else null;
                    if (leader_node_id == null and localRaftStatusIsVoter(status, local_node_id)) {
                        const now_ns = platform_time.monotonicNs();
                        if (last_local_campaign_ns == 0 or
                            now_ns -| last_local_campaign_ns >= data_raft_campaign_retry_interval_ns)
                        {
                            raft.host.http_host.campaignGroup(group_id) catch |err| {
                                std.log.warn("data raft campaign failed group_id={} node_id={} err={}", .{ group_id, local_node_id, err });
                            };
                            last_local_campaign_ns = now_ns;
                        }
                        raft.runRaftRoundOnly() catch |err| {
                            std.log.warn("data raft election drive failed group_id={} node_id={} err={}", .{ group_id, local_node_id, err });
                        };
                    }
                }
            }

            if (target_index) |index| {
                if (req.sync_level != .propose) {
                    try self.waitForLocalRaftBatchApply(group_id, index, deadline_ns);
                }
                try self.write_source.syncReplicatedBatchGroupLocal(alloc, group_id, table_name, req.sync_level);
                return;
            }

            if (allow_remote_forward and local_status_missing) {
                if (try self.forwardRaftBatchToPlacementReplica(alloc, group_id, table_name, req, local_node_id, deadline_ns)) return;
            }

            if (allow_remote_forward) {
                if (leader_node_id) |target_node_id| {
                    if (target_node_id != local_node_id) {
                        const leader_base_uri = try self.dataApiUriForNode(alloc, target_node_id);
                        if (leader_base_uri) |base_uri| {
                            defer alloc.free(base_uri);
                            var executor = antfly.raft.transport.StdHttpExecutor.init(alloc, .{});
                            defer executor.deinit();
                            var client = antfly.public_api.ApiHttpClient.init(alloc, executor.executor());
                            const body = try antfly.public_api.batch.encodeBatchRequest(alloc, req);
                            defer alloc.free(body);
                            var response = client.fetchGroupBatch(base_uri, group_id, table_name, body) catch |err| switch (err) {
                                error.UnexpectedHttpStatus,
                                error.HttpConnectionClosing,
                                error.ConnectionResetByPeer,
                                error.ConnectionRefused,
                                error.BrokenPipe,
                                error.EndOfStream,
                                => {
                                    if (platform_time.monotonicNs() >= deadline_ns) return err;
                                    sleepDataRaftBatchLeaderRetry();
                                    continue;
                                },
                                else => return err,
                            };
                            response.deinit(alloc);
                            return;
                        }
                    }
                }
            }

            const now_ns = platform_time.monotonicNs();
            if (leader_node_id == null and now_ns -| last_metadata_sync_ns >= data_raft_metadata_resync_interval_ns) {
                try self.syncDataRaftFromRemoteMetadata();
                last_metadata_sync_ns = now_ns;
            }

            if (platform_time.monotonicNs() >= deadline_ns) {
                if (self.localDataRaftLeaderReady(group_id)) continue;
                self.logRaftBatchLeaderTimeout(group_id);
                return error.LeaderUnavailable;
            }
            sleepDataRaftBatchLeaderRetry();
        }
    }

    fn dataApiUriForNode(self: *DataServer, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
        const remote_metadata = self.remote_metadata orelse return null;
        var snapshot = try remote_metadata.fetchSnapshot();
        defer freeAdminSnapshotOwned(self.alloc, &snapshot);
        const store = findSnapshotStoreByNodeId(snapshot.stores, node_id) orelse return null;
        if (store.api_url.len == 0) return null;
        return try alloc.dupe(u8, store.api_url);
    }

    fn logRaftBatchLeaderTimeout(self: *DataServer, group_id: u64) void {
        const raft = self.data_raft orelse return;
        lockAtomic(&self.data_raft_mutex);
        defer self.data_raft_mutex.unlock();
        const transport_host = &raft.host.http_host.transport_stack.transport_host;
        const transport_metrics = transport_host.metricsSnapshot();
        if (raft.host.http_host.host.raftStatus(group_id)) |status| {
            std.log.warn("data raft leader wait timed out group_id={} node_id={} role={} leader={?} voters={} term={} election_elapsed={} election_timeout={} votes_granted={} votes_rejected={} votes_unknown={} commit={} last={} applied={} served_groups={} peer_routes={} sent_frames={} send_failures={} retries_scheduled={} retries_exhausted={} pending_retries={}", .{
                group_id,
                status.id,
                status.soft.role,
                status.soft.leader_id,
                status.conf_state.voters.len,
                status.hard.current_term,
                status.election_elapsed,
                status.randomized_election_timeout,
                status.votes_granted,
                status.votes_rejected,
                status.votes_unknown,
                status.hard.commit_index,
                status.last_index,
                status.applied_index,
                transport_host.served_groups.count(),
                transport_host.peer_routes.count(),
                transport_metrics.sent_frames,
                transport_metrics.send_failures,
                transport_metrics.retries_scheduled,
                transport_metrics.retries_exhausted,
                transport_host.pendingRetryCount(),
            });
        } else {
            std.log.warn("data raft leader wait timed out group_id={} status=missing served_groups={} peer_routes={} sent_frames={} send_failures={} retries_scheduled={} retries_exhausted={} pending_retries={}", .{
                group_id,
                transport_host.served_groups.count(),
                transport_host.peer_routes.count(),
                transport_metrics.sent_frames,
                transport_metrics.send_failures,
                transport_metrics.retries_scheduled,
                transport_metrics.retries_exhausted,
                transport_host.pendingRetryCount(),
            });
        }
    }

    fn forwardRaftBatchToPlacementReplica(
        self: *DataServer,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: antfly.db.types.BatchRequest,
        local_node_id: u64,
        deadline_ns: u64,
    ) !bool {
        const remote_metadata = self.remote_metadata orelse return false;
        var snapshot = try remote_metadata.fetchSnapshot();
        defer freeAdminSnapshotOwned(self.alloc, &snapshot);

        var local_has_placement = false;
        var target_node_id: ?u64 = null;
        if (findMergedSnapshotGroupStatus(snapshot.merged_group_statuses, group_id)) |status| {
            if (status.leader_known and status.leader_store_id != 0) {
                if (findSnapshotStore(snapshot.stores, status.leader_store_id)) |store| {
                    target_node_id = store.node_id;
                }
            }
        }
        for (snapshot.placement_intents) |intent| {
            if (intent.record.group_id != group_id) continue;
            if (intent.record.local_node_id == local_node_id) {
                local_has_placement = true;
                continue;
            }
            if (target_node_id == null) target_node_id = intent.record.local_node_id;
        }
        if (local_has_placement or target_node_id == null or target_node_id.? == local_node_id) return false;

        const target_store = findSnapshotStoreByNodeId(snapshot.stores, target_node_id.?) orelse return false;
        if (target_store.api_url.len == 0) return false;

        var executor = antfly.raft.transport.StdHttpExecutor.init(alloc, .{});
        defer executor.deinit();
        var client = antfly.public_api.ApiHttpClient.init(alloc, executor.executor());
        const body = try antfly.public_api.batch.encodeBatchRequest(alloc, req);
        defer alloc.free(body);
        var response = client.fetchGroupBatch(target_store.api_url, group_id, table_name, body) catch |err| switch (err) {
            error.UnexpectedHttpStatus,
            error.HttpConnectionClosing,
            error.ConnectionResetByPeer,
            error.ConnectionRefused,
            error.BrokenPipe,
            error.EndOfStream,
            => {
                if (platform_time.monotonicNs() >= deadline_ns) return err;
                return false;
            },
            else => return err,
        };
        response.deinit(alloc);
        return true;
    }

    fn waitForLocalRaftBatchApply(
        self: *DataServer,
        group_id: u64,
        target_index: u64,
        deadline_ns: u64,
    ) !void {
        const apply_sm = self.data_raft_apply orelse return error.UnsupportedOperation;
        while (apply_sm.appliedIndex(group_id) < target_index) {
            if (platform_time.monotonicNs() >= deadline_ns) return error.LeaderUnavailable;
            if (self.data_raft) |raft| {
                lockAtomic(&self.data_raft_mutex);
                raft.runRaftRoundOnly() catch |err| {
                    std.log.warn("data raft apply wait drive failed group_id={} target_index={} err={}", .{ group_id, target_index, err });
                };
                self.data_raft_mutex.unlock();
            }
            sleepDataRaftBatchLeaderRetry();
        }
    }

    fn localDataRaftLeaderReady(self: *DataServer, group_id: u64) bool {
        const raft = self.data_raft orelse return false;
        lockAtomic(&self.data_raft_mutex);
        defer self.data_raft_mutex.unlock();
        return raft.host.http_host.host.isLocalLeader(group_id);
    }

    fn localRaftStatusIsVoter(status: ?raft_engine.core.Status, local_node_id: u64) bool {
        const raft_status = status orelse return false;
        for (raft_status.conf_state.voters) |node_id| {
            if (node_id == local_node_id) return true;
        }
        return false;
    }

    fn sleepDataRaftBatchLeaderRetry() void {
        var req = std.posix.timespec{
            .sec = @intCast(@divTrunc(data_raft_batch_leader_retry_sleep_ns, std.time.ns_per_s)),
            .nsec = @intCast(@mod(data_raft_batch_leader_retry_sleep_ns, std.time.ns_per_s)),
        };
        while (true) {
            const err = std.posix.errno(std.posix.system.nanosleep(&req, &req));
            switch (err) {
                .SUCCESS => return,
                .INTR => continue,
                else => {
                    std.Thread.yield() catch {};
                    return;
                },
            }
        }
    }

    pub fn localGroupStatusProvider(self: *DataServer) antfly.metadata_service.LocalGroupStatusProvider {
        return .{
            .ptr = self,
            .vtable = &.{
                .collect = collectLocalGroupStatusesForMetadataService,
            },
        };
    }

    fn registerMetadataLocalProviders(self: *DataServer) void {
        if (self.metadata_local_providers_registered) return;
        if (self.metadata_service) |svc| {
            svc.setLocalGroupStatusProvider(self.localGroupStatusProvider());
            svc.setLocalShardDbAdapter(self.localShardDbAdapter());
            self.metadata_local_providers_registered = true;
        }
        if (self.metadata_http_service) |svc| {
            svc.setLocalGroupStatusProvider(self.localGroupStatusProvider());
            svc.setLocalShardDbAdapter(self.localShardDbAdapter());
            self.metadata_local_providers_registered = true;
        }
    }

    fn unregisterMetadataLocalProviders(self: *DataServer) void {
        if (!self.metadata_local_providers_registered) return;
        if (self.metadata_service) |svc| {
            svc.setLocalGroupStatusProvider(null);
            svc.setLocalShardDbAdapter(null);
        }
        if (self.metadata_http_service) |svc| {
            svc.setLocalGroupStatusProvider(null);
            svc.setLocalShardDbAdapter(null);
        }
        self.metadata_local_providers_registered = false;
    }

    fn localChangeHook(self: *DataServer) antfly.public_api.ProvisionedTableWriteSource.LocalChangeHook {
        return .{
            .ptr = self,
            .on_change = onLocalTableChanged,
        };
    }

    fn onLocalTableChanged(
        ptr: *anyopaque,
        table_name: []const u8,
        kind: antfly.public_api.ProvisionedTableWriteSource.LocalChangeKind,
    ) void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        self.markLocalSplitKeyCacheDirty();
        switch (kind) {
            .data => self.markLocalGroupDataChanged(),
            .structural => {
                self.invalidateLocalGroupStatusCache();
                self.refreshVisibleProvisionedReplicaState() catch |err| {
                    std.log.warn("failed to refresh provisioned replica state after structural change table={s} err={}", .{
                        table_name,
                        err,
                    });
                };
                if (self.data_raft_apply) |apply_sm| {
                    _ = self.write_source.transferAdoptableWriteCacheEntriesTo(&apply_sm.write_source, table_name) catch |err| {
                        std.log.warn("failed to transfer provisioned write cache to raft apply table={s} err={}", .{
                            table_name,
                            err,
                        });
                    };
                }
                self.syncDataRaftFromRemoteMetadata() catch |err| {
                    std.log.warn("failed to sync data raft placement after structural change table={s} err={}", .{
                        table_name,
                        err,
                    });
                };
            },
        }
        self.markRuntimeStatusDirty(table_name, kind);
    }

    fn markLocalGroupDataChanged(self: *DataServer) void {
        _ = self;
        // Data writes can be high-frequency during ingest. Keep serving the
        // cached group-status snapshot until its TTL expires; structural
        // changes still invalidate immediately.
    }

    fn markRuntimeStatusDirty(
        self: *DataServer,
        table_name: []const u8,
        kind: antfly.public_api.ProvisionedTableWriteSource.LocalChangeKind,
    ) void {
        _ = table_name;
        self.runtime_status_dirty.store(true, .release);
        if (kind == .data) self.provisioned_startup_catch_up_dirty.store(true, .release);
    }

    fn markLocalSplitKeyCacheDirty(self: *DataServer) void {
        _ = self.local_split_key_generation.fetchAdd(1, .monotonic);
    }

    fn invalidateLocalGroupStatusCache(self: *DataServer) void {
        self.store_status_dirty = true;
        _ = self.local_group_status_generation.fetchAdd(1, .monotonic);
        self.markLocalSplitKeyCacheDirty();
        {
            lockAtomic(&self.local_split_key_cache_mutex);
            defer self.local_split_key_cache_mutex.unlock();
            self.local_split_key_cache.clear(self.alloc);
        }
        lockAtomic(&self.local_group_status_cache_mutex);
        defer self.local_group_status_cache_mutex.unlock();
        self.local_group_status_cache.clear(self.alloc);
        lockAtomic(&self.store_status_cache_mutex);
        defer self.store_status_cache_mutex.unlock();
        self.store_status_heartbeat_cache.clear(self.alloc);
    }

    pub fn bumpVisibleProvisionedGroupLsmGenerations(self: *DataServer) !void {
        var io_impl = std.Io.Threaded.init(self.alloc, .{});
        defer io_impl.deinit();

        var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), self.write_source.replica_root_dir, .{ .iterate = true });
        defer dir.close(io_impl.io());

        var group_ids = std.ArrayListUnmanaged(u64).empty;
        defer group_ids.deinit(self.alloc);

        var iter = dir.iterateAssumeFirstIteration();
        while (try iter.next(io_impl.io())) |entry| {
            if (entry.kind != .directory) continue;
            if (!std.mem.startsWith(u8, entry.name, "group-")) continue;
            const group_id = std.fmt.parseInt(u64, entry.name["group-".len..], 10) catch continue;
            try group_ids.append(self.alloc, group_id);
        }

        self.provisioned_storage.pruneGroupGenerations(group_ids.items);
        if (group_ids.items.len == 0) return;
        try self.provisioned_storage.bumpGroupGenerations(group_ids.items);
    }

    pub fn refreshVisibleProvisionedReplicaState(self: *DataServer) !void {
        try self.bumpVisibleProvisionedGroupLsmGenerations();
        self.provisioned_storage.read_cache.clear();
        self.provisioned_storage.lsm_cache.invalidatePrefix(self.write_source.replica_root_dir);
        self.provisioned_storage.hbc_cache.clear();
    }

    pub fn reconcileVisibleProvisionedReplicaState(self: *DataServer) !void {
        try self.refreshVisibleProvisionedReplicaState();
        lockAtomic(self.write_source.localDbMutex());
        defer self.write_source.localDbMutex().unlock();
        self.write_source.pruneStaleWriteCacheLocked();
    }

    fn collectLocalGroupStatusesForMetadataService(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        stores: []const antfly.metadata.table_manager.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        split_transitions: []const antfly.metadata.SplitTransitionRecord,
        merge_transitions: []const antfly.metadata.MergeTransitionRecord,
        split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
        merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
    ) ![]antfly.metadata.table_manager.GroupStatusReport {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        const group_ids = try collectAllRangeGroupIds(alloc, ranges);
        defer alloc.free(group_ids);
        return try self.collectStoreStatusGroupStatusesWithSources(
            alloc,
            replica_root_dir,
            group_ids,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
            null,
            self.group_leadership_source,
            self.group_membership_source,
        );
    }

    fn localFetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        const lsm_root_generation = self.provisioned_storage.generationForGroup(group_id);
        const change_generation = self.local_split_key_generation.load(.monotonic);
        if (try self.snapshotCachedLocalSplitKey(alloc, group_id, lsm_root_generation, change_generation)) |cached| {
            return switch (cached) {
                .key => |key| key,
                .missing => null,
            };
        }

        const median_key = if (self.data_raft_apply) |apply_sm|
            (apply_sm.write_source.findMedianKeyForGroup(alloc, group_id, lsm_root_generation) catch |err| switch (err) {
                error.FileNotFound => return error.UnknownGroup,
                else => return err,
            }) orelse (self.write_source.findMedianKeyForGroup(alloc, group_id, lsm_root_generation) catch |err| switch (err) {
                error.FileNotFound => return error.UnknownGroup,
                else => return err,
            })
        else
            self.write_source.findMedianKeyForGroup(alloc, group_id, lsm_root_generation) catch |err| switch (err) {
                error.FileNotFound => return error.UnknownGroup,
                else => return err,
            };
        errdefer if (median_key) |key| alloc.free(key);
        try self.storeCachedLocalSplitKey(group_id, lsm_root_generation, change_generation, median_key);
        return median_key;
    }

    fn snapshotCachedLocalSplitKey(
        self: *DataServer,
        alloc: std.mem.Allocator,
        group_id: u64,
        lsm_root_generation: u64,
        change_generation: u64,
    ) !?CachedSplitKey {
        lockAtomic(&self.local_split_key_cache_mutex);
        defer self.local_split_key_cache_mutex.unlock();
        if (try self.local_split_key_cache.snapshot(alloc, group_id, lsm_root_generation, change_generation)) |snapshot| {
            if (snapshot.split_key) |key| return .{ .key = key };
            return null;
        }
        return null;
    }

    fn storeCachedLocalSplitKey(
        self: *DataServer,
        group_id: u64,
        lsm_root_generation: u64,
        change_generation: u64,
        split_key: ?[]const u8,
    ) !void {
        if (split_key == null) return;
        lockAtomic(&self.local_split_key_cache_mutex);
        defer self.local_split_key_cache_mutex.unlock();
        try self.local_split_key_cache.put(self.alloc, group_id, lsm_root_generation, change_generation, split_key);
    }

    fn localSchemaIndexReady(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        schema_version: u32,
        read_schema_version: u32,
    ) !bool {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(alloc, table_name, group_id)) |status| {
            var owned_status = status;
            defer owned_status.deinit(alloc);
            if (runtime_status.statusHasRuntimeFacts(owned_status)) {
                return try antfly.metadata.shard_db_adapter.dbStatsSchemaIndexReady(
                    alloc,
                    owned_status.stats,
                    schema_version,
                    read_schema_version,
                );
            }
        }

        var fallback = antfly.metadata.FallbackLocalShardDbAdapter{
            .replica_root_dir = self.write_source.replica_root_dir,
            .backend_runtime = try self.ensureBackendRuntime(),
        };
        return try fallback.adapter().schemaIndexReady(alloc, table_name, group_id, schema_version, read_schema_version);
    }

    fn localObserveSplit(ptr: *anyopaque, record: antfly.metadata.SplitTransitionRecord) !antfly.metadata.transition_state.SplitObservation {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| return try runtime.shardOperationAdapter().observeSplit(record);
        var runtime = try self.initLocalSplitRuntime(record.source_group_id, record.destination_group_id);
        defer runtime.deinit();
        return .{ .status = try runtime.runtime().observeStatus(record.source_group_id, record.destination_group_id) };
    }

    fn localObserveMerge(ptr: *anyopaque, record: antfly.metadata.MergeTransitionRecord) !antfly.metadata.transition_state.MergeObservation {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| return try runtime.shardOperationAdapter().observeMerge(record);
        var runtime = try self.initLocalMergeRuntime(record.donor_group_id, record.receiver_group_id);
        defer runtime.deinit();
        const status = try runtime.runtime().observeStatus(record.donor_group_id, record.receiver_group_id);
        return .{ .donor = status, .receiver = status };
    }

    fn localPrepareSplitSource(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "prepare_split_source")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .prepare_split_source = op });
        } else {
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().prepareSource(op.source_group_id, op.destination_group_id, op.split_key, op.source_range_end);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localStartSplitSource(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "start_split_source")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .start_split_source = op });
        } else {
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().startSource(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localBootstrapSplitDestination(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "bootstrap_split_destination")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .bootstrap_split_destination = op });
        } else {
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().bootstrapDestination(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localCatchUpSplitDestination(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "catch_up_split_destination")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .catch_up_split_destination = op });
        } else {
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().catchUpDestination(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localFinalizeSplitSource(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "finalize_split_source")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .finalize_split_source = op });
        } else {
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().finalizeSource(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localRollbackSplit(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "rollback_split")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .rollback_split = op });
        } else {
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().rollbackSource(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn initLocalSplitRuntime(self: *DataServer, source_group_id: u64, destination_group_id: u64) !antfly.raft.SplitCoordinatorRuntime {
        const source_root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, source_group_id);
        defer self.alloc.free(source_root_dir);
        const dest_root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, destination_group_id);
        defer self.alloc.free(dest_root_dir);
        var dest_db_options = antfly.db.OpenOptions{};
        if (try self.identityNamespaceForSplitDestination(source_group_id, destination_group_id)) |namespace| {
            dest_db_options.identity_namespace = namespace;
        }
        try self.ensureSplitSourceApplyStoreSeeded(source_root_dir, source_group_id);
        return try antfly.raft.SplitCoordinatorRuntime.init(self.alloc, .{
            .source_root_dir = source_root_dir,
            .dest_root_dir = dest_root_dir,
            .source_group_id = source_group_id,
            .dest_group_id = destination_group_id,
            .dest = .{ .root_dir = dest_root_dir, .db = dest_db_options },
        });
    }

    fn initLocalMergeRuntime(self: *DataServer, donor_group_id: u64, receiver_group_id: u64) !antfly.raft.MergeCoordinatorRuntime {
        const donor_root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, donor_group_id);
        defer self.alloc.free(donor_root_dir);
        const receiver_root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, receiver_group_id);
        defer self.alloc.free(receiver_root_dir);
        var receiver_db_options = antfly.db.OpenOptions{};
        const receiver_namespace = try self.identityNamespaceForLocalGroup(receiver_group_id);
        if (receiver_namespace) |namespace| {
            receiver_db_options.identity_namespace = namespace;
            receiver_db_options.prefer_existing_identity_namespace = true;
        }
        try self.ensureSplitSourceApplyStoreSeeded(donor_root_dir, donor_group_id);
        return try antfly.raft.MergeCoordinatorRuntime.init(self.alloc, .{
            .donor_root_dir = donor_root_dir,
            .receiver_root_dir = receiver_root_dir,
            .donor_group_id = donor_group_id,
            .receiver_group_id = receiver_group_id,
            .receiver = .{ .root_dir = receiver_root_dir, .db = receiver_db_options },
            .receiver_identity_reassignment_namespace = receiver_namespace,
        });
    }

    fn identityNamespaceForLocalGroup(self: *DataServer, group_id: u64) !?antfly.db.DocIdentityNamespace {
        var snapshot = try self.write_source.catalog.adminSnapshot();
        defer self.write_source.catalog.freeAdminSnapshot(&snapshot);
        const range = findRangeByGroupId(snapshot.ranges, group_id) orelse return null;
        return identityNamespaceFromRange(range);
    }

    fn identityNamespaceForSplitDestination(self: *DataServer, source_group_id: u64, destination_group_id: u64) !?antfly.db.DocIdentityNamespace {
        _ = destination_group_id;
        return try self.identityNamespaceForLocalGroupDb(source_group_id);
    }

    fn identityNamespaceForLocalGroupDb(self: *DataServer, group_id: u64) !?antfly.db.DocIdentityNamespace {
        const root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, group_id);
        defer self.alloc.free(root_dir);

        var db = antfly.db.DB.open(self.alloc, root_dir, .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .transaction_recovery = .{ .enabled = false },
            .text_merge = .{ .enabled = false },
            .backend_runtime = try self.ensureBackendRuntime(),
        }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer db.close();
        return db.core.identity_namespace;
    }

    fn ensureSplitSourceApplyStoreSeeded(self: *DataServer, source_root_dir: []const u8, source_group_id: u64) !void {
        var source_store = try antfly.data.RaftApplyStore.init(self.alloc, .{ .root_dir = source_root_dir });
        defer source_store.deinit();
        if ((try source_store.latestBatch(source_group_id)) != null) return;

        var db = antfly.db.DB.open(self.alloc, source_root_dir, .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer db.close();

        var ops = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (ops.items) |op| self.alloc.free(op);
            ops.deinit(self.alloc);
        }

        const byte_range = db.getRange();
        try ops.append(self.alloc, try std.fmt.allocPrint(self.alloc, "range:{s}:{s}", .{
            byte_range.start,
            byte_range.end,
        }));

        const lower = try antfly.internal_keys.documentRangeLowerAlloc(self.alloc, byte_range.start);
        defer self.alloc.free(lower);
        const upper = if (byte_range.end.len > 0) try antfly.internal_keys.documentRangeUpperAlloc(self.alloc, byte_range.end) else null;
        defer if (upper) |owned| self.alloc.free(owned);

        const scanned = try db.core.store.scanRange(self.alloc, lower, if (upper) |owned| owned else "");
        defer antfly.docstore.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            const raw_key = (try antfly.internal_keys.decodePrimaryDocumentKeyAlloc(self.alloc, entry.key)) orelse continue;
            defer self.alloc.free(raw_key);
            try ops.append(self.alloc, try std.fmt.allocPrint(self.alloc, "put:{s}={s}", .{
                raw_key,
                entry.value,
            }));
        }

        const entries = try self.alloc.alloc(raft_engine.core.Entry, ops.items.len);
        defer self.alloc.free(entries);
        for (ops.items, 0..) |op, i| {
            entries[i] = .{
                .term = 1,
                .index = i + 1,
                .entry_type = .normal,
                .data = op,
            };
        }
        const encoded = try antfly.raft.state_machine.encodeCommittedEntries(self.alloc, entries);
        defer self.alloc.free(encoded);
        try source_store.snapshotBuilder().applyBatch(.{
            .group_id = source_group_id,
            .commit_index = entries.len,
            .entries_bytes = encoded,
        });
    }

    fn localAcceptMergeReceiver(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "accept_merge_receiver")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .accept_merge_receiver = op });
        } else {
            var runtime = try self.initLocalMergeRuntime(op.donor_group_id, op.receiver_group_id);
            defer runtime.deinit();
            const merge = runtime.runtime();
            if (op.allow_doc_identity_reassignment) {
                try merge.recordDocIdentityReassignment(op.donor_group_id, op.receiver_group_id);
            }
            try merge.acceptReceiver(op.donor_group_id, op.receiver_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localCatchUpMergeReceiver(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "catch_up_merge_receiver")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .catch_up_merge_receiver = op });
        } else {
            var runtime = try self.initLocalMergeRuntime(op.donor_group_id, op.receiver_group_id);
            defer runtime.deinit();
            const merge = runtime.runtime();
            if (op.allow_doc_identity_reassignment) {
                try merge.recordDocIdentityReassignment(op.donor_group_id, op.receiver_group_id);
            }
            _ = try merge.catchUpReceiver(op.donor_group_id, op.receiver_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localFinalizeMerge(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "finalize_merge")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .finalize_merge = op });
        } else {
            var runtime = try self.initLocalMergeRuntime(op.donor_group_id, op.receiver_group_id);
            defer runtime.deinit();
            const merge = runtime.runtime();
            if (op.allow_doc_identity_reassignment) {
                try merge.recordDocIdentityReassignment(op.donor_group_id, op.receiver_group_id);
            }
            _ = try merge.finalizeMerge(op.donor_group_id, op.receiver_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localRollbackMerge(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "rollback_merge")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .rollback_merge = op });
        } else {
            var runtime = try self.initLocalMergeRuntime(op.donor_group_id, op.receiver_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().rollbackMerge(op.donor_group_id, op.receiver_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    pub fn registerNodeIfConfigured(self: *DataServer) !void {
        const remote_metadata = self.remote_metadata orelse return;
        const registration = self.store_registration orelse return;
        const owned_api_url = if (registration.api_url.len == 0) try self.baseUri(self.alloc) else null;
        defer if (owned_api_url) |url| self.alloc.free(url);
        const api_url = if (registration.api_url.len > 0) registration.api_url else owned_api_url.?;
        const raft_url = if (registration.raft_url.len > 0)
            registration.raft_url
        else if (self.data_raft_base_uri) |uri|
            uri
        else
            "";
        try remote_metadata.registerNode(.{
            .store_id = registration.store_id,
            .node_id = registration.node_id,
            .api_url = api_url,
            .raft_url = raft_url,
            .role = registration.role,
            .health_class = "healthy",
            .failure_domain = registration.failure_domain,
            .live = true,
        });
        self.store_registration_confirmed = true;
        // Startup should not block on reopening every local group DB just to
        // compute an initial best-effort status report. Mark the store dirty
        // and let the main run loop publish status once listeners are up.
        self.store_status_dirty = true;
    }

    fn collectCachedLocalGroupStatuses(
        self: *DataServer,
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        group_ids: []const u64,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        stores: []const antfly.metadata.table_manager.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        split_transitions: []const antfly.metadata.SplitTransitionRecord,
        merge_transitions: []const antfly.metadata.MergeTransitionRecord,
        split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
        merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
    ) ![]antfly.metadata.table_manager.GroupStatusReport {
        return try self.collectCachedLocalGroupStatusesWithSources(
            alloc,
            replica_root_dir,
            group_ids,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
            self.group_leadership_source,
            self.group_membership_source,
        );
    }

    fn collectCachedLocalGroupStatusesWithSources(
        self: *DataServer,
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        group_ids: []const u64,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        stores: []const antfly.metadata.table_manager.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        split_transitions: []const antfly.metadata.SplitTransitionRecord,
        merge_transitions: []const antfly.metadata.MergeTransitionRecord,
        split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
        merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
        group_leadership_source: ?GroupLeadershipSource,
        group_membership_source: ?GroupMembershipSource,
    ) ![]antfly.metadata.table_manager.GroupStatusReport {
        const generation = self.local_group_status_generation.load(.monotonic);
        const fingerprint = localGroupStatusFingerprint(
            group_ids,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
            group_leadership_source,
            group_membership_source,
        );
        if (try self.cloneCachedLocalGroupStatuses(alloc, generation, fingerprint)) |cached| return cached;

        const group_statuses = try self.collectLiveLocalGroupStatusesWithSources(
            alloc,
            replica_root_dir,
            group_ids,
            tables,
            ranges,
            group_leadership_source,
            group_membership_source,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        );

        try self.storeCachedLocalGroupStatuses(generation, fingerprint, group_statuses);
        return group_statuses;
    }

    fn collectStoreStatusGroupStatusesWithSources(
        self: *DataServer,
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        group_ids: []const u64,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        stores: []const antfly.metadata.table_manager.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        split_transitions: []const antfly.metadata.SplitTransitionRecord,
        merge_transitions: []const antfly.metadata.MergeTransitionRecord,
        split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
        merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
        inferred_group_leadership: ?InferredSnapshotLeadershipConfig,
        group_leadership_source: ?GroupLeadershipSource,
        group_membership_source: ?GroupMembershipSource,
    ) ![]antfly.metadata.table_manager.GroupStatusReport {
        const generation = self.local_group_status_generation.load(.monotonic);
        const fingerprint = localGroupStatusFingerprint(
            group_ids,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
            group_leadership_source,
            group_membership_source,
        );
        if (try self.cloneCachedLocalGroupStatusesMatching(alloc, generation, fingerprint, false)) |cached| {
            return try mergeRaftOnlyLocalGroupStatusFallbacks(
                alloc,
                cached,
                group_ids,
                group_leadership_source,
                group_membership_source,
            );
        }
        if (try self.cloneCachedLocalGroupStatusesMatching(alloc, generation, fingerprint, true)) |stale| {
            const stale_owned = stale;
            errdefer freeGroupStatusesOwned(alloc, stale_owned);
            const refreshed = self.collectLiveLocalGroupStatusesWithSources(
                alloc,
                replica_root_dir,
                group_ids,
                tables,
                ranges,
                group_leadership_source,
                group_membership_source,
                stores,
                merged_group_statuses,
                split_transitions,
                merge_transitions,
                split_observations,
                merge_observations,
            ) catch |err| {
                std.log.warn("store status stale group refresh failed err={}", .{err});
                try self.requestLocalGroupStatusRefreshWithSources(
                    generation,
                    fingerprint,
                    replica_root_dir,
                    group_ids,
                    tables,
                    ranges,
                    stores,
                    merged_group_statuses,
                    split_transitions,
                    merge_transitions,
                    split_observations,
                    merge_observations,
                    inferred_group_leadership,
                    group_leadership_source,
                    group_membership_source,
                );
                return try mergeRaftOnlyLocalGroupStatusFallbacks(
                    alloc,
                    stale_owned,
                    group_ids,
                    group_leadership_source,
                    group_membership_source,
                );
            };
            freeGroupStatusesOwned(alloc, stale_owned);
            try self.storeCachedLocalGroupStatuses(generation, fingerprint, refreshed);
            return refreshed;
        }
        try self.requestLocalGroupStatusRefreshWithSources(
            generation,
            fingerprint,
            replica_root_dir,
            group_ids,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
            inferred_group_leadership,
            group_leadership_source,
            group_membership_source,
        );
        return try collectRaftOnlyLocalGroupStatusFallbacks(
            alloc,
            group_ids,
            group_leadership_source,
            group_membership_source,
        );
    }

    fn collectLiveLocalGroupStatusesWithSources(
        self: *DataServer,
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        group_ids: []const u64,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        group_leadership_source: ?GroupLeadershipSource,
        group_membership_source: ?GroupMembershipSource,
        stores: []const antfly.metadata.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        split_transitions: []const antfly.metadata.SplitTransitionRecord,
        merge_transitions: []const antfly.metadata.MergeTransitionRecord,
        split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
        merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
    ) ![]antfly.metadata.table_manager.GroupStatusReport {
        var reports = std.ArrayListUnmanaged(antfly.metadata.table_manager.GroupStatusReport).empty;
        errdefer {
            for (reports.items) |record| antfly.metadata.table_manager.freeGroupStatus(alloc, record);
            reports.deinit(alloc);
        }
        const generation = self.local_group_status_generation.load(.monotonic);
        const fingerprint = localGroupStatusFingerprint(
            group_ids,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
            group_leadership_source,
            group_membership_source,
        );
        var active_target = try self.snapshotProvisionedStartupCatchUpTarget();
        defer if (active_target) |*target| target.deinit(alloc);
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();

        for (group_ids) |group_id| {
            const db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
            defer alloc.free(db_path);

            _ = statFilePath(io_impl.io(), db_path) catch |err| switch (err) {
                error.FileNotFound => {
                    if (collectRaftOnlyLocalGroupStatus(group_id, group_leadership_source, group_membership_source)) |status| {
                        try reports.append(alloc, status);
                    }
                    continue;
                },
                else => return err,
            };

            if (findRangeByGroupId(ranges, group_id)) |range| {
                if (findTableById(tables, range.table_id)) |table| {
                    if (try self.snapshotCachedActiveStartupLocalGroupStatusReport(alloc, group_id, table.name, active_target)) |cached| {
                        try reports.append(alloc, cached);
                        continue;
                    }
                    if (self.shouldSkipActiveStartupGroupStatusOpen(table.name, group_id, active_target)) {
                        continue;
                    }
                    if (self.hasReadBlockingActivityBestEffort(table.name, group_id)) {
                        if (try self.snapshotCachedLocalGroupStatusReport(alloc, group_id, generation, fingerprint, true)) |cached| {
                            try reports.append(alloc, cached);
                            continue;
                        }
                        continue;
                    }

                    switch (self.probeManagedWriterGroupBestEffort(table.name, group_id)) {
                        .leased => {
                            if (try self.snapshotCachedLocalGroupStatusReport(alloc, group_id, generation, fingerprint, true)) |cached| {
                                try reports.append(alloc, cached);
                            }
                            continue;
                        },
                        .unknown => {
                            if (try self.snapshotCachedLocalGroupStatusReport(alloc, group_id, generation, fingerprint, true)) |cached| {
                                try reports.append(alloc, cached);
                                continue;
                            }
                        },
                        .absent => {},
                    }
                    if (try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(alloc, table.name, group_id)) |runtime_cached| {
                        var runtime_status_owned = runtime_cached;
                        defer runtime_status_owned.deinit(alloc);
                        const cached_group = try self.snapshotCachedLocalGroupStatusReport(alloc, group_id, generation, fingerprint, true);
                        try reports.append(alloc, try collectLocalGroupStatusFromRuntimeStatus(
                            alloc,
                            runtime_status_owned,
                            cached_group,
                            replica_root_dir,
                            group_id,
                            group_leadership_source,
                            group_membership_source,
                            stores,
                            merged_group_statuses,
                            split_transitions,
                            merge_transitions,
                            split_observations,
                            merge_observations,
                        ));
                        continue;
                    }

                    var db = try antfly.public_api.table_writes.openManagedDbForStatusWithIndexesJsonAndCache(
                        alloc,
                        db_path,
                        table.indexes_json,
                        &self.provisioned_storage.lsm_cache,
                        &self.provisioned_storage.hbc_cache,
                        self.provisioned_storage.generationForGroup(group_id),
                        &self.provisioned_storage.resource_manager,
                        try self.ensureBackendRuntime(),
                    );
                    defer db.close();
                    try reports.append(alloc, try collectLocalGroupStatusFromDb(
                        alloc,
                        &db,
                        db_path,
                        replica_root_dir,
                        group_id,
                        group_leadership_source,
                        group_membership_source,
                        stores,
                        merged_group_statuses,
                        split_transitions,
                        merge_transitions,
                        split_observations,
                        merge_observations,
                    ));
                    continue;
                }
            }

            try reports.append(alloc, try collectLocalGroupStatus(
                alloc,
                db_path,
                replica_root_dir,
                group_id,
                .{
                    .lsm_cache = &self.provisioned_storage.lsm_cache,
                    .lsm_root_generation = self.provisioned_storage.generationForGroup(group_id),
                    .resource_manager = &self.provisioned_storage.resource_manager,
                    .backend_runtime = try self.ensureBackendRuntime(),
                },
                group_leadership_source,
                group_membership_source,
                stores,
                merged_group_statuses,
                split_transitions,
                merge_transitions,
                split_observations,
                merge_observations,
            ));
        }

        return try reports.toOwnedSlice(alloc);
    }

    fn snapshotCachedActiveStartupLocalGroupStatusReport(
        self: *DataServer,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        active_target: ?ActiveStartupCatchUpTarget,
    ) !?antfly.metadata.table_manager.GroupStatusReport {
        if (!self.shouldSkipActiveStartupGroupStatusOpen(table_name, group_id, active_target)) return null;
        lockAtomic(&self.local_group_status_cache_mutex);
        defer self.local_group_status_cache_mutex.unlock();
        if (!self.local_group_status_cache.valid) return null;
        for (self.local_group_status_cache.group_statuses) |status| {
            if (status.group_id != group_id) continue;
            return try antfly.metadata.table_manager.cloneGroupStatus(alloc, status);
        }
        return null;
    }

    fn snapshotCachedLocalGroupStatusReport(
        self: *DataServer,
        alloc: std.mem.Allocator,
        group_id: u64,
        generation: u64,
        fingerprint: u64,
        allow_stale: bool,
    ) !?antfly.metadata.table_manager.GroupStatusReport {
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        lockAtomic(&self.local_group_status_cache_mutex);
        defer self.local_group_status_cache_mutex.unlock();
        if (!self.local_group_status_cache.valid) return null;
        if (self.local_group_status_cache.generation != generation or self.local_group_status_cache.fingerprint != fingerprint) return null;
        if (!allow_stale and now_ms -| self.local_group_status_cache.collected_at_ms > local_group_status_cache_ttl_ms) return null;
        for (self.local_group_status_cache.group_statuses) |status| {
            if (status.group_id != group_id) continue;
            return try antfly.metadata.table_manager.cloneGroupStatus(alloc, status);
        }
        return null;
    }

    fn cloneCachedLocalGroupStatuses(
        self: *DataServer,
        alloc: std.mem.Allocator,
        generation: u64,
        fingerprint: u64,
    ) !?[]antfly.metadata.table_manager.GroupStatusReport {
        return try self.cloneCachedLocalGroupStatusesMatching(alloc, generation, fingerprint, false);
    }

    fn cloneCachedLocalGroupStatusesMatching(
        self: *DataServer,
        alloc: std.mem.Allocator,
        generation: u64,
        fingerprint: u64,
        allow_stale: bool,
    ) !?[]antfly.metadata.table_manager.GroupStatusReport {
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        lockAtomic(&self.local_group_status_cache_mutex);
        defer self.local_group_status_cache_mutex.unlock();
        const cache = &self.local_group_status_cache;
        if (!cache.valid) return null;
        if (cache.generation != generation or cache.fingerprint != fingerprint) return null;
        if (!allow_stale and now_ms -| cache.collected_at_ms > local_group_status_cache_ttl_ms) return null;
        return try antfly.metadata.table_manager.cloneGroupStatuses(alloc, cache.group_statuses);
    }

    fn localGroupStatusCacheStale(self: *DataServer, now_ms: u64) bool {
        lockAtomic(&self.local_group_status_cache_mutex);
        defer self.local_group_status_cache_mutex.unlock();
        const cache = &self.local_group_status_cache;
        if (!cache.valid) return true;
        return now_ms -| cache.collected_at_ms > local_group_status_cache_ttl_ms;
    }

    fn storeCachedLocalGroupStatuses(
        self: *DataServer,
        generation: u64,
        fingerprint: u64,
        group_statuses: []const antfly.metadata.table_manager.GroupStatusReport,
    ) !void {
        lockAtomic(&self.local_group_status_cache_mutex);
        defer self.local_group_status_cache_mutex.unlock();
        self.local_group_status_cache.clear(self.alloc);
        self.local_group_status_cache = .{
            .valid = true,
            .fingerprint = fingerprint,
            .generation = generation,
            .collected_at_ms = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms)),
            .group_statuses = try antfly.metadata.table_manager.cloneGroupStatuses(self.alloc, group_statuses),
        };
    }

    fn reportStoreStatus(self: *DataServer) !void {
        const remote_metadata = self.remote_metadata orelse return;
        const registration = self.store_registration orelse return;
        var snapshot = try remote_metadata.fetchSnapshot();
        defer freeAdminSnapshotOwned(self.alloc, &snapshot);
        try self.syncDataRaftFromSnapshot(&snapshot);

        var local_group_ids = try collectLocalGroupIds(self.alloc, snapshot.placement_intents, registration.node_id);
        if (local_group_ids.len == 0 and hasSingleRoleStore(snapshot.stores, registration.role, registration.store_id)) {
            const fallback_group_ids = try collectAllRangeGroupIds(self.alloc, snapshot.ranges);
            self.alloc.free(local_group_ids);
            local_group_ids = fallback_group_ids;
        }
        defer self.alloc.free(local_group_ids);

        var inferred_leadership = InferredSnapshotLeadershipSource.init(
            registration.node_id,
            registration.store_id,
            snapshot.stores,
            snapshot.merged_group_statuses,
            snapshot.placement_intents,
        );
        const inferred_group_leadership = if (self.group_leadership_source != null)
            null
        else
            InferredSnapshotLeadershipConfig{
                .local_node_id = registration.node_id,
                .local_store_id = registration.store_id,
                .placement_intents = snapshot.placement_intents,
            };
        const group_statuses = try self.collectStoreStatusGroupStatusesWithSources(
            self.alloc,
            self.write_source.replica_root_dir,
            local_group_ids,
            snapshot.tables,
            snapshot.ranges,
            snapshot.stores,
            snapshot.merged_group_statuses,
            snapshot.split_transitions,
            snapshot.merge_transitions,
            snapshot.split_observations,
            snapshot.merge_observations,
            inferred_group_leadership,
            self.group_leadership_source orelse inferred_leadership.iface(),
            self.group_membership_source,
        );
        defer freeGroupStatusesOwned(self.alloc, group_statuses);
        const runtime_statuses = try self.collectStoreRuntimeStatusReports(
            self.alloc,
            local_group_ids,
            snapshot.tables,
            snapshot.ranges,
            registration,
        );
        defer antfly.metadata.table_manager.freeRuntimeGroupStatusReports(self.alloc, runtime_statuses);

        const report: antfly.metadata.table_manager.StoreStatusReport = .{
            .store_id = registration.store_id,
            .live = true,
            .health_class = "healthy",
            .group_statuses = group_statuses,
            .runtime_statuses = runtime_statuses,
        };
        try remote_metadata.reportNodeStatus(report);
        try self.reportRuntimeSchemaProgress(
            remote_metadata,
            registration.store_id,
            registration.node_id,
            runtime_statuses,
            snapshot.tables,
            snapshot.ranges,
        );
        try self.storeStatusHeartbeatCacheReplace(report);
        self.store_status_dirty = false;
        self.last_store_status_report_at_ms = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
    }

    fn syncDataRaftFromRemoteMetadata(self: *DataServer) !void {
        if (self.data_raft == null) return;
        const remote_metadata = self.remote_metadata orelse return;
        var snapshot = try remote_metadata.fetchSnapshot();
        defer freeAdminSnapshotOwned(self.alloc, &snapshot);
        try self.syncDataRaftFromSnapshot(&snapshot);
    }

    fn syncDataRaftFromSnapshot(self: *DataServer, snapshot: *const antfly.metadata_api.AdminSnapshot) !void {
        const raft = self.data_raft orelse return;
        const factory = self.data_raft_factory orelse return;
        const registration = self.store_registration orelse return;

        var local_intents = std.ArrayListUnmanaged(antfly.raft.PlacementIntent).empty;
        defer {
            for (local_intents.items) |intent| if (intent.peer_node_ids.len > 0) self.alloc.free(intent.peer_node_ids);
            local_intents.deinit(self.alloc);
        }
        for (snapshot.placement_intents) |intent| {
            if (intent.record.local_node_id != registration.node_id and intent.store_id != registration.store_id) continue;
            const peer_node_ids = try collectPlacementPeerNodeIdsForGroup(
                self.alloc,
                snapshot.placement_intents,
                intent.record.group_id,
            );
            try local_intents.append(self.alloc, .{
                .record = intent.record,
                .store_id = intent.store_id,
                .peer_node_ids = peer_node_ids,
            });
        }
        const placement_fingerprint = dataRaftPlacementIntentsFingerprint(local_intents.items);
        const placement_changed = self.last_data_raft_placement_fingerprint == null or self.last_data_raft_placement_fingerprint.? != placement_fingerprint;
        if (placement_changed) {
            self.last_data_raft_placement_fingerprint = placement_fingerprint;
            self.invalidateLocalGroupStatusCache();
            self.store_status_dirty = true;
        }

        var updates = std.ArrayListUnmanaged(antfly.raft.MetadataUpdate).empty;
        defer {
            for (updates.items) |*update| update.deinit(self.alloc);
            updates.deinit(self.alloc);
        }
        for (local_intents.items) |intent| {
            for (snapshot.stores) |peer| {
                if (peer.node_id == 0 or peer.node_id == registration.node_id) continue;
                if (!nodeIdInSlice(intent.peer_node_ids, peer.node_id)) {
                    try updates.append(self.alloc, .{
                        .peer_route = .{
                            .remove = .{
                                .group_id = intent.record.group_id,
                                .node_id = peer.node_id,
                            },
                        },
                    });
                    continue;
                }
                if (peer.raft_url.len == 0) continue;
                try appendOwnedPeerRouteUpsert(
                    self.alloc,
                    &updates,
                    intent.record.group_id,
                    peer.node_id,
                    peer.raft_url,
                );
            }
        }

        lockAtomic(&self.data_raft_mutex);
        defer self.data_raft_mutex.unlock();
        try factory.replacePeerSets(local_intents.items);
        try raft.host.replacePlacementIntents(local_intents.items);
        _ = try raft.host.reconcileOnce();
        if (updates.items.len > 0) try raft.host.applyBatch(updates.items);
        var campaigned = false;
        for (local_intents.items) |intent| {
            if (!localIntentPreferredCampaigner(intent, registration.node_id)) continue;
            const status = raft.host.http_host.host.raftStatus(intent.record.group_id);
            if (status == null or status.?.soft.leader_id != null or !localRaftStatusIsVoter(status, registration.node_id)) continue;
            raft.host.http_host.campaignGroup(intent.record.group_id) catch |err| {
                std.log.warn("data raft bootstrap campaign failed group_id={} node_id={} err={}", .{
                    intent.record.group_id,
                    registration.node_id,
                    err,
                });
                continue;
            };
            campaigned = true;
        }
        if (campaigned) {
            raft.runRaftRoundOnly() catch |err| {
                std.log.warn("data raft bootstrap campaign drive failed node_id={} err={}", .{ registration.node_id, err });
            };
        }
    }

    fn reportStoreStatusHeartbeat(self: *DataServer) !void {
        const remote_metadata = self.remote_metadata orelse return;
        const registration = self.store_registration orelse return;
        var report = (try self.cloneHeartbeatStoreStatusReport(registration.store_id)) orelse return try self.reportStoreStatus();
        defer freeStoreStatusReportOwned(self.alloc, &report);
        try remote_metadata.reportNodeStatus(report);
        self.last_store_status_report_at_ms = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
    }

    fn cloneHeartbeatStoreStatusReport(
        self: *DataServer,
        store_id: u64,
    ) !?antfly.metadata.table_manager.StoreStatusReport {
        lockAtomic(&self.store_status_cache_mutex);
        defer self.store_status_cache_mutex.unlock();
        const cache = &self.store_status_heartbeat_cache;
        if (cache.group_statuses.len == 0 and cache.runtime_statuses.len == 0) return null;
        return .{
            .store_id = store_id,
            .live = cache.live,
            .health_class = try self.alloc.dupe(u8, cache.health_class),
            .capacity_bytes = cache.capacity_bytes,
            .available_bytes = cache.available_bytes,
            .lease_pressure = cache.lease_pressure,
            .read_load = cache.read_load,
            .write_load = cache.write_load,
            .active_backfills = cache.active_backfills,
            .backfill_progress_millis = cache.backfill_progress_millis,
            .group_statuses = try antfly.metadata.table_manager.cloneGroupStatuses(self.alloc, cache.group_statuses),
            .runtime_statuses = try antfly.metadata.table_manager.cloneRuntimeGroupStatusReports(self.alloc, cache.runtime_statuses),
        };
    }

    fn storeStatusHeartbeatCacheReplace(
        self: *DataServer,
        report: antfly.metadata.table_manager.StoreStatusReport,
    ) !void {
        lockAtomic(&self.store_status_cache_mutex);
        defer self.store_status_cache_mutex.unlock();
        self.store_status_heartbeat_cache.clear(self.alloc);
        self.store_status_heartbeat_cache = .{
            .live = report.live,
            .health_class = try self.alloc.dupe(u8, report.health_class),
            .owns_health_class = true,
            .capacity_bytes = report.capacity_bytes,
            .available_bytes = report.available_bytes,
            .lease_pressure = report.lease_pressure,
            .read_load = report.read_load,
            .write_load = report.write_load,
            .active_backfills = report.active_backfills,
            .backfill_progress_millis = report.backfill_progress_millis,
            .group_statuses = try antfly.metadata.table_manager.cloneGroupStatuses(self.alloc, report.group_statuses),
            .runtime_statuses = try antfly.metadata.table_manager.cloneRuntimeGroupStatusReports(self.alloc, report.runtime_statuses),
        };
    }

    fn collectStoreRuntimeStatusReports(
        self: *DataServer,
        alloc: std.mem.Allocator,
        group_ids: []const u64,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        registration: StoreRegistrationConfig,
    ) ![]antfly.metadata.table_manager.RuntimeGroupStatusReport {
        var reports = std.ArrayListUnmanaged(antfly.metadata.table_manager.RuntimeGroupStatusReport).empty;
        errdefer {
            for (reports.items) |record| antfly.metadata.table_manager.freeRuntimeGroupStatusReport(alloc, record);
            reports.deinit(alloc);
        }

        for (group_ids) |group_id| {
            const range = findRangeByGroupId(ranges, group_id) orelse continue;
            const table = findTableById(tables, range.table_id) orelse continue;
            var status = (try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(alloc, table.name, group_id)) orelse continue;
            defer status.deinit(alloc);
            self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
            try reports.append(alloc, try runtimeStatusReportFromLocalStatus(
                alloc,
                table,
                group_id,
                registration,
                status,
            ));
        }

        return try reports.toOwnedSlice(alloc);
    }

    fn liveRuntimeWriteSource(self: *DataServer) *antfly.public_api.ProvisionedTableWriteSource {
        if (self.data_raft_apply) |apply_sm| return &apply_sm.write_source;
        return &self.write_source;
    }

    fn snapshotManagedWriterGroupStatusBestEffort(
        self: *DataServer,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
    ) !?runtime_status.LocalTableRuntimeStatus {
        if (try self.liveRuntimeWriteSource().snapshotManagedWriterGroupStatusBestEffort(alloc, table_name, group_id)) |status| {
            return status;
        }
        if (self.data_raft_apply != null) {
            return try self.write_source.snapshotManagedWriterGroupStatusBestEffort(alloc, table_name, group_id);
        }
        return null;
    }

    fn overlayManagedWriterGroupStatusBestEffort(
        self: *DataServer,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        group_id: u64,
        status: *runtime_status.LocalTableRuntimeStatus,
    ) void {
        if (self.data_raft_apply) |apply_sm| {
            switch (apply_sm.write_source.probeManagedWriterGroupBestEffort(table_name, group_id)) {
                .leased => |cached| {
                    var lease = cached;
                    const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                    defer lease.deinit(release_alloc);
                    apply_sm.write_source.overlayManagedWriterGroupStatusBestEffort(alloc, table_name, group_id, status);
                    return;
                },
                .unknown => return,
                .absent => {},
            }
        }
        self.write_source.overlayManagedWriterGroupStatusBestEffort(alloc, table_name, group_id, status);
    }

    fn hasReadBlockingActivityBestEffort(
        self: *DataServer,
        table_name: []const u8,
        group_id: u64,
    ) bool {
        if (self.liveRuntimeWriteSource().hasReadBlockingActivityBestEffort(table_name, group_id)) return true;
        if (self.data_raft_apply != null and self.write_source.hasReadBlockingActivityBestEffort(table_name, group_id)) return true;
        return false;
    }

    fn probeManagedWriterGroupBestEffort(
        self: *DataServer,
        table_name: []const u8,
        group_id: u64,
    ) antfly.public_api.ProvisionedTableWriteSource.ManagedWriterGroupProbe {
        const live_probe = self.liveRuntimeWriteSource().probeManagedWriterGroupBestEffort(table_name, group_id);
        switch (live_probe) {
            .leased, .unknown => return live_probe,
            .absent => {},
        }
        if (self.data_raft_apply != null) return self.write_source.probeManagedWriterGroupBestEffort(table_name, group_id);
        return live_probe;
    }

    fn applyRuntimeStatusStorageFactsBestEffort(
        self: *DataServer,
        status: *runtime_status.LocalTableRuntimeStatus,
        group_id: u64,
        db: ?*antfly.db.DB,
    ) void {
        if (group_id == 0) return;
        const db_path = antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, group_id) catch return;
        defer self.alloc.free(db_path);
        if (self.runtimeStatusDiskUsageBytesBestEffort(group_id, db_path, status.*)) |disk_bytes| {
            status.disk_bytes = disk_bytes;
        }
        if (status.created_at_millis == 0) {
            if (db) |ptr| {
                status.created_at_millis = (ptr.getGroupCreatedAtMillis(self.alloc, group_id) catch null) orelse 0;
            }
        }
    }

    fn runtimeStatusDiskUsageBytesBestEffort(
        self: *DataServer,
        group_id: u64,
        db_path: []const u8,
        status: runtime_status.LocalTableRuntimeStatus,
    ) ?u64 {
        const now_ns = platform_time.monotonicNs();
        const active = runtimeStatusHasActiveBackgroundWork(status);
        lockAtomic(&self.runtime_status_disk_usage_cache_mutex);
        if (self.runtime_status_disk_usage_cache.get(group_id)) |entry| {
            const fresh = now_ns -| entry.checked_at_ns < runtime_status_disk_usage_refresh_interval_ns;
            const zero_cache_for_nonempty_group = entry.disk_bytes == 0 and status.stats.doc_count > 0;
            if ((active or fresh) and !zero_cache_for_nonempty_group) {
                self.runtime_status_disk_usage_cache_mutex.unlock();
                return entry.disk_bytes;
            }
        }
        self.runtime_status_disk_usage_cache_mutex.unlock();

        if (active and status.stats.doc_count == 0) return null;
        const disk_bytes = directoryUsageBytes(self.alloc, db_path) catch return null;

        lockAtomic(&self.runtime_status_disk_usage_cache_mutex);
        defer self.runtime_status_disk_usage_cache_mutex.unlock();
        self.runtime_status_disk_usage_cache.put(self.alloc, group_id, .{
            .disk_bytes = disk_bytes,
            .checked_at_ns = now_ns,
        }) catch {};
        return disk_bytes;
    }

    fn joinLocalGroupStatusRefreshThread(self: *DataServer) void {
        lockAtomic(&self.local_group_status_refresh_mutex);
        defer self.local_group_status_refresh_mutex.unlock();
        if (self.local_group_status_refresh_thread) |thread| {
            thread.join();
            self.local_group_status_refresh_thread = null;
        }
        self.local_group_status_refresh_active.store(false, .release);
    }

    fn reapLocalGroupStatusRefreshThread(self: *DataServer) void {
        if (self.local_group_status_refresh_active.load(.acquire)) return;
        lockAtomic(&self.local_group_status_refresh_mutex);
        defer self.local_group_status_refresh_mutex.unlock();
        if (self.local_group_status_refresh_active.load(.acquire)) return;
        if (self.local_group_status_refresh_thread) |thread| {
            thread.join();
            self.local_group_status_refresh_thread = null;
        }
    }

    fn joinRuntimeStatusRefreshThread(self: *DataServer) void {
        lockAtomic(&self.runtime_status_refresh_mutex);
        defer self.runtime_status_refresh_mutex.unlock();
        if (self.runtime_status_refresh_thread) |thread| {
            thread.join();
            self.runtime_status_refresh_thread = null;
        }
        self.runtime_status_refresh_active.store(false, .release);
    }

    fn reapRuntimeStatusRefreshThread(self: *DataServer) void {
        if (self.runtime_status_refresh_active.load(.acquire)) return;
        lockAtomic(&self.runtime_status_refresh_mutex);
        defer self.runtime_status_refresh_mutex.unlock();
        if (self.runtime_status_refresh_active.load(.acquire)) return;
        if (self.runtime_status_refresh_thread) |thread| {
            thread.join();
            self.runtime_status_refresh_thread = null;
        }
    }

    fn joinAutoBulkFinishTask(self: *DataServer) void {
        lockAtomic(&self.auto_bulk_finish_mutex);
        defer self.auto_bulk_finish_mutex.unlock();
        self.auto_bulk_finish_stop.store(true, .release);
        if (self.auto_bulk_finish_future) |*future| {
            if (self.auto_bulk_finish_io) |*io_impl| {
                _ = future.await(io_impl.io());
            }
            self.auto_bulk_finish_future = null;
        }
        self.auto_bulk_finish_active.store(false, .release);
        if (self.auto_bulk_finish_io) |*io_impl| {
            io_impl.deinit();
            self.auto_bulk_finish_io = null;
        }
    }

    fn joinProvisionedWarmupThread(self: *DataServer) void {
        lockAtomic(&self.provisioned_warmup_mutex);
        defer self.provisioned_warmup_mutex.unlock();
        if (self.provisioned_warmup_thread) |thread| {
            thread.join();
            self.provisioned_warmup_thread = null;
        }
        self.provisioned_warmup_active.store(false, .release);
    }

    fn reapProvisionedWarmupThread(self: *DataServer) void {
        if (self.provisioned_warmup_active.load(.acquire)) return;
        lockAtomic(&self.provisioned_warmup_mutex);
        defer self.provisioned_warmup_mutex.unlock();
        if (self.provisioned_warmup_active.load(.acquire)) return;
        if (self.provisioned_warmup_thread) |thread| {
            thread.join();
            self.provisioned_warmup_thread = null;
        }
    }

    fn joinProvisionedStartupCatchUpThread(self: *DataServer) void {
        lockAtomic(&self.provisioned_startup_catch_up_mutex);
        defer self.provisioned_startup_catch_up_mutex.unlock();
        if (self.provisioned_startup_catch_up_thread) |thread| {
            thread.join();
            self.provisioned_startup_catch_up_thread = null;
        }
        self.provisioned_startup_catch_up_active.store(false, .release);
    }

    fn reapProvisionedStartupCatchUpThread(self: *DataServer) void {
        if (self.provisioned_startup_catch_up_active.load(.acquire)) return;
        lockAtomic(&self.provisioned_startup_catch_up_mutex);
        defer self.provisioned_startup_catch_up_mutex.unlock();
        if (self.provisioned_startup_catch_up_active.load(.acquire)) return;
        if (self.provisioned_startup_catch_up_thread) |thread| {
            thread.join();
            self.provisioned_startup_catch_up_thread = null;
        }
    }

    fn joinProvisionedRootRefreshThread(self: *DataServer) void {
        lockAtomic(&self.provisioned_root_refresh_mutex);
        defer self.provisioned_root_refresh_mutex.unlock();
        if (self.provisioned_root_refresh_thread) |thread| {
            thread.join();
            self.provisioned_root_refresh_thread = null;
        }
        self.provisioned_root_refresh_active.store(false, .release);
    }

    fn reapProvisionedRootRefreshThread(self: *DataServer) void {
        if (self.provisioned_root_refresh_active.load(.acquire)) return;
        lockAtomic(&self.provisioned_root_refresh_mutex);
        defer self.provisioned_root_refresh_mutex.unlock();
        if (self.provisioned_root_refresh_active.load(.acquire)) return;
        if (self.provisioned_root_refresh_thread) |thread| {
            thread.join();
            self.provisioned_root_refresh_thread = null;
        }
    }

    pub fn requestProvisionedCacheWarmup(self: *DataServer) !void {
        if (@import("builtin").is_test) {
            _ = self.runProvisionedCacheWarmup();
            return;
        }

        self.reapProvisionedWarmupThread();
        if (self.provisioned_warmup_active.load(.acquire)) return;

        lockAtomic(&self.provisioned_warmup_mutex);
        defer self.provisioned_warmup_mutex.unlock();
        if (self.provisioned_warmup_active.load(.acquire)) return;
        self.provisioned_warmup_active.store(true, .release);
        self.provisioned_warmup_thread = try std.Thread.spawn(.{}, provisionedCacheWarmupWorkerMain, .{self});
    }

    fn provisionedCacheWarmupWorkerMain(self: *DataServer) void {
        defer self.provisioned_warmup_active.store(false, .release);
        _ = self.runProvisionedCacheWarmup();
    }

    fn runProvisionedCacheWarmup(self: *DataServer) ProvisionedWarmupStats {
        const started_ns = platform_time.monotonicNs();
        _ = self.provisioned_warmup_started.fetchAdd(1, .monotonic);
        var stats: ProvisionedWarmupStats = .{};
        defer self.provisioned_warmup_last_group_count.store(stats.warmed_group_count, .monotonic);
        defer self.provisioned_warmup_last_duration_ns.store(stats.duration_ns, .monotonic);
        defer stats.duration_ns = platform_time.monotonicNs() - started_ns;
        if (self.provisioned_startup_catch_up_active.load(.acquire)) return stats;

        const registration = self.store_registration orelse return stats;
        const snapshot_opt = self.adminSnapshotPreferCached() catch |err| {
            _ = self.provisioned_warmup_failed.fetchAdd(1, .monotonic);
            std.log.warn("provisioned cache warmup snapshot failed err={}", .{err});
            return stats;
        };
        var snapshot = snapshot_opt orelse return stats;
        defer self.status_source.freeAdminSnapshot(&snapshot);

        var local_group_ids = collectLocalGroupIds(self.alloc, snapshot.placement_intents, registration.node_id) catch |err| {
            _ = self.provisioned_warmup_failed.fetchAdd(1, .monotonic);
            std.log.warn("provisioned cache warmup local groups failed err={}", .{err});
            return stats;
        };
        if (local_group_ids.len == 0 and hasSingleRoleStore(snapshot.stores, registration.role, registration.store_id)) {
            const fallback_group_ids = collectAllRangeGroupIds(self.alloc, snapshot.ranges) catch |err| {
                self.alloc.free(local_group_ids);
                _ = self.provisioned_warmup_failed.fetchAdd(1, .monotonic);
                std.log.warn("provisioned cache warmup fallback groups failed err={}", .{err});
                return stats;
            };
            self.alloc.free(local_group_ids);
            local_group_ids = fallback_group_ids;
        }
        defer self.alloc.free(local_group_ids);

        for (snapshot.tables) |table| {
            const warmed_groups = self.warmProvisionedTableGroups(snapshot.ranges, local_group_ids, table) catch |err| blk: {
                std.log.warn("provisioned cache warmup failed table={s} err={}", .{ table.name, err });
                break :blk 0;
            };
            stats.warmed_group_count += warmed_groups;
        }
        self.requestRuntimeStatusRefresh() catch |err| switch (err) {
            error.ThreadQuotaExceeded,
            error.SystemResources,
            => std.log.warn("provisioned cache warmup runtime status refresh deferred err={}", .{err}),
            else => {
                _ = self.provisioned_warmup_failed.fetchAdd(1, .monotonic);
                std.log.warn("provisioned cache warmup runtime status refresh failed err={}", .{err});
            },
        };
        self.requestProvisionedStartupCatchUp() catch |err| switch (err) {
            error.ThreadQuotaExceeded,
            error.SystemResources,
            => std.log.warn("provisioned cache warmup startup catch-up deferred err={}", .{err}),
            else => {
                _ = self.provisioned_warmup_failed.fetchAdd(1, .monotonic);
                std.log.warn("provisioned cache warmup startup catch-up failed err={}", .{err});
            },
        };
        _ = self.provisioned_warmup_completed.fetchAdd(1, .monotonic);
        return stats;
    }

    fn warmProvisionedTableGroups(
        self: *DataServer,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        local_group_ids: []const u64,
        table: antfly.metadata.table_manager.TableRecord,
    ) !u64 {
        var warmed_group_count: u64 = 0;
        for (local_group_ids) |group_id| {
            const range = findRangeByGroupId(ranges, group_id) orelse continue;
            if (range.table_id != table.table_id) continue;

            const db_path = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, group_id);
            defer self.alloc.free(db_path);

            var io_impl = std.Io.Threaded.init(self.alloc, .{});
            defer io_impl.deinit();
            _ = statFilePath(io_impl.io(), db_path) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };

            // Startup warmup should only preopen query/read handles. Writer
            // opens still run recovery/replay work and can block loaded-state
            // startup for large tables.
            try self.read_source.warmTableGroup(self.alloc, group_id, table.name);
            warmed_group_count += 1;
        }
        return warmed_group_count;
    }

    fn runProvisionedStartupCatchUp(self: *DataServer) ProvisionedStartupCatchUpStats {
        const started_ns = platform_time.monotonicNs();
        const started_at_ms: u64 = @intCast(@divTrunc(started_ns, std.time.ns_per_ms));
        self.provisioned_startup_catch_up_last_run_at_ms.store(started_at_ms, .monotonic);
        _ = self.provisioned_startup_catch_up_started.fetchAdd(1, .monotonic);
        var stats: ProvisionedStartupCatchUpStats = .{};
        var inspection_complete = false;
        defer self.provisioned_startup_catch_up_last_group_count.store(stats.group_count, .monotonic);
        defer self.provisioned_startup_catch_up_last_groups_with_debt.store(stats.groups_with_debt, .monotonic);
        defer self.provisioned_startup_catch_up_last_groups_cleared.store(stats.groups_cleared, .monotonic);
        defer self.provisioned_startup_catch_up_last_busy_groups.store(stats.busy_groups, .monotonic);
        defer self.provisioned_startup_catch_up_last_duration_ns.store(stats.duration_ns, .monotonic);
        defer stats.duration_ns = platform_time.monotonicNs() - started_ns;
        defer self.provisioned_startup_catch_up_dirty.store(
            if (inspection_complete) stats.debt_remaining else true,
            .release,
        );
        defer _ = self.provisioned_startup_catch_up_completed.fetchAdd(1, .monotonic);

        const registration = self.store_registration orelse {
            return stats;
        };

        const snapshot_opt = self.adminSnapshotPreferCached() catch |err| {
            _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
            std.log.warn("provisioned startup catch-up snapshot failed err={}", .{err});
            return stats;
        };
        var snapshot = snapshot_opt orelse return stats;
        defer self.status_source.freeAdminSnapshot(&snapshot);

        var local_group_ids = collectLocalGroupIds(self.alloc, snapshot.placement_intents, registration.node_id) catch |err| {
            _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
            std.log.warn("provisioned startup catch-up local groups failed err={}", .{err});
            return stats;
        };
        if (local_group_ids.len == 0 and hasSingleRoleStore(snapshot.stores, registration.role, registration.store_id)) {
            const fallback_group_ids = collectAllRangeGroupIds(self.alloc, snapshot.ranges) catch |err| {
                self.alloc.free(local_group_ids);
                _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                std.log.warn("provisioned startup catch-up fallback groups failed err={}", .{err});
                return stats;
            };
            self.alloc.free(local_group_ids);
            local_group_ids = fallback_group_ids;
        }
        defer self.alloc.free(local_group_ids);

        if (local_group_ids.len == 0) return stats;

        for (snapshot.tables) |table| {
            for (local_group_ids) |group_id| {
                const range = findRangeByGroupId(snapshot.ranges, group_id) orelse continue;
                if (range.table_id != table.table_id) continue;
                switch (startupCatchUpGroupDisposition(
                    snapshot.stores,
                    snapshot.placement_intents,
                    snapshot.merged_group_statuses,
                    registration,
                    self.group_leadership_source,
                    group_id,
                )) {
                    .attempt => {},
                    .skip_nonlocal => continue,
                    .retry_unknown => {
                        stats.debt_remaining = true;
                        continue;
                    },
                }

                const db_path = antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, group_id) catch |err| {
                    _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                    std.log.warn("provisioned startup catch-up path failed group={} table={s} err={}", .{ group_id, table.name, err });
                    stats.debt_remaining = true;
                    continue;
                };
                defer self.alloc.free(db_path);

                var io_impl = std.Io.Threaded.init(self.alloc, .{});
                defer io_impl.deinit();
                _ = statFilePath(io_impl.io(), db_path) catch |err| switch (err) {
                    error.FileNotFound => {
                        stats.debt_remaining = true;
                        continue;
                    },
                    else => {
                        _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                        std.log.warn("provisioned startup catch-up stat failed group={} table={s} err={}", .{ group_id, table.name, err });
                        stats.debt_remaining = true;
                        continue;
                    },
                };

                const result = result_blk: {
                    self.setProvisionedStartupCatchUpTarget(group_id, table.name) catch |err| {
                        _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                        std.log.warn("provisioned startup catch-up target set failed group={} table={s} err={}", .{ group_id, table.name, err });
                        stats.debt_remaining = true;
                        continue;
                    };
                    defer self.clearProvisionedStartupCatchUpTarget();

                    break :result_blk antfly.public_api.ProvisionedTableWriteSource.catchUpTableGroupBestEffortWithIndexesJson(&self.write_source, self.alloc, group_id, table.name, table.indexes_json) catch |err| {
                        _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                        std.log.warn("provisioned startup catch-up failed group={} table={s} err={}", .{ group_id, table.name, err });
                        stats.debt_remaining = true;
                        continue;
                    };
                };
                stats.group_count += 1;
                if (result.busy) {
                    stats.busy_groups += 1;
                    stats.debt_remaining = true;
                    continue;
                }
                if (!result.had_debt) continue;
                stats.groups_with_debt += 1;
                if (result.cleared_debt) {
                    stats.groups_cleared += 1;
                } else {
                    stats.debt_remaining = true;
                }
            }
        }

        inspection_complete = true;
        if (stats.groups_cleared > 0) {
            self.runtime_status_dirty.store(true, .release);
            self.store_status_dirty = true;
            self.provisioned_root_refresh_dirty.store(true, .release);
            self.requestRuntimeStatusRefresh() catch |err| switch (err) {
                error.ThreadQuotaExceeded,
                error.SystemResources,
                => std.log.warn("provisioned startup catch-up runtime status refresh deferred err={}", .{err}),
                else => std.log.warn("provisioned startup catch-up runtime status refresh failed err={}", .{err}),
            };
        }
        return stats;
    }

    fn adminSnapshotPreferCached(self: *DataServer) !?antfly.metadata_api.AdminSnapshot {
        if (try self.status_source.cachedAdminSnapshot()) |snapshot| return snapshot;
        return try self.status_source.adminSnapshot();
    }

    fn maybeRequestRuntimeStatusRefresh(self: *DataServer) !void {
        const registration = self.store_registration orelse return;
        _ = registration;
        if (!self.runtime_status_dirty.load(.acquire)) return;

        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        const last_at_ms = self.runtime_status_last_refresh_at_ms.load(.monotonic);
        if (last_at_ms != 0 and now_ms -| last_at_ms < runtime_status_refresh_interval_ms) return;
        try self.requestRuntimeStatusRefresh();
    }

    fn maybeRequestProvisionedRootRefresh(self: *DataServer) !void {
        const remote_metadata = self.remote_metadata orelse return;
        const registration = self.store_registration orelse return;
        _ = registration;

        if (self.provisioned_root_refresh_active.load(.acquire)) return;

        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        const last_run_at_ms = self.provisioned_root_refresh_last_run_at_ms.load(.monotonic);
        if (last_run_at_ms != 0 and
            now_ms -| last_run_at_ms < provision_head_poll_startup_interval_ms)
        {
            return;
        }

        if (!self.provisioned_root_refresh_dirty.load(.acquire)) {
            const poll_interval_ms = if (self.last_provision_metadata_epoch == null or self.last_provision_fingerprint == null)
                provision_head_poll_startup_interval_ms
            else
                provision_head_poll_interval_ms;
            if (self.last_provision_head_check_at_ms != 0 and
                now_ms -| self.last_provision_head_check_at_ms < poll_interval_ms)
            {
                return;
            }
            self.last_provision_head_check_at_ms = now_ms;

            const head = try remote_metadata.fetchHead();
            if (self.last_provision_metadata_epoch == head.metadata_epoch and self.last_provision_fingerprint != null) return;
            self.provisioned_root_refresh_dirty.store(true, .release);
        }

        try self.requestProvisionedRootRefresh();
    }

    fn startupCatchUpGroupDisposition(
        snapshot_stores: []const antfly.metadata.table_manager.StoreRecord,
        placement_intents: []const antfly.raft.reconciler.PlacementIntent,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        registration: StoreRegistrationConfig,
        group_leadership_source: ?GroupLeadershipSource,
        group_id: u64,
    ) StartupCatchUpGroupDisposition {
        const source = group_leadership_source orelse return .attempt;
        if (source.isLocalLeader(group_id)) return .attempt;

        if (findMergedSnapshotGroupStatus(merged_group_statuses, group_id)) |status| {
            if (status.leader_known) {
                return if (status.leader_store_id == registration.store_id)
                    .attempt
                else
                    .skip_nonlocal;
            }
        }

        if (hasSingleRoleStore(snapshot_stores, registration.role, registration.store_id)) {
            return .attempt;
        }

        var placement_count: usize = 0;
        var local_match = false;
        for (placement_intents) |intent| {
            if (intent.record.group_id != group_id) continue;
            placement_count += 1;
            if (intent.record.local_node_id == registration.node_id or intent.store_id == registration.store_id) {
                local_match = true;
            }
        }
        if (placement_count == 1 and local_match) return .attempt;
        if (local_match) return .retry_unknown;
        return .skip_nonlocal;
    }

    fn maybeRequestProvisionedStartupCatchUp(self: *DataServer) !void {
        const registration = self.store_registration orelse return;
        _ = registration;
        if (!self.provisioned_startup_catch_up_dirty.load(.acquire)) return;

        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        const last_at_ms = self.provisioned_startup_catch_up_last_run_at_ms.load(.monotonic);
        if (last_at_ms != 0 and now_ms -| last_at_ms < provisioned_startup_catch_up_interval_ms) return;
        try self.requestProvisionedStartupCatchUp();
    }

    const ProvisionedRootRefreshThreadSpawner = *const fn (*DataServer) anyerror!std.Thread;
    const ProvisionedStartupCatchUpThreadSpawner = *const fn (*DataServer) anyerror!std.Thread;

    fn requestAutoBulkFinishBackground(self: *DataServer) !void {
        if (@import("builtin").is_test) {
            self.runAutoBulkFinish();
            return;
        }

        lockAtomic(&self.auto_bulk_finish_mutex);
        defer self.auto_bulk_finish_mutex.unlock();
        if (self.auto_bulk_finish_future != null) return;
        if (self.auto_bulk_finish_io == null) {
            self.auto_bulk_finish_io = std.Io.Threaded.init(self.alloc, .{});
        }
        self.auto_bulk_finish_stop.store(false, .release);
        const io = self.auto_bulk_finish_io.?.io();
        errdefer self.auto_bulk_finish_stop.store(true, .release);
        self.auto_bulk_finish_future = try io.concurrent(autoBulkFinishWorkerMain, .{self});
    }

    fn requestRuntimeStatusRefresh(self: *DataServer) !void {
        if (@import("builtin").is_test) {
            _ = self.runRuntimeStatusRefresh();
            return;
        }

        self.reapRuntimeStatusRefreshThread();
        if (self.runtime_status_refresh_active.load(.acquire)) return;

        lockAtomic(&self.runtime_status_refresh_mutex);
        defer self.runtime_status_refresh_mutex.unlock();
        if (self.runtime_status_refresh_active.load(.acquire)) return;
        self.runtime_status_refresh_active.store(true, .release);
        self.runtime_status_refresh_thread = try std.Thread.spawn(.{}, runtimeStatusRefreshWorkerMain, .{self});
    }

    fn requestProvisionedRootRefresh(self: *DataServer) !void {
        const registration = self.store_registration orelse return;
        _ = registration;
        try self.requestProvisionedRootRefreshWithSpawner(spawnProvisionedRootRefreshThreadMain);
    }

    fn requestProvisionedRootRefreshWithSpawner(
        self: *DataServer,
        spawner: ProvisionedRootRefreshThreadSpawner,
    ) !void {
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));

        self.reapProvisionedRootRefreshThread();
        if (self.provisioned_root_refresh_active.load(.acquire)) return;

        lockAtomic(&self.provisioned_root_refresh_mutex);
        defer self.provisioned_root_refresh_mutex.unlock();
        if (self.provisioned_root_refresh_active.load(.acquire)) return;
        self.provisioned_root_refresh_active.store(true, .release);
        errdefer self.provisioned_root_refresh_active.store(false, .release);
        self.provisioned_root_refresh_thread = try spawner(self);
        self.provisioned_root_refresh_last_run_at_ms.store(now_ms, .monotonic);
    }

    fn requestProvisionedStartupCatchUp(self: *DataServer) !void {
        const registration = self.store_registration orelse return;
        _ = registration;
        if (@import("builtin").is_test) {
            _ = self.runProvisionedStartupCatchUp();
            return;
        }

        try self.requestProvisionedStartupCatchUpWithSpawner(spawnProvisionedStartupCatchUpThreadMain);
    }

    pub fn requestProvisionedStartupCatchUpNow(self: *DataServer) !void {
        try self.requestProvisionedStartupCatchUp();
    }

    fn requestProvisionedStartupCatchUpWithSpawner(
        self: *DataServer,
        spawner: ProvisionedStartupCatchUpThreadSpawner,
    ) !void {
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));

        self.reapProvisionedStartupCatchUpThread();
        if (self.provisioned_startup_catch_up_active.load(.acquire)) return;

        lockAtomic(&self.provisioned_startup_catch_up_mutex);
        defer self.provisioned_startup_catch_up_mutex.unlock();
        if (self.provisioned_startup_catch_up_active.load(.acquire)) return;
        self.provisioned_startup_catch_up_active.store(true, .release);
        errdefer self.provisioned_startup_catch_up_active.store(false, .release);
        self.provisioned_startup_catch_up_thread = try spawner(self);
        self.provisioned_startup_catch_up_last_run_at_ms.store(now_ms, .monotonic);
    }

    fn spawnProvisionedStartupCatchUpThreadMain(self: *DataServer) !std.Thread {
        return try std.Thread.spawn(.{}, provisionedStartupCatchUpWorkerMain, .{self});
    }

    fn spawnProvisionedRootRefreshThreadMain(self: *DataServer) !std.Thread {
        return try std.Thread.spawn(.{}, provisionedRootRefreshWorkerMain, .{self});
    }

    fn runtimeStatusRefreshWorkerMain(self: *DataServer) void {
        defer self.runtime_status_refresh_active.store(false, .release);
        _ = self.runRuntimeStatusRefresh();
    }

    fn autoBulkFinishWorkerMain(self: *DataServer) void {
        while (!self.auto_bulk_finish_stop.load(.acquire)) {
            const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
            const last_run_at_ms = self.auto_bulk_finish_last_run_at_ms.load(.monotonic);
            if (last_run_at_ms == 0 or now_ms -| last_run_at_ms >= auto_bulk_finish_poll_interval_ms) {
                self.auto_bulk_finish_active.store(true, .release);
                self.runAutoBulkFinish();
                self.auto_bulk_finish_active.store(false, .release);
                self.auto_bulk_finish_last_run_at_ms.store(now_ms, .monotonic);
            }
            if (self.auto_bulk_finish_io) |*io_impl| {
                io_impl.io().sleep(std.Io.Duration.fromMilliseconds(auto_bulk_finish_poll_interval_ms), .awake) catch {};
            } else {
                break;
            }
        }
    }

    fn provisionedRootRefreshWorkerMain(self: *DataServer) void {
        defer self.provisioned_root_refresh_active.store(false, .release);
        self.runProvisionedRootRefresh();
    }

    fn provisionedStartupCatchUpWorkerMain(self: *DataServer) void {
        defer self.clearProvisionedStartupCatchUpTarget();
        defer self.provisioned_startup_catch_up_active.store(false, .release);
        _ = self.runProvisionedStartupCatchUp();
    }

    fn runAutoBulkFinish(self: *DataServer) void {
        const started_ns = platform_time.monotonicNs();
        _ = self.auto_bulk_finish_started.fetchAdd(1, .monotonic);
        defer self.auto_bulk_finish_last_duration_ns.store(platform_time.monotonicNs() - started_ns, .monotonic);

        const finished = self.write_source.tryFinishExpiredAutoBulkIngestAndPublishStatus(self.alloc) orelse {
            _ = self.auto_bulk_finish_lock_deferred.fetchAdd(1, .monotonic);
            _ = self.auto_bulk_finish_completed.fetchAdd(1, .monotonic);
            return;
        };
        if (finished) {
            self.runtime_status_dirty.store(true, .release);
            self.store_status_dirty = true;
            self.requestRuntimeStatusRefresh() catch |err| switch (err) {
                error.ThreadQuotaExceeded,
                error.SystemResources,
                => std.log.warn("auto bulk ingest finish runtime status refresh deferred err={}", .{err}),
                else => {
                    _ = self.auto_bulk_finish_failed.fetchAdd(1, .monotonic);
                    std.log.warn("auto bulk ingest finish runtime status refresh failed err={}", .{err});
                },
            };
        }
        _ = self.auto_bulk_finish_completed.fetchAdd(1, .monotonic);
    }

    fn runRuntimeStatusRefresh(self: *DataServer) RuntimeStatusRefreshStats {
        return self.runRuntimeStatusRefreshWithBudget(runtime_status_refresh_max_db_opens_per_run);
    }

    fn logRuntimeStatusRefreshFailure(comptime message: []const u8, err: anyerror) void {
        switch (err) {
            error.FileNotFound,
            error.UnknownGroup,
            error.UnknownStore,
            error.LmdbUnexpected,
            error.Corrupted,
            error.HttpConnectionClosing,
            error.ConnectionResetByPeer,
            error.ConnectionRefused,
            error.BrokenPipe,
            error.EndOfStream,
            => std.log.debug(message, .{err}),
            else => std.log.warn(message, .{err}),
        }
    }

    fn runRuntimeStatusRefreshWithBudget(self: *DataServer, max_db_opens: usize) RuntimeStatusRefreshStats {
        const started_ns = platform_time.monotonicNs();
        _ = self.runtime_status_refresh_started.fetchAdd(1, .monotonic);
        var stats: RuntimeStatusRefreshStats = .{};
        var budget: RuntimeStatusRefreshBudget = .{ .max_db_opens = max_db_opens };
        var pending_runtime_work = false;
        var startup_catch_up_debt_present = false;
        defer self.runtime_status_refresh_last_table_count.store(stats.table_count, .monotonic);
        defer self.runtime_status_refresh_last_group_count.store(stats.group_count, .monotonic);
        defer self.runtime_status_refresh_last_db_opens.store(stats.db_opens, .monotonic);
        defer self.runtime_status_refresh_last_skipped_db_opens.store(stats.skipped_db_opens, .monotonic);
        defer self.runtime_status_refresh_last_placeholder_group_count.store(stats.placeholder_group_count, .monotonic);
        defer self.runtime_status_refresh_last_duration_ns.store(stats.duration_ns, .monotonic);
        defer {
            stats.db_opens = @intCast(budget.db_opens);
            stats.skipped_db_opens = @intCast(budget.skipped_db_opens);
            stats.placeholder_group_count = @intCast(budget.placeholder_group_count);
        }
        defer stats.duration_ns = platform_time.monotonicNs() - started_ns;

        const active_target = self.snapshotProvisionedStartupCatchUpTarget() catch |err| {
            _ = self.runtime_status_refresh_failed.fetchAdd(1, .monotonic);
            logRuntimeStatusRefreshFailure("runtime status refresh target snapshot failed err={}", err);
            return stats;
        };
        var active_target_owned = active_target;
        defer if (active_target_owned) |*target| target.deinit(self.alloc);

        const snapshots = self.collectOwnedRuntimeSnapshots(active_target_owned, &budget) catch |err| {
            _ = self.runtime_status_refresh_failed.fetchAdd(1, .monotonic);
            logRuntimeStatusRefreshFailure("runtime status refresh failed err={}", err);
            return stats;
        };
        defer {
            if (snapshots.len > 0) self.alloc.free(snapshots);
        }
        stats.db_opens = @intCast(budget.db_opens);
        stats.skipped_db_opens = @intCast(budget.skipped_db_opens);
        stats.placeholder_group_count = @intCast(budget.placeholder_group_count);
        stats.table_count = @intCast(snapshots.len);
        for (snapshots) |entry| {
            stats.group_count += @intCast(entry.statuses.items.len);
            if (runtimeStatusWorkPending(entry.statuses.items)) pending_runtime_work = true;
            if (runtimeStatusStartupCatchUpDebtPresent(entry.statuses.items)) startup_catch_up_debt_present = true;
        }
        if (active_target_owned) |target| {
            self.provisioned_storage.runtime_status_cache.replaceOwnedPreservingGroupStatus(snapshots, target.table_name, target.group_id) catch |err| {
                _ = self.runtime_status_refresh_failed.fetchAdd(1, .monotonic);
                std.log.warn("runtime status refresh cache merge failed err={}", .{err});
                return stats;
            };
        } else {
            self.provisioned_storage.runtime_status_cache.replaceOwned(snapshots);
        }
        self.provisioned_startup_catch_up_dirty.store(startup_catch_up_debt_present, .release);
        self.runtime_status_last_refresh_at_ms.store(
            @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms)),
            .monotonic,
        );
        self.runtime_status_dirty.store(pending_runtime_work, .release);
        self.store_status_dirty = true;
        _ = self.runtime_status_refresh_completed.fetchAdd(1, .monotonic);
        return stats;
    }

    fn runtimeStatusWorkPending(statuses: []const runtime_status.LocalTableRuntimeStatus) bool {
        for (statuses) |status| {
            if (runtimeStatusHasActiveBackgroundWork(status)) return true;
        }
        return false;
    }

    fn runtimeStatusStartupCatchUpDebtPresent(statuses: []const runtime_status.LocalTableRuntimeStatus) bool {
        for (statuses) |status| {
            for (status.stats.indexes) |index| {
                if (index.replay_catch_up_required or index.backfill_active) return true;
            }
        }
        return false;
    }

    fn setProvisionedStartupCatchUpTarget(self: *DataServer, group_id: u64, table_name: []const u8) !void {
        lockAtomic(&self.provisioned_startup_catch_up_target_mutex);
        defer self.provisioned_startup_catch_up_target_mutex.unlock();
        if (self.provisioned_startup_catch_up_target_table_name) |existing| {
            self.alloc.free(existing);
        }
        self.provisioned_startup_catch_up_target_table_name = try self.alloc.dupe(u8, table_name);
        self.provisioned_startup_catch_up_target_group_id = group_id;
    }

    fn clearProvisionedStartupCatchUpTarget(self: *DataServer) void {
        lockAtomic(&self.provisioned_startup_catch_up_target_mutex);
        defer self.provisioned_startup_catch_up_target_mutex.unlock();
        if (self.provisioned_startup_catch_up_target_table_name) |existing| {
            self.alloc.free(existing);
            self.provisioned_startup_catch_up_target_table_name = null;
        }
        self.provisioned_startup_catch_up_target_group_id = 0;
    }

    fn snapshotProvisionedStartupCatchUpTarget(self: *DataServer) !?ActiveStartupCatchUpTarget {
        lockAtomic(&self.provisioned_startup_catch_up_target_mutex);
        defer self.provisioned_startup_catch_up_target_mutex.unlock();
        const table_name = self.provisioned_startup_catch_up_target_table_name orelse return null;
        return .{
            .group_id = self.provisioned_startup_catch_up_target_group_id,
            .table_name = try self.alloc.dupe(u8, table_name),
        };
    }

    fn collectOwnedRuntimeSnapshots(
        self: *DataServer,
        active_target: ?ActiveStartupCatchUpTarget,
        budget: *RuntimeStatusRefreshBudget,
    ) ![]runtime_status.TableRuntimeSnapshot {
        const registration = self.store_registration orelse return error.UnsupportedOperation;
        var snapshot = (try self.status_source.adminSnapshot()) orelse return error.UnsupportedOperation;
        defer self.status_source.freeAdminSnapshot(&snapshot);

        var local_group_ids = try collectLocalGroupIds(self.alloc, snapshot.placement_intents, registration.node_id);
        if (local_group_ids.len == 0 and hasSingleRoleStore(snapshot.stores, registration.role, registration.store_id)) {
            const fallback_group_ids = try collectAllRangeGroupIds(self.alloc, snapshot.ranges);
            self.alloc.free(local_group_ids);
            local_group_ids = fallback_group_ids;
        }
        defer self.alloc.free(local_group_ids);

        var snapshots = std.ArrayListUnmanaged(runtime_status.TableRuntimeSnapshot).empty;
        errdefer {
            for (snapshots.items) |*entry| entry.deinit(self.alloc);
            snapshots.deinit(self.alloc);
        }

        for (snapshot.tables) |table| {
            if (try self.collectOwnedRuntimeSnapshotForTable(snapshot.tables, snapshot.ranges, local_group_ids, table, active_target, budget)) |entry| {
                var owned_entry = entry;
                annotateRuntimeSnapshotOwner(&owned_entry, registration);
                try snapshots.append(self.alloc, owned_entry);
            }
        }

        return try snapshots.toOwnedSlice(self.alloc);
    }

    fn annotateRuntimeSnapshotOwner(
        snapshot: *runtime_status.TableRuntimeSnapshot,
        registration: StoreRegistrationConfig,
    ) void {
        for (snapshot.statuses.items) |*status| {
            if (status.metadata.store_id == 0) status.metadata.store_id = registration.store_id;
            if (status.metadata.node_id == 0) status.metadata.node_id = registration.node_id;
        }
    }

    fn collectOwnedRuntimeSnapshotForTable(
        self: *DataServer,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        local_group_ids: []const u64,
        table: antfly.metadata.table_manager.TableRecord,
        active_target: ?ActiveStartupCatchUpTarget,
        budget: *RuntimeStatusRefreshBudget,
    ) !?runtime_status.TableRuntimeSnapshot {
        _ = tables;
        var items = std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus).empty;
        errdefer {
            for (items.items) |*item| item.deinit(self.alloc);
            items.deinit(self.alloc);
        }
        for (local_group_ids) |group_id| {
            const range = findRangeByGroupId(ranges, group_id) orelse continue;
            if (range.table_id != table.table_id) continue;

            if (try self.snapshotCachedActiveStartupGroupStatus(table.name, group_id, active_target)) |cached| {
                var status = cached;
                self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
                try items.append(self.alloc, status);
                continue;
            }
            if (self.shouldSkipActiveStartupGroupStatusOpen(table.name, group_id, active_target)) {
                continue;
            }
            if (try self.snapshotManagedWriterGroupStatusBestEffort(self.alloc, table.name, group_id)) |live_status| {
                var status = live_status;
                status.metadata = status.metadata.withDefaults(.live_writer_publish, platform_time.monotonicNs());
                self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
                try items.append(self.alloc, status);
                continue;
            }
            if (self.hasReadBlockingActivityBestEffort(table.name, group_id)) {
                if (try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(self.alloc, table.name, group_id)) |cached| {
                    var status = cached;
                    self.overlayManagedWriterGroupStatusBestEffort(self.alloc, table.name, group_id, &status);
                    self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
                    try items.append(self.alloc, status);
                    continue;
                }
                if (try syntheticConfiguredRuntimeStatus(self.alloc, table, group_id)) |synthetic| {
                    var status = synthetic;
                    self.overlayManagedWriterGroupStatusBestEffort(self.alloc, table.name, group_id, &status);
                    self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
                    try items.append(self.alloc, status);
                }
                continue;
            }
            switch (self.probeManagedWriterGroupBestEffort(table.name, group_id)) {
                .leased => |cached| {
                    var lease = cached;
                    const release_alloc = if (lease.cache) |cache| cache.alloc else std.heap.page_allocator;
                    defer lease.deinit(release_alloc);
                    var status = runtime_status.LocalTableRuntimeStatus{
                        .group_id = group_id,
                        .metadata = .{
                            .updated_at_ns = platform_time.monotonicNs(),
                            .source = .live_writer_publish,
                            .freshness = .fresh,
                        },
                        .stats = try lease.db.stats(self.alloc),
                    };
                    errdefer status.deinit(self.alloc);
                    self.overlayManagedWriterGroupStatusBestEffort(self.alloc, table.name, group_id, &status);
                    self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, lease.db);
                    try items.append(self.alloc, status);
                    continue;
                },
                .unknown => {
                    if (try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(self.alloc, table.name, group_id)) |cached| {
                        var status = cached;
                        self.overlayManagedWriterGroupStatusBestEffort(self.alloc, table.name, group_id, &status);
                        self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
                        try items.append(self.alloc, status);
                        continue;
                    }
                },
                .absent => {},
            }
            if (try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(self.alloc, table.name, group_id)) |cached| {
                var status = cached;
                if (runtime_status.statusHasRuntimeFacts(status)) {
                    setRuntimeStatusMetadata(&status, .cached_snapshot, .stale);
                    self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
                    try items.append(self.alloc, status);
                    continue;
                }
                status.deinit(self.alloc);
            }
            // Runtime status refresh is a control-plane cache publisher. A cold
            // DB open can replay a large WAL and rebuild transient LSM state,
            // which is exactly the work this path must avoid during hot loads.
            // If there is no live writer and no cached runtime status, publish
            // schema-derived stale status and let the writer/runtime publish
            // concrete counts when it is active again.
            try self.appendCachedOrSyntheticRuntimeStatus(&items, table, group_id, .stale, budget);
        }

        if (items.items.len == 0) {
            items.deinit(self.alloc);
            return null;
        }
        return .{
            .table_name = try self.alloc.dupe(u8, table.name),
            .statuses = .{ .items = try items.toOwnedSlice(self.alloc) },
        };
    }

    fn runtimeStatusHasActiveBackgroundWork(status: runtime_status.LocalTableRuntimeStatus) bool {
        if (status.stats.enrichment.retrying) return true;
        if (status.stats.enrichment.target_sequence > status.stats.enrichment.applied_sequence) return true;
        if (status.stats.async_indexing.startup.active) return true;
        if (status.stats.async_indexing.dense_catch_up.active) return true;
        if (status.stats.async_indexing.bulk_coalescing.active_session) return true;
        for (status.stats.indexes) |index| {
            if (index.backfill_active) return true;
            if (index.catch_up_active) return true;
            if (index.replay_catch_up_required) return true;
            if (index.replay_target_sequence > index.replay_applied_sequence) return true;
        }
        return false;
    }

    fn appendCachedOrSyntheticRuntimeStatus(
        self: *DataServer,
        items: *std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus),
        table: antfly.metadata.table_manager.TableRecord,
        group_id: u64,
        freshness: runtime_status.RuntimeStatusFreshness,
        budget: *RuntimeStatusRefreshBudget,
    ) !void {
        if (try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(self.alloc, table.name, group_id)) |cached| {
            var status = cached;
            if (runtime_status.statusHasRuntimeFacts(status)) {
                setRuntimeStatusMetadata(&status, .cached_snapshot, freshness);
                self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
                try items.append(self.alloc, status);
                return;
            }
            status.deinit(self.alloc);
        }
        if (try syntheticConfiguredRuntimeStatus(self.alloc, table, group_id)) |synthetic| {
            var status = synthetic;
            setRuntimeStatusMetadata(&status, .synthetic_config, freshness);
            self.applyRuntimeStatusStorageFactsBestEffort(&status, group_id, null);
            try items.append(self.alloc, status);
            budget.recordPlaceholder();
            return;
        }
        var missing_status = runtime_status.LocalTableRuntimeStatus{
            .group_id = group_id,
            .metadata = .{
                .updated_at_ns = platform_time.monotonicNs(),
                .source = .synthetic_config,
                .freshness = freshness,
            },
            .stats = .{},
        };
        self.applyRuntimeStatusStorageFactsBestEffort(&missing_status, group_id, null);
        try items.append(self.alloc, missing_status);
        budget.recordPlaceholder();
    }

    fn setRuntimeStatusMetadata(
        status: *runtime_status.LocalTableRuntimeStatus,
        source: runtime_status.RuntimeStatusSource,
        freshness: runtime_status.RuntimeStatusFreshness,
    ) void {
        status.metadata.source = source;
        status.metadata.freshness = freshness;
        status.metadata.updated_at_ns = platform_time.monotonicNs();
    }

    fn syntheticConfiguredRuntimeStatus(
        alloc: std.mem.Allocator,
        table: antfly.metadata.table_manager.TableRecord,
        group_id: u64,
    ) !?runtime_status.LocalTableRuntimeStatus {
        if (table.indexes_json.len == 0) return null;
        var parsed = std.json.parseFromSlice(std.json.Value, alloc, table.indexes_json, .{}) catch return null;
        defer parsed.deinit();

        const object = switch (parsed.value) {
            .object => |object| object,
            else => return null,
        };
        const array_form = object.get("indexes");
        const index_count: usize = if (array_form) |value| switch (value) {
            .array => value.array.items.len,
            else => return null,
        } else object.count();
        if (index_count == 0) return null;

        const indexes = try alloc.alloc(antfly.db.types.DBIndexStats, index_count);
        var initialized: usize = 0;
        errdefer {
            for (indexes[0..initialized]) |*index| alloc.free(@constCast(index.name));
            alloc.free(indexes);
        }

        if (array_form) |value| {
            const array_items = switch (value) {
                .array => |array| array.items,
                else => unreachable,
            };
            for (array_items) |item| {
                if (item != .object) return null;
                const name_value = item.object.get("name") orelse return null;
                if (name_value != .string) return null;
                const kind = parseRuntimeIndexKind(item) orelse return null;
                indexes[initialized] = .{
                    .name = try alloc.dupe(u8, name_value.string),
                    .kind = kind,
                };
                initialized += 1;
            }
        } else {
            var it = object.iterator();
            while (it.next()) |entry| {
                const kind = parseRuntimeIndexKind(entry.value_ptr.*) orelse return null;
                indexes[initialized] = .{
                    .name = try alloc.dupe(u8, entry.key_ptr.*),
                    .kind = kind,
                };
                initialized += 1;
            }
        }

        return .{
            .group_id = group_id,
            .metadata = .{
                .updated_at_ns = platform_time.monotonicNs(),
                .source = .synthetic_config,
                .freshness = .missing,
            },
            .stats = .{
                .index_count = @intCast(indexes.len),
                .indexes = indexes,
            },
        };
    }

    fn parseRuntimeIndexKind(value: std.json.Value) ?antfly.db.types.IndexKind {
        if (value != .object) return .full_text;
        const type_value = value.object.get("type") orelse return .full_text;
        if (type_value != .string) return null;
        if (std.mem.eql(u8, type_value.string, "full_text")) return .full_text;
        if (std.mem.eql(u8, type_value.string, "graph")) return .graph;
        if (std.mem.eql(u8, type_value.string, "embeddings")) {
            const sparse = if (value.object.get("sparse")) |sparse_value| switch (sparse_value) {
                .bool => sparse_value.bool,
                else => return null,
            } else false;
            return if (sparse) .sparse_vector else .dense_vector;
        }
        return null;
    }

    fn shouldSkipActiveStartupGroupStatusOpen(
        self: *DataServer,
        table_name: []const u8,
        group_id: u64,
        active_target: ?ActiveStartupCatchUpTarget,
    ) bool {
        if (!self.provisioned_startup_catch_up_active.load(.acquire)) return false;
        const target = active_target orelse return false;
        if (target.group_id != group_id) return false;
        return std.mem.eql(u8, target.table_name, table_name);
    }

    fn snapshotCachedActiveStartupGroupStatus(
        self: *DataServer,
        table_name: []const u8,
        group_id: u64,
        active_target: ?ActiveStartupCatchUpTarget,
    ) !?runtime_status.LocalTableRuntimeStatus {
        if (!self.write_source.startup_catch_up_active.load(.acquire)) return null;
        if (try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(self.alloc, table_name, group_id)) |cached| {
            return cached;
        }
        const target = active_target orelse return null;
        if (target.group_id != group_id) return null;
        if (!std.mem.eql(u8, target.table_name, table_name)) return null;
        return null;
    }

    fn requestLocalGroupStatusRefreshWithSources(
        self: *DataServer,
        generation: u64,
        fingerprint: u64,
        replica_root_dir: []const u8,
        group_ids: []const u64,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
        stores: []const antfly.metadata.table_manager.StoreRecord,
        merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
        split_transitions: []const antfly.metadata.SplitTransitionRecord,
        merge_transitions: []const antfly.metadata.MergeTransitionRecord,
        split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
        merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
        inferred_group_leadership: ?InferredSnapshotLeadershipConfig,
        group_leadership_source: ?GroupLeadershipSource,
        group_membership_source: ?GroupMembershipSource,
    ) !void {
        if (self.provisioned_startup_catch_up_active.load(.acquire)) return;

        if (@import("builtin").is_test) {
            var refresh = try OwnedLocalGroupStatusRefresh.init(
                self.alloc,
                self,
                generation,
                fingerprint,
                replica_root_dir,
                group_ids,
                tables,
                ranges,
                stores,
                merged_group_statuses,
                split_transitions,
                merge_transitions,
                split_observations,
                merge_observations,
                inferred_group_leadership,
                group_leadership_source,
                group_membership_source,
            );
            defer refresh.deinit();
            self.runOwnedLocalGroupStatusRefresh(&refresh);
            return;
        }

        self.reapLocalGroupStatusRefreshThread();
        if (self.local_group_status_refresh_active.load(.acquire)) return;

        const refresh = try self.alloc.create(OwnedLocalGroupStatusRefresh);
        errdefer self.alloc.destroy(refresh);
        refresh.* = try OwnedLocalGroupStatusRefresh.init(
            self.alloc,
            self,
            generation,
            fingerprint,
            replica_root_dir,
            group_ids,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
            inferred_group_leadership,
            group_leadership_source,
            group_membership_source,
        );
        errdefer refresh.deinit();

        lockAtomic(&self.local_group_status_refresh_mutex);
        defer self.local_group_status_refresh_mutex.unlock();
        if (self.local_group_status_refresh_active.load(.acquire)) {
            refresh.deinit();
            self.alloc.destroy(refresh);
            return;
        }
        self.local_group_status_refresh_active.store(true, .release);
        self.local_group_status_refresh_thread = try std.Thread.spawn(.{}, localGroupStatusRefreshWorkerMain, .{refresh});
    }

    fn runOwnedLocalGroupStatusRefresh(self: *DataServer, refresh: *OwnedLocalGroupStatusRefresh) void {
        const group_statuses = self.collectLiveLocalGroupStatusesWithSources(
            self.alloc,
            refresh.replica_root_dir,
            refresh.group_ids,
            refresh.tables,
            refresh.ranges,
            refresh.effectiveGroupLeadershipSource(),
            refresh.group_membership_source,
            refresh.stores,
            refresh.merged_group_statuses,
            refresh.split_transitions,
            refresh.merge_transitions,
            refresh.split_observations,
            refresh.merge_observations,
        ) catch |err| {
            std.log.warn("store status local group refresh failed err={}", .{err});
            return;
        };
        defer freeGroupStatusesOwned(self.alloc, group_statuses);

        self.storeCachedLocalGroupStatuses(refresh.generation, refresh.fingerprint, group_statuses) catch |err| {
            std.log.warn("store status local group cache update failed err={}", .{err});
            return;
        };
        self.store_status_dirty = true;
    }

    fn localGroupStatusRefreshWorkerMain(refresh: *OwnedLocalGroupStatusRefresh) void {
        const self = refresh.server;
        defer {
            refresh.deinit();
            self.alloc.destroy(refresh);
            self.local_group_status_refresh_active.store(false, .release);
        }
        self.runOwnedLocalGroupStatusRefresh(refresh);
    }

    fn runProvisionedRootRefresh(self: *DataServer) void {
        const started_ns = platform_time.monotonicNs();
        _ = self.provisioned_root_refresh_started.fetchAdd(1, .monotonic);
        self.provisioned_root_refresh_dirty.store(false, .release);
        defer self.provisioned_root_refresh_last_duration_ns.store(platform_time.monotonicNs() - started_ns, .monotonic);

        self.refreshProvisionedReplicaRoot() catch |err| {
            self.provisioned_root_refresh_dirty.store(true, .release);
            _ = self.provisioned_root_refresh_failed.fetchAdd(1, .monotonic);
            std.log.warn("provisioned replica-root refresh failed err={}", .{err});
            return;
        };
        _ = self.provisioned_root_refresh_completed.fetchAdd(1, .monotonic);
    }

    fn refreshProvisionedReplicaRoot(self: *DataServer) !void {
        const remote_metadata = self.remote_metadata orelse return;
        const registration = self.store_registration orelse return;
        self.last_provision_head_check_at_ms = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));

        const head = try remote_metadata.fetchHead();
        if (self.last_provision_metadata_epoch == head.metadata_epoch and self.last_provision_fingerprint != null) return;

        var snapshot = try remote_metadata.fetchSnapshotForHead(head);
        defer freeAdminSnapshotOwned(self.alloc, &snapshot);

        var local_group_ids = try collectLocalGroupIds(self.alloc, snapshot.placement_intents, registration.node_id);
        if (local_group_ids.len == 0 and hasSingleRoleStore(snapshot.stores, registration.role, registration.store_id)) {
            const fallback_group_ids = try collectAllRangeGroupIds(self.alloc, snapshot.ranges);
            self.alloc.free(local_group_ids);
            local_group_ids = fallback_group_ids;
        }
        defer self.alloc.free(local_group_ids);
        self.provisioned_storage.pruneGroupGenerations(local_group_ids);
        if (local_group_ids.len == 0) {
            self.provisioned_storage.read_cache.clear();
            self.write_source.clearWriteCache();
            self.last_provision_fingerprint = null;
            self.last_provision_metadata_epoch = head.metadata_epoch;
            self.invalidateLocalGroupStatusCache();
            self.runtime_status_dirty.store(true, .release);
            // Do not disarm startup catch-up here. An empty local-group snapshot
            // can be transient during startup or metadata convergence, and the
            // catch-up worker itself treats "no local groups yet" as retriable.
            return;
        }

        if (self.shouldDeferProvisionedReplicaRootReconcile()) {
            self.last_provision_metadata_epoch = head.metadata_epoch;
            self.last_provision_fingerprint = null;
            self.provisioned_root_refresh_dirty.store(true, .release);
            self.provisioned_startup_catch_up_dirty.store(true, .release);
            return;
        }

        lockAtomic(self.write_source.localDbMutex());
        defer self.write_source.localDbMutex().unlock();

        try self.reportLocalSchemaProgress(head.metadata_group_id, registration.node_id, local_group_ids, snapshot.tables, snapshot.ranges);

        const fingerprint = antfly.metadata.table_provisioner.provisioningFingerprint(
            head.metadata_group_id,
            local_group_ids,
            snapshot.tables,
            snapshot.ranges,
        );
        if (self.last_provision_fingerprint == fingerprint) {
            self.last_provision_metadata_epoch = head.metadata_epoch;
            return;
        }
        _ = try self.write_source.reconcileReplicaRootTablesWithWriteCacheLocked(
            self.alloc,
            head.metadata_group_id,
            local_group_ids,
            snapshot.tables,
            snapshot.ranges,
            try self.ensureBackendRuntime(),
        );
        try self.provisioned_storage.bumpGroupGenerations(local_group_ids);
        self.provisioned_storage.read_cache.clear();
        self.write_source.pruneStaleWriteCacheLocked();
        self.last_provision_fingerprint = fingerprint;
        self.last_provision_metadata_epoch = head.metadata_epoch;
        self.invalidateLocalGroupStatusCache();
        self.runtime_status_dirty.store(true, .release);
        self.provisioned_startup_catch_up_dirty.store(true, .release);
        self.store_status_dirty = true;
    }

    pub fn shouldDeferProvisionedReplicaRootReconcile(self: *const DataServer) bool {
        if (self.provisioned_startup_catch_up_active.load(.acquire)) return true;
        return self.provisioned_startup_catch_up_dirty.load(.acquire) and
            self.last_provision_metadata_epoch != null and
            self.last_provision_fingerprint == null;
    }

    fn reportLocalSchemaProgress(
        self: *DataServer,
        metadata_group_id: u64,
        local_node_id: u64,
        local_group_ids: []const u64,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
    ) !void {
        const remote_metadata = self.remote_metadata orelse return;
        var shard_db = antfly.metadata.FallbackLocalShardDbAdapter{
            .replica_root_dir = self.write_source.replica_root_dir,
            .backend_runtime = try self.ensureBackendRuntime(),
        };
        const local_progress = try antfly.metadata.table_provisioner.collectLocalSchemaProgressWithOptions(
            self.alloc,
            self.write_source.replica_root_dir,
            metadata_group_id,
            local_node_id,
            local_group_ids,
            tables,
            ranges,
            .{
                .backend_runtime = shard_db.backend_runtime,
                .shard_db_adapter = shard_db.adapter(),
            },
        );
        defer self.alloc.free(local_progress);

        for (local_progress) |record| {
            try remote_metadata.upsertSchemaProgress(record);
        }
    }

    fn reportRuntimeSchemaProgress(
        self: *DataServer,
        remote_metadata: *RemoteMetadataSource,
        store_id: u64,
        local_node_id: u64,
        runtime_statuses: []antfly.metadata.table_manager.RuntimeGroupStatusReport,
        tables: []const antfly.metadata.table_manager.TableRecord,
        ranges: []const antfly.metadata.table_manager.RangeRecord,
    ) !void {
        const stores = [_]antfly.metadata.table_manager.StoreRecord{.{
            .store_id = store_id,
            .node_id = local_node_id,
            .runtime_statuses = runtime_statuses,
        }};
        const local_progress = try antfly.metadata.table_provisioner.collectLocalSchemaProgressFromRuntime(
            self.alloc,
            local_node_id,
            tables,
            ranges,
            stores[0..],
        );
        defer self.alloc.free(local_progress);

        for (local_progress) |record| {
            try remote_metadata.upsertSchemaProgress(record);
        }
    }

    pub fn initFromMetadataApiUrl(
        alloc: std.mem.Allocator,
        cfg: DataServerConfig,
        metadata_api_url: []const u8,
    ) !DataServer {
        const urls = [_][]const u8{metadata_api_url};
        return try initFromMetadataApiUrls(alloc, cfg, &urls);
    }

    pub fn initFromMetadataApiUrls(
        alloc: std.mem.Allocator,
        cfg: DataServerConfig,
        metadata_api_urls: []const []const u8,
    ) !DataServer {
        const remote_metadata = try alloc.create(RemoteMetadataSource);
        errdefer alloc.destroy(remote_metadata);
        remote_metadata.* = try RemoteMetadataSource.init(alloc, metadata_api_urls);
        errdefer remote_metadata.deinit();

        var data_raft_store: ?*raft_engine.core.MemoryStorage = null;
        errdefer if (data_raft_store) |store| {
            store.deinit();
            alloc.destroy(store);
        };
        var data_raft_factory: ?*DataDescriptorFactory = null;
        errdefer if (data_raft_factory) |factory| {
            factory.deinit();
            alloc.destroy(factory);
        };
        var data_raft_apply: ?*RaftTableApplyStateMachine = null;
        errdefer if (data_raft_apply) |apply_sm| {
            apply_sm.deinit();
            alloc.destroy(apply_sm);
        };
        var data_raft: ?*antfly.raft.ManagedHttpHostService = null;
        errdefer if (data_raft) |raft| {
            raft.deinit();
            alloc.destroy(raft);
        };

        if (cfg.enable_data_raft) {
            if (cfg.store_registration) |registration| {
                data_raft_store = try alloc.create(raft_engine.core.MemoryStorage);
                data_raft_store.?.* = raft_engine.core.MemoryStorage.init(alloc);

                data_raft_factory = try alloc.create(DataDescriptorFactory);
                data_raft_factory.?.* = DataDescriptorFactory.init(alloc, data_raft_store.?);

                data_raft_apply = try alloc.create(RaftTableApplyStateMachine);
                data_raft_apply.?.* = RaftTableApplyStateMachine.init(
                    alloc,
                    cfg.replica_root_dir,
                    remote_metadata.catalogSource(),
                    cfg.backend_runtime,
                );

                data_raft = try alloc.create(antfly.raft.ManagedHttpHostService);
                data_raft.?.* = try antfly.raft.ManagedHttpHostService.init(alloc, .{
                    .http = .{
                        .host = .{
                            .local_node_id = registration.node_id,
                            .replica_root_dir = cfg.replica_root_dir,
                            .replica_catalog_path = cfg.replica_catalog_path,
                            .replica_state_backend = cfg.data_raft_state_backend,
                        },
                        .listener = .{
                            .bind_host = cfg.raft_bind_host,
                            .bind_port = cfg.raft_bind_port,
                        },
                        .transport = .{
                            .snapshot = .{
                                .root_dir = cfg.snapshot_root_dir orelse cfg.replica_root_dir,
                            },
                        },
                    },
                }, .{
                    .http = .{
                        .host = .{
                            .descriptor_factory = data_raft_factory.?.iface(),
                            .runtime_hooks = .{
                                .state_machine = data_raft_apply.?.stateMachine(),
                            },
                        },
                    },
                }, .{}, .{
                    .transition_runtime = null,
                });
            }
        }

        return .{
            .alloc = alloc,
            .remote_metadata = remote_metadata,
            .data_raft = data_raft,
            .data_raft_factory = data_raft_factory,
            .data_raft_store = data_raft_store,
            .data_raft_apply = data_raft_apply,
            .store_registration = cfg.store_registration,
            .group_leadership_source = cfg.group_leadership_source orelse if (data_raft) |raft| GroupLeadershipSource.fromManagedHttpHostService(raft) else null,
            .group_membership_source = cfg.group_membership_source orelse if (data_raft) |raft| GroupMembershipSource.fromManagedHttpHostService(raft) else null,
            .local_transition_runtime = if (data_raft) |raft| raft.local_transition_runtime else null,
            .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
            .read_source = antfly.public_api.ProvisionedTableReadSource.init(
                cfg.replica_root_dir,
                remote_metadata.catalogSource(),
                antfly.raft.read_gate.noopReadableLeaseRequester(),
            ),
            .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
                cfg.replica_root_dir,
                remote_metadata.catalogSource(),
            ),
            .status_source = remote_metadata.statusSource(),
            .api_server_cfg = cfg.api_server_cfg,
            .query_async_limit = cfg.query_async_limit,
            .backend_runtime = cfg.backend_runtime,
            .listener_cfg = publicApiListenerConfig(cfg.bind_host, cfg.bind_port),
        };
    }
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

fn appendUniqueNodeId(alloc: std.mem.Allocator, list: *std.ArrayListUnmanaged(u64), node_id: u64) !void {
    for (list.items) |existing| {
        if (existing == node_id) return;
    }
    try list.append(alloc, node_id);
}

fn nodeIdInSlice(node_ids: []const u64, node_id: u64) bool {
    for (node_ids) |candidate| {
        if (candidate == node_id) return true;
    }
    return false;
}

fn collectPlacementPeerNodeIdsForGroup(
    alloc: std.mem.Allocator,
    placement_intents: []const antfly.raft.PlacementIntent,
    group_id: u64,
) ![]u64 {
    var peers = std.ArrayListUnmanaged(u64).empty;
    errdefer peers.deinit(alloc);
    for (placement_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        try appendUniqueNodeId(alloc, &peers, intent.record.local_node_id);
    }
    std.mem.sort(u64, peers.items, {}, comptime std.sort.asc(u64));
    return try peers.toOwnedSlice(alloc);
}

fn appendOwnedPeerRouteUpsert(
    alloc: std.mem.Allocator,
    updates: *std.ArrayListUnmanaged(antfly.raft.MetadataUpdate),
    group_id: u64,
    node_id: u64,
    raft_url: []const u8,
) !void {
    const endpoints = try alloc.alloc(antfly.raft.PeerEndpoint, 1);
    errdefer alloc.free(endpoints);
    const address = try alloc.dupe(u8, raft_url);
    errdefer alloc.free(address);
    const metadata = try alloc.dupe(u8, "");
    errdefer alloc.free(metadata);
    endpoints[0] = .{
        .protocol = .http,
        .address = address,
        .metadata = metadata,
    };
    try updates.append(alloc, .{
        .peer_route = .{
            .upsert = .{
                .group_id = group_id,
                .node_id = node_id,
                .endpoints = endpoints,
            },
        },
    });
}

const RemoteMetadataSource = struct {
    alloc: std.mem.Allocator,
    base_uris: [][]u8,
    preferred_base_uri_index: usize = 0,
    cache_mutex: std.atomic.Mutex = .unlocked,
    cached_head: ?antfly.metadata_api.MetadataHead = null,
    cached_head_at_ms: u64 = 0,
    cached_snapshot: ?antfly.metadata_api.AdminSnapshot = null,
    cached_snapshot_at_ms: u64 = 0,

    fn init(alloc: std.mem.Allocator, base_uris: []const []const u8) !RemoteMetadataSource {
        if (base_uris.len == 0) return error.MissingMetadataApi;
        var owned = try alloc.alloc([]u8, base_uris.len);
        var initialized: usize = 0;
        errdefer {
            for (owned[0..initialized]) |uri| alloc.free(uri);
            alloc.free(owned);
        }
        for (base_uris, 0..) |uri, i| {
            owned[i] = try alloc.dupe(u8, uri);
            initialized += 1;
        }
        return .{
            .alloc = alloc,
            .base_uris = owned,
        };
    }

    fn deinit(self: *RemoteMetadataSource) void {
        lockAtomic(&self.cache_mutex);
        if (self.cached_snapshot) |*snapshot| freeAdminSnapshotOwned(self.alloc, snapshot);
        for (self.base_uris) |uri| self.alloc.free(uri);
        self.alloc.free(self.base_uris);
        self.cache_mutex.unlock();
        self.* = undefined;
    }

    fn metadataApiIndexForAttempt(self: *RemoteMetadataSource, attempt: usize) usize {
        lockAtomic(&self.cache_mutex);
        const start = self.preferred_base_uri_index % self.base_uris.len;
        self.cache_mutex.unlock();
        return (start + attempt) % self.base_uris.len;
    }

    fn noteMetadataApiSuccess(self: *RemoteMetadataSource, index: usize) void {
        lockAtomic(&self.cache_mutex);
        self.preferred_base_uri_index = index;
        self.cache_mutex.unlock();
    }

    fn fetchHead(self: *RemoteMetadataSource) !antfly.metadata_api.MetadataHead {
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        lockAtomic(&self.cache_mutex);
        if (self.cached_head) |head| {
            if (now_ms -| self.cached_head_at_ms <= metadata_head_cache_ttl_ms) {
                defer self.cache_mutex.unlock();
                return head;
            }
        }
        self.cache_mutex.unlock();

        const head = try remoteHead(self);
        lockAtomic(&self.cache_mutex);
        defer self.cache_mutex.unlock();
        self.cached_head = head;
        self.cached_head_at_ms = now_ms;
        return head;
    }

    fn invalidateCache(self: *RemoteMetadataSource) void {
        lockAtomic(&self.cache_mutex);
        defer self.cache_mutex.unlock();
        if (self.cached_snapshot) |*snapshot| freeAdminSnapshotOwned(self.alloc, snapshot);
        self.cached_snapshot = null;
        self.cached_head = null;
        self.cached_head_at_ms = 0;
        self.cached_snapshot_at_ms = 0;
    }

    fn fetchSnapshot(self: *RemoteMetadataSource) !antfly.metadata_api.AdminSnapshot {
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        lockAtomic(&self.cache_mutex);
        if (self.cached_snapshot) |snapshot| {
            if (now_ms -| self.cached_snapshot_at_ms <= metadata_snapshot_cache_ttl_ms) {
                defer self.cache_mutex.unlock();
                return try cloneAdminSnapshotOwned(self.alloc, snapshot);
            }
        }
        self.cache_mutex.unlock();

        const head = try self.fetchHead();
        return try self.fetchSnapshotForHead(head);
    }

    fn cachedSnapshot(self: *RemoteMetadataSource) !?antfly.metadata_api.AdminSnapshot {
        lockAtomic(&self.cache_mutex);
        defer self.cache_mutex.unlock();
        if (self.cached_snapshot) |snapshot| {
            return try cloneAdminSnapshotOwned(self.alloc, snapshot);
        }
        return null;
    }

    fn fetchSnapshotForHead(self: *RemoteMetadataSource, head: antfly.metadata_api.MetadataHead) !antfly.metadata_api.AdminSnapshot {
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        lockAtomic(&self.cache_mutex);
        if (self.cached_head) |cached_head| {
            if (self.cached_snapshot) |snapshot| {
                if (cached_head.metadata_group_id == head.metadata_group_id and
                    cached_head.metadata_epoch == head.metadata_epoch and
                    now_ms -| self.cached_snapshot_at_ms <= metadata_snapshot_cache_ttl_ms)
                {
                    defer self.cache_mutex.unlock();
                    return try cloneAdminSnapshotOwned(self.alloc, snapshot);
                }
            }
        }
        self.cache_mutex.unlock();

        var fresh = try self.fetchSnapshotRemote();
        errdefer freeAdminSnapshotOwned(self.alloc, &fresh);

        lockAtomic(&self.cache_mutex);
        defer self.cache_mutex.unlock();
        if (self.cached_head) |cached_head| {
            if (self.cached_snapshot) |snapshot| {
                if (cached_head.metadata_group_id == head.metadata_group_id and
                    cached_head.metadata_epoch == head.metadata_epoch and
                    now_ms -| self.cached_snapshot_at_ms <= metadata_snapshot_cache_ttl_ms)
                {
                    return try cloneAdminSnapshotOwned(self.alloc, snapshot);
                }
            }
        }
        if (self.cached_snapshot) |*snapshot| freeAdminSnapshotOwned(self.alloc, snapshot);
        self.cached_snapshot = fresh;
        self.cached_head = head;
        self.cached_head_at_ms = now_ms;
        self.cached_snapshot_at_ms = now_ms;
        return try cloneAdminSnapshotOwned(self.alloc, self.cached_snapshot.?);
    }

    fn catalogSource(self: *RemoteMetadataSource) antfly.public_api.table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = remoteAdminSnapshot,
                .free_admin_snapshot = remoteFreeAdminSnapshot,
            },
        };
    }

    fn statusSource(self: *RemoteMetadataSource) antfly.public_api.http_server.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = remoteStatus,
                .admin_snapshot = remoteAdminSnapshot,
                .cached_admin_snapshot = remoteCachedAdminSnapshot,
                .free_admin_snapshot = remoteFreeAdminSnapshot,
                .create_table = remoteCreateTable,
                .restore_table = remoteRestoreTable,
                .drop_table = remoteDropTable,
                .update_schema = remoteUpdateSchema,
                .create_index = remoteCreateIndex,
                .drop_index = remoteDropIndex,
                .wait_table_lifecycle = remoteWaitTableLifecycle,
                .wait_table_projection = remoteWaitTableProjection,
            },
        };
    }

    fn withMetadataApiClient(
        self: *RemoteMetadataSource,
        comptime T: type,
        comptime callFn: anytype,
        ctx: anytype,
    ) !T {
        var last_err: anyerror = error.MissingMetadataApi;
        for (0..self.base_uris.len) |attempt| {
            const index = self.metadataApiIndexForAttempt(attempt);
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const scratch = arena.allocator();
            var executor = antfly.raft.transport.std_http_executor.StdHttpExecutor.init(scratch, .{});
            defer executor.deinit();
            var metadata_client = antfly.metadata_http_client.MetadataHttpClient.init(scratch, executor.executor());
            const result = callFn(self, &metadata_client, self.base_uris[index], ctx) catch |err| {
                last_err = err;
                continue;
            };
            self.noteMetadataApiSuccess(index);
            return result;
        }
        return last_err;
    }

    fn remoteHead(ptr: *anyopaque) !antfly.metadata_api.MetadataHead {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        var last_err: anyerror = error.MissingMetadataApi;
        for (0..self.base_uris.len) |attempt| {
            const index = self.metadataApiIndexForAttempt(attempt);
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const scratch = arena.allocator();
            var executor = antfly.raft.transport.std_http_executor.StdHttpExecutor.init(scratch, .{});
            defer executor.deinit();
            var metadata_client = antfly.metadata_http_client.MetadataHttpClient.init(scratch, executor.executor());
            const head = metadata_client.fetchHead(self.base_uris[index]) catch |err| {
                last_err = err;
                continue;
            };
            self.noteMetadataApiSuccess(index);
            return head;
        }
        return last_err;
    }

    fn remoteStatus(ptr: *anyopaque) !antfly.metadata_api.MetadataStatus {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        var last_err: anyerror = error.MissingMetadataApi;
        for (0..self.base_uris.len) |attempt| {
            const index = self.metadataApiIndexForAttempt(attempt);
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const scratch = arena.allocator();
            var executor = antfly.raft.transport.std_http_executor.StdHttpExecutor.init(scratch, .{});
            defer executor.deinit();
            var metadata_client = antfly.metadata_http_client.MetadataHttpClient.init(scratch, executor.executor());
            const status = metadata_client.fetchStatus(self.base_uris[index]) catch |err| {
                last_err = err;
                continue;
            };
            self.noteMetadataApiSuccess(index);
            return status;
        }
        return last_err;
    }

    fn fetchSnapshotRemote(self: *RemoteMetadataSource) !antfly.metadata_api.AdminSnapshot {
        var last_err: anyerror = error.MissingMetadataApi;
        for (0..self.base_uris.len) |attempt| {
            const index = self.metadataApiIndexForAttempt(attempt);
            var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
            defer arena.deinit();
            const scratch = arena.allocator();
            var executor = antfly.raft.transport.std_http_executor.StdHttpExecutor.init(scratch, .{});
            defer executor.deinit();
            var metadata_client = antfly.metadata_http_client.MetadataHttpClient.init(scratch, executor.executor());
            var parsed = metadata_client.fetchSnapshot(self.base_uris[index]) catch |err| {
                last_err = err;
                continue;
            };
            defer parsed.deinit();
            self.noteMetadataApiSuccess(index);
            return try cloneAdminSnapshotOwned(self.alloc, parsed.value);
        }
        return last_err;
    }

    fn remoteAdminSnapshot(ptr: *anyopaque) !antfly.metadata_api.AdminSnapshot {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        return try self.fetchSnapshot();
    }

    fn remoteCachedAdminSnapshot(ptr: *anyopaque) !?antfly.metadata_api.AdminSnapshot {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        return try self.cachedSnapshot();
    }

    fn remoteFreeAdminSnapshot(ptr: *anyopaque, snapshot: *antfly.metadata_api.AdminSnapshot) void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        freeAdminSnapshotOwned(self.alloc, snapshot);
    }

    fn remoteCreateTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: antfly.public_api.tables.CreateTableRequest) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const body = try antfly.public_api.table_contract.encodeCreateTableRequest(alloc, req);
        defer alloc.free(body);
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.createTable(base_uri, ctx.table_name, ctx.body);
            }
        }.call, .{ .table_name = table_name, .body = body });
        self.invalidateCache();
    }

    fn remoteDropTable(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: []const u8) !void {
                try client.dropTable(base_uri, ctx);
            }
        }.call, table_name);
        self.invalidateCache();
    }

    fn remoteRestoreTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, location_uri: []const u8, backup_id: []const u8) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const body = try std.fmt.allocPrint(alloc, "{{\"backup_id\":\"{s}\",\"location\":\"{s}\"}}", .{ backup_id, location_uri });
        defer alloc.free(body);
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.restoreTable(base_uri, ctx.table_name, ctx.body);
            }
        }.call, .{ .table_name = table_name, .body = body });
        self.invalidateCache();
    }

    fn remoteUpdateSchema(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.updateSchema(base_uri, ctx.table_name, ctx.schema_json);
            }
        }.call, .{ .table_name = table_name, .schema_json = schema_json });
        self.invalidateCache();
    }

    fn remoteCreateIndex(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.createIndex(base_uri, ctx.table_name, ctx.index_name, ctx.index_json);
            }
        }.call, .{ .table_name = table_name, .index_name = index_name, .index_json = index_json });
        self.invalidateCache();
    }

    fn remoteDropIndex(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.dropIndex(base_uri, ctx.table_name, ctx.index_name);
            }
        }.call, .{ .table_name = table_name, .index_name = index_name });
        self.invalidateCache();
    }

    fn remoteWaitTableLifecycle(
        ptr: *anyopaque,
        table_name: []const u8,
        expected: antfly.public_api.http_server.TableVisibility,
    ) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const timeout_ns = 30 * std.time.ns_per_s;
        const poll_interval_ms: u64 = 10;
        const start_ns = platform_time.monotonicNs();

        while (true) {
            var snapshot = try self.fetchSnapshot();

            if (remoteTableLifecycleMatches(&snapshot, table_name, expected)) {
                freeAdminSnapshotOwned(self.alloc, &snapshot);
                return;
            }
            freeAdminSnapshotOwned(self.alloc, &snapshot);
            if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
            platform_clock.Clock.real().sleepMs(poll_interval_ms);
        }
    }

    fn remoteWaitTableProjection(
        ptr: *anyopaque,
        table_name: []const u8,
        schema_json: ?[]const u8,
        indexes_json: ?[]const u8,
    ) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const timeout_ns = 30 * std.time.ns_per_s;
        const poll_interval_ms: u64 = 10;
        const start_ns = platform_time.monotonicNs();

        while (true) {
            var snapshot = try self.fetchSnapshot();

            if (remoteTableProjectionMatches(&snapshot, table_name, schema_json, indexes_json)) {
                freeAdminSnapshotOwned(self.alloc, &snapshot);
                return;
            }
            freeAdminSnapshotOwned(self.alloc, &snapshot);
            if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
            platform_clock.Clock.real().sleepMs(poll_interval_ms);
        }
    }

    fn registerNode(self: *RemoteMetadataSource, record: antfly.metadata.table_manager.StoreRecord) !void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const scratch = arena.allocator();
        const body = try stringifyJsonAlloc(scratch, record);
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: []const u8) !void {
                try client.upsertNode(base_uri, ctx);
            }
        }.call, body);
    }

    fn reportNodeStatus(self: *RemoteMetadataSource, report: antfly.metadata.table_manager.StoreStatusReport) !void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const scratch = arena.allocator();
        const body = try stringifyJsonAlloc(scratch, report);
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: []const u8) !void {
                try client.reportNodeStatus(base_uri, ctx);
            }
        }.call, body);
    }

    fn upsertSchemaProgress(self: *RemoteMetadataSource, record: antfly.metadata.table_manager.SchemaProgressRecord) !void {
        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const scratch = arena.allocator();
        const body = try stringifyJsonAlloc(scratch, record);
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: []const u8) !void {
                try client.upsertSchemaProgress(base_uri, ctx);
            }
        }.call, body);
    }
};

fn remoteTableLifecycleMatches(
    snapshot: *const antfly.metadata_api.AdminSnapshot,
    table_name: []const u8,
    expected: antfly.public_api.http_server.TableVisibility,
) bool {
    const table = remoteFindProjectedTableByName(snapshot.tables, table_name);
    return switch (expected) {
        .present => {
            const record = table orelse return false;
            return remoteTableRangesReady(snapshot, record.table_id);
        },
        .absent => table == null,
    };
}

fn remoteTableProjectionMatches(
    snapshot: *const antfly.metadata_api.AdminSnapshot,
    table_name: []const u8,
    schema_json: ?[]const u8,
    indexes_json: ?[]const u8,
) bool {
    const record = remoteFindProjectedTableByName(snapshot.tables, table_name) orelse return false;
    if (schema_json) |expected_schema_json| {
        if (!remoteJsonDocumentsEqual(record.schema_json, expected_schema_json)) return false;
    }
    if (indexes_json) |expected_indexes_json| {
        if (!remoteIndexesJsonEqual(record.indexes_json, expected_indexes_json)) return false;
    }
    return true;
}

fn remoteJsonDocumentsEqual(lhs_json: []const u8, rhs_json: []const u8) bool {
    if (std.mem.eql(u8, lhs_json, rhs_json)) return true;

    var lhs = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, lhs_json, .{}) catch return false;
    defer lhs.deinit();
    var rhs = std.json.parseFromSlice(std.json.Value, std.heap.page_allocator, rhs_json, .{}) catch return false;
    defer rhs.deinit();
    return json_helpers.jsonValuesEqual(lhs.value, rhs.value);
}

fn remoteIndexesJsonEqual(lhs_json: []const u8, rhs_json: []const u8) bool {
    if (std.mem.eql(u8, lhs_json, rhs_json)) return true;
    return indexes_api.equivalentIndexConfigJson(std.heap.page_allocator, lhs_json, rhs_json) catch false;
}

fn remoteFindProjectedTableByName(
    tables: []const antfly.metadata.table_manager.TableRecord,
    table_name: []const u8,
) ?antfly.metadata.table_manager.TableRecord {
    for (tables) |table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn remoteTableRangesReady(
    snapshot: *const antfly.metadata_api.AdminSnapshot,
    table_id: u64,
) bool {
    var range_count: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        range_count += 1;
        if (!remoteGroupReadyForTableLifecycle(snapshot, range.group_id)) return false;
    }
    return range_count > 0;
}

fn remoteGroupReadyForTableLifecycle(
    snapshot: *const antfly.metadata_api.AdminSnapshot,
    group_id: u64,
) bool {
    const status = blk: {
        for (snapshot.merged_group_statuses) |candidate| {
            if (candidate.group_id == group_id) break :blk candidate;
        }
        return false;
    };

    if (status.updated_at_millis == 0) return false;
    if (!status.leader_known) return false;
    if (status.joint_consensus) return false;
    if (status.transition_pending) return false;
    if (status.replay_required and !status.replay_caught_up) return false;

    var expected: u16 = 0;
    for (snapshot.placement_intents) |intent| {
        if (intent.record.group_id == group_id) expected +|= 1;
    }

    if (status.voter_count_known) {
        if (expected > 0 and status.voter_count != expected) return false;
        return status.healthy_voter_reports >= status.voter_count;
    }

    if (expected == 0) return true;

    var healthy_reporting: usize = 0;
    for (snapshot.stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        for (store.group_statuses) |group_status| {
            if (group_status.group_id != group_id) continue;
            healthy_reporting += 1;
            break;
        }
    }
    return healthy_reporting >= expected;
}

fn cloneAdminSnapshotOwned(alloc: std.mem.Allocator, snapshot: antfly.metadata_api.AdminSnapshot) !antfly.metadata_api.AdminSnapshot {
    return .{
        .status = snapshot.status,
        .tables = try cloneTablesOwned(alloc, snapshot.tables),
        .ranges = try cloneRangesOwned(alloc, snapshot.ranges),
        .stores = try cloneStoresOwned(alloc, snapshot.stores),
        .placement_intents = try clonePlacementIntentsOwned(alloc, snapshot.placement_intents),
        .shuffle_join_leases = try cloneShuffleJoinLeasesOwned(alloc, snapshot.shuffle_join_leases),
        .local_bootstrap_statuses = try cloneLocalBootstrapStatusesOwned(alloc, snapshot.local_bootstrap_statuses),
        .restore_progresses = try cloneRestoreProgressesOwned(alloc, snapshot.restore_progresses),
        .replication_source_statuses = try cloneReplicationSourceStatusesOwned(alloc, snapshot.replication_source_statuses),
        .replication_source_action_hints = try cloneReplicationSourceActionHintsOwned(alloc, snapshot.replication_source_action_hints),
        .split_transitions = try cloneSplitTransitionsOwned(alloc, snapshot.split_transitions),
        .merge_transitions = try cloneMergeTransitionsOwned(alloc, snapshot.merge_transitions),
        .split_observations = try cloneSplitObservationsOwned(alloc, snapshot.split_observations),
        .merge_observations = try cloneMergeObservationsOwned(alloc, snapshot.merge_observations),
        .merged_group_statuses = try cloneMergedGroupStatusesOwned(alloc, snapshot.merged_group_statuses),
    };
}

fn localGroupStatusFingerprint(
    group_ids: []const u64,
    tables: []const antfly.metadata.table_manager.TableRecord,
    ranges: []const antfly.metadata.table_manager.RangeRecord,
    stores: []const antfly.metadata.table_manager.StoreRecord,
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    split_transitions: []const antfly.metadata.SplitTransitionRecord,
    merge_transitions: []const antfly.metadata.MergeTransitionRecord,
    split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
    merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
) u64 {
    var hasher = std.hash.Wyhash.init(0x9f65e0ea4af129ad);
    for (group_ids) |group_id| {
        hasher.update(std.mem.asBytes(&group_id));
        const local_leader = if (group_leadership_source) |source| source.isLocalLeader(group_id) else false;
        hasher.update(std.mem.asBytes(&local_leader));
        const membership = if (group_membership_source) |source| source.membership(group_id) else GroupMembership{};
        hasher.update(std.mem.asBytes(&membership.local_voter));
        hasher.update(std.mem.asBytes(&membership.voter_count));
        hasher.update(std.mem.asBytes(&membership.joint_consensus));
    }
    for (tables) |table| hasher.update(std.mem.asBytes(&table.table_id));
    for (ranges) |range| {
        hasher.update(std.mem.asBytes(&range.group_id));
        hasher.update(std.mem.asBytes(&range.table_id));
    }
    for (stores) |store| {
        hasher.update(std.mem.asBytes(&store.store_id));
        hasher.update(std.mem.asBytes(&store.node_id));
        hasher.update(std.mem.asBytes(&store.live));
    }
    for (merged_group_statuses) |status| {
        hasher.update(std.mem.asBytes(&status.group_id));
        hasher.update(std.mem.asBytes(&status.transition_pending));
        hasher.update(std.mem.asBytes(&status.replay_required));
        hasher.update(std.mem.asBytes(&status.replay_caught_up));
        hasher.update(std.mem.asBytes(&status.cutover_ready));
        hasher.update(std.mem.asBytes(&status.reads_ready_after_cutover));
    }
    for (split_transitions) |record| {
        hasher.update(std.mem.asBytes(&record.transition_id));
        hasher.update(std.mem.asBytes(&record.source_group_id));
        hasher.update(std.mem.asBytes(&record.destination_group_id));
        const phase: u8 = @intFromEnum(record.phase);
        hasher.update(&.{phase});
    }
    for (merge_transitions) |record| {
        hasher.update(std.mem.asBytes(&record.transition_id));
        hasher.update(std.mem.asBytes(&record.donor_group_id));
        hasher.update(std.mem.asBytes(&record.receiver_group_id));
        const phase: u8 = @intFromEnum(record.phase);
        hasher.update(&.{phase});
    }
    for (split_observations) |record| {
        hasher.update(std.mem.asBytes(&record.transition_id));
        const phase: u8 = @intFromEnum(record.observation.status.phase);
        hasher.update(&.{phase});
        hasher.update(std.mem.asBytes(&record.observation.status.bootstrapped));
        hasher.update(std.mem.asBytes(&record.observation.status.replay_required));
        hasher.update(std.mem.asBytes(&record.observation.status.replay_caught_up));
        hasher.update(std.mem.asBytes(&record.observation.status.cutover_ready));
        hasher.update(std.mem.asBytes(&record.observation.status.destination_ready_for_reads));
        hasher.update(std.mem.asBytes(&record.observation.source_local_leader));
        hasher.update(std.mem.asBytes(&record.observation.destination_local_leader));
    }
    for (merge_observations) |record| {
        hasher.update(std.mem.asBytes(&record.transition_id));
        const donor_phase: u8 = @intFromEnum(record.observation.donor.phase);
        const receiver_phase: u8 = @intFromEnum(record.observation.receiver.phase);
        hasher.update(&.{ donor_phase, receiver_phase });
        hasher.update(std.mem.asBytes(&record.observation.donor.replay_required));
        hasher.update(std.mem.asBytes(&record.observation.donor.replay_caught_up));
        hasher.update(std.mem.asBytes(&record.observation.donor.cutover_ready));
        hasher.update(std.mem.asBytes(&record.observation.receiver.replay_required));
        hasher.update(std.mem.asBytes(&record.observation.receiver.replay_caught_up));
        hasher.update(std.mem.asBytes(&record.observation.receiver.cutover_ready));
        hasher.update(std.mem.asBytes(&record.observation.donor_local_leader));
        hasher.update(std.mem.asBytes(&record.observation.receiver_local_leader));
    }
    return hasher.final();
}

fn hashGroupStatus(hasher: *std.hash.Wyhash, status: antfly.metadata.table_manager.GroupStatusReport) void {
    hasher.update(std.mem.asBytes(&status.group_id));
    hasher.update(std.mem.asBytes(&status.doc_count));
    hasher.update(std.mem.asBytes(&status.disk_bytes));
    hasher.update(std.mem.asBytes(&status.empty));
    hasher.update(std.mem.asBytes(&status.local_leader));
    hasher.update(std.mem.asBytes(&status.local_voter));
    hasher.update(std.mem.asBytes(&status.voter_count));
    hasher.update(std.mem.asBytes(&status.joint_consensus));
    hasher.update(std.mem.asBytes(&status.transition_pending));
    hasher.update(std.mem.asBytes(&status.replay_required));
    hasher.update(std.mem.asBytes(&status.replay_caught_up));
    hasher.update(std.mem.asBytes(&status.cutover_ready));
    hasher.update(std.mem.asBytes(&status.reads_ready_after_cutover));
}

fn runtimeStatusReportFromLocalStatus(
    alloc: std.mem.Allocator,
    table: antfly.metadata.table_manager.TableRecord,
    group_id: u64,
    registration: StoreRegistrationConfig,
    status: runtime_status.LocalTableRuntimeStatus,
) !antfly.metadata.table_manager.RuntimeGroupStatusReport {
    const indexes = try alloc.alloc(antfly.metadata.table_manager.RuntimeIndexStatusReport, status.stats.indexes.len);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |record| antfly.metadata.table_manager.freeRuntimeIndexStatusReport(alloc, record);
        if (indexes.len > 0) alloc.free(indexes);
    }
    for (status.stats.indexes, 0..) |index, i| {
        indexes[i] = try runtimeIndexStatusReportFromLocalIndex(alloc, index);
        initialized += 1;
    }
    const table_name = try alloc.dupe(u8, table.name);
    errdefer alloc.free(table_name);
    const source = try alloc.dupe(u8, @tagName(status.metadata.source));
    errdefer alloc.free(source);
    const freshness = try alloc.dupe(u8, @tagName(status.metadata.freshness));
    errdefer alloc.free(freshness);
    return .{
        .table_id = table.table_id,
        .table_name = table_name,
        .group_id = group_id,
        .store_id = registration.store_id,
        .node_id = registration.node_id,
        .updated_at_ns = platform_time.monotonicNs(),
        .source = source,
        .freshness = freshness,
        .topology_generation = status.metadata.topology_generation,
        .lsm_root_generation = status.metadata.lsm_root_generation,
        .status_generation = status.metadata.status_generation,
        .doc_count = status.stats.doc_count,
        .disk_bytes = status.disk_bytes,
        .created_at_millis = status.created_at_millis,
        .index_count = status.stats.index_count,
        .enrichment_enabled = status.stats.enrichment.enabled,
        .enrichment_target_sequence = status.stats.enrichment.target_sequence,
        .enrichment_applied_sequence = status.stats.enrichment.applied_sequence,
        .enrichment_retrying = status.stats.enrichment.retrying,
        .enrichment_worker_failed = status.stats.enrichment.worker_failed,
        .async_indexing_active = status.stats.async_indexing.startup.active or
            status.stats.async_indexing.dense_catch_up.active or
            status.stats.async_indexing.bulk_coalescing.active_session,
        .async_startup_active = status.stats.async_indexing.startup.active,
        .async_dense_catch_up_active = status.stats.async_indexing.dense_catch_up.active,
        .async_bulk_coalescing_active = status.stats.async_indexing.bulk_coalescing.active_session,
        .doc_identity = runtimeDocIdentityStatusReportFromStats(status.stats.doc_identity),
        .doc_set_planning = runtimeDocSetPlanningStatusReportFromStats(status.stats.doc_set_planning),
        .indexes = indexes,
    };
}

fn runtimeDocIdentityStatusReportFromStats(
    stats: antfly.db.types.DocIdentityStats,
) antfly.metadata.table_manager.RuntimeDocIdentityStatusReport {
    return .{
        .namespace_table_id = stats.namespace_table_id,
        .namespace_shard_id = stats.namespace_shard_id,
        .namespace_range_id = stats.namespace_range_id,
        .next_ordinal = stats.next_ordinal,
        .allocated_ordinals = stats.allocated_ordinals,
        .ordinal_capacity_remaining = stats.ordinal_capacity_remaining,
        .ordinal_capacity_exhausted = stats.ordinal_capacity_exhausted,
        .rebuild_required = stats.rebuild_required,
        .state_rows = stats.state_rows,
        .live_ordinals = stats.live_ordinals,
        .tombstone_ordinals = stats.tombstone_ordinals,
        .min_created_generation = stats.min_created_generation,
        .max_created_generation = stats.max_created_generation,
        .min_deleted_generation = stats.min_deleted_generation,
        .max_deleted_generation = stats.max_deleted_generation,
        .scanned_primary_docs = stats.scanned_primary_docs,
        .primary_docs_missing_ordinals = stats.primary_docs_missing_ordinals,
        .primary_docs_missing_identity_state = stats.primary_docs_missing_identity_state,
        .primary_docs_with_tombstone_ordinals = stats.primary_docs_with_tombstone_ordinals,
        .complete = stats.complete,
    };
}

fn runtimeDocSetPlanningStatusReportFromStats(
    stats: antfly.db.types.DocSetPlanningStats,
) antfly.metadata.table_manager.RuntimeDocSetPlanningStatusReport {
    return .{
        .resolved_set_count = stats.resolved_set_count,
        .all_set_count = stats.all_set_count,
        .none_set_count = stats.none_set_count,
        .doc_key_list_count = stats.doc_key_list_count,
        .ordinal_list_count = stats.ordinal_list_count,
        .ordinal_bitmap_count = stats.ordinal_bitmap_count,
        .doc_key_list_docs = stats.doc_key_list_docs,
        .ordinal_list_docs = stats.ordinal_list_docs,
        .ordinal_bitmap_docs = stats.ordinal_bitmap_docs,
        .missing_ordinal_coverage_count = stats.missing_ordinal_coverage_count,
        .bitmap_promotion_count = stats.bitmap_promotion_count,
        .unsupported_filter_shape_count = stats.unsupported_filter_shape_count,
        .stale_identity_generation_rejection_count = stats.stale_identity_generation_rejection_count,
    };
}

fn runtimeIndexStatusReportFromLocalIndex(
    alloc: std.mem.Allocator,
    index: antfly.db.types.DBIndexStats,
) !antfly.metadata.table_manager.RuntimeIndexStatusReport {
    const name = try alloc.dupe(u8, index.name);
    errdefer alloc.free(name);
    const kind = try alloc.dupe(u8, @tagName(index.kind));
    errdefer alloc.free(kind);
    return .{
        .name = name,
        .kind = kind,
        .doc_count = index.doc_count,
        .term_count = index.term_count,
        .edge_count = index.edge_count,
        .node_count = index.node_count,
        .root_node = index.root_node,
        .backfill_active = index.backfill_active,
        .backfill_progress_millis = progressMillis(index.backfill_progress),
        .replay_applied_sequence = index.replay_applied_sequence,
        .replay_target_sequence = index.replay_target_sequence,
        .replay_catch_up_required = index.replay_catch_up_required,
    };
}

fn progressMillis(progress: f64) u16 {
    const clamped = std.math.clamp(progress, 0.0, 1.0);
    return @intFromFloat(clamped * 1000.0);
}

fn collectLocalGroupIds(
    alloc: std.mem.Allocator,
    intents: []antfly.raft.reconciler.PlacementIntent,
    local_node_id: u64,
) ![]u64 {
    var out = std.ArrayListUnmanaged(u64).empty;
    errdefer out.deinit(alloc);

    for (intents) |intent| {
        if (intent.record.local_node_id != local_node_id) continue;
        if (containsU64(out.items, intent.record.group_id)) continue;
        try out.append(alloc, intent.record.group_id);
    }

    std.mem.sort(u64, out.items, {}, std.sort.asc(u64));
    return try out.toOwnedSlice(alloc);
}

fn containsU64(values: []const u64, needle: u64) bool {
    for (values) |value| {
        if (value == needle) return true;
    }
    return false;
}

fn collectAllRangeGroupIds(
    alloc: std.mem.Allocator,
    ranges: []const antfly.metadata.table_manager.RangeRecord,
) ![]u64 {
    var out = std.ArrayListUnmanaged(u64).empty;
    errdefer out.deinit(alloc);

    for (ranges) |range| {
        if (containsU64(out.items, range.group_id)) continue;
        try out.append(alloc, range.group_id);
    }

    std.mem.sort(u64, out.items, {}, std.sort.asc(u64));
    return try out.toOwnedSlice(alloc);
}

fn hasSingleRoleStore(
    stores: []const antfly.metadata.table_manager.StoreRecord,
    role: []const u8,
    store_id: u64,
) bool {
    var matching_store_id: ?u64 = null;
    for (stores) |store| {
        if (!std.mem.eql(u8, store.role, role)) continue;
        if (matching_store_id == null) {
            matching_store_id = store.store_id;
            continue;
        }
        if (matching_store_id.? != store.store_id) return false;
    }
    return matching_store_id != null and matching_store_id.? == store_id;
}

fn findRangeByGroupId(
    ranges: []const antfly.metadata.table_manager.RangeRecord,
    group_id: u64,
) ?antfly.metadata.table_manager.RangeRecord {
    for (ranges) |range| {
        if (range.group_id == group_id) return range;
    }
    return null;
}

fn identityNamespaceFromRange(range: antfly.metadata.table_manager.RangeRecord) antfly.db.DocIdentityNamespace {
    return .{
        .table_id = range.table_id,
        .shard_id = antfly.metadata.table_manager.rangeDocIdentityShardId(range),
        .range_id = antfly.metadata.table_manager.rangeDocIdentityRangeId(range),
    };
}

fn findTableById(
    tables: []const antfly.metadata.table_manager.TableRecord,
    table_id: u64,
) ?antfly.metadata.table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn collectLocalGroupStatuses(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_ids: []const u64,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
    snapshot_stores: []const antfly.metadata.StoreRecord,
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    split_transitions: []const antfly.metadata.SplitTransitionRecord,
    merge_transitions: []const antfly.metadata.MergeTransitionRecord,
    split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
    merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
) ![]antfly.metadata.table_manager.GroupStatusReport {
    var reports = std.ArrayListUnmanaged(antfly.metadata.table_manager.GroupStatusReport).empty;
    errdefer {
        for (reports.items) |record| antfly.metadata.table_manager.freeGroupStatus(alloc, record);
        reports.deinit(alloc);
    }

    for (group_ids) |group_id| {
        const db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(db_path);

        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        _ = statFilePath(io_impl.io(), db_path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };

        try reports.append(alloc, try collectLocalGroupStatus(
            alloc,
            db_path,
            replica_root_dir,
            group_id,
            .{},
            group_leadership_source,
            group_membership_source,
            snapshot_stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        ));
    }

    return try reports.toOwnedSlice(alloc);
}

fn statFilePath(io: anytype, path: []const u8) !std.Io.Dir.Stat {
    if (std.fs.path.isAbsolute(path)) {
        var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
        defer file.close(io);
        return try file.stat(io);
    }
    return try std.Io.Dir.cwd().statFile(io, path, .{});
}

const LocalGroupStatusOpenOptions = struct {
    lsm_cache: ?*lsm_backend_mod.Cache = null,
    lsm_root_generation: u64 = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
};

fn collectLocalGroupStatus(
    alloc: std.mem.Allocator,
    db_path: []const u8,
    replica_root_dir: ?[]const u8,
    group_id: u64,
    open_options: LocalGroupStatusOpenOptions,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
    snapshot_stores: []const antfly.metadata.StoreRecord,
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    split_transitions: []const antfly.metadata.SplitTransitionRecord,
    merge_transitions: []const antfly.metadata.MergeTransitionRecord,
    split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
    merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
) !antfly.metadata.table_manager.GroupStatusReport {
    var db = try antfly.db.DB.open(alloc, db_path, .{
        .open_mode = .status_only,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .transaction_recovery = .{ .enabled = false },
        .text_merge = .{ .enabled = false },
        .lsm_cache = open_options.lsm_cache,
        .lsm_root_generation = open_options.lsm_root_generation,
        .resource_manager = open_options.resource_manager,
        .backend_runtime = open_options.backend_runtime,
    });
    defer db.close();

    return try collectLocalGroupStatusFromDb(
        alloc,
        &db,
        db_path,
        replica_root_dir,
        group_id,
        group_leadership_source,
        group_membership_source,
        snapshot_stores,
        merged_group_statuses,
        split_transitions,
        merge_transitions,
        split_observations,
        merge_observations,
    );
}

fn collectLocalGroupStatusFromDb(
    alloc: std.mem.Allocator,
    db: *antfly.db.DB,
    db_path: []const u8,
    replica_root_dir: ?[]const u8,
    group_id: u64,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
    snapshot_stores: []const antfly.metadata.StoreRecord,
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    split_transitions: []const antfly.metadata.SplitTransitionRecord,
    merge_transitions: []const antfly.metadata.MergeTransitionRecord,
    split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
    merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
) !antfly.metadata.table_manager.GroupStatusReport {
    const stats = try db.stats(alloc);
    defer antfly.db.types.freeDBStats(alloc, stats);

    const now_realtime_ms = platform_clock.Clock.real().nowRealtimeMs();
    const created_at_millis = (try db.getGroupCreatedAtMillis(alloc, group_id)) orelse now_realtime_ms;
    const readiness = if (replica_root_dir) |root_dir| blk: {
        const local_readiness = try deriveLocalGroupReadiness(
            alloc,
            root_dir,
            group_id,
            snapshot_stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        );
        break :blk local_readiness;
    } else antfly.metadata.transition_state.readinessForGroup(group_id, split_transitions, merge_transitions);
    const membership = if (group_membership_source) |source| source.membership(group_id) else GroupMembership{};

    return .{
        .group_id = group_id,
        .doc_count = stats.doc_count,
        .disk_bytes = try directoryUsageBytes(alloc, db_path),
        .empty = stats.doc_count == 0,
        .created_at_millis = created_at_millis,
        .updated_at_millis = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms)),
        .local_leader = if (group_leadership_source) |source| source.isLocalLeader(group_id) else false,
        .local_voter = membership.local_voter,
        .voter_count = membership.voter_count,
        .joint_consensus = membership.joint_consensus,
        .transition_pending = readiness.transition_pending,
        .replay_required = readiness.replay_required,
        .replay_caught_up = readiness.replay_caught_up,
        .cutover_ready = readiness.cutover_ready,
        .reads_ready_after_cutover = readiness.reads_ready_after_cutover,
    };
}

fn collectRaftOnlyLocalGroupStatus(
    group_id: u64,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
) ?antfly.metadata.table_manager.GroupStatusReport {
    const membership = if (group_membership_source) |source| source.membership(group_id) else GroupMembership{};
    if (!membership.local_voter and membership.voter_count == 0) return null;
    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
    return .{
        .group_id = group_id,
        .doc_count = 0,
        .disk_bytes = 0,
        .empty = true,
        .created_at_millis = platform_clock.Clock.real().nowRealtimeMs(),
        .updated_at_millis = now_ms,
        .local_leader = if (group_leadership_source) |source| source.isLocalLeader(group_id) else false,
        .local_voter = membership.local_voter,
        .voter_count = membership.voter_count,
        .joint_consensus = membership.joint_consensus,
    };
}

fn collectRaftOnlyLocalGroupStatusFallbacks(
    alloc: std.mem.Allocator,
    group_ids: []const u64,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
) ![]antfly.metadata.table_manager.GroupStatusReport {
    var reports = std.ArrayListUnmanaged(antfly.metadata.table_manager.GroupStatusReport).empty;
    errdefer {
        for (reports.items) |record| antfly.metadata.table_manager.freeGroupStatus(alloc, record);
        reports.deinit(alloc);
    }
    try appendRaftOnlyLocalGroupStatusFallbacks(
        alloc,
        &reports,
        group_ids,
        group_leadership_source,
        group_membership_source,
    );
    return try reports.toOwnedSlice(alloc);
}

fn mergeRaftOnlyLocalGroupStatusFallbacks(
    alloc: std.mem.Allocator,
    owned_statuses: []antfly.metadata.table_manager.GroupStatusReport,
    group_ids: []const u64,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
) ![]antfly.metadata.table_manager.GroupStatusReport {
    var reports = std.ArrayListUnmanaged(antfly.metadata.table_manager.GroupStatusReport).fromOwnedSlice(owned_statuses);
    errdefer {
        for (reports.items) |record| antfly.metadata.table_manager.freeGroupStatus(alloc, record);
        reports.deinit(alloc);
    }
    try appendRaftOnlyLocalGroupStatusFallbacks(
        alloc,
        &reports,
        group_ids,
        group_leadership_source,
        group_membership_source,
    );
    return try reports.toOwnedSlice(alloc);
}

fn appendRaftOnlyLocalGroupStatusFallbacks(
    alloc: std.mem.Allocator,
    reports: *std.ArrayListUnmanaged(antfly.metadata.table_manager.GroupStatusReport),
    group_ids: []const u64,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
) !void {
    for (group_ids) |group_id| {
        if (groupStatusSliceContainsGroup(reports.items, group_id)) continue;
        if (collectRaftOnlyLocalGroupStatus(group_id, group_leadership_source, group_membership_source)) |status| {
            try reports.append(alloc, status);
        }
    }
}

fn groupStatusSliceContainsGroup(
    reports: []const antfly.metadata.table_manager.GroupStatusReport,
    group_id: u64,
) bool {
    for (reports) |report| {
        if (report.group_id == group_id) return true;
    }
    return false;
}

fn collectLocalGroupStatusFromRuntimeStatus(
    alloc: std.mem.Allocator,
    status: runtime_status.LocalTableRuntimeStatus,
    cached_group: ?antfly.metadata.table_manager.GroupStatusReport,
    replica_root_dir: ?[]const u8,
    group_id: u64,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
    snapshot_stores: []const antfly.metadata.StoreRecord,
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    split_transitions: []const antfly.metadata.SplitTransitionRecord,
    merge_transitions: []const antfly.metadata.MergeTransitionRecord,
    split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
    merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
) !antfly.metadata.table_manager.GroupStatusReport {
    const now_realtime_ms = platform_clock.Clock.real().nowRealtimeMs();
    const readiness = if (replica_root_dir) |root_dir| blk: {
        const local_readiness = try deriveLocalGroupReadiness(
            alloc,
            root_dir,
            group_id,
            snapshot_stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        );
        break :blk local_readiness;
    } else antfly.metadata.transition_state.readinessForGroup(group_id, split_transitions, merge_transitions);
    const membership = if (group_membership_source) |source| source.membership(group_id) else GroupMembership{};
    const fallback = cached_group orelse antfly.metadata.table_manager.GroupStatusReport{ .group_id = group_id };

    return .{
        .group_id = group_id,
        .doc_count = status.stats.doc_count,
        .disk_bytes = if (status.disk_bytes != 0) status.disk_bytes else fallback.disk_bytes,
        .empty = status.stats.doc_count == 0,
        .created_at_millis = if (status.created_at_millis != 0)
            status.created_at_millis
        else if (fallback.created_at_millis != 0)
            fallback.created_at_millis
        else
            now_realtime_ms,
        .updated_at_millis = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms)),
        .local_leader = if (group_leadership_source) |source| source.isLocalLeader(group_id) else fallback.local_leader,
        .local_voter = membership.local_voter,
        .voter_count = membership.voter_count,
        .joint_consensus = membership.joint_consensus,
        .transition_pending = readiness.transition_pending,
        .replay_required = readiness.replay_required,
        .replay_caught_up = readiness.replay_caught_up,
        .cutover_ready = readiness.cutover_ready,
        .reads_ready_after_cutover = readiness.reads_ready_after_cutover,
    };
}

fn deriveLocalGroupReadiness(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    group_id: u64,
    snapshot_stores: []const antfly.metadata.StoreRecord,
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    split_transitions: []const antfly.metadata.SplitTransitionRecord,
    merge_transitions: []const antfly.metadata.MergeTransitionRecord,
    split_observations: []const antfly.metadata.transition_state.SplitObservationRecord,
    merge_observations: []const antfly.metadata.transition_state.MergeObservationRecord,
) !antfly.metadata.transition_state.GroupTransitionReadiness {
    var readiness = antfly.metadata.transition_state.GroupTransitionReadiness{};
    var used_phase_only = false;

    for (split_transitions) |record| {
        if (record.source_group_id != group_id and record.destination_group_id != group_id) continue;
        const result = try antfly.metadata.transition_state.readinessResultForLocalSplitTransition(
            alloc,
            replica_root_dir,
            record,
            split_observations,
        );
        readiness = combineTransitionReadiness(readiness, result.readiness);
        if (result.source == .phase) used_phase_only = true;
    }

    for (merge_transitions) |record| {
        if (record.donor_group_id != group_id and record.receiver_group_id != group_id) continue;
        const result = try antfly.metadata.transition_state.readinessResultForLocalMergeTransition(
            alloc,
            replica_root_dir,
            record,
            merge_observations,
        );
        readiness = combineTransitionReadiness(readiness, result.readiness);
        if (result.source == .phase) used_phase_only = true;
    }

    if (!usedPhaseOnlyTransition(group_id, split_transitions, merge_transitions, readiness, used_phase_only)) {
        return readiness;
    }
    if (findMergedSnapshotGroupStatus(merged_group_statuses, group_id)) |status| {
        return .{
            .transition_pending = status.transition_pending,
            .replay_required = status.replay_required,
            .replay_caught_up = status.replay_caught_up,
            .cutover_ready = status.cutover_ready,
            .reads_ready_after_cutover = status.reads_ready_after_cutover,
        };
    }
    if (latestSnapshotGroupStatus(snapshot_stores, group_id)) |status| {
        return .{
            .transition_pending = status.transition_pending,
            .replay_required = status.replay_required,
            .replay_caught_up = status.replay_caught_up,
            .cutover_ready = status.cutover_ready,
            .reads_ready_after_cutover = status.reads_ready_after_cutover,
        };
    }
    return readiness;
}

fn combineTransitionReadiness(
    current: antfly.metadata.transition_state.GroupTransitionReadiness,
    next: antfly.metadata.transition_state.GroupTransitionReadiness,
) antfly.metadata.transition_state.GroupTransitionReadiness {
    return .{
        .transition_pending = current.transition_pending or next.transition_pending,
        .replay_required = current.replay_required or next.replay_required,
        .replay_caught_up = current.replay_caught_up or next.replay_caught_up,
        .cutover_ready = current.cutover_ready or next.cutover_ready,
        .reads_ready_after_cutover = current.reads_ready_after_cutover or next.reads_ready_after_cutover,
    };
}

fn usedPhaseOnlyTransition(
    group_id: u64,
    split_transitions: []const antfly.metadata.SplitTransitionRecord,
    merge_transitions: []const antfly.metadata.MergeTransitionRecord,
    readiness: antfly.metadata.transition_state.GroupTransitionReadiness,
    used_phase_only: bool,
) bool {
    if (!used_phase_only) return false;
    if (readiness.transition_pending) return true;
    for (split_transitions) |record| {
        if (record.source_group_id == group_id or record.destination_group_id == group_id) return true;
    }
    for (merge_transitions) |record| {
        if (record.donor_group_id == group_id or record.receiver_group_id == group_id) return true;
    }
    return false;
}

fn latestSnapshotGroupStatus(
    stores: []const antfly.metadata.StoreRecord,
    group_id: u64,
) ?antfly.metadata.table_manager.GroupStatusReport {
    var latest: ?antfly.metadata.table_manager.GroupStatusReport = null;
    var latest_leader: ?antfly.metadata.table_manager.GroupStatusReport = null;
    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        for (store.group_statuses) |group_status| {
            if (group_status.group_id != group_id) continue;
            if (group_status.local_leader) {
                if (latest_leader == null or group_status.updated_at_millis >= latest_leader.?.updated_at_millis) {
                    latest_leader = group_status;
                }
            }
            if (latest == null or group_status.updated_at_millis >= latest.?.updated_at_millis) {
                latest = group_status;
            }
        }
    }
    return latest_leader orelse latest;
}

fn findSnapshotStoreByNodeId(
    stores: []const antfly.metadata.StoreRecord,
    node_id: u64,
) ?antfly.metadata.StoreRecord {
    for (stores) |store| {
        if (store.node_id == node_id) return store;
    }
    return null;
}

fn findSnapshotStore(
    stores: []const antfly.metadata.StoreRecord,
    store_id: u64,
) ?antfly.metadata.StoreRecord {
    for (stores) |store| {
        if (store.store_id == store_id) return store;
    }
    return null;
}

fn localIntentPreferredCampaigner(intent: antfly.raft.PlacementIntent, local_node_id: u64) bool {
    var min_node_id = intent.record.local_node_id;
    for (intent.peer_node_ids) |node_id| {
        if (node_id < min_node_id) min_node_id = node_id;
    }
    return local_node_id == min_node_id;
}

fn dataRaftPlacementIntentsFingerprint(intents: []const antfly.raft.PlacementIntent) u64 {
    var hasher = std.hash.Wyhash.init(0x48f9_2026_da7a_4a17);
    hashU64(&hasher, intents.len);
    for (intents) |intent| {
        hashU64(&hasher, intent.record.group_id);
        hashU64(&hasher, intent.record.replica_id);
        hashU64(&hasher, intent.record.local_node_id);
        hashU64(&hasher, @intFromEnum(intent.record.bootstrap_mode));
        hashU64(&hasher, intent.record.metadata_version);
        hashU64(&hasher, intent.store_id);
        hashU64(&hasher, intent.peer_node_ids.len);
        for (intent.peer_node_ids) |peer_node_id| hashU64(&hasher, peer_node_id);
    }
    return hasher.final();
}

fn hashU64(hasher: *std.hash.Wyhash, value: u64) void {
    hasher.update(std.mem.asBytes(&value));
}

fn findMergedSnapshotGroupStatus(
    merged_group_statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
    group_id: u64,
) ?antfly.metadata.reconciler.MergedGroupStatus {
    for (merged_group_statuses) |status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn directoryUsageBytes(alloc: std.mem.Allocator, path: []const u8) !u64 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer dir.close(io_impl.io());

    var total: u64 = 0;
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        total += stat.size;
    }
    return total;
}

fn freeGroupStatusesOwned(alloc: std.mem.Allocator, group_statuses: []const antfly.metadata.table_manager.GroupStatusReport) void {
    antfly.metadata.table_manager.freeGroupStatuses(alloc, group_statuses);
}

fn freeStoreStatusReportOwned(alloc: std.mem.Allocator, report: *antfly.metadata.table_manager.StoreStatusReport) void {
    alloc.free(report.health_class);
    antfly.metadata.table_manager.freeGroupStatuses(alloc, report.group_statuses);
    antfly.metadata.table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
    report.* = undefined;
}

fn freeAdminSnapshotOwned(alloc: std.mem.Allocator, snapshot: *antfly.metadata_api.AdminSnapshot) void {
    for (snapshot.tables) |record| antfly.metadata.table_manager.freeTable(alloc, record);
    alloc.free(snapshot.tables);
    for (snapshot.ranges) |record| antfly.metadata.table_manager.freeRange(alloc, record);
    alloc.free(snapshot.ranges);
    for (snapshot.stores) |record| antfly.metadata.table_manager.freeStore(alloc, record);
    alloc.free(snapshot.stores);
    for (snapshot.placement_intents) |intent| if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
    alloc.free(snapshot.placement_intents);
    for (snapshot.shuffle_join_leases) |record| antfly.metadata.table_manager.freeShuffleJoinLease(alloc, record);
    if (snapshot.shuffle_join_leases.len > 0) alloc.free(snapshot.shuffle_join_leases);
    for (snapshot.local_bootstrap_statuses) |record| {
        if (record.last_error) |value| alloc.free(value);
        if (record.backup_id) |value| alloc.free(value);
        if (record.snapshot_path) |value| alloc.free(value);
    }
    if (snapshot.local_bootstrap_statuses.len > 0) alloc.free(snapshot.local_bootstrap_statuses);
    for (snapshot.restore_progresses) |record| antfly.metadata.table_manager.freeRestoreProgress(alloc, record);
    if (snapshot.restore_progresses.len > 0) alloc.free(snapshot.restore_progresses);
    for (snapshot.replication_source_statuses) |record| antfly.metadata.table_manager.freeReplicationSourceStatus(alloc, record);
    if (snapshot.replication_source_statuses.len > 0) alloc.free(snapshot.replication_source_statuses);
    for (snapshot.replication_source_action_hints) |record| {
        alloc.free(record.table_name);
        alloc.free(record.action);
        alloc.free(record.reason);
        alloc.free(record.reseed_exact_cutover_path);
    }
    if (snapshot.replication_source_action_hints.len > 0) alloc.free(snapshot.replication_source_action_hints);
    for (snapshot.split_transitions) |record| antfly.metadata.table_manager.freeSplitTransitionRecord(alloc, record);
    alloc.free(snapshot.split_transitions);
    for (snapshot.merge_transitions) |record| antfly.metadata.table_manager.freeMergeTransitionRecord(alloc, record);
    alloc.free(snapshot.merge_transitions);
    if (snapshot.split_observations.len > 0) alloc.free(snapshot.split_observations);
    if (snapshot.merge_observations.len > 0) alloc.free(snapshot.merge_observations);
    if (snapshot.merged_group_statuses.len > 0) {
        alloc.free(snapshot.merged_group_statuses);
    }
    snapshot.* = undefined;
}

fn cloneShuffleJoinLeasesOwned(
    alloc: std.mem.Allocator,
    records: []antfly.metadata.table_manager.ShuffleJoinLeaseRecord,
) ![]antfly.metadata.table_manager.ShuffleJoinLeaseRecord {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.metadata.table_manager.ShuffleJoinLeaseRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try antfly.metadata.table_manager.cloneShuffleJoinLease(alloc, record);
    return out;
}

fn cloneLocalBootstrapStatusesOwned(
    alloc: std.mem.Allocator,
    records: []antfly.raft.host.BootstrapStatus,
) ![]antfly.raft.host.BootstrapStatus {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.raft.host.BootstrapStatus, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .group_id = record.group_id,
            .kind = record.kind,
            .phase = record.phase,
            .attempts = record.attempts,
            .last_updated_at_millis = record.last_updated_at_millis,
            .last_error = if (record.last_error) |value| try alloc.dupe(u8, value) else null,
            .backup_id = if (record.backup_id) |value| try alloc.dupe(u8, value) else null,
            .snapshot_path = if (record.snapshot_path) |value| try alloc.dupe(u8, value) else null,
        };
    }
    return out;
}

fn cloneRestoreProgressesOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.metadata.table_manager.RestoreProgressRecord,
) ![]antfly.metadata.table_manager.RestoreProgressRecord {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.metadata.table_manager.RestoreProgressRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try antfly.metadata.table_manager.cloneRestoreProgress(alloc, record);
    return out;
}

fn cloneReplicationSourceStatusesOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.metadata.table_manager.ReplicationSourceStatusRecord,
) ![]antfly.metadata.table_manager.ReplicationSourceStatusRecord {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.metadata.table_manager.ReplicationSourceStatusRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try antfly.metadata.table_manager.cloneReplicationSourceStatus(alloc, record);
    return out;
}

fn cloneReplicationSourceActionHintsOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.metadata_api.ReplicationSourceActionHint,
) ![]antfly.metadata_api.ReplicationSourceActionHint {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.metadata_api.ReplicationSourceActionHint, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .table_id = record.table_id,
            .table_name = try alloc.dupe(u8, record.table_name),
            .source_ordinal = record.source_ordinal,
            .action = try alloc.dupe(u8, record.action),
            .reason = try alloc.dupe(u8, record.reason),
            .reseed_exact_cutover_path = try alloc.dupe(u8, record.reseed_exact_cutover_path),
        };
    }
    return out;
}

fn cloneTablesOwned(alloc: std.mem.Allocator, records: []const antfly.metadata.table_manager.TableRecord) ![]antfly.metadata.table_manager.TableRecord {
    const out = try alloc.alloc(antfly.metadata.table_manager.TableRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try antfly.metadata.table_manager.cloneTable(alloc, record);
    return out;
}

fn cloneRangesOwned(alloc: std.mem.Allocator, records: []const antfly.metadata.table_manager.RangeRecord) ![]antfly.metadata.table_manager.RangeRecord {
    const out = try alloc.alloc(antfly.metadata.table_manager.RangeRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try antfly.metadata.table_manager.cloneRange(alloc, record);
    return out;
}

fn cloneStoresOwned(alloc: std.mem.Allocator, records: []const antfly.metadata.table_manager.StoreRecord) ![]antfly.metadata.table_manager.StoreRecord {
    const out = try alloc.alloc(antfly.metadata.table_manager.StoreRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try antfly.metadata.table_manager.cloneStore(alloc, record);
    return out;
}

fn clonePlacementIntentsOwned(alloc: std.mem.Allocator, intents: []const antfly.raft.reconciler.PlacementIntent) ![]antfly.raft.reconciler.PlacementIntent {
    const out = try alloc.alloc(antfly.raft.reconciler.PlacementIntent, intents.len);
    errdefer alloc.free(out);
    for (intents, 0..) |intent, i| {
        out[i] = .{
            .record = intent.record,
            .store_id = intent.store_id,
            .peer_node_ids = try alloc.dupe(u64, intent.peer_node_ids),
        };
    }
    return out;
}

fn cloneMergedGroupStatusesOwned(
    alloc: std.mem.Allocator,
    statuses: []const antfly.metadata.reconciler.MergedGroupStatus,
) ![]antfly.metadata.reconciler.MergedGroupStatus {
    const out = try alloc.alloc(antfly.metadata.reconciler.MergedGroupStatus, statuses.len);
    errdefer alloc.free(out);
    for (statuses, 0..) |status, i| {
        out[i] = status;
    }
    return out;
}

fn cloneSplitTransitionsOwned(alloc: std.mem.Allocator, records: []const antfly.metadata.transition_state.SplitTransitionRecord) ![]antfly.metadata.transition_state.SplitTransitionRecord {
    const out = try alloc.alloc(antfly.metadata.transition_state.SplitTransitionRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .source_group_id = record.source_group_id,
            .destination_group_id = record.destination_group_id,
            .phase = record.phase,
            .split_key = if (record.split_key) |value| try alloc.dupe(u8, value) else null,
            .source_range_end = if (record.source_range_end) |value| try alloc.dupe(u8, value) else null,
            .rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null,
        };
    }
    return out;
}

fn cloneMergeTransitionsOwned(alloc: std.mem.Allocator, records: []const antfly.metadata.transition_state.MergeTransitionRecord) ![]antfly.metadata.transition_state.MergeTransitionRecord {
    const out = try alloc.alloc(antfly.metadata.transition_state.MergeTransitionRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .donor_group_id = record.donor_group_id,
            .receiver_group_id = record.receiver_group_id,
            .phase = record.phase,
            .rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null,
        };
    }
    return out;
}

fn cloneSplitObservationsOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.metadata.transition_state.SplitObservationRecord,
) ![]antfly.metadata.transition_state.SplitObservationRecord {
    if (records.len == 0) return &.{};
    return try alloc.dupe(antfly.metadata.transition_state.SplitObservationRecord, records);
}

fn cloneMergeObservationsOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.metadata.transition_state.MergeObservationRecord,
) ![]antfly.metadata.transition_state.MergeObservationRecord {
    if (records.len == 0) return &.{};
    return try alloc.dupe(antfly.metadata.transition_state.MergeObservationRecord, records);
}

fn stringifyJsonAlloc(alloc: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

pub fn run(init: std.process.Init) !void {
    const alloc = init.gpa;

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "antfly_data";
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

    if (cli.secret_store_path) |raw_secret_store_path| {
        const normalized_secret_store_path = try normalizeResolvedPathAlloc(alloc, raw_secret_store_path);
        defer alloc.free(normalized_secret_store_path);
        secret_store = try antfly.common.secrets.FileStore.init(alloc, normalized_secret_store_path);
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

    const metadata_api_urls = try resolveMetadataApiUrls(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer metadata_api_urls.deinit(alloc);
    if ((cli.node_id == null) != (cli.store_id == null)) return error.InvalidArguments;
    const auth_enabled = resolveAuthEnabled(cli, if (loaded_config) |*cfg| cfg else null);

    var setup_io = std.Io.Threaded.init(alloc, .{ .stack_size = setup_io_thread_stack_size });
    defer setup_io.deinit();
    try ensureDirAndParent(setup_io.io(), resolved.replica_root_dir, resolved.replica_catalog_path);
    try fs_paths.createDirPathPortable(setup_io.io(), resolved.snapshot_root_dir);
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
        // This seeds only the local auth store and must remain auth-gated.
        // Raft-backed metadata writes during metadata bootstrap can block
        // clustered startup before raft listeners are running.
        try antfly.usermgr.ensureDefaultAdminUser(&user_manager.?);
    }
    defer if (user_manager) |*manager| manager.deinit();
    defer if (auth_runtime) |*runtime| runtime.deinit();
    defer if (auth_backend) |*backend| backend.close();

    var data_server = try DataServer.initFromMetadataApiUrls(alloc, .{
        .bind_host = cli.bind_host orelse "127.0.0.1",
        .bind_port = cli.bind_port orelse 0,
        .raft_bind_host = cli.raft_bind_host orelse "127.0.0.1",
        .raft_bind_port = cli.raft_bind_port orelse 0,
        .replica_root_dir = resolved.replica_root_dir,
        .replica_catalog_path = resolved.replica_catalog_path,
        .snapshot_root_dir = resolved.snapshot_root_dir,
        .store_registration = if (cli.node_id != null and cli.store_id != null) .{
            .node_id = cli.node_id.?,
            .store_id = cli.store_id.?,
            .role = cli.store_role orelse "data",
            .failure_domain = cli.failure_domain orelse "",
        } else null,
        .api_server_cfg = .{
            .auth_enabled = auth_enabled,
            .user_manager = if (user_manager) |*manager| manager else null,
            .secret_store = if (secret_store_initialized) &secret_store else null,
        },
    }, metadata_api_urls.urls);
    defer data_server.deinit();
    try data_server.start();

    const base_uri = try data_server.baseUri(alloc);
    defer alloc.free(base_uri);
    std.debug.print("data api listening on {s}\n", .{base_uri});
    std.debug.print("using metadata api at {s}", .{metadata_api_urls.urls[0]});
    if (metadata_api_urls.urls.len > 1) std.debug.print(" (+{d} more)", .{metadata_api_urls.urls.len - 1});
    std.debug.print("\n", .{});

    var data_health = HealthSource{ .data_server = &data_server };
    const health_enabled = cli.health_enabled orelse if (loaded_config) |*cfg| cfg.health_enabled else true;
    const health_port = if (health_enabled)
        cli.health_port orelse if (loaded_config) |*cfg| cfg.health_port else antfly.common.config.default_health_port
    else
        null;
    const health_server = try antfly.common.health_server.HealthServer.startIfConfigured(
        alloc,
        "data",
        health_port,
        data_health.readiness(),
        data_health.metricsWriter(),
    );
    defer if (health_server) |hs| hs.deinit();

    const tick_ms = cli.tick_ms orelse 25;
    var req = std.posix.timespec{
        .sec = @intCast(tick_ms / std.time.ms_per_s),
        .nsec = @intCast((tick_ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) {
        try data_server.runRound();
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
        if (std.mem.eql(u8, arg, "--api-host")) {
            cfg.bind_host = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--api-port")) {
            cfg.bind_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--raft-host")) {
            cfg.raft_bind_host = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--raft-port")) {
            cfg.raft_bind_port = try std.fmt.parseInt(u16, args.next() orelse return error.InvalidArguments, 10);
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
        if (std.mem.eql(u8, arg, "--auth")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.auth_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--auth=")) {
            cfg.auth_enabled = parseBoolFlag(arg["--auth=".len..]) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--metadata-api")) {
            try cfg.metadata_apis.append(alloc, args.next() orelse return error.InvalidArguments);
            continue;
        }
        if (std.mem.eql(u8, arg, "--node-id")) {
            cfg.node_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--store-id")) {
            cfg.store_id = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArguments, 10);
            continue;
        }
        if (std.mem.eql(u8, arg, "--store-role")) {
            cfg.store_role = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--failure-domain")) {
            cfg.failure_domain = args.next() orelse return error.InvalidArguments;
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
        if (std.mem.eql(u8, arg, "--secret-store-path")) {
            cfg.secret_store_path = args.next() orelse return error.InvalidArguments;
            continue;
        }
        return error.InvalidArguments;
    }
    return cfg;
}

fn resolveLocalBaseDir(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) ![]u8 {
    if (cli.data_dir) |path| return try normalizeResolvedPathAlloc(alloc, path);
    return try antfly.common.config.resolveLocalBaseDir(alloc, cfg);
}

fn resolvePaths(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) !ResolvedPaths {
    const local_base = try resolveLocalBaseDir(alloc, cli, cfg);
    defer alloc.free(local_base);
    const metadata_base = try std.fmt.allocPrint(alloc, "{s}/metadata", .{local_base});
    defer alloc.free(metadata_base);

    if (cli.replica_root_dir != null and cli.replica_catalog_path != null) {
        const base = try std.fmt.allocPrint(alloc, "{s}/data", .{local_base});
        defer alloc.free(base);
        const replica_root_dir = try normalizeResolvedPathAlloc(alloc, cli.replica_root_dir.?);
        errdefer alloc.free(replica_root_dir);
        const replica_catalog_path = try normalizeResolvedPathAlloc(alloc, cli.replica_catalog_path.?);
        errdefer alloc.free(replica_catalog_path);
        const snapshot_root_dir = if (cli.snapshot_root_dir) |path|
            try normalizeResolvedPathAlloc(alloc, path)
        else blk: {
            const raw = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{base});
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
        return .{
            .replica_root_dir = replica_root_dir,
            .replica_catalog_path = replica_catalog_path,
            .snapshot_root_dir = snapshot_root_dir,
            .auth_store_root_dir = auth_store_root_dir,
        };
    }

    const base = try std.fmt.allocPrint(alloc, "{s}/data", .{local_base});
    defer alloc.free(base);
    const replica_root_dir = if (cli.replica_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/replicas", .{base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_root_dir);
    const replica_catalog_path = if (cli.replica_catalog_path) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/catalog.txt", .{base});
        defer alloc.free(raw);
        break :blk try normalizeResolvedPathAlloc(alloc, raw);
    };
    errdefer alloc.free(replica_catalog_path);
    const snapshot_root_dir = if (cli.snapshot_root_dir) |path|
        try normalizeResolvedPathAlloc(alloc, path)
    else blk: {
        const raw = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{base});
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
    return .{
        .replica_root_dir = replica_root_dir,
        .replica_catalog_path = replica_catalog_path,
        .snapshot_root_dir = snapshot_root_dir,
        .auth_store_root_dir = auth_store_root_dir,
    };
}

fn ensureDirAndParent(io: std.Io, replica_root_dir: []const u8, replica_catalog_path: []const u8) !void {
    try fs_paths.createDirPathPortable(io, replica_root_dir);
    if (std.fs.path.dirname(replica_catalog_path)) |parent| {
        try fs_paths.createDirPathPortable(io, parent);
    }
}

fn normalizeResolvedPathAlloc(alloc: std.mem.Allocator, path: []const u8) ![]u8 {
    if (!std.fs.path.isAbsolute(path)) return try alloc.dupe(u8, path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var probe = path;
    while (true) {
        const resolved_z = std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), probe, alloc) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => null,
            else => return err,
        };
        if (resolved_z) |resolved| {
            const resolved_prefix = resolved[0..resolved.len];
            if (probe.len == path.len) return resolved_prefix;

            const suffix_start: usize = if (probe.len == 1) 1 else probe.len + 1;
            const suffix = path[suffix_start..];
            const joined = try std.fs.path.join(alloc, &.{ resolved_prefix, suffix });
            alloc.free(resolved_prefix);
            return joined;
        }

        const parent = std.fs.path.dirname(probe) orelse return try alloc.dupe(u8, path);
        if (parent.len == probe.len) return try alloc.dupe(u8, path);
        probe = parent;
    }
}

fn resolveMetadataApiUrls(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) !ResolvedMetadataApiUrls {
    if (cli.metadata_apis.items.len > 0) {
        const urls = try alloc.alloc([]const u8, cli.metadata_apis.items.len);
        @memcpy(urls, cli.metadata_apis.items);
        return .{ .urls = urls };
    }
    if (cfg) |loaded| {
        if (loaded.metadata.orchestration_urls.len > 0) {
            var urls = try alloc.alloc([]const u8, loaded.metadata.orchestration_urls.len);
            errdefer alloc.free(urls);
            for (loaded.metadata.orchestration_urls, 0..) |entry, i| urls[i] = entry.url;
            return .{ .urls = urls };
        }
    }
    return error.MissingMetadataApi;
}

fn singleMetadataApiUrl(alloc: std.mem.Allocator, value: []const u8) !ResolvedMetadataApiUrls {
    const urls = try alloc.alloc([]const u8, 1);
    urls[0] = value;
    return .{ .urls = urls };
}

fn resolveAuthEnabled(cli: CliConfig, cfg: ?*const antfly.common.config.Config) bool {
    if (cli.auth_enabled) |value| return value;
    if (cfg) |loaded| return loaded.auth_enabled;
    return false;
}

fn parseBoolFlag(value: []const u8) ?bool {
    if (std.mem.eql(u8, value, "true")) return true;
    if (std.mem.eql(u8, value, "false")) return false;
    return null;
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\Usage: {s} [options]
        \\
        \\Options:
        \\  --config <path>                  JSON common config file
        \\  --api-host <host>              Data API bind host (default: 127.0.0.1)
        \\  --api-port <port>              Data API bind port (default: 0)
        \\  --raft-host <host>             Data raft bind host (default: 127.0.0.1)
        \\  --raft-port <port>             Data raft bind port (default: 0 when registered)
        \\  --health <true|false>          Enable health/metrics server (default: true)
        \\  --health-port <port>           Dedicated health/metrics bind port (default: 4200)
        \\  --auth <true|false>            Enable auth middleware and local user store
        \\  --metadata-api <uri>           Metadata orchestration/API URL (repeat for multiple endpoints)
        \\  --node-id <id>                 Register this split data process as metadata node <id>
        \\  --store-id <id>                Register this split data process as metadata store <id>
        \\  --store-role <role>            Registered store role (default: data)
        \\  --failure-domain <name>        Registered store failure domain
        \\  --tick-ms <ms>                 Sleep interval while serving (default: 25)
        \\  --data-dir <path>              Local storage root for data node data
        \\  --replica-root-dir <path>      Replica root directory
        \\  --replica-catalog-path <path>  Replica catalog file path
        \\  --snapshot-root-dir <path>     Data raft snapshot root directory
        \\  --secret-store-path <path>     Antfly secrets.json file path
        \\  -h, --help                     Show this help
        \\
    , .{argv0});
}

test "data runtime module compiles" {
    _ = run;
    _ = runFromIterator;
    _ = DataServerConfig;
    _ = DataServer;
    _ = GroupLeadershipSource;
}

test "data server can register a store without enabling data raft" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-no-raft-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);

    var server = try DataServer.initFromMetadataApiUrl(std.testing.allocator, .{
        .enable_data_raft = false,
        .replica_root_dir = replica_root,
        .store_registration = .{
            .node_id = 1,
            .store_id = 1,
            .api_url = "http://127.0.0.1:1",
        },
    }, "http://127.0.0.1:2");
    defer server.deinit();

    try std.testing.expect(server.data_raft == null);
    try std.testing.expect(server.data_raft_apply == null);

    server.initApiServer();
    try std.testing.expect(server.write_source.raft_batcher == null);
}

test "data server registered data raft uses wal state backend by default" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-default-wal-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);

    var server = try DataServer.initFromMetadataApiUrl(std.testing.allocator, .{
        .replica_root_dir = replica_root,
        .store_registration = .{
            .node_id = 1,
            .store_id = 1,
            .api_url = "http://127.0.0.1:1",
        },
    }, "http://127.0.0.1:2");
    defer server.deinit();

    const data_raft = server.data_raft orelse return error.MissingDataRaft;
    try std.testing.expect(data_raft.host.owned_wal_replica_provider != null);
    try std.testing.expect(data_raft.host.owned_file_replica_provider == null);
}

test "data runtime cli accepts config path" {
    const argv = [_][*:0]const u8{
        "--config",
        "antfly.json",
        "--api-port",
        "8080",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("antfly.json", cfg.config_path.?);
    try std.testing.expectEqual(@as(u16, 8080), cfg.bind_port.?);
}

test "data runtime cli accepts secret store path" {
    const argv = [_][*:0]const u8{ "--secret-store-path", "/run/antfly/secrets/secrets.json" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", cfg.secret_store_path.?);
}

test "data runtime resolves metadata api urls from common config" {
    const alloc = std.testing.allocator;
    const orchestration_urls = try alloc.alloc(antfly.common.config.Config.MetadataConfig.NodeUrl, 2);
    orchestration_urls[0] = .{ .node_id = 1, .url = try alloc.dupe(u8, "http://127.0.0.1:7101") };
    orchestration_urls[1] = .{ .node_id = 2, .url = try alloc.dupe(u8, "http://127.0.0.1:7102") };
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .metadata = .{
            .orchestration_urls = orchestration_urls,
        },
    };
    defer cfg.deinit();

    const resolved = try resolveMetadataApiUrls(alloc, .{}, &cfg);
    defer resolved.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), resolved.urls.len);
    try std.testing.expectEqualStrings("http://127.0.0.1:7101", resolved.urls[0]);
    try std.testing.expectEqualStrings("http://127.0.0.1:7102", resolved.urls[1]);
}

test "data runtime resolves paths from common storage base dir" {
    const alloc = std.testing.allocator;
    var cfg = antfly.common.config.Config{
        .registry = antfly.common.provider_registry.Registry.init(alloc),
        .transcribers = antfly.transcribing.Registry.init(alloc),
        .readers = antfly.readers.Registry.init(alloc),
        .text_to_speech = antfly.synthesizing.Registry.init(alloc),
        .storage = .{ .local_base_dir = try alloc.dupe(u8, "/tmp/antflydb") },
    };
    defer cfg.deinit();

    const resolved = try resolvePaths(alloc, .{}, &cfg);
    defer resolved.deinit(alloc);
    try std.testing.expectEqualStrings("/tmp/antflydb/data/replicas", resolved.replica_root_dir);
    try std.testing.expectEqualStrings("/tmp/antflydb/data/catalog.txt", resolved.replica_catalog_path);
    try std.testing.expectEqualStrings("/tmp/antflydb/data/snapshots", resolved.snapshot_root_dir);
    try std.testing.expectEqualStrings("/tmp/antflydb/metadata/auth", resolved.auth_store_root_dir);
}

test "data runtime parses optional split store registration flags" {
    const argv = [_][*:0]const u8{
        "--metadata-api",
        "http://127.0.0.1:19001",
        "--node-id",
        "11",
        "--store-id",
        "21",
        "--store-role",
        "data",
        "--failure-domain",
        "rack-a",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseCli(std.testing.allocator, &iter);
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), parsed.metadata_apis.items.len);
    try std.testing.expectEqualStrings("http://127.0.0.1:19001", parsed.metadata_apis.items[0]);
    try std.testing.expectEqual(@as(u64, 11), parsed.node_id.?);
    try std.testing.expectEqual(@as(u64, 21), parsed.store_id.?);
    try std.testing.expectEqualStrings("data", parsed.store_role.?);
    try std.testing.expectEqualStrings("rack-a", parsed.failure_domain.?);
}

test "data runtime accepts repeated metadata api flags" {
    const argv = [_][*:0]const u8{
        "--metadata-api",
        "http://127.0.0.1:19001",
        "--metadata-api",
        "http://127.0.0.1:19002",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseCli(std.testing.allocator, &iter);
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 2), parsed.metadata_apis.items.len);
    try std.testing.expectEqualStrings("http://127.0.0.1:19001", parsed.metadata_apis.items[0]);
    try std.testing.expectEqualStrings("http://127.0.0.1:19002", parsed.metadata_apis.items[1]);
}

test "data runtime parses auth flag" {
    const argv = [_][*:0]const u8{
        "--auth",
        "true",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var parsed = try parseCli(std.testing.allocator, &iter);
    defer parsed.deinit(std.testing.allocator);
    try std.testing.expectEqual(true, parsed.auth_enabled.?);
}

test "data runtime leaves auth disabled unless config or cli enables it" {
    var cli = CliConfig{};
    defer cli.deinit(std.testing.allocator);
    try std.testing.expect(!resolveAuthEnabled(cli, null));

    cli.auth_enabled = true;
    try std.testing.expect(resolveAuthEnabled(cli, null));

    cli.auth_enabled = false;
    try std.testing.expect(!resolveAuthEnabled(cli, null));
}

test "data runtime local group status uses injected leadership source" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-leadership-db", .{tmp.sub_path});
    defer std.testing.allocator.free(db_path);

    var db = try antfly.db.DB.open(std.testing.allocator, db_path, .{});
    db.close();

    const Source = struct {
        fn iface() GroupLeadershipSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .is_local_leader = isLocalLeader,
                },
            };
        }

        fn isLocalLeader(_: *anyopaque, group_id: u64) bool {
            return group_id == 77;
        }
    };

    const report = try collectLocalGroupStatus(std.testing.allocator, db_path, null, 77, .{}, Source.iface(), null, &.{}, &.{}, &.{}, &.{}, &.{}, &.{});
    defer antfly.metadata.table_manager.freeGroupStatus(std.testing.allocator, report);
    try std.testing.expect(report.local_leader);
}

test "data runtime local group status status-only open preserves replay debt" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-local-group-status-replay-debt", .{tmp.sub_path});
    defer alloc.free(db_path);

    var appended_sequence: u64 = 0;

    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try antfly.db.internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try antfly.db.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        const payload = try antfly.db.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 0 });
        defer alloc.free(payload);
        try db.core.store.put(artifact_key, payload);

        var dense_embeddings = try alloc.alloc(antfly.db.derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = antfly.db.derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer antfly.db.derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.nextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try db.core.store.appendReplayOpaque(alloc, appended_sequence, encoded);
    }

    {
        var before = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer before.close();
        const stats = try before.stats(alloc);
        defer antfly.db.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_target_sequence);
        try std.testing.expect(stats.indexes[0].replay_catch_up_required);
    }

    const report = try collectLocalGroupStatus(alloc, db_path, null, 77, .{}, null, null, &.{}, &.{}, &.{}, &.{}, &.{}, &.{});
    defer antfly.metadata.table_manager.freeGroupStatus(alloc, report);

    {
        var after = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer after.close();
        const stats = try after.stats(alloc);
        defer antfly.db.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_target_sequence);
        try std.testing.expect(stats.indexes[0].replay_catch_up_required);
    }
}

test "data runtime local group status fingerprint includes live raft state" {
    const Harness = struct {
        leader: bool,
        membership: GroupMembership,

        fn leadershipSource(self: *@This()) GroupLeadershipSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .is_local_leader = isLocalLeader,
                },
            };
        }

        fn membershipSource(self: *@This()) GroupMembershipSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .membership = membershipForGroup,
                },
            };
        }

        fn isLocalLeader(ptr: *anyopaque, _: u64) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.leader;
        }

        fn membershipForGroup(ptr: *anyopaque, _: u64) GroupMembership {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.membership;
        }
    };

    var harness = Harness{
        .leader = false,
        .membership = .{
            .local_voter = true,
            .voter_count = 3,
            .joint_consensus = false,
        },
    };

    const base = localGroupStatusFingerprint(
        &.{77},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        harness.leadershipSource(),
        harness.membershipSource(),
    );

    harness.leader = true;
    const after_leader_change = localGroupStatusFingerprint(
        &.{77},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        harness.leadershipSource(),
        harness.membershipSource(),
    );
    try std.testing.expect(base != after_leader_change);

    harness.leader = false;
    harness.membership.voter_count = 5;
    const after_membership_change = localGroupStatusFingerprint(
        &.{77},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        harness.leadershipSource(),
        harness.membershipSource(),
    );
    try std.testing.expect(base != after_membership_change);
}

test "data runtime local group status fingerprint ignores remote store status reports" {
    const status_a: antfly.metadata.table_manager.GroupStatusReport = .{
        .group_id = 77,
        .doc_count = 10,
        .disk_bytes = 100,
        .empty = false,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 3,
        .joint_consensus = false,
        .transition_pending = false,
        .replay_required = false,
        .replay_caught_up = true,
        .cutover_ready = true,
        .reads_ready_after_cutover = true,
        .created_at_millis = 1,
    };
    const status_b: antfly.metadata.table_manager.GroupStatusReport = .{
        .group_id = 77,
        .doc_count = 20,
        .disk_bytes = 200,
        .empty = false,
        .local_leader = false,
        .local_voter = true,
        .voter_count = 3,
        .joint_consensus = false,
        .transition_pending = false,
        .replay_required = false,
        .replay_caught_up = true,
        .cutover_ready = true,
        .reads_ready_after_cutover = true,
        .created_at_millis = 2,
    };

    const stores_a = [_]antfly.metadata.table_manager.StoreRecord{.{
        .store_id = 1,
        .node_id = 1,
        .role = "data",
        .live = true,
        .health_class = "healthy",
        .group_statuses = @constCast((&[_]antfly.metadata.table_manager.GroupStatusReport{status_a})[0..]),
    }};
    const stores_b = [_]antfly.metadata.table_manager.StoreRecord{.{
        .store_id = 1,
        .node_id = 1,
        .role = "data",
        .live = true,
        .health_class = "healthy",
        .group_statuses = @constCast((&[_]antfly.metadata.table_manager.GroupStatusReport{status_b})[0..]),
    }};

    const fingerprint_a = localGroupStatusFingerprint(
        &.{77},
        &.{},
        &.{},
        &stores_a,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        null,
        null,
    );
    const fingerprint_b = localGroupStatusFingerprint(
        &.{77},
        &.{},
        &.{},
        &stores_b,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        null,
        null,
    );
    try std.testing.expectEqual(fingerprint_a, fingerprint_b);
}

test "data runtime local group status provider collects and caches group statuses" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-provider-cache", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(replica_root_dir, FakeCatalog.iface()),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    const tables = [_]antfly.metadata.table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
    }};
    const ranges = [_]antfly.metadata.table_manager.RangeRecord{.{
        .group_id = 77,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    }};
    const stores = [_]antfly.metadata.table_manager.StoreRecord{.{
        .store_id = 19,
        .node_id = 9,
        .live = true,
        .health_class = "healthy",
    }};

    const provider = server.localGroupStatusProvider();
    const first = try provider.collect(
        alloc,
        replica_root_dir,
        tables[0..],
        ranges[0..],
        stores[0..],
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, first);

    try std.testing.expectEqual(@as(usize, 0), first.len);
    try std.testing.expectEqual(@as(usize, 1), server.local_group_status_cache.group_statuses.len);
    try std.testing.expectEqual(@as(u64, 77), server.local_group_status_cache.group_statuses[0].group_id);

    const second = try provider.collect(
        alloc,
        replica_root_dir,
        tables[0..],
        ranges[0..],
        stores[0..],
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, second);

    try std.testing.expectEqual(@as(usize, 1), second.len);
    const hook = server.localChangeHook();
    server.store_status_dirty = false;
    hook.on_change(hook.ptr, "docs", .data);
    try std.testing.expect(!server.store_status_dirty);
    try std.testing.expectEqual(@as(usize, 1), server.local_group_status_cache.group_statuses.len);

    server.invalidateLocalGroupStatusCache();
    try std.testing.expectEqual(@as(usize, 0), server.local_group_status_cache.group_statuses.len);
    try std.testing.expect(server.localGroupStatusCacheStale(@intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms))));

    try server.storeCachedLocalGroupStatuses(3, 99, &.{});
    try std.testing.expect(!server.localGroupStatusCacheStale(@intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms))));
    const empty_cached = (try server.cloneCachedLocalGroupStatuses(alloc, 3, 99)) orelse return error.TestUnexpectedResult;
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, empty_cached);
    try std.testing.expectEqual(@as(usize, 0), empty_cached.len);
}

test "data runtime local split fallback preserves source identity namespace" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-split-identity", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const source_db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 180);
    defer alloc.free(source_db_path);
    const destination_db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 181);
    defer alloc.free(destination_db_path);

    const source_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 180,
        .range_id = 9000,
    };

    {
        var db = try antfly.db.DB.open(alloc, source_db_path, .{
            .identity_namespace = source_namespace,
            .start_index_workers = false,
        });
        defer db.close();
        try db.updateRange(.{ .start = "doc:a", .end = "doc:z" });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:t", .value = "{\"v\":\"right\"}" }},
        });
    }

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 180,
                    .table_id = 7,
                    .range_id = 9000,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{.{
                    .transition_id = 9001,
                    .source_group_id = 180,
                    .destination_group_id = 181,
                    .split_key = "doc:m",
                    .source_range_end = "doc:z",
                }})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(replica_root_dir, FakeCatalog.iface()),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    var ops = server.localShardOperationAdapter();
    try ops.execute(.{ .prepare_split_source = .{
        .transition_id = 9001,
        .source_group_id = 180,
        .destination_group_id = 181,
        .split_key = "doc:m",
        .source_range_end = "doc:z",
    } });
    try ops.execute(.{ .start_split_source = .{
        .transition_id = 9001,
        .source_group_id = 180,
        .destination_group_id = 181,
    } });
    try ops.execute(.{ .bootstrap_split_destination = .{
        .transition_id = 9001,
        .source_group_id = 180,
        .destination_group_id = 181,
    } });
    try ops.execute(.{ .catch_up_split_destination = .{
        .transition_id = 9001,
        .source_group_id = 180,
        .destination_group_id = 181,
    } });

    var dest = try antfly.db.DB.open(alloc, destination_db_path, .{
        .identity_namespace = source_namespace,
        .start_index_workers = false,
    });
    defer dest.close();

    const replayed = (try dest.get(alloc, "doc:t")) orelse return error.TestUnexpectedResult;
    defer alloc.free(replayed);
    try std.testing.expectEqualStrings("{\"v\":\"right\"}", replayed);

    const stats = try dest.runtimeStatusStatsConsistent(alloc);
    try std.testing.expectEqual(source_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(source_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(source_namespace.range_id, stats.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.allocated_ordinals);
    try std.testing.expect(!stats.doc_identity.rebuild_required);
}

test "data runtime local merge fallback derives receiver identity namespace from catalog" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-merge-identity", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const donor_db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 190);
    defer alloc.free(donor_db_path);
    const receiver_db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 191);
    defer alloc.free(receiver_db_path);

    const donor_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 190,
        .range_id = 9000,
    };
    const old_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 191,
        .range_id = 9001,
    };
    const target_namespace = doc_identity.Namespace{
        .table_id = 7,
        .shard_id = 191,
        .range_id = 9002,
    };

    {
        var db = try antfly.db.DB.open(alloc, donor_db_path, .{
            .identity_namespace = donor_namespace,
            .start_index_workers = false,
        });
        defer db.close();
        try db.updateRange(.{ .start = "doc:m", .end = "doc:z" });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:t", .value = "{\"v\":\"donor\"}" }},
        });
    }

    {
        var db = try antfly.db.DB.open(alloc, receiver_db_path, .{
            .identity_namespace = old_namespace,
            .start_index_workers = false,
        });
        defer db.close();
        try db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" }},
        });
    }

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{
                    .{
                        .group_id = 190,
                        .table_id = 7,
                        .range_id = 9000,
                        .start_key = "",
                        .end_key = "doc:m",
                    },
                    .{
                        .group_id = 191,
                        .table_id = 7,
                        .range_id = 9002,
                        .start_key = "doc:m",
                        .end_key = null,
                    },
                })[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(replica_root_dir, FakeCatalog.iface()),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    var ops = server.localShardOperationAdapter();
    try ops.execute(.{ .accept_merge_receiver = .{
        .transition_id = 9002,
        .donor_group_id = 190,
        .receiver_group_id = 191,
        .allow_doc_identity_reassignment = true,
    } });
    try ops.execute(.{ .catch_up_merge_receiver = .{
        .transition_id = 9002,
        .donor_group_id = 190,
        .receiver_group_id = 191,
        .allow_doc_identity_reassignment = true,
    } });

    var reopened = try antfly.db.DB.open(alloc, receiver_db_path, .{
        .identity_namespace = target_namespace,
        .start_index_workers = false,
    });
    defer reopened.close();

    const replayed = (try reopened.get(alloc, "doc:t")) orelse return error.TestUnexpectedResult;
    defer alloc.free(replayed);
    try std.testing.expectEqualStrings("{\"v\":\"donor\"}", replayed);

    const stats = try reopened.runtimeStatusStatsConsistent(alloc);
    try std.testing.expectEqual(target_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(target_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(target_namespace.range_id, stats.doc_identity.namespace_range_id);

    var txn = try reopened.core.store.beginProbeTxn();
    defer txn.abort();
    const receiver_ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
    const receiver_state = (try doc_identity.lookupStateTxn(&txn, receiver_ordinal)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(target_namespace, "doc:b"), receiver_state.canonical_doc_id);
    const donor_ordinal = (try doc_identity.lookupOrdinalTxn(alloc, &txn, "doc:t")) orelse return error.TestUnexpectedResult;
    const donor_state = (try doc_identity.lookupStateTxn(&txn, donor_ordinal)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(target_namespace, "doc:t"), donor_state.canonical_doc_id);
    try std.testing.expect(receiver_ordinal != donor_ordinal);
}

test "data runtime local split key cache is scoped by root and change generation" {
    const alloc = std.testing.allocator;
    var cache = LocalSplitKeyCache{};
    defer cache.deinit(alloc);

    try cache.put(alloc, 7, 1, 10, "doc:m");

    var hit = (try cache.snapshot(alloc, 7, 1, 10)).?;
    defer hit.deinit(alloc);
    try std.testing.expectEqualStrings("doc:m", hit.split_key.?);

    try std.testing.expect((try cache.snapshot(alloc, 7, 2, 10)) == null);
    try std.testing.expect((try cache.snapshot(alloc, 7, 1, 11)) == null);

    try cache.put(alloc, 7, 1, 11, null);
    var cached_empty = (try cache.snapshot(alloc, 7, 1, 11)).?;
    defer cached_empty.deinit(alloc);
    try std.testing.expect(cached_empty.split_key == null);
}

test "data runtime store status reuses stale cache while refreshing local group status" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-store-status-refresh", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    const tables = [_]antfly.metadata.table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
    }};
    const ranges = [_]antfly.metadata.table_manager.RangeRecord{.{
        .group_id = 77,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    }};
    const stores = [_]antfly.metadata.table_manager.StoreRecord{.{
        .store_id = 19,
        .node_id = 9,
        .live = true,
        .health_class = "healthy",
    }};
    const group_ids = [_]u64{77};
    const fingerprint = localGroupStatusFingerprint(
        &group_ids,
        &tables,
        &ranges,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        null,
        null,
    );

    try server.storeCachedLocalGroupStatuses(1, fingerprint, &.{.{
        .group_id = 77,
        .doc_count = 999,
        .disk_bytes = 123,
        .empty = false,
    }});
    lockAtomic(&server.local_group_status_cache_mutex);
    server.local_group_status_cache.collected_at_ms = 0;
    server.local_group_status_cache_mutex.unlock();
    server.store_status_dirty = false;

    const stale = try server.collectStoreStatusGroupStatusesWithSources(
        alloc,
        replica_root_dir,
        &group_ids,
        &tables,
        &ranges,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        null,
        null,
        null,
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, stale);

    try std.testing.expectEqual(@as(usize, 1), stale.len);
    try std.testing.expectEqual(@as(u64, 999), stale[0].doc_count);
    try std.testing.expect(server.store_status_dirty);
    try std.testing.expectEqual(@as(usize, 1), server.local_group_status_cache.group_statuses.len);
    try std.testing.expectEqual(@as(u64, 0), server.local_group_status_cache.group_statuses[0].doc_count);
    try std.testing.expect(server.local_group_status_cache.collected_at_ms != 0);
}

test "data runtime store status keeps stale cache and skips local group refresh while startup catch-up is active" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-store-status-startup-stale", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    const tables = [_]antfly.metadata.table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
    }};
    const ranges = [_]antfly.metadata.table_manager.RangeRecord{.{
        .group_id = 77,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    }};
    const stores = [_]antfly.metadata.table_manager.StoreRecord{.{
        .store_id = 19,
        .node_id = 9,
        .live = true,
        .health_class = "healthy",
    }};
    const group_ids = [_]u64{77};
    const fingerprint = localGroupStatusFingerprint(
        &group_ids,
        &tables,
        &ranges,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        null,
        null,
    );

    try server.storeCachedLocalGroupStatuses(1, fingerprint, &.{.{
        .group_id = 77,
        .doc_count = 999,
        .disk_bytes = 123,
        .empty = false,
    }});
    lockAtomic(&server.local_group_status_cache_mutex);
    server.local_group_status_cache.collected_at_ms = 0;
    server.local_group_status_cache_mutex.unlock();
    server.provisioned_startup_catch_up_active.store(true, .release);

    const stale = try server.collectStoreStatusGroupStatusesWithSources(
        alloc,
        replica_root_dir,
        &group_ids,
        &tables,
        &ranges,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        null,
        null,
        null,
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, stale);

    try std.testing.expectEqual(@as(usize, 1), stale.len);
    try std.testing.expectEqual(@as(u64, 999), stale[0].doc_count);
    try std.testing.expect(!server.local_group_status_refresh_active.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), server.local_group_status_cache.group_statuses.len);
    try std.testing.expectEqual(@as(u64, 999), server.local_group_status_cache.group_statuses[0].doc_count);
    try std.testing.expectEqual(@as(u64, 0), server.local_group_status_cache.collected_at_ms);
}

test "data runtime live local group status skips the active startup group on a cold cache" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-live-group-status-skip-active-startup", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const docs_db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(docs_db_path);
    const logs_db_path = try std.fmt.allocPrint(alloc, "{s}/group-88/table-db", .{replica_root_dir});
    defer alloc.free(logs_db_path);

    var docs_db = try antfly.db.DB.open(alloc, docs_db_path, .{});
    defer docs_db.close();
    try docs_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    var logs_db = try antfly.db.DB.open(alloc, logs_db_path, .{});
    defer logs_db.close();
    try logs_db.batch(.{
        .writes = &.{.{ .key = "log:a", .value = "{\"title\":\"beta\"}" }},
    });

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    const tables = [_]antfly.metadata.table_manager.TableRecord{
        .{ .table_id = 7, .name = "docs", .placement_role = "data" },
        .{ .table_id = 8, .name = "logs", .placement_role = "data" },
    };
    const ranges = [_]antfly.metadata.table_manager.RangeRecord{
        .{ .group_id = 77, .table_id = 7, .start_key = "", .end_key = null },
        .{ .group_id = 88, .table_id = 8, .start_key = "", .end_key = null },
    };
    const stores = [_]antfly.metadata.table_manager.StoreRecord{
        .{ .store_id = 19, .node_id = 9, .live = true, .health_class = "healthy" },
    };
    const group_ids = [_]u64{ 77, 88 };

    server.provisioned_startup_catch_up_active.store(true, .release);
    try server.setProvisionedStartupCatchUpTarget(77, "docs");
    defer server.clearProvisionedStartupCatchUpTarget();

    const reports = try server.collectLiveLocalGroupStatusesWithSources(
        alloc,
        replica_root_dir,
        &group_ids,
        &tables,
        &ranges,
        null,
        null,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, reports);

    try std.testing.expectEqual(@as(usize, 1), reports.len);
    try std.testing.expectEqual(@as(u64, 88), reports[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), reports[0].doc_count);
}

test "data runtime store status cold miss schedules refresh and returns empty immediately" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-store-status-cold", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    const tables = [_]antfly.metadata.table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
    }};
    const ranges = [_]antfly.metadata.table_manager.RangeRecord{.{
        .group_id = 77,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    }};
    const stores = [_]antfly.metadata.table_manager.StoreRecord{.{
        .store_id = 19,
        .node_id = 9,
        .live = true,
        .health_class = "healthy",
    }};
    const group_ids = [_]u64{77};

    const result = try server.collectStoreStatusGroupStatusesWithSources(
        alloc,
        replica_root_dir,
        &group_ids,
        &tables,
        &ranges,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
        null,
        null,
        null,
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, result);

    try std.testing.expectEqual(@as(usize, 0), result.len);
    try std.testing.expect(server.store_status_dirty);
    try std.testing.expectEqual(@as(usize, 1), server.local_group_status_cache.group_statuses.len);
    try std.testing.expectEqual(@as(u64, 1), server.local_group_status_cache.group_statuses[0].doc_count);
    try std.testing.expect(server.local_group_status_cache.collected_at_ms != 0);
}

test "data runtime metadata local group status provider does not cold-open inline" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-metadata-provider-cold", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    const tables = [_]antfly.metadata.table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
    }};
    const ranges = [_]antfly.metadata.table_manager.RangeRecord{.{
        .group_id = 77,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    }};
    const stores = [_]antfly.metadata.table_manager.StoreRecord{.{
        .store_id = 19,
        .node_id = 9,
        .live = true,
        .health_class = "healthy",
    }};

    const provider = server.localGroupStatusProvider();
    const result = try provider.collect(
        alloc,
        replica_root_dir,
        &tables,
        &ranges,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, result);

    try std.testing.expectEqual(@as(usize, 0), result.len);
    try std.testing.expect(server.store_status_dirty);
    try std.testing.expectEqual(@as(usize, 1), server.local_group_status_cache.group_statuses.len);
    try std.testing.expectEqual(@as(u64, 1), server.local_group_status_cache.group_statuses[0].doc_count);
}

test "data runtime local group refresh prefers runtime status snapshot over DB open" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-local-group-runtime-cache", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    const runtime_indexes = try alloc.alloc(antfly.db.types.DBIndexStats, 1);
    runtime_indexes[0] = .{
        .name = try alloc.dupe(u8, "dense_idx"),
        .kind = .dense_vector,
        .doc_count = 123,
        .replay_applied_sequence = 9,
        .replay_target_sequence = 9,
    };
    const runtime_items = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    runtime_items[0] = .{
        .group_id = 77,
        .disk_bytes = 456,
        .created_at_millis = 789,
        .stats = .{
            .doc_count = 123,
            .index_count = 1,
            .indexes = runtime_indexes,
        },
    };
    const runtime_snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    runtime_snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = runtime_items },
    };
    server.provisioned_storage.runtime_status_cache.replaceOwned(runtime_snapshots);
    alloc.free(runtime_snapshots);

    const tables = [_]antfly.metadata.table_manager.TableRecord{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
    }};
    const ranges = [_]antfly.metadata.table_manager.RangeRecord{.{
        .group_id = 77,
        .table_id = 7,
        .start_key = "",
        .end_key = null,
    }};
    const stores = [_]antfly.metadata.table_manager.StoreRecord{.{
        .store_id = 19,
        .node_id = 9,
        .live = true,
        .health_class = "healthy",
    }};

    const provider = server.localGroupStatusProvider();
    const result = try provider.collect(
        alloc,
        replica_root_dir,
        &tables,
        &ranges,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, result);

    try std.testing.expectEqual(@as(usize, 0), result.len);
    try std.testing.expectEqual(@as(usize, 1), server.local_group_status_cache.group_statuses.len);
    try std.testing.expectEqual(@as(u64, 123), server.local_group_status_cache.group_statuses[0].doc_count);
    try std.testing.expectEqual(@as(u64, 456), server.local_group_status_cache.group_statuses[0].disk_bytes);
    try std.testing.expectEqual(@as(u64, 789), server.local_group_status_cache.group_statuses[0].created_at_millis);
    try std.testing.expect(!server.local_group_status_cache.group_statuses[0].empty);
}

test "data runtime background runtime snapshot warm populates cold status cache without query cache opens" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-runtime-snapshots", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    try server.requestRuntimeStatusRefresh();

    var statuses = (try server.read_source.source().localRuntimeStatuses(alloc, "docs")).?;
    defer statuses.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 77), statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), statuses.items[0].stats.doc_count);
    try std.testing.expectEqual(@as(usize, 0), server.provisioned_storage.read_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), server.runtime_status_refresh_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.runtime_status_refresh_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.runtime_status_refresh_failed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.runtime_status_refresh_last_table_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.runtime_status_refresh_last_group_count.load(.monotonic));
    try std.testing.expect(server.runtime_status_refresh_last_duration_ns.load(.monotonic) > 0);
}

test "data runtime provisioned cache warmup populates only the read cache for local tables" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-provisioned-warmup", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    try std.testing.expectEqual(@as(usize, 0), server.write_source.cachedWriteDbCount());
    try std.testing.expectEqual(@as(usize, 0), server.provisioned_storage.read_cache.entries.items.len);

    try server.requestProvisionedCacheWarmup();

    try std.testing.expectEqual(@as(usize, 0), server.write_source.cachedWriteDbCount());
    try std.testing.expectEqual(@as(usize, 1), server.provisioned_storage.read_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 77), server.provisioned_storage.read_cache.entries.items[0].group_id);
    try std.testing.expectEqualStrings("docs", server.provisioned_storage.read_cache.entries.items[0].table_name);
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_warmup_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_warmup_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_warmup_failed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_warmup_last_group_count.load(.monotonic));
    try std.testing.expect(server.provisioned_warmup_last_duration_ns.load(.monotonic) > 0);
    const warmed_read_cache = server.provisioned_storage.read_cache.cacheStats();
    try std.testing.expectEqual(@as(u64, 0), warmed_read_cache.hit_count);
    try std.testing.expectEqual(@as(u64, 1), warmed_read_cache.miss_count);
    const warmed_write_cache = server.provisioned_storage.write_cache.cacheStats();
    try std.testing.expectEqual(@as(u64, 0), warmed_write_cache.hit_count);
    try std.testing.expectEqual(@as(u64, 0), warmed_write_cache.miss_count);

    var snapshot_statuses = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer snapshot_statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot_statuses.items.len);
    try std.testing.expectEqual(@as(u64, 77), snapshot_statuses.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), snapshot_statuses.items[0].stats.doc_count);

    var warmed_lookup = (try server.read_source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer warmed_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, warmed_lookup.json, "\"alpha\"") != null);
    const post_lookup_read_cache = server.provisioned_storage.read_cache.cacheStats();
    try std.testing.expectEqual(@as(u64, 1), post_lookup_read_cache.hit_count);
    try std.testing.expectEqual(@as(u64, 1), post_lookup_read_cache.miss_count);

    _ = try server.write_source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
        .sync_level = .write,
    });
    const post_batch_write_cache = server.provisioned_storage.write_cache.cacheStats();
    try std.testing.expectEqual(@as(u64, 1), post_batch_write_cache.hit_count);
    try std.testing.expectEqual(@as(u64, 1), post_batch_write_cache.miss_count);

    const pre_live_lookup_read_cache = server.provisioned_storage.read_cache.cacheStats();
    var live_lookup = (try server.read_source.source().lookup(alloc, "docs", "doc:b", .{}, .read_index)).?;
    defer live_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, live_lookup.json, "\"beta\"") != null);
    const post_live_lookup_read_cache = server.provisioned_storage.read_cache.cacheStats();
    try std.testing.expectEqual(pre_live_lookup_read_cache.hit_count, post_live_lookup_read_cache.hit_count);
    try std.testing.expectEqual(pre_live_lookup_read_cache.miss_count, post_live_lookup_read_cache.miss_count);
}

test "data runtime provisioned cache warmup defers while startup catch-up is active" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-warmup-runs-during-startup-catch-up", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const docs_db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(docs_db_path);

    var db = try antfly.db.DB.open(alloc, docs_db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{ .group_id = 77, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{ .store_id = 19, .node_id = 9, .role = "data", .live = true, .health_class = "healthy" }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{ .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 }, .store_id = 19, .peer_node_ids = &.{} }})[0..]),
                .merged_group_statuses = @constCast((&[_]antfly.metadata.reconciler.MergedGroupStatus{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
                .split_observations = @constCast((&[_]antfly.metadata.transition_state.SplitObservationRecord{})[0..]),
                .merge_observations = @constCast((&[_]antfly.metadata.transition_state.MergeObservationRecord{})[0..]),
                .restore_progresses = @constCast((&[_]antfly.metadata.table_manager.RestoreProgressRecord{})[0..]),
                .shuffle_join_leases = @constCast((&[_]antfly.metadata.table_manager.ShuffleJoinLeaseRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);
    server.provisioned_startup_catch_up_active.store(true, .release);

    const stats = server.runProvisionedCacheWarmup();
    try std.testing.expectEqual(@as(u64, 0), stats.warmed_group_count);
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_warmup_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_warmup_completed.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), server.provisioned_storage.read_cache.entries.items.len);
}

test "data runtime status refresh preserves only the active catch-up group while refreshing unrelated tables" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-preserve-live-progress", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const docs_db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(docs_db_path);
    const logs_db_path = try std.fmt.allocPrint(alloc, "{s}/group-88/table-db", .{replica_root_dir});
    defer alloc.free(logs_db_path);

    var docs_db = try antfly.db.DB.open(alloc, docs_db_path, .{});
    defer docs_db.close();
    try docs_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    var logs_db = try antfly.db.DB.open(alloc, logs_db_path, .{});
    defer logs_db.close();
    try logs_db.batch(.{
        .writes = &.{
            .{ .key = "log:a", .value = "{\"title\":\"beta\"}" },
            .{ .key = "log:b", .value = "{\"title\":\"gamma\"}" },
        },
        .sync_level = .write,
    });

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{ .{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }, .{
                    .table_id = 8,
                    .name = "logs",
                    .placement_role = "data",
                } })[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{ .{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }, .{
                    .group_id = 88,
                    .table_id = 8,
                    .start_key = "",
                    .end_key = null,
                } })[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    const indexes = try alloc.alloc(antfly.db.types.DBIndexStats, 1);
    indexes[0] = .{
        .name = try alloc.dupe(u8, "dv_v1"),
        .kind = .dense_vector,
        .doc_count = 787500,
        .node_count = 7131,
        .replay_applied_sequence = 993,
        .replay_target_sequence = 1001001,
        .replay_catch_up_required = true,
    };
    const statuses = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    statuses[0] = .{
        .group_id = 77,
        .stats = .{
            .doc_count = 787500,
            .index_count = 1,
            .indexes = indexes,
        },
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = statuses },
    };
    const stale_logs_indexes = try alloc.alloc(antfly.db.types.DBIndexStats, 1);
    stale_logs_indexes[0] = .{
        .name = try alloc.dupe(u8, "kw_v1"),
        .kind = .full_text,
        .doc_count = 0,
    };
    const stale_logs_statuses = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    stale_logs_statuses[0] = .{
        .group_id = 88,
        .stats = .{
            .doc_count = 0,
            .index_count = 1,
            .indexes = stale_logs_indexes,
        },
    };
    const refreshed_snapshots = try alloc.realloc(snapshots, 2);
    defer alloc.free(refreshed_snapshots);
    refreshed_snapshots[1] = .{
        .table_name = try alloc.dupe(u8, "logs"),
        .statuses = .{ .items = stale_logs_statuses },
    };
    server.provisioned_storage.runtime_status_cache.replaceOwned(refreshed_snapshots);

    server.provisioned_startup_catch_up_active.store(true, .release);
    try server.setProvisionedStartupCatchUpTarget(77, "docs");
    defer server.clearProvisionedStartupCatchUpTarget();
    try server.requestRuntimeStatusRefresh();

    var docs_cached = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer docs_cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), docs_cached.items.len);
    try std.testing.expectEqual(@as(u64, 993), docs_cached.items[0].stats.indexes[0].replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 1001001), docs_cached.items[0].stats.indexes[0].replay_target_sequence);
    try std.testing.expect(docs_cached.items[0].stats.indexes[0].replay_catch_up_required);

    try std.testing.expect((try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "logs")) == null);
}

test "data runtime status refresh skips opening the active startup group when no cached snapshot exists yet" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-skip-active-startup-open", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const docs_db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(docs_db_path);
    const logs_db_path = try std.fmt.allocPrint(alloc, "{s}/group-88/table-db", .{replica_root_dir});
    defer alloc.free(logs_db_path);

    var docs_db = try antfly.db.DB.open(alloc, docs_db_path, .{});
    defer docs_db.close();
    try docs_db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    var logs_db = try antfly.db.DB.open(alloc, logs_db_path, .{});
    defer logs_db.close();
    try logs_db.batch(.{
        .writes = &.{
            .{ .key = "log:a", .value = "{\"title\":\"beta\"}" },
            .{ .key = "log:b", .value = "{\"title\":\"gamma\"}" },
        },
        .sync_level = .write,
    });

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{ .{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }, .{
                    .table_id = 8,
                    .name = "logs",
                    .placement_role = "data",
                } })[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{ .{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }, .{
                    .group_id = 88,
                    .table_id = 8,
                    .start_key = "",
                    .end_key = null,
                } })[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    server.provisioned_startup_catch_up_active.store(true, .release);
    try server.setProvisionedStartupCatchUpTarget(77, "docs");
    defer server.clearProvisionedStartupCatchUpTarget();

    try server.requestRuntimeStatusRefresh();

    try std.testing.expectEqual(@as(u64, 0), server.runtime_status_refresh_failed.load(.monotonic));
    try std.testing.expect((try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")) == null);
    try std.testing.expect((try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "logs")) == null);
}

test "data runtime status refresh publishes synthetic missing status for absent local group db" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-missing-placeholder", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const indexes_json = "{\"indexes\":[{\"name\":\"search_idx\",\"type\":\"full_text\",\"config\":{}}]}";

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    const stats = server.runRuntimeStatusRefreshWithBudget(8);
    try std.testing.expectEqual(@as(u64, 1), stats.table_count);
    try std.testing.expectEqual(@as(u64, 1), stats.group_count);
    try std.testing.expectEqual(@as(u64, 0), stats.db_opens);
    try std.testing.expectEqual(@as(u64, 0), stats.skipped_db_opens);
    try std.testing.expectEqual(@as(u64, 1), stats.placeholder_group_count);

    var docs_cached = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer docs_cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), docs_cached.items.len);
    try std.testing.expectEqual(@as(u64, 77), docs_cached.items[0].group_id);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.synthetic_config, docs_cached.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.stale, docs_cached.items[0].metadata.freshness);
    try std.testing.expectEqual(@as(usize, 1), docs_cached.items[0].stats.indexes.len);
    try std.testing.expectEqualStrings("search_idx", docs_cached.items[0].stats.indexes[0].name);
}

test "data runtime status refresh budget reuses cached group status instead of opening db" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-budget-cached", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    const statuses = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    statuses[0] = .{
        .group_id = 77,
        .metadata = .{
            .source = .live_writer_publish,
            .freshness = .fresh,
        },
        .stats = .{
            .doc_count = 123,
        },
    };
    const snapshots = try alloc.alloc(runtime_status.TableRuntimeSnapshot, 1);
    snapshots[0] = .{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = statuses },
    };
    server.provisioned_storage.runtime_status_cache.replaceOwned(snapshots);
    alloc.free(snapshots);

    const stats = server.runRuntimeStatusRefreshWithBudget(0);
    try std.testing.expectEqual(@as(u64, 1), stats.table_count);
    try std.testing.expectEqual(@as(u64, 1), stats.group_count);
    try std.testing.expectEqual(@as(u64, 0), stats.db_opens);
    try std.testing.expectEqual(@as(u64, 0), stats.skipped_db_opens);
    try std.testing.expectEqual(@as(u64, 0), stats.placeholder_group_count);

    var docs_cached = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer docs_cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), docs_cached.items.len);
    try std.testing.expectEqual(@as(u64, 123), docs_cached.items[0].stats.doc_count);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.cached_snapshot, docs_cached.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.stale, docs_cached.items[0].metadata.freshness);
}

test "data runtime status refresh reuses managed writer snapshot instead of reopening table db" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const fake_replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-fake-root", .{tmp.sub_path});
    defer alloc.free(fake_replica_root_dir);
    const actual_replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-live-cache-root", .{tmp.sub_path});
    defer alloc.free(actual_replica_root_dir);
    const actual_db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{actual_replica_root_dir});
    defer alloc.free(actual_db_path);

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var write_cache = antfly.public_api.ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            fake_replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            fake_replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);
    server.write_source.write_cache = &write_cache;

    {
        lockAtomic(server.write_source.localDbMutex());
        defer server.write_source.localDbMutex().unlock();
        var cached = try write_cache.getOrOpenLocked(actual_db_path, FakeCatalog.iface(), 77, 0, "docs");
        defer cached.deinit(alloc);
        try cached.db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
        try server.provisioned_storage.runtime_status_cache.upsertGroupStatus("docs", .{
            .group_id = 77,
            .stats = try cached.db.stats(alloc),
        });
        try cached.db.batch(.{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
            .sync_level = .write,
        });
    }

    server.write_source.testingMarkTableRequestActive("docs");

    try server.requestRuntimeStatusRefresh();

    var docs_cached = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer docs_cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), docs_cached.items.len);
    try std.testing.expectEqual(@as(u64, 0), docs_cached.items[0].stats.doc_count);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.live_writer_publish, docs_cached.items[0].metadata.source);
    try std.testing.expectEqual(@as(u64, 19), docs_cached.items[0].metadata.store_id);
    try std.testing.expectEqual(@as(u64, 9), docs_cached.items[0].metadata.node_id);
}

test "data runtime status refresh falls back to live managed writer status when cache entry is missing" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-live-managed-writer-fallback", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var write_cache = antfly.public_api.ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);
    server.write_source.write_cache = &write_cache;

    {
        lockAtomic(server.write_source.localDbMutex());
        defer server.write_source.localDbMutex().unlock();
        var cached = try write_cache.getOrOpenLocked(db_path, FakeCatalog.iface(), 77, 0, "docs");
        defer cached.deinit(alloc);
        try cached.db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
    }

    try std.testing.expect((try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")) == null);

    try server.requestRuntimeStatusRefresh();

    var docs_cached = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer docs_cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), docs_cached.items.len);
    try std.testing.expectEqual(@as(u64, 77), docs_cached.items[0].group_id);
    try std.testing.expectEqual(@as(u64, 1), docs_cached.items[0].stats.doc_count);
}

test "data runtime status refresh publishes placeholder when live managed writer is busy and cache entry is missing" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-live-managed-writer-busy-fallback", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var write_cache = antfly.public_api.ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);
    server.write_source.write_cache = &write_cache;

    {
        lockAtomic(server.write_source.localDbMutex());
        defer server.write_source.localDbMutex().unlock();
        var cached = try write_cache.getOrOpenLocked(db_path, FakeCatalog.iface(), 77, 0, "docs");
        defer cached.deinit(alloc);
        try cached.db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
    }

    try std.testing.expect((try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")) == null);

    lockAtomic(server.write_source.localDbMutex());
    defer server.write_source.localDbMutex().unlock();
    try server.requestRuntimeStatusRefresh();

    var docs_cached = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer docs_cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), docs_cached.items.len);
    try std.testing.expectEqual(@as(u64, 77), docs_cached.items[0].group_id);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.synthetic_config, docs_cached.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.stale, docs_cached.items[0].metadata.freshness);
    try std.testing.expect(!runtime_status.statusHasRuntimeFacts(docs_cached.items[0]));
    try std.testing.expectEqual(@as(u64, 0), docs_cached.items[0].stats.doc_count);
}

test "data local group status refresh skips active group when cache entry is missing" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-active-group-live-fallback", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{});
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    server.write_source.testingMarkGroupOperationActive("docs", 77);

    const tables = [_]antfly.metadata.table_manager.TableRecord{
        .{ .table_id = 7, .name = "docs", .placement_role = "data" },
    };
    const ranges = [_]antfly.metadata.table_manager.RangeRecord{
        .{ .group_id = 77, .table_id = 7, .start_key = "", .end_key = null },
    };
    const stores = [_]antfly.metadata.table_manager.StoreRecord{
        .{ .store_id = 19, .node_id = 9, .live = true, .health_class = "healthy" },
    };
    const group_ids = [_]u64{77};

    const reports = try server.collectLiveLocalGroupStatusesWithSources(
        alloc,
        replica_root_dir,
        &group_ids,
        &tables,
        &ranges,
        null,
        null,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatuses(alloc, reports);

    try std.testing.expectEqual(@as(usize, 0), reports.len);
}

test "data runtime status refresh publishes sibling placeholder when only one group has managed writer" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-refresh-mixed-managed-writer", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const group_77_db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(group_77_db_path);
    const group_78_db_path = try std.fmt.allocPrint(alloc, "{s}/group-78/table-db", .{replica_root_dir});
    defer alloc.free(group_78_db_path);

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{
                    .{
                        .group_id = 77,
                        .table_id = 7,
                        .start_key = "",
                        .end_key = "m",
                    },
                    .{
                        .group_id = 78,
                        .table_id = 7,
                        .start_key = "m",
                        .end_key = null,
                    },
                })[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{
                    .{
                        .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                        .store_id = 19,
                        .peer_node_ids = &.{9},
                    },
                    .{
                        .record = .{ .group_id = 78, .replica_id = 1, .local_node_id = 9 },
                        .store_id = 19,
                        .peer_node_ids = &.{9},
                    },
                })[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var write_cache = antfly.public_api.ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);
    server.write_source.write_cache = &write_cache;

    {
        lockAtomic(server.write_source.localDbMutex());
        defer server.write_source.localDbMutex().unlock();

        var cached = try write_cache.getOrOpenLocked(group_77_db_path, FakeCatalog.iface(), 77, 0, "docs");
        defer cached.deinit(alloc);
        try cached.db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
        try server.provisioned_storage.runtime_status_cache.upsertGroupStatus("docs", .{
            .group_id = 77,
            .stats = try cached.db.stats(alloc),
        });
    }

    {
        var db = try antfly.db.DB.open(alloc, group_78_db_path, .{});
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"zeta\"}" }},
            .sync_level = .write,
        });
    }

    try server.requestRuntimeStatusRefresh();

    var docs_cached = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer docs_cached.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), docs_cached.items.len);

    var saw_77 = false;
    var saw_78 = false;
    for (docs_cached.items) |status| {
        if (status.group_id == 77 and status.stats.doc_count == 1) saw_77 = true;
        if (status.group_id == 78 and
            status.metadata.source == .synthetic_config and
            !runtime_status.statusHasRuntimeFacts(status) and
            status.stats.doc_count == 0) saw_78 = true;
    }
    try std.testing.expect(saw_77);
    try std.testing.expect(saw_78);
}

test "data runtime provisioned startup catch-up clears replay debt for local groups" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-startup-catch-up", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);
    const indexes_json = "{\"dv_v1\":{\"type\":\"embeddings\",\"field\":\"embedding\",\"dims\":2}}";

    var appended_sequence: u64 = 0;
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try antfly.db.internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try antfly.db.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        const payload = try antfly.db.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 0 });
        defer alloc.free(payload);
        try db.core.store.put(artifact_key, payload);

        var dense_embeddings = try alloc.alloc(antfly.db.derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = antfly.db.derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer antfly.db.derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.nextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try db.core.store.appendReplayOpaque(alloc, appended_sequence, encoded);
    }

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{.{
                    .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                    .store_id = 19,
                    .peer_node_ids = &.{9},
                }})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    {
        var before = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer before.close();
        const stats = try before.stats(alloc);
        defer antfly.db.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_target_sequence);
        try std.testing.expect(stats.indexes[0].replay_catch_up_required);
    }

    const FalseLeader = struct {
        fn iface() GroupLeadershipSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .is_local_leader = isLocalLeader,
                },
            };
        }

        fn isLocalLeader(_: *anyopaque, _: u64) bool {
            return false;
        }
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .group_leadership_source = FalseLeader.iface(),
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    try server.requestRuntimeStatusRefresh();
    try std.testing.expect(server.provisioned_startup_catch_up_dirty.load(.monotonic));

    try server.requestProvisionedStartupCatchUp();

    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_failed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_groups_cleared.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_busy_groups.load(.monotonic));
    try std.testing.expect(!server.provisioned_startup_catch_up_dirty.load(.monotonic));

    {
        var after = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer after.close();
        const stats = try after.stats(alloc);
        defer antfly.db.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_target_sequence);
        try std.testing.expect(!stats.indexes[0].replay_catch_up_required);
    }
}

test "data runtime startup catch-up retries unresolved leadership and later clears debt" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-startup-catch-up-retry-leadership", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);
    const indexes_json = "{\"indexes\":[{\"name\":\"dv_v1\",\"type\":\"embeddings\",\"config\":{\"field\":\"embedding\",\"dims\":2}}]}";

    var appended_sequence: u64 = 0;
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });

        const stored_key = try antfly.db.internal_keys.documentKeyAlloc(alloc, "doc:a");
        defer alloc.free(stored_key);
        try db.core.store.putBatch(&.{
            .{ .key = stored_key, .value = "{\"title\":\"alpha\"}" },
        }, &.{});

        const artifact_key = try antfly.db.internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        const payload = try antfly.db.enrichment_artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &[_]f32{ 1, 0 });
        defer alloc.free(payload);
        try db.core.store.put(artifact_key, payload);

        var dense_embeddings = try alloc.alloc(antfly.db.derived_types.DerivedDenseEmbeddingWrite, 1);
        var batch = antfly.db.derived_types.DerivedBatch{
            .dense_embeddings = dense_embeddings,
        };
        defer antfly.db.derived_types.deinitDerivedBatch(alloc, &batch);
        dense_embeddings[0] = .{
            .index_name = try alloc.dupe(u8, "dv_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .vector = try alloc.dupe(f32, &[_]f32{ 1, 0 }),
        };

        appended_sequence = db.core.store.nextReplaySequence(1);
        var record = try change_journal_mod.recordFromDerivedBatch(alloc, batch, appended_sequence);
        defer change_journal_mod.deinitRecord(alloc, &record);
        const encoded = try change_journal_mod.encodeRecord(alloc, record);
        defer alloc.free(encoded);
        try db.core.store.appendReplayOpaque(alloc, appended_sequence, encoded);
    }

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = indexes_json,
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{
                    .{
                        .store_id = 19,
                        .node_id = 9,
                        .role = "data",
                        .live = true,
                        .health_class = "healthy",
                    },
                    .{
                        .store_id = 20,
                        .node_id = 10,
                        .role = "data",
                        .live = true,
                        .health_class = "healthy",
                    },
                })[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{
                    .{
                        .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                        .store_id = 19,
                        .peer_node_ids = &.{ 9, 10 },
                    },
                    .{
                        .record = .{ .group_id = 77, .replica_id = 2, .local_node_id = 10 },
                        .store_id = 20,
                        .peer_node_ids = &.{ 9, 10 },
                    },
                })[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast((&[_]antfly.metadata.reconciler.MergedGroupStatus{.{
                    .group_id = 77,
                    .leader_known = false,
                }})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const LeadershipHarness = struct {
        local_leader: bool,

        fn iface(self: *@This()) GroupLeadershipSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .is_local_leader = isLocalLeader,
                },
            };
        }

        fn isLocalLeader(ptr: *anyopaque, _: u64) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.local_leader;
        }
    };

    {
        var before = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer before.close();
        const stats = try before.stats(alloc);
        defer antfly.db.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_target_sequence);
        try std.testing.expect(stats.indexes[0].replay_catch_up_required);
    }

    var leadership = LeadershipHarness{ .local_leader = false };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .group_leadership_source = leadership.iface(),
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    _ = server.runProvisionedStartupCatchUp();

    try std.testing.expect(server.provisioned_startup_catch_up_dirty.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_groups_cleared.load(.monotonic));

    {
        var mid = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer mid.close();
        const stats = try mid.stats(alloc);
        defer antfly.db.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 0), stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_target_sequence);
        try std.testing.expect(stats.indexes[0].replay_catch_up_required);
    }

    leadership.local_leader = true;
    _ = server.runProvisionedStartupCatchUp();

    try std.testing.expectEqual(@as(u64, 2), server.provisioned_startup_catch_up_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 2), server.provisioned_startup_catch_up_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_groups_cleared.load(.monotonic));
    try std.testing.expect(!server.provisioned_startup_catch_up_dirty.load(.monotonic));

    {
        var after = try antfly.db.DB.open(alloc, db_path, .{
            .open_mode = .query_readonly,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer after.close();
        const stats = try after.stats(alloc);
        defer antfly.db.types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(usize, 1), stats.indexes.len);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_applied_sequence);
        try std.testing.expectEqual(appended_sequence, stats.indexes[0].replay_target_sequence);
        try std.testing.expect(!stats.indexes[0].replay_catch_up_required);
    }
}

test "data runtime startup catch-up stays dirty when metadata snapshot is unavailable" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-startup-catch-up-no-snapshot", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const EmptyCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const NoSnapshotStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            EmptyCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            EmptyCatalog.iface(),
        ),
        .status_source = NoSnapshotStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    _ = server.runProvisionedStartupCatchUp();

    try std.testing.expect(server.provisioned_startup_catch_up_dirty.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
}

test "data runtime keeps status refresh dirty for non-startup async index work" {
    var catch_up_indexes = [_]antfly.db.types.DBIndexStats{.{
        .name = "full_text_index_v0",
        .kind = .full_text,
        .catch_up_active = true,
    }};
    const catch_up_statuses = [_]runtime_status.LocalTableRuntimeStatus{.{
        .stats = .{
            .indexes = catch_up_indexes[0..],
            .index_count = 1,
        },
    }};
    try std.testing.expect(DataServer.runtimeStatusWorkPending(catch_up_statuses[0..]));
    try std.testing.expect(!DataServer.runtimeStatusStartupCatchUpDebtPresent(catch_up_statuses[0..]));

    const bulk_statuses = [_]runtime_status.LocalTableRuntimeStatus{.{
        .stats = .{
            .async_indexing = .{
                .bulk_coalescing = .{ .active_session = true },
            },
        },
    }};
    try std.testing.expect(DataServer.runtimeStatusWorkPending(bulk_statuses[0..]));
    try std.testing.expect(!DataServer.runtimeStatusStartupCatchUpDebtPresent(bulk_statuses[0..]));
}

test "data runtime defers replica-root reconcile only for unresolved startup debt on known metadata" {
    var server: DataServer = .{
        .alloc = std.testing.allocator,
        .provisioned_storage = undefined,
        .read_source = undefined,
        .write_source = undefined,
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };

    server.provisioned_startup_catch_up_active.store(false, .monotonic);
    server.provisioned_startup_catch_up_dirty.store(true, .monotonic);
    server.last_provision_metadata_epoch = null;
    server.last_provision_fingerprint = null;
    try std.testing.expect(!server.shouldDeferProvisionedReplicaRootReconcile());

    server.last_provision_metadata_epoch = 17;
    try std.testing.expect(server.shouldDeferProvisionedReplicaRootReconcile());

    server.last_provision_fingerprint = 99;
    try std.testing.expect(!server.shouldDeferProvisionedReplicaRootReconcile());

    server.provisioned_startup_catch_up_active.store(true, .monotonic);
    try std.testing.expect(server.shouldDeferProvisionedReplicaRootReconcile());
}

test "data runtime data changes mark provisioned startup catch-up dirty" {
    var server: DataServer = .{
        .alloc = std.testing.allocator,
        .provisioned_storage = undefined,
        .read_source = undefined,
        .write_source = undefined,
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };

    server.runtime_status_dirty.store(false, .release);
    server.provisioned_startup_catch_up_dirty.store(false, .release);

    DataServer.onLocalTableChanged(&server, "docs", .data);

    try std.testing.expect(server.runtime_status_dirty.load(.acquire));
    try std.testing.expect(server.provisioned_startup_catch_up_dirty.load(.acquire));
}

test "data runtime structural changes preserve writer-published runtime status" {
    const alloc = std.testing.allocator;
    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = undefined,
        .write_source = undefined,
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.provisioned_storage.deinit();

    try server.provisioned_storage.runtime_status_cache.upsertGroupStatus("docs", .{
        .group_id = 77,
        .stats = .{ .doc_count = 2, .indexes = &.{} },
    });

    server.runtime_status_dirty.store(false, .release);
    server.provisioned_startup_catch_up_dirty.store(false, .release);
    server.markRuntimeStatusDirty("docs", .structural);

    try std.testing.expect(server.runtime_status_dirty.load(.acquire));
    try std.testing.expect(!server.provisioned_startup_catch_up_dirty.load(.acquire));
    var statuses = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expectEqual(@as(u64, 2), statuses.items[0].stats.doc_count);
}

test "data runtime startup catch-up prefers cached admin snapshot" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-cached-startup-snapshot", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const SnapshotSource = struct {
        cached_calls: usize = 0,
        admin_calls: usize = 0,

        fn iface(self: *@This()) antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .cached_admin_snapshot = cachedAdminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(ptr: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.admin_calls += 1;
            return error.Unexpected;
        }

        fn cachedAdminSnapshot(ptr: *anyopaque) !?antfly.metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cached_calls += 1;
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                    .indexes_json = "{\"dense_idx\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}}",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 7001,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{.{
                    .store_id = 19,
                    .node_id = 9,
                    .role = "data",
                    .live = true,
                    .health_class = "healthy",
                }})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var snapshot_source = SnapshotSource{};
    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = snapshot_source.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    _ = server.runProvisionedStartupCatchUp();

    try std.testing.expectEqual(@as(usize, 1), snapshot_source.cached_calls);
    try std.testing.expectEqual(@as(usize, 0), snapshot_source.admin_calls);
}

test "data runtime runRound does not refresh provisioned replica root inline while worker is active" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-provision-active", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const remote_metadata = try alloc.create(RemoteMetadataSource);
    const metadata_api_urls = [_][]const u8{"http://127.0.0.1:1"};
    remote_metadata.* = try RemoteMetadataSource.init(alloc, &metadata_api_urls);

    var server: DataServer = .{
        .alloc = alloc,
        .remote_metadata = remote_metadata,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    server.store_status_dirty = false;
    server.runtime_status_dirty.store(false, .release);
    server.provisioned_startup_catch_up_dirty.store(false, .release);
    server.provisioned_root_refresh_dirty.store(true, .release);
    server.provisioned_root_refresh_active.store(true, .release);
    server.provision_ticks = 3;

    try server.runRound();

    try std.testing.expectEqual(@as(usize, 0), server.provision_ticks);
    try std.testing.expect(server.provisioned_root_refresh_dirty.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_root_refresh_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.last_provision_head_check_at_ms);
}

test "data runtime provisioned root refresh spawn failure preserves retry bookkeeping" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-provision-spawn-failure", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    const FailingSpawner = struct {
        fn run(_: *DataServer) !std.Thread {
            return error.ThreadQuotaExceeded;
        }
    };

    server.provisioned_root_refresh_dirty.store(true, .release);
    try std.testing.expectError(
        error.ThreadQuotaExceeded,
        server.requestProvisionedRootRefreshWithSpawner(FailingSpawner.run),
    );
    try std.testing.expect(server.provisioned_root_refresh_dirty.load(.acquire));
    try std.testing.expect(!server.provisioned_root_refresh_active.load(.acquire));
    try std.testing.expect(server.provisioned_root_refresh_thread == null);
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_root_refresh_last_run_at_ms.load(.monotonic));
}

test "data runtime startup catch-up stays dirty when local groups are not visible yet" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-startup-catch-up-no-local-groups", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{
                    .{
                        .store_id = 19,
                        .node_id = 9,
                        .role = "data",
                        .live = true,
                        .health_class = "healthy",
                    },
                    .{
                        .store_id = 20,
                        .node_id = 10,
                        .role = "data",
                        .live = true,
                        .health_class = "healthy",
                    },
                })[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    _ = server.runProvisionedStartupCatchUp();

    try std.testing.expect(server.provisioned_startup_catch_up_dirty.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
}

test "data runtime startup catch-up stays dirty when local leadership is unresolved" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-startup-catch-up-leadership-unresolved", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 7,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 77,
                    .table_id = 7,
                    .start_key = "",
                    .end_key = null,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{
                    .{
                        .store_id = 19,
                        .node_id = 9,
                        .role = "data",
                        .live = true,
                        .health_class = "healthy",
                    },
                    .{
                        .store_id = 20,
                        .node_id = 10,
                        .role = "data",
                        .live = true,
                        .health_class = "healthy",
                    },
                })[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{
                    .{
                        .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 9 },
                        .store_id = 19,
                        .peer_node_ids = &.{ 9, 10 },
                    },
                    .{
                        .record = .{ .group_id = 77, .replica_id = 2, .local_node_id = 10 },
                        .store_id = 20,
                        .peer_node_ids = &.{ 9, 10 },
                    },
                })[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
                .merged_group_statuses = @constCast((&[_]antfly.metadata.reconciler.MergedGroupStatus{.{
                    .group_id = 77,
                    .leader_known = false,
                }})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FalseLeader = struct {
        fn iface() GroupLeadershipSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .is_local_leader = isLocalLeader,
                },
            };
        }

        fn isLocalLeader(_: *anyopaque, _: u64) bool {
            return false;
        }
    };

    var server: DataServer = .{
        .alloc = alloc,
        .store_registration = .{
            .node_id = 9,
            .store_id = 19,
            .role = "data",
            .failure_domain = "test",
        },
        .group_leadership_source = FalseLeader.iface(),
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    _ = server.runProvisionedStartupCatchUp();

    try std.testing.expect(server.provisioned_startup_catch_up_dirty.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
}

test "data runtime startup catch-up spawn failure preserves retry bookkeeping" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-startup-spawn-failure", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    server.provisioned_startup_catch_up_last_run_at_ms.store(77, .monotonic);

    const FailingSpawner = struct {
        fn run(_: *DataServer) !std.Thread {
            return error.ThreadQuotaExceeded;
        }
    };

    try std.testing.expectError(
        error.ThreadQuotaExceeded,
        server.requestProvisionedStartupCatchUpWithSpawner(FailingSpawner.run),
    );
    try std.testing.expectEqual(@as(u64, 77), server.provisioned_startup_catch_up_last_run_at_ms.load(.monotonic));
    try std.testing.expect(!server.provisioned_startup_catch_up_active.load(.acquire));
    try std.testing.expect(server.provisioned_startup_catch_up_thread == null);
}

test "data runtime local group status reflects active transition readiness" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-transition-db", .{tmp.sub_path});
    defer std.testing.allocator.free(db_path);

    var db = try antfly.db.DB.open(std.testing.allocator, db_path, .{});
    db.close();

    const report = try collectLocalGroupStatus(std.testing.allocator, db_path, null, 88, .{}, null, null, &.{}, &.{}, &[_]antfly.metadata.SplitTransitionRecord{.{
        .transition_id = 88001,
        .source_group_id = 77,
        .destination_group_id = 88,
        .phase = .cutover_pending,
        .split_key = "doc:m",
    }}, &.{}, &.{}, &.{});
    defer antfly.metadata.table_manager.freeGroupStatus(std.testing.allocator, report);
    try std.testing.expect(report.transition_pending);
    try std.testing.expect(report.replay_required);
    try std.testing.expect(report.replay_caught_up);
    try std.testing.expect(report.cutover_ready);
    try std.testing.expect(report.reads_ready_after_cutover);
}

test "data runtime local group status uses metadata transition observation when local pair is absent" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-metadata-observation", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-88/table-db", .{replica_root_dir});
    defer std.testing.allocator.free(db_path);

    var db = try antfly.db.DB.open(std.testing.allocator, db_path, .{});
    db.close();

    const report = try collectLocalGroupStatus(
        std.testing.allocator,
        db_path,
        replica_root_dir,
        88,
        .{},
        null,
        null,
        &.{},
        &.{},
        &[_]antfly.metadata.SplitTransitionRecord{.{
            .transition_id = 88011,
            .source_group_id = 77,
            .destination_group_id = 88,
            .phase = .bootstrap_peer,
            .split_key = "doc:m",
        }},
        &.{},
        &[_]antfly.metadata.transition_state.SplitObservationRecord{.{
            .transition_id = 88011,
            .observation = .{
                .status = .{
                    .phase = .cutover_ready,
                    .source_split_phase = .splitting,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 7,
                    .dest_delta_sequence = 7,
                },
            },
        }},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatus(std.testing.allocator, report);
    try std.testing.expect(report.transition_pending);
    try std.testing.expect(report.replay_required);
    try std.testing.expect(report.replay_caught_up);
    try std.testing.expect(report.cutover_ready);
    try std.testing.expect(report.reads_ready_after_cutover);
}

test "data runtime local group status uses injected membership source" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-membership-db", .{tmp.sub_path});
    defer std.testing.allocator.free(db_path);

    var db = try antfly.db.DB.open(std.testing.allocator, db_path, .{});
    db.close();

    const Source = struct {
        fn iface() GroupMembershipSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .membership = membership,
                },
            };
        }

        fn membership(_: *anyopaque, group_id: u64) GroupMembership {
            return .{
                .local_voter = group_id == 99,
                .voter_count = if (group_id == 99) 3 else 0,
                .joint_consensus = false,
            };
        }
    };

    const report = try collectLocalGroupStatus(std.testing.allocator, db_path, null, 99, .{}, null, Source.iface(), &.{}, &.{}, &.{}, &.{}, &.{}, &.{});
    defer antfly.metadata.table_manager.freeGroupStatus(std.testing.allocator, report);
    try std.testing.expect(report.local_voter);
    try std.testing.expectEqual(@as(u16, 3), report.voter_count);
}

test "data runtime infers local leader from single local placement" {
    const stores = [_]antfly.metadata.StoreRecord{
        .{ .store_id = 19, .node_id = 9 },
    };
    const intents = [_]antfly.raft.reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 7701, .replica_id = 1, .local_node_id = 9 },
            .store_id = 19,
            .peer_node_ids = &.{},
        },
    };
    var source = InferredSnapshotLeadershipSource.init(9, 19, stores[0..], &.{}, intents[0..]);
    try std.testing.expect(source.iface().isLocalLeader(7701));
}

test "data runtime owned inferred leadership source clones placement intents" {
    const stores = [_]antfly.metadata.StoreRecord{
        .{ .store_id = 19, .node_id = 9 },
    };
    var intents = [_]antfly.raft.reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 7701, .replica_id = 1, .local_node_id = 9 },
            .store_id = 19,
            .peer_node_ids = &.{},
        },
    };
    var owned = try OwnedInferredSnapshotLeadershipSource.init(
        std.testing.allocator,
        9,
        19,
        stores[0..],
        &.{},
        intents[0..],
    );
    defer owned.deinit(std.testing.allocator);

    intents[0].record.local_node_id = 10;
    intents[0].store_id = 29;

    try std.testing.expect(owned.iface().isLocalLeader(7701));
}

test "data runtime does not infer local leader from replicated placement" {
    const stores = [_]antfly.metadata.StoreRecord{
        .{ .store_id = 19, .node_id = 9 },
        .{ .store_id = 29, .node_id = 10 },
    };
    const intents = [_]antfly.raft.reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 8801, .replica_id = 1, .local_node_id = 9 },
            .store_id = 19,
            .peer_node_ids = &.{10},
        },
        .{
            .record = .{ .group_id = 8801, .replica_id = 2, .local_node_id = 10 },
            .store_id = 29,
            .peer_node_ids = &.{9},
        },
    };
    var source = InferredSnapshotLeadershipSource.init(9, 19, stores[0..], &.{}, intents[0..]);
    try std.testing.expect(!source.iface().isLocalLeader(8801));
}

test "data runtime infers local leader from single-store fallback snapshot" {
    const stores = [_]antfly.metadata.StoreRecord{
        .{ .store_id = 19, .node_id = 9 },
    };
    var source = InferredSnapshotLeadershipSource.init(9, 19, stores[0..], &.{}, &.{});
    try std.testing.expect(source.iface().isLocalLeader(9901));
}

test "data runtime infers local leader from existing metadata group status reports" {
    const stores = [_]antfly.metadata.StoreRecord{
        .{
            .store_id = 19,
            .node_id = 9,
            .live = true,
            .health_class = "healthy",
            .group_statuses = @constCast((&[_]antfly.metadata.GroupStatusReport{
                .{
                    .group_id = 9911,
                    .local_leader = true,
                    .updated_at_millis = 10,
                },
            })[0..]),
        },
        .{
            .store_id = 29,
            .node_id = 10,
            .live = true,
            .health_class = "healthy",
            .group_statuses = @constCast((&[_]antfly.metadata.GroupStatusReport{
                .{
                    .group_id = 9911,
                    .local_leader = false,
                    .updated_at_millis = 11,
                },
            })[0..]),
        },
    };
    var source = InferredSnapshotLeadershipSource.init(9, 19, stores[0..], &.{}, &.{});
    try std.testing.expect(source.iface().isLocalLeader(9911));
}

test "data runtime does not infer local leader from conflicting metadata group status reports" {
    const stores = [_]antfly.metadata.StoreRecord{
        .{
            .store_id = 19,
            .node_id = 9,
            .live = true,
            .health_class = "healthy",
            .group_statuses = @constCast((&[_]antfly.metadata.GroupStatusReport{
                .{
                    .group_id = 9921,
                    .local_leader = true,
                    .updated_at_millis = 10,
                },
            })[0..]),
        },
        .{
            .store_id = 29,
            .node_id = 10,
            .live = true,
            .health_class = "healthy",
            .group_statuses = @constCast((&[_]antfly.metadata.GroupStatusReport{
                .{
                    .group_id = 9921,
                    .local_leader = true,
                    .updated_at_millis = 11,
                },
            })[0..]),
        },
    };
    var source = InferredSnapshotLeadershipSource.init(9, 19, stores[0..], &.{}, &.{});
    try std.testing.expect(!source.iface().isLocalLeader(9921));
}

test "data runtime prefers merged snapshot leader truth for replicated placement" {
    const stores = [_]antfly.metadata.StoreRecord{
        .{ .store_id = 19, .node_id = 9 },
        .{ .store_id = 29, .node_id = 10 },
    };
    const merged = [_]antfly.metadata.reconciler.MergedGroupStatus{
        .{
            .group_id = 9931,
            .leader_known = true,
            .leader_store_id = 19,
        },
    };
    const intents = [_]antfly.raft.reconciler.PlacementIntent{
        .{
            .record = .{ .group_id = 9931, .replica_id = 1, .local_node_id = 9 },
            .store_id = 19,
            .peer_node_ids = &.{10},
        },
        .{
            .record = .{ .group_id = 9931, .replica_id = 2, .local_node_id = 10 },
            .store_id = 29,
            .peer_node_ids = &.{9},
        },
    };
    var source = InferredSnapshotLeadershipSource.init(9, 19, stores[0..], merged[0..], intents[0..]);
    try std.testing.expect(source.iface().isLocalLeader(9931));
}

test "data runtime local group status falls back to snapshot heartbeat readiness" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-snapshot-fallback", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-88/table-db", .{replica_root_dir});
    defer std.testing.allocator.free(db_path);

    var db = try antfly.db.DB.open(std.testing.allocator, db_path, .{});
    db.close();

    const snapshot_stores = [_]antfly.metadata.StoreRecord{
        .{
            .store_id = 19,
            .node_id = 9,
            .live = true,
            .health_class = "healthy",
            .group_statuses = @constCast((&[_]antfly.metadata.GroupStatusReport{
                .{
                    .group_id = 88,
                    .updated_at_millis = 10,
                    .transition_pending = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .reads_ready_after_cutover = true,
                },
            })[0..]),
        },
    };

    const report = try collectLocalGroupStatus(
        std.testing.allocator,
        db_path,
        replica_root_dir,
        88,
        .{},
        null,
        null,
        snapshot_stores[0..],
        &.{},
        &[_]antfly.metadata.SplitTransitionRecord{.{
            .transition_id = 88002,
            .source_group_id = 77,
            .destination_group_id = 88,
            .phase = .bootstrap_peer,
            .split_key = "doc:m",
        }},
        &.{},
        &.{},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatus(std.testing.allocator, report);
    try std.testing.expect(report.transition_pending);
    try std.testing.expect(report.replay_required);
    try std.testing.expect(report.replay_caught_up);
    try std.testing.expect(report.cutover_ready);
    try std.testing.expect(report.reads_ready_after_cutover);
}

test "data runtime local group status prefers merged snapshot readiness fallback" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-merged-fallback", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-98/table-db", .{replica_root_dir});
    defer std.testing.allocator.free(db_path);

    var db = try antfly.db.DB.open(std.testing.allocator, db_path, .{});
    db.close();

    const merged = [_]antfly.metadata.reconciler.MergedGroupStatus{
        .{
            .group_id = 98,
            .leader_known = true,
            .leader_store_id = 19,
            .transition_pending = true,
            .replay_required = true,
            .replay_caught_up = true,
            .cutover_ready = true,
            .reads_ready_after_cutover = true,
        },
    };

    const report = try collectLocalGroupStatus(
        std.testing.allocator,
        db_path,
        replica_root_dir,
        98,
        .{},
        null,
        null,
        &.{},
        merged[0..],
        &[_]antfly.metadata.SplitTransitionRecord{.{
            .transition_id = 98002,
            .source_group_id = 97,
            .destination_group_id = 98,
            .phase = .bootstrap_peer,
            .split_key = "doc:m",
        }},
        &.{},
        &.{},
        &.{},
    );
    defer antfly.metadata.table_manager.freeGroupStatus(std.testing.allocator, report);
    try std.testing.expect(report.transition_pending);
    try std.testing.expect(report.replay_required);
    try std.testing.expect(report.replay_caught_up);
    try std.testing.expect(report.cutover_ready);
    try std.testing.expect(report.reads_ready_after_cutover);
}

test "data runtime remote admin snapshot clone preserves replication status surfaces" {
    const snapshot: antfly.metadata_api.AdminSnapshot = .{
        .status = .{ .metadata_group_id = 1, .metrics = .{} },
        .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
        .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{ .group_id = 11, .table_id = 7, .start_key = "", .end_key = null }})[0..]),
        .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{})[0..]),
        .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
        .shuffle_join_leases = @constCast((&[_]antfly.metadata.table_manager.ShuffleJoinLeaseRecord{.{ .job_id = 9, .owner_group_id = 11, .expires_at_ms = 1234 }})[0..]),
        .local_bootstrap_statuses = @constCast((&[_]antfly.raft.host.BootstrapStatus{.{ .group_id = 11, .kind = .backup_db_snapshot_restore, .phase = .failed, .last_error = "boom", .backup_id = "b1", .snapshot_path = "/tmp/snap" }})[0..]),
        .restore_progresses = @constCast((&[_]antfly.metadata.table_manager.RestoreProgressRecord{.{ .table_id = 7, .node_id = 2, .group_id = 11, .backup_id = "b1" }})[0..]),
        .replication_source_statuses = @constCast((&[_]antfly.metadata.table_manager.ReplicationSourceStatusRecord{.{ .table_id = 7, .source_ordinal = 0, .source_kind = "postgres", .external_table = "users", .cutover_mode = "slot_resumed", .slot_name = "slot_old", .publication_name = "pub_old", .phase = "streaming", .checkpoint = "lsn:0/10" }})[0..]),
        .replication_source_action_hints = @constCast((&[_]antfly.metadata_api.ReplicationSourceActionHint{.{ .table_id = 7, .table_name = @constCast("docs"), .source_ordinal = 0, .action = "reseed_exact_cutover", .reason = "existing_slot_non_exact_cutover", .reseed_exact_cutover_path = @constCast("/internal/v1/tables/docs/replication-sources/0/reseed-exact-cutover") }})[0..]),
        .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
        .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
        .split_observations = @constCast((&[_]antfly.metadata.transition_state.SplitObservationRecord{})[0..]),
        .merge_observations = @constCast((&[_]antfly.metadata.transition_state.MergeObservationRecord{})[0..]),
        .merged_group_statuses = @constCast((&[_]antfly.metadata.reconciler.MergedGroupStatus{})[0..]),
    };

    var cloned = try cloneAdminSnapshotOwned(std.testing.allocator, snapshot);
    defer freeAdminSnapshotOwned(std.testing.allocator, &cloned);

    try std.testing.expectEqual(@as(usize, 1), cloned.replication_source_statuses.len);
    try std.testing.expectEqualStrings("slot_resumed", cloned.replication_source_statuses[0].cutover_mode);
    try std.testing.expectEqual(@as(usize, 1), cloned.replication_source_action_hints.len);
    try std.testing.expectEqualStrings("reseed_exact_cutover", cloned.replication_source_action_hints[0].action);
    try std.testing.expectEqual(@as(usize, 1), cloned.local_bootstrap_statuses.len);
    try std.testing.expectEqualStrings("boom", cloned.local_bootstrap_statuses[0].last_error.?);
    try std.testing.expectEqual(@as(usize, 1), cloned.restore_progresses.len);
    try std.testing.expectEqualStrings("b1", cloned.restore_progresses[0].backup_id);
}

test "data runtime metrics use prometheus labels for resource and cache dimensions" {
    var resource_manager = resource_manager_mod.ResourceManager.init(.{});
    var writer_buf: [65536]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);

    try writeResourceMetrics(&writer, &resource_manager);
    const resource_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, resource_output, "# HELP antfly_resource_used_bytes") != null);
    try std.testing.expect(std.mem.indexOf(u8, resource_output, "antfly_resource_used_bytes{slice=\"lsm.block_table_cache\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, resource_output, "antfly_resource_pressure{slice=\"text_merge.buffers\"}") != null);

    var cache = lsm_backend_mod.Cache.init(std.testing.allocator, 1024 * 1024);
    defer cache.deinit();

    writer = .fixed(&writer_buf);
    try writeLsmCacheMetrics(&writer, cache.snapshotStats());
    const cache_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, cache_output, "# HELP antfly_lsm_cache_hits_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, cache_output, "antfly_lsm_cache_hits_total{kind=\"run_table_index\"}") != null);
    try std.testing.expect(std.mem.indexOf(u8, cache_output, "antfly_lsm_cache_waits_total{kind=\"run_table_block\"}") != null);

    writer = .fixed(&writer_buf);
    try writeLsmNativeStorageMetrics(&writer, .{ .fd_cache_entries = 7, .fd_cache_capacity = 1024 });
    const native_storage_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, native_storage_output, "antfly_lsm_native_fd_cache_entries 7") != null);
    try std.testing.expect(std.mem.indexOf(u8, native_storage_output, "antfly_lsm_native_fd_cache_capacity 1024") != null);

    writer = .fixed(&writer_buf);
    try writeProcessMemoryMetrics(&writer, .{
        .available = true,
        .resident_bytes = 11,
        .footprint_bytes = 13,
        .wired_bytes = 19,
        .pageins = 23,
        .malloc_available = true,
        .malloc_allocated_bytes = 29,
        .malloc_zone_bytes = 31,
    });
    const process_memory_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, process_memory_output, "antfly_process_memory_available 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, process_memory_output, "antfly_process_footprint_bytes 13") != null);
    try std.testing.expect(std.mem.indexOf(u8, process_memory_output, "antfly_process_pageins_total 23") != null);
    try std.testing.expect(std.mem.indexOf(u8, process_memory_output, "antfly_process_malloc_allocated_bytes 29") != null);

    writer = .fixed(&writer_buf);
    try writeFullTextMemoryMetrics(&writer, .{
        .text_indexes = 1,
        .text_segments = 2,
        .text_segment_bytes = 4096,
        .text_mmap_segment_bytes = 3072,
        .text_heap_segment_bytes = 1024,
        .inverted_term_dict_bytes = 512,
        .inverted_term_block_bytes = 256,
        .inverted_term_index_bytes = 128,
        .inverted_fst_bytes = 64,
        .inverted_postings_bytes = 2048,
        .inverted_postings_payload_bytes = 128,
        .inverted_skip_bytes = 32,
        .configured_lmdb_main_map_bytes = 8192,
    });
    const full_text_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, full_text_output, "antfly_full_text_segment_bytes 4096") != null);
    try std.testing.expect(std.mem.indexOf(u8, full_text_output, "antfly_full_text_mmap_segment_bytes 3072") != null);
    try std.testing.expect(std.mem.indexOf(u8, full_text_output, "antfly_full_text_inverted_term_dict_bytes 512") != null);
    try std.testing.expect(std.mem.indexOf(u8, full_text_output, "antfly_full_text_inverted_postings_bytes 2048") != null);
    try std.testing.expect(std.mem.indexOf(u8, full_text_output, "antfly_full_text_inverted_fst_bytes 64") != null);
    try std.testing.expect(std.mem.indexOf(u8, full_text_output, "antfly_full_text_inverted_skip_bytes 32") != null);

    writer = .fixed(&writer_buf);
    try writeLsmMaintenanceMetrics(&writer, .{
        .mutable_entries = 11,
        .mutable_bytes = 2048,
        .immutable_memtables = 2,
        .immutable_entries = 7,
        .immutable_bytes = 1024,
        .total_runs = 3,
        .total_run_bytes = 4096,
        .total_run_logical_entry_bytes = 8192,
        .total_run_physical_entry_bytes = 3072,
        .l0_runs = 2,
        .l0_bytes = 2048,
        .overlapping_l0_runs = 2,
        .soft_limit_l0_runs = 4,
        .hard_limit_l0_runs = 8,
        .soft_limit_l0_bytes = 65536,
        .hard_limit_l0_bytes = 131072,
        .lower_level_runs = 5,
        .lower_level_bytes = 4096,
        .max_level = 2,
        .level_overflow_runs = 1,
        .level_overflow_bytes = 256,
        .obsolete_paths = 1,
        .active_readers = 6,
        .manifest_dirty = true,
        .obsolete_manifest_dirty = true,
        .compaction_scheduler_active_oldest_age_ns = 99,
        .compaction_scheduler_grants = 3,
        .compaction_scheduler_denied_capacity = 1,
        .compaction_scheduler_remembered_pending = 1,
        .compaction_scheduler_remembered_pending_runs = 4,
        .compaction_scheduler_remembered_pending_bytes = 8192,
        .compaction_scheduler_remembered_hits = 2,
        .background_io_budget_bytes = 1000,
        .background_io_reserved_bytes = 750,
        .background_io_denied_jobs = 2,
        .background_io_oversized_jobs = 1,
        .backend_lock_waits = 9,
        .backend_lock_wait_ns = 100,
        .backend_lock_max_wait_ns = 25,
    });
    const maintenance_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "# HELP antfly_lsm_total_run_bytes") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_mutable_bytes 2048") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_immutable_memtables 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_immutable_bytes 1024") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_l0_runs 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_overlapping_l0_runs 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_soft_limit_l0_runs 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_level_overflow_runs 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_paths 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_active_readers 6") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_manifest_dirty 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_active_oldest_age_ns 99") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_grants_total 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_denied_capacity_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_remembered_pending 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_remembered_pending_runs 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_remembered_pending_bytes 8192") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_remembered_hits_total 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_background_io_budget_bytes 1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_background_io_reserved_bytes_total 750") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_backend_lock_waits_total 9") != null);

    writer = .fixed(&writer_buf);
    try writeLsmWriteMetrics(&writer, .{
        .flushes = 1,
        .flush_input_entries = 2,
        .flush_output_bytes = 3,
        .table_file_writes = 4,
        .table_file_bytes = 5,
        .write_pressure_events = 21,
        .write_pressure_compactions = 6,
        .write_pressure_compaction_steps = 22,
        .write_pressure_overloads = 23,
        .write_pressure_rejections = 24,
        .wal_pressure_flushes = 7,
        .wal_append_records = 8,
        .wal_append_entries = 9,
        .wal_append_bytes = 10,
        .wal_append_ns = 11,
        .wal_sync_records = 12,
        .wal_sync_ns = 13,
        .wal_replay_records = 14,
        .wal_replay_bytes = 15,
        .wal_replay_ns = 16,
        .wal_resets = 17,
        .wal_reset_ns = 18,
        .immutable_rotations = 19,
        .immutable_flushes = 20,
    });
    const write_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_flushes_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_table_file_writes_total 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_events_total 21") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_compactions_total 6") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_compaction_steps_total 22") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_overloads_total 23") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_rejections_total 24") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_wal_pressure_flushes_total 7") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_wal_append_records_total 8") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_wal_sync_records_total 12") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_wal_sync_ns_total 13") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_wal_replay_records_total 14") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_wal_resets_total 17") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_immutable_rotations_total 19") != null);

    writer = .fixed(&writer_buf);
    try writeTextMergeMetrics(&writer, .{
        .enabled = true,
        .pending_indexes = 1,
        .pending_segments = 3,
        .pending_bytes = 4096,
        .pending_heap_bytes = 1024,
        .pending_mmap_bytes = 3072,
        .in_flight_merges = 1,
        .completed_merges = 2,
        .deferred_for_pressure = 4,
        .merge_input_segments_total = 5,
        .merge_input_bytes_total = 600,
        .merge_output_segments_total = 2,
        .merge_output_bytes_total = 300,
        .last_merge_input_segments = 3,
        .last_merge_input_bytes = 256,
        .last_merge_output_segments = 1,
        .last_merge_output_bytes = 128,
        .max_pending_bytes = 8192,
    });
    const text_merge_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_enabled 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_pending_bytes 4096") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_pending_heap_bytes 1024") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_pending_mmap_bytes 3072") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_input_bytes_total 600") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_output_bytes_total 300") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_last_input_bytes 256") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_last_output_bytes 128") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_completed_total 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, text_merge_output, "antfly_text_merge_deferred_for_pressure_total 4") != null);
}

test "data runtime health metrics include replay debt and provisioned warmup counters" {
    const FakeStatus = struct {
        fn iface() antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(_: *anyopaque) !antfly.metadata_api.MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return try FakeStatus.adminSnapshot(undefined);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = std.testing.allocator,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(std.testing.allocator),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            ".",
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            ".",
            FakeCatalog.iface(),
        ),
        .status_source = FakeStatus.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);
    server.initApiServer();

    var status_resp = try server.http_server.?.handle(.{ .method = .GET, .uri = antfly.public_api.http_routes.Routes.status });
    defer status_resp.deinit(std.testing.allocator);

    const items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 77,
        .stats = .{
            .doc_count = 3,
            .index_count = 2,
            .indexes = try std.testing.allocator.alloc(antfly.db.types.DBIndexStats, 2),
            .async_indexing = .{
                .startup = .{
                    .active = true,
                    .phase = .opening_db,
                    .wal_retained_segments = 4,
                    .wal_retained_bytes = 99,
                    .wal_checkpoint_oldest_retained_segment = 2,
                    .wal_checkpoint_covered_through_segment = 3,
                    .wal_checkpoint_current_segment = 5,
                    .wal_checkpoint_lag_segments = 2,
                    .wal_replay_retained_segments = 1,
                    .wal_replay_retained_bytes = 44,
                    .wal_replay_current_segment = 6,
                    .lsm_open_stores = 3,
                    .lsm_open_completed = 2,
                    .lsm_open_total_ns = 1000,
                    .lsm_open_manifest_ns = 111,
                    .lsm_open_wal_replay_ns = 222,
                    .lsm_open_loaded_runs = 5,
                    .lsm_open_mutable_entries_after_replay = 7,
                    .wal_replay_records = 8,
                    .wal_replay_entries = 9,
                    .wal_replay_bytes = 10,
                    .wal_replay_ns = 222,
                    .wal_replay_truncated_tail_bytes = 66,
                },
            },
        },
    };
    items[0].stats.indexes[0] = .{
        .name = try std.testing.allocator.dupe(u8, "text"),
        .kind = .full_text,
        .doc_count = 3,
        .replay_applied_sequence = 7,
        .replay_target_sequence = 10,
        .replay_catch_up_required = true,
    };
    items[0].stats.indexes[1] = .{
        .name = try std.testing.allocator.dupe(u8, "dense"),
        .kind = .dense_vector,
        .doc_count = 3,
        .replay_applied_sequence = 5,
        .replay_target_sequence = 5,
    };

    const snapshots = try std.testing.allocator.alloc(runtime_status.TableRuntimeSnapshot, 1);
    snapshots[0] = .{
        .table_name = try std.testing.allocator.dupe(u8, "docs"),
        .statuses = .{ .items = items },
    };
    server.provisioned_storage.runtime_status_cache.replaceOwned(snapshots);

    server.provisioned_warmup_started.store(2, .monotonic);
    server.provisioned_warmup_completed.store(1, .monotonic);
    server.provisioned_warmup_failed.store(1, .monotonic);
    server.provisioned_warmup_last_group_count.store(4, .monotonic);
    server.provisioned_warmup_last_duration_ns.store(99, .monotonic);
    server.provisioned_startup_catch_up_started.store(5, .monotonic);
    server.provisioned_startup_catch_up_completed.store(4, .monotonic);
    server.provisioned_startup_catch_up_failed.store(1, .monotonic);
    server.provisioned_startup_catch_up_last_group_count.store(3, .monotonic);
    server.provisioned_startup_catch_up_last_groups_with_debt.store(2, .monotonic);
    server.provisioned_startup_catch_up_last_groups_cleared.store(1, .monotonic);
    server.provisioned_startup_catch_up_last_busy_groups.store(1, .monotonic);
    server.provisioned_startup_catch_up_last_duration_ns.store(123, .monotonic);
    server.runtime_status_refresh_started.store(3, .monotonic);
    server.runtime_status_refresh_completed.store(2, .monotonic);
    server.runtime_status_refresh_failed.store(1, .monotonic);
    server.runtime_status_refresh_last_table_count.store(1, .monotonic);
    server.runtime_status_refresh_last_group_count.store(1, .monotonic);
    server.runtime_status_refresh_last_db_opens.store(2, .monotonic);
    server.runtime_status_refresh_last_skipped_db_opens.store(3, .monotonic);
    server.runtime_status_refresh_last_placeholder_group_count.store(4, .monotonic);
    server.runtime_status_refresh_last_duration_ns.store(55, .monotonic);
    server.provisioned_root_refresh_active.store(true, .release);
    server.provisioned_root_refresh_dirty.store(true, .release);
    server.provisioned_root_refresh_started.store(8, .monotonic);
    server.provisioned_root_refresh_completed.store(7, .monotonic);
    server.provisioned_root_refresh_failed.store(1, .monotonic);
    server.provisioned_root_refresh_last_duration_ns.store(66, .monotonic);

    var health = HealthSource{ .data_server = &server };
    var writer_buf: [65536]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try health.metricsWriter().writeMetrics(&writer);
    const output = writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_tables 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_groups 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_indexes 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_api_requests_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_api_first_request_elapsed_ms") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_started_total 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_completed_total 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_failed_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_last_table_count 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_last_group_count 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_last_db_opens 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_last_skipped_db_opens 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_last_placeholder_group_count 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_refresh_last_duration_ns 55") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_root_refresh_active 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_root_refresh_dirty 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_root_refresh_started_total 8") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_root_refresh_completed_total 7") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_root_refresh_failed_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_root_refresh_last_duration_ns 66") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_query_fanout_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_query_fanout_ns_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_query_fanout_planned_parallel_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_query_fanout_planned_sequential_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_query_fanout_planned_width_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_query_fanout_planned_width_count") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_query_fanout_async_limit 8") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_text_stats_fanout_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_text_stats_fanout_ns_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_text_stats_fanout_planned_parallel_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_text_stats_fanout_planned_sequential_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_text_stats_fanout_planned_width_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_text_stats_fanout_planned_width_count") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_preflight_fanout_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_preflight_fanout_ns_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_preflight_fanout_planned_parallel_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_preflight_fanout_planned_sequential_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_preflight_fanout_planned_width_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_preflight_fanout_planned_width_count") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_expand_fanout_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_expand_fanout_ns_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_expand_fanout_planned_parallel_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_expand_fanout_planned_sequential_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_expand_fanout_planned_width_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_expand_fanout_planned_width_count") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_hydrate_fanout_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_hydrate_fanout_ns_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_hydrate_fanout_planned_parallel_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_hydrate_fanout_planned_sequential_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_hydrate_fanout_planned_width_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_parallel_graph_hydrate_fanout_planned_width_count") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_replay_debt_tables 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_replay_debt_groups 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_replay_debt_indexes 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_replay_debt_sequences 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_replay_debt_max_index_sequences 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_warmup_started_total 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_warmup_completed_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_warmup_failed_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_warmup_last_group_count 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_warmup_last_duration_ns 99") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_startup_catch_up_started_total 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_startup_catch_up_completed_total 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_startup_catch_up_failed_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_startup_catch_up_last_group_count 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_startup_catch_up_last_groups_with_debt 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_startup_catch_up_last_groups_cleared 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_startup_catch_up_last_busy_groups 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_startup_catch_up_last_duration_ns 123") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_read_cache_hits_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_read_cache_misses_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_write_cache_hits_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_provisioned_write_cache_misses_total") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_mutable_bytes 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_immutable_memtables 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_immutable_bytes 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_retained_segments 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_retained_bytes 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_checkpoint_oldest_retained_segment 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_checkpoint_covered_through_segment 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_checkpoint_current_segment 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_checkpoint_lag_segments 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_replay_retained_segments 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_replay_retained_bytes 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_replay_current_segment 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_background_io_budget_bytes 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_background_io_denied_jobs_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_append_records_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_sync_ns_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_lsm_wal_resets_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_active 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_retained_segments 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_retained_bytes 99") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_checkpoint_oldest_retained_segment 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_checkpoint_covered_through_segment 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_checkpoint_current_segment 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_checkpoint_lag_segments 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_retained_segments 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_retained_bytes 44") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_current_segment 6") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_stores 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_completed 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_total_ns 1000") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_manifest_ns 111") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_wal_replay_ns 222") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_loaded_runs 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_mutable_entries_after_replay 7") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_records 8") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_entries 9") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_bytes 10") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_ns 222") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_truncated_tail_bytes 66") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_phase{phase=\"opening_db\"} 1") != null);
}

test "data runtime lsm maintenance scheduler defers under resource pressure" {
    const alloc = std.testing.allocator;

    const FakeCatalog = struct {
        fn iface() antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogCall;
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = antfly.public_api.ProvisionedTableReadSource.init(
            "/tmp/unused-antfly-data-runtime-maintenance",
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init("/tmp/unused-antfly-data-runtime-maintenance", FakeCatalog.iface()),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    server.deferLsmMaintenance(100, 50);
    try std.testing.expectEqual(@as(u64, 150), server.lsm_maintenance_next_eligible_ns.load(.monotonic));

    var reservation = try server.provisioned_storage.resource_manager.reserve(.lsm_compaction_work, 769 * 1024 * 1024);
    defer reservation.release();
    try std.testing.expect(server.resourcePressureDefersBackgroundMaintenance());
}
