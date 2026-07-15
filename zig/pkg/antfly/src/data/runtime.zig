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
const platform_sync = @import("antfly_platform").sync;
const antfly = @import("../root.zig");
const indexes_api = @import("../api/indexes.zig");
const json_helpers = @import("../api/json_helpers.zig");
const fs_paths = @import("../common/fs_paths.zig");
const runtime_status = @import("../api/runtime_status.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const lsm_backend_mod = @import("../storage/lsm_backend.zig");
const resource_manager_mod = @import("../storage/resource_manager.zig");
const index_manager_mod = @import("../storage/db/catalog/index_manager.zig");
const change_journal_mod = @import("../storage/db/derived/change_journal.zig");
const doc_identity = @import("../storage/db/doc_identity.zig");
const data_raft_batch = @import("raft_batch.zig");
const platform_clock = @import("../platform/clock.zig");
const process_memory_mod = @import("../platform/process_memory.zig");
const platform_time = @import("../platform/time.zig");
const platform = @import("antfly_platform");
const raft_engine = @import("raft_engine");

const health_metrics = antfly.common.health_server;
const setup_io_thread_stack_size = 1 * 1024 * 1024;
const local_group_status_cache_ttl_ms: u64 = 60 * std.time.ms_per_s;
const store_status_report_interval_ticks: usize = 40;
const store_status_heartbeat_interval_ms: u64 = 30 * std.time.ms_per_s;
const StoreStatusReportKind = enum { none, heartbeat, full };
const metadata_head_cache_ttl_ms: u64 = std.time.ms_per_s;
const metadata_snapshot_cache_ttl_ms: u64 = std.time.ms_per_s;
const provision_head_poll_startup_interval_ms: u64 = std.time.ms_per_s;
const provision_head_poll_interval_ms: u64 = 5 * std.time.ms_per_s;
const runtime_status_refresh_interval_ms: u64 = std.time.ms_per_s;
const runtime_status_refresh_max_db_opens_per_run: usize = 16;
const runtime_status_disk_usage_refresh_interval_ns: u64 = 30 * std.time.ns_per_s;
const auto_bulk_finish_poll_interval_ms: u64 = 250;
const provisioned_startup_catch_up_interval_ms: u64 = std.time.ms_per_s;
const metrics_lsm_maintenance_snapshot_ttl_ns: u64 = 60 * std.time.ns_per_s;
const data_raft_batch_leader_wait_ns: u64 = 5 * std.time.ns_per_s;
const data_raft_batch_leader_retry_sleep_ns: u64 = 50 * std.time.ns_per_ms;
const data_raft_metadata_resync_interval_ns: u64 = 500 * std.time.ns_per_ms;
const data_raft_campaign_retry_interval_ns: u64 = 500 * std.time.ns_per_ms;
const data_raft_metadata_sync_interval_ms: u64 = 250;
const metadata_bootstrap_retry_base_ms: u64 = 250;
const metadata_bootstrap_retry_max_ms: u64 = 5 * std.time.ms_per_s;
const metadata_bootstrap_retry_jitter_ms: u64 = 250;
const public_api_max_connection_threads: u32 = 64;
const trusted_principal_secret_key = "antfly.trusted_principal.secret";
const trusted_principal_issuer_key = "antfly.trusted_principal.issuer";

fn decodeSplitDeltaDocumentKeyAlloc(alloc: std.mem.Allocator, internal_key: []const u8) ![]u8 {
    const logical_key = (try antfly.internal_keys.decodePrimaryDocumentKeyAlloc(alloc, internal_key)) orelse
        return error.InvalidAppliedDataDocumentKey;
    defer alloc.free(logical_key);
    if (!std.mem.startsWith(u8, logical_key, "g:")) return error.InvalidAppliedDataDocumentKey;
    const sep = std.mem.indexOfScalarPos(u8, logical_key, 2, ':') orelse return error.InvalidAppliedDataDocumentKey;
    return try alloc.dupe(u8, logical_key[sep + 1 ..]);
}

const CliConfig = struct {
    config_path: ?[]const u8 = null,
    bind_host: ?[]const u8 = null,
    bind_port: ?u16 = null,
    health_enabled: ?bool = null,
    health_port: ?u16 = null,
    raft_bind_host: ?[]const u8 = null,
    raft_bind_port: ?u16 = null,
    auth_enabled: ?bool = null,
    ard_base_url: ?[]const u8 = null,
    ard_publisher_domain: ?[]const u8 = null,
    ard_display_name: ?[]const u8 = null,
    ard_public_catalog_enabled: bool = false,
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
    extension_package_store_dir: ?[]const u8 = null,
    secret_store_paths: std.ArrayListUnmanaged([]const u8) = .empty,
    help: bool = false,

    fn deinit(self: *CliConfig, alloc: std.mem.Allocator) void {
        self.secret_store_paths.deinit(alloc);
        self.metadata_apis.deinit(alloc);
        self.* = undefined;
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
        .serve_in_connection_threads = true,
        .max_connection_threads = public_api_max_connection_threads,
    };
}

test "data public API listener uses public API request body limit" {
    const cfg = publicApiListenerConfig("127.0.0.1", 8080);
    try std.testing.expectEqual(antfly.public_api.http_server.public_api_max_request_body_bytes, cfg.max_request_bytes);
    try std.testing.expect(cfg.serve_in_connection_threads);
    try std.testing.expectEqual(public_api_max_connection_threads, cfg.max_connection_threads);
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
                    .election_tick = 30,
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
        self.write_source.bindWriteCaches(&self.write_cache, &storage.startup_write_cache);
        self.write_source.runtime_status_cache = &storage.runtime_status_cache;
        _ = self.write_source.withGroupVisibleRootGeneration(storage.groupVisibleRootGenerationSource());
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
            if (!data_raft_batch.looksLikeEnvelope(entry.data)) continue;
            var decoded = try data_raft_batch.decode(self.alloc, entry.data);
            defer decoded.deinit(self.alloc);
            // Split lifecycle commands belong exclusively to the durable Raft
            // apply store. Sending an otherwise empty command through the
            // document DB can fail on unrelated index/runtime state after the
            // lifecycle mutation is already durable, leaving Raft replaying a
            // partially applied command.
            if (!batchRequiresDocumentDbApply(decoded.batch.req)) continue;
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

fn batchRequiresDocumentDbApply(req: antfly.db.types.BatchRequest) bool {
    return req.split_transition == null;
}

/// Backs the standalone data server's health/metrics endpoints. Readiness is
/// local-only so Kubernetes probes cannot block indefinitely behind a wedged
/// remote metadata API.
pub const HealthSource = struct {
    data_server: *DataServer,
    lsm_maintenance_metrics_mutex: std.atomic.Mutex = .unlocked,
    lsm_maintenance_metrics_stats: lsm_backend_mod.Backend.MaintenanceStats = .{},
    lsm_maintenance_metrics_built_at_ns: u64 = 0,
    lsm_maintenance_metrics_last_refresh_duration_ns: u64 = 0,
    lsm_maintenance_metrics_refreshes: u64 = 0,
    lsm_maintenance_metrics_refreshing: bool = false,

    const CachedLsmMaintenanceStats = struct {
        stats: lsm_backend_mod.Backend.MaintenanceStats,
        age_ns: u64,
        last_refresh_duration_ns: u64,
        refreshes: u64,
    };

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
        return self.data_server.http_server != null and self.data_server.listener != null;
    }

    fn writeMetrics(ptr: *anyopaque, writer: *std.Io.Writer) anyerror!void {
        const self: *HealthSource = @ptrCast(@alignCast(ptr));
        const runtime_summary = self.data_server.provisioned_storage.runtime_status_cache.summary();
        const read_cache_stats = self.data_server.provisioned_storage.read_cache.cacheStats();
        const write_cache_stats = self.data_server.provisioned_storage.write_cache.cacheStats();
        const live_write_source = self.data_server.liveRuntimeWriteSource();
        const auto_bulk_stats = live_write_source.autoBulkIngestStatsBestEffort();
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
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_hits_total", "counter", "Query embedding cache hits", api_request_stats.query_embedding_cache.hits);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_misses_total", "counter", "Query embedding cache producer misses", api_request_stats.query_embedding_cache.misses);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_coalesced_waiters_total", "counter", "Query embedding requests coalesced behind an in-flight producer", api_request_stats.query_embedding_cache.coalesced_waiters);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_producers_total", "counter", "Query embedding producer computations", api_request_stats.query_embedding_cache.producer_computations);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_uncached_computations_total", "counter", "Uncached query embedding computations admitted through the shared provider bound", api_request_stats.query_embedding_cache.uncached_computations);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_producer_compute_ns_total", "counter", "Nanoseconds spent computing admitted query embeddings", api_request_stats.query_embedding_cache.producer_compute_ns_total);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_inflight_rejections_total", "counter", "Query embedding computations rejected by the in-flight admission limit", api_request_stats.query_embedding_cache.inflight_rejections);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_waiter_timeouts_total", "counter", "Coalesced query embedding waiters that reached their request deadline", api_request_stats.query_embedding_cache.waiter_timeouts);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_inflight", "gauge", "Distinct query embedding computations currently in flight", api_request_stats.query_embedding_cache.inflight);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_inflight_limit", "gauge", "Maximum distinct query embedding computations allowed in flight", api_request_stats.query_embedding_cache.max_inflight);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_evictions_total", "counter", "Query embedding cache LRU evictions", api_request_stats.query_embedding_cache.evictions);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_expirations_total", "counter", "Expired query embedding cache entries", api_request_stats.query_embedding_cache.expirations);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_rejected_admissions_total", "counter", "Query embedding results rejected by cache admission control", api_request_stats.query_embedding_cache.rejected_admissions);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_entries", "gauge", "Live query embedding cache entries", api_request_stats.query_embedding_cache.entries);
        try health_metrics.appendPromMetric(writer, "antfly_query_embedding_cache_live_bytes", "gauge", "Accounted live query embedding cache bytes", api_request_stats.query_embedding_cache.live_bytes);
        try health_metrics.appendPromMetric(writer, "antfly_inference_cache_budget_bytes", "gauge", "Shared inference cache bytes in use", api_request_stats.inference_cache_budget.used_bytes);
        try health_metrics.appendPromMetric(writer, "antfly_inference_cache_budget_limit_bytes", "gauge", "Shared inference cache byte limit", api_request_stats.inference_cache_budget.max_bytes);
        try health_metrics.appendPromMetric(writer, "antfly_inference_cache_budget_rejected_reservations_total", "counter", "Shared inference cache reservations rejected by the hard limit", api_request_stats.inference_cache_budget.rejected_reservations);
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
        try self.writeHAMetrics(writer);
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
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_last_quarantined_groups", "gauge", "Provisioned table groups parked by zero-progress startup catch-up backoff in the most recent run", self.data_server.provisioned_startup_catch_up_last_quarantined_groups.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_startup_catch_up_last_duration_ns", "gauge", "Duration of the most recent provisioned startup catch-up run in monotonic nanoseconds", self.data_server.provisioned_startup_catch_up_last_duration_ns.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_read_cache_hits_total", "counter", "Provisioned read-cache hits served from already-open local table DBs", read_cache_stats.hit_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_read_cache_misses_total", "counter", "Provisioned read-cache opens that had to open a local table DB", read_cache_stats.miss_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_write_cache_hits_total", "counter", "Provisioned write-cache hits served from already-open local table DBs", write_cache_stats.hit_count);
        try health_metrics.appendPromMetric(writer, "antfly_data_provisioned_write_cache_misses_total", "counter", "Provisioned write-cache opens that had to open a local table DB", write_cache_stats.miss_count);
        try writeResourceMetrics(writer, &self.data_server.provisioned_storage.resource_manager);
        try writeLsmCacheMetrics(writer, self.data_server.provisioned_storage.lsm_cache.snapshotStats());
        try writeLsmNativeStorageMetrics(writer, live_write_source.lsmNativeStorageStatsBestEffort());
        try writeFullTextMemoryMetrics(writer, live_write_source.textMemoryAttributionStatsBestEffort());
        try writeProcessMemoryMetrics(writer, process_memory_mod.snapshot());
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_score", "gauge", "Maximum cached table LSM maintenance pressure score; 1 can mean debt exists but exact score has not been refreshed", live_write_source.lsmMaintenanceScoreBestEffort());
        try health_metrics.appendPromMetric(writer, "antfly_lsm_cached_write_dbs", "gauge", "Cached writable table DBs with local LSM state", @intCast(live_write_source.cachedWriteDbCountBestEffort()));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_active", "gauge", "Whether the data server LSM maintenance background worker is currently active", if (self.data_server.lsm_maintenance_active.load(.acquire)) 1 else 0);
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_started_total", "counter", "Data server LSM maintenance background worker wake cycles started", self.data_server.lsm_maintenance_started.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_completed_total", "counter", "Data server LSM maintenance background worker wake cycles completed with no immediate work remaining", self.data_server.lsm_maintenance_completed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_failed_total", "counter", "Data server LSM maintenance background worker wake cycles that observed an error", self.data_server.lsm_maintenance_failed.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_capacity_denied_total", "counter", "Data server LSM maintenance background wake cycles denied by resource capacity", self.data_server.lsm_maintenance_capacity_denied.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_bulk_deferred_total", "counter", "Data server LSM maintenance background wake cycles deferred behind active bulk ingest", self.data_server.lsm_maintenance_bulk_deferred.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_lock_deferred_total", "counter", "Data server LSM maintenance background wake cycles deferred behind foreground locks", self.data_server.lsm_maintenance_lock_deferred.load(.monotonic));
        try health_metrics.appendPromMetric(writer, "antfly_lsm_maintenance_background_next_eligible_ns", "gauge", "Monotonic timestamp when background LSM maintenance can next run", self.data_server.lsm_maintenance_next_eligible_ns.load(.monotonic));
        const lsm_maintenance_stats = self.cachedLsmMaintenanceStats();
        try writeLsmMaintenanceSnapshotMetrics(writer, lsm_maintenance_stats);
        try writeLsmMaintenanceMetrics(writer, lsm_maintenance_stats.stats);
        try writeLsmWriteMetrics(writer, live_write_source.lsmWriteStatsBestEffort());
        try writeTextMergeMetrics(writer, live_write_source.textMergeStatsBestEffort());
        try writeAsyncIndexingMetrics(writer, live_write_source.asyncIndexingStatsBestEffort());
        try antfly.db.query_metrics.writePrometheus(writer);
        try antfly.db.enrichment_utf8_text.writePrometheus(writer);
    }

    fn writeHAMetrics(self: *HealthSource, writer: *std.Io.Writer) !void {
        const ds = self.data_server;
        const ctx = ds.ha_cfg.admin_context;
        const primary = if (ctx) |ha_ctx|
            ha_ctx.primary orelse ds.ha_cfg.internal_primary
        else
            ds.ha_cfg.internal_primary;
        const standby = if (ctx) |ha_ctx| ha_ctx.standby else null;

        try health_metrics.appendPromMetric(writer, "antfly_ha_runtime_configured", "gauge", "Whether this data runtime has any HA role configured", if (primary != null or standby != null) 1 else 0);

        if (primary) |handle| {
            var snapshot = try antfly.ha.status.primarySnapshot(ds.alloc, handle, ds.ha_cfg.primary_retention_policy, ds.ha_cfg.primary_sync_policy);
            defer snapshot.deinit(ds.alloc);
            var metrics = try antfly.ha.metrics.fromPrimarySnapshot(ds.alloc, snapshot);
            defer metrics.deinit(ds.alloc);
            const text = try antfly.ha.metrics.renderPrimaryPrometheusAlloc(ds.alloc, metrics);
            defer ds.alloc.free(text);
            try writer.print("{s}", .{text});
        }

        try health_metrics.appendPromMetric(writer, "antfly_ha_primary_mirror_last_lsn", "gauge", "Last HA replication LSN mirrored by this data runtime", ds.ha_primary_mirror_last_lsn.load(.acquire));
        try health_metrics.appendPromMetric(writer, "antfly_ha_primary_mirror_failures_total", "counter", "HA primary replication mirror failures observed by this data runtime", ds.ha_primary_mirror_failure_count.load(.acquire));
        try health_metrics.appendPromMetric(writer, "antfly_ha_primary_sync_rejects_total", "counter", "HA synchronous commit decisions rejected before local commit", ds.ha_primary_mirror_sync_reject_count.load(.acquire));
        try health_metrics.appendPromMetric(writer, "antfly_ha_primary_sync_waits_total", "counter", "HA synchronous commit waits observed by this data runtime", ds.ha_primary_mirror_sync_wait_count.load(.acquire));
        try health_metrics.appendPromMetric(writer, "antfly_ha_primary_sync_degraded_total", "counter", "HA synchronous commit decisions degraded to async by policy", ds.ha_primary_mirror_sync_degraded_count.load(.acquire));
        try health_metrics.appendPromMetric(writer, "antfly_ha_standby_replication_failures_total", "counter", "HA standby replication rounds that exited early with an error", ds.ha_standby_replication_failure_count.load(.acquire));
        try health_metrics.appendPromMetric(writer, "antfly_ha_standby_replication_last_attempt_ns", "gauge", "Monotonic timestamp for the most recent HA standby replication attempt", ds.ha_standby_replication_last_attempt_ns.load(.acquire));
        try health_metrics.appendPromMetric(writer, "antfly_ha_standby_replication_last_success_ns", "gauge", "Monotonic timestamp for the most recent successful HA standby replication round", ds.ha_standby_replication_last_success_ns.load(.acquire));

        if (standby) |handle| {
            const upstream_lsn = if (ds.ha_cfg.standby_replication != null)
                handle.currentProgress().received_lsn
            else
                null;
            const snapshot = antfly.ha.status.standbySnapshot(handle, upstream_lsn);
            const metrics = antfly.ha.metrics.fromStandbySnapshot(snapshot);
            const text = try antfly.ha.metrics.renderStandbyPrometheusAlloc(ds.alloc, metrics);
            defer ds.alloc.free(text);
            try writer.print("{s}", .{text});
        }
    }

    fn cachedLsmMaintenanceStats(self: *HealthSource) CachedLsmMaintenanceStats {
        const now_ns: u64 = @intCast(platform_time.monotonicNs());
        var should_refresh = false;
        lockAtomic(&self.lsm_maintenance_metrics_mutex);
        if ((self.lsm_maintenance_metrics_built_at_ns == 0 or ageSinceNs(now_ns, self.lsm_maintenance_metrics_built_at_ns) >= metrics_lsm_maintenance_snapshot_ttl_ns) and !self.lsm_maintenance_metrics_refreshing) {
            self.lsm_maintenance_metrics_refreshing = true;
            should_refresh = true;
        }
        const cached = self.lsmMaintenanceStatsSnapshotLocked(now_ns);
        self.lsm_maintenance_metrics_mutex.unlock();
        if (!should_refresh) return cached;

        const refresh_started_ns: u64 = @intCast(platform_time.monotonicNs());
        const refreshed_stats = self.data_server.liveRuntimeWriteSource().lsmMaintenanceStatsBestEffort();
        const refresh_finished_ns: u64 = @intCast(platform_time.monotonicNs());

        lockAtomic(&self.lsm_maintenance_metrics_mutex);
        self.lsm_maintenance_metrics_stats = refreshed_stats;
        self.lsm_maintenance_metrics_built_at_ns = refresh_finished_ns;
        self.lsm_maintenance_metrics_last_refresh_duration_ns = refresh_finished_ns - refresh_started_ns;
        self.lsm_maintenance_metrics_refreshes += 1;
        self.lsm_maintenance_metrics_refreshing = false;
        const refreshed = self.lsmMaintenanceStatsSnapshotLocked(refresh_finished_ns);
        self.lsm_maintenance_metrics_mutex.unlock();
        return refreshed;
    }

    fn lsmMaintenanceStatsSnapshotLocked(self: *const HealthSource, now_ns: u64) CachedLsmMaintenanceStats {
        return .{
            .stats = self.lsm_maintenance_metrics_stats,
            .age_ns = ageSinceNs(now_ns, self.lsm_maintenance_metrics_built_at_ns),
            .last_refresh_duration_ns = self.lsm_maintenance_metrics_last_refresh_duration_ns,
            .refreshes = self.lsm_maintenance_metrics_refreshes,
        };
    }
};

fn ageSinceNs(now_ns: u64, built_at_ns: u64) u64 {
    if (built_at_ns == 0 or now_ns < built_at_ns) return 0;
    return now_ns - built_at_ns;
}

fn writeLsmMaintenanceSnapshotMetrics(writer: *std.Io.Writer, snapshot: HealthSource.CachedLsmMaintenanceStats) !void {
    try health_metrics.appendPromMetric(writer, "antfly_metrics_lsm_snapshot_age_seconds", "gauge", "Age of the cached LSM maintenance metrics snapshot", snapshot.age_ns / std.time.ns_per_s);
    try health_metrics.appendPromMetric(writer, "antfly_metrics_lsm_snapshot_refreshes_total", "counter", "Cached LSM maintenance metrics snapshot refreshes completed", snapshot.refreshes);
    try health_metrics.appendPromMetric(writer, "antfly_metrics_lsm_snapshot_last_refresh_duration_ns", "gauge", "Duration of the most recent LSM maintenance metrics snapshot refresh in monotonic nanoseconds", snapshot.last_refresh_duration_ns);
}

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
    try health_metrics.appendPromMetric(writer, "antfly_lsm_bulk_ingest_current_scan_clone_active_bytes", "gauge", "Active bytes held by bulk-ingest current-scan mutable clones", stats.bulk_ingest_current_scan_clone_active_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_bulk_ingest_current_scan_clone_peak_active_bytes", "gauge", "Peak active bytes held by bulk-ingest current-scan mutable clones", stats.bulk_ingest_current_scan_clone_peak_active_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_bulk_ingest_current_scan_clone_budget_denials_total", "counter", "Bulk-ingest current-scan mutable clone fallbacks caused by clone memory budget", stats.bulk_ingest_current_scan_clone_budget_denials);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_bulk_ingest_current_scan_clone_oom_fallbacks_total", "counter", "Bulk-ingest current-scan mutable clone fallbacks caused by allocation failure", stats.bulk_ingest_current_scan_clone_oom_fallbacks);
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
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_stall_l0_run_debt", "gauge", "Cached write LSM level-zero runs currently over hard write-stall pressure", stats.write_stall_l0_run_debt);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_soft_limit_l0_bytes", "gauge", "Configured cached write LSM soft level-zero byte pressure threshold", stats.soft_limit_l0_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_hard_limit_l0_bytes", "gauge", "Configured cached write LSM hard level-zero byte pressure threshold", stats.hard_limit_l0_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_stall_l0_byte_debt", "gauge", "Cached write LSM level-zero bytes currently over hard write-stall pressure", stats.write_stall_l0_byte_debt);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_lower_level_runs", "gauge", "Cached write LSM lower-level run count", stats.lower_level_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_lower_level_bytes", "gauge", "Cached write LSM lower-level run bytes", stats.lower_level_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_max_level", "gauge", "Highest cached write LSM lower level currently populated", stats.max_level);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_level_overflow_runs", "gauge", "Cached write LSM lower-level runs over configured level targets", stats.level_overflow_runs);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_level_overflow_bytes", "gauge", "Cached write LSM lower-level bytes over configured level targets", stats.level_overflow_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_paths", "gauge", "Cached write LSM obsolete table paths waiting for cleanup", stats.obsolete_paths);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_paths_pinned_by_readers", "gauge", "Cached write LSM obsolete table paths retained by active readers", stats.obsolete_paths_pinned_by_readers);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_paths_pinned_by_versions", "gauge", "Cached write LSM obsolete table paths retained by active versions", stats.obsolete_paths_pinned_by_versions);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_paths_waiting_for_retry", "gauge", "Cached write LSM obsolete table paths waiting for delete retry or grace delay", stats.obsolete_paths_waiting_for_retry);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_paths_reclaimable", "gauge", "Cached write LSM obsolete table paths currently eligible for deletion", stats.obsolete_paths_reclaimable);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_delete_failures_total", "counter", "Cached write LSM obsolete table file delete failures", stats.obsolete_delete_failures);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_obsolete_delete_retries_total", "counter", "Cached write LSM obsolete table file delete retries scheduled", stats.obsolete_delete_retries);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_active_readers", "gauge", "Cached write LSM readers currently retaining run or memtable snapshots", stats.active_readers);
    try health_metrics.appendPromMetricHeader(writer, "antfly_lsm_active_readers_by_kind", "gauge", "Cached write LSM readers currently retaining snapshots by owner class");
    for (stats.active_readers_by_kind, 0..) |count, i| {
        const kind: lsm_backend_mod.ReaderPinKind = @enumFromInt(i);
        const labels = [_]health_metrics.PromLabel{.{ .name = "kind", .value = lsm_backend_mod.readerPinKindName(kind) }};
        try health_metrics.appendPromSampleLabeled(writer, "antfly_lsm_active_readers_by_kind", &labels, count);
    }
    try health_metrics.appendPromMetricHeader(writer, "antfly_lsm_obsolete_paths_pinned_by_reader_kind", "gauge", "Cached write LSM obsolete table paths retained by active readers by owner class");
    for (stats.obsolete_paths_pinned_by_reader_kind, 0..) |count, i| {
        const kind: lsm_backend_mod.ReaderPinKind = @enumFromInt(i);
        const labels = [_]health_metrics.PromLabel{.{ .name = "kind", .value = lsm_backend_mod.readerPinKindName(kind) }};
        try health_metrics.appendPromSampleLabeled(writer, "antfly_lsm_obsolete_paths_pinned_by_reader_kind", &labels, count);
    }
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
    try health_metrics.appendPromMetric(writer, "antfly_lsm_compaction_scheduler_oversized_skips_total", "counter", "Cached write LSM compaction candidates skipped by strict input-size limits", stats.compaction_scheduler_oversized_skips);
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
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_l0_run_debt_total", "counter", "Level-zero hard-limit run debt observed at cached write LSM write-pressure events", stats.write_pressure_l0_run_debt);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_l0_byte_debt_total", "counter", "Level-zero hard-limit byte debt observed at cached write LSM write-pressure events", stats.write_pressure_l0_byte_debt);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_overloads_total", "counter", "Cached write LSM write-pressure events that remained above hard limits after the foreground budget", stats.write_pressure_overloads);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_overload_l0_run_debt_total", "counter", "Level-zero hard-limit run debt remaining after cached write LSM foreground write-pressure budgets", stats.write_pressure_overload_l0_run_debt);
    try health_metrics.appendPromMetric(writer, "antfly_lsm_write_pressure_overload_l0_byte_debt_total", "counter", "Level-zero hard-limit byte debt remaining after cached write LSM foreground write-pressure budgets", stats.write_pressure_overload_l0_byte_debt);
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
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_recovered_temp_cleanup_ns", "gauge", "Summed recovered table temp file cleanup duration during startup open", stats.startup.lsm_open_recovered_temp_cleanup_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_manifest_ns", "gauge", "Summed LSM manifest load duration during startup open", stats.startup.lsm_open_manifest_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_ensuring_dirs_ns", "gauge", "Summed LSM directory creation duration during startup open", stats.startup.lsm_open_ensuring_dirs_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_wal_replay_ns", "gauge", "Summed LSM WAL replay duration during startup open", stats.startup.lsm_open_wal_replay_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_mounting_runs_ns", "gauge", "Summed LSM run mounting duration during startup open", stats.startup.lsm_open_mounting_runs_ns);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_loaded_runs", "gauge", "LSM runs loaded during startup open", stats.startup.lsm_open_loaded_runs);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_obsolete_paths", "gauge", "LSM obsolete paths loaded during startup open", stats.startup.lsm_open_obsolete_paths);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_mutable_entries_after_replay", "gauge", "LSM mutable entries after startup WAL replay", stats.startup.lsm_open_mutable_entries_after_replay);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_immutable_memtables_after_replay", "gauge", "LSM immutable memtables after startup WAL replay", stats.startup.lsm_open_immutable_memtables_after_replay);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_recovered_temp_files_deleted", "gauge", "Recovered table temp files deleted during startup open", stats.startup.lsm_open_recovered_temp_files_deleted);
    try health_metrics.appendPromMetric(writer, "antfly_async_index_startup_lsm_open_recovered_temp_bytes_deleted", "gauge", "Recovered table temp bytes deleted during startup open", stats.startup.lsm_open_recovered_temp_bytes_deleted);
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
        resource_manager_mod.Slice.lite_native_page_cache,
        resource_manager_mod.Slice.lite_native_link_cache,
        resource_manager_mod.Slice.lite_docstore_snapshot_cache,
        resource_manager_mod.Slice.inference_prompt_cache,
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
    try health_metrics.appendPromMetric(writer, "antfly_process_anonymous_bytes", "gauge", "Process anonymous resident bytes reported by the operating system", stats.anonymous_bytes);
    try health_metrics.appendPromMetric(writer, "antfly_process_private_dirty_bytes", "gauge", "Process private dirty bytes reported by the operating system", stats.private_dirty_bytes);
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
    ha: DataServerHAConfig = .{},
};

pub const DataServerHAConfig = struct {
    admin_context: ?antfly.ha.admin_exec.Context = null,
    standby_owner: ?*?antfly.ha.standby.Standby = null,
    admin_bearer_token: ?[]const u8 = null,
    /// Durable primary-local root for immutable runtime-owned seed generations.
    /// The admin API never accepts caller-selected source or destination paths.
    seed_capture_root: ?[]const u8 = null,
    /// Durable activated-generation root used to expose standby activation
    /// lifecycle receipts through the authenticated admin inventory.
    seed_activation_root: ?[]const u8 = null,
    /// Kubernetes pod identity attached to durable lifecycle evidence.
    pod_uid: ?[]const u8 = null,
    /// Optional storage-specific producer for an immutable logical snapshot.
    /// DataServer still validates and packages the result; providers cannot
    /// select the published generation root or bypass the mutation barrier.
    seed_snapshot_provider: ?HASeedSnapshotProvider = null,
    internal_primary: ?*antfly.ha.primary.Primary = null,
    primary_retention_policy: antfly.ha.slot_store.RetentionPolicy = .{},
    primary_sync_policy: antfly.ha.primary.SyncPolicy = .{},
    primary_sync_wait: HASyncWaitConfig = .{},
    standby_replication: ?HAStandbyReplicationConfig = null,
};

pub const HASeedSnapshotRequest = struct {
    capture_root: []const u8,
    generation: []const u8,
    auth_artifact_json: ?[]const u8 = null,
};

pub const HASeedPreparedSnapshot = struct {
    root: []u8,

    pub fn deinit(self: *HASeedPreparedSnapshot, alloc: std.mem.Allocator) void {
        alloc.free(self.root);
        self.* = undefined;
    }
};

pub const HASeedSnapshotProvider = struct {
    ptr: *anyopaque,
    prepare_fn: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        request: HASeedSnapshotRequest,
    ) anyerror!HASeedPreparedSnapshot,

    pub fn prepare(
        self: HASeedSnapshotProvider,
        alloc: std.mem.Allocator,
        request: HASeedSnapshotRequest,
    ) !HASeedPreparedSnapshot {
        return try self.prepare_fn(self.ptr, alloc, request);
    }
};

const ha_seed_snapshot_format_version = antfly.ha.seed_materialization.topology_format_version;
const ha_seed_snapshot_topology_name = antfly.ha.seed_materialization.topology_name;
const ha_seed_snapshot_max_topology_bytes = 16 * 1024 * 1024;
const ha_seed_snapshot_max_store_bytes: u64 = 64 * 1024 * 1024 * 1024;

const HASeedSnapshotTopologyReplica = antfly.ha.seed_materialization.ReplicaSnapshot;
const HASeedSnapshotExtensionArtifact = antfly.ha.seed_materialization.ExtensionArtifact;
const HASeedSnapshotAuthArtifact = antfly.ha.seed_materialization.AuthArtifact;
const HASeedSnapshotTopology = antfly.ha.seed_materialization.Topology;

const HASeedExtensionCaptureProbe = struct {
    ptr: *anyopaque,
    after_copy_fn: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror!void,

    fn afterCopy(self: @This(), alloc: std.mem.Allocator) !void {
        try self.after_copy_fn(self.ptr, alloc);
    }
};

fn freeHASeedSnapshotExtensionArtifacts(
    alloc: std.mem.Allocator,
    artifacts: *std.ArrayListUnmanaged(HASeedSnapshotExtensionArtifact),
) void {
    for (artifacts.items) |artifact| {
        alloc.free(artifact.path);
        alloc.free(artifact.sha256);
    }
    artifacts.deinit(alloc);
}

fn haSeedSnapshotFileSha256HexAlloc(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
) ![]u8 {
    const stat = try std.Io.Dir.cwd().statFile(io, path, .{ .follow_symlinks = false });
    if (stat.kind != .file) return error.HASeedSnapshotUnexpectedArtifact;
    if (stat.size > ha_seed_snapshot_max_store_bytes) return error.HASeedSnapshotStoreTooLarge;
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = file.reader(io, &.{});
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    var total: u64 = 0;
    while (true) {
        const read = try reader.interface.readSliceShort(&buffer);
        if (read == 0) break;
        total = try std.math.add(u64, total, read);
        if (total > ha_seed_snapshot_max_store_bytes) return error.HASeedSnapshotStoreTooLarge;
        hasher.update(buffer[0..read]);
    }
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const encoded = try alloc.alloc(u8, digest.len * 2);
    for (digest, 0..) |byte, index| {
        encoded[index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        encoded[index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return encoded;
}

fn validLowerSha256(value: []const u8) bool {
    if (value.len != std.crypto.hash.sha2.Sha256.digest_length * 2) return false;
    for (value) |byte| {
        if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return false;
    }
    return true;
}

fn haSeedPackageManifestsEqual(
    alloc: std.mem.Allocator,
    left: antfly.extensions.PackageManifest,
    right: antfly.extensions.PackageManifest,
) !bool {
    const left_json = try std.json.Stringify.valueAlloc(alloc, left, .{});
    defer alloc.free(left_json);
    const right_json = try std.json.Stringify.valueAlloc(alloc, right, .{});
    defer alloc.free(right_json);
    return std.mem.eql(u8, left_json, right_json);
}

fn haSeedExtensionEntryForPath(
    path: []const u8,
    entries: []const antfly.extensions.PackageStoreEntry,
) !usize {
    var found: ?usize = null;
    for (entries, 0..) |entry, index| {
        if (!antfly.ha.validation.isAbsoluteNormalizedPathWithinRoot(path, entry.package_root_path)) continue;
        if (found != null) return error.HASeedExtensionOverlappingPackageRoots;
        found = index;
    }
    return found orelse error.HASeedExtensionUnexpectedArtifact;
}

fn haSeedExtensionArtifactDirectory(
    artifacts: []const HASeedSnapshotExtensionArtifact,
    path: []const u8,
) bool {
    for (artifacts) |artifact| {
        if (artifact.path.len > path.len and
            std.mem.startsWith(u8, artifact.path, path) and
            artifact.path[path.len] == std.fs.path.sep)
            return true;
    }
    return false;
}

pub const HASyncWaitConfig = struct {
    max_rounds: usize = 200,
    sleep_ns: u64 = 10 * std.time.ns_per_ms,
    poll_ctx: ?*anyopaque = null,
    poll_fn: ?antfly.db.HAProgressPollFn = null,
};

fn haPrimarySyncWaitFromConfig(cfg: HASyncWaitConfig) antfly.db.HAPrimaryProgressSyncWait {
    return .{
        .max_rounds = cfg.max_rounds,
        .sleep_ns = cfg.sleep_ns,
        .poll_ctx = cfg.poll_ctx,
        .poll_fn = cfg.poll_fn,
    };
}

pub const HAStandbyReplicationConfig = struct {
    upstream_base_uri: []const u8,
    slot_name: []const u8,
    /// Authentication authority for the upstream internal HA endpoints.
    /// Standalone wires this from the same env-backed secret used by its
    /// authenticated HA admin listener; it is never accepted on argv.
    bearer_token: ?[]const u8 = null,
    standby_log_path: ?[]const u8 = null,
    standby_progress_path: ?[]const u8 = null,
    options: HAStandbyReplicationOptions = .{},
    catch_up_until_end_of_wal: bool = false,
    executor: ?antfly.common.http.RequestExecutor = null,
};

pub const HAStandbyReplicationOptions = antfly.ha.http_replication_client.ReplicateOptions;
pub const HAStandbyReplicationResult = antfly.ha.http_replication_client.Result;
pub const HAStandbyReplicationLoopResult = antfly.ha.http_replication_client.LoopResult;

const HAStandbyReplicationErrorCode = enum(u8) {
    none = 0,
    HttpConnectionClosing,
    ConnectionResetByPeer,
    ConnectionRefused,
    BrokenPipe,
    EndOfStream,
    NoAddressReturned,
    UnexpectedHttpStatus,
    NotListening,
    UnsupportedReplicationFormat,
    ReplicationResponseLsnMismatch,
    ReplicationResponseEndOfWalMismatch,
    ReplicationFrameLsnMismatch,
    UnexpectedRecordLsn,
    UnexpectedPreviousLsn,
    RecordAlreadyReceived,
    ConflictingReceivedRecord,
    MissingReceivedRecord,
    WrongCluster,
    WrongShard,
    WrongTable,
    WrongTimeline,
    WrongEpoch,
    InvalidTimelineSwitch,
    HAReplicationRecordMissingTableId,
    HAReplicationRecordMissingShardId,
    HAReplicationRecordUnknownTable,
    HAReplicationRecordUnknownShard,
    HAReplicationRecordApplyUnsupported,
    UnsupportedBatchMutationCodec,
    UnsupportedBatchMutationPayloadVersion,
    UnsupportedRecordKind,
    UnsupportedPayloadCodec,
    InvalidMagic,
    HeaderCrcMismatch,
    PayloadCrcMismatch,
    Other,
};

fn haStandbyReplicationErrorCode(err: anyerror) HAStandbyReplicationErrorCode {
    return switch (err) {
        error.HttpConnectionClosing => .HttpConnectionClosing,
        error.ConnectionResetByPeer => .ConnectionResetByPeer,
        error.ConnectionRefused => .ConnectionRefused,
        error.BrokenPipe => .BrokenPipe,
        error.EndOfStream => .EndOfStream,
        error.NoAddressReturned => .NoAddressReturned,
        error.UnexpectedHttpStatus => .UnexpectedHttpStatus,
        error.NotListening => .NotListening,
        error.UnsupportedReplicationFormat => .UnsupportedReplicationFormat,
        error.ReplicationResponseLsnMismatch => .ReplicationResponseLsnMismatch,
        error.ReplicationResponseEndOfWalMismatch => .ReplicationResponseEndOfWalMismatch,
        error.ReplicationFrameLsnMismatch => .ReplicationFrameLsnMismatch,
        error.UnexpectedRecordLsn => .UnexpectedRecordLsn,
        error.UnexpectedPreviousLsn => .UnexpectedPreviousLsn,
        error.RecordAlreadyReceived => .RecordAlreadyReceived,
        error.ConflictingReceivedRecord => .ConflictingReceivedRecord,
        error.MissingReceivedRecord => .MissingReceivedRecord,
        error.WrongCluster => .WrongCluster,
        error.WrongShard => .WrongShard,
        error.WrongTable => .WrongTable,
        error.WrongTimeline => .WrongTimeline,
        error.WrongEpoch => .WrongEpoch,
        error.InvalidTimelineSwitch => .InvalidTimelineSwitch,
        error.HAReplicationRecordMissingTableId => .HAReplicationRecordMissingTableId,
        error.HAReplicationRecordMissingShardId => .HAReplicationRecordMissingShardId,
        error.HAReplicationRecordUnknownTable => .HAReplicationRecordUnknownTable,
        error.HAReplicationRecordUnknownShard => .HAReplicationRecordUnknownShard,
        error.HAReplicationRecordApplyUnsupported => .HAReplicationRecordApplyUnsupported,
        error.UnsupportedBatchMutationCodec => .UnsupportedBatchMutationCodec,
        error.UnsupportedBatchMutationPayloadVersion => .UnsupportedBatchMutationPayloadVersion,
        error.UnsupportedRecordKind => .UnsupportedRecordKind,
        error.UnsupportedPayloadCodec => .UnsupportedPayloadCodec,
        error.InvalidMagic => .InvalidMagic,
        error.HeaderCrcMismatch => .HeaderCrcMismatch,
        error.PayloadCrcMismatch => .PayloadCrcMismatch,
        else => .Other,
    };
}

fn haStandbyReplicationErrorName(code: HAStandbyReplicationErrorCode) ?[]const u8 {
    return switch (code) {
        .none => null,
        .HttpConnectionClosing => "HttpConnectionClosing",
        .ConnectionResetByPeer => "ConnectionResetByPeer",
        .ConnectionRefused => "ConnectionRefused",
        .BrokenPipe => "BrokenPipe",
        .EndOfStream => "EndOfStream",
        .NoAddressReturned => "NoAddressReturned",
        .UnexpectedHttpStatus => "UnexpectedHttpStatus",
        .NotListening => "NotListening",
        .UnsupportedReplicationFormat => "UnsupportedReplicationFormat",
        .ReplicationResponseLsnMismatch => "ReplicationResponseLsnMismatch",
        .ReplicationResponseEndOfWalMismatch => "ReplicationResponseEndOfWalMismatch",
        .ReplicationFrameLsnMismatch => "ReplicationFrameLsnMismatch",
        .UnexpectedRecordLsn => "UnexpectedRecordLsn",
        .UnexpectedPreviousLsn => "UnexpectedPreviousLsn",
        .RecordAlreadyReceived => "RecordAlreadyReceived",
        .ConflictingReceivedRecord => "ConflictingReceivedRecord",
        .MissingReceivedRecord => "MissingReceivedRecord",
        .WrongCluster => "WrongCluster",
        .WrongShard => "WrongShard",
        .WrongTable => "WrongTable",
        .WrongTimeline => "WrongTimeline",
        .WrongEpoch => "WrongEpoch",
        .InvalidTimelineSwitch => "InvalidTimelineSwitch",
        .HAReplicationRecordMissingTableId => "HAReplicationRecordMissingTableId",
        .HAReplicationRecordMissingShardId => "HAReplicationRecordMissingShardId",
        .HAReplicationRecordUnknownTable => "HAReplicationRecordUnknownTable",
        .HAReplicationRecordUnknownShard => "HAReplicationRecordUnknownShard",
        .HAReplicationRecordApplyUnsupported => "HAReplicationRecordApplyUnsupported",
        .UnsupportedBatchMutationCodec => "UnsupportedBatchMutationCodec",
        .UnsupportedBatchMutationPayloadVersion => "UnsupportedBatchMutationPayloadVersion",
        .UnsupportedRecordKind => "UnsupportedRecordKind",
        .UnsupportedPayloadCodec => "UnsupportedPayloadCodec",
        .InvalidMagic => "InvalidMagic",
        .HeaderCrcMismatch => "HeaderCrcMismatch",
        .PayloadCrcMismatch => "PayloadCrcMismatch",
        .Other => "Other",
    };
}

fn isNonFatalHAStandbyReplicationError(err: anyerror) bool {
    return switch (err) {
        // Promotion is durably committed before the runtime adopts the
        // standby's resources. Keep the process available and fail closed so
        // operators can repair configuration or storage and the next round
        // can resume the idempotent adoption.
        error.HAPromotedPrimaryLogMissing,
        error.HAPromotedPrimarySlotsMissing,
        error.HAStandbyOwnershipRequired,
        error.HAStandbyOwnerMismatch,
        error.PromotedLogMismatch,
        error.PromotedProgressMismatch,
        error.LsmRootWriterAlreadyOpen,
        error.WriterLocked,
        error.InvalidPromotionHandoff,
        error.MissingPromotionSwitch,
        error.HttpConnectionClosing,
        error.ConnectionResetByPeer,
        error.ConnectionRefused,
        error.NoAddressReturned,
        error.BrokenPipe,
        error.EndOfStream,
        error.UnexpectedHttpStatus,
        error.NotListening,
        error.UnsupportedReplicationFormat,
        error.InternalReplicationDidNotAdvance,
        error.ReplicationResponseLsnMismatch,
        error.ReplicationResponseEndOfWalMismatch,
        error.ReplicationFrameLsnMismatch,
        error.UnexpectedRecordLsn,
        error.UnexpectedPreviousLsn,
        error.RecordAlreadyReceived,
        error.ConflictingReceivedRecord,
        error.MissingReceivedRecord,
        error.WrongCluster,
        error.WrongShard,
        error.WrongTable,
        error.WrongTimeline,
        error.WrongEpoch,
        error.NonMonotonicTimeline,
        error.InvalidTimelineSwitch,
        error.TimelineSwitchRequiresAppliedProgress,
        error.HAReplicationRecordMissingTableId,
        error.HAReplicationRecordMissingShardId,
        error.HAReplicationRecordUnknownTable,
        error.HAReplicationRecordUnknownShard,
        error.HAReplicationRecordApplyUnsupported,
        error.NotBatchMutationRecord,
        error.UnsupportedBatchMutationCodec,
        error.UnsupportedBatchMutationPayloadVersion,
        error.NotMetadataMutationRecord,
        error.UnsupportedMetadataMutationCodec,
        error.UnsupportedMetadataMutationPayloadVersion,
        error.UnsupportedMetadataMutationKind,
        error.UnsupportedRecordKind,
        error.UnsupportedPayloadCodec,
        error.UnsupportedVersion,
        error.UnsupportedHeaderLength,
        error.InvalidMagic,
        error.HeaderCrcMismatch,
        error.PayloadCrcMismatch,
        error.TrailingBytes,
        => true,
        else => false,
    };
}

fn isRetryableMetadataBootstrapError(err: anyerror) bool {
    return switch (err) {
        error.HttpConnectionClosing,
        error.ConnectionResetByPeer,
        error.ConnectionRefused,
        error.BrokenPipe,
        error.EndOfStream,
        error.UnexpectedHttpStatus,
        error.NotListening,
        error.NotLeader,
        error.ProposalDropped,
        error.LeaderTransferInProgress,
        error.StoreRegistrationNotVisible,
        => true,
        else => false,
    };
}

fn metadataBootstrapRetryDelayMs(registration: ?StoreRegistrationConfig, attempts: u32, now_ms: u64) u64 {
    const capped_attempts = @min(attempts, 5);
    const exponential = metadata_bootstrap_retry_base_ms << @intCast(capped_attempts -| 1);
    const bounded = @min(exponential, metadata_bootstrap_retry_max_ms);
    var hasher = std.hash.Wyhash.init(0x8d6a_2026_4c1f_b007);
    hashU64(&hasher, now_ms);
    hashU64(&hasher, attempts);
    if (registration) |record| {
        hashU64(&hasher, record.node_id);
        hashU64(&hasher, record.store_id);
    }
    const jitter = hasher.final() % (metadata_bootstrap_retry_jitter_ms + 1);
    return @min(bounded +| jitter, metadata_bootstrap_retry_max_ms + metadata_bootstrap_retry_jitter_ms);
}

fn chooseStoreStatusReportKind(
    ticks: usize,
    dirty: bool,
    last_report_at_ms: u64,
    due_heartbeat: bool,
    due_full_refresh: bool,
    due_data_raft_refresh: bool,
) StoreStatusReportKind {
    if (last_report_at_ms == 0 or dirty) return .full;
    const tick_due = ticks >= store_status_report_interval_ticks;
    if (!tick_due) return .none;
    if (due_full_refresh or due_data_raft_refresh) return .full;
    if (due_heartbeat) return .heartbeat;
    return .none;
}

test "data runtime treats metadata leadership churn as retryable bootstrap failure" {
    try std.testing.expect(isRetryableMetadataBootstrapError(error.NotLeader));
    try std.testing.expect(isRetryableMetadataBootstrapError(error.ProposalDropped));
    try std.testing.expect(isRetryableMetadataBootstrapError(error.LeaderTransferInProgress));
    try std.testing.expect(isRetryableMetadataBootstrapError(error.ConnectionRefused));
    try std.testing.expect(isRetryableMetadataBootstrapError(error.StoreRegistrationNotVisible));
    try std.testing.expect(!isRetryableMetadataBootstrapError(error.InvalidArguments));
}

test "data runtime metadata bootstrap retry delay is bounded and jittered" {
    const registration = StoreRegistrationConfig{
        .node_id = 7,
        .store_id = 11,
        .role = "data",
    };
    const first = metadataBootstrapRetryDelayMs(registration, 1, 1000);
    const later = metadataBootstrapRetryDelayMs(registration, 4, 1000);
    const capped = metadataBootstrapRetryDelayMs(registration, 16, 1000);
    try std.testing.expect(first >= metadata_bootstrap_retry_base_ms);
    try std.testing.expect(first <= metadata_bootstrap_retry_base_ms + metadata_bootstrap_retry_jitter_ms);
    try std.testing.expect(later >= first);
    try std.testing.expect(capped <= metadata_bootstrap_retry_max_ms + metadata_bootstrap_retry_jitter_ms);
}

test "data runtime store status scheduler publishes first and dirty reports immediately" {
    try std.testing.expectEqual(
        StoreStatusReportKind.full,
        chooseStoreStatusReportKind(0, true, 0, true, false, false),
    );
    try std.testing.expectEqual(
        StoreStatusReportKind.full,
        chooseStoreStatusReportKind(0, false, 0, true, false, false),
    );
    try std.testing.expectEqual(
        StoreStatusReportKind.full,
        chooseStoreStatusReportKind(0, true, 42, false, false, false),
    );
}

test "data runtime store status scheduler keeps clean periodic reports tick gated" {
    try std.testing.expectEqual(
        StoreStatusReportKind.none,
        chooseStoreStatusReportKind(store_status_report_interval_ticks - 1, false, 42, true, false, false),
    );
    try std.testing.expectEqual(
        StoreStatusReportKind.none,
        chooseStoreStatusReportKind(store_status_report_interval_ticks - 1, false, 42, false, false, true),
    );
    try std.testing.expectEqual(
        StoreStatusReportKind.none,
        chooseStoreStatusReportKind(store_status_report_interval_ticks - 1, false, 42, false, true, false),
    );
    try std.testing.expectEqual(
        StoreStatusReportKind.heartbeat,
        chooseStoreStatusReportKind(store_status_report_interval_ticks, false, 42, true, false, false),
    );
    try std.testing.expectEqual(
        StoreStatusReportKind.full,
        chooseStoreStatusReportKind(store_status_report_interval_ticks, false, 42, false, true, false),
    );
    try std.testing.expectEqual(
        StoreStatusReportKind.full,
        chooseStoreStatusReportKind(store_status_report_interval_ticks, false, 42, false, false, true),
    );
}

test "idle cached runtime status stays fresh only for the published root generation" {
    const fresh = runtime_status.LocalTableRuntimeStatus{
        .group_id = 7,
        .metadata = .{
            .source = .live_writer_publish,
            .freshness = .fresh,
            .lsm_root_generation = 3,
        },
        .stats = .{},
    };
    try std.testing.expect(DataServer.cachedRuntimeStatusRemainsFresh(fresh, 3));
    try std.testing.expect(!DataServer.cachedRuntimeStatusRemainsFresh(fresh, 4));

    var pending = fresh;
    pending.stats.enrichment.target_sequence = 9;
    pending.stats.enrichment.applied_sequence = 8;
    try std.testing.expect(!DataServer.cachedRuntimeStatusRemainsFresh(pending, 3));
}

test "data runtime stamps one producer generation on every reported group" {
    const alloc = std.testing.allocator;
    const statuses = try alloc.alloc(runtime_status.LocalTableRuntimeStatus, 2);
    statuses[0] = .{ .group_id = 7, .stats = .{} };
    statuses[1] = .{
        .group_id = 8,
        .metadata = .{ .store_id = 99, .node_id = 100, .status_generation = 1 },
        .stats = .{},
    };
    var snapshot = runtime_status.TableRuntimeSnapshot{
        .table_name = try alloc.dupe(u8, "docs"),
        .statuses = .{ .items = statuses },
    };
    defer snapshot.deinit(alloc);

    DataServer.annotateRuntimeSnapshotOwner(&snapshot, .{
        .store_id = 11,
        .node_id = 13,
    }, 17);

    try std.testing.expectEqual(@as(u64, 11), statuses[0].metadata.store_id);
    try std.testing.expectEqual(@as(u64, 13), statuses[0].metadata.node_id);
    try std.testing.expectEqual(@as(u64, 17), statuses[0].metadata.status_generation);
    try std.testing.expectEqual(@as(u64, 99), statuses[1].metadata.store_id);
    try std.testing.expectEqual(@as(u64, 100), statuses[1].metadata.node_id);
    try std.testing.expectEqual(@as(u64, 17), statuses[1].metadata.status_generation);
}

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
    voter_set_known: bool = false,
    voter_set_fingerprint: antfly.metadata.table_manager.VoterSetFingerprint = [_]u8{0} ** antfly.metadata.table_manager.voter_set_fingerprint_len,
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
                            .voter_set_known = true,
                            .voter_set_fingerprint = antfly.metadata.table_manager.voterSetFingerprint(raft_status.conf_state.voters, null),
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
                            .voter_set_known = true,
                            .voter_set_fingerprint = antfly.metadata.table_manager.voterSetFingerprint(raft_status.conf_state.voters, null),
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

const TransitionDbLeaseContext = struct {
    alloc: std.mem.Allocator,
    cached: antfly.public_api.ProvisionedTableWriteCache.CachedDb,

    fn release(ptr: *anyopaque) void {
        const self: *TransitionDbLeaseContext = @ptrCast(@alignCast(ptr));
        self.cached.deinit(self.alloc);
        self.alloc.destroy(self);
    }
};

pub const DataServer = struct {
    alloc: std.mem.Allocator,
    remote_metadata: ?*RemoteMetadataSource = null,
    metadata_service: ?*antfly.metadata_service.MetadataService = null,
    metadata_http_service: ?*antfly.metadata_service.MetadataHttpService = null,
    data_raft: ?*antfly.raft.ManagedHttpHostService = null,
    data_raft_mutex: std.atomic.Mutex = .unlocked,
    local_transition_mutex: std.atomic.Mutex = .unlocked,
    data_raft_factory: ?*DataDescriptorFactory = null,
    data_raft_store: ?*raft_engine.core.MemoryStorage = null,
    data_raft_apply: ?*RaftTableApplyStateMachine = null,
    data_raft_base_uri: ?[]u8 = null,
    metadata_local_providers_registered: bool = false,
    store_registration: ?StoreRegistrationConfig = null,
    store_registration_confirmed: bool = false,
    metadata_bootstrap_retry_mutex: std.atomic.Mutex = .unlocked,
    metadata_bootstrap_retry_attempts: u32 = 0,
    next_metadata_bootstrap_retry_at_ms: u64 = 0,
    group_leadership_source: ?GroupLeadershipSource = null,
    group_membership_source: ?GroupMembershipSource = null,
    local_transition_runtime: ?antfly.raft.TransitionRuntime = null,
    store_status_ticks: std.atomic.Value(usize) = .init(0),
    store_status_dirty: std.atomic.Value(bool) = .init(true),
    last_store_status_report_at_ms: u64 = 0,
    last_data_raft_metadata_sync_at_ms: u64 = 0,
    last_data_raft_placement_fingerprint: ?u64 = null,
    last_data_raft_status_fingerprint: ?u64 = null,
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
    runtime_status_generation: std.atomic.Value(u64) = .init(1),
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
    provisioned_startup_catch_up_last_quarantined_groups: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_last_duration_ns: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_last_run_at_ms: std.atomic.Value(u64) = .init(0),
    provisioned_startup_catch_up_epoch: std.atomic.Value(u64) = .init(1),
    provisioned_startup_catch_up_not_before_ms: std.atomic.Value(u64) = .init(0),
    runtime_status_dirty: std.atomic.Value(bool) = .init(true),
    runtime_status_last_refresh_at_ms: std.atomic.Value(u64) = .init(0),
    runtime_status_disk_usage_cache_mutex: std.atomic.Mutex = .unlocked,
    runtime_status_disk_usage_cache: std.AutoHashMapUnmanaged(u64, RuntimeStatusDiskUsageCacheEntry) = .empty,
    store_status_cache_mutex: std.atomic.Mutex = .unlocked,
    store_status_heartbeat_cache: StoreStatusHeartbeatCache = .{},
    provisioned_storage: antfly.public_api.ProvisionedGroupStorage,
    read_source: antfly.public_api.ProvisionedTableReadSource,
    write_source: antfly.public_api.ProvisionedTableWriteSource,
    replica_catalog_path: ?[]const u8 = null,
    /// Long-lived backing for the cross-shard entity-resolution candidate
    /// source; its `CandidateSource` vtable points into this field, so it must
    /// not move. Wraps `read_source.source()` and is handed to the write
    /// source(s) so every managed DB blocks entity resolution across shards.
    distributed_candidate_source: ?antfly.public_api.DistributedCandidateSource = null,
    /// Long-lived backing for the promoter's cross-shard entity sink; its
    /// `EntitySink` vtable points into this field, so it must not move. Wraps
    /// `write_source.source()` and is handed to the write source(s) so the
    /// promoter upserts canonical entities into whichever shard owns them.
    distributed_entity_sink: ?antfly.public_api.DistributedEntitySink = null,
    status_source: antfly.public_api.http_server.StatusSource,
    http_server: ?antfly.public_api.ApiHttpServer = null,
    api_server_cfg: antfly.public_api.http_server.ApiHttpServerConfig,
    ha_cfg: DataServerHAConfig = .{},
    ha_state_mutex: std.atomic.Mutex = .unlocked,
    /// Global primary mutation/capture ordering point. All DB/catalog writers
    /// share this instance through their HA mirror configuration.
    ha_mutation_barrier: antfly.db.HAMutationBarrier = .{},
    ha_public_gate_state: antfly.ha.public_gate_state.State = .{},
    ha_admin_server: ?antfly.ha.http_admin.Server = null,
    ha_internal_server: ?antfly.ha.http_internal.Server = null,
    ha_standby_replication_http_executor: ?antfly.common.http.StdHttpExecutor = null,
    ha_promoted_primary: ?antfly.ha.primary.Primary = null,
    ha_primary_sync_wait: antfly.db.HAPrimaryProgressSyncWait = .{},
    ha_primary_mirror_last_lsn: std.atomic.Value(u64) = .init(0),
    ha_primary_mirror_failure_count: std.atomic.Value(u64) = .init(0),
    ha_primary_mirror_last_gate_lsn: std.atomic.Value(u64) = .init(0),
    ha_primary_mirror_last_gate_action: std.atomic.Value(u8) = .init(255),
    ha_primary_mirror_sync_reject_count: std.atomic.Value(u64) = .init(0),
    ha_primary_mirror_sync_wait_count: std.atomic.Value(u64) = .init(0),
    ha_primary_mirror_sync_degraded_count: std.atomic.Value(u64) = .init(0),
    ha_standby_replication_failure_count: std.atomic.Value(u64) = .init(0),
    ha_standby_replication_last_error: std.atomic.Value(u8) = .init(@intFromEnum(HAStandbyReplicationErrorCode.none)),
    ha_standby_replication_last_attempt_ns: std.atomic.Value(u64) = .init(0),
    ha_standby_replication_last_success_ns: std.atomic.Value(u64) = .init(0),
    ha_standby_replication_next_attempt_ns: std.atomic.Value(u64) = .init(0),
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
    lsm_maintenance_obsolete_reclaim_due_ns: std.atomic.Value(u64) = .init(0),
    dense_posting_maintenance_next_eligible_ns: std.atomic.Value(u64) = .init(0),

    const lsm_maintenance_worker_idle_sleep_ns = 250 * std.time.ns_per_ms;
    const lsm_maintenance_worker_retry_sleep_ns = 100 * std.time.ns_per_ms;
    const lsm_maintenance_worker_bulk_defer_ns = 500 * std.time.ns_per_ms;
    const lsm_maintenance_worker_pressure_defer_ns = 500 * std.time.ns_per_ms;
    const lsm_maintenance_worker_max_steps_per_wake = 8;
    // Dense posting repair is time-driven rather than score-driven: checking
    // the backlog requires a posting-state scan, so the worker probes on a
    // fixed cadence and retries quickly only while repairs are landing.
    const dense_posting_maintenance_idle_interval_ns = 30 * std.time.ns_per_s;
    const dense_posting_maintenance_retry_interval_ns = 1 * std.time.ns_per_s;
    const ha_replication_default_max_records_per_apply = 256;
    const ha_replication_default_max_encoded_bytes_per_apply = 4 * 1024 * 1024;
    const ha_replication_default_max_response_bytes = 8 * 1024 * 1024;
    const ha_standby_replication_retry_interval_ns = 1 * std.time.ns_per_s;

    fn haStandbyReplicationHTTPExecutorConfig() antfly.common.http.StdHttpExecutorConfig {
        return .{
            .max_response_bytes = ha_replication_default_max_response_bytes,
            .resolve_before_connect = true,
        };
    }

    const ProvisionedWarmupStats = struct {
        warmed_group_count: u64 = 0,
        duration_ns: u64 = 0,
    };

    const ProvisionedStartupCatchUpStats = struct {
        group_count: u64 = 0,
        groups_with_debt: u64 = 0,
        groups_cleared: u64 = 0,
        busy_groups: u64 = 0,
        quarantined_groups: u64 = 0,
        debt_remaining: bool = false,
        unparked_debt_remaining: bool = false,
        earliest_retry_at_ms: u64 = 0,
        duration_ns: u64 = 0,
    };

    fn startupCatchUpNotBeforeMs(stats: ProvisionedStartupCatchUpStats, inspection_complete: bool) u64 {
        if (!inspection_complete or !stats.debt_remaining or stats.unparked_debt_remaining) return 0;
        return stats.earliest_retry_at_ms;
    }

    const StartupCatchUpCompletion = struct {
        dirty: bool,
        not_before_ms: u64,
    };

    fn startupCatchUpCompletion(
        stats: ProvisionedStartupCatchUpStats,
        inspection_complete: bool,
        started_epoch: u64,
        current_epoch: u64,
    ) StartupCatchUpCompletion {
        if (started_epoch != current_epoch) return .{ .dirty = true, .not_before_ms = 0 };
        return .{
            .dirty = if (inspection_complete) stats.debt_remaining else true,
            .not_before_ms = startupCatchUpNotBeforeMs(stats, inspection_complete),
        };
    }

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
            .replica_catalog_path = cfg.replica_catalog_path,
            .status_source = status_source,
            .api_server_cfg = cfg.api_server_cfg,
            .ha_cfg = cfg.ha,
            .ha_primary_sync_wait = haPrimarySyncWaitFromConfig(cfg.ha.primary_sync_wait),
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
            .replica_catalog_path = cfg.replica_catalog_path,
            .status_source = antfly.public_api.http_server.StatusSource.fromMetadataService(svc),
            .api_server_cfg = cfg.api_server_cfg,
            .ha_cfg = cfg.ha,
            .ha_primary_sync_wait = haPrimarySyncWaitFromConfig(cfg.ha.primary_sync_wait),
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
            .replica_catalog_path = cfg.replica_catalog_path,
            .status_source = antfly.public_api.http_server.StatusSource.fromMetadataHttpService(svc),
            .api_server_cfg = cfg.api_server_cfg,
            .ha_cfg = cfg.ha,
            .ha_primary_sync_wait = haPrimarySyncWaitFromConfig(cfg.ha.primary_sync_wait),
            .query_async_limit = cfg.query_async_limit,
            .backend_runtime = cfg.backend_runtime,
            .listener_cfg = publicApiListenerConfig(cfg.bind_host, cfg.bind_port),
        };
    }

    /// Initialize the ApiHttpServer without creating a listener.
    /// Call this when you want to use the API server with an external
    /// httpx.Server instead of the built-in StdHttpListener.
    pub fn initApiServer(self: *DataServer) !void {
        if (self.http_server != null) return;
        var api_server_cfg = self.api_server_cfg;
        var owned_restore_job_store_path: ?[]u8 = null;
        defer if (owned_restore_job_store_path) |path| self.alloc.free(path);
        // Filesystem-backed runtimes keep API job state beside their shard
        // catalog by default. Lite overrides this after construction with an
        // engine namespace so its artifact remains genuinely single-file.
        if (api_server_cfg.restore_job_store_path == null and
            api_server_cfg.session_store_path == null and
            api_server_cfg.session_store == null and
            api_server_cfg.deployment_mode != .serverless)
        {
            owned_restore_job_store_path = try std.fmt.allocPrint(self.alloc, "{s}/api-restore-jobs", .{self.write_source.replica_root_dir});
            api_server_cfg.restore_job_store_path = owned_restore_job_store_path;
        }
        api_server_cfg.shard_ops = self.localShardOperationAdapter();
        api_server_cfg.shard_db_adapter = self.localShardDbAdapter();
        api_server_cfg.backend_runtime = self.backend_runtime;
        self.configureHAPublicGateState();
        self.attachHaExecutors(&api_server_cfg);
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
        _ = self.read_source.withHAReadGate(self.haReadGate());
        const ha_write_gate = self.haWriteGate();
        const ha_primary_mirror = self.haPrimaryMirror();
        _ = try self.write_source.withHAWriteGate(ha_write_gate);
        _ = try self.write_source.withHAMirror(ha_primary_mirror);
        if (self.data_raft_apply) |apply_sm| {
            apply_sm.attachProvisionedStorage(&self.provisioned_storage);
            _ = apply_sm.write_source.withSecretStore(api_server_cfg.secret_store);
            apply_sm.write_cache.secret_store = api_server_cfg.secret_store;
            _ = apply_sm.write_source.withRemoteContent(api_server_cfg.remote_content);
            apply_sm.write_cache.remote_content = api_server_cfg.remote_content;
            _ = try apply_sm.write_source.withHAWriteGate(ha_write_gate);
            _ = try apply_sm.write_source.withHAMirror(ha_primary_mirror);
            apply_sm.write_source.setLocalChangeHook(self.localChangeHook());
            _ = self.write_source.withLocalWriteOwner(&apply_sm.write_source);
        }
        self.read_source.primary_lookup_db = self.localPrimaryLookupDbSource();
        self.write_source.setLocalChangeHook(self.localChangeHook());
        _ = self.write_source.withRaftBatcher(if (self.data_raft != null) self.localRaftBatcher() else null);
        const promotion_leadership = self.promotionLeadershipSource();
        _ = self.write_source.withPromotionLeadershipSource(promotion_leadership);
        if (self.data_raft_apply) |apply_sm| {
            _ = apply_sm.write_source.withPromotionLeadershipSource(promotion_leadership);
        }

        // Cross-shard entity-resolution blocking: wrap the routing-aware read
        // source so resolution workers fetch candidate entities from whichever
        // shard owns the entity table, then hand it to the write source(s) that
        // open managed DBs. Captures `read_source.source()` (ptr+vtable), so
        // later read-source mutations remain visible.
        self.distributed_candidate_source = .{ .reads = self.read_source.source() };
        const candidate_source = self.distributed_candidate_source.?.candidateSource();
        _ = self.write_source.withResolutionCandidateSource(candidate_source);
        if (self.data_raft_apply) |apply_sm| {
            _ = apply_sm.write_source.withResolutionCandidateSource(candidate_source);
        }

        // The promoter's cross-shard entity sink: wrap the routing-aware write
        // source so the promoter upserts canonical entity documents into the
        // shard that owns each entity key, then hand it to the write source(s)
        // that open managed DBs.
        // Promote each document's entities atomically through the 2PC commit
        // path (multi-participant across the entity table's shards).
        self.distributed_entity_sink = .{ .writes = self.write_source.source(), .transactional = true };
        const entity_sink = self.distributed_entity_sink.?.entitySink();
        _ = self.write_source.withEntitySink(entity_sink);
        if (self.data_raft_apply) |apply_sm| {
            _ = apply_sm.write_source.withEntitySink(entity_sink);
        }
        self.syncInferenceRuntimeConfig();
        // Request allocations must share identity with the process allocator
        // used by DB internals (c_allocator when libc is linked): search hits
        // adopt index-owned metadata buffers, so a distinct request allocator
        // identity turns those adoptions into cross-allocator frees.
        self.http_server = try antfly.public_api.ApiHttpServer.initWithConfig(
            self.alloc,
            api_server_cfg,
            self.status_source,
            self.read_source.source(),
            self.write_source.source(),
        );
        self.http_server.?.antfly_provider = self.read_source.antfly_provider;
    }

    pub fn applyHAReplicationRecord(self: *DataServer, record: antfly.ha.replication_record.RecordView) !void {
        var snapshot = try self.write_source.catalog.adminSnapshot();
        defer self.write_source.catalog.freeAdminSnapshot(&snapshot);
        const route = try resolveHAReplicationRecordRoute(&snapshot, record);
        try self.write_source.applyHAReplicationRecordGroupLocal(
            self.alloc,
            route.group_id,
            route.table_name,
            record,
        );
    }

    pub fn applyHAReplicationRecordCallback(ctx: *anyopaque, record: antfly.ha.replication_record.RecordView) anyerror!void {
        const self: *DataServer = @ptrCast(@alignCast(ctx));
        try self.applyHAReplicationRecord(record);
    }

    const HAStandbyFetchSnapshot = struct {
        identity: antfly.ha.standby.Identity,
        requested_lsn: u64,
    };

    pub fn replicateHAStandbyAvailable(
        self: *DataServer,
        executor: antfly.common.http.RequestExecutor,
        upstream_base_uri: []const u8,
        slot_name: []const u8,
        options: HAStandbyReplicationOptions,
    ) !HAStandbyReplicationResult {
        if (self.http_server == null) try self.initApiServer();
        var client = antfly.ha.http_replication_client.Client.initWithOptions(
            self.alloc,
            executor,
            self.haStandbyReplicationAuth(),
        );
        return try self.replicateHAStandbyAvailableWithClient(&client, upstream_base_uri, slot_name, options);
    }

    pub fn replicateHAStandbyUntilCaughtUp(
        self: *DataServer,
        executor: antfly.common.http.RequestExecutor,
        upstream_base_uri: []const u8,
        slot_name: []const u8,
        options: HAStandbyReplicationOptions,
    ) !HAStandbyReplicationLoopResult {
        if (self.http_server == null) try self.initApiServer();
        var client = antfly.ha.http_replication_client.Client.initWithOptions(
            self.alloc,
            executor,
            self.haStandbyReplicationAuth(),
        );
        var iterations: usize = 0;
        var received_count: usize = 0;
        var applied_count: usize = 0;
        var batch_options = options;

        while (true) {
            const result = try self.replicateHAStandbyAvailableWithClient(
                &client,
                upstream_base_uri,
                slot_name,
                batch_options,
            );
            batch_options.verify_upstream = false;
            iterations += 1;
            received_count += result.received_count;
            applied_count += result.applied_count;

            if (result.end_of_wal) {
                return .{
                    .iterations = iterations,
                    .received_count = received_count,
                    .applied_count = applied_count,
                    .progress = result.progress,
                    .current_lsn = result.current_lsn,
                    .last_sent_lsn = result.last_sent_lsn,
                    .next_lsn = result.next_lsn,
                };
            }
            if (result.received_count == 0 and result.applied_count == 0) {
                return error.InternalReplicationDidNotAdvance;
            }
        }
    }

    fn haStandbyReplicationAuth(self: *const DataServer) antfly.ha.http_replication_client.AuthOptions {
        const configured = if (self.ha_cfg.standby_replication) |cfg| cfg.bearer_token else null;
        return .{ .bearer_token = configured orelse self.ha_cfg.admin_bearer_token };
    }

    fn replicateHAStandbyAvailableWithClient(
        self: *DataServer,
        client: *antfly.ha.http_replication_client.Client,
        upstream_base_uri: []const u8,
        slot_name: []const u8,
        options: HAStandbyReplicationOptions,
    ) !HAStandbyReplicationResult {
        const snapshot = try self.snapshotHAStandbyForFetch();
        var batch_options = options;
        if (batch_options.max_records == 0) {
            batch_options.max_records = ha_replication_default_max_records_per_apply;
        }
        if (batch_options.max_encoded_bytes == 0) {
            batch_options.max_encoded_bytes = ha_replication_default_max_encoded_bytes_per_apply;
        }
        var batch = try client.fetchAvailable(
            upstream_base_uri,
            slot_name,
            snapshot.identity,
            snapshot.requested_lsn,
            batch_options,
        );
        defer batch.deinit();

        lockAtomic(&self.ha_state_mutex);
        const ctx = self.ha_cfg.admin_context orelse {
            self.ha_state_mutex.unlock();
            return error.HAStandbyStateChanged;
        };
        const standby = ctx.standby orelse {
            self.ha_state_mutex.unlock();
            return error.HAStandbyStateChanged;
        };
        if (!std.meta.eql(standby.identity, snapshot.identity) or
            standby.nextReceiveLsn() != snapshot.requested_lsn)
        {
            self.ha_state_mutex.unlock();
            return error.HAStandbyStateChanged;
        }

        const applied = client.applyFetched(
            &batch,
            standby,
            self,
            DataServer.applyHAReplicationRecordCallback,
        ) catch |err| {
            const status_identity = standby.identity;
            const status_progress = standby.currentProgress();
            self.ha_public_gate_state.publishStandbyProgress(status_progress);
            self.ha_state_mutex.unlock();
            _ = client.updateStandbyStatusSnapshot(
                upstream_base_uri,
                slot_name,
                status_identity,
                status_progress,
            ) catch {};
            return err;
        };
        const status_identity = standby.identity;
        self.ha_public_gate_state.publishStandbyProgress(applied.progress);
        self.ha_state_mutex.unlock();

        try client.updateStandbyStatusSnapshot(
            upstream_base_uri,
            slot_name,
            status_identity,
            applied.progress,
        );
        return .{
            .received_count = applied.received_count,
            .applied_count = applied.applied_count,
            .progress = applied.progress,
            .current_lsn = batch.current_lsn,
            .last_sent_lsn = batch.last_sent_lsn,
            .next_lsn = batch.next_lsn,
            .end_of_wal = batch.end_of_wal,
        };
    }

    fn snapshotHAStandbyForFetch(self: *DataServer) !HAStandbyFetchSnapshot {
        lockAtomic(&self.ha_state_mutex);
        defer self.ha_state_mutex.unlock();
        const ctx = self.ha_cfg.admin_context orelse return error.HAStandbyNotConfigured;
        const standby = ctx.standby orelse return error.HAStandbyNotConfigured;
        return .{
            .identity = standby.identity,
            .requested_lsn = standby.nextReceiveLsn(),
        };
    }

    fn runHAStandbyReplicationRound(self: *DataServer) !void {
        const cfg = self.ha_cfg.standby_replication orelse return;
        lockAtomic(&self.ha_state_mutex);
        const promoted = self.openPromotedPrimaryFromStandbyIfReady(cfg) catch |err| {
            self.ha_state_mutex.unlock();
            return err;
        };
        self.ha_state_mutex.unlock();
        if (promoted) return;
        const executor = try self.haStandbyReplicationExecutor(cfg);
        self.recordHAStandbyReplicationAttempt();
        if (cfg.catch_up_until_end_of_wal) {
            _ = try self.replicateHAStandbyUntilCaughtUp(
                executor,
                cfg.upstream_base_uri,
                cfg.slot_name,
                cfg.options,
            );
        } else {
            _ = try self.replicateHAStandbyAvailable(
                executor,
                cfg.upstream_base_uri,
                cfg.slot_name,
                cfg.options,
            );
        }
        self.recordHAStandbyReplicationSuccess();
    }

    fn openPromotedPrimaryFromStandbyIfReady(self: *DataServer, cfg: HAStandbyReplicationConfig) !bool {
        if (self.ha_cfg.admin_context == null) return false;
        if (self.ha_cfg.admin_context.?.standby == null) return false;
        if (self.ha_promoted_primary != null) return true;

        const standby = self.ha_cfg.admin_context.?.standby.?;
        const handoff = standby.promotedPrimaryHandoff() catch |err| switch (err) {
            error.StandbyNotPromoted, error.PromotionNotApplied => return false,
            else => return err,
        };
        const log_path = cfg.standby_log_path orelse return error.HAPromotedPrimaryLogMissing;
        const progress_path = cfg.standby_progress_path orelse return error.HAPromotedPrimarySlotsMissing;
        try self.validateHAStandbyPromotionOwner(standby);
        var io_impl = std.Io.Threaded.init(self.alloc, .{});
        defer io_impl.deinit();
        if (!try fs_paths.pathsReferToSameExistingFile(self.alloc, io_impl.io(), log_path, standby.receiveLogPath())) {
            return error.PromotedLogMismatch;
        }
        if (!try fs_paths.pathsReferToSameExistingFile(self.alloc, io_impl.io(), progress_path, standby.progressWalPath())) {
            return error.PromotedProgressMismatch;
        }

        const slots_path_buf = try std.fmt.allocPrint(self.alloc, "{s}.promoted-primary-slots", .{progress_path});
        defer self.alloc.free(slots_path_buf);
        const slots_path = try self.alloc.dupeZ(u8, slots_path_buf);
        defer self.alloc.free(slots_path);

        try self.write_source.prepareHAConfigTransition();
        if (self.data_raft_apply) |apply_sm| try apply_sm.write_source.prepareHAConfigTransition();
        var promoted_primary = try antfly.ha.primary.Primary.adoptPromotedStandby(
            self.alloc,
            standby,
            slots_path.ptr,
            handoff,
            .{},
        );
        errdefer promoted_primary.close();

        self.ha_public_gate_state.beginPromotion();
        self.ha_cfg.standby_owner.?.* = null;
        self.ha_cfg.admin_context.?.standby = null;
        if (self.ha_admin_server) |*server| {
            server.ctx.standby = null;
        }

        self.ha_promoted_primary = promoted_primary;
        const promoted_primary_handle = &self.ha_promoted_primary.?;
        self.ha_cfg.internal_primary = promoted_primary_handle;
        self.ha_cfg.standby_replication = null;
        const promoted_node_id = self.ha_cfg.admin_context.?.standby_node_id;
        self.ha_cfg.admin_context.?.primary = promoted_primary_handle;
        self.ha_cfg.admin_context.?.primary_node_id = promoted_node_id;
        self.ha_cfg.admin_context.?.promoted_standby_handoff = handoff;
        if (self.ha_admin_server) |*server| {
            server.ctx.primary = promoted_primary_handle;
            server.ctx.primary_node_id = promoted_node_id;
            server.ctx.promoted_standby_handoff = handoff;
            server.auth.seed_capture = if (self.ha_cfg.seed_capture_root != null) .{
                .ptr = self,
                .run_fn = DataServer.captureHASeedCallback,
            } else null;
            if (server.auth.lifecycle_receipts) |*receipts| {
                receipts.node_id = promoted_node_id;
                receipts.role = .primary;
            }
        }
        self.ha_internal_server = null;
        self.api_server_cfg.ha_internal_executor = null;
        if (self.ha_cfg.admin_context.?.primary) |handle| {
            self.ha_internal_server = antfly.ha.http_internal.Server.initWithOptions(platform.allocator.processAllocator(std.heap.smp_allocator), handle, .{
                .state_mutex = &self.ha_state_mutex,
            });
            self.api_server_cfg.ha_internal_executor = self.ha_internal_server.?.executor();
        }
        if (self.http_server) |*server| {
            server.setHAInternalExecutor(self.api_server_cfg.ha_internal_executor);
        }
        self.rewireHAPromotionMirrors();
        self.ha_public_gate_state.publishPrimary(
            promoted_primary_handle,
            haContextPrimaryIsFenced(self.ha_cfg.admin_context.?),
        );
        return true;
    }

    fn validateHAStandbyPromotionOwner(self: *DataServer, standby: *antfly.ha.standby.Standby) !void {
        const owner = self.ha_cfg.standby_owner orelse return error.HAStandbyOwnershipRequired;
        if (owner.*) |*owned| {
            if (owned != standby) return error.HAStandbyOwnerMismatch;
            return;
        }
        return error.HAStandbyOwnershipRequired;
    }

    fn rewireHAPromotionMirrors(self: *DataServer) void {
        const ha_primary_mirror = self.haPrimaryMirror();
        _ = self.write_source.withPreparedHAMirror(ha_primary_mirror);
        if (self.data_raft_apply) |apply_sm| {
            _ = apply_sm.write_source.withPreparedHAMirror(ha_primary_mirror);
        }
    }

    fn haStandbyReplicationExecutor(
        self: *DataServer,
        cfg: HAStandbyReplicationConfig,
    ) !antfly.common.http.RequestExecutor {
        if (cfg.executor) |executor| return executor;
        if (self.ha_standby_replication_http_executor == null) {
            self.ha_standby_replication_http_executor = antfly.common.http.StdHttpExecutor.init(
                self.alloc,
                haStandbyReplicationHTTPExecutorConfig(),
            );
        }
        return self.ha_standby_replication_http_executor.?.executor();
    }

    fn configureHAPublicGateState(self: *DataServer) void {
        const ctx = self.ha_cfg.admin_context orelse return;
        if (ctx.standby) |standby| {
            self.publishHAStandbyPublicGateState(standby, true);
            return;
        }
        if (ctx.primary) |primary| {
            self.ha_public_gate_state.configurePrimary(primary, haContextPrimaryIsFenced(ctx));
        }
    }

    fn refreshHAPublicGateState(self: *DataServer) void {
        const ctx = self.ha_cfg.admin_context orelse return;
        if (ctx.standby) |standby| {
            self.publishHAStandbyPublicGateState(standby, false);
            return;
        }
        if (ctx.primary != null) {
            self.ha_public_gate_state.publishPrimaryFence(haContextPrimaryIsFenced(ctx));
        }
    }

    fn publishHAStandbyPublicGateState(
        self: *DataServer,
        standby: *antfly.ha.standby.Standby,
        configure_role: bool,
    ) void {
        _ = standby.promotedPrimaryHandoff() catch |err| switch (err) {
            error.StandbyNotPromoted, error.PromotionNotApplied => {
                if (configure_role) {
                    self.ha_public_gate_state.configureStandby(standby.currentProgress());
                } else {
                    self.ha_public_gate_state.publishStandbyProgress(standby.currentProgress());
                }
                return;
            },
            else => {
                self.ha_public_gate_state.beginPromotion();
                return;
            },
        };
        self.ha_public_gate_state.beginPromotion();
    }

    fn haPublicGateStateChangedCallback(ptr: *anyopaque) void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        self.refreshHAPublicGateState();
    }

    fn haPrimaryFenceStartedCallback(ptr: *anyopaque) void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        // Called while ha_state_mutex is held. HA WAL mirror/ack paths take
        // the same mutex, so publishing the fence here establishes a strict
        // linearization point before the durable primary tail is sampled.
        self.ha_public_gate_state.publishPrimaryFence(true);
    }

    fn haWriteGate(self: *DataServer) ?antfly.db.HAWriteGate {
        const ctx = self.ha_cfg.admin_context orelse return null;
        if (ctx.standby != null or ctx.primary != null) return .{ .shared = .{
            .state = &self.ha_public_gate_state,
        } };
        return null;
    }

    fn haPrimaryMirror(self: *DataServer) ?antfly.db.HAAsyncEffectMirror {
        if (self.ha_cfg.admin_context) |ctx| {
            if (ctx.standby != null) return null;
            if (ctx.primary) |primary| return self.haPrimaryMirrorFor(primary);
        }
        if (self.ha_cfg.internal_primary) |primary| return self.haPrimaryMirrorFor(primary);
        return null;
    }

    fn haPrimaryMirrorFor(self: *DataServer, primary: *antfly.ha.primary.Primary) antfly.db.HAAsyncEffectMirror {
        var mirror = antfly.db.HAAsyncEffectMirror{
            .primary = primary,
            .mutation_barrier = &self.ha_mutation_barrier,
            .transition_mutex = &self.ha_state_mutex,
            .last_lsn = &self.ha_primary_mirror_last_lsn,
            .failure_count = &self.ha_primary_mirror_failure_count,
            .sync_policy = self.ha_cfg.primary_sync_policy,
            .last_gate_lsn = &self.ha_primary_mirror_last_gate_lsn,
            .last_gate_action = &self.ha_primary_mirror_last_gate_action,
            .sync_reject_count = &self.ha_primary_mirror_sync_reject_count,
            .sync_wait_count = &self.ha_primary_mirror_sync_wait_count,
            .sync_degraded_count = &self.ha_primary_mirror_sync_degraded_count,
        };
        if (mirror.sync_policy.mode != .async and mirror.sync_policy.failure_policy == .block) {
            mirror.sync_wait_ctx = &self.ha_primary_sync_wait;
            mirror.sync_wait_fn = antfly.db.HAPrimaryProgressSyncWait.wait;
        }
        return mirror;
    }

    fn haReadGate(self: *DataServer) ?antfly.public_api.HAReadGate {
        const ctx = self.ha_cfg.admin_context orelse return null;
        if (ctx.standby != null or ctx.primary != null) return .{ .shared = &self.ha_public_gate_state };
        return null;
    }

    fn haOwnerJobCanRun(
        self: *DataServer,
        kind: antfly.ha.owner_job_gate.JobKind,
    ) bool {
        _ = kind;
        return self.ha_public_gate_state.ownerJobsCanRun();
    }

    fn defaultHASeedSnapshotProvider(self: *DataServer) HASeedSnapshotProvider {
        return .{ .ptr = self, .prepare_fn = prepareDefaultHASeedSnapshotCallback };
    }

    fn prepareDefaultHASeedSnapshotCallback(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        request: HASeedSnapshotRequest,
    ) !HASeedPreparedSnapshot {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        return try self.prepareDefaultHASeedSnapshot(alloc, request);
    }

    fn prepareDefaultHASeedSnapshot(
        self: *DataServer,
        alloc: std.mem.Allocator,
        request: HASeedSnapshotRequest,
    ) !HASeedPreparedSnapshot {
        const catalog_path = self.replica_catalog_path orelse return error.HASeedCaptureCatalogMissing;
        const snapshots_root = try std.fmt.allocPrint(alloc, "{s}.runtime-snapshots", .{request.capture_root});
        defer alloc.free(snapshots_root);
        const building_name = try std.fmt.allocPrint(alloc, ".building-{s}", .{request.generation});
        defer alloc.free(building_name);
        const building_root = try std.fs.path.join(alloc, &.{ snapshots_root, building_name });
        defer alloc.free(building_root);
        const published_root = try std.fs.path.join(alloc, &.{ snapshots_root, request.generation });
        errdefer alloc.free(published_root);

        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        try fs_paths.createDirPathPortable(io, snapshots_root);
        std.Io.Dir.cwd().deleteTree(io, building_root) catch {};
        std.Io.Dir.cwd().deleteTree(io, published_root) catch {};
        try fs_paths.createDirPathPortable(io, building_root);
        errdefer std.Io.Dir.cwd().deleteTree(io, building_root) catch {};

        var replica_catalog = try antfly.raft.FileReplicaCatalog.init(alloc, catalog_path);
        defer replica_catalog.deinit();
        const records = try replica_catalog.catalog().listReplicas(alloc);
        defer {
            for (records) |*record| record.deinit(alloc);
            alloc.free(records);
        }
        if (records.len == 0) return error.HASeedSnapshotEmptyCatalog;
        std.mem.sort(antfly.raft.ReplicaRecord, records, {}, struct {
            fn lessThan(_: void, left: antfly.raft.ReplicaRecord, right: antfly.raft.ReplicaRecord) bool {
                return left.group_id < right.group_id;
            }
        }.lessThan);

        var metadata_snapshot = try self.write_source.catalog.adminSnapshot();
        defer self.write_source.catalog.freeAdminSnapshot(&metadata_snapshot);
        if (metadata_snapshot.status.metadata_epoch == 0 or
            metadata_snapshot.tables.len == 0 or
            metadata_snapshot.ranges.len == 0 or
            records.len != metadata_snapshot.ranges.len)
            return error.HASeedSnapshotIncompleteTopology;
        std.mem.sort(antfly.metadata.TableRecord, metadata_snapshot.tables, {}, struct {
            fn lessThan(_: void, left: antfly.metadata.TableRecord, right: antfly.metadata.TableRecord) bool {
                return left.table_id < right.table_id;
            }
        }.lessThan);
        std.mem.sort(antfly.metadata.RangeRecord, metadata_snapshot.ranges, {}, struct {
            fn lessThan(_: void, left: antfly.metadata.RangeRecord, right: antfly.metadata.RangeRecord) bool {
                return left.group_id < right.group_id;
            }
        }.lessThan);
        std.mem.sort(antfly.extensions.PackageManifest, metadata_snapshot.extension_packages, {}, struct {
            fn lessThan(_: void, left: antfly.extensions.PackageManifest, right: antfly.extensions.PackageManifest) bool {
                const name_order = std.mem.order(u8, left.name, right.name);
                if (name_order != .eq) return name_order == .lt;
                return std.mem.order(u8, left.version, right.version) == .lt;
            }
        }.lessThan);

        var topology_replicas = std.ArrayListUnmanaged(HASeedSnapshotTopologyReplica).empty;
        defer {
            for (topology_replicas.items) |replica| {
                alloc.free(replica.snapshot_path);
                alloc.free(replica.logical_sha256);
            }
            topology_replicas.deinit(alloc);
        }

        for (records, 0..) |record, index| {
            if (index > 0 and records[index - 1].group_id == record.group_id)
                return error.HASeedSnapshotDuplicateReplica;
            const range = findHASeedRange(metadata_snapshot.ranges, record.group_id) orelse
                return error.HASeedSnapshotReplicaMissingFromTopology;
            const table = findHASeedTable(metadata_snapshot.tables, range.table_id) orelse
                return error.HASeedSnapshotTableMissingFromTopology;
            const snapshot_path = try std.fmt.allocPrint(alloc, "replicas/group-{d}", .{record.group_id});
            errdefer alloc.free(snapshot_path);
            const destination_root = try std.fs.path.join(alloc, &.{ building_root, snapshot_path });
            defer alloc.free(destination_root);
            const snapshot_token = try std.fmt.allocPrint(alloc, "{s}-g{d}", .{ request.generation, record.group_id });
            defer alloc.free(snapshot_token);
            try self.write_source.captureHASeedReplicaSnapshot(
                alloc,
                table.name,
                record.group_id,
                snapshot_token,
                destination_root,
            );
            const store_path = try std.fs.path.join(alloc, &.{ destination_root, "store.bin" });
            defer alloc.free(store_path);
            const digest = try haSeedSnapshotFileSha256HexAlloc(alloc, io, store_path);
            errdefer alloc.free(digest);
            try topology_replicas.append(alloc, .{
                .group_id = record.group_id,
                .table_id = range.table_id,
                .table_name = table.name,
                .snapshot_path = snapshot_path,
                .logical_sha256 = digest,
                .identity_table_id = table.table_id,
                .identity_shard_id = range.doc_identity_shard_id,
                .identity_range_id = range.doc_identity_range_id,
            });
        }

        var topology_extension_artifacts = std.ArrayListUnmanaged(HASeedSnapshotExtensionArtifact).empty;
        defer freeHASeedSnapshotExtensionArtifacts(alloc, &topology_extension_artifacts);
        try self.captureHASeedExtensionArtifacts(
            alloc,
            io,
            building_root,
            metadata_snapshot.extension_packages,
            &topology_extension_artifacts,
            null,
        );

        var topology_auth_artifact: ?HASeedSnapshotAuthArtifact = null;
        var auth_digest_owned: ?[]u8 = null;
        defer if (auth_digest_owned) |digest| alloc.free(digest);
        if (request.auth_artifact_json) |auth_json| {
            const auth_path = try std.fs.path.join(alloc, &.{ building_root, "auth/auth-seed.json" });
            defer alloc.free(auth_path);
            const auth_parent = std.fs.path.dirname(auth_path) orelse return error.InvalidHASeedSnapshotRoot;
            try fs_paths.createDirPathPortable(io, auth_parent);
            {
                var auth_file = try std.Io.Dir.cwd().createFile(io, auth_path, .{ .exclusive = true });
                defer auth_file.close(io);
                try auth_file.writeStreamingAll(io, auth_json);
                try auth_file.sync(io);
            }
            try fs_paths.syncDirPortable(io, auth_parent);
            auth_digest_owned = try haSeedSnapshotFileSha256HexAlloc(alloc, io, auth_path);
            topology_auth_artifact = .{
                .path = "auth/auth-seed.json",
                .size_bytes = auth_json.len,
                .sha256 = auth_digest_owned.?,
            };
        }

        const topology_json = try std.json.Stringify.valueAlloc(alloc, HASeedSnapshotTopology{
            .generation = request.generation,
            .catalog = .{
                .epoch = metadata_snapshot.status.metadata_epoch,
                .tables = metadata_snapshot.tables,
                .ranges = metadata_snapshot.ranges,
                .extension_packages = metadata_snapshot.extension_packages,
                .installed_extensions = metadata_snapshot.installed_extensions,
                .extension_members = metadata_snapshot.extension_members,
                .extension_dependencies = metadata_snapshot.extension_dependencies,
            },
            .replicas = topology_replicas.items,
            .extension_artifacts = topology_extension_artifacts.items,
            .auth_enabled = request.auth_artifact_json != null,
            .auth_artifact = topology_auth_artifact,
        }, .{});
        defer alloc.free(topology_json);
        if (topology_json.len > ha_seed_snapshot_max_topology_bytes) return error.HASeedSnapshotTopologyTooLarge;
        const topology_path = try std.fs.path.join(alloc, &.{ building_root, ha_seed_snapshot_topology_name });
        defer alloc.free(topology_path);
        const topology_temp_path = try std.fmt.allocPrint(alloc, "{s}.tmp", .{topology_path});
        defer alloc.free(topology_temp_path);
        {
            var file = try std.Io.Dir.cwd().createFile(io, topology_temp_path, .{ .truncate = true });
            defer file.close(io);
            try file.writeStreamingAll(io, topology_json);
            try file.sync(io);
        }
        if (std.fs.path.isAbsolute(topology_path))
            try std.Io.Dir.renameAbsolute(topology_temp_path, topology_path, io)
        else
            try std.Io.Dir.rename(std.Io.Dir.cwd(), topology_temp_path, std.Io.Dir.cwd(), topology_path, io);
        try fs_paths.syncDirPortable(io, building_root);

        if (std.fs.path.isAbsolute(published_root))
            try std.Io.Dir.renameAbsolute(building_root, published_root, io)
        else
            try std.Io.Dir.rename(std.Io.Dir.cwd(), building_root, std.Io.Dir.cwd(), published_root, io);
        try fs_paths.syncDirPortable(io, snapshots_root);
        return .{ .root = published_root };
    }

    fn findHASeedRange(ranges: []const antfly.metadata.RangeRecord, group_id: u64) ?antfly.metadata.RangeRecord {
        for (ranges) |range| if (range.group_id == group_id) return range;
        return null;
    }

    fn findHASeedTable(tables: []const antfly.metadata.TableRecord, table_id: u64) ?antfly.metadata.TableRecord {
        for (tables) |table| if (table.table_id == table_id) return table;
        return null;
    }

    fn captureHASeedExtensionArtifacts(
        self: *DataServer,
        alloc: std.mem.Allocator,
        io: std.Io,
        building_root: []const u8,
        packages: []const antfly.extensions.PackageManifest,
        artifacts: *std.ArrayListUnmanaged(HASeedSnapshotExtensionArtifact),
        probe: ?HASeedExtensionCaptureProbe,
    ) !void {
        const store_root = self.api_server_cfg.extension_package_store_dir orelse {
            if (packages.len != 0) return error.HASeedExtensionCatalogMismatch;
            return;
        };
        if (!antfly.ha.validation.isAbsoluteNormalizedPath(store_root))
            return error.InvalidHASeedExtensionStoreRoot;

        const entries = try antfly.extensions.scanPackageStoreAlloc(alloc, io, store_root);
        defer antfly.extensions.freePackageStoreEntries(alloc, entries);
        if (entries.len != packages.len) return error.HASeedExtensionCatalogMismatch;

        const catalog_index_by_entry = try alloc.alloc(usize, entries.len);
        defer if (catalog_index_by_entry.len > 0) alloc.free(catalog_index_by_entry);
        @memset(catalog_index_by_entry, std.math.maxInt(usize));
        for (packages, 0..) |package, package_index| {
            package.validate() catch return error.HASeedExtensionCatalogMismatch;
            var matched_entry: ?usize = null;
            for (entries, 0..) |entry, entry_index| {
                if (!std.mem.eql(u8, package.name, entry.manifest.name) or
                    !std.mem.eql(u8, package.version, entry.manifest.version)) continue;
                if (matched_entry != null) return error.HASeedExtensionCatalogMismatch;
                matched_entry = entry_index;
            }
            const entry_index = matched_entry orelse return error.HASeedExtensionCatalogMismatch;
            if (catalog_index_by_entry[entry_index] != std.math.maxInt(usize) or
                !try haSeedPackageManifestsEqual(alloc, package, entries[entry_index].manifest))
                return error.HASeedExtensionCatalogMismatch;
            catalog_index_by_entry[entry_index] = package_index;
        }
        for (catalog_index_by_entry) |package_index| {
            if (package_index == std.math.maxInt(usize)) return error.HASeedExtensionCatalogMismatch;
        }

        var store_dir = std.Io.Dir.cwd().openDir(io, store_root, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return error.HASeedExtensionCatalogMismatch,
            else => return err,
        };
        defer store_dir.close(io);
        var walker = try store_dir.walk(alloc);
        defer walker.deinit();
        while (try walker.next(io)) |entry| {
            if (entry.kind == .directory) continue;
            if (entry.kind != .file) return error.HASeedExtensionUnsafeArtifact;
            if (artifacts.items.len >= antfly.ha.seed_materialization.max_files)
                return error.HASeedExtensionTooManyArtifacts;

            const source_path = try std.fs.path.join(alloc, &.{ store_root, entry.path });
            defer alloc.free(source_path);
            const entry_index = try haSeedExtensionEntryForPath(source_path, entries);
            const package = packages[catalog_index_by_entry[entry_index]];
            const source_before = try std.Io.Dir.cwd().statFile(io, source_path, .{ .follow_symlinks = false });
            if (source_before.kind != .file or source_before.size > antfly.ha.seed_materialization.max_file_bytes)
                return error.HASeedExtensionUnsafeArtifact;
            const digest_before = try haSeedSnapshotFileSha256HexAlloc(alloc, io, source_path);
            errdefer alloc.free(digest_before);
            const artifact_path = try std.fs.path.join(alloc, &.{ "extensions", entry.path });
            errdefer alloc.free(artifact_path);
            if (!antfly.ha.validation.isNormalizedPath(artifact_path)) return error.HASeedExtensionUnsafeArtifact;
            const destination_path = try std.fs.path.join(alloc, &.{ building_root, artifact_path });
            defer alloc.free(destination_path);
            try std.Io.Dir.copyFile(std.Io.Dir.cwd(), source_path, std.Io.Dir.cwd(), destination_path, io, .{
                .make_path = true,
                .replace = false,
            });
            try fs_paths.syncFileAndParentPortable(io, destination_path);

            const source_after = try std.Io.Dir.cwd().statFile(io, source_path, .{ .follow_symlinks = false });
            const digest_after = try haSeedSnapshotFileSha256HexAlloc(alloc, io, source_path);
            defer alloc.free(digest_after);
            const destination_after = try std.Io.Dir.cwd().statFile(io, destination_path, .{ .follow_symlinks = false });
            const destination_digest = try haSeedSnapshotFileSha256HexAlloc(alloc, io, destination_path);
            defer alloc.free(destination_digest);
            if (source_after.kind != .file or destination_after.kind != .file or
                source_before.size != source_after.size or source_before.size != destination_after.size or
                !std.mem.eql(u8, digest_before, digest_after) or
                !std.mem.eql(u8, digest_before, destination_digest))
                return error.HASeedExtensionChangedDuringCapture;
            try artifacts.append(alloc, .{
                .package_name = package.name,
                .package_version = package.version,
                .path = artifact_path,
                .size_bytes = source_before.size,
                .sha256 = digest_before,
            });
        }

        std.mem.sort(HASeedSnapshotExtensionArtifact, artifacts.items, {}, struct {
            fn lessThan(_: void, left: HASeedSnapshotExtensionArtifact, right: HASeedSnapshotExtensionArtifact) bool {
                return std.mem.order(u8, left.path, right.path) == .lt;
            }
        }.lessThan);
        for (artifacts.items, 0..) |artifact, index| {
            if (index > 0 and std.mem.eql(u8, artifacts.items[index - 1].path, artifact.path))
                return error.HASeedExtensionUnexpectedArtifact;
        }

        // A second full walk binds the published image to one filesystem
        // observation. It detects source additions, removals, type changes,
        // and mutations that race the per-file copy loop.
        if (probe) |capture_probe| try capture_probe.afterCopy(alloc);
        var verify_walker = try store_dir.walk(alloc);
        defer verify_walker.deinit();
        var verified_files: usize = 0;
        while (try verify_walker.next(io)) |entry| {
            if (entry.kind == .directory) continue;
            if (entry.kind != .file) return error.HASeedExtensionChangedDuringCapture;
            const source_path = try std.fs.path.join(alloc, &.{ store_root, entry.path });
            defer alloc.free(source_path);
            const entry_index = haSeedExtensionEntryForPath(source_path, entries) catch
                return error.HASeedExtensionChangedDuringCapture;
            const package = packages[catalog_index_by_entry[entry_index]];
            const artifact_path = try std.fs.path.join(alloc, &.{ "extensions", entry.path });
            defer alloc.free(artifact_path);
            var found: ?HASeedSnapshotExtensionArtifact = null;
            for (artifacts.items) |artifact| {
                if (std.mem.eql(u8, artifact.path, artifact_path) and
                    std.mem.eql(u8, artifact.package_name, package.name) and
                    std.mem.eql(u8, artifact.package_version, package.version))
                {
                    found = artifact;
                    break;
                }
            }
            const artifact = found orelse return error.HASeedExtensionChangedDuringCapture;
            const stat = try std.Io.Dir.cwd().statFile(io, source_path, .{ .follow_symlinks = false });
            const digest = try haSeedSnapshotFileSha256HexAlloc(alloc, io, source_path);
            defer alloc.free(digest);
            if (stat.kind != .file or stat.size != artifact.size_bytes or
                !std.mem.eql(u8, digest, artifact.sha256))
                return error.HASeedExtensionChangedDuringCapture;
            verified_files += 1;
        }
        if (verified_files != artifacts.items.len) return error.HASeedExtensionChangedDuringCapture;
    }

    fn validateHASeedPreparedSnapshot(
        alloc: std.mem.Allocator,
        capture_root: []const u8,
        generation: []const u8,
        prepared_root: []const u8,
    ) !void {
        const allowed_root = try std.fmt.allocPrint(alloc, "{s}.runtime-snapshots", .{capture_root});
        defer alloc.free(allowed_root);
        if (!antfly.ha.validation.isAbsoluteNormalizedPathWithinRoot(prepared_root, allowed_root) or
            std.mem.eql(u8, prepared_root, allowed_root)) return error.InvalidHASeedSnapshotRoot;
        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        const topology_path = try std.fs.path.join(alloc, &.{ prepared_root, ha_seed_snapshot_topology_name });
        defer alloc.free(topology_path);
        const topology_json = try std.Io.Dir.cwd().readFileAlloc(
            io,
            topology_path,
            alloc,
            .limited(ha_seed_snapshot_max_topology_bytes),
        );
        defer alloc.free(topology_json);
        var parsed = std.json.parseFromSlice(
            HASeedSnapshotTopology,
            alloc,
            topology_json,
            .{ .ignore_unknown_fields = false },
        ) catch return error.InvalidHASeedSnapshotTopology;
        defer parsed.deinit();
        const topology = parsed.value;
        if (topology.format_version != ha_seed_snapshot_format_version or
            !std.mem.eql(u8, topology.generation, generation) or topology.replicas.len == 0)
            return error.InvalidHASeedSnapshotTopology;
        antfly.ha.seed_materialization.validateTopology(
            alloc,
            io,
            prepared_root,
            generation,
            topology,
        ) catch |err| switch (err) {
            error.SeedLogicalDigestMismatch => return error.HASeedSnapshotLogicalDigestMismatch,
            else => return error.InvalidHASeedSnapshotTopology,
        };

        for (topology.replicas, 0..) |replica, index| {
            if (index > 0 and topology.replicas[index - 1].group_id >= replica.group_id)
                return error.InvalidHASeedSnapshotTopology;
            const expected_path = try std.fmt.allocPrint(alloc, "replicas/group-{d}", .{replica.group_id});
            defer alloc.free(expected_path);
            if (!std.mem.eql(u8, replica.snapshot_path, expected_path) or
                replica.table_id == 0 or replica.table_name.len == 0 or
                !validLowerSha256(replica.logical_sha256))
                return error.InvalidHASeedSnapshotTopology;
            const store_path = try std.fs.path.join(alloc, &.{ prepared_root, replica.snapshot_path, "store.bin" });
            defer alloc.free(store_path);
            const actual_digest = try haSeedSnapshotFileSha256HexAlloc(alloc, io, store_path);
            defer alloc.free(actual_digest);
            if (!std.mem.eql(u8, actual_digest, replica.logical_sha256))
                return error.HASeedSnapshotLogicalDigestMismatch;
        }

        var root = try std.Io.Dir.cwd().openDir(io, prepared_root, .{ .iterate = true });
        defer root.close(io);
        var walker = try root.walk(alloc);
        defer walker.deinit();
        while (try walker.next(io)) |entry| {
            if (entry.kind == .directory) {
                if (std.mem.eql(u8, entry.path, "replicas")) continue;
                if (topology.auth_artifact != null and std.mem.eql(u8, entry.path, "auth")) continue;
                for (topology.replicas) |replica| {
                    if (std.mem.eql(u8, entry.path, replica.snapshot_path)) break;
                } else if (!haSeedExtensionArtifactDirectory(topology.extension_artifacts, entry.path)) {
                    return error.HASeedSnapshotUnexpectedArtifact;
                }
                continue;
            }
            if (entry.kind != .file) return error.HASeedSnapshotUnexpectedArtifact;
            if (std.mem.eql(u8, entry.path, ha_seed_snapshot_topology_name)) continue;
            for (topology.extension_artifacts) |artifact| {
                if (std.mem.eql(u8, entry.path, artifact.path)) break;
            } else {
                if (topology.auth_artifact) |artifact| {
                    if (std.mem.eql(u8, entry.path, artifact.path)) continue;
                }
                for (topology.replicas) |replica| {
                    const store_rel = try std.fs.path.join(alloc, &.{ replica.snapshot_path, "store.bin" });
                    defer alloc.free(store_rel);
                    if (std.mem.eql(u8, entry.path, store_rel)) break;
                    const journal_rel = try std.fs.path.join(alloc, &.{ replica.snapshot_path, "change-journal.bin" });
                    defer alloc.free(journal_rel);
                    if (std.mem.eql(u8, entry.path, journal_rel)) break;
                } else return error.HASeedSnapshotUnexpectedArtifact;
                continue;
            }
        }
    }

    fn captureHASeedCallback(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        slot_name: []const u8,
        generation: []const u8,
        binding: antfly.ha.seed_artifact.LifecycleBinding,
    ) !antfly.ha.http_admin.Server.SeedCaptureResult {
        const self: *DataServer = @ptrCast(@alignCast(ptr));

        // Global ordering is non-negotiable: capture excludes every durable
        // mutation before freezing the role pointer that keeps Primary alive.
        var lease = self.ha_mutation_barrier.acquireExclusive();
        defer lease.release();
        lockAtomic(&self.ha_state_mutex);
        defer self.ha_state_mutex.unlock();

        const ctx = self.ha_cfg.admin_context orelse return error.HASeedCaptureUnavailable;
        if (ctx.standby != null) return error.HASeedCaptureRequiresPrimary;
        if (haContextPrimaryIsFenced(ctx)) return error.PrimaryFenced;
        const primary = ctx.primary orelse return error.HASeedCaptureRequiresPrimary;
        if (self.ha_cfg.internal_primary) |current| {
            if (current != primary) return error.HASeedCapturePrimaryChanged;
        }
        const capture_root = self.ha_cfg.seed_capture_root orelse return error.HASeedCaptureRootMissing;
        const node_id = ctx.primary_node_id orelse return error.HAPrimaryNodeIdMissing;
        var auth_lease: ?antfly.usermgr.UserManager.SeedCaptureLease = null;
        defer if (auth_lease) |*auth_capture_lease| auth_capture_lease.release();
        var auth_artifact_json: ?[]u8 = null;
        defer if (auth_artifact_json) |body| alloc.free(body);
        if (self.api_server_cfg.auth_enabled) {
            const manager = self.api_server_cfg.user_manager orelse return error.HASeedCaptureAuthManagerMissing;
            auth_lease = manager.acquireSeedCaptureLease();
            auth_artifact_json = try manager.exportPortableSeedWithLease(alloc, generation, &auth_lease.?);
        }
        if (self.lsm_maintenance_active.load(.acquire) or self.auto_bulk_finish_active.load(.acquire))
            return error.HASeedSnapshotRuntimeBusy;
        var activity_lease = try self.write_source.acquireHASeedCaptureActivityLease();
        defer activity_lease.release();
        const provider = self.ha_cfg.seed_snapshot_provider orelse self.defaultHASeedSnapshotProvider();
        var prepared = try provider.prepare(alloc, .{
            .capture_root = capture_root,
            .generation = generation,
            .auth_artifact_json = auth_artifact_json,
        });
        defer prepared.deinit(alloc);
        try validateHASeedPreparedSnapshot(alloc, capture_root, generation, prepared.root);

        const sources = [_]antfly.ha.seed_capture.Source{.{ .tree = .{
            .source_root = prepared.root,
            .artifact_prefix = "",
            .kind = .artifact,
        } }};
        var capture = try antfly.ha.seed_capture.captureWithExclusiveLease(alloc, .{
            .primary = primary,
            .barrier = &self.ha_mutation_barrier,
            .slot_name = slot_name,
            .generation = generation,
            .capture_root = capture_root,
            .sources = &sources,
            .binding = binding,
            .pod_uid = self.ha_cfg.pod_uid,
        }, &lease);
        errdefer capture.deinit(alloc);
        return .{
            .capture = capture,
            .node_id = try alloc.dupe(u8, node_id),
        };
    }

    fn attachHaExecutors(self: *DataServer, api_server_cfg: *antfly.public_api.http_server.ApiHttpServerConfig) void {
        if (api_server_cfg.ha_admin_executor == null) {
            if (self.ha_cfg.admin_context) |ctx| {
                self.ha_admin_server = antfly.ha.http_admin.Server.initWithOptions(platform.allocator.processAllocator(std.heap.smp_allocator), ctx, .{
                    .bearer_token = self.ha_cfg.admin_bearer_token,
                    .state_mutex = if (ctx.primary != null or ctx.standby != null) &self.ha_state_mutex else null,
                    .seed_capture = if (ctx.primary != null and self.ha_cfg.seed_capture_root != null) .{
                        .ptr = self,
                        .run_fn = DataServer.captureHASeedCallback,
                    } else null,
                    .lifecycle_receipts = if (self.ha_cfg.seed_capture_root != null or self.ha_cfg.seed_activation_root != null) .{
                        .capture_root = self.ha_cfg.seed_capture_root,
                        .activation_root = self.ha_cfg.seed_activation_root,
                        .node_id = if (ctx.primary != null) ctx.primary_node_id else ctx.standby_node_id,
                        .role = if (ctx.primary != null) .primary else if (ctx.standby != null) .standby else .unknown,
                        .pod_uid = self.ha_cfg.pod_uid,
                    } else null,
                    .primary_fence_started = .{
                        .ptr = self,
                        .run_fn = DataServer.haPrimaryFenceStartedCallback,
                    },
                    .state_changed = .{
                        .ptr = self,
                        .run_fn = DataServer.haPublicGateStateChangedCallback,
                    },
                    .standby_status_extras = .{
                        .ptr = self,
                        .last_error = haStandbyReplicationLastErrorCallback,
                        .last_attempt_ns = haStandbyReplicationLastAttemptNsCallback,
                        .last_success_ns = haStandbyReplicationLastSuccessNsCallback,
                        .replication_failures_total = haStandbyReplicationFailuresTotalCallback,
                    },
                });
                api_server_cfg.ha_admin_executor = self.ha_admin_server.?.executor();
            }
        }
        if (api_server_cfg.ha_internal_executor == null) {
            const primary = self.ha_cfg.internal_primary orelse if (self.ha_cfg.admin_context) |ctx| ctx.primary else null;
            if (primary) |handle| {
                self.ha_internal_server = antfly.ha.http_internal.Server.initWithOptions(platform.allocator.processAllocator(std.heap.smp_allocator), handle, .{
                    .state_mutex = &self.ha_state_mutex,
                });
                api_server_cfg.ha_internal_executor = self.ha_internal_server.?.executor();
            }
        }
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
        self.syncInferenceRuntimeConfig();
    }

    fn configuredInferenceAPIURL(self: *const DataServer) ?[]const u8 {
        const node_config = self.api_server_cfg.node_config orelse return null;
        return node_config.inference.api_url;
    }

    fn syncInferenceRuntimeConfig(self: *DataServer) void {
        const inference_api_url = self.configuredInferenceAPIURL();
        _ = self.read_source.withInferenceAPIURL(inference_api_url);
        _ = self.write_source.withInferenceAPIURL(inference_api_url);
        if (self.data_raft_apply) |apply_sm| {
            _ = apply_sm.write_source.withInferenceAPIURL(inference_api_url);
            apply_sm.write_cache.inference_api_url = inference_api_url;
        }
    }

    pub fn start(self: *DataServer) !void {
        _ = try self.ensureBackendRuntime();
        self.registerMetadataLocalProviders();
        try self.initApiServer();
        try self.http_server.?.resumeRestoreJobsOnce();
        if (self.data_raft) |raft| {
            try raft.start();
            self.data_raft_base_uri = try raft.baseUri(self.alloc);
        }
        if (self.listener == null) {
            // ApiHttpServer owns responses with its request allocator. Keep the
            // listener on that exact allocator identity; using smp_allocator
            // here while the API uses c_allocator causes cross-allocator frees.
            const request_alloc = self.http_server.?.alloc;
            self.listener = if (self.backend_runtime) |runtime|
                if (runtime.apiIoImpl()) |io_impl|
                    antfly.raft.transport.std_http_listener.StdHttpListener.initShared(
                        request_alloc,
                        self.listener_cfg,
                        self.http_server.?.executor(),
                        io_impl,
                    )
                else
                    antfly.raft.transport.std_http_listener.StdHttpListener.init(
                        request_alloc,
                        self.listener_cfg,
                        self.http_server.?.executor(),
                    )
            else
                antfly.raft.transport.std_http_listener.StdHttpListener.init(
                    request_alloc,
                    self.listener_cfg,
                    self.http_server.?.executor(),
                );
        }
        self.listener.?.setStreamingExecutor(self.http_server.?.streamingExecutor());
        try self.listener.?.start();
        if (self.store_registration != null) {
            self.store_status_dirty.store(true, .release);
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

    fn metadataBootstrapRetryDue(self: *DataServer, now_ms: u64) bool {
        lockAtomic(&self.metadata_bootstrap_retry_mutex);
        defer self.metadata_bootstrap_retry_mutex.unlock();
        return self.next_metadata_bootstrap_retry_at_ms == 0 or
            now_ms >= self.next_metadata_bootstrap_retry_at_ms;
    }

    fn clearMetadataBootstrapRetry(self: *DataServer) void {
        lockAtomic(&self.metadata_bootstrap_retry_mutex);
        defer self.metadata_bootstrap_retry_mutex.unlock();
        self.metadata_bootstrap_retry_attempts = 0;
        self.next_metadata_bootstrap_retry_at_ms = 0;
    }

    fn deferMetadataBootstrapRetry(self: *DataServer, now_ms: u64) void {
        lockAtomic(&self.metadata_bootstrap_retry_mutex);
        defer self.metadata_bootstrap_retry_mutex.unlock();
        self.metadata_bootstrap_retry_attempts = self.metadata_bootstrap_retry_attempts +| 1;
        const delay_ms = metadataBootstrapRetryDelayMs(
            self.store_registration,
            self.metadata_bootstrap_retry_attempts,
            now_ms,
        );
        self.next_metadata_bootstrap_retry_at_ms = now_ms +| delay_ms;
    }

    fn recordMetadataBootstrapError(self: *DataServer, err: anyerror, now_ms: u64) !void {
        if (!isRetryableMetadataBootstrapError(err)) return err;
        self.deferMetadataBootstrapRetry(now_ms);
    }

    fn recordProvisionedRootRefreshMetadataError(self: *DataServer, err: anyerror, now_ms: u64) !void {
        if (!isRetryableMetadataBootstrapError(err)) return err;
        self.provisioned_root_refresh_dirty.store(true, .release);
        self.deferMetadataBootstrapRetry(now_ms);
    }

    fn metadataBootstrapRetryAttemptsForTest(self: *DataServer) u32 {
        lockAtomic(&self.metadata_bootstrap_retry_mutex);
        defer self.metadata_bootstrap_retry_mutex.unlock();
        return self.metadata_bootstrap_retry_attempts;
    }

    fn nextMetadataBootstrapRetryAtMsForTest(self: *DataServer) u64 {
        lockAtomic(&self.metadata_bootstrap_retry_mutex);
        defer self.metadata_bootstrap_retry_mutex.unlock();
        return self.next_metadata_bootstrap_retry_at_ms;
    }

    pub fn runRound(self: *DataServer) !void {
        const now_ns = platform_time.monotonicNs();
        if (self.haStandbyReplicationRetryDue(now_ns)) replication: {
            self.runHAStandbyReplicationRound() catch |err| {
                _ = self.ha_standby_replication_failure_count.fetchAdd(1, .monotonic);
                self.recordHAStandbyReplicationError(err);
                self.deferHAStandbyReplicationRetry(now_ns);
                if (!isNonFatalHAStandbyReplicationError(err)) return err;
                // Replication is a fail-closed maintenance loop. Keep the runtime
                // and HA admin surface alive so the operator can observe and
                // repair transport, compatibility, or durable-state failures.
                std.log.warn("HA standby replication round skipped err={}", .{err});
                break :replication;
            };
            self.clearHAStandbyReplicationRetry();
            self.clearHAStandbyReplicationError();
        }
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
            const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
            if (self.metadataBootstrapRetryDue(now_ms)) {
                const registration_ready = blk: {
                    if (!self.store_registration_confirmed) {
                        self.registerNodeIfConfigured() catch |err| {
                            try self.recordMetadataBootstrapError(err, now_ms);
                            break :blk false;
                        };
                    }
                    break :blk true;
                };
                if (registration_ready) {
                    _ = self.store_status_ticks.fetchAdd(1, .monotonic);
                    const due_store_status_heartbeat = self.last_store_status_report_at_ms == 0 or
                        now_ms -| self.last_store_status_report_at_ms >= store_status_heartbeat_interval_ms;
                    const due_full_store_status = self.localGroupStatusCacheStale(now_ms);
                    const due_data_raft_status_refresh = self.data_raft != null;
                    const report_kind = chooseStoreStatusReportKind(
                        self.store_status_ticks.load(.acquire),
                        self.store_status_dirty.load(.acquire),
                        self.last_store_status_report_at_ms,
                        due_store_status_heartbeat,
                        due_full_store_status,
                        due_data_raft_status_refresh,
                    );
                    if (report_kind != .none) {
                        self.store_status_ticks.store(0, .release);
                        const result = switch (report_kind) {
                            .full => self.reportStoreStatus(),
                            .heartbeat => self.reportStoreStatusHeartbeat(),
                            .none => unreachable,
                        };
                        result catch |err| switch (err) {
                            // Split runtime can briefly observe placement before the
                            // local replica root is fully provisioned on disk.
                            error.LsmRootWriterAlreadyOpen,
                            error.FileNotFound,
                            error.UnknownGroup,
                            error.LmdbUnexpected,
                            error.Corrupted,
                            => {},
                            error.UnknownStore => {
                                self.store_registration_confirmed = false;
                                self.registerNodeIfConfigured() catch |register_err| try self.recordMetadataBootstrapError(register_err, now_ms);
                            },
                            else => |retry_err| try self.recordMetadataBootstrapError(retry_err, now_ms),
                        };
                    }
                }

                if (self.metadataBootstrapRetryDue(now_ms)) {
                    self.provision_ticks += 1;
                    if (self.provision_ticks >= 4) {
                        self.provision_ticks = 0;
                        self.maybeRequestProvisionedRootRefresh() catch |err| switch (err) {
                            // Local split-runtime provisioning can race with active writes.
                            // Treat those as transient and retry on the next provision tick.
                            error.LsmRootWriterAlreadyOpen,
                            error.WriterLocked,
                            error.FileNotFound,
                            error.UnknownGroup,
                            error.LmdbUnexpected,
                            error.Corrupted,
                            => {},
                            else => |retry_err| try self.recordProvisionedRootRefreshMetadataError(retry_err, now_ms),
                        };
                    }
                }
            }
        }
        try self.maybeRequestRuntimeStatusRefresh();
        try self.maybeRequestProvisionedStartupCatchUp();
        // Session cleanup and lease renewal may touch durable storage. Keep
        // that work off HTTP request fibers; this runtime loop is the sole
        // opportunistic scheduler for the API server.
        if (self.http_server) |*http_server| {
            http_server.scheduleSessionMaintenance() catch |err| {
                // Session maintenance is periodic and retryable. A transient
                // allocation, queue, or storage error must not terminate the
                // data runtime's availability loop.
                std.log.warn("failed to schedule session maintenance err={s}", .{@errorName(err)});
            };
        }
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
        if (self.ha_admin_server) |*server| server.deinit();
        if (self.ha_promoted_primary) |*primary| primary.close();
        if (self.ha_standby_replication_http_executor) |*executor| executor.deinit();
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
        self.ha_admin_server = null;
        self.ha_standby_replication_http_executor = null;
        self.ha_promoted_primary = null;
        self.ha_internal_server = null;
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
        if (!self.haOwnerJobCanRun(.compaction_publish)) return;
        _ = self.liveRuntimeWriteSource().runLsmMaintenanceRound() catch |err| switch (err) {
            error.LsmRootWriterAlreadyOpen,
            error.ReadOnly,
            error.FileNotFound,
            error.LmdbUnexpected,
            error.Corrupted,
            => {},
            else => return err,
        };
    }

    fn requestLsmMaintenanceBackground(self: *DataServer) !void {
        if (!self.haOwnerJobCanRun(.compaction_publish)) return;
        const now_ns = platform_time.monotonicNs();
        if (now_ns < self.lsm_maintenance_next_eligible_ns.load(.monotonic)) return;
        if (self.resourcePressureDefersBackgroundMaintenance()) {
            self.deferLsmMaintenance(now_ns, lsm_maintenance_worker_pressure_defer_ns);
            _ = self.lsm_maintenance_capacity_denied.fetchAdd(1, .monotonic);
            return;
        }
        if (!self.backgroundMaintenanceDue(now_ns)) return;
        self.lsm_maintenance_wake.store(true, .release);
        if (self.lsm_maintenance_thread == null) {
            self.lsm_maintenance_stop.store(false, .release);
            self.lsm_maintenance_thread = try std.Thread.spawn(.{}, lsmMaintenanceWorkerMain, .{self});
        }
    }

    fn clearHAStandbyReplicationError(self: *DataServer) void {
        self.ha_standby_replication_last_error.store(@intFromEnum(HAStandbyReplicationErrorCode.none), .release);
    }

    fn haStandbyReplicationRetryDue(self: *DataServer, now_ns: u64) bool {
        const next_attempt_ns = self.ha_standby_replication_next_attempt_ns.load(.acquire);
        return next_attempt_ns == 0 or now_ns >= next_attempt_ns;
    }

    fn deferHAStandbyReplicationRetry(self: *DataServer, now_ns: u64) void {
        self.ha_standby_replication_next_attempt_ns.store(
            now_ns +| ha_standby_replication_retry_interval_ns,
            .release,
        );
    }

    fn clearHAStandbyReplicationRetry(self: *DataServer) void {
        self.ha_standby_replication_next_attempt_ns.store(0, .release);
    }

    fn recordHAStandbyReplicationAttempt(self: *DataServer) void {
        self.ha_standby_replication_last_attempt_ns.store(platform_time.monotonicNs(), .release);
    }

    fn recordHAStandbyReplicationSuccess(self: *DataServer) void {
        self.ha_standby_replication_last_success_ns.store(platform_time.monotonicNs(), .release);
    }

    fn recordHAStandbyReplicationError(self: *DataServer, err: anyerror) void {
        self.ha_standby_replication_last_error.store(@intFromEnum(haStandbyReplicationErrorCode(err)), .release);
    }

    fn haStandbyReplicationLastError(self: *DataServer) ?[]const u8 {
        const code: HAStandbyReplicationErrorCode = @enumFromInt(self.ha_standby_replication_last_error.load(.acquire));
        return haStandbyReplicationErrorName(code);
    }

    fn haStandbyReplicationLastErrorCallback(ptr: *anyopaque) ?[]const u8 {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        return self.haStandbyReplicationLastError();
    }

    fn haStandbyReplicationLastAttemptNs(self: *DataServer) ?u64 {
        const value = self.ha_standby_replication_last_attempt_ns.load(.acquire);
        return if (value == 0) null else value;
    }

    fn haStandbyReplicationLastAttemptNsCallback(ptr: *anyopaque) ?u64 {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        return self.haStandbyReplicationLastAttemptNs();
    }

    fn haStandbyReplicationLastSuccessNs(self: *DataServer) ?u64 {
        const value = self.ha_standby_replication_last_success_ns.load(.acquire);
        return if (value == 0) null else value;
    }

    fn haStandbyReplicationLastSuccessNsCallback(ptr: *anyopaque) ?u64 {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        return self.haStandbyReplicationLastSuccessNs();
    }

    fn haStandbyReplicationFailuresTotal(self: *DataServer) ?u64 {
        return self.ha_standby_replication_failure_count.load(.acquire);
    }

    fn haStandbyReplicationFailuresTotalCallback(ptr: *anyopaque) ?u64 {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        return self.haStandbyReplicationFailuresTotal();
    }

    fn densePostingMaintenanceDue(self: *DataServer, now_ns: u64) bool {
        return now_ns >= self.dense_posting_maintenance_next_eligible_ns.load(.monotonic);
    }

    fn backgroundMaintenanceDue(self: *DataServer, now_ns: u64) bool {
        if (!self.haOwnerJobCanRun(.compaction_publish)) return false;
        const live_write_source = self.liveRuntimeWriteSource();
        if (live_write_source.lsmMaintenanceScoreBestEffort() > 0) {
            self.lsm_maintenance_obsolete_reclaim_due_ns.store(0, .release);
            return true;
        }
        if (self.densePostingMaintenanceDue(now_ns)) return true;
        const obsolete_due_ns = self.lsm_maintenance_obsolete_reclaim_due_ns.load(.monotonic);
        if (obsolete_due_ns != 0 and now_ns < obsolete_due_ns) return false;
        if (live_write_source.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay_ns| {
            if (delay_ns > 0) {
                self.deferLsmObsoleteReclaim(now_ns, delay_ns);
                return false;
            }
            self.lsm_maintenance_obsolete_reclaim_due_ns.store(0, .release);
            return true;
        }
        self.lsm_maintenance_obsolete_reclaim_due_ns.store(0, .release);
        return false;
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

    fn deferLsmObsoleteReclaim(self: *DataServer, now_ns: u64, delay_ns: u64) void {
        const due_ns = now_ns +| delay_ns;
        const current = self.lsm_maintenance_obsolete_reclaim_due_ns.load(.monotonic);
        if (current == 0 or due_ns < current) {
            self.lsm_maintenance_obsolete_reclaim_due_ns.store(due_ns, .release);
        }
    }

    fn lsmMaintenanceWorkerMain(self: *DataServer) void {
        while (!self.lsm_maintenance_stop.load(.acquire)) {
            const woke = self.lsm_maintenance_wake.swap(false, .acq_rel);
            const now_ns = platform_time.monotonicNs();
            if (now_ns < self.lsm_maintenance_next_eligible_ns.load(.monotonic)) {
                sleepLsmMaintenanceWorker();
                continue;
            }
            if (!woke and !self.backgroundMaintenanceDue(now_ns)) {
                sleepLsmMaintenanceWorker();
                continue;
            }
            if (!self.haOwnerJobCanRun(.compaction_publish)) {
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
            const live_write_source = self.liveRuntimeWriteSource();
            var completed = false;
            var steps: usize = 0;
            while (steps < lsm_maintenance_worker_max_steps_per_wake and !self.lsm_maintenance_stop.load(.acquire)) : (steps += 1) {
                const did_work = live_write_source.runLsmMaintenanceRoundBestEffort() catch |err| {
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
                    if (live_write_source.lsmMaintenanceScoreBestEffort() > 0) {
                        _ = self.lsm_maintenance_lock_deferred.fetchAdd(1, .monotonic);
                        self.deferLsmMaintenance(platform_time.monotonicNs(), lsm_maintenance_worker_retry_sleep_ns);
                    } else if (live_write_source.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay_ns| {
                        if (delay_ns > 0) self.deferLsmObsoleteReclaim(platform_time.monotonicNs(), delay_ns);
                    }
                    completed = true;
                    break;
                }
            }
            if (steps >= lsm_maintenance_worker_max_steps_per_wake) {
                if (live_write_source.lsmMaintenanceScoreBestEffort() > 0) {
                    self.deferLsmMaintenance(platform_time.monotonicNs(), lsm_maintenance_worker_retry_sleep_ns);
                } else if (live_write_source.nextLsmMaintenanceWakeDelayNsBestEffort()) |delay_ns| {
                    if (delay_ns > 0) self.deferLsmObsoleteReclaim(platform_time.monotonicNs(), delay_ns);
                }
            }
            if (completed) _ = self.lsm_maintenance_completed.fetchAdd(1, .monotonic);
            // Bulk loads can leave leaf postings with stale quantized payloads
            // (deferred splits), which forces exact member scoring on every
            // query that visits them. Drain a bounded amount of that repair
            // work alongside the regular LSM maintenance.
            const posting_now_ns = platform_time.monotonicNs();
            if (self.densePostingMaintenanceDue(posting_now_ns)) {
                const posting_steps = live_write_source.runDensePostingMaintenanceRoundBestEffort() catch 0;
                const next_delay_ns: u64 = if (posting_steps > 0)
                    dense_posting_maintenance_retry_interval_ns
                else
                    dense_posting_maintenance_idle_interval_ns;
                self.dense_posting_maintenance_next_eligible_ns.store(posting_now_ns +| next_delay_ns, .release);
                if (posting_steps > 0) {
                    std.log.info("dense posting maintenance repaired steps={d}", .{posting_steps});
                }
            }
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
                    platform.time.yieldBriefly();
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

    fn promotionLeadershipSource(self: *DataServer) ?antfly.public_api.table_writes.ProvisionedTableWriteCache.PromotionLeadershipSource {
        if (self.group_leadership_source == null) return null;
        return .{
            .ptr = self,
            .vtable = &.{
                .is_local_leader = struct {
                    fn isLocalLeader(ptr: *anyopaque, group_id: u64) bool {
                        const server: *DataServer = @ptrCast(@alignCast(ptr));
                        const source = server.group_leadership_source orelse return true;
                        return source.isLocalLeader(group_id);
                    }
                }.isLocalLeader,
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
        try self.proposeRaftBatchGroup(alloc, group_id, table_name, req, true, true);
    }

    fn localRaftBatchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: antfly.db.types.BatchRequest,
    ) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        try self.proposeRaftBatchGroup(alloc, group_id, table_name, req, true, false);
    }

    fn proposeRaftBatchGroup(
        self: *DataServer,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: antfly.db.types.BatchRequest,
        allow_remote_forward: bool,
        refresh_metadata: bool,
    ) !void {
        const raft = self.data_raft orelse return error.UnsupportedOperation;
        if (refresh_metadata) {
            try self.syncDataRaftFromRemoteMetadata();
        } else if (raft.host.http_host.host.raftStatus(group_id) == null and self.remote_metadata != null) {
            const remote_metadata = self.remote_metadata.?;
            var snapshot = (try remote_metadata.cachedSnapshot()) orelse return error.MetadataSnapshotUnavailable;
            defer freeAdminSnapshotOwned(self.alloc, &snapshot);
            try self.syncDataRaftFromSnapshot(&snapshot);
        }
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
                    if (leader_node_id == null and localRaftStatusShouldBootstrapCampaign(status, local_node_id)) {
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
                const sync_source = if (self.data_raft_apply) |apply_sm| &apply_sm.write_source else &self.write_source;
                try sync_source.syncReplicatedBatchGroupLocal(alloc, group_id, table_name, req.sync_level);
                return;
            }

            if (allow_remote_forward and local_status_missing) {
                if (try self.forwardRaftBatchToPlacementReplica(alloc, group_id, table_name, req, local_node_id, deadline_ns, refresh_metadata)) return;
            }

            if (allow_remote_forward) {
                if (leader_node_id) |target_node_id| {
                    if (target_node_id != local_node_id) {
                        const leader_base_uri = try self.dataApiUriForNode(alloc, target_node_id, refresh_metadata);
                        if (leader_base_uri) |base_uri| {
                            defer alloc.free(base_uri);
                            var executor = antfly.raft.transport.StdHttpExecutor.init(alloc, .{});
                            defer executor.deinit();
                            var client = antfly.public_api.ApiHttpClient.init(alloc, executor.executor());
                            const body = try antfly.public_api.batch.encodeBatchRequest(alloc, req);
                            defer alloc.free(body);
                            var response = client.fetchGroupBatch(base_uri, group_id, table_name, body) catch |err| switch (err) {
                                error.LeaderUnavailable,
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
            if (refresh_metadata and leader_node_id == null and now_ns -| last_metadata_sync_ns >= data_raft_metadata_resync_interval_ns) {
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

    fn dataApiUriForNode(self: *DataServer, alloc: std.mem.Allocator, node_id: u64, refresh_metadata: bool) !?[]u8 {
        const remote_metadata = self.remote_metadata orelse return null;
        var snapshot = if (refresh_metadata)
            try remote_metadata.fetchSnapshot()
        else
            (try remote_metadata.cachedSnapshot()) orelse return error.MetadataSnapshotUnavailable;
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
        refresh_metadata: bool,
    ) !bool {
        const remote_metadata = self.remote_metadata orelse return false;
        var snapshot = if (refresh_metadata)
            try remote_metadata.fetchSnapshot()
        else
            (try remote_metadata.cachedSnapshot()) orelse return error.MetadataSnapshotUnavailable;
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
            error.LeaderUnavailable,
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

    fn localRaftStatusShouldBootstrapCampaign(status: ?raft_engine.core.Status, local_node_id: u64) bool {
        const raft_status = status orelse return false;
        if (raft_status.soft.leader_id != null) return false;
        switch (raft_status.soft.role) {
            .follower, .pre_candidate => {},
            .candidate => if (raft_status.election_elapsed < raft_status.randomized_election_timeout) return false,
            .leader => return false,
        }
        return localRaftStatusIsVoter(raft_status, local_node_id);
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
                    platform.time.yieldBriefly();
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
                if (self.data_raft_apply) |apply_sm| {
                    // Raft-backed tables write through the apply state machine.
                    // Move create-time seeded writers before refreshing visible
                    // roots so status reconciliation and the first batch share
                    // one root writer owner.
                    _ = self.write_source.transferAdoptableWriteCacheEntriesTo(&apply_sm.write_source, table_name) catch |err| {
                        std.log.warn("failed to transfer provisioned write cache to raft apply table={s} err={}", .{
                            table_name,
                            err,
                        });
                    };
                }
                self.refreshVisibleProvisionedReplicaState() catch |err| {
                    std.log.warn("failed to refresh provisioned replica state after structural change table={s} err={}", .{
                        table_name,
                        err,
                    });
                };
                self.syncDataRaftFromRemoteMetadata() catch |err| {
                    std.log.warn("failed to sync data raft placement after structural change table={s} err={}", .{
                        table_name,
                        err,
                    });
                };
                self.reportStoreStatusAfterStructuralChange(table_name);
            },
        }
        self.markRuntimeStatusDirty(table_name, kind);
    }

    fn reportStoreStatusAfterStructuralChange(self: *DataServer, table_name: []const u8) void {
        if (self.remote_metadata == null or self.store_registration == null) return;
        if (!self.store_registration_confirmed) {
            self.registerNodeIfConfigured() catch |err| {
                std.log.warn("failed to register store before structural status report table={s} err={}", .{
                    table_name,
                    err,
                });
                return;
            };
        }
        self.reportStoreStatus() catch |err| switch (err) {
            error.FileNotFound,
            error.UnknownGroup,
            error.LmdbUnexpected,
            error.Corrupted,
            => {},
            else => std.log.warn("failed to report store status after structural change table={s} err={}", .{
                table_name,
                err,
            }),
        };
    }

    fn markLocalGroupDataChanged(self: *DataServer) void {
        // Data writes can be high-frequency during ingest. Keep serving the
        // cached group-status snapshot until its TTL expires; structural
        // changes still invalidate immediately. Runtime index status is
        // already cached by the writer, so mark the store report dirty without
        // forcing a local group-status refresh. The next runtime round should
        // publish promptly; otherwise slow debug builds can sit behind the
        // periodic tick gate even though fresh writer status is available.
        self.markStoreStatusDirtyImmediate();
    }

    fn markStoreStatusDirtyImmediate(self: *DataServer) void {
        self.store_status_dirty.store(true, .release);
        self.store_status_ticks.store(store_status_report_interval_ticks, .release);
    }

    fn observeDataRaftStatusFingerprint(self: *DataServer, fingerprint: u64) void {
        if (self.last_data_raft_status_fingerprint != null and
            self.last_data_raft_status_fingerprint.? == fingerprint)
        {
            return;
        }
        self.last_data_raft_status_fingerprint = fingerprint;
        self.markStoreStatusDirtyImmediate();
    }

    fn markRuntimeStatusDirty(
        self: *DataServer,
        table_name: []const u8,
        kind: antfly.public_api.ProvisionedTableWriteSource.LocalChangeKind,
    ) void {
        _ = table_name;
        self.runtime_status_dirty.store(true, .release);
        switch (kind) {
            .data => self.provisioned_startup_catch_up_dirty.store(true, .release),
            .structural => {
                self.clearProvisionedStartupCatchUpBackoffs();
                self.provisioned_startup_catch_up_dirty.store(true, .release);
            },
        }
    }

    fn markLocalSplitKeyCacheDirty(self: *DataServer) void {
        _ = self.local_split_key_generation.fetchAdd(1, .monotonic);
    }

    fn invalidateLocalGroupStatusCache(self: *DataServer) void {
        self.store_status_dirty.store(true, .release);
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

    pub fn bumpVisibleProvisionedGroupRootGenerations(self: *DataServer) !void {
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

        const group_ids_ptr = group_ids.items.ptr;
        const group_ids_capacity = group_ids.capacity;

        self.provisioned_storage.pruneGroupVisibleRootGenerations(group_ids.items);
        std.debug.assert(group_ids.items.ptr == group_ids_ptr and group_ids.capacity == group_ids_capacity);
        if (group_ids.items.len == 0) return;
        try self.provisioned_storage.bumpGroupVisibleRootGenerations(group_ids.items);
        std.debug.assert(group_ids.items.ptr == group_ids_ptr and group_ids.capacity == group_ids_capacity);
    }

    pub fn refreshVisibleProvisionedReplicaState(self: *DataServer) !void {
        try self.bumpVisibleProvisionedGroupRootGenerations();
        self.provisioned_storage.read_cache.clear();
        self.provisioned_storage.lsm_cache.invalidatePrefix(self.write_source.replica_root_dir);
        self.provisioned_storage.hbc_cache.clear();
    }

    pub fn reconcileVisibleProvisionedReplicaState(self: *DataServer) !void {
        try self.refreshVisibleProvisionedReplicaState();
        self.pruneStaleVisibleWriteCaches();
    }

    fn pruneStaleVisibleWriteCaches(self: *DataServer) void {
        const apply_sm = self.data_raft_apply orelse {
            lockAtomic(self.write_source.localDbMutex());
            defer self.write_source.localDbMutex().unlock();
            self.write_source.pruneStaleWriteCacheLocked();
            return;
        };

        pruneStaleWriteCachesInLockOrder(&self.write_source, &apply_sm.write_source);
    }

    fn pruneStaleWriteCachesInLockOrder(
        primary: *antfly.public_api.ProvisionedTableWriteSource,
        secondary: *antfly.public_api.ProvisionedTableWriteSource,
    ) void {
        if (primary == secondary) {
            lockAtomic(primary.localDbMutex());
            defer primary.localDbMutex().unlock();
            primary.pruneStaleWriteCacheLocked();
            return;
        }

        const primary_mutex = primary.localDbMutex();
        const secondary_mutex = secondary.localDbMutex();
        const first_mutex = if (@intFromPtr(primary_mutex) < @intFromPtr(secondary_mutex)) primary_mutex else secondary_mutex;
        const second_mutex = if (first_mutex == primary_mutex) secondary_mutex else primary_mutex;

        lockAtomic(first_mutex);
        defer first_mutex.unlock();
        lockAtomic(second_mutex);
        defer second_mutex.unlock();

        primary.pruneStaleWriteCacheLocked();
        secondary.pruneStaleWriteCacheLocked();
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
        const lsm_root_generation = self.provisioned_storage.visibleRootGenerationForGroup(group_id);
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
        if (self.data_raft != null) {
            lockAtomic(&self.local_transition_mutex);
            defer self.local_transition_mutex.unlock();
            return .{ .status = try self.observeReplicatedSplit(record.source_group_id, record.destination_group_id) };
        }
        lockAtomic(&self.local_transition_mutex);
        defer self.local_transition_mutex.unlock();
        return .{ .status = try self.observeLocalSplit(record.source_group_id, record.destination_group_id) };
    }

    fn lockLocalTransition(self: *DataServer) void {
        lockAtomic(&self.local_transition_mutex);
        lockAtomic(&self.data_raft_mutex);
    }

    fn unlockLocalTransition(self: *DataServer) void {
        self.data_raft_mutex.unlock();
        self.local_transition_mutex.unlock();
    }

    fn localObserveMerge(ptr: *anyopaque, record: antfly.metadata.MergeTransitionRecord) !antfly.metadata.transition_state.MergeObservation {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| return try runtime.shardOperationAdapter().observeMerge(record);
        self.lockLocalTransition();
        defer self.unlockLocalTransition();
        var runtime = try self.initLocalMergeRuntime(record.donor_group_id, record.receiver_group_id);
        defer runtime.deinit();
        const status = try runtime.runtime().observeStatus(record.donor_group_id, record.receiver_group_id);
        return .{ .donor = status, .receiver = status };
    }

    fn localPrepareSplitSource(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "prepare_split_source")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.data_raft != null) {
            lockAtomic(&self.local_transition_mutex);
            defer self.local_transition_mutex.unlock();
            try self.replicateSplitSourceTransition(op.source_group_id, op.destination_group_id, .prepare, op.split_key);
        } else if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .prepare_split_source = op });
        } else {
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().prepareSource(op.source_group_id, op.destination_group_id, op.split_key, op.source_range_end);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localStartSplitSource(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "start_split_source")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.data_raft != null) {
            lockAtomic(&self.local_transition_mutex);
            defer self.local_transition_mutex.unlock();
            try self.replicateSplitSourceTransition(op.source_group_id, op.destination_group_id, .start, "");
        } else if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .start_split_source = op });
        } else {
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().startSource(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localBootstrapSplitDestination(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "bootstrap_split_destination")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.data_raft != null) {
            lockAtomic(&self.local_transition_mutex);
            defer self.local_transition_mutex.unlock();
            try self.replicateSplitBootstrap(
                op.source_group_id,
                op.destination_group_id,
                op.destination_base_uri orelse return error.MissingDestinationRoute,
            );
        } else if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .bootstrap_split_destination = op });
        } else {
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().bootstrapDestination(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localCatchUpSplitDestination(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "catch_up_split_destination")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.data_raft != null) {
            lockAtomic(&self.local_transition_mutex);
            defer self.local_transition_mutex.unlock();
            try self.replicateSplitCatchUp(
                op.source_group_id,
                op.destination_group_id,
                op.destination_base_uri orelse return error.MissingDestinationRoute,
            );
        } else if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .catch_up_split_destination = op });
        } else {
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().catchUpDestination(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn replicateSplitBootstrap(
        self: *DataServer,
        source_group_id: u64,
        destination_group_id: u64,
        destination_base_uri: []const u8,
    ) !void {
        const source_root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, source_group_id);
        defer self.alloc.free(source_root_dir);
        try self.ensureSplitSourceApplyStoreSeeded(source_root_dir, source_group_id);
        const source_store = self.localTransitionApplyStore() orelse return error.MissingSplitSourceStore;
        const handoff = try source_store.captureSplitHandoff(self.alloc, source_group_id);
        defer antfly.data.storage.shard_state_store.freeHandoff(self.alloc, handoff);
        const table_name = (try self.tableNameForLocalGroupAlloc(source_group_id)) orelse return error.UnknownGroup;
        defer self.alloc.free(table_name);
        const replication = try self.splitReplicationContext(source_group_id, destination_group_id);

        const max_writes_per_batch: usize = 128;
        var offset: usize = 0;
        while (offset < handoff.entries.len) {
            const end = @min(offset + max_writes_per_batch, handoff.entries.len);
            const writes = try self.alloc.alloc(antfly.db.types.BatchWrite, end - offset);
            defer self.alloc.free(writes);
            for (handoff.entries[offset..end], 0..) |entry, i| {
                writes[i] = .{ .key = entry.key, .value = entry.value };
            }
            try self.sendSplitDestinationBatch(destination_base_uri, destination_group_id, table_name, replication, .{
                .writes = writes,
                .sync_level = .write,
            });
            offset = end;
        }
        try self.replicateSplitCheckpoint(destination_base_uri, source_group_id, destination_group_id, table_name, replication, handoff.byte_range, handoff.base_delta_sequence);
    }

    fn replicateSplitSourceTransition(
        self: *DataServer,
        source_group_id: u64,
        destination_group_id: u64,
        kind: antfly.db.types.SplitTransitionMutation.Kind,
        split_key: []const u8,
    ) !void {
        const source_root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, source_group_id);
        defer self.alloc.free(source_root_dir);
        try self.ensureSplitSourceApplyStoreSeeded(source_root_dir, source_group_id);
        const source_store = self.localTransitionApplyStore() orelse return error.MissingSplitSourceStore;
        const source_state = try source_store.currentSplitState(self.alloc, source_group_id);
        defer if (source_state) |state| antfly.data.storage.shard_state_store.freeSplitState(self.alloc, state);

        const effective_split_key = if (kind == .start and split_key.len == 0 and source_state != null)
            source_state.?.split_key
        else
            split_key;
        switch (kind) {
            .prepare => if (source_state) |state| {
                if (std.mem.eql(u8, state.split_key, effective_split_key)) return;
                return error.ConflictingSplitTransition;
            },
            .start => if (source_state) |state| {
                if (state.phase != .prepare) {
                    if (std.mem.eql(u8, state.split_key, effective_split_key) and state.new_shard_id == destination_group_id) return;
                    return error.ConflictingSplitTransition;
                }
            } else return error.SplitInProgress,
            .finalize, .rollback => if (source_state == null) return,
        }

        const table_name = (try self.tableNameForLocalGroupAlloc(source_group_id)) orelse return error.UnknownGroup;
        defer self.alloc.free(table_name);
        try self.proposeRaftBatchGroup(self.alloc, source_group_id, table_name, .{
            .sync_level = .write,
            .split_transition = .{
                .kind = kind,
                .destination_group_id = destination_group_id,
                .split_key = effective_split_key,
            },
        }, true, false);
    }

    fn replicateSplitCatchUp(
        self: *DataServer,
        source_group_id: u64,
        destination_group_id: u64,
        destination_base_uri: []const u8,
    ) !void {
        const source_store = self.localTransitionApplyStore() orelse return error.MissingSplitSourceStore;
        const table_name = (try self.tableNameForLocalGroupAlloc(source_group_id)) orelse return error.UnknownGroup;
        defer self.alloc.free(table_name);
        const source_state = (try source_store.currentSplitState(self.alloc, source_group_id)) orelse return error.SplitInProgress;
        defer antfly.data.storage.shard_state_store.freeSplitState(self.alloc, source_state);
        const replication = try self.splitReplicationContext(source_group_id, destination_group_id);
        const source_seq = try source_store.currentSplitDeltaSequence(self.alloc, source_group_id);
        const source_ack = try self.splitProgressForSource(source_group_id, destination_group_id);
        const after_seq = if (source_ack) |ack| ack else 0;
        const deltas = try source_store.listSplitDeltasAfter(self.alloc, source_group_id, after_seq);
        defer antfly.shard.freeDeltas(self.alloc, deltas);

        for (deltas) |delta| {
            var writes = std.ArrayListUnmanaged(antfly.db.types.BatchWrite).empty;
            defer writes.deinit(self.alloc);
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (deletes.items) |key| self.alloc.free(@constCast(key));
                deletes.deinit(self.alloc);
            }
            var owned_write_keys = std.ArrayListUnmanaged([]u8).empty;
            defer {
                for (owned_write_keys.items) |key| self.alloc.free(key);
                owned_write_keys.deinit(self.alloc);
            }
            for (delta.writes) |write| {
                const key = try decodeSplitDeltaDocumentKeyAlloc(self.alloc, write.key);
                try owned_write_keys.append(self.alloc, key);
                try writes.append(self.alloc, .{ .key = key, .value = write.value });
            }
            for (delta.deletes) |raw_key| {
                try deletes.append(self.alloc, try decodeSplitDeltaDocumentKeyAlloc(self.alloc, raw_key));
            }
            if (writes.items.len > 0 or deletes.items.len > 0) {
                try self.sendSplitDestinationBatch(destination_base_uri, destination_group_id, table_name, replication, .{
                    .writes = writes.items,
                    .deletes = deletes.items,
                    .sync_level = .write,
                });
            }
        }
        try self.replicateSplitCheckpoint(destination_base_uri, source_group_id, destination_group_id, table_name, replication, .{
            .start = source_state.split_key,
            .end = source_state.original_range_end,
        }, source_seq);
    }

    fn replicateSplitCheckpoint(
        self: *DataServer,
        destination_base_uri: []const u8,
        source_group_id: u64,
        destination_group_id: u64,
        table_name: []const u8,
        replication: antfly.db.types.SplitReplicationContext,
        byte_range: antfly.db.types.ByteRange,
        delta_sequence: u64,
    ) !void {
        try self.sendSplitDestinationBatch(destination_base_uri, destination_group_id, table_name, replication, .{
            .sync_level = .write,
            .split_checkpoint = .{
                .kind = .destination,
                .source_group_id = source_group_id,
                .destination_group_id = destination_group_id,
                .range_start = byte_range.start,
                .range_end = byte_range.end,
                .delta_sequence = delta_sequence,
            },
        });
        try self.proposeRaftBatchGroup(self.alloc, source_group_id, table_name, .{
            .sync_level = .write,
            .split_checkpoint = .{
                .kind = .source_ack,
                .source_group_id = source_group_id,
                .destination_group_id = destination_group_id,
                .delta_sequence = delta_sequence,
            },
        }, true, false);
    }

    fn sendSplitDestinationBatch(
        self: *DataServer,
        destination_base_uri: []const u8,
        destination_group_id: u64,
        table_name: []const u8,
        replication: antfly.db.types.SplitReplicationContext,
        req: antfly.db.types.BatchRequest,
    ) !void {
        if (replication.destination_group_id != destination_group_id) return error.InvalidBatchRequest;
        var executor = antfly.raft.transport.StdHttpExecutor.init(self.alloc, .{});
        defer executor.deinit();
        var client = antfly.public_api.ApiHttpClient.init(self.alloc, executor.executor());
        var replicated_req = req;
        replicated_req.split_replication = replication;
        const body = try antfly.public_api.batch.encodeBatchRequest(self.alloc, replicated_req);
        defer self.alloc.free(body);
        // Transition work is retried from persisted metadata state. Bound each
        // attempt so a destination election cannot starve metadata Raft ticks.
        var response = try client.fetchGroupBatchWithTimeout(destination_base_uri, destination_group_id, table_name, body, 250);
        response.deinit(self.alloc);
    }

    fn splitReplicationContext(
        self: *DataServer,
        source_group_id: u64,
        destination_group_id: u64,
    ) !antfly.db.types.SplitReplicationContext {
        if (source_group_id == destination_group_id) return error.InvalidBatchRequest;
        return .{
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .identity_namespace = (try self.identityNamespaceForLocalGroup(source_group_id)) orelse
                return error.MissingIdentityNamespace,
        };
    }

    fn localFinalizeSplitSource(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "finalize_split_source")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.data_raft != null) {
            lockAtomic(&self.local_transition_mutex);
            defer self.local_transition_mutex.unlock();
            try self.replicateSplitSourceTransition(op.source_group_id, op.destination_group_id, .finalize, "");
        } else if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .finalize_split_source = op });
        } else {
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
            var runtime = try self.initLocalSplitRuntime(op.source_group_id, op.destination_group_id);
            defer runtime.deinit();
            _ = try runtime.runtime().finalizeSource(op.source_group_id, op.destination_group_id);
        }
        self.invalidateLocalGroupStatusCache();
    }

    fn localRollbackSplit(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "rollback_split")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.data_raft != null) {
            lockAtomic(&self.local_transition_mutex);
            defer self.local_transition_mutex.unlock();
            try self.replicateSplitSourceTransition(op.source_group_id, op.destination_group_id, .rollback, "");
        } else if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .rollback_split = op });
        } else {
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
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
        try self.ensureSplitSourceApplyStoreSeeded(source_root_dir, source_group_id);
        const destination_namespace = try self.identityNamespaceForSplitDestination(source_group_id, destination_group_id);
        const dest_lease = try self.leaseTransitionDbForTableGroup(source_group_id, destination_group_id, destination_namespace);
        errdefer if (dest_lease) |lease| lease.release();
        dest_db_options.identity_namespace = destination_namespace;
        return try antfly.raft.SplitCoordinatorRuntime.init(self.alloc, .{
            .source_root_dir = source_root_dir,
            .dest_root_dir = dest_root_dir,
            .source_group_id = source_group_id,
            .dest_group_id = destination_group_id,
            .source_store = self.localTransitionApplyStore(),
            .dest = .{ .root_dir = dest_root_dir, .db = dest_db_options },
            .dest_lease = dest_lease,
        });
    }

    fn observeLocalSplit(self: *DataServer, source_group_id: u64, destination_group_id: u64) !antfly.data.SplitTransitionStatus {
        const source_root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, source_group_id);
        defer self.alloc.free(source_root_dir);
        try self.ensureSplitSourceApplyStoreSeeded(source_root_dir, source_group_id);
        const source_namespace = try self.identityNamespaceForLocalGroup(source_group_id);
        const source_lease = try self.leaseTransitionDbForTableGroup(source_group_id, source_group_id, source_namespace);
        defer if (source_lease) |lease| lease.release();
        const progress_db = if (source_lease) |lease| lease.db else return error.MissingSplitProgressStore;
        return try antfly.data.storage.observeSplitStatus(self.alloc, .{
            .source_root_dir = source_root_dir,
            .dest_root_dir = "",
            .source_group_id = source_group_id,
            .dest_group_id = destination_group_id,
            .source = .{ .root_dir = source_root_dir },
            .source_store = self.localTransitionApplyStore(),
            .progress_db = progress_db,
        });
    }

    fn observeReplicatedSplit(self: *DataServer, source_group_id: u64, destination_group_id: u64) !antfly.data.SplitTransitionStatus {
        const source_root_dir = try antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, source_group_id);
        defer self.alloc.free(source_root_dir);
        try self.ensureSplitSourceApplyStoreSeeded(source_root_dir, source_group_id);
        const source_store = self.localTransitionApplyStore() orelse return error.MissingSplitSourceStore;
        const source_state = try source_store.currentSplitState(self.alloc, source_group_id);
        defer if (source_state) |state| antfly.data.storage.shard_state_store.freeSplitState(self.alloc, state);
        const acknowledgement = try source_store.currentSplitAcknowledgement(self.alloc, source_group_id);
        const bootstrapped = if (acknowledgement) |ack| ack.destination_group_id == destination_group_id else false;
        return antfly.data.storage.range_transition.deriveSplitStatus(
            if (source_state) |state| state.phase else null,
            bootstrapped,
            try source_store.currentSplitDeltaSequence(self.alloc, source_group_id),
            if (bootstrapped) acknowledgement.?.delta_sequence else 0,
        );
    }

    fn splitProgressForSource(self: *DataServer, source_group_id: u64, destination_group_id: u64) !?u64 {
        const namespace = try self.identityNamespaceForLocalGroup(source_group_id);
        const lease = (try self.leaseTransitionDbForTableGroup(source_group_id, source_group_id, namespace)) orelse return null;
        defer lease.release();
        const marker = (try lease.db.getSplitBootstrapMarker(self.alloc)) orelse return null;
        if (marker.source_group_id != source_group_id or marker.destination_group_id != destination_group_id) return null;
        return try lease.db.getSplitDeltaFinalSeq(self.alloc);
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
        const receiver_lease = try self.leaseTransitionDbForTableGroup(receiver_group_id, receiver_group_id, receiver_namespace);
        errdefer if (receiver_lease) |lease| lease.release();
        return try antfly.raft.MergeCoordinatorRuntime.init(self.alloc, .{
            .donor_root_dir = donor_root_dir,
            .receiver_root_dir = receiver_root_dir,
            .donor_group_id = donor_group_id,
            .receiver_group_id = receiver_group_id,
            .donor_store = self.localTransitionApplyStore(),
            .receiver = .{ .root_dir = receiver_root_dir, .db = receiver_db_options },
            .receiver_lease = receiver_lease,
            .receiver_identity_reassignment_namespace = receiver_namespace,
        });
    }

    fn leaseTransitionDbForTableGroup(
        self: *DataServer,
        table_group_id: u64,
        db_group_id: u64,
        expected_identity_namespace: ?antfly.db.DocIdentityNamespace,
    ) !?antfly.data.storage.db_split_handoff.BorrowedDestinationDb {
        if (try self.leaseExistingTransitionDb(db_group_id)) |cached_value| {
            var cached = cached_value;
            const namespace_matches = if (expected_identity_namespace) |expected|
                cached.db.core.identity_namespace.eql(expected)
            else
                true;
            if (namespace_matches) {
                return try self.wrapTransitionDbLease(cached);
            }
            // Release the stale generation before the explicit transition
            // open below retires it and installs the source namespace.
            cached.deinit(self.alloc);
        }
        const table_name = (try self.tableNameForLocalGroupAlloc(table_group_id)) orelse return null;
        defer self.alloc.free(table_name);
        const cached = if (expected_identity_namespace) |namespace|
            (try self.liveRuntimeWriteSource().leaseCachedTransitionGroupWriter(self.alloc, db_group_id, table_name, namespace)) orelse return null
        else
            (try self.liveRuntimeWriteSource().leaseCachedGroupWriter(self.alloc, db_group_id, table_name)) orelse return null;
        return try self.wrapTransitionDbLease(cached);
    }

    fn leaseExistingTransitionDb(
        self: *DataServer,
        group_id: u64,
    ) !?antfly.public_api.ProvisionedTableWriteCache.CachedDb {
        const live_source = self.liveRuntimeWriteSource();
        if (live_source.leaseManagedWriterGroupForTransition(group_id)) |cached| return cached;
        if (live_source != &self.write_source) {
            if (self.write_source.leaseManagedWriterGroupForTransition(group_id)) |cached| return cached;
        }
        return null;
    }

    fn wrapTransitionDbLease(
        self: *DataServer,
        cached: antfly.public_api.ProvisionedTableWriteCache.CachedDb,
    ) !antfly.data.storage.db_split_handoff.BorrowedDestinationDb {
        const ctx = self.alloc.create(TransitionDbLeaseContext) catch |err| {
            var owned = cached;
            owned.deinit(self.alloc);
            return err;
        };
        ctx.* = .{ .alloc = self.alloc, .cached = cached };
        return .{
            .db = ctx.cached.db,
            .ctx = ctx,
            .release_fn = TransitionDbLeaseContext.release,
        };
    }

    fn identityNamespaceForLocalGroup(self: *DataServer, group_id: u64) !?antfly.db.DocIdentityNamespace {
        if (self.remote_metadata) |remote_metadata| {
            var snapshot = (try remote_metadata.cachedSnapshot()) orelse return error.MetadataSnapshotUnavailable;
            defer freeAdminSnapshotOwned(self.alloc, &snapshot);
            const range = findRangeByGroupId(snapshot.ranges, group_id) orelse return null;
            return identityNamespaceFromRange(range);
        }
        var snapshot = try self.write_source.catalog.adminSnapshot();
        defer self.write_source.catalog.freeAdminSnapshot(&snapshot);
        const range = findRangeByGroupId(snapshot.ranges, group_id) orelse return null;
        return identityNamespaceFromRange(range);
    }

    fn identityNamespaceForSplitDestination(self: *DataServer, source_group_id: u64, destination_group_id: u64) !?antfly.db.DocIdentityNamespace {
        return (try self.identityNamespaceForLocalGroup(destination_group_id)) orelse
            try self.identityNamespaceForLocalGroup(source_group_id);
    }

    fn tableNameForLocalGroupAlloc(self: *DataServer, group_id: u64) !?[]u8 {
        if (self.remote_metadata) |remote_metadata| {
            var snapshot = (try remote_metadata.cachedSnapshot()) orelse return error.MetadataSnapshotUnavailable;
            defer freeAdminSnapshotOwned(self.alloc, &snapshot);
            const range = findRangeByGroupId(snapshot.ranges, group_id) orelse return null;
            const table = findTableById(snapshot.tables, range.table_id) orelse return null;
            return try self.alloc.dupe(u8, table.name);
        }
        var snapshot = try self.write_source.catalog.adminSnapshot();
        defer self.write_source.catalog.freeAdminSnapshot(&snapshot);
        const range = findRangeByGroupId(snapshot.ranges, group_id) orelse return null;
        const table = findTableById(snapshot.tables, range.table_id) orelse return null;
        return try self.alloc.dupe(u8, table.name);
    }

    fn ensureSplitSourceApplyStoreSeeded(self: *DataServer, source_root_dir: []const u8, source_group_id: u64) !void {
        if (self.localTransitionApplyStore()) |source_store| {
            return try self.ensureSplitSourceApplyStoreSeededInto(source_store, source_root_dir, source_group_id);
        }
        var source_store = try antfly.data.RaftApplyStore.init(self.alloc, .{ .root_dir = source_root_dir });
        defer source_store.deinit();
        try self.ensureSplitSourceApplyStoreSeededInto(&source_store, source_root_dir, source_group_id);
    }

    fn localTransitionApplyStore(self: *DataServer) ?*antfly.data.RaftApplyStore {
        const raft = self.data_raft orelse return null;
        return raft.host.owned_data_store;
    }

    fn ensureSplitSourceApplyStoreSeededInto(
        self: *DataServer,
        source_store: *antfly.data.RaftApplyStore,
        source_root_dir: []const u8,
        source_group_id: u64,
    ) !void {
        if (try source_store.currentSplitState(self.alloc, source_group_id)) |state| {
            antfly.data.storage.shard_state_store.freeSplitState(self.alloc, state);
            return;
        }
        // A live data-Raft store is already authoritative. Avoid reopening and
        // rescanning its document DB, and never replace state underneath an
        // apply that has established a durable watermark.
        if ((try source_store.latestBatch(source_group_id)) != null) return;

        if (try self.tableNameForLocalGroupAlloc(source_group_id)) |table_name| {
            defer self.alloc.free(table_name);
            if (try self.liveRuntimeWriteSource().leaseCachedGroupWriter(self.alloc, source_group_id, table_name)) |cached_db| {
                var cached = cached_db;
                defer cached.deinit(self.alloc);
                try self.seedSplitSourceApplyStoreFromDb(source_store, source_group_id, cached.db);
                return;
            }
        }

        var db = antfly.db.DB.open(self.alloc, source_root_dir, .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer db.close();
        try self.seedSplitSourceApplyStoreFromDb(source_store, source_group_id, &db);
    }

    fn seedSplitSourceApplyStoreFromDb(
        self: *DataServer,
        source_store: *antfly.data.RaftApplyStore,
        source_group_id: u64,
        db: *antfly.db.DB,
    ) !void {
        var entries = std.ArrayListUnmanaged(antfly.data.AppliedDataKV).empty;
        defer {
            for (entries.items) |entry| {
                self.alloc.free(@constCast(entry.key));
                self.alloc.free(@constCast(entry.value));
            }
            entries.deinit(self.alloc);
        }

        const byte_range = db.getRange();
        const lower = try antfly.internal_keys.documentRangeLowerAlloc(self.alloc, byte_range.start);
        defer self.alloc.free(lower);
        const upper = if (byte_range.end.len > 0) try antfly.internal_keys.documentRangeUpperAlloc(self.alloc, byte_range.end) else null;
        defer if (upper) |owned| self.alloc.free(owned);

        const scanned = try db.core.store.scanRange(self.alloc, lower, if (upper) |owned| owned else "");
        defer antfly.docstore.DocStore.freeResults(self.alloc, scanned);
        for (scanned) |entry| {
            const raw_key = (try antfly.internal_keys.decodePrimaryDocumentKeyAlloc(self.alloc, entry.key)) orelse continue;
            errdefer self.alloc.free(raw_key);
            try entries.append(self.alloc, .{
                .key = raw_key,
                .value = try self.alloc.dupe(u8, entry.value),
            });
        }
        _ = try source_store.seedGroupSnapshotIfAbsent(self.alloc, source_group_id, byte_range, entries.items);
    }

    fn localAcceptMergeReceiver(ptr: *anyopaque, op: @FieldType(antfly.metadata.TransitionAction, "accept_merge_receiver")) !void {
        const self: *DataServer = @ptrCast(@alignCast(ptr));
        if (self.local_transition_runtime) |runtime| {
            try runtime.shardOperationAdapter().execute(.{ .accept_merge_receiver = op });
        } else {
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
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
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
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
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
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
            self.lockLocalTransition();
            defer self.unlockLocalTransition();
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
        const record: antfly.metadata.table_manager.StoreRecord = .{
            .store_id = registration.store_id,
            .node_id = registration.node_id,
            .api_url = api_url,
            .raft_url = raft_url,
            .role = registration.role,
            .health_class = "healthy",
            .failure_domain = registration.failure_domain,
            .live = true,
        };
        try remote_metadata.registerNode(record);
        var snapshot = try remote_metadata.fetchSnapshot();
        defer freeAdminSnapshotOwned(self.alloc, &snapshot);
        if (!storeRegistrationVisible(snapshot.stores, record)) return error.StoreRegistrationNotVisible;
        self.store_registration_confirmed = true;
        self.clearMetadataBootstrapRetry();
        // Startup should not block on reopening every local group DB just to
        // compute an initial best-effort status report. Mark the store dirty
        // and let the main run loop publish status once listeners are up.
        self.store_status_dirty.store(true, .release);
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
            errdefer freeGroupStatusesOwned(alloc, stale);
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
                stale,
                group_ids,
                group_leadership_source,
                group_membership_source,
            );
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
                        var current = cached;
                        overlayLiveRaftGroupStatus(&current, group_leadership_source, group_membership_source);
                        try reports.append(alloc, current);
                        continue;
                    }
                    if (self.shouldSkipActiveStartupGroupStatusOpen(table.name, group_id, active_target)) {
                        continue;
                    }
                    if (self.hasReadBlockingActivityBestEffort(table.name, group_id)) {
                        if (try self.snapshotCachedLocalGroupStatusReport(alloc, group_id, generation, fingerprint, true)) |cached| {
                            var current = cached;
                            overlayLiveRaftGroupStatus(&current, group_leadership_source, group_membership_source);
                            try reports.append(alloc, current);
                            continue;
                        }
                        continue;
                    }

                    var writer_probe = self.probeManagedWriterGroupBestEffort(table.name, group_id);
                    defer writer_probe.deinit();
                    switch (writer_probe) {
                        .leased => {
                            if (try self.snapshotCachedLocalGroupStatusReport(alloc, group_id, generation, fingerprint, true)) |cached| {
                                var current = cached;
                                overlayLiveRaftGroupStatus(&current, group_leadership_source, group_membership_source);
                                try reports.append(alloc, current);
                            }
                            continue;
                        },
                        .unknown => {
                            if (try self.snapshotCachedLocalGroupStatusReport(alloc, group_id, generation, fingerprint, true)) |cached| {
                                var current = cached;
                                overlayLiveRaftGroupStatus(&current, group_leadership_source, group_membership_source);
                                try reports.append(alloc, current);
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
                        self.provisioned_storage.visibleRootGenerationForGroup(group_id),
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
                    .lsm_root_generation = self.provisioned_storage.visibleRootGenerationForGroup(group_id),
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
        // Claim the work before collecting the report. A mutation racing with
        // collection sets the bit again and is therefore published by a later
        // round; clearing at the end would lose that notification.
        _ = self.store_status_dirty.swap(false, .acq_rel);
        errdefer self.store_status_dirty.store(true, .release);
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
        self.last_store_status_report_at_ms = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        self.clearMetadataBootstrapRetry();
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
            var local_intent = intent;
            local_intent.peer_node_ids = peer_node_ids;
            try local_intents.append(self.alloc, local_intent);
        }
        const placement_fingerprint = dataRaftPlacementIntentsFingerprint(local_intents.items);
        const placement_changed = self.last_data_raft_placement_fingerprint == null or self.last_data_raft_placement_fingerprint.? != placement_fingerprint;
        if (placement_changed) {
            self.last_data_raft_placement_fingerprint = placement_fingerprint;
            self.invalidateLocalGroupStatusCache();
            self.markStoreStatusDirtyImmediate();
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
            const status = raft.host.http_host.host.raftStatus(intent.record.group_id);
            if (drainingLeaderShouldHandoff(snapshot.placement_intents, status, intent, registration.node_id)) {
                raft.host.http_host.campaignGroup(intent.record.group_id) catch |err| {
                    std.log.warn("data raft draining leader handoff campaign failed group_id={} node_id={} err={}", .{
                        intent.record.group_id,
                        registration.node_id,
                        err,
                    });
                    continue;
                };
                campaigned = true;
                continue;
            }
            if (!localIntentPreferredCampaigner(intent, registration.node_id)) continue;
            if (!localRaftStatusShouldBootstrapCampaign(status, registration.node_id)) continue;
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

        self.observeDataRaftStatusFingerprint(dataRaftLocalStatusFingerprint(raft, local_intents.items));
    }

    fn reportStoreStatusHeartbeat(self: *DataServer) !void {
        const remote_metadata = self.remote_metadata orelse return;
        const registration = self.store_registration orelse return;
        var report = (try self.cloneHeartbeatStoreStatusReport(registration.store_id)) orelse return try self.reportStoreStatus();
        defer freeStoreStatusReportOwned(self.alloc, &report);
        for (report.group_statuses) |*group_status| {
            overlayLiveRaftGroupStatus(group_status, self.group_leadership_source, self.group_membership_source);
        }
        try remote_metadata.reportNodeStatus(report);
        self.last_store_status_report_at_ms = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        self.clearMetadataBootstrapRetry();
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
        const started_epoch = self.provisioned_startup_catch_up_epoch.load(.acquire);
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
        defer self.provisioned_startup_catch_up_last_quarantined_groups.store(stats.quarantined_groups, .monotonic);
        defer self.provisioned_startup_catch_up_last_duration_ns.store(stats.duration_ns, .monotonic);
        defer stats.duration_ns = platform_time.monotonicNs() - started_ns;
        defer {
            const completion = startupCatchUpCompletion(
                stats,
                inspection_complete,
                started_epoch,
                self.provisioned_startup_catch_up_epoch.load(.acquire),
            );
            // Publish the deadline before dirty; the release store makes both
            // fields visible as one scheduler decision.
            self.provisioned_startup_catch_up_not_before_ms.store(completion.not_before_ms, .monotonic);
            self.provisioned_startup_catch_up_dirty.store(completion.dirty, .release);
        }
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
                        stats.unparked_debt_remaining = true;
                        continue;
                    },
                }

                const db_path = antfly.metadata.groupDbPathFromReplicaRoot(self.alloc, self.write_source.replica_root_dir, group_id) catch |err| {
                    _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                    std.log.warn("provisioned startup catch-up path failed group={} table={s} err={}", .{ group_id, table.name, err });
                    stats.debt_remaining = true;
                    stats.unparked_debt_remaining = true;
                    continue;
                };
                defer self.alloc.free(db_path);

                var io_impl = std.Io.Threaded.init(self.alloc, .{});
                defer io_impl.deinit();
                _ = statFilePath(io_impl.io(), db_path) catch |err| switch (err) {
                    error.FileNotFound => {
                        stats.debt_remaining = true;
                        stats.unparked_debt_remaining = true;
                        continue;
                    },
                    else => {
                        _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                        std.log.warn("provisioned startup catch-up stat failed group={} table={s} err={}", .{ group_id, table.name, err });
                        stats.debt_remaining = true;
                        stats.unparked_debt_remaining = true;
                        continue;
                    },
                };

                const result = result_blk: {
                    self.setProvisionedStartupCatchUpTarget(group_id, table.name) catch |err| {
                        _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                        std.log.warn("provisioned startup catch-up target set failed group={} table={s} err={}", .{ group_id, table.name, err });
                        stats.debt_remaining = true;
                        stats.unparked_debt_remaining = true;
                        continue;
                    };
                    defer self.clearProvisionedStartupCatchUpTarget();

                    break :result_blk self.liveRuntimeWriteSource().catchUpTableGroupBestEffortWithMetadata(self.alloc, group_id, table.name, .{
                        .indexes_json = table.indexes_json,
                        .schema_json = table.schema_json,
                        .identity_namespace = .{
                            .table_id = table.table_id,
                            .shard_id = antfly.metadata.table_manager.rangeDocIdentityShardId(range),
                            .range_id = antfly.metadata.table_manager.rangeDocIdentityRangeId(range),
                        },
                    }) catch |err| {
                        _ = self.provisioned_startup_catch_up_failed.fetchAdd(1, .monotonic);
                        std.log.warn("provisioned startup catch-up failed group={} table={s} err={}", .{ group_id, table.name, err });
                        stats.debt_remaining = true;
                        stats.unparked_debt_remaining = true;
                        continue;
                    };
                };
                stats.group_count += 1;
                if (result.busy) {
                    stats.busy_groups += 1;
                    if (self.runtime_status_dirty.load(.acquire) or self.runtime_status_refresh_active.load(.acquire)) {
                        stats.debt_remaining = true;
                        stats.unparked_debt_remaining = true;
                        continue;
                    }
                    const cached_debt: ?bool = self.cachedRuntimeStatusHasStartupCatchUpDebt(table.name, group_id) catch true;
                    if (cached_debt orelse true) {
                        stats.debt_remaining = true;
                        stats.unparked_debt_remaining = true;
                    }
                    continue;
                }
                if (result.quarantined) {
                    stats.groups_with_debt += 1;
                    stats.quarantined_groups += 1;
                    stats.debt_remaining = true;
                    if (stats.earliest_retry_at_ms == 0 or result.retry_at_ms < stats.earliest_retry_at_ms) {
                        stats.earliest_retry_at_ms = result.retry_at_ms;
                    }
                    if (result.quarantine_retry_scheduled) {
                        const retry_in_ms = result.retry_at_ms -| started_at_ms;
                        std.log.err("provisioned startup catch-up quarantined after repeated zero progress group={} table={s} retry_in_ms={}", .{ group_id, table.name, retry_in_ms });
                    }
                    continue;
                }
                if (!result.had_debt) continue;
                stats.groups_with_debt += 1;
                if (result.terminal_degraded) {
                    std.log.warn("provisioned startup catch-up found terminal degraded state group={} table={s}", .{ group_id, table.name });
                    self.runtime_status_dirty.store(true, .release);
                    self.store_status_dirty.store(true, .release);
                    continue;
                }
                if (result.cleared_debt) {
                    stats.groups_cleared += 1;
                } else {
                    stats.debt_remaining = true;
                    stats.unparked_debt_remaining = true;
                    std.log.warn("provisioned startup catch-up debt persists group={} table={s}", .{ group_id, table.name });
                }
            }
        }

        inspection_complete = true;
        if (stats.groups_cleared > 0) {
            self.runtime_status_dirty.store(true, .release);
            self.store_status_dirty.store(true, .release);
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
        const not_before_ms = self.provisioned_startup_catch_up_not_before_ms.load(.acquire);
        if (not_before_ms != 0 and now_ms < not_before_ms) return;
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
        self.clearProvisionedStartupCatchUpBackoffs();
        self.provisioned_startup_catch_up_dirty.store(true, .release);
        try self.requestProvisionedStartupCatchUp();
    }

    fn clearProvisionedStartupCatchUpBackoffs(self: *DataServer) void {
        _ = self.provisioned_startup_catch_up_epoch.fetchAdd(1, .release);
        self.provisioned_startup_catch_up_not_before_ms.store(0, .release);
        const source = self.liveRuntimeWriteSource();
        source.clearStartupCatchUpBackoffs();
        if (source != &self.write_source) self.write_source.clearStartupCatchUpBackoffs();
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

        const finished = self.liveRuntimeWriteSource().tryFinishExpiredAutoBulkIngestAndPublishStatus(self.alloc) orelse {
            _ = self.auto_bulk_finish_lock_deferred.fetchAdd(1, .monotonic);
            _ = self.auto_bulk_finish_completed.fetchAdd(1, .monotonic);
            return;
        };
        if (finished) {
            self.runtime_status_dirty.store(true, .release);
            self.store_status_dirty.store(true, .release);
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

        const status_generation = self.runtime_status_generation.fetchAdd(1, .monotonic);
        const snapshots = self.collectOwnedRuntimeSnapshots(active_target_owned, &budget, status_generation) catch |err| {
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
        self.store_status_dirty.store(true, .release);
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

    fn cachedRuntimeStatusHasStartupCatchUpDebt(self: *DataServer, table_name: []const u8, group_id: u64) !?bool {
        const status = try self.provisioned_storage.runtime_status_cache.snapshotGroupStatus(self.alloc, table_name, group_id);
        var owned = status orelse return null;
        defer owned.deinit(self.alloc);
        return runtimeStatusStartupCatchUpDebtPresent((&[_]runtime_status.LocalTableRuntimeStatus{owned})[0..]);
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
        status_generation: u64,
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
                annotateRuntimeSnapshotOwner(&owned_entry, registration, status_generation);
                try snapshots.append(self.alloc, owned_entry);
            }
        }

        return try snapshots.toOwnedSlice(self.alloc);
    }

    fn annotateRuntimeSnapshotOwner(
        snapshot: *runtime_status.TableRuntimeSnapshot,
        registration: StoreRegistrationConfig,
        status_generation: u64,
    ) void {
        for (snapshot.statuses.items) |*status| {
            if (status.metadata.store_id == 0) status.metadata.store_id = registration.store_id;
            if (status.metadata.node_id == 0) status.metadata.node_id = registration.node_id;
            status.metadata.status_generation = status_generation;
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
                    if (cachedRuntimeStatusRemainsFresh(
                        status,
                        self.provisioned_storage.visibleRootGenerationForGroup(group_id),
                    )) {
                        status.metadata.source = .cached_snapshot;
                    } else {
                        setRuntimeStatusMetadata(&status, .cached_snapshot, .stale);
                    }
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

    fn cachedRuntimeStatusRemainsFresh(
        status: runtime_status.LocalTableRuntimeStatus,
        visible_root_generation: u64,
    ) bool {
        return status.metadata.freshness == .fresh and
            status.metadata.lsm_root_generation == visible_root_generation and
            !runtimeStatusHasActiveBackgroundWork(status);
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
        if (!self.liveRuntimeWriteSource().startup_catch_up_active.load(.acquire)) return null;
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
        self.store_status_dirty.store(true, .release);
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
            const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
            self.recordProvisionedRootRefreshMetadataError(err, now_ms) catch {
                self.provisioned_root_refresh_dirty.store(true, .release);
            };
            _ = self.provisioned_root_refresh_failed.fetchAdd(1, .monotonic);
            std.log.warn("provisioned replica-root refresh failed err={}", .{err});
            return;
        };
        self.clearMetadataBootstrapRetry();
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
        const refresh_write_source = self.liveRuntimeWriteSource();
        self.provisioned_storage.pruneGroupVisibleRootGenerations(local_group_ids);
        if (local_group_ids.len == 0) {
            self.provisioned_storage.read_cache.clear();
            try refresh_write_source.clearWriteCache();
            if (refresh_write_source != &self.write_source) try self.write_source.clearWriteCache();
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

        const fingerprint = blk: {
            const next_fingerprint = blk_fingerprint: {
                lockAtomic(refresh_write_source.localDbMutex());
                defer refresh_write_source.localDbMutex().unlock();

                try self.reportLocalSchemaProgress(head.metadata_group_id, registration.node_id, local_group_ids, snapshot.tables, snapshot.ranges);

                break :blk_fingerprint antfly.metadata.table_provisioner.provisioningFingerprint(
                    head.metadata_group_id,
                    local_group_ids,
                    snapshot.tables,
                    snapshot.ranges,
                );
            };
            if (self.last_provision_fingerprint == next_fingerprint) {
                self.last_provision_metadata_epoch = head.metadata_epoch;
                return;
            }

            const backend_runtime = try self.ensureBackendRuntime();
            for (local_group_ids) |group_id| {
                if (group_id == head.metadata_group_id) continue;
                const range = findRangeByGroupId(snapshot.ranges, group_id) orelse continue;
                const table = findTableById(snapshot.tables, range.table_id) orelse continue;

                {
                    var activity = refresh_write_source.beginGroupRefreshActivity(table.name, group_id);
                    defer activity.deinit();

                    var group_ids_one = [_]u64{group_id};
                    {
                        lockAtomic(refresh_write_source.localDbMutex());
                        defer refresh_write_source.localDbMutex().unlock();
                        _ = try refresh_write_source.reconcileReplicaRootTablesWithWriteCacheLocked(
                            self.alloc,
                            head.metadata_group_id,
                            group_ids_one[0..],
                            snapshot.tables,
                            snapshot.ranges,
                            backend_runtime,
                        );
                    }
                    try self.provisioned_storage.bumpGroupVisibleRootGenerations(group_ids_one[0..]);
                }
            }
            self.provisioned_storage.read_cache.clear();
            break :blk next_fingerprint;
        };

        self.pruneStaleVisibleWriteCaches();
        self.last_provision_fingerprint = fingerprint;
        self.last_provision_metadata_epoch = head.metadata_epoch;
        self.clearProvisionedStartupCatchUpBackoffs();
        self.invalidateLocalGroupStatusCache();
        self.runtime_status_dirty.store(true, .release);
        self.provisioned_startup_catch_up_dirty.store(true, .release);
        self.store_status_dirty.store(true, .release);
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

        var owned_backend_runtime: ?backend_runtime_mod.BackendRuntimeHandle = null;
        errdefer if (owned_backend_runtime) |*runtime| runtime.deinit();
        var backend_runtime = cfg.backend_runtime;

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
                if (backend_runtime == null) {
                    owned_backend_runtime = try backend_runtime_mod.BackendRuntimeHandle.init(alloc, .{});
                    backend_runtime = owned_backend_runtime.?.ptr();
                }
                const raft_backend_runtime = backend_runtime.?;

                data_raft_store = try alloc.create(raft_engine.core.MemoryStorage);
                data_raft_store.?.* = raft_engine.core.MemoryStorage.init(alloc);

                data_raft_factory = try alloc.create(DataDescriptorFactory);
                data_raft_factory.?.* = DataDescriptorFactory.init(alloc, data_raft_store.?);

                data_raft_apply = try alloc.create(RaftTableApplyStateMachine);
                data_raft_apply.?.* = RaftTableApplyStateMachine.init(
                    alloc,
                    cfg.replica_root_dir,
                    remote_metadata.catalogSource(),
                    raft_backend_runtime,
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
                        .listener = antfly.raft.httpListenerConfig(cfg.raft_bind_host, cfg.raft_bind_port),
                        .transport = .{
                            .snapshot = .{
                                .root_dir = cfg.snapshot_root_dir orelse cfg.replica_root_dir,
                            },
                        },
                    },
                }, .{
                    .http = .{
                        .backend_runtime = raft_backend_runtime,
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

        const server = DataServer{
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
            .ha_cfg = cfg.ha,
            .query_async_limit = cfg.query_async_limit,
            .backend_runtime = backend_runtime,
            .owned_backend_runtime = owned_backend_runtime,
            .listener_cfg = publicApiListenerConfig(cfg.bind_host, cfg.bind_port),
        };
        owned_backend_runtime = null;
        return server;
    }
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    // Bounded spin, then yield (platform_sync): this guards the raft round
    // (which can run for milliseconds), and a pure spin here pins a core per
    // waiter — on CPU-constrained hosts (CI runners) that starves the very
    // threads that would release the lock.
    platform_sync.lockYielding(mutex);
}

fn haContextPrimaryIsFenced(ctx: antfly.ha.admin_exec.Context) bool {
    const primary = ctx.primary orelse return false;
    const fence_store = ctx.fence_store orelse return false;
    const node_id = ctx.primary_node_id orelse return false;
    const receipt = fence_store.currentBorrowed() orelse return false;
    return antfly.ha.write_gate.primaryFencedByReceipt(primary.identity, node_id, receipt);
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
    const TestFaults = if (@import("builtin").is_test) struct {
        fetch_head_error: ?anyerror = null,
    } else struct {};

    alloc: std.mem.Allocator,
    base_uris: [][]u8,
    preferred_base_uri_index: usize = 0,
    cache_mutex: std.atomic.Mutex = .unlocked,
    cached_head: ?antfly.metadata_api.MetadataHead = null,
    cached_head_at_ms: u64 = 0,
    cached_snapshot: ?antfly.metadata_api.AdminSnapshot = null,
    cached_snapshot_at_ms: u64 = 0,
    test_faults: TestFaults = .{},

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
        if (@import("builtin").is_test) {
            if (self.test_faults.fetch_head_error) |err| return err;
        }
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
                .replace_table_definition = remoteReplaceTableDefinition,
                .restore_table = remoteRestoreTable,
                .drop_table = remoteDropTable,
                .update_schema = remoteUpdateSchema,
                .create_index = remoteCreateIndex,
                .drop_index = remoteDropIndex,
                .put_artifact_enrichment = remotePutArtifactEnrichment,
                .delete_artifact_enrichment = remoteDeleteArtifactEnrichment,
                .wait_table_lifecycle = remoteWaitTableLifecycle,
                .wait_table_projection = remoteWaitTableProjection,
                .install_extension = remoteInstallExtension,
                .update_extension = remoteUpdateExtension,
                .drop_extension = remoteDropExtension,
                .enable_extension = remoteEnableExtension,
                .disable_extension = remoteDisableExtension,
                .configure_extension = remoteConfigureExtension,
                .restore_extensions = remoteRestoreExtensions,
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

    fn remoteReplaceTableDefinition(ptr: *anyopaque, expected: antfly.metadata.TableRecord, replacement: antfly.metadata.TableRecord) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const body = try std.json.Stringify.valueAlloc(self.alloc, .{
            .expected = expected,
            .definition = replacement,
        }, .{});
        defer self.alloc.free(body);
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.replaceTableDefinition(base_uri, ctx.table_name, ctx.body);
            }
        }.call, .{ .table_name = replacement.name, .body = body });
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

    fn remotePutArtifactEnrichment(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, artifact_name: []const u8, enrichment_json: []const u8) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.putArtifactEnrichment(base_uri, ctx.table_name, ctx.artifact_name, ctx.enrichment_json);
            }
        }.call, .{ .table_name = table_name, .artifact_name = artifact_name, .enrichment_json = enrichment_json });
        self.invalidateCache();
    }

    fn remoteDeleteArtifactEnrichment(ptr: *anyopaque, _: std.mem.Allocator, table_name: []const u8, artifact_name: []const u8) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.deleteArtifactEnrichment(base_uri, ctx.table_name, ctx.artifact_name);
            }
        }.call, .{ .table_name = table_name, .artifact_name = artifact_name });
        self.invalidateCache();
    }

    fn remoteInstallExtension(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        extension_name: []const u8,
        req: antfly.extensions.InstallExtensionRequest,
    ) !antfly.extensions.InstalledExtension {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const body = try stringifyJsonAlloc(alloc, req);
        defer alloc.free(body);
        const installed = try self.withMetadataApiClient(antfly.extensions.InstalledExtension, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !antfly.extensions.InstalledExtension {
                var parsed = try client.installExtension(base_uri, ctx.extension_name, ctx.body);
                defer parsed.deinit();
                return try antfly.extensions.cloneInstalledExtensionAlloc(ctx.alloc, parsed.value);
            }
        }.call, .{ .alloc = alloc, .extension_name = extension_name, .body = body });
        if (!req.dry_run) self.invalidateCache();
        return installed;
    }

    fn remoteUpdateExtension(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        extension_name: []const u8,
        req: antfly.extensions.UpdateExtensionRequest,
    ) !antfly.extensions.InstalledExtension {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const body = try stringifyJsonAlloc(alloc, req);
        defer alloc.free(body);
        const installed = try self.withMetadataApiClient(antfly.extensions.InstalledExtension, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !antfly.extensions.InstalledExtension {
                var parsed = try client.updateExtension(base_uri, ctx.extension_name, ctx.body);
                defer parsed.deinit();
                return try antfly.extensions.cloneInstalledExtensionAlloc(ctx.alloc, parsed.value);
            }
        }.call, .{ .alloc = alloc, .extension_name = extension_name, .body = body });
        if (!req.dry_run) self.invalidateCache();
        return installed;
    }

    fn remoteDropExtension(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        extension_name: []const u8,
        req: antfly.extensions.DropExtensionRequest,
    ) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const body = try stringifyJsonAlloc(alloc, req);
        defer alloc.free(body);
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !void {
                try client.dropExtension(base_uri, ctx.extension_name, ctx.body);
            }
        }.call, .{ .extension_name = extension_name, .body = body });
        if (!req.dry_run) self.invalidateCache();
    }

    fn remoteEnableExtension(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        extension_name: []const u8,
    ) !antfly.extensions.InstalledExtension {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const installed = try self.withMetadataApiClient(antfly.extensions.InstalledExtension, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !antfly.extensions.InstalledExtension {
                var parsed = try client.enableExtension(base_uri, ctx.extension_name);
                defer parsed.deinit();
                return try antfly.extensions.cloneInstalledExtensionAlloc(ctx.alloc, parsed.value);
            }
        }.call, .{ .alloc = alloc, .extension_name = extension_name });
        self.invalidateCache();
        return installed;
    }

    fn remoteDisableExtension(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        extension_name: []const u8,
    ) !antfly.extensions.InstalledExtension {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const installed = try self.withMetadataApiClient(antfly.extensions.InstalledExtension, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !antfly.extensions.InstalledExtension {
                var parsed = try client.disableExtension(base_uri, ctx.extension_name);
                defer parsed.deinit();
                return try antfly.extensions.cloneInstalledExtensionAlloc(ctx.alloc, parsed.value);
            }
        }.call, .{ .alloc = alloc, .extension_name = extension_name });
        self.invalidateCache();
        return installed;
    }

    fn remoteConfigureExtension(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        extension_name: []const u8,
        req: antfly.extensions.ConfigureExtensionRequest,
    ) !antfly.extensions.InstalledExtension {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const body = try stringifyJsonAlloc(alloc, req);
        defer alloc.free(body);
        const installed = try self.withMetadataApiClient(antfly.extensions.InstalledExtension, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: anytype) !antfly.extensions.InstalledExtension {
                var parsed = try client.configureExtension(base_uri, ctx.extension_name, ctx.body);
                defer parsed.deinit();
                return try antfly.extensions.cloneInstalledExtensionAlloc(ctx.alloc, parsed.value);
            }
        }.call, .{ .alloc = alloc, .extension_name = extension_name, .body = body });
        self.invalidateCache();
        return installed;
    }

    fn remoteRestoreExtensions(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        installed: []const antfly.extensions.InstalledExtension,
        members: []const antfly.extensions.ExtensionMember,
        dependencies: []const antfly.extensions.ExtensionDependency,
    ) !void {
        const self: *RemoteMetadataSource = @ptrCast(@alignCast(ptr));
        const body = try stringifyJsonAlloc(alloc, .{
            .installed_extensions = installed,
            .extension_members = members,
            .extension_dependencies = dependencies,
        });
        defer alloc.free(body);
        try self.withMetadataApiClient(void, struct {
            fn call(_: *RemoteMetadataSource, client: *antfly.metadata_http_client.MetadataHttpClient, base_uri: []const u8, ctx: []const u8) !void {
                try client.restoreExtensions(base_uri, ctx);
            }
        }.call, body);
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
        .extension_packages = try cloneExtensionPackagesOwned(alloc, snapshot.extension_packages),
        .installed_extensions = try cloneInstalledExtensionsOwned(alloc, snapshot.installed_extensions),
        .extension_members = try cloneExtensionMembersOwned(alloc, snapshot.extension_members),
        .extension_dependencies = try cloneExtensionDependenciesOwned(alloc, snapshot.extension_dependencies),
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
        hasher.update(std.mem.asBytes(&membership.voter_set_known));
        hasher.update(&membership.voter_set_fingerprint);
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
    hasher.update(std.mem.asBytes(&status.voter_set_known));
    hasher.update(&status.voter_set_fingerprint);
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
        .coverage_produced_count = index.coverage_produced_count,
        .coverage_skipped_count = index.coverage_skipped_count,
        .coverage_terminal_failed_count = index.coverage_terminal_failed_count,
        .coverage_generation = index.coverage_generation,
        .coverage_config_hash = index.coverage_config_hash,
        .coverage_identity_ready = index.coverage_identity_ready,
        .coverage_summary_ready = index.coverage_summary_ready,
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

fn storeRegistrationVisible(
    stores: []const antfly.metadata.table_manager.StoreRecord,
    record: antfly.metadata.table_manager.StoreRecord,
) bool {
    for (stores) |store| {
        if (store.store_id != record.store_id) continue;
        if (store.node_id != record.node_id) continue;
        if (!std.mem.eql(u8, store.role, record.role)) continue;
        if (!std.mem.eql(u8, store.api_url, record.api_url)) continue;
        if (!std.mem.eql(u8, store.raft_url, record.raft_url)) continue;
        return true;
    }
    return false;
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

const HAReplicationRecordRoute = struct {
    group_id: u64,
    table_name: []const u8,
};

fn resolveHAReplicationRecordRoute(
    snapshot: *const antfly.metadata_api.AdminSnapshot,
    record: antfly.ha.replication_record.RecordView,
) !HAReplicationRecordRoute {
    if (record.table_id == 0) return error.HAReplicationRecordMissingTableId;
    if (record.shard_id == 0) return error.HAReplicationRecordMissingShardId;
    const table = findTableById(snapshot.tables, record.table_id) orelse return error.HAReplicationRecordUnknownTable;

    if (findRangeByGroupId(snapshot.ranges, record.shard_id)) |range| {
        if (range.table_id == record.table_id) {
            return .{ .group_id = range.group_id, .table_name = table.name };
        }
    }

    for (snapshot.ranges) |range| {
        if (range.table_id != record.table_id) continue;
        if (antfly.metadata.table_manager.rangeDocIdentityShardId(range) != record.shard_id) continue;
        return .{ .group_id = range.group_id, .table_name = table.name };
    }

    return error.HAReplicationRecordUnknownShard;
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
        .voter_set_known = membership.voter_set_known,
        .voter_set_fingerprint = membership.voter_set_fingerprint,
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
        .voter_set_known = membership.voter_set_known,
        .voter_set_fingerprint = membership.voter_set_fingerprint,
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
    for (reports.items) |*report| {
        overlayLiveRaftGroupStatus(report, group_leadership_source, group_membership_source);
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

fn overlayLiveRaftGroupStatus(
    report: *antfly.metadata.table_manager.GroupStatusReport,
    group_leadership_source: ?GroupLeadershipSource,
    group_membership_source: ?GroupMembershipSource,
) void {
    if (group_leadership_source) |source| report.local_leader = source.isLocalLeader(report.group_id);
    if (group_membership_source) |source| {
        const membership = source.membership(report.group_id);
        report.local_voter = membership.local_voter;
        report.voter_count = membership.voter_count;
        report.voter_set_known = membership.voter_set_known;
        report.voter_set_fingerprint = membership.voter_set_fingerprint;
        report.joint_consensus = membership.joint_consensus;
    }
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
        .voter_set_known = membership.voter_set_known,
        .voter_set_fingerprint = membership.voter_set_fingerprint,
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

fn drainingLeaderShouldHandoff(
    placement_intents: []const antfly.raft.PlacementIntent,
    status: ?raft_engine.core.Status,
    local_intent: antfly.raft.PlacementIntent,
    local_node_id: u64,
) bool {
    if (local_intent.serving_state != .serving) return false;
    const raft_status = status orelse return false;
    const leader_node_id = raft_status.soft.leader_id orelse return false;
    if (leader_node_id == local_node_id) return false;
    if (!DataServer.localRaftStatusIsVoter(raft_status, local_node_id)) return false;
    // A leader can disappear from the latest placement snapshot before its
    // followers observe a clean step-down. Treat both an explicitly draining
    // leader and an absent leader as stale; otherwise every surviving voter
    // can wait forever on a node that no longer hosts the group.
    if (groupLeaderPlacementIsServing(placement_intents, local_intent.record.group_id, leader_node_id)) return false;
    return localNodePreferredServingPeer(placement_intents, local_intent.record.group_id, local_node_id);
}

fn groupLeaderPlacementIsServing(
    placement_intents: []const antfly.raft.PlacementIntent,
    group_id: u64,
    leader_node_id: u64,
) bool {
    for (placement_intents) |intent| {
        if (intent.record.group_id == group_id and
            intent.record.local_node_id == leader_node_id and
            intent.serving_state == .serving)
        {
            return true;
        }
    }
    return false;
}

fn localNodePreferredServingPeer(
    placement_intents: []const antfly.raft.PlacementIntent,
    group_id: u64,
    local_node_id: u64,
) bool {
    var min_node_id: ?u64 = null;
    for (placement_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (intent.serving_state != .serving) continue;
        if (min_node_id == null or intent.record.local_node_id < min_node_id.?) {
            min_node_id = intent.record.local_node_id;
        }
    }
    return min_node_id != null and min_node_id.? == local_node_id;
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

fn dataRaftLocalStatusFingerprint(
    raft: *antfly.raft.ManagedHttpHostService,
    intents: []const antfly.raft.PlacementIntent,
) u64 {
    var hasher = std.hash.Wyhash.init(0x4d2c_e31a_7f8b_2026);
    hashU64(&hasher, intents.len);
    for (intents) |intent| {
        hashU64(&hasher, intent.record.group_id);
        const status = raft.host.http_host.host.raftStatus(intent.record.group_id) orelse {
            hashU64(&hasher, 0);
            continue;
        };
        hashU64(&hasher, 1);
        hashU64(&hasher, status.id);
        hashU64(&hasher, @intFromEnum(status.soft.role));
        if (status.soft.leader_id) |leader_id| {
            hashU64(&hasher, 1);
            hashU64(&hasher, leader_id);
        } else {
            hashU64(&hasher, 0);
        }
        hashU64(&hasher, status.conf_state.voters.len);
        for (status.conf_state.voters) |node_id| hashU64(&hasher, node_id);
        hashU64(&hasher, status.conf_state.voters_outgoing.len);
        for (status.conf_state.voters_outgoing) |node_id| hashU64(&hasher, node_id);
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
    for (snapshot.extension_packages) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (snapshot.extension_packages.len > 0) alloc.free(snapshot.extension_packages);
    for (snapshot.installed_extensions) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (snapshot.installed_extensions.len > 0) alloc.free(snapshot.installed_extensions);
    for (snapshot.extension_members) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (snapshot.extension_members.len > 0) alloc.free(snapshot.extension_members);
    for (snapshot.extension_dependencies) |record| {
        var owned = record;
        owned.deinitOwned(alloc);
    }
    if (snapshot.extension_dependencies.len > 0) alloc.free(snapshot.extension_dependencies);
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

fn cloneExtensionPackagesOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.extensions.PackageManifest,
) ![]antfly.extensions.PackageManifest {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.extensions.PackageManifest, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| {
            var owned = record;
            owned.deinitOwned(alloc);
        }
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try antfly.extensions.clonePackageManifestAlloc(alloc, record);
        initialized += 1;
    }
    return out;
}

fn cloneInstalledExtensionsOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.extensions.InstalledExtension,
) ![]antfly.extensions.InstalledExtension {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.extensions.InstalledExtension, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| {
            var owned = record;
            owned.deinitOwned(alloc);
        }
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try antfly.extensions.cloneInstalledExtensionAlloc(alloc, record);
        initialized += 1;
    }
    return out;
}

fn cloneExtensionMembersOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.extensions.ExtensionMember,
) ![]antfly.extensions.ExtensionMember {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.extensions.ExtensionMember, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| {
            var owned = record;
            owned.deinitOwned(alloc);
        }
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try antfly.extensions.cloneExtensionMemberAlloc(alloc, record);
        initialized += 1;
    }
    return out;
}

fn cloneExtensionDependenciesOwned(
    alloc: std.mem.Allocator,
    records: []const antfly.extensions.ExtensionDependency,
) ![]antfly.extensions.ExtensionDependency {
    if (records.len == 0) return &.{};
    const out = try alloc.alloc(antfly.extensions.ExtensionDependency, records.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |record| {
            var owned = record;
            owned.deinitOwned(alloc);
        }
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try antfly.extensions.cloneExtensionDependencyAlloc(alloc, record);
        initialized += 1;
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

    const metadata_api_urls = try resolveMetadataApiUrls(alloc, cli, if (loaded_config) |*cfg| cfg else null);
    defer metadata_api_urls.deinit(alloc);
    if ((cli.node_id == null) != (cli.store_id == null)) return error.InvalidArguments;
    const auth_enabled = resolveAuthEnabled(cli, if (loaded_config) |*cfg| cfg else null);
    const trusted_principal_secret = try resolveTrustedPrincipalSecret(
        alloc,
        if (secret_store_initialized) &secret_store else null,
    );
    defer if (trusted_principal_secret) |value| alloc.free(value);
    const effective_auth_enabled = auth_enabled or trusted_principal_secret != null;

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

    const trusted_principal_issuer = try resolveTrustedPrincipalIssuer(
        alloc,
        if (secret_store_initialized) &secret_store else null,
    );
    defer if (trusted_principal_issuer) |value| alloc.free(value);

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
            .auth_enabled = effective_auth_enabled,
            .trusted_principal_secret = trusted_principal_secret,
            .trusted_principal_issuer = trusted_principal_issuer,
            .ard_base_url = cli.ard_base_url,
            .ard_publisher_domain = cli.ard_publisher_domain orelse "antfly.local",
            .ard_display_name = cli.ard_display_name orelse "Antfly",
            .ard_public_catalog_enabled = cli.ard_public_catalog_enabled,
            .user_manager = if (user_manager) |*manager| manager else null,
            .secret_store = if (secret_store_initialized) &secret_store else null,
            .remote_content = if (loaded_config) |*cfg| if (cfg.remote_content) |*remote_content| remote_content else null else null,
            .inference_api_key = if (loaded_config) |*cfg| if (cfg.inference.api_key) |value| value else null else null,
            .extension_package_store_dir = resolved.extension_package_store_dir,
            .node_config = if (loaded_config) |*cfg| cfg else null,
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

    const tick_ms = cli.tick_ms orelse 100;
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
        if (std.mem.eql(u8, arg, "--ard-publisher-domain")) {
            cfg.ard_publisher_domain = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-base-url")) {
            cfg.ard_base_url = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-display-name")) {
            cfg.ard_display_name = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--ard-public-catalog")) {
            const value = args.next() orelse return error.InvalidArguments;
            cfg.ard_public_catalog_enabled = parseBoolFlag(value) orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.startsWith(u8, arg, "--ard-public-catalog=")) {
            cfg.ard_public_catalog_enabled = parseBoolFlag(arg["--ard-public-catalog=".len..]) orelse return error.InvalidArguments;
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
        if (std.mem.eql(u8, arg, "--extension-package-store")) {
            cfg.extension_package_store_dir = args.next() orelse return error.InvalidArguments;
            continue;
        }
        if (std.mem.eql(u8, arg, "--secret-store-path")) {
            try cfg.secret_store_paths.append(alloc, args.next() orelse return error.InvalidArguments);
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

fn resolvePaths(
    alloc: std.mem.Allocator,
    cli: CliConfig,
    cfg: ?*const antfly.common.config.Config,
) !ResolvedPaths {
    const local_base = try resolveLocalBaseDir(alloc, cli, cfg);
    defer alloc.free(local_base);
    const metadata_base = try std.fmt.allocPrint(alloc, "{s}/metadata", .{local_base});
    defer alloc.free(metadata_base);
    const extension_package_store_dir = try resolveExtensionPackageStoreDir(alloc, cli.extension_package_store_dir, local_base);
    errdefer alloc.free(extension_package_store_dir);

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
            .extension_package_store_dir = extension_package_store_dir,
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
        .extension_package_store_dir = extension_package_store_dir,
    };
}

fn ensureDirAndParent(io: std.Io, replica_root_dir: []const u8, replica_catalog_path: []const u8) !void {
    try fs_paths.createDirPathPortable(io, replica_root_dir);
    if (std.fs.path.dirname(replica_catalog_path)) |parent| {
        try fs_paths.createDirPathPortable(io, parent);
    }
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

    var probe = path;
    while (true) {
        const resolved_z = std.Io.Dir.realPathFileAbsoluteAlloc(io_impl.io(), probe, alloc) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => null,
            else => return err,
        };
        if (resolved_z) |resolved| {
            defer alloc.free(resolved);
            const resolved_prefix = resolved[0..resolved.len];
            if (probe.len == path.len) return try alloc.dupe(u8, resolved_prefix);

            const suffix_start: usize = if (probe.len == 1) 1 else probe.len + 1;
            const suffix = path[suffix_start..];
            return try std.fs.path.join(alloc, &.{ resolved_prefix, suffix });
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

fn resolveTrustedPrincipalSecret(
    alloc: std.mem.Allocator,
    secret_store: ?*antfly.common.secrets.FileStore,
) !?[]u8 {
    return try resolveTrustedPrincipalConfigValue(alloc, secret_store, trusted_principal_secret_key);
}

fn resolveTrustedPrincipalIssuer(
    alloc: std.mem.Allocator,
    secret_store: ?*antfly.common.secrets.FileStore,
) !?[]u8 {
    return try resolveTrustedPrincipalConfigValue(alloc, secret_store, trusted_principal_issuer_key);
}

fn resolveTrustedPrincipalConfigValue(
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
        \\  --ard-base-url <url>           Absolute public base URL for ARD catalog artifact links
        \\  --ard-publisher-domain <name>  ARD did:web publisher domain (default: antfly.local)
        \\  --ard-display-name <name>      ARD catalog host display name (default: Antfly)
        \\  --ard-public-catalog <bool>    Publish anonymous /.well-known ARD bootstrap when auth is enabled
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
        \\  --extension-package-store <path> Extension package store directory
        \\  --secret-store-path <path>     Antfly secrets.json file path; repeat for fallback layers
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

test "data raft bootstrap campaign retries leaderless voter elections" {
    var voters = [_]u64{ 1, 2, 3 };
    var status = raft_engine.core.Status{
        .id = 1,
        .group_id = 7001,
        .soft = .{ .leader_id = null, .role = .follower },
        .hard = .{},
        .conf_state = .{ .voters = voters[0..] },
    };

    try std.testing.expect(DataServer.localRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.role = .pre_candidate;
    try std.testing.expect(DataServer.localRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.role = .candidate;
    status.election_elapsed = 4;
    status.randomized_election_timeout = 5;
    try std.testing.expect(!DataServer.localRaftStatusShouldBootstrapCampaign(status, 1));
    status.election_elapsed = 5;
    try std.testing.expect(DataServer.localRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.role = .leader;
    status.soft.leader_id = 1;
    try std.testing.expect(!DataServer.localRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.role = .follower;
    status.soft.leader_id = 2;
    try std.testing.expect(!DataServer.localRaftStatusShouldBootstrapCampaign(status, 1));

    status.soft.leader_id = null;
    try std.testing.expect(!DataServer.localRaftStatusShouldBootstrapCampaign(status, 4));
}

test "data raft draining leader handoff campaigns preferred serving survivor" {
    var voters = [_]u64{ 101, 102, 103 };
    const status = raft_engine.core.Status{
        .id = 102,
        .group_id = 7002,
        .soft = .{ .leader_id = 101, .role = .follower },
        .hard = .{},
        .conf_state = .{ .voters = voters[0..] },
    };
    const intents = [_]antfly.raft.PlacementIntent{
        .{ .record = .{ .group_id = 7002, .replica_id = 1, .local_node_id = 101 }, .serving_state = .draining },
        .{ .record = .{ .group_id = 7002, .replica_id = 2, .local_node_id = 102 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 7002, .replica_id = 3, .local_node_id = 103 }, .serving_state = .serving },
    };

    try std.testing.expect(drainingLeaderShouldHandoff(&intents, status, intents[1], 102));
    try std.testing.expect(!drainingLeaderShouldHandoff(&intents, status, intents[2], 103));
    try std.testing.expect(!drainingLeaderShouldHandoff(&intents, status, intents[0], 101));
}

test "data raft removed leader handoff campaigns preferred serving survivor" {
    var voters = [_]u64{ 102, 103, 104 };
    const status = raft_engine.core.Status{
        .id = 102,
        .group_id = 7003,
        .soft = .{ .leader_id = 101, .role = .follower },
        .hard = .{},
        .conf_state = .{ .voters = voters[0..] },
    };
    const intents = [_]antfly.raft.PlacementIntent{
        .{ .record = .{ .group_id = 7003, .replica_id = 1, .local_node_id = 102 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 7003, .replica_id = 2, .local_node_id = 103 }, .serving_state = .serving },
        .{ .record = .{ .group_id = 7003, .replica_id = 3, .local_node_id = 104 }, .serving_state = .serving },
    };

    try std.testing.expect(drainingLeaderShouldHandoff(&intents, status, intents[0], 102));
    try std.testing.expect(!drainingLeaderShouldHandoff(&intents, status, intents[1], 103));
}

test "data runtime live writer source follows raft apply ownership" {
    const Catalog = struct {
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
                .status = .{ .metadata_group_id = 1, .metadata_epoch = 7, .metrics = .{} },
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

    var server: DataServer = undefined;
    server.data_raft_apply = null;
    server.write_source = antfly.public_api.ProvisionedTableWriteSource.init("/tmp/unused-antfly-live-writer-source", Catalog.iface());
    defer server.write_source.deinit();

    try std.testing.expectEqual(&server.write_source, server.liveRuntimeWriteSource());

    var apply_sm = RaftTableApplyStateMachine.init(std.testing.allocator, "/tmp/unused-antfly-live-writer-source", Catalog.iface(), null);
    defer apply_sm.deinit();
    server.data_raft_apply = &apply_sm;

    try std.testing.expectEqual(&apply_sm.write_source, server.liveRuntimeWriteSource());
}

test "data raft source split lifecycle commands bypass document db apply" {
    try std.testing.expect(!batchRequiresDocumentDbApply(.{
        .split_transition = .{
            .kind = .prepare,
            .destination_group_id = 7002,
            .split_key = "doc:m",
        },
    }));
    try std.testing.expect(batchRequiresDocumentDbApply(.{
        .split_checkpoint = .{
            .kind = .source_ack,
            .source_group_id = 7001,
            .destination_group_id = 7002,
            .delta_sequence = 1,
        },
    }));
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

    try server.initApiServer();
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
    try std.testing.expectEqualStrings("/run/antfly/secrets/secrets.json", cfg.secret_store_paths.items[0]);
}

test "data runtime cli accepts extension package store path" {
    const argv = [_][*:0]const u8{ "--extension-package-store", "/opt/antfly/extensions" };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("/opt/antfly/extensions", cfg.extension_package_store_dir.?);
}

test "data runtime cli accepts ARD identity flags" {
    const argv = [_][*:0]const u8{
        "--ard-publisher-domain",
        "tenant.example.com",
        "--ard-base-url",
        "https://tenant.example.com",
        "--ard-display-name",
        "Tenant Antfly",
        "--ard-public-catalog=true",
    };
    var iter = std.process.Args.Iterator.init(.{ .vector = argv[0..] });
    var cfg = try parseCli(std.testing.allocator, &iter);
    defer cfg.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("https://tenant.example.com", cfg.ard_base_url.?);
    try std.testing.expectEqualStrings("tenant.example.com", cfg.ard_publisher_domain.?);
    try std.testing.expectEqualStrings("Tenant Antfly", cfg.ard_display_name.?);
    try std.testing.expect(cfg.ard_public_catalog_enabled);
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
    try std.testing.expectEqualStrings("/tmp/antflydb/extensions", resolved.extension_package_store_dir);
}

test "data runtime resolves extension package store env before local default" {
    const alloc = std.testing.allocator;

    const env_resolved = try resolveExtensionPackageStoreDirWithEnv(alloc, null, "/tmp/antflydb", "/antfly-extension-env");
    defer alloc.free(env_resolved);
    try std.testing.expectEqualStrings("/antfly-extension-env", env_resolved);

    const cli_resolved = try resolveExtensionPackageStoreDirWithEnv(alloc, "/antfly-cli-extensions", "/tmp/antflydb", "/antfly-extension-env");
    defer alloc.free(cli_resolved);
    try std.testing.expectEqualStrings("/antfly-cli-extensions", cli_resolved);
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

test "data runtime resolves trusted principal secret from secret store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/trusted-principal-secrets.json", .{tmp.sub_path});
    defer alloc.free(store_path);

    var store = try antfly.common.secrets.FileStore.init(alloc, store_path);
    defer store.deinit();
    var entry = try store.put(alloc, trusted_principal_secret_key, "cloudaf-shared-secret");
    defer entry.deinit(alloc);

    const resolved = try resolveTrustedPrincipalSecret(alloc, &store);
    defer if (resolved) |value| alloc.free(value);
    try std.testing.expectEqualStrings("cloudaf-shared-secret", resolved.?);
}

test "data runtime resolves trusted principal issuer from secret store" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const store_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/trusted-principal-issuer.json", .{tmp.sub_path});
    defer alloc.free(store_path);

    var store = try antfly.common.secrets.FileStore.init(alloc, store_path);
    defer store.deinit();
    var entry = try store.put(alloc, trusted_principal_issuer_key, "cloudaf");
    defer entry.deinit(alloc);

    const resolved = try resolveTrustedPrincipalIssuer(alloc, &store);
    defer if (resolved) |value| alloc.free(value);
    try std.testing.expectEqualStrings("cloudaf", resolved.?);
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
            .identity_namespace = .{ .table_id = 7, .shard_id = 77, .range_id = 77 },
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
            .voter_set_known = true,
            .voter_set_fingerprint = antfly.metadata.table_manager.voterSetFingerprint(&.{ 1, 2, 3 }, null),
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

    harness.membership.voter_count = 3;
    harness.membership.voter_set_fingerprint = antfly.metadata.table_manager.voterSetFingerprint(&.{ 1, 2, 4 }, null);
    const after_same_count_membership_change = localGroupStatusFingerprint(
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
    try std.testing.expect(base != after_same_count_membership_change);
}

test "data runtime overlays live raft membership onto cached storage status" {
    const Source = struct {
        membership: GroupMembership,

        fn iface(self: *@This()) GroupMembershipSource {
            return .{ .ptr = self, .vtable = &.{ .membership = read } };
        }

        fn read(ptr: *anyopaque, _: u64) GroupMembership {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.membership;
        }
    };

    const expected = antfly.metadata.table_manager.voterSetFingerprint(&.{ 101, 102, 104 }, null);
    var source = Source{ .membership = .{
        .local_voter = true,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = expected,
    } };
    var cached: antfly.metadata.table_manager.GroupStatusReport = .{
        .group_id = 77,
        .doc_count = 12,
        .voter_count = 2,
    };

    overlayLiveRaftGroupStatus(&cached, null, source.iface());

    try std.testing.expectEqual(@as(u64, 12), cached.doc_count);
    try std.testing.expect(cached.local_voter);
    try std.testing.expectEqual(@as(u16, 3), cached.voter_count);
    try std.testing.expect(cached.voter_set_known);
    try std.testing.expectEqualSlices(u8, &expected, &cached.voter_set_fingerprint);
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
    const db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 77);
    defer alloc.free(db_path);

    var db = try antfly.db.DB.open(alloc, db_path, .{
        .identity_namespace = .{ .table_id = 7, .shard_id = 77, .range_id = 77 },
    });
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
    server.store_status_dirty.store(false, .release);
    server.store_status_ticks.store(0, .release);
    hook.on_change(hook.ptr, "docs", .data);
    try std.testing.expect(server.store_status_dirty.load(.acquire));
    try std.testing.expectEqual(store_status_report_interval_ticks, server.store_status_ticks.load(.acquire));
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

test "data runtime raft status changes force immediate store status publication" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/data-runtime-raft-status-publication", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root_dir);

    var server: DataServer = .{
        .alloc = std.testing.allocator,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(std.testing.allocator),
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

    server.store_status_dirty.store(false, .release);
    server.store_status_ticks.store(0, .release);
    server.observeDataRaftStatusFingerprint(11);
    try std.testing.expect(server.store_status_dirty.load(.acquire));
    try std.testing.expectEqual(store_status_report_interval_ticks, server.store_status_ticks.load(.acquire));

    server.store_status_dirty.store(false, .release);
    server.store_status_ticks.store(0, .release);
    server.observeDataRaftStatusFingerprint(11);
    try std.testing.expect(!server.store_status_dirty.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.store_status_ticks.load(.acquire));

    server.observeDataRaftStatusFingerprint(12);
    try std.testing.expect(server.store_status_dirty.load(.acquire));
    try std.testing.expectEqual(store_status_report_interval_ticks, server.store_status_ticks.load(.acquire));
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

test "data runtime split apply store seeding reuses cached source writer" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-split-seed-cached-writer", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const source_db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 180);
    defer alloc.free(source_db_path);

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
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var write_cache = antfly.public_api.table_writes.ProvisionedTableWriteCache.init(alloc);
    defer write_cache.deinit();
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
    server.write_source.write_cache = &write_cache;
    defer server.deinit();

    var held = (try server.write_source.leaseCachedGroupWriter(alloc, 180, "docs")) orelse return error.TestUnexpectedResult;
    defer held.deinit(alloc);

    const namespace = (try server.identityNamespaceForSplitDestination(180, 181)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(namespace.eql(source_namespace));

    const transition_lease = (try server.leaseTransitionDbForTableGroup(180, 181, namespace)) orelse return error.TestUnexpectedResult;
    defer transition_lease.release();
    try std.testing.expect(transition_lease.db.core.identity_namespace.eql(source_namespace));

    try server.ensureSplitSourceApplyStoreSeeded(source_db_path, 180);

    var source_store = try antfly.data.RaftApplyStore.init(alloc, .{ .root_dir = source_db_path });
    defer source_store.deinit();
    const latest = (try source_store.latestBatch(180)) orelse return error.TestUnexpectedResult;
    try std.testing.expect(latest.entry_count > 0);
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
    const db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 77);
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
    server.store_status_dirty.store(false, .release);

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
    server.joinLocalGroupStatusRefreshThread();
    try std.testing.expect(server.store_status_dirty.load(.acquire));
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

test "data runtime store status cold miss schedules a nonblocking refresh" {
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
    server.joinLocalGroupStatusRefreshThread();
    try std.testing.expect(server.store_status_dirty.load(.acquire));
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
    server.joinLocalGroupStatusRefreshThread();
    try std.testing.expect(server.store_status_dirty.load(.acquire));
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

    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .identity_namespace = .{ .table_id = 7, .shard_id = 77, .range_id = 77 },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
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

test "data runtime provisioned cache warmup populates runtime status without pinning db caches" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-provisioned-warmup", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try antfly.metadata.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 77);
    defer alloc.free(db_path);

    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .identity_namespace = .{ .table_id = 7, .shard_id = 77, .range_id = 77 },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .sync_level = .write,
        });
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
    try std.testing.expectEqual(@as(usize, 0), server.provisioned_storage.read_cache.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_warmup_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_warmup_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_warmup_failed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_warmup_last_group_count.load(.monotonic));
    try std.testing.expect(server.provisioned_warmup_last_duration_ns.load(.monotonic) > 0);
    const warmed_read_cache = server.provisioned_storage.read_cache.cacheStats();
    try std.testing.expectEqual(@as(u64, 0), warmed_read_cache.hit_count);
    try std.testing.expectEqual(@as(u64, 0), warmed_read_cache.miss_count);
    const warmed_write_cache = server.provisioned_storage.write_cache.cacheStats();
    try std.testing.expectEqual(@as(u64, 0), warmed_write_cache.hit_count);
    try std.testing.expectEqual(@as(u64, 0), warmed_write_cache.miss_count);

    var snapshot_statuses = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer snapshot_statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), snapshot_statuses.items.len);
    try std.testing.expectEqual(@as(u64, 77), snapshot_statuses.items[0].group_id);
    try std.testing.expectEqual(runtime_status.RuntimeStatusSource.synthetic_config, snapshot_statuses.items[0].metadata.source);
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.stale, snapshot_statuses.items[0].metadata.freshness);
    try std.testing.expectEqual(@as(u64, 0), snapshot_statuses.items[0].stats.doc_count);

    var warmed_lookup = (try server.read_source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index)).?;
    defer warmed_lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, warmed_lookup.json, "\"alpha\"") != null);
    const post_lookup_read_cache = server.provisioned_storage.read_cache.cacheStats();
    try std.testing.expectEqual(@as(u64, 0), post_lookup_read_cache.hit_count);
    try std.testing.expectEqual(@as(u64, 0), post_lookup_read_cache.miss_count);

    _ = try server.write_source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        .timestamp_ns = 2,
        .sync_level = .write,
    });
    const post_batch_write_cache = server.provisioned_storage.write_cache.cacheStats();
    try std.testing.expectEqual(@as(u64, 0), post_batch_write_cache.hit_count);
    try std.testing.expect(post_batch_write_cache.miss_count >= 1);

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

test "data runtime status refresh budget preserves fresh cached group status for visible generation" {
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
    try std.testing.expectEqual(runtime_status.RuntimeStatusFreshness.fresh, docs_cached.items[0].metadata.freshness);
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

test "data runtime startup catch-up parks scheduler when only quarantined debt remains" {
    const retry_at_ms = 45_000;
    try std.testing.expectEqual(@as(u64, 0), DataServer.startupCatchUpNotBeforeMs(.{}, true));
    try std.testing.expectEqual(@as(u64, 0), DataServer.startupCatchUpNotBeforeMs(.{
        .debt_remaining = true,
        .earliest_retry_at_ms = retry_at_ms,
    }, false));
    try std.testing.expectEqual(retry_at_ms, DataServer.startupCatchUpNotBeforeMs(.{
        .debt_remaining = true,
        .earliest_retry_at_ms = retry_at_ms,
    }, true));
    try std.testing.expectEqual(@as(u64, 0), DataServer.startupCatchUpNotBeforeMs(.{
        .debt_remaining = true,
        .unparked_debt_remaining = true,
        .earliest_retry_at_ms = retry_at_ms,
    }, true));

    const current = DataServer.startupCatchUpCompletion(.{
        .debt_remaining = true,
        .earliest_retry_at_ms = retry_at_ms,
    }, true, 7, 7);
    try std.testing.expect(current.dirty);
    try std.testing.expectEqual(retry_at_ms, current.not_before_ms);

    const stale = DataServer.startupCatchUpCompletion(.{}, true, 7, 8);
    try std.testing.expect(stale.dirty);
    try std.testing.expectEqual(@as(u64, 0), stale.not_before_ms);
}

test "data runtime provisioned startup catch-up clears replay debt for local groups" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-startup-catch-up", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);
    const public_index_json = "{\"type\":\"embeddings\",\"external\":true,\"dimension\":2}";
    var parsed_public_index = try std.json.parseFromSlice(std.json.Value, alloc, public_index_json, .{});
    defer parsed_public_index.deinit();
    const stored_index_json = try antfly.inference.managed_embedder.translateEmbeddingsIndexConfigJson(
        alloc,
        "dv_v1",
        parsed_public_index.value,
    );
    defer alloc.free(stored_index_json);

    var appended_sequence: u64 = 0;
    var coverage_incarnation: u64 = 0;
    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .identity_namespace = .{ .table_id = 7, .shard_id = 77, .range_id = 77 },
        });
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = stored_index_json,
        });
        const configs = try db.listIndexes(alloc);
        defer antfly.db.types.freeIndexConfigs(alloc, configs);
        try std.testing.expectEqual(@as(usize, 1), configs.len);
        coverage_incarnation = configs[0].coverage_generation;
        try std.testing.expect(coverage_incarnation != 0);

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

    const indexes_json = try std.fmt.allocPrint(
        alloc,
        "{{\"dv_v1\":{{\"type\":\"embeddings\",\"external\":true,\"dimension\":2,\"_coverage_incarnation\":{d}}}}}",
        .{coverage_incarnation},
    );
    defer alloc.free(indexes_json);

    const FakeStatus = struct {
        tables: [1]antfly.metadata.table_manager.TableRecord,

        fn iface(self: *@This()) antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = self,
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

        fn adminSnapshot(ptr: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
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
        status_source: *FakeStatus,

        fn iface(self: *@This()) antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try FakeStatus.adminSnapshot(self.status_source);
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

    var fake_status = FakeStatus{ .tables = .{.{
        .table_id = 7,
        .name = "docs",
        .placement_role = "data",
        .indexes_json = indexes_json,
    }} };
    var fake_catalog = FakeCatalog{ .status_source = &fake_status };

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
            fake_catalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            replica_root_dir,
            fake_catalog.iface(),
        ),
        .status_source = fake_status.iface(),
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);

    server.provisioned_startup_catch_up_dirty.store(false, .monotonic);
    try server.requestRuntimeStatusRefresh();
    server.provisioned_startup_catch_up_dirty.store(true, .monotonic);

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

test "data runtime startup catch-up clears dirty bit for terminal degraded index load" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-terminal-startup-load", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root_dir});
    defer alloc.free(db_path);
    const indexes_json = "{\"dv_v1\":{\"type\":\"embeddings\",\"external\":true,\"dimension\":2,\"_coverage_incarnation\":42}}";
    const identity_namespace = doc_identity.Namespace{ .table_id = 7, .shard_id = 77, .range_id = 77 };

    {
        var db = try antfly.db.DB.open(alloc, db_path, .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
            .identity_namespace = identity_namespace,
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2}",
        });
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
    server.runtime_status_dirty.store(false, .release);
    server.store_status_dirty.store(false, .release);
    server.provisioned_startup_catch_up_dirty.store(true, .release);

    index_manager_mod.test_inject_index_open_error = error.FileNotFound;
    defer index_manager_mod.test_inject_index_open_error = null;

    _ = server.runProvisionedStartupCatchUp();

    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_started.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_completed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_failed.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_groups_cleared.load(.monotonic));
    try std.testing.expect(!server.provisioned_startup_catch_up_dirty.load(.acquire));
    try std.testing.expect(server.runtime_status_dirty.load(.acquire));
    try std.testing.expect(server.store_status_dirty.load(.acquire));

    var statuses = (try server.provisioned_storage.runtime_status_cache.snapshot(alloc, "docs")).?;
    defer statuses.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), statuses.items.len);
    try std.testing.expect(statuses.items[0].stats.repair_degraded);
    try std.testing.expectEqual(@as(usize, 1), statuses.items[0].stats.indexes.len);
    try std.testing.expect(statuses.items[0].stats.indexes[0].repair_degraded);
    try std.testing.expect(statuses.items[0].stats.indexes[0].load_error != null);
    try std.testing.expectEqualStrings("startup catch-up open failed: FileNotFound", statuses.items[0].stats.indexes[0].load_error.?);
}

test "data runtime startup catch-up clears no-debt busy writer groups" {
    const alloc = std.testing.allocator;

    const cases = [_]struct {
        suffix: []const u8,
        runtime_status_dirty: bool = false,
        runtime_status_refresh_active: bool = false,
        expect_dirty: bool,
    }{
        .{ .suffix = "quiescent", .expect_dirty = false },
        .{ .suffix = "runtime-dirty", .runtime_status_dirty = true, .expect_dirty = true },
        .{ .suffix = "refresh-active", .runtime_status_refresh_active = true, .expect_dirty = true },
    };

    for (cases) |tc| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();

        const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-startup-catch-up-busy-no-debt-{s}", .{ tmp.sub_path, tc.suffix });
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
                        .indexes_json = "{\"indexes\":[]}",
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

        try write_cache.beginBulkIngestLocked("docs");
        {
            lockAtomic(server.write_source.localDbMutex());
            defer server.write_source.localDbMutex().unlock();

            var cached = try write_cache.getOrOpenLocked(db_path, FakeCatalog.iface(), 77, 0, "docs");
            defer cached.deinit(alloc);
            const stats = try cached.db.stats(alloc);
            defer antfly.db.types.freeDBStats(alloc, stats);
            try server.provisioned_storage.runtime_status_cache.upsertGroupStatus("docs", .{
                .group_id = 77,
                .stats = stats,
            });
        }

        server.runtime_status_dirty.store(tc.runtime_status_dirty, .release);
        server.runtime_status_refresh_active.store(tc.runtime_status_refresh_active, .release);
        server.provisioned_startup_catch_up_dirty.store(true, .monotonic);
        _ = server.runProvisionedStartupCatchUp();

        try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_started.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_completed.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_group_count.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
        try std.testing.expectEqual(@as(u64, 1), server.provisioned_startup_catch_up_last_busy_groups.load(.monotonic));
        try std.testing.expectEqual(tc.expect_dirty, server.provisioned_startup_catch_up_dirty.load(.monotonic));
    }
}

test "data runtime startup catch-up retries unresolved leadership and observes leader open replay" {
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
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_groups_with_debt.load(.monotonic));
    try std.testing.expectEqual(@as(u64, 0), server.provisioned_startup_catch_up_last_groups_cleared.load(.monotonic));
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

test "data runtime structural changes preserve writer-published runtime status and schedule catch-up" {
    const alloc = std.testing.allocator;
    var server: DataServer = .{
        .alloc = alloc,
        .provisioned_storage = antfly.public_api.ProvisionedGroupStorage.init(alloc),
        .read_source = undefined,
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init(
            "unused",
            antfly.public_api.table_catalog.emptyCatalogSource(),
        ),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.write_source.deinit();
    defer server.provisioned_storage.deinit();

    try server.provisioned_storage.runtime_status_cache.upsertGroupStatus("docs", .{
        .group_id = 77,
        .stats = .{ .doc_count = 2, .indexes = &.{} },
    });

    server.runtime_status_dirty.store(false, .release);
    server.provisioned_startup_catch_up_dirty.store(false, .release);
    server.markRuntimeStatusDirty("docs", .structural);

    try std.testing.expect(server.runtime_status_dirty.load(.acquire));
    try std.testing.expect(server.provisioned_startup_catch_up_dirty.load(.acquire));
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
    const metadata_api_urls = [_][]const u8{"http://metadata.test"};
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

    server.store_registration_confirmed = true;
    server.store_status_dirty.store(false, .release);
    server.last_store_status_report_at_ms = 1;
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

test "data runtime runRound backs off retryable provision metadata failures" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-provision-metadata-backoff", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const remote_metadata = try alloc.create(RemoteMetadataSource);
    const metadata_api_urls = [_][]const u8{"http://metadata.test"};
    remote_metadata.* = try RemoteMetadataSource.init(alloc, &metadata_api_urls);
    remote_metadata.test_faults.fetch_head_error = error.NotLeader;

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

    server.store_registration_confirmed = true;
    server.store_status_dirty.store(false, .release);
    server.last_store_status_report_at_ms = 1;
    server.runtime_status_dirty.store(false, .release);
    server.provisioned_startup_catch_up_dirty.store(false, .release);
    server.provisioned_root_refresh_dirty.store(false, .release);
    server.provision_ticks = 3;

    try server.runRound();

    try std.testing.expectEqual(@as(u32, 1), server.metadataBootstrapRetryAttemptsForTest());
    try std.testing.expect(server.nextMetadataBootstrapRetryAtMsForTest() != 0);
    try std.testing.expect(server.provisioned_root_refresh_dirty.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.provision_ticks);
    try std.testing.expect(server.last_provision_head_check_at_ms != 0);

    const next_retry_at_ms = server.nextMetadataBootstrapRetryAtMsForTest();
    const last_head_check_at_ms = server.last_provision_head_check_at_ms;
    server.provision_ticks = 3;

    try server.runRound();

    try std.testing.expectEqual(@as(u32, 1), server.metadataBootstrapRetryAttemptsForTest());
    try std.testing.expectEqual(next_retry_at_ms, server.nextMetadataBootstrapRetryAtMsForTest());
    try std.testing.expectEqual(last_head_check_at_ms, server.last_provision_head_check_at_ms);
    try std.testing.expectEqual(@as(usize, 3), server.provision_ticks);
}

test "data runtime provisioned root refresh worker backs off retryable metadata failures" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-provision-worker-backoff", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);

    const remote_metadata = try alloc.create(RemoteMetadataSource);
    const metadata_api_urls = [_][]const u8{"http://metadata.test"};
    remote_metadata.* = try RemoteMetadataSource.init(alloc, &metadata_api_urls);
    remote_metadata.test_faults.fetch_head_error = error.NotLeader;

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

    server.provisioned_root_refresh_dirty.store(true, .release);

    server.runProvisionedRootRefresh();

    try std.testing.expectEqual(@as(u64, 1), server.provisioned_root_refresh_failed.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), server.metadataBootstrapRetryAttemptsForTest());
    try std.testing.expect(server.nextMetadataBootstrapRetryAtMsForTest() != 0);
    try std.testing.expect(server.provisioned_root_refresh_dirty.load(.acquire));

    const next_retry_at_ms = server.nextMetadataBootstrapRetryAtMsForTest();
    server.store_registration_confirmed = true;
    server.store_status_dirty.store(false, .release);
    server.runtime_status_dirty.store(false, .release);
    server.provisioned_startup_catch_up_dirty.store(false, .release);
    server.provision_ticks = 3;

    try server.runRound();

    try std.testing.expectEqual(@as(u32, 1), server.metadataBootstrapRetryAttemptsForTest());
    try std.testing.expectEqual(next_retry_at_ms, server.nextMetadataBootstrapRetryAtMsForTest());
    try std.testing.expectEqual(@as(usize, 3), server.provision_ticks);
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
    var writer_buf: [262144]u8 = undefined;
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
        .anonymous_bytes = 12,
        .private_dirty_bytes = 17,
        .footprint_bytes = 13,
        .wired_bytes = 19,
        .pageins = 23,
        .malloc_available = true,
        .malloc_allocated_bytes = 29,
        .malloc_zone_bytes = 31,
    });
    const process_memory_output = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, process_memory_output, "antfly_process_memory_available 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, process_memory_output, "antfly_process_anonymous_bytes 12") != null);
    try std.testing.expect(std.mem.indexOf(u8, process_memory_output, "antfly_process_private_dirty_bytes 17") != null);
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
        .write_stall_l0_run_debt = 5,
        .soft_limit_l0_bytes = 65536,
        .hard_limit_l0_bytes = 131072,
        .write_stall_l0_byte_debt = 16384,
        .lower_level_runs = 5,
        .lower_level_bytes = 4096,
        .max_level = 2,
        .level_overflow_runs = 1,
        .level_overflow_bytes = 256,
        .obsolete_paths = 1,
        .obsolete_paths_pinned_by_readers = 2,
        .obsolete_paths_pinned_by_versions = 3,
        .obsolete_paths_waiting_for_retry = 4,
        .obsolete_paths_reclaimable = 5,
        .obsolete_delete_failures = 6,
        .obsolete_delete_retries = 7,
        .active_readers = 6,
        .active_readers_by_kind = blk: {
            var counts: [lsm_backend_mod.reader_pin_kind_count]u64 = [_]u64{0} ** lsm_backend_mod.reader_pin_kind_count;
            counts[@intFromEnum(lsm_backend_mod.ReaderPinKind.compaction)] = 2;
            break :blk counts;
        },
        .obsolete_paths_pinned_by_reader_kind = blk: {
            var counts: [lsm_backend_mod.reader_pin_kind_count]u64 = [_]u64{0} ** lsm_backend_mod.reader_pin_kind_count;
            counts[@intFromEnum(lsm_backend_mod.ReaderPinKind.compaction)] = 3;
            break :blk counts;
        },
        .manifest_dirty = true,
        .obsolete_manifest_dirty = true,
        .compaction_scheduler_active_oldest_age_ns = 99,
        .compaction_scheduler_grants = 3,
        .compaction_scheduler_denied_capacity = 1,
        .compaction_scheduler_oversized_skips = 10,
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
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_write_stall_l0_run_debt 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_write_stall_l0_byte_debt 16384") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_level_overflow_runs 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_paths 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_paths_pinned_by_readers 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_paths_pinned_by_versions 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_paths_waiting_for_retry 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_paths_reclaimable 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_delete_failures_total 6") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_delete_retries_total 7") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_active_readers 6") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_active_readers_by_kind{kind=\"compaction\"} 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_obsolete_paths_pinned_by_reader_kind{kind=\"compaction\"} 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_manifest_dirty 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_active_oldest_age_ns 99") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_grants_total 3") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_denied_capacity_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, maintenance_output, "antfly_lsm_compaction_scheduler_oversized_skips_total 10") != null);
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
        .write_pressure_l0_run_debt = 25,
        .write_pressure_l0_byte_debt = 26,
        .write_pressure_overload_l0_run_debt = 27,
        .write_pressure_overload_l0_byte_debt = 28,
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
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_l0_run_debt_total 25") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_l0_byte_debt_total 26") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_overloads_total 23") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_overload_l0_run_debt_total 27") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_output, "antfly_lsm_write_pressure_overload_l0_byte_debt_total 28") != null);
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
        .api_server_cfg = .{},
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();
    server.provisioned_storage.attachSources(&server.read_source, &server.write_source);
    try server.initApiServer();

    var status_resp = try server.http_server.?.handle(.{ .method = .GET, .uri = antfly.public_api.http_routes.Routes.status });
    defer status_resp.deinit(server.http_server.?.alloc);

    const items = try std.testing.allocator.alloc(runtime_status.LocalTableRuntimeStatus, 1);
    items[0] = .{
        .group_id = 77,
        .metadata = .{
            .source = .live_writer_publish,
            .freshness = .fresh,
        },
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
                    .lsm_open_recovered_temp_cleanup_ns = 333,
                    .lsm_open_manifest_ns = 111,
                    .lsm_open_wal_replay_ns = 222,
                    .lsm_open_loaded_runs = 5,
                    .lsm_open_mutable_entries_after_replay = 7,
                    .lsm_open_recovered_temp_files_deleted = 4,
                    .lsm_open_recovered_temp_bytes_deleted = 2048,
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
    defer std.testing.allocator.free(snapshots);
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
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    try health.metricsWriter().writeMetrics(&writer.writer);
    const output = writer.writer.buffered();

    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_tables 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_groups 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_runtime_status_indexes 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_api_requests_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_data_api_first_request_elapsed_ms") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_hits_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_coalesced_waiters_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_uncached_computations_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_producer_compute_ns_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_inflight_rejections_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_waiter_timeouts_total 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_inflight 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_inflight_limit 16") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_query_embedding_cache_live_bytes 0") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_cache_budget_limit_bytes 67108864") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_inference_cache_budget_rejected_reservations_total 0") != null);
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
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_metrics_lsm_snapshot_age_seconds") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_metrics_lsm_snapshot_refreshes_total 1") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_metrics_lsm_snapshot_last_refresh_duration_ns") != null);
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
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_recovered_temp_cleanup_ns 333") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_manifest_ns 111") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_wal_replay_ns 222") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_loaded_runs 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_mutable_entries_after_replay 7") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_recovered_temp_files_deleted 4") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_lsm_open_recovered_temp_bytes_deleted 2048") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_records 8") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_entries 9") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_bytes 10") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_ns 222") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_wal_replay_truncated_tail_bytes 66") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "antfly_async_index_startup_phase{phase=\"opening_db\"} 1") != null);
}

const TestHASeedSnapshotProvider = struct {
    db_path: []const u8,
    calls: usize = 0,
    unsupported: bool = false,

    fn iface(self: *@This()) HASeedSnapshotProvider {
        return .{ .ptr = self, .prepare_fn = prepare };
    }

    fn prepare(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        request: HASeedSnapshotRequest,
    ) !HASeedPreparedSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        if (self.unsupported) return error.HASeedSnapshotUnsupportedBackend;

        var io_impl = std.Io.Threaded.init(alloc, .{});
        defer io_impl.deinit();
        const io = io_impl.io();
        const provider_root = try std.fmt.allocPrint(alloc, "{s}.runtime-snapshots", .{request.capture_root});
        defer alloc.free(provider_root);
        const prepared_root = try std.fs.path.join(alloc, &.{ provider_root, request.generation });
        errdefer alloc.free(prepared_root);
        std.Io.Dir.cwd().deleteTree(io, prepared_root) catch {};
        const replica_snapshot_root = try std.fs.path.join(alloc, &.{ prepared_root, "replicas/group-1" });
        defer alloc.free(replica_snapshot_root);

        var snapshot_db = try antfly.db.DB.open(alloc, self.db_path, .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .start_index_workers = false,
        });
        defer snapshot_db.close();
        const snapshot_token = try std.fmt.allocPrint(alloc, "{s}-g1", .{request.generation});
        defer alloc.free(snapshot_token);
        _ = try snapshot_db.snapshot(snapshot_token);
        const source_snapshot_root = try std.fmt.allocPrint(alloc, "{s}.snapshots/{s}", .{ self.db_path, snapshot_token });
        defer alloc.free(source_snapshot_root);
        try antfly.public_api.backups.copyDirectoryRecursive(alloc, source_snapshot_root, replica_snapshot_root);

        const store_path = try std.fs.path.join(alloc, &.{ replica_snapshot_root, "store.bin" });
        defer alloc.free(store_path);
        const store_bytes = try std.Io.Dir.cwd().readFileAlloc(io, store_path, alloc, .limited(16 * 1024 * 1024));
        defer alloc.free(store_bytes);
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(store_bytes, &digest, .{});
        var digest_hex: [std.crypto.hash.sha2.Sha256.digest_length * 2]u8 = undefined;
        for (digest, 0..) |byte, index| {
            digest_hex[index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
            digest_hex[index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
        }

        const topology_json = try std.json.Stringify.valueAlloc(alloc, HASeedSnapshotTopology{
            .generation = request.generation,
            .catalog = .{
                .epoch = 7,
                .tables = &.{.{
                    .table_id = 20,
                    .name = "docs",
                    .placement_role = "data",
                }},
                .ranges = &.{.{
                    .group_id = 1,
                    .range_id = 1,
                    .table_id = 20,
                    .start_key = "",
                    .end_key = null,
                    .doc_identity_shard_id = 10,
                    .doc_identity_range_id = 1,
                }},
            },
            .replicas = &.{.{
                .group_id = 1,
                .table_id = 20,
                .table_name = "docs",
                .snapshot_path = "replicas/group-1",
                .logical_sha256 = digest_hex[0..],
                .identity_table_id = 20,
                .identity_shard_id = 10,
                .identity_range_id = 1,
            }},
        }, .{});
        defer alloc.free(topology_json);
        const topology_path = try std.fs.path.join(alloc, &.{ prepared_root, "TOPOLOGY.json" });
        defer alloc.free(topology_path);
        var topology_file = try std.Io.Dir.cwd().createFile(io, topology_path, .{ .truncate = true });
        try topology_file.writeStreamingAll(io, topology_json);
        try topology_file.sync(io);
        topology_file.close(io);
        try fs_paths.syncDirPortable(io, prepared_root);
        return .{ .root = prepared_root };
    }
};

test "data server wires configured HA executors into API server" {
    const alloc = std.testing.allocator;
    const FakeStatus = struct {
        include_extension: bool = false,

        fn iface(self: *@This()) antfly.public_api.http_server.StatusSource {
            return .{
                .ptr = self,
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

        fn adminSnapshot(ptr: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metadata_epoch = 7, .metrics = .{} },
                .tables = @constCast((&[_]antfly.metadata.table_manager.TableRecord{.{
                    .table_id = 20,
                    .name = "docs",
                    .placement_role = "data",
                }})[0..]),
                .ranges = @constCast((&[_]antfly.metadata.table_manager.RangeRecord{.{
                    .group_id = 1,
                    .range_id = 1,
                    .table_id = 20,
                    .start_key = "",
                    .end_key = null,
                    .doc_identity_shard_id = 10,
                    .doc_identity_range_id = 1,
                }})[0..]),
                .stores = @constCast((&[_]antfly.metadata.table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]antfly.raft.reconciler.PlacementIntent{})[0..]),
                .extension_packages = if (self.include_extension)
                    @constCast((&[_]antfly.extensions.PackageManifest{.{
                        .name = "memoryaf",
                        .version = "1.0.0",
                        .digest = "sha256:fixture",
                        .install = .{ .scopes_supported = &.{.cluster} },
                    }})[0..])
                else
                    @constCast((&[_]antfly.extensions.PackageManifest{})[0..]),
                .split_transitions = @constCast((&[_]antfly.metadata.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]antfly.metadata.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    const FakeCatalog = struct {
        status: *FakeStatus,

        fn iface(self: *@This()) antfly.public_api.table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !antfly.metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try FakeStatus.adminSnapshot(self.status);
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *antfly.metadata_api.AdminSnapshot) void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, alloc);
    defer alloc.free(cwd);
    const fixture_root_rel = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/data-runtime-ha-executors", .{tmp.sub_path});
    defer alloc.free(fixture_root_rel);
    const capture_fixture_root = try std.fs.path.resolve(alloc, &.{ cwd, fixture_root_rel });
    defer alloc.free(capture_fixture_root);

    const primary_log_raw = try std.fs.path.join(alloc, &.{ capture_fixture_root, "primary.log" });
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fs.path.join(alloc, &.{ capture_fixture_root, "primary-slots" });
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);
    const restore_jobs_path = try std.fs.path.join(alloc, &.{ capture_fixture_root, "restore-jobs" });
    defer alloc.free(restore_jobs_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const replica_root = try std.fs.path.join(alloc, &.{ capture_fixture_root, "replicas" });
    defer alloc.free(replica_root);
    const replica_db_path = try std.fs.path.join(alloc, &.{ replica_root, "group-1/table-db" });
    defer alloc.free(replica_db_path);
    const raft_wal_sentinel = try std.fs.path.join(alloc, &.{ replica_root, "group-1/raft/wal-000001" });
    defer alloc.free(raft_wal_sentinel);
    const replica_catalog_path = try std.fs.path.join(alloc, &.{ capture_fixture_root, "catalog.txt" });
    defer alloc.free(replica_catalog_path);
    const seed_capture_root = try std.fs.path.join(alloc, &.{ capture_fixture_root, "seed-captures" });
    defer alloc.free(seed_capture_root);
    const extension_store_root = try std.fs.path.join(alloc, &.{ capture_fixture_root, "extensions" });
    defer alloc.free(extension_store_root);
    const extension_manifest_path = try std.fs.path.join(alloc, &.{ extension_store_root, "memoryaf/extension.json" });
    defer alloc.free(extension_manifest_path);
    const extension_runtime_path = try std.fs.path.join(alloc, &.{ extension_store_root, "memoryaf/runtime.wasm" });
    defer alloc.free(extension_runtime_path);
    const capture_binding = antfly.ha.seed_artifact.LifecycleBinding{
        .topology_id = "topology-a",
        .topology_generation = 7,
        .node_id = "standby-a",
        .target_pvc_name = "standby-a-data",
        .target_pvc_uid = "pvc-uid-7",
    };
    std.Io.Dir.cwd().deleteTree(io_impl.io(), capture_fixture_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), capture_fixture_root) catch {};
    {
        var db = try antfly.db.DB.open(alloc, replica_db_path, .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .identity_namespace = .{ .table_id = 20, .shard_id = 10, .range_id = 1 },
            .start_index_workers = false,
        });
        defer db.close();
        const schema_json =
            \\{"version":1,"default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","additionalProperties":true,"properties":{"title":{"type":"string","x-antfly-field":{"type":"text"}}}}}}}
        ;
        try db.setSchemaJson(alloc, schema_json);
        try db.addIndex(.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{\"field\":\"title\"}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:seed", .value = "{\"title\":\"seed state\"}" }},
            .sync_level = .full_index,
        });
        const txn_id = try db.beginTransaction(10_000);
        try db.writeIntents(txn_id, &.{.{ .key = "doc:txn", .value = "{\"title\":\"txn state\"}" }}, &.{});
        try db.commitTransaction(txn_id, 10_001);
        try db.core.store.appendReplayOpaque(alloc, 1, "derived-change-journal-state");
    }
    try fs_paths.createDirPathPortable(io_impl.io(), std.fs.path.dirname(raft_wal_sentinel).?);
    var raft_fixture = try std.Io.Dir.cwd().createFile(io_impl.io(), raft_wal_sentinel, .{ .truncate = true });
    try raft_fixture.writeStreamingAll(io_impl.io(), "node-local-raft-wal-must-not-ship");
    try raft_fixture.sync(io_impl.io());
    raft_fixture.close(io_impl.io());
    {
        var replica_catalog = try antfly.raft.FileReplicaCatalog.init(alloc, replica_catalog_path);
        defer replica_catalog.deinit();
        try replica_catalog.catalog().upsertReplica(.{
            .group_id = 1,
            .replica_id = 101,
            .local_node_id = 11,
            .bootstrap_mode = .persisted,
            .metadata_version = 1,
        });
    }
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), restore_jobs_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), restore_jobs_path) catch {};

    try fs_paths.createDirPathPortable(io_impl.io(), std.fs.path.dirname(extension_manifest_path).?);
    var extension_manifest_file = try std.Io.Dir.cwd().createFile(io_impl.io(), extension_manifest_path, .{ .truncate = true });
    try extension_manifest_file.writeStreamingAll(io_impl.io(),
        \\{"manifest_api_version":"extensions/v1","name":"memoryaf","version":"1.0.0","kind":"extension","digest":"sha256:fixture","install":{"scopes_supported":["cluster"]}}
    );
    try extension_manifest_file.sync(io_impl.io());
    extension_manifest_file.close(io_impl.io());
    var extension_runtime_file = try std.Io.Dir.cwd().createFile(io_impl.io(), extension_runtime_path, .{ .truncate = true });
    try extension_runtime_file.writeStreamingAll(io_impl.io(), "portable-extension-runtime");
    try extension_runtime_file.sync(io_impl.io());
    extension_runtime_file.close(io_impl.io());
    try fs_paths.syncDirPortable(io_impl.io(), std.fs.path.dirname(extension_manifest_path).?);

    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });
    // Keep the metrics fixture independent of backup-log growth: this slot is
    // the one unhealthy streaming standby whose reseed signal we exercise.
    try primary.markSlotReseedRequired("standby-a");

    var fake_status = FakeStatus{};
    var fake_catalog = FakeCatalog{ .status = &fake_status };
    var seed_snapshot_provider = TestHASeedSnapshotProvider{ .db_path = replica_db_path };
    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .replica_catalog_path = replica_catalog_path,
        .api_server_cfg = .{
            .restore_job_store_path = restore_jobs_path,
            .extension_package_store_dir = extension_store_root,
        },
        .ha = .{
            .admin_context = .{
                .primary = &primary,
                .primary_node_id = "primary-a",
            },
            .admin_bearer_token = "runtime-secret-token",
            .seed_capture_root = seed_capture_root,
            .pod_uid = "pod-primary-a",
            .seed_snapshot_provider = seed_snapshot_provider.iface(),
            .primary_retention_policy = .{},
        },
    }, fake_catalog.iface(), fake_status.iface());
    defer server.deinit();
    try server.initApiServer();

    try std.testing.expect(server.ha_admin_server != null);
    try std.testing.expect(server.ha_internal_server != null);
    try std.testing.expect(server.haOwnerJobCanRun(.compaction_publish));

    var admin_unauthorized = try server.http_server.?.handle(.{
        .method = .GET,
        .uri = antfly.admin.routes.ha_primary_status,
    });
    defer admin_unauthorized.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 401), admin_unauthorized.status);

    var admin_resp = try server.http_server.?.handle(.{
        .method = .GET,
        .uri = antfly.admin.routes.ha_primary_status,
        .authorization = "Bearer runtime-secret-token",
    });
    defer admin_resp.deinit(server.http_server.?.alloc);
    try std.testing.expect(admin_resp.owner_allocator != null);
    try std.testing.expectEqual(@as(u16, 200), admin_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, admin_resp.body, "\"current_lsn\"") != null);

    const capture_body = "{\"slot_name\":\"standby-capture\",\"generation\":\"seed-standby-capture-3\",\"topology_id\":\"topology-a\",\"topology_generation\":7,\"node_id\":\"standby-a\",\"target_pvc_name\":\"standby-a-data\",\"target_pvc_uid\":\"pvc-uid-7\"}";
    var capture_resp = try server.http_server.?.handle(.{
        .method = .POST,
        .uri = antfly.admin.routes.ha_base_backups_capture,
        .authorization = "Bearer runtime-secret-token",
        .content_type = "application/json",
        .body = capture_body,
    });
    defer capture_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), capture_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, capture_resp.body, "\"action_kind\":\"seed_capture\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capture_resp.body, "\"state\":\"applied\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, capture_resp.body, "\"manifest_path\":") != null);
    const CaptureDigestResponse = struct { capture_receipt_sha256: []const u8 };
    var capture_response = try std.json.parseFromSlice(CaptureDigestResponse, alloc, capture_resp.body, .{ .ignore_unknown_fields = true });
    defer capture_response.deinit();
    const runtime_capture_receipt_path = try std.fs.path.join(alloc, &.{
        seed_capture_root,
        "generations/seed-standby-capture-3/COMPLETE.json",
    });
    defer alloc.free(runtime_capture_receipt_path);
    const runtime_capture_receipt = try std.Io.Dir.cwd().readFileAlloc(
        io_impl.io(),
        runtime_capture_receipt_path,
        alloc,
        .limited(1024 * 1024),
    );
    defer alloc.free(runtime_capture_receipt);
    var runtime_capture_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(runtime_capture_receipt, &runtime_capture_digest, .{});
    var runtime_capture_digest_hex: [std.crypto.hash.sha2.Sha256.digest_length * 2]u8 = undefined;
    for (runtime_capture_digest, 0..) |byte, index| {
        runtime_capture_digest_hex[index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        runtime_capture_digest_hex[index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    try std.testing.expectEqualStrings(&runtime_capture_digest_hex, capture_response.value.capture_receipt_sha256);
    // Contract: DataServer must package only provider-produced logical output,
    // never recursively copy the live replica tree (which contains Raft WAL).
    try std.testing.expectEqual(@as(usize, 1), seed_snapshot_provider.calls);
    const captured_raft_wal = try std.fs.path.join(alloc, &.{
        seed_capture_root,
        "generations/seed-standby-capture-3/content/replicas/group-1/raft/wal-000001",
    });
    defer alloc.free(captured_raft_wal);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io_impl.io(), captured_raft_wal, .{}));

    const captured_topology_path = try std.fs.path.join(alloc, &.{
        seed_capture_root,
        "generations/seed-standby-capture-3/content/TOPOLOGY.json",
    });
    defer alloc.free(captured_topology_path);
    const captured_topology_json = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), captured_topology_path, alloc, .limited(1024 * 1024));
    defer alloc.free(captured_topology_json);
    var captured_topology = try std.json.parseFromSlice(
        antfly.ha.seed_materialization.Topology,
        alloc,
        captured_topology_json,
        .{ .ignore_unknown_fields = false },
    );
    defer captured_topology.deinit();
    try std.testing.expectEqual(antfly.ha.seed_materialization.topology_format_version, captured_topology.value.format_version);
    try std.testing.expectEqualStrings("seed-standby-capture-3", captured_topology.value.generation);
    try std.testing.expectEqual(@as(u64, 7), captured_topology.value.catalog.epoch);
    try std.testing.expectEqual(@as(usize, 1), captured_topology.value.catalog.tables.len);
    try std.testing.expectEqual(@as(usize, 1), captured_topology.value.catalog.ranges.len);
    try std.testing.expectEqual(@as(usize, 1), captured_topology.value.replicas.len);
    const captured_replica = captured_topology.value.replicas[0];
    try std.testing.expectEqual(@as(u64, 1), captured_replica.group_id);
    try std.testing.expectEqual(@as(u64, 20), captured_replica.table_id);
    try std.testing.expectEqualStrings("docs", captured_replica.table_name);
    try std.testing.expectEqual(@as(u64, 20), captured_replica.identity_table_id);
    try std.testing.expectEqual(@as(u64, 10), captured_replica.identity_shard_id);
    try std.testing.expectEqual(@as(u64, 1), captured_replica.identity_range_id);

    const captured_snapshot_root = try std.fs.path.join(alloc, &.{
        seed_capture_root,
        "generations/seed-standby-capture-3/content",
        captured_replica.snapshot_path,
    });
    defer alloc.free(captured_snapshot_root);
    const captured_journal_path = try std.fs.path.join(alloc, &.{ captured_snapshot_root, "change-journal.bin" });
    defer alloc.free(captured_journal_path);
    try std.Io.Dir.accessAbsolute(io_impl.io(), captured_journal_path, .{});
    const restored_db_path = try std.fs.path.join(alloc, &.{ capture_fixture_root, "restored/group-1/table-db" });
    defer alloc.free(restored_db_path);
    {
        var transition = try antfly.db.generation_lifecycle.beginProcessExclusiveWithRuntime(restored_db_path, null);
        defer transition.deinit();
        var staged = try transition.beginStaging();
        defer staged.deinit();
        try antfly.db.DB.restoreSnapshotToStagedGeneration(&staged, alloc, captured_snapshot_root, staged.path(), .{
            .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
            .start_index_workers = false,
        });
        try std.testing.expectEqual(antfly.db.generation_lifecycle.PublicationOutcome.durable, try staged.publish());
    }
    var restored_db = try antfly.db.DB.open(alloc, restored_db_path, .{
        .primary_backend = .{ .lsm = .{ .flush_threshold = 1 } },
        .start_index_workers = false,
    });
    defer restored_db.close();
    const restored_schema = (try restored_db.getSchemaJson(alloc)) orelse return error.TestExpectedEqual;
    defer alloc.free(restored_schema);
    try std.testing.expect(std.mem.indexOf(u8, restored_schema, "title") != null);
    const restored_doc = (try restored_db.get(alloc, "doc:seed")) orelse return error.TestExpectedEqual;
    defer alloc.free(restored_doc);
    try std.testing.expectEqualStrings("{\"title\":\"seed state\"}", restored_doc);
    const restored_txn_doc = (try restored_db.get(alloc, "doc:txn")) orelse return error.TestExpectedEqual;
    defer alloc.free(restored_txn_doc);
    try std.testing.expectEqualStrings("{\"title\":\"txn state\"}", restored_txn_doc);
    var restored_search = try restored_db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "title", .text = "seed" } },
    });
    defer restored_search.deinit();
    try std.testing.expectEqual(@as(u32, 1), restored_search.total_hits);
    _ = try restored_db.snapshot("canonical-digest");
    const restored_store_path = try std.fmt.allocPrint(alloc, "{s}.snapshots/canonical-digest/store.bin", .{restored_db_path});
    defer alloc.free(restored_store_path);
    const restored_store_bytes = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), restored_store_path, alloc, .limited(16 * 1024 * 1024));
    defer alloc.free(restored_store_bytes);
    var restored_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(restored_store_bytes, &restored_digest, .{});
    var restored_digest_hex: [std.crypto.hash.sha2.Sha256.digest_length * 2]u8 = undefined;
    for (restored_digest, 0..) |byte, index| {
        restored_digest_hex[index * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        restored_digest_hex[index * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    try std.testing.expectEqualStrings(captured_replica.logical_sha256, &restored_digest_hex);
    const captured_slot = primary.slot("standby-capture") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(antfly.ha.slot_store.SlotLifecycle.seeding, captured_slot.lifecycle);
    try std.testing.expect(!captured_slot.active);
    try std.testing.expect(!captured_slot.reseed_required);

    var capture_retry = try server.http_server.?.handle(.{
        .method = .POST,
        .uri = antfly.admin.routes.ha_base_backups_capture,
        .authorization = "Bearer runtime-secret-token",
        .content_type = "application/json",
        .body = capture_body,
    });
    defer capture_retry.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), capture_retry.status);
    try std.testing.expect(std.mem.indexOf(u8, capture_retry.body, "\"state\":\"already_applied\"") != null);
    var capture_retry_response = try std.json.parseFromSlice(CaptureDigestResponse, alloc, capture_retry.body, .{ .ignore_unknown_fields = true });
    defer capture_retry_response.deinit();
    try std.testing.expectEqualStrings(capture_response.value.capture_receipt_sha256, capture_retry_response.value.capture_receipt_sha256);

    server.ha_cfg.seed_snapshot_provider = null;
    fake_status.include_extension = true;
    var default_capture = try DataServer.captureHASeedCallback(
        &server,
        alloc,
        "standby-default-provider",
        "seed-default-provider-4",
        capture_binding,
    );
    defer default_capture.deinit(alloc);
    const default_topology_path = try std.fs.path.join(alloc, &.{ default_capture.capture.content_root, "TOPOLOGY.json" });
    defer alloc.free(default_topology_path);
    try std.Io.Dir.accessAbsolute(io_impl.io(), default_topology_path, .{});
    const default_topology_json = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), default_topology_path, alloc, .limited(1024 * 1024));
    defer alloc.free(default_topology_json);
    var default_topology = try std.json.parseFromSlice(
        antfly.ha.seed_materialization.Topology,
        alloc,
        default_topology_json,
        .{ .ignore_unknown_fields = false },
    );
    defer default_topology.deinit();
    try std.testing.expectEqual(@as(usize, 1), default_topology.value.catalog.extension_packages.len);
    try std.testing.expectEqual(@as(usize, 2), default_topology.value.extension_artifacts.len);
    for (default_topology.value.extension_artifacts) |artifact| {
        const captured_extension_path = try std.fs.path.join(alloc, &.{ default_capture.capture.content_root, artifact.path });
        defer alloc.free(captured_extension_path);
        const source_extension_path = try std.fs.path.join(alloc, &.{ extension_store_root, artifact.path["extensions/".len..] });
        defer alloc.free(source_extension_path);
        const captured_extension = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), captured_extension_path, alloc, .limited(1024 * 1024));
        defer alloc.free(captured_extension);
        const source_extension = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), source_extension_path, alloc, .limited(1024 * 1024));
        defer alloc.free(source_extension);
        try std.testing.expectEqualStrings(source_extension, captured_extension);
    }

    // Deterministically mutate a source binary after the first copy pass.
    // Capture must discard the whole building generation rather than publish
    // a mixed package-store observation.
    const extension_race_building_root = try std.fs.path.join(alloc, &.{ capture_fixture_root, "extension-race-building" });
    defer alloc.free(extension_race_building_root);
    try fs_paths.createDirPathPortable(io_impl.io(), extension_race_building_root);
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), extension_race_building_root) catch {};
    const ExtensionRaceProbe = struct {
        runtime_path: []const u8,

        fn afterCopy(ptr: *anyopaque, hook_alloc: std.mem.Allocator) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var hook_io_impl = std.Io.Threaded.init(hook_alloc, .{});
            defer hook_io_impl.deinit();
            var file = try std.Io.Dir.cwd().createFile(hook_io_impl.io(), self.runtime_path, .{ .truncate = true });
            defer file.close(hook_io_impl.io());
            try file.writeStreamingAll(hook_io_impl.io(), "raced-extension-runtime");
            try file.sync(hook_io_impl.io());
        }
    };
    var extension_race_probe = ExtensionRaceProbe{ .runtime_path = extension_runtime_path };
    var raced_artifacts = std.ArrayListUnmanaged(HASeedSnapshotExtensionArtifact).empty;
    defer freeHASeedSnapshotExtensionArtifacts(alloc, &raced_artifacts);
    try std.testing.expectError(error.HASeedExtensionChangedDuringCapture, server.captureHASeedExtensionArtifacts(
        alloc,
        io_impl.io(),
        extension_race_building_root,
        &.{.{
            .name = "memoryaf",
            .version = "1.0.0",
            .digest = "sha256:fixture",
            .install = .{ .scopes_supported = &.{.cluster} },
        }},
        &raced_artifacts,
        .{ .ptr = &extension_race_probe, .after_copy_fn = ExtensionRaceProbe.afterCopy },
    ));
    const default_raft_wal = try std.fs.path.join(alloc, &.{ default_capture.capture.content_root, "replicas/group-1/raft/wal-000001" });
    defer alloc.free(default_raft_wal);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io_impl.io(), default_raft_wal, .{}));
    const default_captured_slot = primary.slot("standby-default-provider") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(antfly.ha.slot_store.SlotLifecycle.seeding, default_captured_slot.lifecycle);
    try std.testing.expect(!default_captured_slot.reseed_required);

    // Auth-enabled capture is generation-bound and materializes a complete
    // standalone auth backend alongside the restored data replicas.
    var auth_backend = antfly.lsm_backend.Backend.init(alloc, .{ .flush_threshold = 1 });
    defer auth_backend.close();
    var auth_runtime = try auth_backend.runtimeNamespaceStore(alloc);
    defer auth_runtime.deinit();
    var auth_users = antfly.usermgr.StorageUserStore.init(alloc, auth_runtime);
    var auth_casbin = antfly.usermgr.StorageCasbinAdapter.init(alloc, auth_runtime);
    var auth_manager = try antfly.usermgr.UserManager.init(
        alloc,
        auth_users.iface(),
        try antfly.usermgr.initDefaultEnforcer(alloc, auth_casbin.iface()),
    );
    defer auth_manager.deinit();
    var auth_admin = try antfly.usermgr.Permission.initOwned(alloc, .@"*", "*", .admin);
    defer auth_admin.deinit(alloc);
    var auth_alice = try auth_manager.createUserWithMetadata("alice", "seed-secret", &.{auth_admin}, "{\"tenant_id\":\"acme\"}");
    defer auth_alice.deinit(alloc);
    var auth_reader = try antfly.usermgr.Permission.initOwned(alloc, .table, "docs", .read);
    defer auth_reader.deinit(alloc);
    try auth_manager.addPermissionToSubject("role:tenant-reader", auth_reader);
    try auth_manager.addRoleToUser("alice", "role:tenant-reader");
    try auth_manager.setSubjectRowFilter("role:tenant-reader", "docs", "{\"term\":{\"tenant_id\":\"acme\"}}");
    var auth_key_filter = try antfly.usermgr.RowFilterEntry.initOwned(alloc, "docs", "{\"term\":{\"tier\":\"gold\"}}");
    defer auth_key_filter.deinit(alloc);
    var auth_key = try auth_manager.createApiKey("alice", "seed-ci", &.{auth_reader}, &.{auth_key_filter}, null);
    defer auth_key.deinit(alloc);
    server.api_server_cfg.auth_enabled = true;
    server.api_server_cfg.user_manager = &auth_manager;
    var auth_capture = try DataServer.captureHASeedCallback(
        &server,
        alloc,
        "standby-auth-enabled",
        "seed-standby-auth-enabled-5",
        capture_binding,
    );
    defer auth_capture.deinit(alloc);
    server.api_server_cfg.auth_enabled = false;
    server.api_server_cfg.user_manager = null;
    const auth_topology_path = try std.fs.path.join(alloc, &.{ auth_capture.capture.content_root, "TOPOLOGY.json" });
    defer alloc.free(auth_topology_path);
    const auth_topology_json = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), auth_topology_path, alloc, .limited(16 * 1024 * 1024));
    defer alloc.free(auth_topology_json);
    var auth_topology = try std.json.parseFromSlice(HASeedSnapshotTopology, alloc, auth_topology_json, .{ .ignore_unknown_fields = false });
    defer auth_topology.deinit();
    try std.testing.expect(auth_topology.value.auth_enabled);
    try std.testing.expect(auth_topology.value.auth_artifact != null);
    const auth_live_root = try std.fs.path.join(alloc, &.{ capture_fixture_root, "auth-materialized" });
    defer alloc.free(auth_live_root);
    var auth_materialized = try antfly.ha.seed_materialization.materialize(alloc, .{
        .raw_generation_root = auth_capture.capture.content_root,
        .live_installing_root = auth_live_root,
        .generation = "seed-standby-auth-enabled-5",
        .target_local_node_id = 22,
        .seed_receipt_sha256 = "0000000000000000000000000000000000000000000000000000000000000000",
        .capture_receipt_sha256 = "1111111111111111111111111111111111111111111111111111111111111111",
        .raw_manifest_sha256 = "2222222222222222222222222222222222222222222222222222222222222222",
        .raw_aggregate_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
    });
    defer auth_materialized.deinit(alloc);
    const restored_auth_root = try std.fs.path.join(alloc, &.{ auth_live_root, "metadata/auth" });
    defer alloc.free(restored_auth_root);
    {
        var restored_auth_backend = try antfly.lsm_backend.BackendHandle.open(alloc, restored_auth_root, .{ .flush_threshold = 1 });
        defer restored_auth_backend.close();
        var restored_auth_runtime = try restored_auth_backend.backend.runtimeNamespaceStore(alloc);
        defer restored_auth_runtime.deinit();
        var restored_auth_users = antfly.usermgr.StorageUserStore.init(alloc, restored_auth_runtime);
        var restored_auth_casbin = antfly.usermgr.StorageCasbinAdapter.init(alloc, restored_auth_runtime);
        var restored_auth = try antfly.usermgr.UserManager.init(
            alloc,
            restored_auth_users.iface(),
            try antfly.usermgr.initDefaultEnforcer(alloc, restored_auth_casbin.iface()),
        );
        defer restored_auth.deinit();
        var restored_alice = try restored_auth.authenticateUser("alice", "seed-secret");
        defer restored_alice.deinit(alloc);
        try std.testing.expectEqualStrings("{\"tenant_id\":\"acme\"}", restored_alice.metadata_json);
        try std.testing.expect(try restored_auth.enforce("alice", .table, "docs", .read));
        var restored_key = try restored_auth.validateApiKey(auth_key.key.key_id, auth_key.key_secret);
        defer restored_key.deinit(alloc);
        try std.testing.expectEqualStrings("alice", restored_key.username);
        try std.testing.expectEqual(@as(usize, 1), restored_key.roles.len);
        try std.testing.expectEqualStrings("role:tenant-reader", restored_key.roles[0]);
        try std.testing.expectEqual(@as(usize, 1), restored_key.row_filter.len);
        try restored_auth.updatePassword("alice", "post-seed-secret");
    }
    // Materialization is a one-shot install. Subsequent auth mutations survive
    // restart and are not replayed over by the immutable seed artifact.
    {
        var restarted_backend = try antfly.lsm_backend.BackendHandle.open(alloc, restored_auth_root, .{ .flush_threshold = 1 });
        defer restarted_backend.close();
        var restarted_runtime = try restarted_backend.backend.runtimeNamespaceStore(alloc);
        defer restarted_runtime.deinit();
        var restarted_users = antfly.usermgr.StorageUserStore.init(alloc, restarted_runtime);
        var restarted_casbin = antfly.usermgr.StorageCasbinAdapter.init(alloc, restarted_runtime);
        var restarted_auth = try antfly.usermgr.UserManager.init(
            alloc,
            restarted_users.iface(),
            try antfly.usermgr.initDefaultEnforcer(alloc, restarted_casbin.iface()),
        );
        defer restarted_auth.deinit();
        var restarted_alice = try restarted_auth.authenticateUser("alice", "post-seed-secret");
        defer restarted_alice.deinit(alloc);
        try std.testing.expectError(error.InvalidPassword, restarted_auth.authenticateUser("alice", "seed-secret"));
    }

    // A store package absent from the catalog must not ride along in a seed.
    fake_status.include_extension = false;
    try std.testing.expectError(error.HASeedExtensionCatalogMismatch, DataServer.captureHASeedCallback(
        &server,
        alloc,
        "standby-unregistered-extension",
        "seed-standby-unregistered-extension-5",
        capture_binding,
    ));
    try std.testing.expect(primary.slot("standby-unregistered-extension") == null);
    fake_status.include_extension = true;

    // Conversely, every catalog package must have an exact package-store
    // image. Missing binaries cannot yield a standby that only looks healthy.
    std.Io.Dir.cwd().deleteTree(io_impl.io(), extension_store_root) catch {};
    try std.testing.expectError(error.HASeedExtensionCatalogMismatch, DataServer.captureHASeedCallback(
        &server,
        alloc,
        "standby-missing-extension",
        "seed-standby-missing-extension-6",
        capture_binding,
    ));
    try std.testing.expect(primary.slot("standby-missing-extension") == null);

    // Auth-enabled capture without a concrete manager still fails before a
    // slot is burned; it can never silently emit an empty auth artifact.
    server.api_server_cfg.auth_enabled = true;
    if (DataServer.captureHASeedCallback(
        &server,
        alloc,
        "standby-auth-missing-manager",
        "seed-standby-auth-missing-manager-5",
        capture_binding,
    )) |unexpected_value| {
        var unexpected = unexpected_value;
        unexpected.deinit(alloc);
        return error.TestExpectedError;
    } else |err| try std.testing.expectEqual(error.HASeedCaptureAuthManagerMissing, err);
    try std.testing.expect(primary.slot("standby-auth-missing-manager") == null);
    server.api_server_cfg.auth_enabled = false;

    // Contract: maintenance, bulk/structural activity, or an unsupported
    // snapshot provider must fail closed before backup_start burns a slot.
    var active_group_operation = server.write_source.beginGroupRefreshActivity("docs", 1);
    try std.testing.expectError(error.HASeedSnapshotRuntimeBusy, DataServer.captureHASeedCallback(
        &server,
        alloc,
        "standby-structural-busy",
        "seed-standby-structural-busy-5",
        capture_binding,
    ));
    active_group_operation.deinit();

    server.lsm_maintenance_active.store(true, .release);
    defer server.lsm_maintenance_active.store(false, .release);
    try std.testing.expectError(error.HASeedSnapshotRuntimeBusy, DataServer.captureHASeedCallback(
        &server,
        alloc,
        "standby-busy",
        "seed-standby-busy-6",
        capture_binding,
    ));
    server.lsm_maintenance_active.store(false, .release);

    server.ha_cfg.seed_snapshot_provider = seed_snapshot_provider.iface();
    seed_snapshot_provider.unsupported = true;
    try std.testing.expectError(error.HASeedSnapshotUnsupportedBackend, DataServer.captureHASeedCallback(
        &server,
        alloc,
        "standby-unsupported",
        "seed-standby-unsupported-7",
        capture_binding,
    ));

    var internal_resp = try server.http_server.?.handle(.{
        .method = .GET,
        .uri = antfly.internal.routes.ha_replication_identify,
    });
    defer internal_resp.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 200), internal_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, internal_resp.body, "\"record_format_version\"") != null);

    var health = HealthSource{ .data_server = &server };
    var writer_buf: [262144]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try health.metricsWriter().writeMetrics(&writer);
    const metrics = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_runtime_configured 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_primary_slot_count 3\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_primary_retention_retained_age_ns 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_primary_retention_active_slots 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_primary_retention_reseed_recommended 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_slot_received_lsn{slot=\"standby-a\"} 0\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_slot_status_code{slot=\"standby-a\"} 2\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_primary_mirror_failures_total 0\n") != null);
}

test "data server mirrors managed primary writes into HA replication log" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-primary-mirror-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const primary_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-primary-mirror-log-{d}", .{nonce});
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-primary-mirror-slots-{d}", .{nonce});
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};

    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();

    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .primary = &primary,
                .primary_node_id = "primary-a",
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    const source_mirror = server.write_source.ha_async_mirror orelse return error.TestExpectedEqual;
    try std.testing.expect(source_mirror.primary == &primary);
    const cache_mirror = server.provisioned_storage.write_cache.ha_async_mirror orelse return error.TestExpectedEqual;
    try std.testing.expect(cache_mirror.primary == &primary);

    _ = try server.write_source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .timestamp_ns = 123,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    try std.testing.expectEqual(primary.lastLsn(), server.ha_primary_mirror_last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), server.ha_primary_mirror_failure_count.load(.acquire));

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .batch_mutation), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 100), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 77), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 7), entry.record.table_id);

    var decoded = try antfly.ha.effects.decodeBatchMutationRequest(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1), decoded.value.request.writes.len);
    try std.testing.expectEqualStrings("doc:a", decoded.value.request.writes[0].key);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", decoded.value.request.writes[0].value);
    try std.testing.expectEqual(@as(u64, 123), decoded.value.request.timestamp_ns);
}

test "data server fail-closed sync policy rejects primary writes before local commit" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-sync-reject-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const primary_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-sync-reject-log-{d}", .{nonce});
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-sync-reject-slots-{d}", .{nonce});
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};

    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    const standby_names = [_][]const u8{"standby-a"};

    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .primary = &primary,
                .primary_node_id = "primary-a",
            },
            .primary_sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .fail_closed,
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    try std.testing.expectError(error.SyncPolicyUnsatisfied, server.write_source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:rejected", .value = "{\"title\":\"rejected\"}" }},
        .timestamp_ns = 123,
        .sync_level = .write,
    }));

    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), server.ha_primary_mirror_last_gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(antfly.ha.commit_gate.Action.reject), server.ha_primary_mirror_last_gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), server.ha_primary_mirror_sync_reject_count.load(.acquire));
}

test "data server block sync policy waits for standby acknowledgement before commit returns" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-sync-block-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const primary_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-sync-block-log-{d}", .{nonce});
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-sync-block-slots-{d}", .{nonce});
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};

    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    const standby_names = [_][]const u8{"standby-a"};

    const Poll = struct {
        calls: u64 = 0,

        fn update(ctx: *anyopaque, primary_arg: *antfly.ha.primary.Primary, target_lsn: u64, policy: antfly.ha.primary.SyncPolicy, round: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            try std.testing.expectEqual(@as(usize, 0), round);
            try std.testing.expectEqual(antfly.ha.primary.DurabilityMode.remote_write, policy.mode);
            try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, 0);
        }
    };
    var poll = Poll{};

    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .primary = &primary,
                .primary_node_id = "primary-a",
            },
            .primary_sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .primary_sync_wait = .{
                .max_rounds = 4,
                .sleep_ns = 0,
                .poll_ctx = &poll,
                .poll_fn = Poll.update,
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    _ = try server.write_source.source().batch(alloc, "docs", .{
        .writes = &.{.{ .key = "doc:block", .value = "{\"title\":\"block\"}" }},
        .timestamp_ns = 123,
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(u64, 2), poll.calls);
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 2), server.ha_primary_mirror_last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), server.ha_primary_mirror_last_gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(antfly.ha.commit_gate.Action.acknowledge), server.ha_primary_mirror_last_gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), server.ha_primary_mirror_sync_wait_count.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), server.ha_primary_mirror_sync_reject_count.load(.acquire));

    var found = (try server.read_source.source().lookup(alloc, "docs", "doc:block", .{}, .read_index)) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"block\"}", found.json);
}

test "data server propagates standby HA write gate into provisioned write sources" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-standby-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-standby-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    var standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = ".",
        .ha = .{
            .admin_context = .{
                .standby = &standby,
                .standby_node_id = "standby-a",
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    const source_gate = server.write_source.ha_write_gate orelse return error.TestExpectedEqual;
    switch (source_gate) {
        .shared => |gate| {
            try std.testing.expect(gate.state == &server.ha_public_gate_state);
            try std.testing.expect(gate.generation == null);
        },
        .standby, .fenced_primary, .primary => return error.TestExpectedEqual,
    }

    const cache_gate = server.provisioned_storage.write_cache.ha_write_gate orelse return error.TestExpectedEqual;
    switch (cache_gate) {
        .shared => |gate| {
            try std.testing.expect(gate.state == &server.ha_public_gate_state);
            try std.testing.expect(gate.generation == null);
        },
        .standby, .fenced_primary, .primary => return error.TestExpectedEqual,
    }

    const read_gate = server.read_source.ha_read_gate orelse return error.TestExpectedEqual;
    switch (read_gate) {
        .shared => |state| try std.testing.expect(state == &server.ha_public_gate_state),
        .standby => return error.TestExpectedEqual,
    }
    try std.testing.expectError(
        error.HAReadRequiresPrimary,
        server.read_source.source().lookup(alloc, "docs", "doc:a", .{}, .read_index),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        server.write_source.source().batchGroupLocal(alloc, 10, "docs", .{
            .writes = &.{.{ .key = "doc:local", .value = "{\"title\":\"local-write\"}" }},
            .timestamp_ns = 1,
            .sync_level = .write,
        }),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        server.write_source.source().createTable(alloc, "docs", .{}),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        server.write_source.source().beginBulkIngest(alloc, "docs"),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        server.write_source.source().reprocessDocumentArtifactGroupLocal(alloc, 10, "docs", "doc:local", "embedding"),
    );
    try std.testing.expect(!server.haOwnerJobCanRun(.compaction_publish));
    try server.runLsmMaintenanceForegroundRound();
    try server.requestLsmMaintenanceBackground();
    try std.testing.expect(server.lsm_maintenance_thread == null);
}

test "storage.ha data server rejects writes and owner jobs after primary promotion fence" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const primary_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-fenced-primary-log-{d}", .{nonce});
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-fenced-primary-slots-{d}", .{nonce});
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);
    const fence_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-fenced-primary-fence-{d}", .{nonce});
    defer alloc.free(fence_raw);
    const fence_path = try alloc.dupeZ(u8, fence_raw);
    defer alloc.free(fence_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_path) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_path) catch {};

    const identity = antfly.ha.primary.Identity{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "before-fence" });
    _ = try primary.append(.{ .payload = "after-controller-observation" });

    var fence_store = try antfly.ha.fencing.Store.open(alloc, fence_path.ptr, .{});
    defer fence_store.close();

    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = ".",
        .ha = .{
            .admin_context = .{
                .primary = &primary,
                .primary_node_id = "primary-a",
                .fence_store = &fence_store,
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    const source_gate = server.write_source.ha_write_gate orelse return error.TestExpectedEqual;
    switch (source_gate) {
        .shared => |gate| {
            try std.testing.expect(gate.state == &server.ha_public_gate_state);
            try std.testing.expect(gate.generation == null);
        },
        .primary, .fenced_primary, .standby => return error.TestExpectedEqual,
    }
    try source_gate.check();

    var fence_response = try server.http_server.?.handle(.{
        .method = .POST,
        .uri = antfly.admin.routes.ha_fence,
        .content_type = "application/json",
        .body = "{\"identity\":{\"cluster_id\":100,\"shard_id\":10,\"table_id\":20,\"timeline_id\":1,\"epoch\":1},\"old_primary_id\":\"primary-a\",\"promoted_node_id\":\"standby-a\",\"new_timeline_id\":2,\"new_epoch\":2,\"required_lsn\":1,\"observed_lsn\":1,\"force\":false,\"reason\":\"data-server-test\"}",
    });
    defer fence_response.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 200), fence_response.status);
    try std.testing.expect(std.mem.indexOf(u8, fence_response.body, "\"node_id\":\"primary-a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, fence_response.body, "\"required_lsn\":2") != null);
    try std.testing.expect(std.mem.indexOf(u8, fence_response.body, "\"observed_lsn\":2") != null);
    try std.testing.expectError(error.HAFencedPrimary, source_gate.check());
    try std.testing.expect((try server.read_source.source().lookup(alloc, "docs", "doc:local", .{}, .stale)) == null);
    try std.testing.expectError(
        error.HAReadRequiresPrimary,
        server.read_source.source().lookup(alloc, "docs", "doc:local", .{}, .read_index),
    );

    try std.testing.expectError(error.HAFencedPrimary, server.write_source.source().batchGroupLocal(alloc, 10, "docs", .{
        .writes = &.{.{ .key = "doc:local", .value = "{\"title\":\"local-write\"}" }},
        .timestamp_ns = 1,
        .sync_level = .write,
    }));
    try std.testing.expect(!server.haOwnerJobCanRun(.compaction_publish));
}

test "data server applies routed HA replication records through standby write gate" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-routed-apply-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-routed-apply-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-routed-apply-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    var standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .standby = &standby,
                .standby_node_id = "standby-a",
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    const payload = try antfly.ha.effects.encodeBatchMutationRequestAlloc(alloc, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        .sync_level = .write,
    });
    defer alloc.free(payload);

    try server.applyHAReplicationRecord(.{
        .kind = .batch_mutation,
        .payload_codec = .json,
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
        .lsn = 1,
        .previous_lsn = 0,
        .payload = payload,
    });

    var lookup = (try server.read_source.source().lookupGroupLocal(
        alloc,
        77,
        "docs",
        "doc:a",
        .{},
        .stale,
    )) orelse return error.TestExpectedEqual;
    defer lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "\"title\":\"alpha\"") != null);
}

test "data server pulls and applies HA standby replication through internal HTTP client" {
    const alloc = std.testing.allocator;
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

    const AuthenticatedExecutor = struct {
        upstream: antfly.common.http.RequestExecutor,
        accepted: usize = 0,
        rejected: usize = 0,

        fn executor(self: *@This()) antfly.common.http.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(
            ptr: *anyopaque,
            response_alloc: std.mem.Allocator,
            request: antfly.common.http.HttpRequest,
        ) !antfly.common.http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (request.authorization == null or
                !std.mem.eql(u8, request.authorization.?, "Bearer runtime-secret-token"))
            {
                self.rejected += 1;
                return .{
                    .status = 401,
                    .content_type = try response_alloc.dupe(u8, "text/plain"),
                    .body = try response_alloc.dupe(u8, "unauthorized"),
                };
            }
            self.accepted += 1;
            return try self.upstream.execute(response_alloc, request);
        }
    };

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-replicate-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const primary_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-replicate-primary-log-{d}", .{nonce});
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-replicate-primary-slots-{d}", .{nonce});
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-replicate-standby-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-replicate-standby-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    const identity: antfly.ha.primary.Identity = .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try antfly.ha.effects.appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc:http", .value = "{\"title\":\"from-http\"}" }},
        .sync_level = .write,
    }, .{});

    var standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, identity, .{});
    defer standby.close();

    var primary_internal = antfly.ha.http_internal.Server.init(alloc, &primary);
    var authenticated_executor = AuthenticatedExecutor{ .upstream = primary_internal.executor() };
    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .standby = &standby,
                .standby_node_id = "standby-a",
            },
            .admin_bearer_token = "runtime-secret-token",
            .standby_replication = .{
                .upstream_base_uri = "http://primary.internal.test",
                .slot_name = "standby-a",
                .options = .{ .max_records = 1 },
                .catch_up_until_end_of_wal = true,
                .executor = authenticated_executor.executor(),
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();

    try server.runHAStandbyReplicationRound();

    const progress = standby.currentProgress();
    try std.testing.expectEqual(@as(u64, 1), progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), progress.applied_lsn);
    try std.testing.expect(authenticated_executor.accepted >= 3);
    try std.testing.expectEqual(@as(usize, 0), authenticated_executor.rejected);
    try std.testing.expect(server.ha_standby_replication_last_attempt_ns.load(.acquire) > 0);
    try std.testing.expect(server.ha_standby_replication_last_success_ns.load(.acquire) > 0);

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);

    var admin_status = try server.http_server.?.handle(.{
        .method = .GET,
        .uri = antfly.admin.routes.ha_standby_status,
        .authorization = "Bearer runtime-secret-token",
    });
    defer admin_status.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 200), admin_status.status);
    try std.testing.expect(std.mem.indexOf(u8, admin_status.body, "\"last_attempt_ns\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, admin_status.body, "\"last_success_ns\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, admin_status.body, "\"replication_failures_total\":0") != null);

    var health = HealthSource{ .data_server = &server };
    var writer_buf: [262144]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try health.metricsWriter().writeMetrics(&writer);
    const metrics = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_runtime_configured 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_received_lsn 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_applied_lsn 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_upstream_configured 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_caught_up_to_received 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_replication_last_attempt_ns ") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_replication_last_success_ns ") != null);

    var lookup = (try server.read_source.source().lookupGroupLocal(
        alloc,
        77,
        "docs",
        "doc:http",
        .{},
        .stale,
    )) orelse return error.TestExpectedEqual;
    defer lookup.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, lookup.json, "\"title\":\"from-http\"") != null);
}

test "data server HA replication network wait leaves state mutex available" {
    const alloc = std.testing.allocator;
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
    const BlockingExecutor = struct {
        upstream: antfly.common.http.RequestExecutor,
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn executor(self: *@This()) antfly.common.http.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(
            ptr: *anyopaque,
            alloc_arg: std.mem.Allocator,
            req: antfly.common.http.HttpRequest,
        ) anyerror!antfly.common.http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.endsWith(u8, req.uri, antfly.internal.routes.ha_replication_start)) {
                self.entered.store(true, .release);
                while (!self.release.load(.acquire)) std.Thread.yield() catch {};
            }
            return try self.upstream.execute(alloc_arg, req);
        }
    };
    const ReplicationThread = struct {
        server: *DataServer,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.server.runHAStandbyReplicationRound() catch |err| {
                self.err = err;
            };
        }
    };

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-unlocked-network-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const primary_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-unlocked-network-primary-log-{d}", .{nonce});
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-unlocked-network-primary-slots-{d}", .{nonce});
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-unlocked-network-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-unlocked-network-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    const identity: antfly.ha.standby.Identity = .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    var primary_internal = antfly.ha.http_internal.Server.init(alloc, &primary);
    var standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, identity, .{});
    defer standby.close();
    var blocking_executor = BlockingExecutor{ .upstream = primary_internal.executor() };
    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .standby = &standby,
                .standby_node_id = "standby-a",
            },
            .standby_replication = .{
                .upstream_base_uri = "http://primary.internal.test",
                .slot_name = "standby-a",
                .options = .{ .verify_upstream = false },
                .executor = blocking_executor.executor(),
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    var replication_thread = ReplicationThread{ .server = &server };
    const thread = try std.Thread.spawn(.{}, ReplicationThread.run, .{&replication_thread});
    var joined = false;
    defer if (!joined) {
        blocking_executor.release.store(true, .release);
        thread.join();
    };

    var spins: usize = 0;
    while (!blocking_executor.entered.load(.acquire) and spins < 1_000_000) : (spins += 1) {
        std.Thread.yield() catch {};
    }
    try std.testing.expect(blocking_executor.entered.load(.acquire));

    const mutex_available = server.ha_state_mutex.tryLock();
    var promotion_err: ?anyerror = null;
    if (mutex_available) {
        _ = standby.promote(.{
            .new_timeline_id = 2,
            .new_epoch = 2,
            .fencing_confirmed = true,
        }) catch |err| {
            promotion_err = err;
        };
        server.ha_state_mutex.unlock();
    }
    blocking_executor.release.store(true, .release);
    thread.join();
    joined = true;

    try std.testing.expect(mutex_available);
    try std.testing.expect(promotion_err == null);
    const replication_err = replication_thread.err orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(error.HAStandbyStateChanged, replication_err);
}

test "data server promotion rewires live HTTP internal HA executor" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-promoted-http-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-promoted-http-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-promoted-http-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    var standby: ?antfly.ha.standby.Standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer if (standby) |*handle| handle.close();
    if (standby) |*handle| {
        _ = try handle.promote(.{
            .new_timeline_id = 2,
            .new_epoch = 2,
            .fencing_confirmed = true,
        });
    }

    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .standby = if (standby) |*handle| handle else null,
                .standby_node_id = "standby-a",
            },
            .standby_owner = &standby,
            .standby_replication = .{
                .upstream_base_uri = "http://primary.internal.test",
                .slot_name = "standby-a",
                .standby_log_path = standby_log,
                .standby_progress_path = standby_progress,
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    const public_read_gate_before = server.read_source.ha_read_gate orelse return error.TestExpectedEqual;
    const public_write_gate_before = server.write_source.ha_write_gate orelse return error.TestExpectedEqual;
    try std.testing.expectError(error.HAReadRequiresPrimary, public_read_gate_before.check(.stale));
    try std.testing.expectError(error.HAPromotedStandbyRequiresPrimaryOpen, public_write_gate_before.check());

    var before = try server.http_server.?.handle(.{
        .method = .GET,
        .uri = antfly.internal.routes.ha_replication_identify,
    });
    defer before.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 404), before.status);

    try server.runHAStandbyReplicationRound();
    try std.testing.expect(standby == null);
    try std.testing.expect(server.ha_promoted_primary != null);
    try std.testing.expect(server.ha_cfg.admin_context.?.standby == null);
    try std.testing.expect(server.ha_cfg.admin_context.?.promoted_standby_handoff != null);
    try std.testing.expect(server.ha_admin_server.?.auth.state_mutex == &server.ha_state_mutex);
    try std.testing.expect(server.ha_internal_server.?.state_mutex == &server.ha_state_mutex);
    const public_read_gate_after = server.read_source.ha_read_gate orelse return error.TestExpectedEqual;
    const public_write_gate_after = server.write_source.ha_write_gate orelse return error.TestExpectedEqual;
    try std.testing.expect(std.meta.eql(public_read_gate_before, public_read_gate_after));
    try std.testing.expect(std.meta.eql(public_write_gate_before, public_write_gate_after));
    try public_read_gate_before.check(.stale);
    try public_write_gate_before.check();

    var write_check = try server.http_server.?.handle(.{
        .method = .POST,
        .uri = antfly.admin.routes.ha_write_check,
        .content_type = "application/json",
        .body = "{\"role\":\"standby\",\"expected_identity\":{\"cluster_id\":100,\"shard_id\":77,\"table_id\":7,\"timeline_id\":2,\"epoch\":2}}",
    });
    defer write_check.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 200), write_check.status);
    try std.testing.expect(std.mem.indexOf(u8, write_check.body, "\"role\":\"promoted_standby\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_check.body, "\"action\":\"open_promoted_primary\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, write_check.body, "\"promotion_handoff\"") != null);

    var owner_job_check = try server.http_server.?.handle(.{
        .method = .POST,
        .uri = antfly.admin.routes.ha_owner_job_check,
        .content_type = "application/json",
        .body = "{\"role\":\"standby\",\"kind\":\"retention_advance\",\"expected_identity\":{\"cluster_id\":100,\"shard_id\":77,\"table_id\":7,\"timeline_id\":2,\"epoch\":2}}",
    });
    defer owner_job_check.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 200), owner_job_check.status);
    try std.testing.expect(std.mem.indexOf(u8, owner_job_check.body, "\"role\":\"promoted_standby\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, owner_job_check.body, "\"action\":\"open_promoted_primary\"") != null);

    var internal_resp = try server.http_server.?.handle(.{
        .method = .GET,
        .uri = antfly.internal.routes.ha_replication_identify,
    });
    defer internal_resp.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 200), internal_resp.status);
    try std.testing.expect(std.mem.indexOf(u8, internal_resp.body, "\"record_format_version\"") != null);
}

test "data server promotion open failure preserves retryable standby" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-promote-retry-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-promote-retry-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-promote-retry-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);
    const wrong_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-promote-retry-wrong-log-{d}", .{nonce});
    defer alloc.free(wrong_log_raw);
    const wrong_log = try alloc.dupeZ(u8, wrong_log_raw);
    defer alloc.free(wrong_log);
    const promoted_slots_raw = try std.fmt.allocPrint(alloc, "{s}.promoted-primary-slots", .{standby_progress});
    defer alloc.free(promoted_slots_raw);
    const promoted_slots = try alloc.dupeZ(u8, promoted_slots_raw);
    defer alloc.free(promoted_slots);
    const standby_log_alias = try std.fmt.allocPrint(alloc, "./{s}", .{standby_log});
    defer alloc.free(standby_log_alias);
    const standby_progress_alias = try std.fmt.allocPrint(alloc, "./{s}", .{standby_progress});
    defer alloc.free(standby_progress_alias);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), wrong_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), promoted_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), wrong_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), promoted_slots) catch {};

    var standby: ?antfly.ha.standby.Standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer if (standby) |*handle| handle.close();
    if (standby) |*handle| {
        _ = try handle.promote(.{
            .new_timeline_id = 2,
            .new_epoch = 2,
            .fencing_confirmed = true,
        });
    }

    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .standby = if (standby) |*handle| handle else null,
                .standby_node_id = "standby-a",
            },
            .standby_owner = &standby,
            .standby_replication = .{
                .upstream_base_uri = "http://primary.internal.test",
                .slot_name = "standby-a",
                .standby_log_path = wrong_log,
                .standby_progress_path = standby_progress,
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();
    try server.initApiServer();

    const public_read_gate = server.read_source.ha_read_gate orelse return error.TestExpectedEqual;
    const public_write_gate = server.write_source.ha_write_gate orelse return error.TestExpectedEqual;
    try std.testing.expectError(error.HAReadRequiresPrimary, public_read_gate.check(.stale));
    try std.testing.expectError(error.HAPromotedStandbyRequiresPrimaryOpen, public_write_gate.check());

    var slot_blocker: ?antfly.ha.primary.Primary = try antfly.ha.primary.Primary.open(alloc, wrong_log.ptr, promoted_slots.ptr, .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 2,
        .epoch = 2,
    }, .{});
    defer if (slot_blocker) |*handle| handle.close();

    try std.testing.expectError(error.PromotedLogMismatch, server.runHAStandbyReplicationRound());
    try std.testing.expect(standby != null);
    try std.testing.expect(server.ha_promoted_primary == null);
    try std.testing.expect(server.ha_cfg.admin_context.?.standby != null);
    try std.testing.expect(server.ha_cfg.admin_context.?.primary == null);

    // A durable promotion whose runtime resources cannot yet be adopted is a
    // fail-closed retry state, not a reason to terminate the data process.
    try server.runRound();
    try std.testing.expectEqual(@as(u64, 1), server.ha_standby_replication_failure_count.load(.acquire));
    try std.testing.expect(standby != null);
    try std.testing.expect(server.ha_promoted_primary == null);

    server.ha_cfg.standby_replication.?.standby_log_path = standby_log_alias;
    server.ha_cfg.standby_replication.?.standby_progress_path = standby_progress_alias;
    try std.testing.expectError(error.LsmRootWriterAlreadyOpen, server.runHAStandbyReplicationRound());
    server.clearHAStandbyReplicationRetry();
    try server.runRound();
    try std.testing.expectEqual(@as(u64, 2), server.ha_standby_replication_failure_count.load(.acquire));
    try std.testing.expect(standby != null);
    try std.testing.expect(server.ha_promoted_primary == null);

    if (slot_blocker) |*handle| handle.close();
    slot_blocker = null;
    try server.runHAStandbyReplicationRound();
    try std.testing.expect(standby == null);
    try std.testing.expect(server.ha_promoted_primary != null);
    try std.testing.expect(server.ha_cfg.admin_context.?.standby == null);
    try std.testing.expect(server.ha_cfg.admin_context.?.primary != null);
}

test "data server resumes HA standby replication from durable progress after restart" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-resume-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const primary_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-resume-primary-log-{d}", .{nonce});
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-resume-primary-slots-{d}", .{nonce});
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-resume-standby-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-resume-standby-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    const identity: antfly.ha.primary.Identity = .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try antfly.ha.effects.appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc:first", .value = "{\"title\":\"first\"}" }},
        .sync_level = .write,
    }, .{});
    _ = try antfly.ha.effects.appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc:second", .value = "{\"title\":\"second\"}" }},
        .sync_level = .write,
    }, .{});

    var primary_internal = antfly.ha.http_internal.Server.init(alloc, &primary);

    {
        var standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, identity, .{});
        defer standby.close();

        var server = DataServer.initFromLocalMetadataSources(alloc, .{
            .replica_root_dir = replica_root,
            .ha = .{
                .admin_context = .{
                    .standby = &standby,
                    .standby_node_id = "standby-a",
                },
                .standby_replication = .{
                    .upstream_base_uri = "http://primary.internal.test",
                    .slot_name = "standby-a",
                    .options = .{ .max_records = 1 },
                    .catch_up_until_end_of_wal = false,
                    .executor = primary_internal.executor(),
                },
            },
        }, FakeCatalog.iface(), FakeStatus.iface());
        defer server.deinit();

        try server.runHAStandbyReplicationRound();

        const progress = standby.currentProgress();
        try std.testing.expectEqual(@as(u64, 1), progress.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), progress.applied_lsn);
        const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);

        var first_lookup = (try server.read_source.source().lookupGroupLocal(
            alloc,
            77,
            "docs",
            "doc:first",
            .{},
            .stale,
        )) orelse return error.TestExpectedEqual;
        defer first_lookup.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, first_lookup.json, "\"title\":\"first\"") != null);
        const second_lookup = try server.read_source.source().lookupGroupLocal(
            alloc,
            77,
            "docs",
            "doc:second",
            .{},
            .stale,
        );
        try std.testing.expect(second_lookup == null);
    }

    {
        var standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, identity, .{});
        defer standby.close();
        const restored_progress = standby.currentProgress();
        try std.testing.expectEqual(@as(u64, 1), restored_progress.received_lsn);
        try std.testing.expectEqual(@as(u64, 1), restored_progress.applied_lsn);
        try std.testing.expectEqual(@as(u64, 2), standby.nextReceiveLsn());

        var server = DataServer.initFromLocalMetadataSources(alloc, .{
            .replica_root_dir = replica_root,
            .ha = .{
                .admin_context = .{
                    .standby = &standby,
                    .standby_node_id = "standby-a",
                },
                .standby_replication = .{
                    .upstream_base_uri = "http://primary.internal.test",
                    .slot_name = "standby-a",
                    .options = .{ .max_records = 1 },
                    .catch_up_until_end_of_wal = true,
                    .executor = primary_internal.executor(),
                },
            },
        }, FakeCatalog.iface(), FakeStatus.iface());
        defer server.deinit();

        try server.runHAStandbyReplicationRound();

        const progress = standby.currentProgress();
        try std.testing.expectEqual(@as(u64, 2), progress.received_lsn);
        try std.testing.expectEqual(@as(u64, 2), progress.applied_lsn);
        const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 2), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 2), slot.applied_lsn);
        try std.testing.expectEqual(@as(u64, 0), server.ha_standby_replication_failure_count.load(.acquire));
        try std.testing.expect(server.ha_standby_replication_last_success_ns.load(.acquire) > 0);

        var first_lookup = (try server.read_source.source().lookupGroupLocal(
            alloc,
            77,
            "docs",
            "doc:first",
            .{},
            .stale,
        )) orelse return error.TestExpectedEqual;
        defer first_lookup.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, first_lookup.json, "\"title\":\"first\"") != null);

        var second_lookup = (try server.read_source.source().lookupGroupLocal(
            alloc,
            77,
            "docs",
            "doc:second",
            .{},
            .stale,
        )) orelse return error.TestExpectedEqual;
        defer second_lookup.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, second_lookup.json, "\"title\":\"second\"") != null);
    }
}

test "data runtime records and backs off HA standby replication round failures" {
    const alloc = std.testing.allocator;
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

    const FailingExecutor = struct {
        fn executor(self: *@This()) antfly.common.http.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, _: std.mem.Allocator, _: antfly.common.http.HttpRequest) !antfly.common.http.HttpResponse {
            return error.NoAddressReturned;
        }
    };

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-replicate-failed-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-replicate-failed-standby-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-http-replicate-failed-standby-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    var standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 7,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var failing_executor = FailingExecutor{};
    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .standby = &standby,
                .standby_node_id = "standby-a",
            },
            .standby_replication = .{
                .upstream_base_uri = "http://primary.internal.test",
                .slot_name = "standby-a",
                .executor = failing_executor.executor(),
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();

    try server.runRound();
    try std.testing.expectEqual(@as(u64, 1), server.ha_standby_replication_failure_count.load(.acquire));
    try std.testing.expect(server.ha_standby_replication_last_attempt_ns.load(.acquire) > 0);
    try std.testing.expectEqual(@as(u64, 0), server.ha_standby_replication_last_success_ns.load(.acquire));
    const next_attempt_ns = server.ha_standby_replication_next_attempt_ns.load(.acquire);
    try std.testing.expect(next_attempt_ns > 0);
    try std.testing.expect(!server.haStandbyReplicationRetryDue(next_attempt_ns - 1));
    try std.testing.expect(server.haStandbyReplicationRetryDue(next_attempt_ns));

    var admin_status = try server.http_server.?.handle(.{
        .method = .GET,
        .uri = antfly.admin.routes.ha_standby_status,
    });
    defer admin_status.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 200), admin_status.status);
    try std.testing.expect(std.mem.indexOf(u8, admin_status.body, "\"last_error\":\"NoAddressReturned\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, admin_status.body, "\"last_attempt_ns\":") != null);
    try std.testing.expect(std.mem.indexOf(u8, admin_status.body, "\"replication_failures_total\":1") != null);

    var health = HealthSource{ .data_server = &server };
    var writer_buf: [262144]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&writer_buf);
    try health.metricsWriter().writeMetrics(&writer);
    const metrics = writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_runtime_configured 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_replication_failures_total 1\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_replication_last_attempt_ns ") != null);
    try std.testing.expect(std.mem.indexOf(u8, metrics, "antfly_ha_standby_replication_last_success_ns 0\n") != null);
}

test "data runtime HA replication HTTP budget covers base64 apply envelope" {
    const cfg = DataServer.haStandbyReplicationHTTPExecutorConfig();
    const encoded_batch_bytes = std.base64.standard.Encoder.calcSize(
        DataServer.ha_replication_default_max_encoded_bytes_per_apply,
    );
    try std.testing.expect(cfg.max_response_bytes >= encoded_batch_bytes + 64 * 1024);
    try std.testing.expect(cfg.resolve_before_connect);
}

test "data runtime records HA standby apply failures without stopping run round" {
    const alloc = std.testing.allocator;
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

    const nonce = platform_time.monotonicNs();
    const replica_root_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-apply-failed-root-{d}", .{nonce});
    defer alloc.free(replica_root_raw);
    const replica_root = try alloc.dupeZ(u8, replica_root_raw);
    defer alloc.free(replica_root);
    const primary_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-apply-failed-primary-log-{d}", .{nonce});
    defer alloc.free(primary_log_raw);
    const primary_log = try alloc.dupeZ(u8, primary_log_raw);
    defer alloc.free(primary_log);
    const primary_slots_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-apply-failed-primary-slots-{d}", .{nonce});
    defer alloc.free(primary_slots_raw);
    const primary_slots = try alloc.dupeZ(u8, primary_slots_raw);
    defer alloc.free(primary_slots);
    const standby_log_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-apply-failed-standby-log-{d}", .{nonce});
    defer alloc.free(standby_log_raw);
    const standby_log = try alloc.dupeZ(u8, standby_log_raw);
    defer alloc.free(standby_log);
    const standby_progress_raw = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/data-runtime-ha-apply-failed-standby-progress-{d}", .{nonce});
    defer alloc.free(standby_progress_raw);
    const standby_progress = try alloc.dupeZ(u8, standby_progress_raw);
    defer alloc.free(standby_progress);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), replica_root) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    defer std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};

    const identity: antfly.ha.primary.Identity = .{
        .cluster_id = 100,
        .shard_id = 77,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try antfly.ha.primary.Primary.open(alloc, primary_log.ptr, primary_slots.ptr, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    _ = try antfly.ha.effects.appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc:missing-table", .value = "{\"title\":\"missing-table\"}" }},
        .sync_level = .write,
    }, .{});

    var standby = try antfly.ha.standby.Standby.open(alloc, standby_log.ptr, standby_progress.ptr, identity, .{});
    defer standby.close();

    var primary_internal = antfly.ha.http_internal.Server.init(alloc, &primary);
    var server = DataServer.initFromLocalMetadataSources(alloc, .{
        .replica_root_dir = replica_root,
        .ha = .{
            .admin_context = .{
                .standby = &standby,
                .standby_node_id = "standby-a",
            },
            .standby_replication = .{
                .upstream_base_uri = "http://primary.internal.test",
                .slot_name = "standby-a",
                .options = .{ .max_records = 1 },
                .executor = primary_internal.executor(),
            },
        },
    }, FakeCatalog.iface(), FakeStatus.iface());
    defer server.deinit();

    try server.runRound();
    try std.testing.expectEqual(@as(u64, 1), server.ha_standby_replication_failure_count.load(.acquire));
    try std.testing.expect(server.ha_standby_replication_last_attempt_ns.load(.acquire) > 0);
    try std.testing.expectEqual(@as(u64, 0), server.ha_standby_replication_last_success_ns.load(.acquire));

    const progress = standby.currentProgress();
    try std.testing.expectEqual(@as(u64, 1), progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 0), progress.applied_lsn);
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 0), slot.applied_lsn);

    var admin_status = try server.http_server.?.handle(.{
        .method = .GET,
        .uri = antfly.admin.routes.ha_standby_status,
    });
    defer admin_status.deinit(server.http_server.?.alloc);
    try std.testing.expectEqual(@as(u16, 200), admin_status.status);
    try std.testing.expect(std.mem.indexOf(u8, admin_status.body, "\"last_error\":\"HAReplicationRecordMissingTableId\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, admin_status.body, "\"replication_failures_total\":1") != null);
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

test "data runtime background maintenance is due for dense posting cadence without lsm debt" {
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
            "/tmp/unused-antfly-data-runtime-dense-posting-maintenance",
            FakeCatalog.iface(),
            antfly.raft.read_gate.noopReadableLeaseRequester(),
        ),
        .write_source = antfly.public_api.ProvisionedTableWriteSource.init("/tmp/unused-antfly-data-runtime-dense-posting-maintenance", FakeCatalog.iface()),
        .status_source = undefined,
        .api_server_cfg = undefined,
        .query_async_limit = .limited(8),
        .listener_cfg = undefined,
    };
    defer server.deinit();

    try std.testing.expectEqual(@as(u64, 0), server.write_source.lsmMaintenanceScoreBestEffort());

    server.dense_posting_maintenance_next_eligible_ns.store(100, .release);
    try std.testing.expect(server.backgroundMaintenanceDue(100));
    try std.testing.expect(server.backgroundMaintenanceDue(101));
    try std.testing.expect(!server.backgroundMaintenanceDue(99));
}
