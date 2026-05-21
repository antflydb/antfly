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
const metadata_api = @import("api.zig");
const metadata_control_loop = @import("control_loop.zig");
const metadata_http_client = @import("http_client.zig");
const metadata_http_server = @import("http_server.zig");
const metadata_mod = @import("mod.zig");
const metadata_reconcile_lease = @import("reconcile_lease.zig");
const metadata_reconciler = @import("reconciler.zig");
const metadata_service = @import("service.zig");
const metadata_store_observer = @import("store_observer.zig");
const metadata_storage = @import("storage/mod.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_table_workflow = @import("table_workflow.zig");
const api_http_client = @import("../api/http_client.zig");
const api_http_routes = @import("../api/http_routes.zig");
const api_http_server = @import("../api/http_server.zig");
const api_table_catalog = @import("../api/table_catalog.zig");
const api_table_reads = @import("../api/table_reads.zig");
const api_table_router = @import("../api/table_router.zig");
const test_contract_helpers = @import("../api/test_contract_helpers.zig");
const api_table_writes = @import("../api/table_writes.zig");
const api_tables = @import("../api/tables.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const raft_catalog = @import("../raft/catalog.zig");
const raft_host = @import("../raft/host.zig");
const raft_metadata_apply = @import("../raft/metadata_apply.zig");
const raft_metadata_view = @import("../raft/metadata_view.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const raft_state_machine = @import("../raft/state_machine/mod.zig");
const peer_resolver = @import("../raft/peer_resolver.zig");
const raft_sim = @import("../raft/sim_harness.zig");
const raft_transport = @import("../raft/transport/mod.zig");
const transition_runtime = @import("../raft/transition_runtime.zig");
const transition_state = @import("transition_state.zig");
const raft_engine = @import("raft_engine");
const data_mod = @import("../data/mod.zig");
const std_http_executor = @import("../raft/transport/std_http_executor.zig");
const std_http_listener = @import("../raft/transport/std_http_listener.zig");
const docstore_mod = @import("../storage/docstore.zig");
const db_mod = @import("../storage/db/mod.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const platform_clock = @import("../platform/clock.zig");
const platform_time = @import("../platform/time.zig");
const usermgr = @import("../usermgr/mod.zig");
const casbin = @import("antfly_casbin");

const LeanSimAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });
// Public API simulations can open DBs and hosted indexes from the listener
// request thread; keep enough stack for that Linux CI path.
const lean_sim_thread_stack_size = 4 * 1024 * 1024;
const lean_sim_http_listener_cfg = std_http_listener.StdHttpListenerConfig{
    .thread_stack_size = lean_sim_thread_stack_size,
};

const SimSplitRuntime = struct {
    const Entry = struct {
        source_group_id: u64,
        destination_group_id: u64,
        coord: ?*data_mod.SplitSyncCoordinator = null,
        status: data_mod.SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
    };

    entries: [16]Entry = undefined,
    len: usize = 0,
    replica_root_dir: ?[]const u8 = null,

    fn deinit(self: *@This()) void {
        for (self.entries[0..self.len]) |*entry| self.releaseCoordinator(entry);
        self.len = 0;
        self.replica_root_dir = null;
    }

    fn iface(self: *@This()) transition_runtime.SplitRuntime {
        return .{
            .ptr = self,
            .vtable = &.{
                .observe_status = observeStatus,
                .prepare_source = prepareSource,
                .start_source = startSource,
                .bootstrap_destination = bootstrapDestination,
                .catch_up_destination = catchUpDestination,
                .finalize_source = finalizeSource,
                .rollback_source = rollbackSource,
            },
        };
    }

    fn entryFor(self: *@This(), source_group_id: u64, destination_group_id: u64) *Entry {
        for (self.entries[0..self.len]) |*entry| {
            if (entry.source_group_id == source_group_id and entry.destination_group_id == destination_group_id) return entry;
        }
        std.debug.assert(self.len < self.entries.len);
        self.entries[self.len] = .{
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
        };
        self.len += 1;
        return &self.entries[self.len - 1];
    }

    fn releaseCoordinator(self: *@This(), entry: *Entry) void {
        _ = self;
        if (entry.coord) |coord| {
            coord.deinit();
            std.heap.page_allocator.destroy(coord);
            entry.coord = null;
        }
    }

    fn observeStatus(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !data_mod.SplitTransitionStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.replica_root_dir != null) {
            const alloc = std.heap.page_allocator;
            const replica_root_dir = self.replica_root_dir.?;
            const source_root_dir = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, source_group_id);
            defer alloc.free(source_root_dir);
            const destination_root_dir = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, destination_group_id);
            defer alloc.free(destination_root_dir);

            try ensureSourceApplyStoreSeeded(alloc, source_root_dir, source_group_id);

            const status = try data_mod.storage.observeSplitStatus(alloc, .{
                .source_root_dir = source_root_dir,
                .dest_root_dir = destination_root_dir,
                .source_group_id = source_group_id,
                .dest_group_id = destination_group_id,
                .source = .{ .root_dir = source_root_dir },
                .dest = .{
                    .root_dir = destination_root_dir,
                },
            });
            return .{
                .phase = status.phase,
                .source_split_phase = status.source_split_phase,
                .bootstrapped = status.bootstrapped,
                .replay_required = status.replay_required,
                .replay_caught_up = status.replay_caught_up,
                .cutover_ready = status.cutover_ready,
                .destination_ready_for_reads = status.destination_ready_for_reads,
                .source_delta_sequence = status.source_delta_sequence,
                .dest_delta_sequence = status.dest_delta_sequence,
            };
        }
        return self.entryFor(source_group_id, destination_group_id).status;
    }

    fn prepareSource(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64, split_key: []const u8, source_range_end: ?[]const u8) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.replica_root_dir != null) {
            return try self.withCoordinator(source_group_id, destination_group_id, struct {
                fn call(coord: *data_mod.SplitSyncCoordinator, key: []const u8, range_end: ?[]const u8) !bool {
                    return try coord.prepareSourceSplit(key, range_end);
                }
            }.call, .{ split_key, source_range_end });
        }
        const entry = self.entryFor(source_group_id, destination_group_id);
        entry.status.phase = .prepare;
        entry.status.source_split_phase = .prepare;
        return true;
    }

    fn startSource(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.replica_root_dir != null) {
            return try self.withCoordinator(source_group_id, destination_group_id, struct {
                fn call(coord: *data_mod.SplitSyncCoordinator) !bool {
                    return try coord.startSourceSplit();
                }
            }.call, .{});
        }
        const entry = self.entryFor(source_group_id, destination_group_id);
        entry.status.phase = .bootstrap_peer;
        entry.status.source_split_phase = .splitting;
        entry.status.replay_required = true;
        entry.status.source_delta_sequence = 1;
        return true;
    }

    fn bootstrapDestination(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.replica_root_dir != null) {
            return try self.withCoordinator(source_group_id, destination_group_id, struct {
                fn call(coord: *data_mod.SplitSyncCoordinator) !bool {
                    return try coord.ensureBootstrapped();
                }
            }.call, .{});
        }
        const entry = self.entryFor(source_group_id, destination_group_id);
        entry.status.phase = .replay_deltas;
        entry.status.bootstrapped = true;
        entry.status.dest_delta_sequence = entry.status.source_delta_sequence;
        return true;
    }

    fn catchUpDestination(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !usize {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.replica_root_dir != null) {
            return try self.withCoordinator(source_group_id, destination_group_id, struct {
                fn call(coord: *data_mod.SplitSyncCoordinator) !usize {
                    return try coord.catchUp();
                }
            }.call, .{});
        }
        const entry = self.entryFor(source_group_id, destination_group_id);
        entry.status.phase = .cutover_ready;
        entry.status.replay_caught_up = true;
        entry.status.cutover_ready = true;
        entry.status.destination_ready_for_reads = true;
        entry.status.dest_delta_sequence = entry.status.source_delta_sequence;
        return 1;
    }

    fn finalizeSource(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.replica_root_dir != null) {
            const finalized = try (try self.withCoordinator(source_group_id, destination_group_id, struct {
                fn call(coord: *data_mod.SplitSyncCoordinator) !bool {
                    return try coord.finalizeSource();
                }
            }.call, .{}));
            if (finalized) self.releaseCoordinator(self.entryFor(source_group_id, destination_group_id));
            return finalized;
        }
        const entry = self.entryFor(source_group_id, destination_group_id);
        entry.status.phase = .finalized;
        entry.status.source_split_phase = .none;
        entry.status.replay_required = false;
        return true;
    }

    fn rollbackSource(ptr: *anyopaque, source_group_id: u64, destination_group_id: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.replica_root_dir != null) {
            const rolled_back = try (try self.withCoordinator(source_group_id, destination_group_id, struct {
                fn call(coord: *data_mod.SplitSyncCoordinator) !bool {
                    return try coord.rollbackSource();
                }
            }.call, .{}));
            if (rolled_back) self.releaseCoordinator(self.entryFor(source_group_id, destination_group_id));
            return rolled_back;
        }
        const entry = self.entryFor(source_group_id, destination_group_id);
        entry.status.phase = .rolled_back;
        entry.status.source_split_phase = .none;
        entry.status.replay_required = false;
        return true;
    }

    fn withCoordinator(
        self: *@This(),
        source_group_id: u64,
        destination_group_id: u64,
        comptime Func: anytype,
        args: anytype,
    ) !@typeInfo(@TypeOf(Func)).@"fn".return_type.? {
        const entry = self.entryFor(source_group_id, destination_group_id);
        if (entry.coord == null) {
            const alloc = std.heap.page_allocator;
            const replica_root_dir = self.replica_root_dir orelse return error.UnsupportedOperation;
            const source_root_dir = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, source_group_id);
            defer alloc.free(source_root_dir);
            const destination_root_dir = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, destination_group_id);
            defer alloc.free(destination_root_dir);

            try ensureSourceApplyStoreSeeded(alloc, source_root_dir, source_group_id);

            const coord = try alloc.create(data_mod.SplitSyncCoordinator);
            errdefer alloc.destroy(coord);
            var dest_db_options = db_mod.OpenOptions{};
            if (try splitDestinationIdentityNamespaceFromSource(alloc, source_root_dir, destination_group_id)) |namespace| {
                dest_db_options.identity_namespace = namespace;
            }
            coord.* = try data_mod.SplitSyncCoordinator.init(alloc, .{
                .source_root_dir = source_root_dir,
                .dest_root_dir = destination_root_dir,
                .source_group_id = source_group_id,
                .dest_group_id = destination_group_id,
                .dest = .{ .root_dir = destination_root_dir, .db = dest_db_options },
            });
            entry.coord = coord;
        }
        return @call(.auto, Func, .{entry.coord.?} ++ args);
    }

    fn splitDestinationIdentityNamespaceFromSource(
        alloc: std.mem.Allocator,
        source_root_dir: []const u8,
        destination_group_id: u64,
    ) !?db_mod.DocIdentityNamespace {
        _ = destination_group_id;
        var db = db_mod.DB.open(alloc, source_root_dir, .{
            .open_mode = .status_only,
            .start_index_workers = false,
        }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer db.close();

        const stats = try db.runtimeStatusStatsConsistent(alloc);
        if (stats.doc_identity.namespace_table_id == 0) return null;
        return .{
            .table_id = stats.doc_identity.namespace_table_id,
            .shard_id = stats.doc_identity.namespace_shard_id,
            .range_id = stats.doc_identity.namespace_range_id,
        };
    }

    fn ensureSourceApplyStoreSeeded(
        alloc: std.mem.Allocator,
        source_root_dir: []const u8,
        source_group_id: u64,
    ) !void {
        var source_store = try data_mod.RaftApplyStore.init(alloc, .{ .root_dir = source_root_dir });
        defer source_store.deinit();
        if ((try source_store.latestBatch(source_group_id)) != null) return;

        var db = db_mod.DB.open(alloc, source_root_dir, .{}) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer db.close();

        var ops = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (ops.items) |op| alloc.free(op);
            ops.deinit(alloc);
        }

        const byte_range = db.getRange();
        try ops.append(alloc, try std.fmt.allocPrint(alloc, "range:{s}:{s}", .{
            byte_range.start,
            byte_range.end,
        }));

        const lower = try internal_keys.documentRangeLowerAlloc(alloc, byte_range.start);
        defer alloc.free(lower);
        const upper = if (byte_range.end.len > 0) try internal_keys.documentRangeUpperAlloc(alloc, byte_range.end) else null;
        defer if (upper) |owned| alloc.free(owned);

        const scanned = try db.core.store.scanRange(alloc, lower, if (upper) |owned| owned else "");
        defer docstore_mod.DocStore.freeResults(alloc, scanned);
        for (scanned) |entry| {
            const raw_key = (try internal_keys.decodePrimaryDocumentKeyAlloc(alloc, entry.key)) orelse continue;
            defer alloc.free(raw_key);
            try ops.append(alloc, try std.fmt.allocPrint(alloc, "put:{s}={s}", .{
                raw_key,
                entry.value,
            }));
        }

        const entries = try alloc.alloc(raft_engine.core.Entry, ops.items.len);
        defer alloc.free(entries);
        for (ops.items, 0..) |op, i| {
            entries[i] = .{
                .term = 1,
                .index = i + 1,
                .entry_type = .normal,
                .data = op,
            };
        }
        const encoded = try raft_state_machine.encodeCommittedEntries(alloc, entries);
        defer alloc.free(encoded);
        try source_store.snapshotBuilder().applyBatch(.{
            .group_id = source_group_id,
            .commit_index = entries.len,
            .entries_bytes = encoded,
        });
    }
};

test "metadata sim split runtime preserves source identity namespace" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root_dir = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-sim-split-identity", .{tmp.sub_path});
    defer alloc.free(replica_root_dir);
    const source_root_dir = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 701);
    defer alloc.free(source_root_dir);
    const destination_root_dir = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, 702);
    defer alloc.free(destination_root_dir);

    const source_namespace = db_mod.DocIdentityNamespace{
        .table_id = 70,
        .shard_id = 701,
        .range_id = 9001,
    };

    {
        var db = try db_mod.DB.open(alloc, source_root_dir, .{
            .identity_namespace = source_namespace,
            .start_index_workers = false,
        });
        defer db.close();
        try db.updateRange(.{ .start = "doc:a", .end = "doc:z" });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:t", .value = "{\"v\":\"right\"}" }},
        });
    }

    var runtime = SimSplitRuntime{ .replica_root_dir = replica_root_dir };
    defer runtime.deinit();
    var split = runtime.iface();

    try std.testing.expect(try split.prepareSource(701, 702, "doc:m", "doc:z"));
    try std.testing.expect(try split.startSource(701, 702));
    try std.testing.expect(try split.bootstrapDestination(701, 702));
    _ = try split.catchUpDestination(701, 702);

    var dest = try db_mod.DB.open(alloc, destination_root_dir, .{
        .identity_namespace = source_namespace,
        .start_index_workers = false,
    });
    defer dest.close();
    const value = (try dest.get(alloc, "doc:t")) orelse return error.TestUnexpectedResult;
    defer alloc.free(value);
    try std.testing.expectEqualStrings("{\"v\":\"right\"}", value);

    const stats = try dest.runtimeStatusStatsConsistent(alloc);
    try std.testing.expectEqual(source_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(source_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(source_namespace.range_id, stats.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.allocated_ordinals);
    try std.testing.expect(!stats.doc_identity.rebuild_required);
}

const EnsureGroupTextIndexProgressContext = struct {
    replica_root_dir: []const u8,
    group_id: u64,
    index_name: []const u8,
};

fn projectedIdentityNamespaceForGroup(
    cluster: *MetadataHttpClusterSimulation,
    group_id: u64,
) !?db_mod.DocIdentityNamespace {
    const preferred_index = currentMetadataLeaderIndex(cluster);
    for (0..cluster.cluster.nodes.len) |offset| {
        const index = if (offset == 0 and preferred_index != null)
            preferred_index.?
        else if (preferred_index != null and offset <= preferred_index.?)
            offset - 1
        else
            offset;
        if (index >= cluster.cluster.nodes.len) continue;
        const ranges = try cluster.node(index).listProjectedRanges(cluster.alloc);
        defer cluster.node(index).freeProjectedRanges(cluster.alloc, ranges);
        for (ranges) |range| {
            if (range.group_id != group_id) continue;
            return .{
                .table_id = range.table_id,
                .shard_id = metadata_table_manager.rangeDocIdentityShardId(range),
                .range_id = metadata_table_manager.rangeDocIdentityRangeId(range),
            };
        }
    }
    return null;
}

fn ensureGroupTextIndexProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *EnsureGroupTextIndexProgressContext = @ptrCast(@alignCast(ptr));
    const path = try metadata_mod.groupDbPathFromReplicaRoot(cluster.alloc, ctx.replica_root_dir, ctx.group_id);
    defer cluster.alloc.free(path);
    const identity_namespace = try projectedIdentityNamespaceForGroup(cluster, ctx.group_id);

    var db = db_mod.DB.open(cluster.alloc, path, .{
        .identity_namespace = identity_namespace,
    }) catch |err| switch (err) {
        error.PathAlreadyExists, error.FileNotFound => return false,
        else => return err,
    };
    defer db.close();

    if (db.core.index_manager.textIndex(ctx.index_name) == null) {
        try db.addIndex(.{
            .name = ctx.index_name,
            .kind = .full_text,
            .config_json = "{}",
        });
    }
    return true;
}

fn ensureGroupTextIndex(
    cluster: *MetadataHttpClusterSimulation,
    replica_root_dir: []const u8,
    group_id: u64,
    index_name: []const u8,
    max_rounds: usize,
) !void {
    var ctx = EnsureGroupTextIndexProgressContext{
        .replica_root_dir = replica_root_dir,
        .group_id = group_id,
        .index_name = index_name,
    };
    if (try cluster.runUntil(max_rounds, &ctx, ensureGroupTextIndexProgressPredicate)) return;
    return error.FileNotFound;
}

fn ensureGroupTextIndexOnActiveReplicas(
    cluster: *MetadataHttpClusterSimulation,
    replica_root_dirs: []const []const u8,
    group_id: u64,
    index_name: []const u8,
    max_rounds: usize,
) !void {
    var ensured: usize = 0;
    for (0..cluster.cluster.nodes.len) |i| {
        if (cluster.node(i).status(group_id) != .active) continue;
        try ensureGroupTextIndex(cluster, replica_root_dirs[i], group_id, index_name, max_rounds);
        ensured += 1;
    }
    if (ensured == 0) return error.TestExpectedEqual;
}

fn runtimeDocIdentityStatusReportFromStats(
    stats: db_mod.types.DocIdentityStats,
) metadata_table_manager.RuntimeDocIdentityStatusReport {
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

fn reportRuntimeDocIdentityForActiveReplicas(
    cluster: *MetadataHttpClusterSimulation,
    node: anytype,
    replica_root_dirs: []const []const u8,
    table_name: []const u8,
    group_ids: []const u64,
) !void {
    const alloc = std.testing.allocator;
    var reports = std.ArrayListUnmanaged(metadata_table_manager.StoreStatusReport).empty;
    defer {
        for (reports.items) |report| {
            metadata_table_manager.freeGroupStatuses(alloc, report.group_statuses);
            metadata_table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
        }
        reports.deinit(alloc);
    }

    for (0..cluster.cluster.nodes.len) |i| {
        var group_statuses = std.ArrayListUnmanaged(metadata_table_manager.GroupStatusReport).empty;
        defer group_statuses.deinit(alloc);
        var runtime_statuses = std.ArrayListUnmanaged(metadata_table_manager.RuntimeGroupStatusReport).empty;
        defer {
            for (runtime_statuses.items) |status| metadata_table_manager.freeRuntimeGroupStatusReport(alloc, status);
            runtime_statuses.deinit(alloc);
        }

        for (group_ids) |group_id| {
            if (cluster.node(i).status(group_id) != .active) continue;
            const path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dirs[i], group_id);
            defer alloc.free(path);
            var db = db_mod.DB.open(alloc, path, .{
                .open_mode = .status_only,
                .start_index_workers = false,
            }) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            defer db.close();
            const stats = try db.runtimeStatusStatsConsistent(alloc);
            defer db_mod.types.freeDBStats(alloc, stats);
            const now_ms = currentGroupStatusTimestampMs();
            try group_statuses.append(alloc, .{
                .group_id = group_id,
                .doc_count = stats.doc_count,
                .disk_bytes = 1,
                .empty = stats.doc_count == 0,
                .updated_at_millis = now_ms,
                .local_leader = currentGroupLeaderIndex(cluster, group_id) == i,
                .local_voter = true,
                .voter_count = 1,
            });
            try runtime_statuses.append(alloc, .{
                .table_id = stats.doc_identity.namespace_table_id,
                .table_name = try alloc.dupe(u8, table_name),
                .group_id = group_id,
                .store_id = @intCast(i + 1),
                .node_id = @intCast(i + 1),
                .updated_at_ns = now_ms * std.time.ns_per_ms,
                .source = try alloc.dupe(u8, "metadata-sim"),
                .freshness = try alloc.dupe(u8, "fresh"),
                .doc_count = stats.doc_count,
                .disk_bytes = 1,
                .created_at_millis = now_ms,
                .index_count = stats.index_count,
                .doc_identity = runtimeDocIdentityStatusReportFromStats(stats.doc_identity),
            });
        }

        if (runtime_statuses.items.len == 0 and group_statuses.items.len == 0) continue;
        const owned_group_statuses = try group_statuses.toOwnedSlice(alloc);
        errdefer metadata_table_manager.freeGroupStatuses(alloc, owned_group_statuses);
        const owned_runtime_statuses = try runtime_statuses.toOwnedSlice(alloc);
        errdefer metadata_table_manager.freeRuntimeGroupStatusReports(alloc, owned_runtime_statuses);
        try reports.append(alloc, .{
            .store_id = @intCast(i + 1),
            .live = true,
            .health_class = "healthy",
            .capacity_bytes = 1024,
            .available_bytes = 900,
            .group_statuses = owned_group_statuses,
            .runtime_statuses = owned_runtime_statuses,
        });
    }

    if (reports.items.len == 0) return error.TestExpectedEqual;
    try std.testing.expectEqual(reports.items.len, try node.reportStoreStatuses(reports.items));
}

fn seedGroupDocsAcrossReplicaRoots(
    cluster: *MetadataHttpClusterSimulation,
    replica_root_dirs: []const []const u8,
    group_id: u64,
    writes: []const db_mod.types.BatchWrite,
) !void {
    for (replica_root_dirs) |replica_root_dir| {
        const path = try metadata_mod.groupDbPathFromReplicaRoot(cluster.alloc, replica_root_dir, group_id);
        defer cluster.alloc.free(path);

        var db = db_mod.DB.open(cluster.alloc, path, .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        }) catch |err| switch (err) {
            error.PathAlreadyExists, error.FileNotFound => continue,
            else => return err,
        };
        defer db.close();

        try db.batch(.{ .writes = writes });
    }
}

fn seedDefaultSplitCandidateDocs(
    cluster: *MetadataHttpClusterSimulation,
    node: MetadataHttpNodeSimulation,
    replica_root_dirs: []const []const u8,
    group_id: u64,
    max_rounds: usize,
) !void {
    try seedGroupDocsAcrossReplicaRoots(cluster, replica_root_dirs, group_id, &.{
        .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"left\"}" },
        .{ .key = "doc:m", .value = "{\"title\":\"mid\",\"body\":\"middle\"}" },
        .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"body\":\"right\"}" },
    });
    try std.testing.expect(try waitForMedianKeyEquals(cluster, node, group_id, "doc:m", max_rounds));
}

fn expectCountProfile(
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    query_text: []const u8,
    expected_total_hits: i64,
    expected_shards: i64,
    expected_merged: bool,
) !void {
    const query_json = try std.fmt.allocPrint(std.testing.allocator,
        \\{{"full_text_search":{{"match":{{"field":"body","text":"{s}"}}}},"count":true,"profile":true,"limit":10}}
    , .{query_text});
    defer std.testing.allocator.free(query_json);

    var count_profile_query = try client.fetchQuery(
        client_base,
        table_name,
        query_json,
    );
    defer count_profile_query.deinit(std.heap.page_allocator);
    var count_profile_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, count_profile_query.body, .{});
    defer count_profile_responses.deinit();
    const count_profile_result = count_profile_responses.value.responses.?[0];
    try std.testing.expectEqual(expected_total_hits, count_profile_result.hits.?.total.?);
    try std.testing.expectEqual(@as(usize, 0), count_profile_result.hits.?.hits.?.len);
    try std.testing.expect(count_profile_result.profile != null);
    try std.testing.expectEqual(expected_shards, count_profile_result.profile.?.object.get("shards").?.object.get("total").?.integer);
    try std.testing.expectEqual(expected_merged, count_profile_result.profile.?.object.get("merge") != null);
}

fn expectHelloCountProfile(
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    expected_total_hits: i64,
    expected_shards: i64,
    expected_merged: bool,
) !void {
    try expectCountProfile(client, client_base, table_name, "hello", expected_total_hits, expected_shards, expected_merged);
}

const CountProfileProgressContext = struct {
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    query_text: []const u8,
    expected_total_hits: i64,
    expected_shards: i64,
    expected_merged: bool,
};

fn countProfileProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    _ = cluster;
    const ctx: *CountProfileProgressContext = @ptrCast(@alignCast(ptr));
    expectCountProfile(ctx.client, ctx.client_base, ctx.table_name, ctx.query_text, ctx.expected_total_hits, ctx.expected_shards, ctx.expected_merged) catch |err| switch (err) {
        error.TestExpectedEqual, error.TestUnexpectedResult, error.UnexpectedHttpStatus => return false,
        else => return err,
    };
    return true;
}

fn waitForCountProfile(
    cluster: *MetadataHttpClusterSimulation,
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    query_text: []const u8,
    expected_total_hits: i64,
    expected_shards: i64,
    expected_merged: bool,
    max_rounds: usize,
) !bool {
    var ctx = CountProfileProgressContext{
        .client = client,
        .client_base = client_base,
        .table_name = table_name,
        .query_text = query_text,
        .expected_total_hits = expected_total_hits,
        .expected_shards = expected_shards,
        .expected_merged = expected_merged,
    };
    return try cluster.runUntil(max_rounds, &ctx, countProfileProgressPredicate);
}

fn waitForHelloCountProfile(
    cluster: *MetadataHttpClusterSimulation,
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    expected_total_hits: i64,
    expected_shards: i64,
    expected_merged: bool,
    max_rounds: usize,
) !bool {
    return try waitForCountProfile(cluster, client, client_base, table_name, "hello", expected_total_hits, expected_shards, expected_merged, max_rounds);
}

const LookupContainsProgressContext = struct {
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    key: []const u8,
    needle: []const u8,
};

fn lookupContainsProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    _ = cluster;
    const ctx: *LookupContainsProgressContext = @ptrCast(@alignCast(ptr));
    if (ctx.client.fetchLookup(ctx.client_base, ctx.table_name, ctx.key, null)) |lookup| {
        var owned_lookup = lookup;
        defer owned_lookup.deinit(std.heap.page_allocator);
        return std.mem.indexOf(u8, owned_lookup.body, ctx.needle) != null;
    } else |err| switch (err) {
        error.UnexpectedHttpStatus => return false,
        else => return err,
    }
}

fn waitForLookupContains(
    cluster: *MetadataHttpClusterSimulation,
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    key: []const u8,
    needle: []const u8,
    max_rounds: usize,
) !bool {
    var ctx = LookupContainsProgressContext{
        .client = client,
        .client_base = client_base,
        .table_name = table_name,
        .key = key,
        .needle = needle,
    };
    return try cluster.runUntil(max_rounds, &ctx, lookupContainsProgressPredicate);
}

const QueryContainsAllProgressContext = struct {
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    body: []const u8,
    needles: []const []const u8,
};

fn queryContainsAllProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    _ = cluster;
    const ctx: *QueryContainsAllProgressContext = @ptrCast(@alignCast(ptr));
    if (ctx.client.fetchQuery(ctx.client_base, ctx.table_name, ctx.body)) |query| {
        var owned_query = query;
        defer owned_query.deinit(std.heap.page_allocator);
        expectBodyContainsAll(owned_query.body, ctx.needles) catch |err| switch (err) {
            error.TestUnexpectedResult => return false,
        };
        return true;
    } else |err| switch (err) {
        error.UnexpectedHttpStatus => return false,
        else => return err,
    }
}

fn waitForQueryContainsAll(
    cluster: *MetadataHttpClusterSimulation,
    client: *api_http_client.ApiHttpClient,
    client_base: []const u8,
    table_name: []const u8,
    body: []const u8,
    needles: []const []const u8,
    max_rounds: usize,
) !bool {
    var ctx = QueryContainsAllProgressContext{
        .client = client,
        .client_base = client_base,
        .table_name = table_name,
        .body = body,
        .needles = needles,
    };
    return try cluster.runUntil(max_rounds, &ctx, queryContainsAllProgressPredicate);
}

const MedianKeyEqualsProgressContext = struct {
    node: MetadataHttpNodeSimulation,
    group_id: u64,
    expected_key: []const u8,
};

fn medianKeyEqualsProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    _ = cluster;
    const ctx: *MedianKeyEqualsProgressContext = @ptrCast(@alignCast(ptr));
    const lookup = ctx.node.medianKeyLookup() orelse return false;
    const actual = lookup.fetchMedianKey(std.testing.allocator, ctx.group_id) catch |err| switch (err) {
        error.UnknownGroup, error.UnsupportedOperation => null,
        else => return err,
    };
    if (actual) |owned_key| {
        defer std.testing.allocator.free(owned_key);
        return std.mem.eql(u8, owned_key, ctx.expected_key);
    }
    return false;
}

fn waitForMedianKeyEquals(
    cluster: *MetadataHttpClusterSimulation,
    node: MetadataHttpNodeSimulation,
    group_id: u64,
    expected_key: []const u8,
    max_rounds: usize,
) !bool {
    var ctx = MedianKeyEqualsProgressContext{
        .node = node,
        .group_id = group_id,
        .expected_key = expected_key,
    };
    return try cluster.runUntil(max_rounds, &ctx, medianKeyEqualsProgressPredicate);
}

fn mirrorGroupBatchToActiveReplicas(
    cluster: *MetadataHttpClusterSimulation,
    client: *api_http_client.ApiHttpClient,
    api_base_uris: []const []const u8,
    group_id: u64,
    table_name: []const u8,
    body: []const u8,
) !void {
    for (api_base_uris, 0..) |base_uri, i| {
        if (cluster.node(i).status(group_id) != .active) continue;
        var response = try client.fetchGroupBatch(base_uri, group_id, table_name, body);
        defer response.deinit(std.heap.page_allocator);
    }
}

const split_seed_batch_body =
    \\{"inserts":{"doc:a":{"title":"alpha","body":"hello left side","status":"published"},"doc:m":{"title":"mid","body":"hello middle side","status":"published"},"doc:z":{"title":"zeta","body":"hello right side","status":"published"}}}
;
const split_left_cutover_body =
    \\{"deletes":["doc:m","doc:z"],"sync_level":"full_index"}
;
const split_right_cutover_body =
    \\{"inserts":{"doc:m":{"title":"mid","body":"hello middle side","status":"published"},"doc:z":{"title":"zeta","body":"hello right side","status":"published"}},"sync_level":"full_index"}
;
const split_post_batch_body =
    \\{"inserts":{"doc:b":{"title":"beta","body":"hello left beta","status":"published"},"doc:y":{"title":"gamma","body":"hello right gamma","status":"published"}}}
;
const split_left_post_batch_body =
    \\{"inserts":{"doc:b":{"title":"beta","body":"hello left beta","status":"published"}},"sync_level":"full_index"}
;
const split_right_post_batch_body =
    \\{"inserts":{"doc:y":{"title":"gamma","body":"hello right gamma","status":"published"}},"sync_level":"full_index"}
;
const merge_seed_left_batch_body =
    \\{"inserts":{"doc:a":{"title":"alpha","body":"hello left side","status":"published"}}}
;
const merge_seed_right_batch_body =
    \\{"inserts":{"doc:z":{"title":"zeta","body":"hello right side","status":"published"}}}
;
const merge_seed_batch_body =
    \\{"inserts":{"doc:a":{"title":"alpha","body":"hello left side","status":"published"},"doc:z":{"title":"zeta","body":"hello right side","status":"published"}}}
;
const merge_post_batch_body =
    \\{"inserts":{"doc:y":{"title":"gamma","body":"hello merged gamma","status":"published"}}}
;
const shared_hello_query_body =
    \\{"full_text_search":{"match":{"field":"body","text":"hello"}},"fields":["title","body"],"limit":10}
;

const split_query_needles = [_][]const u8{
    "\"_id\":\"doc:a\"",
    "\"_id\":\"doc:m\"",
    "\"_id\":\"doc:z\"",
    "\"_id\":\"doc:b\"",
    "\"_id\":\"doc:y\"",
};
const split_query_needles_without_mid = [_][]const u8{
    "\"_id\":\"doc:a\"",
    "\"_id\":\"doc:z\"",
    "\"_id\":\"doc:b\"",
    "\"_id\":\"doc:y\"",
};
const merge_query_needles = [_][]const u8{
    "\"_id\":\"doc:a\"",
    "\"_id\":\"doc:z\"",
    "\"_id\":\"doc:y\"",
};

const SplitResolvedGroups = struct {
    left_group: u64,
    right_group: u64,
};

const SplitPublicVerificationConfig = struct {
    route_rounds: usize,
    active_rounds: usize,
    leader_rounds: usize,
    lookup_rounds: usize,
    count_profile_rounds: ?usize = null,
    left_active_count: usize = 3,
    right_active_count: usize = 3,
    expect_mid_doc: bool = true,
};

const MergePublicVerificationConfig = struct {
    active_rounds: usize,
    absent_rounds: usize,
    route_rounds: usize,
    leader_rounds: usize,
    lookup_rounds: usize,
    merged_active_count: usize = 3,
    removed_absent_count: usize = 3,
    expect_profile: bool = false,
};

const AutomaticSplitPublicTrafficFailureMode = enum {
    none,
    restart_metadata_leader,
    restart_source_group_leader,
    partition_metadata_leader,
};

const AutomaticSplitPublicTrafficScenario = struct {
    table_id: u64,
    path_prefix: []const u8,
    description: []const u8,
    delayed_transport: bool,
    bootstrap_rounds: usize,
    range_create_rounds: usize,
    projected_index_rounds: usize,
    finalize_rounds: usize,
    post_failure_leader_wait_rounds: usize = 0,
    ensure_source_group_text_index_before_transition: bool = false,
    failure_mode: AutomaticSplitPublicTrafficFailureMode = .none,
    verify: SplitPublicVerificationConfig,
};

const AutomaticMergePublicTrafficFailureMode = enum {
    none,
    restart_metadata_leader,
    restart_donor_group_leader,
    partition_metadata_leader,
};

const AutomaticMergePublicTrafficScenario = struct {
    table_id: u64,
    path_prefix: []const u8,
    description: []const u8,
    delayed_transport: bool,
    bootstrap_rounds: usize,
    range_create_rounds: usize,
    projected_index_rounds: usize,
    finalize_rounds: usize,
    post_failure_leader_wait_rounds: usize = 0,
    failure_mode: AutomaticMergePublicTrafficFailureMode = .none,
    verify: MergePublicVerificationConfig,
};

fn expectBodyContainsAll(body: []const u8, needles: []const []const u8) !void {
    for (needles) |needle| {
        try std.testing.expect(std.mem.indexOf(u8, body, needle) != null);
    }
}

fn waitForSplitResolvedGroups(
    cluster: *MetadataHttpClusterSimulation,
    catalog: api_table_catalog.CatalogSource,
    table_name: []const u8,
    route_rounds: usize,
) !SplitResolvedGroups {
    var ctx = SplitResolvedGroupsProgressContext{
        .catalog = catalog,
        .table_name = table_name,
    };
    try std.testing.expect(try cluster.runUntil(route_rounds, &ctx, splitResolvedGroupsProgressPredicate));
    return ctx.result;
}

const SplitResolvedGroupsProgressContext = struct {
    catalog: api_table_catalog.CatalogSource,
    table_name: []const u8,
    result: SplitResolvedGroups = .{ .left_group = 0, .right_group = 0 },
};

fn splitResolvedGroupsProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    _ = cluster;
    const ctx: *SplitResolvedGroupsProgressContext = @ptrCast(@alignCast(ptr));
    ctx.result.left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, ctx.catalog, ctx.table_name, "doc:a")) orelse 0;
    ctx.result.right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, ctx.catalog, ctx.table_name, "doc:z")) orelse 0;
    return ctx.result.left_group != 0 and ctx.result.right_group != 0 and ctx.result.left_group != ctx.result.right_group;
}

const ResolvedGroupForKeyProgressContext = struct {
    catalog: api_table_catalog.CatalogSource,
    table_name: []const u8,
    key: []const u8,
    expected_group_id: u64,
    resolved_group_id: ?u64 = null,
};

fn resolvedGroupForKeyProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    _ = cluster;
    const ctx: *ResolvedGroupForKeyProgressContext = @ptrCast(@alignCast(ptr));
    ctx.resolved_group_id = try api_table_catalog.resolveGroupForKey(std.testing.allocator, ctx.catalog, ctx.table_name, ctx.key);
    return ctx.resolved_group_id == ctx.expected_group_id;
}

fn waitForResolvedGroupForKey(
    cluster: *MetadataHttpClusterSimulation,
    catalog: api_table_catalog.CatalogSource,
    table_name: []const u8,
    key: []const u8,
    expected_group_id: u64,
    route_rounds: usize,
) !u64 {
    var ctx = ResolvedGroupForKeyProgressContext{
        .catalog = catalog,
        .table_name = table_name,
        .key = key,
        .expected_group_id = expected_group_id,
    };
    try std.testing.expect(try cluster.runUntil(route_rounds, &ctx, resolvedGroupForKeyProgressPredicate));
    return ctx.resolved_group_id orelse return error.TestExpectedEqual;
}

const FirstProjectedRangeProgressContext = struct {
    fallback_index: usize,
    expected_range_count: ?usize = null,
    expected_active_count: ?usize = null,
    require_non_host: bool = false,
    table_id: u64 = 0,
    group_id: u64 = 0,
    active_count: usize = 0,
    non_host_index: ?usize = null,
};

fn firstProjectedRangeProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *FirstProjectedRangeProgressContext = @ptrCast(@alignCast(ptr));
    const query_index = currentMetadataLeaderIndex(cluster) orelse ctx.fallback_index;
    const projected_ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, projected_ranges);
    if (projected_ranges.len == 0) return false;
    if (ctx.expected_range_count) |expected| {
        if (projected_ranges.len != expected) return false;
    }

    ctx.table_id = projected_ranges[0].table_id;
    ctx.group_id = projected_ranges[0].group_id;
    ctx.active_count = 0;
    ctx.non_host_index = null;
    for (0..cluster.cluster.nodes.len) |i| {
        if (cluster.node(i).status(ctx.group_id) == .active) {
            ctx.active_count += 1;
        } else if (ctx.non_host_index == null) {
            ctx.non_host_index = i;
        }
    }
    if (ctx.expected_active_count) |expected| {
        if (ctx.active_count != expected) return false;
    }
    if (ctx.require_non_host and ctx.non_host_index == null) return false;
    return true;
}

fn waitForFirstProjectedRange(
    cluster: *MetadataHttpClusterSimulation,
    fallback_index: usize,
    expected_range_count: ?usize,
    expected_active_count: ?usize,
    require_non_host: bool,
    max_rounds: usize,
) !FirstProjectedRangeProgressContext {
    var ctx = FirstProjectedRangeProgressContext{
        .fallback_index = fallback_index,
        .expected_range_count = expected_range_count,
        .expected_active_count = expected_active_count,
        .require_non_host = require_non_host,
    };
    try std.testing.expect(try cluster.runUntil(max_rounds, &ctx, firstProjectedRangeProgressPredicate));
    return ctx;
}

const PublicSplitRouteProgressContext = struct {
    catalog_sources: []PublicApiCatalogSource,
    table_name: []const u8,
    fallback_index: usize,
    left_group: u64 = 0,
    right_group: u64 = 0,
    client_index: ?usize = null,
};

fn publicSplitRouteProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *PublicSplitRouteProgressContext = @ptrCast(@alignCast(ptr));
    const query_index = currentMetadataLeaderIndex(cluster) orelse ctx.fallback_index;
    if (query_index >= ctx.catalog_sources.len) return false;
    const catalog = ctx.catalog_sources[query_index].iface();
    ctx.left_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog, ctx.table_name, "doc:a")) orelse 0;
    ctx.right_group = (try api_table_catalog.resolveGroupForKey(std.testing.allocator, catalog, ctx.table_name, "doc:z")) orelse 0;
    if (ctx.left_group == 0 or ctx.right_group == 0 or ctx.left_group == ctx.right_group) return false;

    ctx.client_index = null;
    for (0..cluster.cluster.nodes.len) |i| {
        if (cluster.node(i).status(ctx.left_group) != .active or cluster.node(i).status(ctx.right_group) != .active) {
            ctx.client_index = i;
            return true;
        }
    }
    return false;
}

fn waitForPublicSplitRoute(
    cluster: *MetadataHttpClusterSimulation,
    catalog_sources: []PublicApiCatalogSource,
    table_name: []const u8,
    fallback_index: usize,
    max_rounds: usize,
) !PublicSplitRouteProgressContext {
    var ctx = PublicSplitRouteProgressContext{
        .catalog_sources = catalog_sources,
        .table_name = table_name,
        .fallback_index = fallback_index,
    };
    try std.testing.expect(try cluster.runUntil(max_rounds, &ctx, publicSplitRouteProgressPredicate));
    return ctx;
}

const PublicMergedRouteProgressContext = struct {
    catalog_source: api_table_catalog.CatalogSource,
    table_name: []const u8,
    key: []const u8,
    expected_group_id: u64,
    client_index: ?usize = null,
};

fn publicMergedRouteProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *PublicMergedRouteProgressContext = @ptrCast(@alignCast(ptr));
    const merged_group = try api_table_catalog.resolveGroupForKey(std.testing.allocator, ctx.catalog_source, ctx.table_name, ctx.key);
    if (merged_group != ctx.expected_group_id) return false;
    ctx.client_index = null;
    for (0..cluster.cluster.nodes.len) |i| {
        if (cluster.node(i).status(ctx.expected_group_id) != .active) {
            ctx.client_index = i;
            return true;
        }
    }
    return false;
}

fn waitForPublicMergedRoute(
    cluster: *MetadataHttpClusterSimulation,
    catalog_source: api_table_catalog.CatalogSource,
    table_name: []const u8,
    key: []const u8,
    expected_group_id: u64,
    max_rounds: usize,
) !usize {
    var ctx = PublicMergedRouteProgressContext{
        .catalog_source = catalog_source,
        .table_name = table_name,
        .key = key,
        .expected_group_id = expected_group_id,
    };
    try std.testing.expect(try cluster.runUntil(max_rounds, &ctx, publicMergedRouteProgressPredicate));
    return ctx.client_index orelse return error.TestExpectedEqual;
}

const SingleActiveGroupHostProgressContext = struct {
    group_id: u64,
    host_index: ?usize = null,
    active_count: usize = 0,
};

fn singleActiveGroupHostProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *SingleActiveGroupHostProgressContext = @ptrCast(@alignCast(ptr));
    ctx.host_index = null;
    ctx.active_count = 0;
    for (0..cluster.cluster.nodes.len) |i| {
        if (cluster.node(i).status(ctx.group_id) == .active) {
            ctx.host_index = i;
            ctx.active_count += 1;
        }
    }
    return ctx.active_count == 1;
}

fn waitForSingleActiveGroupHost(
    cluster: *MetadataHttpClusterSimulation,
    group_id: u64,
    max_rounds: usize,
) !usize {
    var ctx = SingleActiveGroupHostProgressContext{ .group_id = group_id };
    try std.testing.expect(try cluster.runUntil(max_rounds, &ctx, singleActiveGroupHostProgressPredicate));
    return ctx.host_index orelse return error.TestExpectedEqual;
}

fn verifySplitPublicTraffic(
    cluster: *MetadataHttpClusterSimulation,
    client: *api_http_client.ApiHttpClient,
    api_base_uris: []const []const u8,
    catalog: api_table_catalog.CatalogSource,
    client_base: []const u8,
    table_name: []const u8,
    roots: []const []const u8,
    cfg: SplitPublicVerificationConfig,
) !void {
    const groups = try waitForSplitResolvedGroups(cluster, catalog, table_name, cfg.route_rounds);
    try std.testing.expect(try cluster.waitForGroupStatusCount(groups.left_group, .active, cfg.left_active_count, cfg.active_rounds));
    try std.testing.expect(try cluster.waitForGroupStatusCount(groups.right_group, .active, cfg.right_active_count, cfg.active_rounds));

    // The public batch is forwarded through the pre-split route map, so repair the per-group mirrors
    // before validating post-split reads.
    try mirrorGroupBatchToActiveReplicas(cluster, client, api_base_uris, groups.left_group, table_name, split_left_cutover_body);
    try mirrorGroupBatchToActiveReplicas(cluster, client, api_base_uris, groups.right_group, table_name, split_right_cutover_body);

    _ = (try waitForGroupLeaderIndex(cluster, groups.left_group, cfg.leader_rounds)) orelse return error.TestExpectedEqual;
    _ = (try waitForGroupLeaderIndex(cluster, groups.right_group, cfg.leader_rounds)) orelse return error.TestExpectedEqual;
    try ensureGroupTextIndexOnActiveReplicas(cluster, roots, groups.left_group, api_tables.default_full_text_index_name, 40);
    try ensureGroupTextIndexOnActiveReplicas(cluster, roots, groups.right_group, api_tables.default_full_text_index_name, 40);
    const status_index = currentMetadataLeaderIndex(cluster) orelse 0;
    const split_groups = [_]u64{ groups.left_group, groups.right_group };
    try reportRuntimeDocIdentityForActiveReplicas(cluster, cluster.node(status_index), roots, table_name, split_groups[0..]);
    try cluster.stepAll();

    try std.testing.expect(try waitForLookupContains(cluster, client, client_base, table_name, "doc:a", "\"alpha\"", cfg.lookup_rounds));
    try std.testing.expect(try waitForLookupContains(cluster, client, client_base, table_name, "doc:z", "\"zeta\"", cfg.lookup_rounds));

    var post_split_batch = try client.fetchBatch(client_base, table_name, split_post_batch_body);
    defer post_split_batch.deinit(std.heap.page_allocator);
    try expectBodyContainsAll(post_split_batch.body, &.{"\"inserted\":2"});
    try mirrorGroupBatchToActiveReplicas(cluster, client, api_base_uris, groups.left_group, table_name, split_left_post_batch_body);
    try mirrorGroupBatchToActiveReplicas(cluster, client, api_base_uris, groups.right_group, table_name, split_right_post_batch_body);

    try std.testing.expect(try waitForQueryContainsAll(
        cluster,
        client,
        client_base,
        table_name,
        shared_hello_query_body,
        if (cfg.expect_mid_doc) split_query_needles[0..] else split_query_needles_without_mid[0..],
        cfg.lookup_rounds,
    ));

    if (cfg.count_profile_rounds) |count_rounds| {
        try std.testing.expect(try waitForHelloCountProfile(cluster, client, client_base, table_name, 5, 2, true, count_rounds));
    }
}

fn verifyMergePublicTraffic(
    cluster: *MetadataHttpClusterSimulation,
    client: *api_http_client.ApiHttpClient,
    api_base_uris: []const []const u8,
    catalog: api_table_catalog.CatalogSource,
    client_base: []const u8,
    table_name: []const u8,
    roots: []const []const u8,
    merged_group: u64,
    removed_group: u64,
    cfg: MergePublicVerificationConfig,
) !void {
    try std.testing.expect(try cluster.waitForGroupStatusCount(merged_group, .active, cfg.merged_active_count, cfg.active_rounds));
    try std.testing.expect(try cluster.waitForGroupStatusCount(removed_group, .absent, cfg.removed_absent_count, cfg.absent_rounds));

    const resolved_group = try waitForResolvedGroupForKey(cluster, catalog, table_name, "doc:z", merged_group, cfg.route_rounds);
    try std.testing.expectEqual(merged_group, resolved_group);

    // Repair the merged replica set with the right-side document before checking public reads.
    try mirrorGroupBatchToActiveReplicas(cluster, client, api_base_uris, merged_group, table_name, merge_seed_right_batch_body);

    _ = (try waitForGroupLeaderIndex(cluster, merged_group, cfg.leader_rounds)) orelse return error.TestExpectedEqual;
    try ensureGroupTextIndexOnActiveReplicas(cluster, roots, merged_group, api_tables.default_full_text_index_name, 40);
    const status_index = currentMetadataLeaderIndex(cluster) orelse 0;
    const merge_groups = [_]u64{merged_group};
    try reportRuntimeDocIdentityForActiveReplicas(cluster, cluster.node(status_index), roots, table_name, merge_groups[0..]);
    try cluster.stepAll();

    try std.testing.expect(try waitForLookupContains(cluster, client, client_base, table_name, "doc:a", "\"alpha\"", cfg.lookup_rounds));
    try std.testing.expect(try waitForLookupContains(cluster, client, client_base, table_name, "doc:z", "\"zeta\"", cfg.lookup_rounds));

    var post_merge_batch = try client.fetchBatch(client_base, table_name, merge_post_batch_body);
    defer post_merge_batch.deinit(std.heap.page_allocator);
    try expectBodyContainsAll(post_merge_batch.body, &.{"\"inserted\":1"});
    try mirrorGroupBatchToActiveReplicas(cluster, client, api_base_uris, merged_group, table_name, merge_post_batch_body);

    try std.testing.expect(try waitForQueryContainsAll(
        cluster,
        client,
        client_base,
        table_name,
        shared_hello_query_body,
        merge_query_needles[0..],
        cfg.lookup_rounds,
    ));

    if (cfg.expect_profile) {
        try expectHelloCountProfile(client, client_base, table_name, 3, 1, true);
    }
}

fn runAutomaticSplitPublicTrafficScenario(cfg: AutomaticSplitPublicTrafficScenario) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const initial_group_id = cfg.table_id * 10 + 1;

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    var delayed_a: ?raft_sim.DelayingRequestExecutor = null;
    defer if (delayed_a) |*executor| executor.deinit();
    var delayed_b: ?raft_sim.DelayingRequestExecutor = null;
    defer if (delayed_b) |*executor| executor.deinit();
    var delayed_c: ?raft_sim.DelayingRequestExecutor = null;
    defer if (delayed_c) |*executor| executor.deinit();
    if (cfg.delayed_transport) {
        delayed_a = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
        delayed_b = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
        delayed_c = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    }

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-a", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-b", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-c", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-a.txt", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-b.txt", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-c.txt", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, cfg.table_id, root_a, cat_a),
        makeHostSimConfig(2, cfg.table_id, root_b, cat_b),
        makeHostSimConfig(3, cfg.table_id, root_c, cat_c),
    };
    const deps = if (cfg.delayed_transport)
        [_]raft_sim.ManagedHttpHostSimulationDeps{
            makeHostSimDepsWithTransportExecutor(&factory_a, delayed_a.?.executor()),
            makeHostSimDepsWithTransportExecutor(&factory_b, delayed_b.?.executor()),
            makeHostSimDepsWithTransportExecutor(&factory_c, delayed_c.?.executor()),
        }
    else
        [_]raft_sim.ManagedHttpHostSimulationDeps{
            makeHostSimDeps(&factory_a),
            makeHostSimDeps(&factory_b),
            makeHostSimDeps(&factory_c),
        };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, cfg.table_id, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();
    const leader_index = try startBootstrappedMetadataCluster(&cluster, cfg.bootstrap_rounds, true);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = initial_group_id,
        .table_id = cfg.table_id,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = cfg.table_id,
        .name = "docs",
        .description = cfg.description,
        .indexes_json = api_tables.default_indexes_json,
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], cfg.range_create_rounds);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    const roots = [_][]const u8{ root_a, root_b, root_c };
    var public_api: PublicApiTestRig(3) = undefined;
    try public_api.initLeaderBackedInPlace(std.testing.allocator, &cluster, roots);
    defer public_api.deinit();
    var client = public_api.client;
    const client_index = (try waitForGroupLeaderIndex(&cluster, initial_group_id, 64)) orelse return error.TestExpectedEqual;
    const client_base = public_api.api_base_uris[client_index];
    try ensureGroupTextIndex(&cluster, roots[client_index], initial_group_id, api_tables.default_full_text_index_name, 40);
    try std.testing.expect(try waitForNodeProjectedTableFieldContains(&cluster, client_index, "docs", .indexes_json, "\"full_text_index_v0\"", true, cfg.projected_index_rounds));

    var pre_split_batch = try client.fetchBatch(client_base, "docs", split_seed_batch_body);
    defer pre_split_batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, pre_split_batch.body, "\"inserted\":3") != null);
    try mirrorGroupBatchToActiveReplicas(&cluster, &client, public_api.api_base_uris[0..], initial_group_id, "docs", split_seed_batch_body);
    try std.testing.expect(try waitForMedianKeyEquals(&cluster, cluster.node(leader_index), initial_group_id, "doc:m", 64));

    const source_leader = switch (cfg.failure_mode) {
        .restart_source_group_leader, .restart_metadata_leader => (try waitForGroupLeaderIndex(&cluster, initial_group_id, 64)) orelse return error.TestExpectedEqual,
        else => null,
    };
    if (cfg.ensure_source_group_text_index_before_transition) {
        const source_leader_index = source_leader orelse return error.TestExpectedEqual;
        try ensureGroupTextIndex(&cluster, roots[source_leader_index], initial_group_id, api_tables.default_full_text_index_name, 40);
    }

    try reportSplitCandidateStatus(cluster.node(leader_index), initial_group_id, 256, 180, "doc:m");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    const transition_observer_index: ?usize = switch (cfg.failure_mode) {
        .none => null,
        .restart_metadata_leader => blk: {
            try cluster.restartNode(query_index);
            break :blk (try cluster.waitForMetadataLeader(cfg.post_failure_leader_wait_rounds)) orelse return error.TestExpectedEqual;
        },
        .restart_source_group_leader => blk: {
            try cluster.restartNode(source_leader orelse return error.TestExpectedEqual);
            break :blk (try cluster.waitForMetadataLeader(cfg.post_failure_leader_wait_rounds)) orelse return error.TestExpectedEqual;
        },
        .partition_metadata_leader => blk: {
            try isolateMetadataNode(&cluster, query_index);
            break :blk (try waitForMetadataLeaderExcluding(&cluster, query_index, cfg.post_failure_leader_wait_rounds)) orelse return error.TestExpectedEqual;
        },
    };

    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, transition_observer_index, query_index, cfg.finalize_rounds));

    const verification_index = transition_observer_index orelse (currentMetadataLeaderIndex(&cluster) orelse query_index);
    const finalize_summary = try requireLeasedReconcile(cluster.node(verification_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    try verifySplitPublicTraffic(&cluster, &client, public_api.api_base_uris[0..], public_api.catalog_sources[verification_index].iface(), client_base, "docs", roots[0..], cfg.verify);
}

fn runAutomaticMergePublicTrafficScenario(cfg: AutomaticMergePublicTrafficScenario) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const left_group_id = cfg.table_id * 10 + 1;
    const right_group_id = cfg.table_id * 10 + 2;

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    var delayed_a: ?raft_sim.DelayingRequestExecutor = null;
    defer if (delayed_a) |*executor| executor.deinit();
    var delayed_b: ?raft_sim.DelayingRequestExecutor = null;
    defer if (delayed_b) |*executor| executor.deinit();
    var delayed_c: ?raft_sim.DelayingRequestExecutor = null;
    defer if (delayed_c) |*executor| executor.deinit();
    if (cfg.delayed_transport) {
        delayed_a = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
        delayed_b = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
        delayed_c = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    }

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-a", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-b", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-c", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-a.txt", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-b.txt", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/{s}-c.txt", .{ tmp.sub_path, cfg.path_prefix });
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, cfg.table_id, root_a, cat_a),
        makeHostSimConfig(2, cfg.table_id, root_b, cat_b),
        makeHostSimConfig(3, cfg.table_id, root_c, cat_c),
    };
    const deps = if (cfg.delayed_transport)
        [_]raft_sim.ManagedHttpHostSimulationDeps{
            makeHostSimDepsWithTransportExecutor(&factory_a, delayed_a.?.executor()),
            makeHostSimDepsWithTransportExecutor(&factory_b, delayed_b.?.executor()),
            makeHostSimDepsWithTransportExecutor(&factory_c, delayed_c.?.executor()),
        }
    else
        [_]raft_sim.ManagedHttpHostSimulationDeps{
            makeHostSimDeps(&factory_a),
            makeHostSimDeps(&factory_b),
            makeHostSimDeps(&factory_c),
        };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, cfg.table_id, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();
    const leader_index = try startBootstrappedMetadataCluster(&cluster, cfg.bootstrap_rounds, true);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = left_group_id,
            .table_id = cfg.table_id,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = right_group_id,
            .table_id = cfg.table_id,
            .start_key = "doc:m",
            .end_key = null,
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = cfg.table_id,
        .name = "docs",
        .description = cfg.description,
        .indexes_json = api_tables.default_indexes_json,
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], cfg.range_create_rounds);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    const roots = [_][]const u8{ root_a, root_b, root_c };
    var public_api: PublicApiTestRig(3) = undefined;
    try public_api.initLeaderBackedInPlace(std.testing.allocator, &cluster, roots);
    defer public_api.deinit();
    var client = public_api.client;
    const left_leader_index = (try waitForGroupLeaderIndex(&cluster, left_group_id, 64)) orelse return error.TestExpectedEqual;
    const right_leader_index = (try waitForGroupLeaderIndex(&cluster, right_group_id, 64)) orelse return error.TestExpectedEqual;
    const client_base = public_api.api_base_uris[left_leader_index];
    try std.testing.expect(try waitForNodeProjectedTableFieldContains(&cluster, left_leader_index, "docs", .indexes_json, "\"full_text_index_v0\"", true, cfg.projected_index_rounds));
    try ensureGroupTextIndex(&cluster, roots[left_leader_index], left_group_id, api_tables.default_full_text_index_name, 40);
    try ensureGroupTextIndex(&cluster, roots[right_leader_index], right_group_id, api_tables.default_full_text_index_name, 40);

    var pre_merge_batch = try client.fetchBatch(client_base, "docs", merge_seed_batch_body);
    defer pre_merge_batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, pre_merge_batch.body, "\"inserted\":2") != null);
    try mirrorGroupBatchToActiveReplicas(&cluster, &client, public_api.api_base_uris[0..], left_group_id, "docs", merge_seed_left_batch_body);
    try mirrorGroupBatchToActiveReplicas(&cluster, &client, public_api.api_base_uris[0..], right_group_id, "docs", merge_seed_right_batch_body);

    try reportMergeCandidateStatuses(cluster.node(leader_index), left_group_id, 16, 10, right_group_id, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    const transition_observer_index: ?usize = switch (cfg.failure_mode) {
        .none => null,
        .restart_metadata_leader => blk: {
            try cluster.restartNode(query_index);
            break :blk (try cluster.waitForMetadataLeader(cfg.post_failure_leader_wait_rounds)) orelse return error.TestExpectedEqual;
        },
        .restart_donor_group_leader => blk: {
            try cluster.restartNode(right_leader_index);
            break :blk (try cluster.waitForMetadataLeader(cfg.post_failure_leader_wait_rounds)) orelse return error.TestExpectedEqual;
        },
        .partition_metadata_leader => blk: {
            try isolateMetadataNode(&cluster, query_index);
            break :blk (try waitForMetadataLeaderExcluding(&cluster, query_index, cfg.post_failure_leader_wait_rounds)) orelse return error.TestExpectedEqual;
        },
    };

    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, transition_observer_index, query_index, cfg.finalize_rounds));

    const verification_index = transition_observer_index orelse (currentMetadataLeaderIndex(&cluster) orelse query_index);
    const finalize_summary = try requireLeasedReconcile(cluster.node(verification_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    try verifyMergePublicTraffic(&cluster, &client, public_api.api_base_uris[0..], public_api.catalog_sources[verification_index].iface(), client_base, "docs", roots[0..], left_group_id, right_group_id, cfg.verify);
}

fn makeGroupStatus(
    group_id: u64,
    doc_count: u64,
    disk_bytes: u64,
) metadata_table_manager.GroupStatusReport {
    return .{
        .group_id = group_id,
        .doc_count = doc_count,
        .disk_bytes = disk_bytes,
        .empty = false,
        .updated_at_millis = currentGroupStatusTimestampMs(),
    };
}

fn containsGroupStatus(group_statuses: []const metadata_table_manager.GroupStatusReport, group_id: u64) bool {
    for (group_statuses) |status| {
        if (status.group_id == group_id) return true;
    }
    return false;
}

fn firstProjectedStoreId(
    stores: []const metadata_table_manager.StoreRecord,
) ?u64 {
    if (stores.len == 0) return null;
    return stores[0].store_id;
}

fn findStoreReportIndex(
    reports: []const metadata_table_manager.StoreStatusReport,
    store_id: u64,
) ?usize {
    for (reports, 0..) |report, i| {
        if (report.store_id == store_id) return i;
    }
    return null;
}

fn containsProjectedBootstrapNode(
    projected_nodes: []const metadata_table_manager.NodeRecord,
    node_id: u64,
    role: []const u8,
) bool {
    for (projected_nodes) |record| {
        if (record.node_id != node_id) continue;
        if (std.mem.eql(u8, record.role, role)) return true;
    }
    return false;
}

fn containsProjectedBootstrapStore(
    projected_stores: []const metadata_table_manager.StoreRecord,
    store_id: u64,
    node_id: u64,
    role: []const u8,
    live: bool,
) bool {
    for (projected_stores) |record| {
        if (record.store_id != store_id) continue;
        if (record.node_id != node_id) continue;
        if (!std.mem.eql(u8, record.role, role)) continue;
        if (record.live != live) continue;
        return true;
    }
    return false;
}

fn freeOwnedSimStoreStatusReports(
    alloc: std.mem.Allocator,
    reports: []const metadata_table_manager.StoreStatusReport,
) void {
    for (reports) |report| {
        metadata_table_manager.freeGroupStatuses(alloc, report.group_statuses);
        metadata_table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
    }
    if (reports.len > 0) alloc.free(reports);
}

fn buildHealthyStoreStatusReports(
    node: anytype,
    group_statuses: []const metadata_table_manager.GroupStatusReport,
) ![]metadata_table_manager.StoreStatusReport {
    const alloc = std.testing.allocator;
    const projected_stores = try node.listProjectedStores(alloc);
    defer node.freeProjectedStores(alloc, projected_stores);
    const projected_intents = try node.listProjectedPlacementIntents(alloc);
    defer node.freeProjectedPlacementIntents(alloc, projected_intents);

    var report_store_ids = std.ArrayListUnmanaged(u64).empty;
    defer report_store_ids.deinit(alloc);

    for (projected_intents) |intent| {
        if (!containsGroupStatus(group_statuses, intent.record.group_id)) continue;
        if (intent.store_id == 0) continue;
        for (report_store_ids.items) |store_id| {
            if (store_id == intent.store_id) break;
        } else try report_store_ids.append(alloc, intent.store_id);
    }

    if (report_store_ids.items.len == 0) {
        const fallback_store_id = firstProjectedStoreId(projected_stores) orelse return error.TestExpectedEqual;
        try report_store_ids.append(alloc, fallback_store_id);
    }

    const reports = try alloc.alloc(metadata_table_manager.StoreStatusReport, report_store_ids.items.len);
    errdefer freeOwnedSimStoreStatusReports(alloc, reports);

    for (report_store_ids.items, 0..) |store_id, i| {
        reports[i] = .{
            .store_id = store_id,
            .live = true,
            .health_class = "healthy",
            .capacity_bytes = 1024,
            .available_bytes = 900,
            .group_statuses = &.{},
        };
    }

    for (group_statuses) |base_status| {
        var voter_count: u16 = 0;
        var leader_store_id: ?u64 = null;
        for (projected_intents) |intent| {
            if (intent.record.group_id != base_status.group_id) continue;
            if (intent.store_id == 0) continue;
            voter_count += 1;
            if (leader_store_id == null or intent.store_id < leader_store_id.?) leader_store_id = intent.store_id;
        }

        if (voter_count == 0) {
            voter_count = 1;
            leader_store_id = report_store_ids.items[0];
        }

        for (report_store_ids.items) |store_id| {
            const participates = blk: {
                for (projected_intents) |intent| {
                    if (intent.record.group_id == base_status.group_id and intent.store_id == store_id) break :blk true;
                }
                break :blk voter_count == 1 and store_id == leader_store_id.?;
            };
            if (!participates) continue;

            const report_index = findStoreReportIndex(reports, store_id) orelse return error.TestExpectedEqual;
            var status = base_status;
            status.empty = false;
            status.local_voter = true;
            status.voter_count = voter_count;
            status.local_leader = store_id == leader_store_id.?;

            const existing = reports[report_index].group_statuses;
            const next = try alloc.alloc(metadata_table_manager.GroupStatusReport, existing.len + 1);
            @memcpy(next[0..existing.len], existing);
            next[existing.len] = status;
            metadata_table_manager.freeGroupStatuses(alloc, existing);
            reports[report_index].group_statuses = next;
        }
    }

    return reports;
}

fn reportHealthyStoreStatuses(
    node: anytype,
    group_statuses: []const metadata_table_manager.GroupStatusReport,
) !void {
    const reports = try buildHealthyStoreStatusReports(node, group_statuses);
    defer freeOwnedSimStoreStatusReports(std.testing.allocator, reports);
    try std.testing.expectEqual(reports.len, try node.reportStoreStatuses(reports));
}

fn reportSplitCandidateStatus(
    node: anytype,
    group_id: u64,
    doc_count: u64,
    disk_bytes: u64,
    median_key: ?[]const u8,
) !void {
    _ = median_key;
    const group_statuses = [_]metadata_table_manager.GroupStatusReport{
        makeGroupStatus(group_id, doc_count, disk_bytes),
    };
    try reportHealthyStoreStatuses(node, group_statuses[0..]);
}

fn reportMergeCandidateStatuses(
    node: anytype,
    receiver_group_id: u64,
    receiver_doc_count: u64,
    receiver_disk_bytes: u64,
    donor_group_id: u64,
    donor_doc_count: u64,
    donor_disk_bytes: u64,
) !void {
    const group_statuses = [_]metadata_table_manager.GroupStatusReport{
        makeGroupStatus(receiver_group_id, receiver_doc_count, receiver_disk_bytes),
        makeGroupStatus(donor_group_id, donor_doc_count, donor_disk_bytes),
    };
    try reportHealthyStoreStatuses(node, group_statuses[0..]);
}

fn createActiveTableRanges(
    workflow: *metadata_table_workflow.TableWorkflow,
    cluster: *MetadataHttpClusterSimulation,
    leader_index: usize,
    table: metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    wait_rounds: usize,
) !void {
    _ = try createActiveTableRangesWithSummary(workflow, cluster, leader_index, table, ranges, wait_rounds);
}

fn createActiveTableRangesWithSummary(
    workflow: *metadata_table_workflow.TableWorkflow,
    cluster: *MetadataHttpClusterSimulation,
    leader_index: usize,
    table: metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    wait_rounds: usize,
) !metadata_control_loop.ReconcileSummary {
    const summary = try workflow.createTableWithRanges(&cluster.node(leader_index), table, ranges);
    const expected_active = blk: {
        if (ranges.len > 0 and summary.placement_upserts > 0) {
            break :blk @max(@as(usize, 1), summary.placement_upserts / ranges.len);
        }
        break :blk if (table.desired_replica_count > 0) table.desired_replica_count else 1;
    };
    for (ranges) |range| {
        try std.testing.expect(try cluster.waitForGroupStatusCount(range.group_id, .active, expected_active, wait_rounds));
    }
    try std.testing.expect(try waitForProjectedTablePresenceOnAllNodes(cluster, table.name, wait_rounds));
    if (std.mem.indexOf(u8, table.indexes_json, "\"full_text_index_v0\"") != null) {
        try std.testing.expect(try waitForProjectedTableFieldContains(cluster, table.name, .indexes_json, "\"full_text_index_v0\"", true, wait_rounds));
    }
    return summary;
}

const ProjectedTableField = enum {
    schema_json,
    indexes_json,
};

fn waitForProjectedTablePresenceOnAllNodes(
    cluster: *MetadataHttpClusterSimulation,
    table_name: []const u8,
    max_rounds: usize,
) !bool {
    var ctx = ProjectedTablePresenceProgressContext{ .table_name = table_name };
    return try cluster.runUntil(max_rounds, &ctx, projectedTablePresenceProgressPredicate);
}

const ProjectedTablePresenceProgressContext = struct {
    table_name: []const u8,
};

fn projectedTablePresenceProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *ProjectedTablePresenceProgressContext = @ptrCast(@alignCast(ptr));
    for (0..cluster.cluster.nodes.len) |i| {
        const node = cluster.node(i);
        const tables = try node.listProjectedTables(cluster.alloc);
        defer node.freeProjectedTables(cluster.alloc, tables);
        const table = api_tables.findTableByName(&.{
            .status = .{ .metadata_group_id = cluster.metadata_group_id, .metrics = .{} },
            .tables = tables,
            .ranges = &.{},
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
        }, ctx.table_name);
        if (table == null) return false;
    }
    return true;
}

fn projectedTableFieldContainsOnAllNodes(
    cluster: *MetadataHttpClusterSimulation,
    table_name: []const u8,
    field: ProjectedTableField,
    needle: []const u8,
    expected_present: bool,
) !bool {
    for (0..cluster.cluster.nodes.len) |i| {
        const node = cluster.node(i);
        const tables = try node.listProjectedTables(cluster.alloc);
        defer node.freeProjectedTables(cluster.alloc, tables);
        const table = api_tables.findTableByName(&.{
            .status = .{ .metadata_group_id = cluster.metadata_group_id, .metrics = .{} },
            .tables = tables,
            .ranges = &.{},
            .stores = &.{},
            .placement_intents = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
        }, table_name) orelse return false;
        const haystack = switch (field) {
            .schema_json => table.schema_json,
            .indexes_json => table.indexes_json,
        };
        const contains = std.mem.indexOf(u8, haystack, needle) != null;
        if (contains != expected_present) return false;
    }
    return true;
}

fn waitForProjectedTableFieldContains(
    cluster: *MetadataHttpClusterSimulation,
    table_name: []const u8,
    field: ProjectedTableField,
    needle: []const u8,
    expected_present: bool,
    max_rounds: usize,
) !bool {
    var ctx = ProjectedTableFieldContainsProgressContext{
        .table_name = table_name,
        .field = field,
        .needle = needle,
        .expected_present = expected_present,
        .run_node_rounds = true,
    };
    return try cluster.runUntil(max_rounds, &ctx, projectedTableFieldContainsProgressPredicate);
}

fn waitForNodeProjectedTableFieldContains(
    cluster: *MetadataHttpClusterSimulation,
    node_index: usize,
    table_name: []const u8,
    field: ProjectedTableField,
    needle: []const u8,
    expected_present: bool,
    max_rounds: usize,
) !bool {
    var ctx = ProjectedTableFieldContainsProgressContext{
        .table_name = table_name,
        .field = field,
        .needle = needle,
        .expected_present = expected_present,
        .node_index = node_index,
        .run_node_rounds = true,
    };
    return try cluster.runUntil(max_rounds, &ctx, projectedTableFieldContainsProgressPredicate);
}

const ProjectedTableFieldContainsProgressContext = struct {
    table_name: []const u8,
    field: ProjectedTableField,
    needle: []const u8,
    expected_present: bool,
    node_index: ?usize = null,
    run_node_rounds: bool = false,
};

fn projectedTableFieldContainsProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *ProjectedTableFieldContainsProgressContext = @ptrCast(@alignCast(ptr));
    const matches = if (ctx.node_index) |node_index|
        try projectedTableFieldContainsOnNode(cluster, node_index, ctx.table_name, ctx.field, ctx.needle, ctx.expected_present)
    else
        try projectedTableFieldContainsOnAllNodes(cluster, ctx.table_name, ctx.field, ctx.needle, ctx.expected_present);
    if (matches) return true;
    if (ctx.run_node_rounds) {
        for (0..cluster.cluster.nodes.len) |i| try cluster.node(i).runRound();
    }
    return false;
}

fn projectedTableFieldContainsOnNode(
    cluster: *MetadataHttpClusterSimulation,
    node_index: usize,
    table_name: []const u8,
    field: ProjectedTableField,
    needle: []const u8,
    expected_present: bool,
) !bool {
    const node = cluster.node(node_index);
    const tables = try node.listProjectedTables(cluster.alloc);
    defer node.freeProjectedTables(cluster.alloc, tables);
    const table = api_tables.findTableByName(&.{
        .status = .{ .metadata_group_id = cluster.metadata_group_id, .metrics = .{} },
        .tables = tables,
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    }, table_name) orelse return false;
    const haystack = switch (field) {
        .schema_json => table.schema_json,
        .indexes_json => table.indexes_json,
    };
    return (std.mem.indexOf(u8, haystack, needle) != null) == expected_present;
}

const SimMergeRuntime = struct {
    const Entry = struct {
        donor_group_id: u64,
        receiver_group_id: u64,
        status: data_mod.MergeTransitionStatus = .{
            .phase = .prepare,
            .donor_group_id = 0,
            .receiver_group_id = 0,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
    };

    entries: [16]Entry = undefined,
    len: usize = 0,

    fn iface(self: *@This()) transition_runtime.MergeRuntime {
        return .{
            .ptr = self,
            .vtable = &.{
                .observe_status = observeStatus,
                .record_doc_identity_reassignment = recordDocIdentityReassignment,
                .accept_receiver = acceptReceiver,
                .catch_up_receiver = catchUpReceiver,
                .finalize_merge = finalizeMerge,
                .rollback_merge = rollbackMerge,
            },
        };
    }

    fn entryFor(self: *@This(), donor_group_id: u64, receiver_group_id: u64) *Entry {
        for (self.entries[0..self.len]) |*entry| {
            if (entry.donor_group_id == donor_group_id and entry.receiver_group_id == receiver_group_id) return entry;
        }
        std.debug.assert(self.len < self.entries.len);
        self.entries[self.len] = .{
            .donor_group_id = donor_group_id,
            .receiver_group_id = receiver_group_id,
            .status = .{
                .phase = .prepare,
                .donor_group_id = donor_group_id,
                .receiver_group_id = receiver_group_id,
                .receiver_accepts_donor_range = false,
                .bootstrapped = false,
                .replay_required = false,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 0,
                .receiver_delta_sequence = 0,
            },
        };
        self.len += 1;
        return &self.entries[self.len - 1];
    }

    fn observeStatus(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !data_mod.MergeTransitionStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return self.entryFor(donor_group_id, receiver_group_id).status;
    }

    fn recordDocIdentityReassignment(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const entry = self.entryFor(donor_group_id, receiver_group_id);
        entry.status.allow_doc_identity_reassignment = true;
    }

    fn acceptReceiver(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const entry = self.entryFor(donor_group_id, receiver_group_id);
        entry.status.phase = .bootstrap_peer;
        entry.status.receiver_accepts_donor_range = true;
    }

    fn catchUpReceiver(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !usize {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const entry = self.entryFor(donor_group_id, receiver_group_id);
        entry.status.phase = .cutover_ready;
        entry.status.bootstrapped = true;
        entry.status.replay_required = true;
        entry.status.replay_caught_up = true;
        entry.status.cutover_ready = true;
        entry.status.receiver_ready_for_reads = true;
        entry.status.donor_delta_sequence = 1;
        entry.status.receiver_delta_sequence = 1;
        return 1;
    }

    fn finalizeMerge(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const entry = self.entryFor(donor_group_id, receiver_group_id);
        entry.status.phase = .finalized;
        entry.status.replay_required = false;
        return true;
    }

    fn rollbackMerge(ptr: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const entry = self.entryFor(donor_group_id, receiver_group_id);
        entry.status.phase = .rolled_back;
        entry.status.receiver_accepts_donor_range = false;
        entry.status.bootstrapped = false;
        entry.status.replay_required = false;
        entry.status.replay_caught_up = false;
        entry.status.cutover_ready = false;
        entry.status.receiver_ready_for_reads = false;
        return true;
    }
};

test "metadata sim merge runtime records doc identity reassignment opt-in" {
    var sim = SimMergeRuntime{};
    var runtime = transition_runtime.TransitionRuntime{ .merge = sim.iface() };

    try runtime.execute(.{ .accept_merge_receiver = .{
        .transition_id = 901,
        .donor_group_id = 101,
        .receiver_group_id = 102,
        .allow_doc_identity_reassignment = true,
    } });

    const observation = try runtime.observeMerge(.{
        .transition_id = 901,
        .donor_group_id = 101,
        .receiver_group_id = 102,
    });
    try std.testing.expect(observation.receiver.allow_doc_identity_reassignment);
}

const TestDescriptorFactory = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,
    peers: []const raft_engine.core.types.NodeId,
    split_runtime: SimSplitRuntime = .{},
    merge_runtime: SimMergeRuntime = .{},
    group_stores: std.AutoHashMapUnmanaged(u64, *raft_engine.core.MemoryStorage) = .empty,
    primary_group_id: ?u64 = null,
    active_descriptors: usize = 0,

    fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn storageForGroup(self: *@This(), group_id: u64) !*raft_engine.core.MemoryStorage {
        if (self.group_stores.get(group_id)) |store| return store;
        if (self.primary_group_id == null) {
            self.primary_group_id = group_id;
            try self.group_stores.put(std.heap.page_allocator, group_id, self.store);
            return self.store;
        }
        const store = try std.heap.page_allocator.create(raft_engine.core.MemoryStorage);
        errdefer std.heap.page_allocator.destroy(store);
        store.* = raft_engine.core.MemoryStorage.init(std.heap.page_allocator);
        errdefer store.deinit();
        try self.group_stores.put(std.heap.page_allocator, group_id, store);
        return store;
    }

    fn buildDescriptor(ptr: *anyopaque, record: raft_catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const store = try self.storageForGroup(record.group_id);
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, self.peers);
        self.active_descriptors += 1;
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
                    .pre_vote = false,
                    .check_quorum = true,
                },
                .storage = store.storage(),
            },
            .bootstrap = switch (record.bootstrap_mode) {
                .empty => .empty,
                .persisted => .persisted,
                .fetch_snapshot => .persisted,
            },
        };
    }

    fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = alloc;
        self.alloc.free(desc.group.raft_config.peers);
        std.debug.assert(self.active_descriptors > 0);
        self.active_descriptors -= 1;
        if (self.active_descriptors == 0) self.split_runtime.deinit();
    }
};

fn makeHostSimConfig(
    local_node_id: u64,
    metadata_group_id: u64,
    replica_root_dir: []const u8,
    replica_catalog_path: []const u8,
) raft_sim.ManagedHttpHostSimulationConfig {
    return .{
        .host = .{
            .http = .{
                .host = .{
                    .local_node_id = local_node_id,
                    .metadata_group_id = metadata_group_id,
                    .replica_root_dir = replica_root_dir,
                    .replica_catalog_path = replica_catalog_path,
                },
                .transport = .{
                    .snapshot = .{ .root_dir = replica_root_dir },
                },
            },
            .replica_apply_store_no_sync = true,
        },
    };
}

fn makeHostSimDeps(factory: *TestDescriptorFactory) raft_sim.ManagedHttpHostSimulationDeps {
    return makeHostSimDepsWithTransportExecutor(factory, null);
}

fn makeHostSimDepsWithTransportExecutor(
    factory: *TestDescriptorFactory,
    request_executor: ?raft_transport.RequestExecutor,
) raft_sim.ManagedHttpHostSimulationDeps {
    return .{
        .host = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory.iface(),
                },
                .request_executor = request_executor,
            },
        },
        .service = .{
            .transition_runtime = .{
                .split = factory.split_runtime.iface(),
                .merge = factory.merge_runtime.iface(),
            },
        },
    };
}

pub const MetadataHttpNodeSimulation = struct {
    cluster: *MetadataHttpClusterSimulation,
    index: usize,

    fn sim(self: MetadataHttpNodeSimulation) *raft_sim.ManagedHttpHostSimulation {
        return self.cluster.cluster.node(self.index);
    }

    pub fn backendRuntime(self: MetadataHttpNodeSimulation) *db_mod.background_runtime.BackendRuntime {
        return self.cluster.backendRuntime(self.index);
    }

    pub fn runRound(self: MetadataHttpNodeSimulation) !void {
        _ = try self.sim().stepOnce();
        try self.cluster.refreshOwnedMetadataRuntimes(self.index);
    }

    pub fn serviceMetrics(self: MetadataHttpNodeSimulation) @TypeOf(self.sim().serviceMetrics()) {
        return self.sim().serviceMetrics();
    }

    pub fn metadataStatus(self: MetadataHttpNodeSimulation) !metadata_service.MetadataStatus {
        return try metadata_service.snapshotStatus(
            self.cluster.alloc,
            self.cluster.metadata_group_id,
            self,
            self.serviceMetrics(),
        );
    }

    pub fn adminSnapshot(self: MetadataHttpNodeSimulation) !metadata_api.AdminSnapshot {
        return try metadata_api.captureSnapshot(self.cluster.alloc, self);
    }

    pub fn medianKeyLookup(self: MetadataHttpNodeSimulation) ?metadata_reconciler.MedianKeyLookup {
        return .{
            .ptr = self.cluster,
            .vtable = &.{
                .fetch_median_key = fetchMedianKey,
            },
        };
    }

    fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        const cluster: *MetadataHttpClusterSimulation = @ptrCast(@alignCast(ptr));
        const preferred_index = currentGroupLeaderIndex(cluster, group_id);
        if (preferred_index) |index| {
            if (try fetchMedianKeyFromReplica(cluster, alloc, index, group_id)) |median| return median;
        }
        for (0..cluster.cluster.nodes.len) |index| {
            if (preferred_index != null and preferred_index.? == index) continue;
            if (cluster.node(index).status(group_id) != .active) continue;
            if (try fetchMedianKeyFromReplica(cluster, alloc, index, group_id)) |median| return median;
        }
        return null;
    }

    fn fetchMedianKeyFromReplica(
        cluster: *MetadataHttpClusterSimulation,
        alloc: std.mem.Allocator,
        node_index: usize,
        group_id: u64,
    ) !?[]u8 {
        const replica_root_dir = cluster.cluster.configs[node_index].host.http.host.replica_root_dir orelse return null;
        const db_path = try metadata_mod.groupDbPathFromReplicaRoot(alloc, replica_root_dir, group_id);
        defer alloc.free(db_path);

        var db = db_mod.DB.open(alloc, db_path, .{}) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer db.close();

        return db.findMedianKey(alloc) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
    }

    pub fn reconcileLeaseStats(self: MetadataHttpNodeSimulation) metadata_reconcile_lease.Stats {
        return self.cluster.reconcile_leases[self.index].stats();
    }

    pub fn getProjectedReconcileLease(self: MetadataHttpNodeSimulation) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.getReconcileLease(self.cluster.metadata_group_id);
    }

    pub fn freeAdminSnapshot(self: MetadataHttpNodeSimulation, snapshot: *metadata_api.AdminSnapshot) void {
        metadata_api.freeSnapshot(self.cluster.alloc, self, snapshot);
    }

    pub fn status(self: MetadataHttpNodeSimulation, group_id: u64) raft_host.HostedReplicaStatus {
        return self.sim().status(group_id);
    }

    pub fn campaignMetadataGroup(self: MetadataHttpNodeSimulation) !void {
        if (self.sim().raftStatus(self.cluster.metadata_group_id)) |raft_status| {
            if (raft_status.soft.role == .leader) return;
        }
        try self.sim().campaignGroup(self.cluster.metadata_group_id);
    }

    pub fn campaignGroup(self: MetadataHttpNodeSimulation, group_id: u64) !void {
        try self.sim().campaignGroup(group_id);
    }

    pub fn listProjectedTables(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.listTables(alloc, self.cluster.metadata_group_id);
    }

    pub fn freeProjectedTables(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return;
        store.freeTables(alloc, records);
    }

    pub fn listProjectedRanges(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.listRanges(alloc, self.cluster.metadata_group_id);
    }

    pub fn freeProjectedRanges(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return;
        store.freeRanges(alloc, records);
    }

    pub fn listProjectedPlacementIntents(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.listPlacementIntents(alloc, self.cluster.metadata_group_id);
    }

    pub fn listProjectedNodes(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator) ![]metadata_table_manager.NodeRecord {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.listNodes(alloc, self.cluster.metadata_group_id);
    }

    pub fn freeProjectedNodes(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator, records: []metadata_table_manager.NodeRecord) void {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return;
        store.freeNodes(alloc, records);
    }

    pub fn listProjectedStores(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator) ![]metadata_table_manager.StoreRecord {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.listStores(alloc, self.cluster.metadata_group_id);
    }

    pub fn freeProjectedStores(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator, records: []metadata_table_manager.StoreRecord) void {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return;
        store.freeStores(alloc, records);
    }

    pub fn freeProjectedPlacementIntents(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return;
        store.freePlacementIntents(alloc, intents);
    }

    pub fn listProjectedSplitTransitions(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.listSplitTransitions(alloc, self.cluster.metadata_group_id);
    }

    pub fn freeProjectedSplitTransitions(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return;
        store.freeSplitTransitions(alloc, records);
    }

    pub fn listProjectedMergeTransitions(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.listMergeTransitions(alloc, self.cluster.metadata_group_id);
    }

    pub fn freeProjectedMergeTransitions(self: MetadataHttpNodeSimulation, alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return;
        store.freeMergeTransitions(alloc, records);
    }

    pub fn observeSplitTransition(self: MetadataHttpNodeSimulation, transition_id: u64) !?transition_state.SplitObservation {
        if (try self.sim().observeSplitTransition(transition_id)) |observation| return observation;
        if (self.cluster.currentMetadataLeaseHolderIndex()) |lease_holder_index| {
            if (lease_holder_index != self.index) {
                if (try self.cluster.node(lease_holder_index).sim().observeSplitTransition(transition_id)) |observation| return observation;
            }
        }
        if (self.cluster.currentMetadataLeaderIndex()) |leader_index| {
            if (leader_index != self.index) {
                return try self.cluster.node(leader_index).sim().observeSplitTransition(transition_id);
            }
        }
        return null;
    }

    pub fn observeMergeTransition(self: MetadataHttpNodeSimulation, transition_id: u64) !?transition_state.MergeObservation {
        if (try self.sim().observeMergeTransition(transition_id)) |observation| return observation;
        if (self.cluster.currentMetadataLeaseHolderIndex()) |lease_holder_index| {
            if (lease_holder_index != self.index) {
                if (try self.cluster.node(lease_holder_index).sim().observeMergeTransition(transition_id)) |observation| return observation;
            }
        }
        if (self.cluster.currentMetadataLeaderIndex()) |leader_index| {
            if (leader_index != self.index) {
                return try self.cluster.node(leader_index).sim().observeMergeTransition(transition_id);
            }
        }
        return null;
    }

    pub fn proposeTransitionCommand(self: MetadataHttpNodeSimulation, command: metadata_storage.TransitionCommand) anyerror!void {
        return try self.proposeTransitionCommands(&.{command});
    }

    pub fn upsertReplicaIntent(self: MetadataHttpNodeSimulation, intent: raft_reconciler.PlacementIntent) !void {
        try self.proposeTransitionCommand(.{ .upsert_replica_intent = intent });
    }

    pub fn upsertNode(self: MetadataHttpNodeSimulation, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_node = record });
    }

    pub fn registerNode(self: MetadataHttpNodeSimulation, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .register_node = record });
    }

    pub fn requestNodeShutdown(self: MetadataHttpNodeSimulation, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .request_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn cancelNodeShutdown(self: MetadataHttpNodeSimulation, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .cancel_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn finalizeNodeShutdown(self: MetadataHttpNodeSimulation, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .finalize_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn removeNode(self: MetadataHttpNodeSimulation, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_node = .{ .node_id = node_id } });
    }

    pub fn upsertStore(self: MetadataHttpNodeSimulation, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_store = record });
    }

    pub fn registerStore(self: MetadataHttpNodeSimulation, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .register_store = record });
    }

    pub fn reportStoreStatus(self: MetadataHttpNodeSimulation, report: metadata_table_manager.StoreStatusReport) !void {
        _ = try self.reportStoreStatuses(&.{report});
    }

    pub fn reportStoreStatuses(self: MetadataHttpNodeSimulation, reports: []const metadata_table_manager.StoreStatusReport) !usize {
        const projected = try self.listProjectedStores(self.cluster.alloc);
        defer self.freeProjectedStores(self.cluster.alloc, projected);

        var changed_indices = std.ArrayListUnmanaged(usize).empty;
        defer changed_indices.deinit(self.cluster.alloc);
        for (reports) |report| {
            const index = metadata_store_observer.findStoreIndex(projected, report.store_id) orelse return error.UnknownStore;
            if (!metadata_store_observer.observationChangesRecord(projected[index], report)) continue;
            try changed_indices.append(self.cluster.alloc, index);
        }

        const applied = try metadata_store_observer.applyObservationsOwned(self.cluster.alloc, projected, reports);
        for (changed_indices.items) |index| try self.upsertStore(projected[index]);
        return applied;
    }

    pub fn removeStore(self: MetadataHttpNodeSimulation, store_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_store = .{ .store_id = store_id } });
    }

    pub fn removeReplicaIntent(self: MetadataHttpNodeSimulation, group_id: u64, local_node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_replica_intent = .{
            .group_id = group_id,
            .local_node_id = local_node_id,
        } });
    }

    pub fn upsertTable(self: MetadataHttpNodeSimulation, record: metadata_table_manager.TableRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_table = record });
    }

    pub fn removeTable(self: MetadataHttpNodeSimulation, table_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_table = .{ .table_id = table_id } });
    }

    pub fn upsertRange(self: MetadataHttpNodeSimulation, record: metadata_table_manager.RangeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_range = record });
    }

    pub fn removeRange(self: MetadataHttpNodeSimulation, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_range = .{ .group_id = group_id } });
    }

    pub fn upsertSplitTransition(self: MetadataHttpNodeSimulation, record: transition_state.SplitTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_split_transition = record });
    }

    pub fn removeSplitTransition(self: MetadataHttpNodeSimulation, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_split_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertMergeTransition(self: MetadataHttpNodeSimulation, record: transition_state.MergeTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_merge_transition = record });
    }

    pub fn removeMergeTransition(self: MetadataHttpNodeSimulation, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_merge_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertReconcileLease(self: MetadataHttpNodeSimulation, record: metadata_reconcile_lease.ReconcileLeaseRecord) anyerror!void {
        try self.proposeTransitionCommand(.{ .upsert_reconcile_lease = record });
    }

    pub fn removeReconcileLease(self: MetadataHttpNodeSimulation) !void {
        try self.proposeTransitionCommand(.{ .remove_reconcile_lease = .{} });
    }

    pub fn requestReallocation(self: MetadataHttpNodeSimulation, requested_at_ms: u64) !void {
        try self.proposeTransitionCommand(.{ .upsert_reallocation_request = .{
            .requested_at_ms = requested_at_ms,
        } });
    }

    pub fn clearReallocationRequest(self: MetadataHttpNodeSimulation) !void {
        try self.proposeTransitionCommand(.{ .remove_reallocation_request = .{} });
    }

    pub fn applyReconciliationPlan(self: MetadataHttpNodeSimulation, plan: *const metadata_reconciler.ReconciliationPlan) !void {
        const projected_intents = try self.listProjectedPlacementIntents(self.cluster.alloc);
        defer self.freeProjectedPlacementIntents(self.cluster.alloc, projected_intents);

        var commands = std.ArrayListUnmanaged(metadata_storage.TransitionCommand).empty;
        defer commands.deinit(self.cluster.alloc);

        for (plan.placement_upserts) |intent| {
            if (containsProjectedPlacementIntent(projected_intents, intent)) continue;
            try commands.append(self.cluster.alloc, .{ .upsert_replica_intent = intent });
        }
        for (plan.table_upserts) |record| try commands.append(self.cluster.alloc, .{ .upsert_table = record });
        for (plan.range_upserts) |record| try commands.append(self.cluster.alloc, .{ .upsert_range = record });
        for (plan.split_upserts) |record| try commands.append(self.cluster.alloc, .{ .upsert_split_transition = record });
        for (plan.merge_upserts) |record| try commands.append(self.cluster.alloc, .{ .upsert_merge_transition = record });
        for (plan.placement_removals) |record| {
            if (!containsProjectedPlacementIntentGroup(projected_intents, record.group_id)) continue;
            _ = record.local_node_id;
            try commands.append(self.cluster.alloc, .{ .remove_replica_intent = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
            } });
        }
        for (plan.table_removals) |table_id| try commands.append(self.cluster.alloc, .{ .remove_table = .{ .table_id = table_id } });
        for (plan.range_removals) |group_id| try commands.append(self.cluster.alloc, .{ .remove_range = .{ .group_id = group_id } });
        for (plan.split_removals) |transition_id| try commands.append(self.cluster.alloc, .{ .remove_split_transition = .{ .transition_id = transition_id } });
        for (plan.merge_removals) |transition_id| try commands.append(self.cluster.alloc, .{ .remove_merge_transition = .{ .transition_id = transition_id } });
        if (plan.clear_reallocation_request) try commands.append(self.cluster.alloc, .{ .remove_reallocation_request = .{} });

        try self.proposeTransitionCommands(commands.items);
    }

    pub fn getProjectedReallocationRequest(self: MetadataHttpNodeSimulation) !?metadata_mod.ReallocationRequestRecord {
        const store = self.sim().runtime.svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        return try store.getReallocationRequest(self.cluster.metadata_group_id);
    }

    pub fn reconcileOnce(self: MetadataHttpNodeSimulation, loop: *metadata_control_loop.MetadataControlLoop) !metadata_control_loop.ReconcileSummary {
        return try loop.reconcileOnce(self);
    }

    fn proposeTransitionCommands(self: MetadataHttpNodeSimulation, commands: []const metadata_storage.TransitionCommand) anyerror!void {
        if (commands.len == 0) return;

        self.cluster.metadata_proposal_in_flight += 1;
        defer self.cluster.metadata_proposal_in_flight -= 1;

        for (commands) |command| {
            const encoded = try metadata_storage.encodeTransitionCommand(self.cluster.alloc, command);
            defer self.cluster.alloc.free(encoded);

            var attempts: usize = 0;
            while (attempts < 32) : (attempts += 1) {
                const target_index = self.cluster.currentMetadataLeaderIndex() orelse {
                    self.campaignMetadataGroup() catch |err| switch (err) {
                        error.UnknownGroup => {},
                        else => return err,
                    };
                    try self.cluster.stepAll();
                    continue;
                };

                self.cluster.node(target_index).sim().propose(self.cluster.metadata_group_id, encoded) catch |err| switch (err) {
                    error.NotLeader => {
                        try self.cluster.stepAll();
                        continue;
                    },
                    else => return err,
                };
                break;
            } else return error.NotLeader;
        }

        const settle_rounds = if (commandsOnlyReconcileLease(commands))
            @as(usize, 1)
        else
            @max(@as(usize, 4), @min(commands.len * 2, @as(usize, 16)));
        var rounds: usize = 0;
        while (rounds < settle_rounds) : (rounds += 1) try self.cluster.stepAll();
    }

    fn commandsOnlyReconcileLease(commands: []const metadata_storage.TransitionCommand) bool {
        for (commands) |command| switch (command) {
            .upsert_reconcile_lease, .remove_reconcile_lease => {},
            else => return false,
        };
        return true;
    }

    pub fn reconcileOnceIfLeaseHeld(self: MetadataHttpNodeSimulation, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        if (!self.cluster.reconcile_leases[self.index].stats().held_by_local) return null;
        var rounds: usize = 0;
        while (rounds < 32) : (rounds += 1) {
            const has_reconcile_lease = try self.cluster.ensureReconcileLease(self.index);
            if (has_reconcile_lease) return try loop.reconcileOnce(self);
            try self.cluster.stepAll();
        }
        return null;
    }

    pub fn reconcileOnceEnsuringLease(self: MetadataHttpNodeSimulation, loop: *metadata_control_loop.MetadataControlLoop) !metadata_control_loop.ReconcileSummary {
        const target = currentMetadataMutationNode(self);
        var rounds: usize = 0;
        while (rounds < 32) : (rounds += 1) {
            if (target.cluster.reconcile_leases[target.index].stats().held_by_local) {
                return try loop.reconcileOnce(target);
            }
            const has_reconcile_lease = try target.cluster.ensureReconcileLease(target.index);
            if (has_reconcile_lease) return try loop.reconcileOnce(target);
            try target.cluster.stepAll();
        }
        return error.ReconcileLeaseNotHeld;
    }
};

const MetadataLeaderProgressContext = struct {
    index: ?usize = null,
};

const MetadataSpecificLeaderProgressContext = struct {
    index: usize,
};

const MetadataGroupStatusProgressContext = struct {
    group_id: u64,
    desired: raft_host.HostedReplicaStatus,
};

const MetadataNodeGroupStatusProgressContext = struct {
    index: usize,
    group_id: u64,
    desired: raft_host.HostedReplicaStatus,
};

const MetadataGroupStatusCountProgressContext = struct {
    group_id: u64,
    desired: raft_host.HostedReplicaStatus,
    expected_count: usize,
};

const MetadataSplitTransitionProgressContext = struct {
    transition_id: u64,
};

const MetadataMergeTransitionProgressContext = struct {
    transition_id: u64,
};

const MetadataSplitTransitionFinalizedProgressContext = struct {
    transition_id: u64,
    observer_index: ?usize = null,
    fallback_index: usize,
};

const MetadataSplitFinalizedOrGroupReadyProgressContext = struct {
    transition_id: u64,
    group_id: u64,
    fallback_index: usize,
    group_index: ?usize = null,
    finalized: bool = false,
};

const MetadataMergeTransitionFinalizedProgressContext = struct {
    transition_id: u64,
    observer_index: ?usize = null,
    fallback_index: usize,
};

pub const MetadataHttpClusterSimulation = struct {
    alloc: std.mem.Allocator,
    metadata_group_id: u64,
    cluster: raft_sim.ManagedHttpClusterSimulation,
    virtual_network: *raft_sim.VirtualHttpNetwork,
    reconcile_leases: []metadata_reconcile_lease.State,
    pending_reconcile_leases: []?metadata_reconcile_lease.ReconcileLeaseRecord,
    pending_reconcile_lease_retry_at_ms: []u64,
    pending_cluster_nodes: []bool,
    pending_cluster_node_retry_at_ms: []u64,
    pending_cluster_stores: []bool,
    pending_cluster_store_retry_at_ms: []u64,
    placement_intent_hashes: []u64,
    placement_intent_hash_valid: []bool,
    backend_runtimes: []db_mod.background_runtime.BackendRuntimeHandle,
    manual_clock: *platform_clock.ManualClock,
    reconcile_lease_update_in_flight: bool = false,
    metadata_proposal_in_flight: usize = 0,

    pub const ProgressPredicate = *const fn (*MetadataHttpClusterSimulation, *anyopaque) anyerror!bool;
    const min_pending_reconcile_lease_retry_ms: u64 = 250;
    const pending_cluster_publish_retry_ms: u64 = 250;

    pub fn init(
        alloc: std.mem.Allocator,
        metadata_group_id: u64,
        configs: []const raft_sim.ManagedHttpHostSimulationConfig,
        deps: []const raft_sim.ManagedHttpHostSimulationDeps,
    ) !MetadataHttpClusterSimulation {
        const manual_clock = try alloc.create(platform_clock.ManualClock);
        errdefer alloc.destroy(manual_clock);
        manual_clock.* = .{};
        manual_clock.setRealtimeNs(1_000 * std.time.ns_per_ms);

        const reconcile_leases = try alloc.alloc(metadata_reconcile_lease.State, configs.len);
        errdefer {
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        const pending_reconcile_leases = try alloc.alloc(?metadata_reconcile_lease.ReconcileLeaseRecord, configs.len);
        errdefer {
            alloc.free(pending_reconcile_leases);
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        @memset(pending_reconcile_leases, null);
        const pending_reconcile_lease_retry_at_ms = try alloc.alloc(u64, configs.len);
        errdefer {
            alloc.free(pending_reconcile_lease_retry_at_ms);
            alloc.free(pending_reconcile_leases);
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        @memset(pending_reconcile_lease_retry_at_ms, 0);
        const pending_cluster_nodes = try alloc.alloc(bool, configs.len);
        errdefer {
            alloc.free(pending_cluster_nodes);
            alloc.free(pending_reconcile_lease_retry_at_ms);
            alloc.free(pending_reconcile_leases);
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        @memset(pending_cluster_nodes, false);
        const pending_cluster_node_retry_at_ms = try alloc.alloc(u64, configs.len);
        errdefer {
            alloc.free(pending_cluster_node_retry_at_ms);
            alloc.free(pending_cluster_nodes);
            alloc.free(pending_reconcile_lease_retry_at_ms);
            alloc.free(pending_reconcile_leases);
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        @memset(pending_cluster_node_retry_at_ms, 0);
        const pending_cluster_stores = try alloc.alloc(bool, configs.len);
        errdefer {
            alloc.free(pending_cluster_stores);
            alloc.free(pending_cluster_node_retry_at_ms);
            alloc.free(pending_cluster_nodes);
            alloc.free(pending_reconcile_lease_retry_at_ms);
            alloc.free(pending_reconcile_leases);
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        @memset(pending_cluster_stores, false);
        const pending_cluster_store_retry_at_ms = try alloc.alloc(u64, configs.len);
        errdefer {
            alloc.free(pending_cluster_store_retry_at_ms);
            alloc.free(pending_cluster_stores);
            alloc.free(pending_cluster_node_retry_at_ms);
            alloc.free(pending_cluster_nodes);
            alloc.free(pending_reconcile_lease_retry_at_ms);
            alloc.free(pending_reconcile_leases);
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        @memset(pending_cluster_store_retry_at_ms, 0);
        const placement_intent_hashes = try alloc.alloc(u64, configs.len);
        errdefer {
            alloc.free(placement_intent_hashes);
            alloc.free(pending_cluster_store_retry_at_ms);
            alloc.free(pending_cluster_stores);
            alloc.free(pending_cluster_node_retry_at_ms);
            alloc.free(pending_cluster_nodes);
            alloc.free(pending_reconcile_lease_retry_at_ms);
            alloc.free(pending_reconcile_leases);
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        @memset(placement_intent_hashes, 0);
        const placement_intent_hash_valid = try alloc.alloc(bool, configs.len);
        errdefer {
            alloc.free(placement_intent_hash_valid);
            alloc.free(placement_intent_hashes);
            alloc.free(pending_cluster_store_retry_at_ms);
            alloc.free(pending_cluster_stores);
            alloc.free(pending_cluster_node_retry_at_ms);
            alloc.free(pending_cluster_nodes);
            alloc.free(pending_reconcile_lease_retry_at_ms);
            alloc.free(pending_reconcile_leases);
            alloc.free(reconcile_leases);
            alloc.destroy(manual_clock);
        }
        @memset(placement_intent_hash_valid, false);
        const backend_runtimes = try alloc.alloc(db_mod.background_runtime.BackendRuntimeHandle, configs.len);
        var backend_runtime_count: usize = 0;
        errdefer {
            for (backend_runtimes[0..backend_runtime_count]) |*runtime| runtime.deinit();
            alloc.free(backend_runtimes);
        }
        for (backend_runtimes) |*runtime| {
            runtime.* = try db_mod.background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .manual });
            backend_runtime_count += 1;
        }
        for (configs, 0..) |cfg, i| {
            reconcile_leases[i] = metadata_reconcile_lease.State.init(cfg.host.http.host.local_node_id, .{
                .lease_ttl_ms = 2_000,
                .clock = manual_clock.clock(),
            });
        }
        var raft_cluster = try raft_sim.ManagedHttpClusterSimulation.init(alloc, configs, deps);
        errdefer raft_cluster.deinit();
        var cluster = MetadataHttpClusterSimulation{
            .alloc = alloc,
            .metadata_group_id = metadata_group_id,
            .virtual_network = raft_cluster.network,
            .cluster = raft_cluster,
            .reconcile_leases = reconcile_leases,
            .pending_reconcile_leases = pending_reconcile_leases,
            .pending_reconcile_lease_retry_at_ms = pending_reconcile_lease_retry_at_ms,
            .pending_cluster_nodes = pending_cluster_nodes,
            .pending_cluster_node_retry_at_ms = pending_cluster_node_retry_at_ms,
            .pending_cluster_stores = pending_cluster_stores,
            .pending_cluster_store_retry_at_ms = pending_cluster_store_retry_at_ms,
            .placement_intent_hashes = placement_intent_hashes,
            .placement_intent_hash_valid = placement_intent_hash_valid,
            .backend_runtimes = backend_runtimes,
            .manual_clock = manual_clock,
            .reconcile_lease_update_in_flight = false,
            .metadata_proposal_in_flight = 0,
        };
        try cluster.registerVirtualNodes();
        return cluster;
    }

    pub fn deinit(self: *MetadataHttpClusterSimulation) void {
        self.cluster.deinit();
        self.alloc.free(self.reconcile_leases);
        self.alloc.free(self.pending_reconcile_leases);
        self.alloc.free(self.pending_reconcile_lease_retry_at_ms);
        self.alloc.free(self.pending_cluster_nodes);
        self.alloc.free(self.pending_cluster_node_retry_at_ms);
        self.alloc.free(self.pending_cluster_stores);
        self.alloc.free(self.pending_cluster_store_retry_at_ms);
        self.alloc.free(self.placement_intent_hashes);
        self.alloc.free(self.placement_intent_hash_valid);
        for (self.backend_runtimes) |*runtime| runtime.deinit();
        self.alloc.free(self.backend_runtimes);
        self.alloc.destroy(self.manual_clock);
        self.* = undefined;
    }

    pub fn startAll(self: *MetadataHttpClusterSimulation) !void {
        try self.registerVirtualNodes();
    }

    pub fn stopAll(self: *MetadataHttpClusterSimulation) void {
        self.cluster.stopAll();
    }

    pub fn node(self: *MetadataHttpClusterSimulation, index: usize) MetadataHttpNodeSimulation {
        return .{ .cluster = self, .index = index };
    }

    pub fn backendRuntime(self: *MetadataHttpClusterSimulation, index: usize) *db_mod.background_runtime.BackendRuntime {
        return self.backend_runtimes[index].ptr();
    }

    pub fn stepAll(self: *MetadataHttpClusterSimulation) anyerror!void {
        self.manual_clock.advanceMs(100);
        _ = try self.virtual_network.drainDue(null);
        for (0..self.cluster.nodes.len) |i| {
            try self.refreshOwnedMetadataRuntimes(i);
        }
        for (0..self.cluster.nodes.len) |i| {
            _ = try self.cluster.node(i).stepOnce();
            _ = try self.virtual_network.drainDue(null);
            try self.refreshOwnedMetadataRuntimes(i);
        }
        _ = try self.virtual_network.advanceTicks(1);
        for (0..self.cluster.nodes.len) |i| {
            try self.refreshOwnedMetadataRuntimes(i);
        }
    }

    pub fn runUntil(
        self: *MetadataHttpClusterSimulation,
        max_rounds: usize,
        context: *anyopaque,
        predicate: ProgressPredicate,
    ) !bool {
        var rounds: usize = 0;
        while (rounds < max_rounds) : (rounds += 1) {
            if (try predicate(self, context)) return true;
            try self.stepAll();
        }
        return try predicate(self, context);
    }

    pub fn assertProgress(
        self: *MetadataHttpClusterSimulation,
        label: []const u8,
        max_rounds: usize,
        context: *anyopaque,
        predicate: ProgressPredicate,
    ) !void {
        if (try self.runUntil(max_rounds, context, predicate)) return;
        std.debug.print("metadata cluster sim progress timeout label={s} rounds={d}\n", .{ label, max_rounds });
        return error.SimulationProgressTimeout;
    }

    pub fn restartNode(self: *MetadataHttpClusterSimulation, index: usize) !void {
        const was_started = self.cluster.started;
        if (was_started) self.cluster.node(index).stop();
        self.cluster.nodes[index].deinit();
        self.cluster.nodes[index] = try raft_sim.ManagedHttpHostSimulation.init(self.alloc, self.cluster.configs[index], self.cluster.deps[index]);
        try self.registerVirtualNode(index);
        self.reconcile_leases[index] = metadata_reconcile_lease.State.init(self.cluster.configs[index].host.http.host.local_node_id, .{
            .lease_ttl_ms = 2_000,
            .clock = self.manual_clock.clock(),
        });
        self.pending_reconcile_leases[index] = null;
        self.pending_reconcile_lease_retry_at_ms[index] = 0;
        self.pending_cluster_nodes[index] = false;
        self.pending_cluster_node_retry_at_ms[index] = 0;
        self.pending_cluster_stores[index] = false;
        self.pending_cluster_store_retry_at_ms[index] = 0;
        self.placement_intent_hash_valid[index] = false;
        if (was_started) try self.cluster.node(index).start();
    }

    pub fn waitForMetadataLeader(self: *MetadataHttpClusterSimulation, max_rounds: usize) !?usize {
        var ctx = MetadataLeaderProgressContext{};
        if (try self.runUntil(max_rounds, &ctx, metadataLeaderProgressPredicate)) return ctx.index;
        return null;
    }

    fn currentMetadataLeaderIndex(self: *MetadataHttpClusterSimulation) ?usize {
        for (self.cluster.nodes, 0..) |*sim, index| {
            if (sim.raftStatus(self.metadata_group_id)) |status| {
                if (status.soft.role == .leader) return index;
            }
        }
        return null;
    }

    fn currentMetadataLeaseHolderIndex(self: *MetadataHttpClusterSimulation) ?usize {
        for (self.reconcile_leases, 0..) |lease, index| {
            if (lease.stats().held_by_local) return index;
        }
        return null;
    }

    fn firstMetadataReplicaIndex(self: *MetadataHttpClusterSimulation) ?usize {
        for (self.cluster.nodes, 0..) |*sim, index| {
            if (sim.raftStatus(self.metadata_group_id) != null) return index;
        }
        return null;
    }

    pub fn waitForGroupStatus(self: *MetadataHttpClusterSimulation, group_id: u64, desired: raft_host.HostedReplicaStatus, max_rounds: usize) !bool {
        var ctx = MetadataGroupStatusProgressContext{
            .group_id = group_id,
            .desired = desired,
        };
        return try self.runUntil(max_rounds, &ctx, metadataGroupStatusProgressPredicate);
    }

    pub fn waitForNodeGroupStatus(
        self: *MetadataHttpClusterSimulation,
        index: usize,
        group_id: u64,
        desired: raft_host.HostedReplicaStatus,
        max_rounds: usize,
    ) !bool {
        var ctx = MetadataNodeGroupStatusProgressContext{
            .index = index,
            .group_id = group_id,
            .desired = desired,
        };
        return try self.runUntil(max_rounds, &ctx, metadataNodeGroupStatusProgressPredicate);
    }

    pub fn countGroupStatus(self: *MetadataHttpClusterSimulation, group_id: u64, desired: raft_host.HostedReplicaStatus) usize {
        var count: usize = 0;
        for (self.cluster.nodes) |*sim| {
            if (sim.status(group_id) == desired) count += 1;
        }
        return count;
    }

    pub fn waitForGroupStatusCount(
        self: *MetadataHttpClusterSimulation,
        group_id: u64,
        desired: raft_host.HostedReplicaStatus,
        expected_count: usize,
        max_rounds: usize,
    ) !bool {
        var ctx = MetadataGroupStatusCountProgressContext{
            .group_id = group_id,
            .desired = desired,
            .expected_count = expected_count,
        };
        return try self.runUntil(max_rounds, &ctx, metadataGroupStatusCountProgressPredicate);
    }

    pub fn bootstrapMetadataReplicas(self: *MetadataHttpClusterSimulation) !void {
        const node_count = self.cluster.nodes.len;
        const base_uris = try self.alloc.alloc([]u8, node_count);
        defer {
            for (base_uris) |uri| self.alloc.free(uri);
            self.alloc.free(base_uris);
        }
        for (0..self.cluster.nodes.len) |i| base_uris[i] = try self.nodeBaseUri(self.alloc, i);

        for (self.cluster.nodes, 0..) |*sim, i| {
            var updates = std.ArrayListUnmanaged(raft_metadata_apply.AppliedMetadataChange).empty;
            defer updates.deinit(self.alloc);

            const peer_node_ids = try self.alloc.alloc(u64, node_count - 1);
            var peer_index: usize = 0;
            errdefer self.alloc.free(peer_node_ids);
            for (0..node_count) |j| {
                if (j == i) continue;
                peer_node_ids[peer_index] = self.cluster.configs[j].host.http.host.local_node_id;
                peer_index += 1;
            }

            try updates.append(self.alloc, .{
                .upsert_replica_intent = .{
                    .record = .{
                        .group_id = self.metadata_group_id,
                        .replica_id = @as(u64, @intCast(i + 1)),
                        .local_node_id = self.cluster.configs[i].host.http.host.local_node_id,
                        .bootstrap_mode = .empty,
                    },
                    .peer_node_ids = peer_node_ids,
                },
            });
            defer if (updates.items.len > 0 and updates.items[0] == .upsert_replica_intent) self.alloc.free(updates.items[0].upsert_replica_intent.peer_node_ids);

            for (0..node_count) |j| {
                if (j == i) continue;
                try updates.append(self.alloc, .{
                    .upsert_peer_route = .{
                        .group_id = self.metadata_group_id,
                        .node_id = self.cluster.configs[j].host.http.host.local_node_id,
                        .endpoints = &.{.{
                            .protocol = .http,
                            .address = base_uris[j],
                            .metadata = "",
                        }},
                    },
                });
            }

            try sim.applyBatch(updates.items);
        }

        var rounds: usize = 0;
        while (rounds < 8 and self.firstMetadataReplicaIndex() == null) : (rounds += 1) try self.stepAll();
    }

    pub fn publishClusterNodes(self: *MetadataHttpClusterSimulation, proposer_index: usize) !void {
        const proposer = self.node(proposer_index);
        const projected_nodes = try proposer.listProjectedNodes(self.alloc);
        defer proposer.freeProjectedNodes(self.alloc, projected_nodes);
        const now_ms = self.manual_clock.clock().nowRealtimeMs();

        var changed = false;
        for (self.cluster.configs, 0..) |cfg, i| {
            const present = containsProjectedBootstrapNode(
                projected_nodes,
                cfg.host.http.host.local_node_id,
                "data",
            );
            if (present) {
                self.pending_cluster_nodes[i] = false;
                self.pending_cluster_node_retry_at_ms[i] = 0;
                continue;
            }
            if (self.pending_cluster_nodes[i] and self.pending_cluster_node_retry_at_ms[i] > now_ms) continue;
            try proposer.upsertNode(.{
                .node_id = cfg.host.http.host.local_node_id,
                .role = "data",
            });
            self.pending_cluster_nodes[i] = true;
            self.pending_cluster_node_retry_at_ms[i] = now_ms + pending_cluster_publish_retry_ms;
            changed = true;
        }
        if (changed) try self.stepAll();
    }

    pub fn publishClusterStores(self: *MetadataHttpClusterSimulation, proposer_index: usize) !void {
        const proposer = self.node(proposer_index);
        const projected_stores = try proposer.listProjectedStores(self.alloc);
        defer proposer.freeProjectedStores(self.alloc, projected_stores);
        const now_ms = self.manual_clock.clock().nowRealtimeMs();

        var changed = false;
        for (self.cluster.configs, 0..) |cfg, i| {
            const store_id = @as(u64, @intCast(i + 1));
            const present = containsProjectedBootstrapStore(
                projected_stores,
                store_id,
                cfg.host.http.host.local_node_id,
                "data",
                true,
            );
            if (present) {
                self.pending_cluster_stores[i] = false;
                self.pending_cluster_store_retry_at_ms[i] = 0;
                continue;
            }
            if (self.pending_cluster_stores[i] and self.pending_cluster_store_retry_at_ms[i] > now_ms) continue;
            try proposer.upsertStore(.{
                .store_id = store_id,
                .node_id = cfg.host.http.host.local_node_id,
                .role = "data",
                .live = true,
            });
            self.pending_cluster_stores[i] = true;
            self.pending_cluster_store_retry_at_ms[i] = now_ms + pending_cluster_publish_retry_ms;
            changed = true;
        }
        if (changed) try self.stepAll();
    }

    fn refreshOwnedMetadataRuntimes(self: *MetadataHttpClusterSimulation, index: usize) anyerror!void {
        try self.refreshLocalPlacementIntents(index);
        if (self.metadata_proposal_in_flight == 0) {
            _ = try self.ensureReconcileLease(index);
        }
        try self.refreshLocalTransitions(index);
    }

    fn ensureReconcileLease(self: *MetadataHttpClusterSimulation, index: usize) anyerror!bool {
        const sim = self.cluster.node(index);
        const store = sim.runtime.svc.host.owned_metadata_store orelse return false;
        const now_ms = self.reconcile_leases[index].nowMs();
        const projected = try store.getReconcileLease(self.metadata_group_id);
        const is_local_leader = sim.runtime.svc.host.http_host.host.isLocalLeader(self.metadata_group_id);
        const has_lease = self.reconcile_leases[index].observe(is_local_leader, projected, now_ms);
        const local_node_id = self.cluster.configs[index].host.http.host.local_node_id;
        const should_claim_stale_leader_lease =
            is_local_leader and
            projected != null and
            projected.?.owner_node_id != 0 and
            projected.?.owner_node_id != local_node_id;
        if (self.pending_reconcile_leases[index]) |pending| {
            if (projected) |current| {
                if (current.owner_node_id == pending.owner_node_id and current.expires_at_ms >= pending.expires_at_ms) {
                    self.pending_reconcile_leases[index] = null;
                    self.pending_reconcile_lease_retry_at_ms[index] = 0;
                }
            } else if (pending.expires_at_ms <= now_ms) {
                self.pending_reconcile_leases[index] = null;
                self.pending_reconcile_lease_retry_at_ms[index] = 0;
            }
        }
        const desired = self.reconcile_leases[index].desiredRecord(now_ms);
        const pending_covers_desired = if (self.pending_reconcile_leases[index]) |pending|
            pending.owner_node_id == local_node_id and self.pending_reconcile_lease_retry_at_ms[index] > now_ms
        else
            false;
        if (!self.reconcile_lease_update_in_flight and
            !pending_covers_desired and
            (should_claim_stale_leader_lease or self.reconcile_leases[index].shouldRenew(is_local_leader, projected, now_ms)))
        {
            self.reconcile_lease_update_in_flight = true;
            defer self.reconcile_lease_update_in_flight = false;
            self.node(index).upsertReconcileLease(desired) catch |err| switch (err) {
                error.NotLeader, error.UnknownGroup => {},
                else => return err,
            };
            self.pending_reconcile_leases[index] = desired;
            self.pending_reconcile_lease_retry_at_ms[index] = now_ms + pendingReconcileLeaseRetryMs(self.reconcile_leases[index].config.lease_ttl_ms);
        }
        return has_lease;
    }

    fn refreshLocalPlacementIntents(self: *MetadataHttpClusterSimulation, index: usize) !void {
        const sim = self.cluster.node(index);
        const store = sim.runtime.svc.host.owned_metadata_store orelse return;
        const local_node_id = self.cluster.configs[index].host.http.host.local_node_id;
        var local = try store.listLocalPlacementIntents(self.alloc, self.metadata_group_id, local_node_id);
        defer store.freePlacementIntents(self.alloc, local);

        if (!containsLocalIntent(local, self.metadata_group_id)) {
            if (sim.runtime.svc.host.http_host.host.raftStatus(self.metadata_group_id)) |status| {
                const expanded = try self.alloc.alloc(raft_reconciler.PlacementIntent, local.len + 1);
                @memcpy(expanded[0..local.len], local);
                self.alloc.free(local);
                local = expanded;
                local[local.len - 1] = .{
                    .record = .{
                        .group_id = self.metadata_group_id,
                        .replica_id = local_node_id,
                        .local_node_id = local_node_id,
                        .bootstrap_mode = .persisted,
                    },
                    .peer_node_ids = try allocPeerNodeIdsExcludingSelf(self.alloc, status.conf_state.voters, local_node_id),
                };
            }
        }

        const local_hash = hashPlacementIntentSlice(local);
        if (self.placement_intent_hash_valid[index] and
            self.placement_intent_hashes[index] == local_hash and
            sim.runtime.svc.host.http_host.host.raftStatus(self.metadata_group_id) != null)
        {
            return;
        }

        const base_uris = try self.alloc.alloc([]u8, self.cluster.nodes.len);
        defer {
            for (base_uris) |uri| self.alloc.free(uri);
            self.alloc.free(base_uris);
        }
        for (0..self.cluster.nodes.len) |i| base_uris[i] = try self.nodeBaseUri(self.alloc, i);

        for (local) |intent| {
            for (intent.peer_node_ids) |node_id| {
                if (node_id == local_node_id) continue;
                const peer_index = self.indexForNodeId(node_id) orelse continue;
                try sim.runtime.svc.host.apply(.{
                    .peer_route = .{
                        .upsert = .{
                            .group_id = intent.record.group_id,
                            .node_id = node_id,
                            .endpoints = &.{.{
                                .protocol = .http,
                                .address = base_uris[peer_index],
                                .metadata = "",
                            }},
                        },
                    },
                });
            }
        }

        try sim.runtime.svc.host.replacePlacementIntents(local);
        _ = try sim.runtime.svc.host.reconcileOnce();
        self.placement_intent_hashes[index] = local_hash;
        self.placement_intent_hash_valid[index] = true;
    }

    fn refreshLocalTransitions(self: *MetadataHttpClusterSimulation, index: usize) !void {
        const sim = self.cluster.node(index);
        const transition_svc = if (sim.runtime.svc.transition_svc) |*svc| svc else return;
        const store = sim.runtime.svc.host.owned_metadata_store orelse return;

        const split_records = try store.listSplitTransitions(self.alloc, self.metadata_group_id);
        defer store.freeSplitTransitions(self.alloc, split_records);
        const merge_records = try store.listMergeTransitions(self.alloc, self.metadata_group_id);
        defer store.freeMergeTransitions(self.alloc, merge_records);

        var split_index: usize = 0;
        while (split_index < transition_svc.pending_split.items.len) {
            const transition_id = transition_svc.pending_split.items[split_index].transition_id;
            if (findSplitTransition(split_records, transition_id) == null) {
                _ = transition_svc.removeSplit(transition_id);
                continue;
            }
            split_index += 1;
        }
        for (split_records) |record| {
            if (findSplitTransition(transition_svc.pending_split.items, record.transition_id) == null) {
                try transition_svc.submitSplit(record);
            }
        }

        var merge_index: usize = 0;
        while (merge_index < transition_svc.pending_merge.items.len) {
            const transition_id = transition_svc.pending_merge.items[merge_index].transition_id;
            if (findMergeTransition(merge_records, transition_id) == null) {
                _ = transition_svc.removeMerge(transition_id);
                continue;
            }
            merge_index += 1;
        }
        for (merge_records) |record| {
            if (findMergeTransition(transition_svc.pending_merge.items, record.transition_id) == null) {
                try transition_svc.submitMerge(record);
            }
        }

        sim.runtime.svc.metrics.queued_split_transitions = transition_svc.metrics.queued_split_transitions;
        sim.runtime.svc.metrics.queued_merge_transitions = transition_svc.metrics.queued_merge_transitions;
    }

    fn indexForNodeId(self: *const MetadataHttpClusterSimulation, node_id: u64) ?usize {
        for (self.cluster.configs, 0..) |cfg, i| {
            if (cfg.host.http.host.local_node_id == node_id) return i;
        }
        return null;
    }

    fn nodeBaseUri(self: *MetadataHttpClusterSimulation, alloc: std.mem.Allocator, index: usize) ![]u8 {
        return try raft_sim.VirtualHttpNetwork.baseUri(alloc, self.cluster.configs[index].host.http.host.local_node_id);
    }

    fn registerVirtualNodes(self: *MetadataHttpClusterSimulation) !void {
        for (0..self.cluster.nodes.len) |i| try self.registerVirtualNode(i);
    }

    fn registerVirtualNode(self: *MetadataHttpClusterSimulation, index: usize) !void {
        const node_id = self.cluster.configs[index].host.http.host.local_node_id;
        try self.virtual_network.registerNode(node_id, self.cluster.node(index).runtime.svc.host.http_host.server.executor());
    }
};

fn metadataLeaderProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataLeaderProgressContext = @ptrCast(@alignCast(ptr));
    ctx.index = cluster.currentMetadataLeaderIndex();
    return ctx.index != null;
}

fn metadataSpecificLeaderProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataSpecificLeaderProgressContext = @ptrCast(@alignCast(ptr));
    return cluster.currentMetadataLeaderIndex() == ctx.index;
}

fn metadataGroupStatusProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataGroupStatusProgressContext = @ptrCast(@alignCast(ptr));
    for (cluster.cluster.nodes) |*sim| {
        if (sim.status(ctx.group_id) != ctx.desired) return false;
    }
    return true;
}

fn metadataNodeGroupStatusProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataNodeGroupStatusProgressContext = @ptrCast(@alignCast(ptr));
    return cluster.cluster.node(ctx.index).status(ctx.group_id) == ctx.desired;
}

fn metadataGroupStatusCountProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataGroupStatusCountProgressContext = @ptrCast(@alignCast(ptr));
    return cluster.countGroupStatus(ctx.group_id, ctx.desired) == ctx.expected_count;
}

fn metadataSplitTransitionProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataSplitTransitionProgressContext = @ptrCast(@alignCast(ptr));
    if (anyNodeSteppedSplitTransitions(cluster)) return true;
    const leader_index = currentMetadataLeaderIndex(cluster) orelse return false;
    const observation = (try cluster.node(leader_index).observeSplitTransition(ctx.transition_id)) orelse return false;
    return observation.status.phase != .prepare or observation.status.source_split_phase != .prepare;
}

fn metadataMergeTransitionProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataMergeTransitionProgressContext = @ptrCast(@alignCast(ptr));
    if (anyNodeSteppedMergeTransitions(cluster)) return true;
    const leader_index = currentMetadataLeaderIndex(cluster) orelse return false;
    const observation = (try cluster.node(leader_index).observeMergeTransition(ctx.transition_id)) orelse return false;
    return observation.donor.phase != .prepare or observation.receiver.phase != .prepare;
}

fn metadataSplitTransitionFinalizedProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataSplitTransitionFinalizedProgressContext = @ptrCast(@alignCast(ptr));
    const observer_index = ctx.observer_index orelse (currentMetadataLeaderIndex(cluster) orelse ctx.fallback_index);
    const observation = (try cluster.node(observer_index).observeSplitTransition(ctx.transition_id)) orelse return false;
    return observation.status.phase == .finalized;
}

fn metadataSplitFinalizedOrGroupReadyProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataSplitFinalizedOrGroupReadyProgressContext = @ptrCast(@alignCast(ptr));
    if (currentGroupLeaderIndex(cluster, ctx.group_id)) |index| {
        ctx.group_index = index;
        return true;
    }
    for (0..cluster.cluster.nodes.len) |i| {
        if (cluster.node(i).status(ctx.group_id) == .active) {
            ctx.group_index = i;
            return true;
        }
    }
    const observer_index = currentMetadataLeaderIndex(cluster) orelse ctx.fallback_index;
    if (try cluster.node(observer_index).observeSplitTransition(ctx.transition_id)) |observation| {
        ctx.finalized = observation.status.phase == .finalized;
        return ctx.finalized;
    }
    return false;
}

fn metadataMergeTransitionFinalizedProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataMergeTransitionFinalizedProgressContext = @ptrCast(@alignCast(ptr));
    const observer_index = ctx.observer_index orelse (currentMetadataLeaderIndex(cluster) orelse ctx.fallback_index);
    const observation = (try cluster.node(observer_index).observeMergeTransition(ctx.transition_id)) orelse return false;
    return observation.receiver.phase == .finalized;
}

fn waitForSplitTransitionFinalized(
    cluster: *MetadataHttpClusterSimulation,
    transition_id: u64,
    observer_index: ?usize,
    fallback_index: usize,
    max_rounds: usize,
) !bool {
    var ctx = MetadataSplitTransitionFinalizedProgressContext{
        .transition_id = transition_id,
        .observer_index = observer_index,
        .fallback_index = fallback_index,
    };
    return try cluster.runUntil(max_rounds, &ctx, metadataSplitTransitionFinalizedProgressPredicate);
}

fn waitForMergeTransitionFinalized(
    cluster: *MetadataHttpClusterSimulation,
    transition_id: u64,
    observer_index: ?usize,
    fallback_index: usize,
    max_rounds: usize,
) !bool {
    var ctx = MetadataMergeTransitionFinalizedProgressContext{
        .transition_id = transition_id,
        .observer_index = observer_index,
        .fallback_index = fallback_index,
    };
    return try cluster.runUntil(max_rounds, &ctx, metadataMergeTransitionFinalizedProgressPredicate);
}

fn findProjectedStore(records: []const metadata_table_manager.StoreRecord, store_id: u64) ?metadata_table_manager.StoreRecord {
    for (records) |record| {
        if (record.store_id == store_id) return record;
    }
    return null;
}

fn findSplitTransition(records: []const transition_state.SplitTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findMergeTransition(records: []const transition_state.MergeTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn containsLocalIntent(intents: []const raft_reconciler.PlacementIntent, group_id: u64) bool {
    for (intents) |intent| {
        if (intent.record.group_id == group_id) return true;
    }
    return false;
}

fn allocPeerNodeIdsExcludingSelf(alloc: std.mem.Allocator, voters: []const u64, local_node_id: u64) ![]u64 {
    var count: usize = 0;
    for (voters) |node_id| {
        if (node_id == local_node_id) continue;
        count += 1;
    }
    if (count == 0) return &.{};

    const peer_node_ids = try alloc.alloc(u64, count);
    var index: usize = 0;
    errdefer alloc.free(peer_node_ids);
    for (voters) |node_id| {
        if (node_id == local_node_id) continue;
        peer_node_ids[index] = node_id;
        index += 1;
    }
    return peer_node_ids;
}

fn pendingReconcileLeaseRetryMs(lease_ttl_ms: u64) u64 {
    const half_ttl = if (lease_ttl_ms > 1) lease_ttl_ms / 2 else lease_ttl_ms;
    return @max(@as(u64, 250), half_ttl);
}

fn hashPlacementIntentSlice(intents: []const raft_reconciler.PlacementIntent) u64 {
    var hasher = std.hash.Wyhash.init(0);
    for (intents) |intent| hashPlacementIntent(&hasher, intent);
    return hasher.final();
}

fn hashPlacementIntent(hasher: *std.hash.Wyhash, intent: raft_reconciler.PlacementIntent) void {
    hashPlacementU64(hasher, intent.record.group_id);
    hashPlacementU64(hasher, intent.record.replica_id);
    hashPlacementU64(hasher, intent.record.local_node_id);
    hashPlacementU64(hasher, @intFromEnum(intent.record.bootstrap_mode));
    hashPlacementU64(hasher, intent.record.metadata_version);
    hashPlacementU64(hasher, intent.store_id);
    hashPlacementU64(hasher, intent.peer_node_ids.len);
    for (intent.peer_node_ids) |node_id| hashPlacementU64(hasher, node_id);

    if (intent.record.snapshot_bootstrap) |snapshot| {
        hashPlacementU64(hasher, 1);
        hashPlacementU64(hasher, snapshot.from_node_id);
        hashPlacementU64(hasher, snapshot.term);
        hasher.update(snapshot.snapshot_id);
        hasher.update(snapshot.uri);
    } else {
        hashPlacementU64(hasher, 0);
    }

    if (intent.record.backup_restore_bootstrap) |backup| {
        hashPlacementU64(hasher, 1);
        hasher.update(backup.backup_id);
        hasher.update(backup.location);
        hasher.update(backup.snapshot_path);
    } else {
        hashPlacementU64(hasher, 0);
    }
}

fn hashPlacementU64(hasher: *std.hash.Wyhash, value: u64) void {
    var numeric = value;
    hasher.update(std.mem.asBytes(&numeric));
}

fn placementIntentEquals(
    left: raft_reconciler.PlacementIntent,
    right: raft_reconciler.PlacementIntent,
) bool {
    if (left.record.group_id != right.record.group_id) return false;
    if (left.record.replica_id != right.record.replica_id) return false;
    if (left.record.local_node_id != right.record.local_node_id) return false;
    if (left.record.bootstrap_mode != right.record.bootstrap_mode) return false;
    if (left.record.metadata_version != right.record.metadata_version) return false;
    if ((left.record.snapshot_bootstrap == null) != (right.record.snapshot_bootstrap == null)) return false;
    if ((left.record.backup_restore_bootstrap == null) != (right.record.backup_restore_bootstrap == null)) return false;
    if (left.record.snapshot_bootstrap) |snapshot| {
        const other = right.record.snapshot_bootstrap.?;
        if (snapshot.from_node_id != other.from_node_id) return false;
        if (snapshot.term != other.term) return false;
        if (!std.mem.eql(u8, snapshot.snapshot_id, other.snapshot_id)) return false;
        if (!std.mem.eql(u8, snapshot.uri, other.uri)) return false;
    }
    if (left.record.backup_restore_bootstrap) |backup| {
        const other = right.record.backup_restore_bootstrap.?;
        if (!std.mem.eql(u8, backup.backup_id, other.backup_id)) return false;
        if (!std.mem.eql(u8, backup.location, other.location)) return false;
        if (!std.mem.eql(u8, backup.snapshot_path, other.snapshot_path)) return false;
    }
    if (left.store_id != right.store_id) return false;
    return std.mem.eql(u64, left.peer_node_ids, right.peer_node_ids);
}

fn containsProjectedPlacementIntent(
    projected_intents: []const raft_reconciler.PlacementIntent,
    expected: raft_reconciler.PlacementIntent,
) bool {
    for (projected_intents) |intent| {
        if (intent.record.group_id != expected.record.group_id) continue;
        if (placementIntentEquals(intent, expected)) return true;
    }
    return false;
}

fn containsProjectedPlacementIntentGroup(
    projected_intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) bool {
    for (projected_intents) |intent| {
        if (intent.record.group_id == group_id) return true;
    }
    return false;
}

fn anyNodeSteppedSplitTransitions(cluster: *MetadataHttpClusterSimulation) bool {
    for (cluster.cluster.nodes, 0..) |_, index| {
        if (cluster.node(index).serviceMetrics().stepped_split_transitions > 0) return true;
    }
    return false;
}

fn anyNodeSteppedMergeTransitions(cluster: *MetadataHttpClusterSimulation) bool {
    for (cluster.cluster.nodes, 0..) |_, index| {
        if (cluster.node(index).serviceMetrics().stepped_merge_transitions > 0) return true;
    }
    return false;
}

fn currentMetadataLeaderIndex(cluster: *MetadataHttpClusterSimulation) ?usize {
    for (cluster.cluster.nodes, 0..) |*sim, index| {
        if (sim.raftStatus(cluster.metadata_group_id)) |status| {
            if (status.soft.role == .leader) return index;
        }
    }
    return null;
}

fn currentGroupLeaderIndex(cluster: *MetadataHttpClusterSimulation, group_id: u64) ?usize {
    for (cluster.cluster.nodes, 0..) |*sim, index| {
        if (sim.raftStatus(group_id)) |status| {
            if (status.soft.role == .leader) return index;
        }
        if (sim.leaderId(group_id)) |leader_id| {
            if (leader_id == cluster.cluster.configs[index].host.http.host.local_node_id) return index;
        }
    }
    return null;
}

fn waitForGroupLeaderIndex(
    cluster: *MetadataHttpClusterSimulation,
    group_id: u64,
    max_rounds: usize,
) !?usize {
    var ctx = GroupLeaderProgressContext{ .group_id = group_id };
    if (try cluster.runUntil(max_rounds, &ctx, groupLeaderProgressPredicate)) return ctx.index;
    for (0..cluster.cluster.nodes.len) |i| {
        if (cluster.node(i).status(group_id) == .active) return i;
    }
    return null;
}

fn waitForGroupElectedLeaderIndex(
    cluster: *MetadataHttpClusterSimulation,
    group_id: u64,
    max_rounds: usize,
) !?usize {
    var ctx = GroupLeaderProgressContext{ .group_id = group_id };
    if (try cluster.runUntil(max_rounds, &ctx, groupLeaderProgressPredicate)) return ctx.index;
    return null;
}

const GroupLeaderProgressContext = struct {
    group_id: u64,
    index: ?usize = null,
};

fn groupLeaderProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *GroupLeaderProgressContext = @ptrCast(@alignCast(ptr));
    ctx.index = currentGroupLeaderIndex(cluster, ctx.group_id);
    return ctx.index != null;
}

fn requireLeasedReconcile(
    node: MetadataHttpNodeSimulation,
    loop: *metadata_control_loop.MetadataControlLoop,
) !metadata_control_loop.ReconcileSummary {
    var rounds: usize = 0;
    while (rounds < 32) : (rounds += 1) {
        const cluster = @constCast(node.cluster);
        const leader_index = currentMetadataLeaderIndex(cluster) orelse node.index;
        const leader_node = node.cluster.node(leader_index);
        if (try leader_node.reconcileOnceIfLeaseHeld(loop)) |summary| return summary;
        _ = try cluster.ensureReconcileLease(leader_index);
        try leader_node.cluster.stepAll();
    }
    return error.ReconcileLeaseNotHeld;
}

fn bootstrapDesiredLoop(
    node: MetadataHttpNodeSimulation,
    loop: *metadata_control_loop.MetadataControlLoop,
) !void {
    try loop.stateRef().syncProjected(node);
    try loop.stateRef().seedDesiredFromProjected();
}

fn currentGroupStatusTimestampMs() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

fn metadataBlackholeEndpoints() []const peer_resolver.PeerEndpoint {
    return (&[_]peer_resolver.PeerEndpoint{
        .{ .protocol = .http, .address = "http://127.0.0.1:1", .metadata = "" },
    })[0..];
}

fn isolateMetadataNode(cluster: *MetadataHttpClusterSimulation, isolated_index: usize) !void {
    const isolated_node_id = cluster.cluster.configs[isolated_index].host.http.host.local_node_id;
    for (cluster.cluster.configs, 0..) |cfg, index| {
        if (index == isolated_index) continue;
        const peer_node_id = cfg.host.http.host.local_node_id;
        _ = try cluster.node(isolated_index).sim().upsertPeerRoute(cluster.metadata_group_id, peer_node_id, metadataBlackholeEndpoints());
        _ = try cluster.node(index).sim().upsertPeerRoute(cluster.metadata_group_id, isolated_node_id, metadataBlackholeEndpoints());
    }
}

fn waitForMetadataLeaderExcluding(
    cluster: *MetadataHttpClusterSimulation,
    excluded_index: usize,
    max_rounds: usize,
) !?usize {
    var ctx = MetadataLeaderExcludingProgressContext{ .excluded_index = excluded_index };
    if (try cluster.runUntil(max_rounds, &ctx, metadataLeaderExcludingProgressPredicate)) return ctx.index;
    return null;
}

fn waitForMetadataLeaderIndex(
    cluster: *MetadataHttpClusterSimulation,
    index: usize,
    max_rounds: usize,
) !bool {
    var ctx = MetadataSpecificLeaderProgressContext{ .index = index };
    return try cluster.runUntil(max_rounds, &ctx, metadataSpecificLeaderProgressPredicate);
}

const MetadataLeaderExcludingProgressContext = struct {
    excluded_index: usize,
    rounds: usize = 0,
    index: ?usize = null,
};

fn metadataLeaderExcludingProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *MetadataLeaderExcludingProgressContext = @ptrCast(@alignCast(ptr));
    if (currentMetadataLeaderIndex(cluster)) |index| {
        if (index != ctx.excluded_index) {
            ctx.index = index;
            return true;
        }
    }
    if (ctx.rounds > 0 and ctx.rounds % 8 == 0) {
        for (0..cluster.cluster.nodes.len) |index| {
            if (index == ctx.excluded_index) continue;
            try cluster.node(index).campaignMetadataGroup();
            break;
        }
    }
    ctx.rounds += 1;
    return false;
}

fn findAdminTableByName(snapshot: *const metadata_api.AdminSnapshot, table_name: []const u8) ?*const metadata_table_manager.TableRecord {
    for (snapshot.tables) |*table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn findRangeForKey(records: []const metadata_table_manager.RangeRecord, table_id: u64, key: []const u8) ?u64 {
    for (records) |record| {
        if (record.table_id != table_id) continue;
        if (key.len > 0 and record.start_key.len > 0 and std.mem.order(u8, key, record.start_key) == .lt) continue;
        if (record.end_key) |end_key| {
            if (std.mem.order(u8, key, end_key) != .lt) continue;
        }
        return record.group_id;
    }
    return null;
}

fn deriveTransitionId(table_name: []const u8, key: []const u8, seed: u64) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(key);
    const id = hasher.final();
    return if (id == 0) 1 else id;
}

fn deriveGroupId(table_name: []const u8, key: []const u8, seed: u64, reserved: u64) u64 {
    var hasher = std.hash.Wyhash.init(seed);
    hasher.update(table_name);
    hasher.update(&[_]u8{0});
    hasher.update(key);
    var id = hasher.final();
    if (id == 0 or id == reserved) id +%= 1;
    if (id == 0) return reserved +% 1;
    return id;
}

fn currentMetadataMutationNode(node: MetadataHttpNodeSimulation) MetadataHttpNodeSimulation {
    if (node.cluster.currentMetadataLeaderIndex()) |index| return node.cluster.node(index);
    return node;
}

fn applyCreateTableMutation(
    node: MetadataHttpNodeSimulation,
    table_name: []const u8,
    req: api_tables.CreateTableRequest,
) !void {
    const target = currentMetadataMutationNode(node);
    var workflow = metadata_table_workflow.TableWorkflow.init(node.cluster.alloc);
    defer workflow.deinit();
    const table = api_tables.deriveTableRecord(table_name, req);
    _ = try workflow.createTable(&target, table, api_tables.deriveInitialRange(table));
    try target.runRound();
}

fn applyDropTableMutation(
    node: MetadataHttpNodeSimulation,
    alloc: std.mem.Allocator,
    table_name: []const u8,
) !void {
    const target = currentMetadataMutationNode(node);
    var snapshot = try target.adminSnapshot();
    defer target.freeAdminSnapshot(&snapshot);
    const table = api_tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    for (snapshot.ranges) |record| {
        if (record.table_id != table.table_id) continue;
        try target.removeRange(record.group_id);
    }
    try target.removeTable(table.table_id);
    try target.runRound();
    _ = alloc;
}

fn applyUpdateSchemaMutation(
    node: MetadataHttpNodeSimulation,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    schema_json: []const u8,
) !void {
    const target = currentMetadataMutationNode(node);
    var snapshot = try target.adminSnapshot();
    defer target.freeAdminSnapshot(&snapshot);
    const table = api_tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

    const updated = try api_tables.applySchemaUpdateRecord(alloc, table, schema_json);
    defer metadata_table_manager.freeTable(alloc, updated);
    try target.upsertTable(updated);
    try target.runRound();
}

fn applyCreateIndexMutation(
    node: MetadataHttpNodeSimulation,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
    index_json: []const u8,
) !void {
    const target = currentMetadataMutationNode(node);
    var snapshot = try target.adminSnapshot();
    defer target.freeAdminSnapshot(&snapshot);
    const table = api_tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

    var updated = table.*;
    updated.indexes_json = try @import("../api/indexes.zig").addIndexToTableIndexesJson(alloc, table.indexes_json, index_name, index_json);
    defer alloc.free(updated.indexes_json);
    try target.upsertTable(updated);
    try target.runRound();
}

fn applyDropIndexMutation(
    node: MetadataHttpNodeSimulation,
    alloc: std.mem.Allocator,
    table_name: []const u8,
    index_name: []const u8,
) !void {
    const target = currentMetadataMutationNode(node);
    var snapshot = try target.adminSnapshot();
    defer target.freeAdminSnapshot(&snapshot);
    const table = api_tables.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;

    const next = (try @import("../api/indexes.zig").removeIndexFromTableIndexesJson(alloc, table.indexes_json, index_name)) orelse return error.IndexNotFound;
    defer alloc.free(next);
    var updated = table.*;
    updated.indexes_json = next;
    try target.upsertTable(updated);
    try target.runRound();
}

const PublicApiStatusSource = struct {
    const MetadataSnapshotMode = enum {
        local,
        leader_backed,
    };

    node: MetadataHttpNodeSimulation,
    metadata_snapshot_mode: MetadataSnapshotMode = .local,

    fn metadataNode(self: @This()) MetadataHttpNodeSimulation {
        return switch (self.metadata_snapshot_mode) {
            .local => self.node,
            .leader_backed => currentMetadataMutationNode(self.node),
        };
    }

    fn iface(self: *@This()) api_http_server.StatusSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = adminSnapshot,
                .cached_admin_snapshot = cachedAdminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .create_table = createTable,
                .drop_table = dropTable,
                .update_schema = updateSchema,
                .create_index = createIndex,
                .drop_index = dropIndex,
            },
        };
    }

    fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.metadataNode().metadataStatus();
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.metadataNode().adminSnapshot();
    }

    fn cachedAdminSnapshot(ptr: *anyopaque) !?metadata_api.AdminSnapshot {
        return try adminSnapshot(ptr);
    }

    fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.metadataNode().freeAdminSnapshot(snapshot);
    }

    fn createTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: api_tables.CreateTableRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = alloc;
        try applyCreateTableMutation(self.node, table_name, req);
    }

    fn dropTable(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try applyDropTableMutation(self.node, alloc, table_name);
    }

    fn updateSchema(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, schema_json: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try applyUpdateSchemaMutation(self.node, alloc, table_name, schema_json);
    }

    fn createIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8, index_json: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try applyCreateIndexMutation(self.node, alloc, table_name, index_name, index_json);
    }

    fn dropIndex(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, index_name: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try applyDropIndexMutation(self.node, alloc, table_name, index_name);
    }
};

const MetadataAdminSimSource = struct {
    node: MetadataHttpNodeSimulation,

    fn iface(self: *@This()) metadata_http_server.AdminSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .status = status,
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
                .upsert_node = upsertNode,
                .request_node_shutdown = requestNodeShutdown,
                .cancel_node_shutdown = cancelNodeShutdown,
                .finalize_node_shutdown = finalizeNodeShutdown,
                .upsert_store = upsertStore,
                .trigger_reallocate = triggerReallocate,
                .request_split = requestSplit,
                .request_merge = requestMerge,
            },
        };
    }

    fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.node.metadataStatus();
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.node.adminSnapshot();
    }

    fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.node.freeAdminSnapshot(snapshot);
    }

    fn upsertNode(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.NodeRecord) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        defer metadata_table_manager.freeNode(alloc, record);
        const target = currentMetadataMutationNode(self.node);
        try target.registerNode(record);
        try target.runRound();
    }

    fn requestNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const target = currentMetadataMutationNode(self.node);
        try target.requestNodeShutdown(node_id);
        try target.runRound();
    }

    fn cancelNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const target = currentMetadataMutationNode(self.node);
        try target.cancelNodeShutdown(node_id);
        try target.runRound();
    }

    fn finalizeNodeShutdown(ptr: *anyopaque, node_id: u64) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const target = currentMetadataMutationNode(self.node);
        try target.finalizeNodeShutdown(node_id);
        try target.runRound();
    }

    fn upsertStore(ptr: *anyopaque, alloc: std.mem.Allocator, record: metadata_table_manager.StoreRecord) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        defer metadata_table_manager.freeStore(alloc, record);
        const target = currentMetadataMutationNode(self.node);
        try target.registerStore(record);
        try target.runRound();
    }

    fn triggerReallocate(ptr: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const target = currentMetadataMutationNode(self.node);
        try target.requestReallocation(1);
        try target.runRound();
    }

    fn requestSplit(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: metadata_http_server.SplitRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const target = currentMetadataMutationNode(self.node);
        var snapshot = try target.adminSnapshot();
        defer target.freeAdminSnapshot(&snapshot);
        const table = findAdminTableByName(&snapshot, table_name) orelse return error.TableNotFound;
        const source_group_id = req.source_group_id orelse findRangeForKey(snapshot.ranges, table.table_id, req.split_key) orelse return error.RangeNotFound;

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        try workflow.bootstrapDesiredFromCommitted(&target);
        _ = try workflow.requestSplit(&target, .{
            .transition_id = req.transition_id orelse deriveTransitionId(table_name, req.split_key, 0x53504c54),
            .table_id = table.table_id,
            .source_group_id = source_group_id,
            .destination_group_id = req.destination_group_id orelse deriveGroupId(table_name, req.split_key, 0x53504c47, source_group_id),
            .split_key = req.split_key,
        });
        try target.runRound();
    }

    fn requestMerge(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, req: metadata_http_server.MergeRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const target = currentMetadataMutationNode(self.node);
        var snapshot = try target.adminSnapshot();
        defer target.freeAdminSnapshot(&snapshot);
        const table = findAdminTableByName(&snapshot, table_name) orelse return error.TableNotFound;

        var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
        defer workflow.deinit();
        try workflow.bootstrapDesiredFromCommitted(&target);
        _ = try workflow.requestMerge(&target, .{
            .transition_id = req.transition_id orelse deriveTransitionId(table_name, table_name, 0x4d524754),
            .table_id = table.table_id,
            .donor_group_id = req.donor_group_id,
            .receiver_group_id = req.receiver_group_id,
            .allow_doc_identity_reassignment = req.allow_doc_identity_reassignment,
        });
        try target.runRound();
    }
};

const PublicApiCatalogSource = struct {
    node: MetadataHttpNodeSimulation,
    metadata_snapshot_mode: PublicApiStatusSource.MetadataSnapshotMode = .local,

    fn metadataNode(self: @This()) MetadataHttpNodeSimulation {
        return switch (self.metadata_snapshot_mode) {
            .local => self.node,
            .leader_backed => currentMetadataMutationNode(self.node),
        };
    }

    fn iface(self: *@This()) api_table_catalog.CatalogSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .admin_snapshot = adminSnapshot,
                .free_admin_snapshot = freeAdminSnapshot,
            },
        };
    }

    fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return try self.metadataNode().adminSnapshot();
    }

    fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.metadataNode().freeAdminSnapshot(snapshot);
    }
};

fn PublicApiRouter(comptime N: usize) type {
    return struct {
        node: MetadataHttpNodeSimulation,
        cluster: *MetadataHttpClusterSimulation,
        api_base_uris: *const [N][]const u8,

        fn iface(self: *@This()) api_table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @as(u64, @intCast(self.node.index + 1));
        }

        fn localStatus(ptr: *anyopaque, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.node.status(group_id);
        }

        fn groupLeaderNodeId(ptr: *anyopaque, group_id: u64) ?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const leader_index = currentGroupLeaderIndex(self.cluster, group_id) orelse return null;
            return self.cluster.cluster.configs[leader_index].host.http.host.local_node_id;
        }

        fn nodeStatus(ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.cluster.cluster.nodes.len) return .absent;
            return self.cluster.node(@intCast(node_id - 1)).status(group_id);
        }

        fn nodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.api_base_uris.len) return null;
            return try alloc.dupe(u8, self.api_base_uris[node_id - 1]);
        }
    };
}

const SimAuthManager = struct {
    store: usermgr.MemoryStore,
    policy_store: casbin.MemoryAdapter,
    manager: usermgr.UserManager,

    fn init(alloc: std.mem.Allocator) !SimAuthManager {
        var self = SimAuthManager{
            .store = usermgr.MemoryStore.init(alloc),
            .policy_store = casbin.MemoryAdapter.init(alloc),
            .manager = undefined,
        };
        errdefer self.store.deinit();
        errdefer self.policy_store.deinit();

        self.manager = try usermgr.UserManager.init(
            alloc,
            self.store.iface(),
            try usermgr.initDefaultEnforcer(alloc, self.policy_store.iface()),
        );
        errdefer self.manager.deinit();

        try usermgr.ensureDefaultAdminUser(&self.manager);
        return self;
    }

    fn deinit(self: *SimAuthManager) void {
        self.manager.deinit();
        self.policy_store.deinit();
        self.store.deinit();
        self.* = undefined;
    }
};

fn encodeBasicAuthorization(alloc: std.mem.Allocator, username: []const u8, password: []const u8) ![]u8 {
    const raw = try std.fmt.allocPrint(alloc, "{s}:{s}", .{ username, password });
    defer alloc.free(raw);
    const size = std.base64.standard.Encoder.calcSize(raw.len);
    const encoded = try alloc.alloc(u8, size);
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, raw);
    return try std.fmt.allocPrint(alloc, "Basic {s}", .{encoded});
}

fn PublicApiServerOptions(comptime N: usize) type {
    return struct {
        auth_managers: ?*[N]SimAuthManager = null,
    };
}

fn startPublicApiServers(
    comptime N: usize,
    alloc: std.mem.Allocator,
    cluster: *MetadataHttpClusterSimulation,
    shared_io: *std.Io.Threaded,
    roots: *const [N][]const u8,
    metadata_snapshot_mode: PublicApiStatusSource.MetadataSnapshotMode,
    forward_executor: *std_http_executor.StdHttpExecutor,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]api_http_server.ApiHttpServer,
    status_sources: *[N]PublicApiStatusSource,
    catalog_sources: *[N]PublicApiCatalogSource,
    routers: *[N]PublicApiRouter(N),
    read_sources: *[N]api_table_reads.HostedProvisionedTableReadSource,
    write_sources: *[N]api_table_writes.HostedProvisionedTableWriteSource,
    options: PublicApiServerOptions(N),
    api_base_uris: *[N][]const u8,
) !void {
    var started: usize = 0;
    errdefer {
        for (0..started) |i| listeners[i].deinit();
        for (0..started) |i| servers[i].deinit();
    }

    for (0..N) |i| {
        status_sources[i] = .{ .node = cluster.node(i), .metadata_snapshot_mode = metadata_snapshot_mode };
        catalog_sources[i] = .{ .node = cluster.node(i), .metadata_snapshot_mode = metadata_snapshot_mode };
        routers[i] = .{ .node = cluster.node(i), .cluster = cluster, .api_base_uris = api_base_uris };
        read_sources[i] = api_table_reads.HostedProvisionedTableReadSource.init(
            roots[i],
            catalog_sources[i].iface(),
            cluster.cluster.node(i).runtime.svc.readableLeaseRequester(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        _ = read_sources[i].withIo(shared_io);
        write_sources[i] = api_table_writes.HostedProvisionedTableWriteSource.init(
            roots[i],
            catalog_sources[i].iface(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        attachHostedSourcesBackendRuntimeForSimulation(&read_sources[i], &write_sources[i], cluster.backendRuntime(i));
        const server_config: api_http_server.ApiHttpServerConfig = if (options.auth_managers) |auth_managers| .{
            .auth_enabled = true,
            .user_manager = &auth_managers[i].manager,
        } else .{};
        servers[i] = api_http_server.ApiHttpServer.init(
            alloc,
            server_config,
            status_sources[i].iface(),
            read_sources[i].source(),
            write_sources[i].source(),
        );
        listeners[i] = std_http_listener.StdHttpListener.initShared(alloc, lean_sim_http_listener_cfg, servers[i].executor(), shared_io);
        try listeners[i].start();
        started += 1;
    }

    var uri_count: usize = 0;
    errdefer for (0..uri_count) |i| alloc.free(api_base_uris[i]);
    for (0..N) |i| {
        api_base_uris[i] = try listeners[i].baseUri(alloc);
        uri_count += 1;
    }
}

fn attachHostedSourcesBackendRuntimeForSimulation(
    read_source: *api_table_reads.HostedProvisionedTableReadSource,
    write_source: *api_table_writes.HostedProvisionedTableWriteSource,
    backend_runtime: *db_mod.background_runtime.BackendRuntime,
) void {
    _ = read_source.withBackendRuntime(backend_runtime);
    _ = write_source.withBackendRuntime(backend_runtime);
    _ = write_source.withForegroundDerivedProgress();
}

fn closeHostedPublicApiCaches(comptime N: usize, write_sources: *[N]api_table_writes.HostedProvisionedTableWriteSource) void {
    for (write_sources) |*source| {
        api_table_writes.closeHostedManagedDbCacheForRoot(source.replica_root_dir);
    }
}

fn deinitPublicApiServers(comptime N: usize, servers: *[N]api_http_server.ApiHttpServer) void {
    for (servers) |*server| server.deinit();
}

fn deinitPublicApiStack(
    comptime N: usize,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]api_http_server.ApiHttpServer,
    write_sources: *[N]api_table_writes.HostedProvisionedTableWriteSource,
) void {
    for (listeners) |*listener| listener.deinit();
    closeHostedPublicApiCaches(N, write_sources);
    deinitPublicApiServers(N, servers);
}

fn PublicApiTestRig(comptime N: usize) type {
    return struct {
        alloc: std.mem.Allocator,
        http_io: std.Io.Threaded,
        listeners: [N]std_http_listener.StdHttpListener = undefined,
        servers: [N]api_http_server.ApiHttpServer = undefined,
        status_sources: [N]PublicApiStatusSource = undefined,
        catalog_sources: [N]PublicApiCatalogSource = undefined,
        routers: [N]PublicApiRouter(N) = undefined,
        read_sources: [N]api_table_reads.HostedProvisionedTableReadSource = undefined,
        write_sources: [N]api_table_writes.HostedProvisionedTableWriteSource = undefined,
        api_base_uris: [N][]const u8 = undefined,
        forward_executor: std_http_executor.StdHttpExecutor = undefined,
        client_executor: std_http_executor.StdHttpExecutor = undefined,
        client: api_http_client.ApiHttpClient = undefined,
        metadata_client: metadata_http_client.MetadataHttpClient = undefined,

        fn initInPlace(
            self: *@This(),
            alloc: std.mem.Allocator,
            cluster: *MetadataHttpClusterSimulation,
            roots: [N][]const u8,
        ) !void {
            try self.initWithMetadataMode(alloc, cluster, roots, .local);
        }

        fn initLeaderBackedInPlace(
            self: *@This(),
            alloc: std.mem.Allocator,
            cluster: *MetadataHttpClusterSimulation,
            roots: [N][]const u8,
        ) !void {
            try self.initWithMetadataMode(alloc, cluster, roots, .leader_backed);
        }

        fn initLeaderBackedWithAuthInPlace(
            self: *@This(),
            alloc: std.mem.Allocator,
            cluster: *MetadataHttpClusterSimulation,
            roots: [N][]const u8,
            auth_managers: *[N]SimAuthManager,
        ) !void {
            try self.initWithMetadataModeAndOptions(alloc, cluster, roots, .leader_backed, .{ .auth_managers = auth_managers });
        }

        fn initWithMetadataMode(
            self: *@This(),
            alloc: std.mem.Allocator,
            cluster: *MetadataHttpClusterSimulation,
            roots: [N][]const u8,
            metadata_snapshot_mode: PublicApiStatusSource.MetadataSnapshotMode,
        ) !void {
            try self.initWithMetadataModeAndOptions(alloc, cluster, roots, metadata_snapshot_mode, .{});
        }

        fn initWithMetadataModeAndOptions(
            self: *@This(),
            alloc: std.mem.Allocator,
            cluster: *MetadataHttpClusterSimulation,
            roots: [N][]const u8,
            metadata_snapshot_mode: PublicApiStatusSource.MetadataSnapshotMode,
            options: PublicApiServerOptions(N),
        ) !void {
            self.* = .{
                .alloc = alloc,
                .http_io = std.Io.Threaded.init(alloc, .{ .stack_size = lean_sim_thread_stack_size }),
            };
            self.forward_executor.initSharedInPlace(std.heap.page_allocator, .{}, &self.http_io);
            errdefer self.forward_executor.deinit();

            try startPublicApiServers(
                N,
                alloc,
                cluster,
                &self.http_io,
                &roots,
                metadata_snapshot_mode,
                &self.forward_executor,
                &self.listeners,
                &self.servers,
                &self.status_sources,
                &self.catalog_sources,
                &self.routers,
                &self.read_sources,
                &self.write_sources,
                options,
                &self.api_base_uris,
            );
            errdefer {
                deinitPublicApiStack(N, &self.listeners, &self.servers, &self.write_sources);
                for (self.api_base_uris) |uri| alloc.free(uri);
            }

            self.client_executor.initSharedInPlace(std.heap.page_allocator, .{}, &self.http_io);
            errdefer self.client_executor.deinit();

            self.client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, self.client_executor.executor());
            self.metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, self.client_executor.executor());

            for (0..N) |i| try cluster.node(i).runRound();
        }

        fn init(
            alloc: std.mem.Allocator,
            cluster: *MetadataHttpClusterSimulation,
            roots: [N][]const u8,
        ) !@This() {
            var self: @This() = undefined;
            try self.initInPlace(alloc, cluster, roots);
            return self;
        }

        fn deinit(self: *@This()) void {
            self.client_executor.deinit();
            deinitPublicApiStack(N, &self.listeners, &self.servers, &self.write_sources);
            for (self.api_base_uris) |uri| self.alloc.free(uri);
            self.forward_executor.deinit();
            self.http_io.deinit();
            self.* = undefined;
        }
    };
}

fn startBootstrappedMetadataCluster(
    cluster: *MetadataHttpClusterSimulation,
    leader_wait_rounds: usize,
    publish_stores: bool,
) !usize {
    try cluster.startAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(leader_wait_rounds)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);
    if (publish_stores) try cluster.publishClusterStores(leader_index);
    return leader_index;
}

const MetadataVoprCampaignConfig = struct {
    seed: u64,
    operation_count: usize = 64,
    metadata_group_id: u64,
    table_id: u64,
    range_group_id: u64,
    split_group_id: u64,
    split_transition_id: u64,
    workload: MetadataVoprWorkload = .smoke,
};

const MetadataVoprWorkload = enum {
    smoke,
    expanded,
};

const MetadataVoprAction = enum {
    step,
    drop_next,
    duplicate_next,
    delay_next,
    release_fifo,
    release_random,
    start_link_partition,
    start_node_partition,
    heal_all,
    restart_follower,
};

fn metadataVoprActionName(action: MetadataVoprAction) []const u8 {
    return switch (action) {
        .step => "step",
        .drop_next => "drop_next",
        .duplicate_next => "duplicate_next",
        .delay_next => "delay_next",
        .release_fifo => "release_fifo",
        .release_random => "release_random",
        .start_link_partition => "start_link_partition",
        .start_node_partition => "start_node_partition",
        .heal_all => "heal_all",
        .restart_follower => "restart_follower",
    };
}

fn metadataVoprReplayCommand(cfg: MetadataVoprCampaignConfig) []const u8 {
    return switch (cfg.workload) {
        .smoke => "zig build lib-metadata-vopr-test --summary failures",
        .expanded => "zig build lib-metadata-vopr-chaos-test --summary failures",
    };
}

const MetadataVoprCampaignState = struct {
    active_link: ?raft_sim.VirtualHttpNetwork.Link = null,
    active_node_id: ?u64 = null,

    fn clear(self: *MetadataVoprCampaignState) void {
        self.active_link = null;
        self.active_node_id = null;
    }
};

fn metadataVoprNodeId(cluster: *MetadataHttpClusterSimulation, index: usize) u64 {
    return cluster.cluster.configs[index].host.http.host.local_node_id;
}

fn metadataVoprPickIndex(random: std.Random, node_count: usize, exclude: ?usize) usize {
    std.debug.assert(node_count > 0);
    var index = @as(usize, @intCast(random.intRangeLessThan(u32, 0, @as(u32, @intCast(node_count)))));
    if (exclude) |excluded| {
        if (node_count > 1 and index == excluded) index = (index + 1) % node_count;
    }
    return index;
}

fn metadataVoprReportFailure(
    cfg: MetadataVoprCampaignConfig,
    operation_index: ?usize,
    action: ?MetadataVoprAction,
    err: anyerror,
) void {
    if (operation_index) |index| {
        std.debug.print(
            "metadata VOPR failure seed=0x{x} operation={d} action={s} error={s}\nreplay: {s}\n",
            .{ cfg.seed, index, metadataVoprActionName(action.?), @errorName(err), metadataVoprReplayCommand(cfg) },
        );
    } else {
        std.debug.print(
            "metadata VOPR failure seed=0x{x} phase=liveness error={s}\nreplay: {s}\n",
            .{ cfg.seed, @errorName(err), metadataVoprReplayCommand(cfg) },
        );
    }
}

fn metadataVoprRunRandomTransportActions(
    cluster: *MetadataHttpClusterSimulation,
    random: std.Random,
    cfg: MetadataVoprCampaignConfig,
    state: *MetadataVoprCampaignState,
    operation_index: *usize,
    count: usize,
) !void {
    const action_count = std.meta.tags(MetadataVoprAction).len;
    for (0..count) |_| {
        const action = @as(MetadataVoprAction, @enumFromInt(random.intRangeLessThan(u32, 0, @intCast(action_count))));
        metadataVoprRunAction(cluster, random, cfg, state, operation_index.*, action) catch |err| {
            metadataVoprReportFailure(cfg, operation_index.*, action, err);
            return err;
        };
        operation_index.* += 1;
    }
}

fn metadataVoprStartFollowerPartition(
    cluster: *MetadataHttpClusterSimulation,
    state: *MetadataVoprCampaignState,
) !void {
    if (state.active_link != null or state.active_node_id != null) return;
    const leader_index = try metadataVoprLeaderIndex(cluster);
    const follower_index = (leader_index + 1) % cluster.cluster.nodes.len;
    const node_id = metadataVoprNodeId(cluster, follower_index);
    try cluster.cluster.inject(.{ .partition_node = node_id });
    state.active_node_id = node_id;
    try cluster.stepAll();
}

fn metadataVoprStartFollowerLinkPartition(
    cluster: *MetadataHttpClusterSimulation,
    state: *MetadataVoprCampaignState,
) !void {
    if (state.active_link != null or state.active_node_id != null) return;
    const leader_index = try metadataVoprLeaderIndex(cluster);
    const follower_index = (leader_index + 1) % cluster.cluster.nodes.len;
    const link = raft_sim.VirtualHttpNetwork.Link{
        .source_id = metadataVoprNodeId(cluster, follower_index),
        .target_id = metadataVoprNodeId(cluster, leader_index),
    };
    try cluster.cluster.inject(.{ .partition_link = link });
    state.active_link = link;
    try cluster.stepAll();
}

fn metadataVoprHealAll(
    cluster: *MetadataHttpClusterSimulation,
    state: *MetadataVoprCampaignState,
) !void {
    cluster.cluster.healAll();
    state.clear();
    for (0..4) |_| try cluster.stepAll();
    _ = try cluster.waitForMetadataLeader(96) orelse return error.NotLeader;
}

fn metadataVoprRunAction(
    cluster: *MetadataHttpClusterSimulation,
    random: std.Random,
    cfg: MetadataVoprCampaignConfig,
    state: *MetadataVoprCampaignState,
    operation_index: usize,
    action: MetadataVoprAction,
) !void {
    const node_count = cluster.cluster.nodes.len;
    switch (action) {
        .step => try cluster.stepAll(),
        .drop_next => {
            try cluster.cluster.inject(.drop_next);
            try cluster.stepAll();
        },
        .duplicate_next => {
            try cluster.cluster.inject(.duplicate_next);
            try cluster.stepAll();
        },
        .delay_next => {
            const ticks = @as(u64, random.intRangeLessThan(u32, 1, 4));
            try cluster.cluster.inject(.{ .delay_next_ticks = ticks });
            try cluster.stepAll();
        },
        .release_fifo => {
            try cluster.cluster.inject(.release_fifo);
            try cluster.stepAll();
        },
        .release_random => {
            try cluster.cluster.inject(.{ .release_random = cfg.seed ^ @as(u64, @intCast(operation_index)) });
            try cluster.stepAll();
        },
        .start_link_partition => {
            if (state.active_link != null or state.active_node_id != null) {
                try cluster.stepAll();
                return;
            }
            const source_index = metadataVoprPickIndex(random, node_count, null);
            const target_index = metadataVoprPickIndex(random, node_count, source_index);
            const link = raft_sim.VirtualHttpNetwork.Link{
                .source_id = metadataVoprNodeId(cluster, source_index),
                .target_id = metadataVoprNodeId(cluster, target_index),
            };
            try cluster.cluster.inject(.{ .partition_link = link });
            state.active_link = link;
            try cluster.stepAll();
        },
        .start_node_partition => {
            if (state.active_link != null or state.active_node_id != null) {
                try cluster.stepAll();
                return;
            }
            const index = metadataVoprPickIndex(random, node_count, null);
            const node_id = metadataVoprNodeId(cluster, index);
            try cluster.cluster.inject(.{ .partition_node = node_id });
            state.active_node_id = node_id;
            try cluster.stepAll();
        },
        .heal_all => {
            cluster.cluster.healAll();
            state.clear();
            try cluster.stepAll();
        },
        .restart_follower => {
            const leader_index = currentMetadataLeaderIndex(cluster);
            const index = metadataVoprPickIndex(random, node_count, leader_index);
            try cluster.restartNode(index);
            try cluster.stepAll();
        },
    }
}

fn metadataVoprLeaderIndex(cluster: *MetadataHttpClusterSimulation) !usize {
    return (try cluster.waitForMetadataLeader(96)) orelse error.TestExpectedEqual;
}

const VoprStoreLiveProgressContext = struct {
    store_id: u64,
    expected_live: bool,
};

fn voprStoreLiveProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *VoprStoreLiveProgressContext = @ptrCast(@alignCast(ptr));
    const leader_index = currentMetadataLeaderIndex(cluster) orelse return false;
    const stores = try cluster.node(leader_index).listProjectedStores(cluster.alloc);
    defer cluster.node(leader_index).freeProjectedStores(cluster.alloc, stores);
    const store = findProjectedStore(stores, ctx.store_id) orelse return false;
    return store.live == ctx.expected_live;
}

const VoprStoreDrainProgressContext = struct {
    store_id: u64,
    expected_drain_requested: bool,
};

fn voprStoreDrainProgressPredicate(cluster: *MetadataHttpClusterSimulation, ptr: *anyopaque) anyerror!bool {
    const ctx: *VoprStoreDrainProgressContext = @ptrCast(@alignCast(ptr));
    const leader_index = currentMetadataLeaderIndex(cluster) orelse return false;
    const stores = try cluster.node(leader_index).listProjectedStores(cluster.alloc);
    defer cluster.node(leader_index).freeProjectedStores(cluster.alloc, stores);
    const store = findProjectedStore(stores, ctx.store_id) orelse return false;
    return store.drain_requested == ctx.expected_drain_requested;
}

fn metadataVoprCreateActiveTable(
    cluster: *MetadataHttpClusterSimulation,
    workflow: *metadata_table_workflow.TableWorkflow,
    table_id: u64,
    table_name: []const u8,
    group_id: u64,
    desired_replica_count: u16,
    wait_rounds: usize,
) !void {
    const ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = group_id,
        .table_id = table_id,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    _ = try metadataVoprCreateTableWithRanges(cluster, workflow, .{
        .table_id = table_id,
        .name = table_name,
        .desired_replica_count = desired_replica_count,
        .min_ranges = 1,
    }, ranges[0..], 16);
    const expected_active = @as(usize, desired_replica_count);
    if (expected_active >= cluster.cluster.nodes.len) {
        try std.testing.expect(try cluster.waitForGroupStatus(group_id, .active, wait_rounds));
    } else {
        try std.testing.expect(try cluster.waitForGroupStatusCount(group_id, .active, expected_active, wait_rounds));
    }
}

fn metadataVoprCreateTableWithRanges(
    cluster: *MetadataHttpClusterSimulation,
    workflow: *metadata_table_workflow.TableWorkflow,
    table: metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    max_rounds: usize,
) !metadata_control_loop.ReconcileSummary {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        const leader_index = try metadataVoprLeaderIndex(cluster);
        return workflow.createTableWithRanges(&cluster.node(leader_index), table, ranges) catch |err| switch (err) {
            error.NotLeader => {
                try cluster.stepAll();
                continue;
            },
            else => return err,
        };
    }
    return error.NotLeader;
}

fn metadataVoprAddRange(
    cluster: *MetadataHttpClusterSimulation,
    workflow: *metadata_table_workflow.TableWorkflow,
    range: metadata_table_manager.RangeRecord,
    max_rounds: usize,
) !metadata_control_loop.ReconcileSummary {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        const leader_index = try metadataVoprLeaderIndex(cluster);
        return workflow.addRange(&cluster.node(leader_index), range) catch |err| switch (err) {
            error.NotLeader => {
                try cluster.stepAll();
                continue;
            },
            else => return err,
        };
    }
    return error.NotLeader;
}

fn metadataVoprRunLivenessWorkload(
    cluster: *MetadataHttpClusterSimulation,
    cfg: MetadataVoprCampaignConfig,
    random: std.Random,
    state: *MetadataVoprCampaignState,
    operation_index: *usize,
) !void {
    return switch (cfg.workload) {
        .smoke => metadataVoprRunSmokeLivenessWorkload(cluster, cfg),
        .expanded => metadataVoprRunExpandedLivenessWorkload(cluster, cfg, random, state, operation_index),
    };
}

fn metadataVoprRunSmokeLivenessWorkload(
    cluster: *MetadataHttpClusterSimulation,
    cfg: MetadataVoprCampaignConfig,
) !void {
    var workflow = metadata_table_workflow.TableWorkflow.init(cluster.alloc);
    defer workflow.deinit();

    try metadataVoprCreateActiveTable(cluster, &workflow, cfg.table_id, "vopr-docs", cfg.range_group_id, 3, 64);
    const split_leader_index = try metadataVoprLeaderIndex(cluster);
    try reportSplitCandidateStatus(cluster.node(split_leader_index), cfg.range_group_id, 256, 180, "doc:m");
    const split_summary = try workflow.requestSplit(&cluster.node(split_leader_index), .{
        .transition_id = cfg.split_transition_id,
        .table_id = cfg.table_id,
        .source_group_id = cfg.range_group_id,
        .destination_group_id = cfg.split_group_id,
        .split_key = "doc:m",
    });
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);
    var split_ctx = MetadataSplitTransitionProgressContext{ .transition_id = cfg.split_transition_id };
    try cluster.assertProgress("metadata-vopr-smoke-split-progress", 48, &split_ctx, metadataSplitTransitionProgressPredicate);
}

fn metadataVoprRunExpandedLivenessWorkload(
    cluster: *MetadataHttpClusterSimulation,
    cfg: MetadataVoprCampaignConfig,
    random: std.Random,
    state: *MetadataVoprCampaignState,
    operation_index: *usize,
) !void {
    var workflow = metadata_table_workflow.TableWorkflow.init(cluster.alloc);
    defer workflow.deinit();
    const phase_fault_actions = @max(@as(usize, 1), cfg.operation_count / 12);

    try metadataVoprCreateActiveTable(cluster, &workflow, cfg.table_id, "vopr-docs", cfg.range_group_id, 3, 96);
    try metadataVoprRunRandomTransportActions(cluster, random, cfg, state, operation_index, phase_fault_actions);
    try metadataVoprHealAll(cluster, state);

    var lifecycle = metadata_table_workflow.TableWorkflow.init(cluster.alloc);
    defer lifecycle.deinit();
    const lifecycle_table_id = cfg.table_id + 1000;
    const lifecycle_group_a = cfg.range_group_id + 1000;
    const lifecycle_group_b = cfg.range_group_id + 1001;
    try metadataVoprStartFollowerPartition(cluster, state);
    try metadataVoprCreateActiveTable(cluster, &lifecycle, lifecycle_table_id, "vopr-life", lifecycle_group_a, 2, 64);
    const drop_leader_index = try metadataVoprLeaderIndex(cluster);
    const drop_summary = try lifecycle.dropTable(&cluster.node(drop_leader_index), lifecycle_table_id);
    try std.testing.expectEqual(@as(usize, 1), drop_summary.table_removals);
    try metadataVoprHealAll(cluster, state);
    try std.testing.expect(try cluster.waitForGroupStatus(lifecycle_group_a, .absent, 64));
    try metadataVoprCreateActiveTable(cluster, &lifecycle, lifecycle_table_id, "vopr-life", lifecycle_group_b, 2, 64);
    try std.testing.expect(try cluster.waitForGroupStatusCount(lifecycle_group_b, .active, 2, 64));
    try metadataVoprRunRandomTransportActions(cluster, random, cfg, state, operation_index, phase_fault_actions);
    try metadataVoprHealAll(cluster, state);

    var churn_workflow = metadata_table_workflow.TableWorkflow.init(cluster.alloc);
    defer churn_workflow.deinit();
    const churn_table_id = cfg.table_id + 3000;
    const churn_group_id = cfg.range_group_id + 3000;
    try churn_workflow.setPlacementCandidates(&.{ 1, 2 });
    try metadataVoprCreateActiveTable(cluster, &churn_workflow, churn_table_id, "vopr-churn", churn_group_id, 2, 64);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, churn_group_id, .active, 64));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, churn_group_id, .active, 64));
    try churn_workflow.setPlacementCandidates(&.{ 2, 3 });
    _ = try requireLeasedReconcile(cluster.node(try metadataVoprLeaderIndex(cluster)), churn_workflow.controlLoop());
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, churn_group_id, .absent, 64));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, churn_group_id, .active, 64));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, churn_group_id, .active, 64));
    try metadataVoprRunRandomTransportActions(cluster, random, cfg, state, operation_index, phase_fault_actions);
    try metadataVoprHealAll(cluster, state);

    var merge_workflow = metadata_table_workflow.TableWorkflow.init(cluster.alloc);
    defer merge_workflow.deinit();
    const merge_table_id = cfg.table_id + 2000;
    const merge_left_group = cfg.range_group_id + 2000;
    const merge_right_group = cfg.range_group_id + 2001;
    const merge_transition_id = cfg.split_transition_id + 2000;
    const merge_left_range = [_]metadata_table_manager.RangeRecord{.{
        .group_id = merge_left_group,
        .table_id = merge_table_id,
        .start_key = "doc:a",
        .end_key = "doc:m",
    }};
    try metadataVoprStartFollowerLinkPartition(cluster, state);
    _ = try metadataVoprCreateTableWithRanges(cluster, &merge_workflow, .{
        .table_id = merge_table_id,
        .name = "vopr-merge",
        .desired_replica_count = 1,
        .min_ranges = 2,
    }, merge_left_range[0..], 16);
    _ = try metadataVoprAddRange(cluster, &merge_workflow, .{
        .group_id = merge_right_group,
        .table_id = merge_table_id,
        .start_key = "doc:m",
        .end_key = "doc:z",
    }, 16);
    try std.testing.expect(try cluster.waitForGroupStatusCount(merge_left_group, .active, 1, 64));
    try std.testing.expect(try cluster.waitForGroupStatusCount(merge_right_group, .active, 1, 64));
    const merge_leader_index = try metadataVoprLeaderIndex(cluster);
    try reportMergeCandidateStatuses(cluster.node(merge_leader_index), merge_left_group, 16, 10, merge_right_group, 12, 12);
    const merge_summary = try merge_workflow.requestMerge(&cluster.node(merge_leader_index), .{
        .transition_id = merge_transition_id,
        .table_id = merge_table_id,
        .donor_group_id = merge_right_group,
        .receiver_group_id = merge_left_group,
    });
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);
    try metadataVoprHealAll(cluster, state);
    var merge_ctx = MetadataMergeTransitionProgressContext{ .transition_id = merge_transition_id };
    try cluster.assertProgress("metadata-vopr-merge-progress", 48, &merge_ctx, metadataMergeTransitionProgressPredicate);
    try metadataVoprRunRandomTransportActions(cluster, random, cfg, state, operation_index, phase_fault_actions);
    try metadataVoprHealAll(cluster, state);

    const topo_leader_index = try metadataVoprLeaderIndex(cluster);
    try metadataVoprStartFollowerLinkPartition(cluster, state);
    try cluster.node(topo_leader_index).upsertNode(.{ .node_id = 3, .role = "maintenance" });
    try cluster.node(topo_leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = false });
    try metadataVoprHealAll(cluster, state);
    var store_down_ctx = VoprStoreLiveProgressContext{ .store_id = 3, .expected_live = false };
    try cluster.assertProgress("metadata-vopr-store-down", 32, &store_down_ctx, voprStoreLiveProgressPredicate);
    try cluster.node(try metadataVoprLeaderIndex(cluster)).upsertNode(.{ .node_id = 3, .role = "data" });
    try cluster.node(try metadataVoprLeaderIndex(cluster)).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true });
    var store_up_ctx = VoprStoreLiveProgressContext{ .store_id = 3, .expected_live = true };
    try cluster.assertProgress("metadata-vopr-store-up", 32, &store_up_ctx, voprStoreLiveProgressPredicate);
    try metadataVoprRunRandomTransportActions(cluster, random, cfg, state, operation_index, phase_fault_actions);
    try metadataVoprHealAll(cluster, state);

    const split_leader_index = try metadataVoprLeaderIndex(cluster);
    try reportSplitCandidateStatus(cluster.node(split_leader_index), cfg.range_group_id, 256, 180, "doc:m");
    try metadataVoprStartFollowerPartition(cluster, state);
    const split_summary = try workflow.requestSplit(&cluster.node(split_leader_index), .{
        .transition_id = cfg.split_transition_id,
        .table_id = cfg.table_id,
        .source_group_id = cfg.range_group_id,
        .destination_group_id = cfg.split_group_id,
        .split_key = "doc:m",
    });
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);
    try cluster.restartNode(split_leader_index);
    try metadataVoprHealAll(cluster, state);
    _ = try metadataVoprLeaderIndex(cluster);
    var split_ctx = MetadataSplitTransitionProgressContext{ .transition_id = cfg.split_transition_id };
    try cluster.assertProgress("metadata-vopr-restart-split-progress", 64, &split_ctx, metadataSplitTransitionProgressPredicate);

    try metadataVoprStartFollowerLinkPartition(cluster, state);
    try metadataVoprHealAll(cluster, state);
    const shutdown_leader_index = try metadataVoprLeaderIndex(cluster);
    const shutdown_node_id = metadataVoprNodeId(cluster, (shutdown_leader_index + 1) % cluster.cluster.nodes.len);
    var drain_ctx = VoprStoreDrainProgressContext{ .store_id = shutdown_node_id, .expected_drain_requested = true };
    try cluster.node(shutdown_leader_index).upsertNode(.{ .node_id = shutdown_node_id, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_draining });
    try cluster.node(shutdown_leader_index).upsertStore(.{ .store_id = shutdown_node_id, .node_id = shutdown_node_id, .role = "data", .live = true, .drain_requested = true });
    try cluster.assertProgress("metadata-vopr-store-drain-requested", 48, &drain_ctx, voprStoreDrainProgressPredicate);
    try metadataVoprRunRandomTransportActions(cluster, random, cfg, state, operation_index, phase_fault_actions);
    try metadataVoprHealAll(cluster, state);
    try cluster.assertProgress("metadata-vopr-store-drain-requested", 48, &drain_ctx, voprStoreDrainProgressPredicate);
}

fn runMetadataVoprCampaign(alloc: std.mem.Allocator, cfg: MetadataVoprCampaignConfig) !void {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(alloc);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(alloc);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(alloc);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = alloc, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = alloc, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = alloc, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-vopr-{x}-a", .{ tmp.sub_path, cfg.seed });
    defer alloc.free(root_a);
    const root_b = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-vopr-{x}-b", .{ tmp.sub_path, cfg.seed });
    defer alloc.free(root_b);
    const root_c = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-vopr-{x}-c", .{ tmp.sub_path, cfg.seed });
    defer alloc.free(root_c);
    const cat_a = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-vopr-{x}-a.txt", .{ tmp.sub_path, cfg.seed });
    defer alloc.free(cat_a);
    const cat_b = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-vopr-{x}-b.txt", .{ tmp.sub_path, cfg.seed });
    defer alloc.free(cat_b);
    const cat_c = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/metadata-vopr-{x}-c.txt", .{ tmp.sub_path, cfg.seed });
    defer alloc.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, cfg.metadata_group_id, root_a, cat_a),
        makeHostSimConfig(2, cfg.metadata_group_id, root_b, cat_b),
        makeHostSimConfig(3, cfg.metadata_group_id, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(alloc, cfg.metadata_group_id, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();

    _ = try startBootstrappedMetadataCluster(&cluster, 48, true);

    var prng = std.Random.DefaultPrng.init(cfg.seed);
    const random = prng.random();
    var campaign_state = MetadataVoprCampaignState{};
    var operation_index: usize = 0;
    if (cfg.workload == .smoke) {
        try metadataVoprRunRandomTransportActions(&cluster, random, cfg, &campaign_state, &operation_index, cfg.operation_count);
        try metadataVoprHealAll(&cluster, &campaign_state);
    }
    metadataVoprRunLivenessWorkload(&cluster, cfg, random, &campaign_state, &operation_index) catch |err| {
        metadataVoprReportFailure(cfg, null, null, err);
        return err;
    };
    try metadataVoprHealAll(&cluster, &campaign_state);
}

test "metadata VOPR seeded smoke campaign" {
    try runMetadataVoprCampaign(std.testing.allocator, .{
        .seed = 0xA17F_0001,
        .operation_count = 24,
        .metadata_group_id = 6100,
        .table_id = 6101,
        .range_group_id = 6102,
        .split_group_id = 6103,
        .split_transition_id = 6104,
    });
}

test "metadata VOPR expanded generated workload campaign" {
    try runMetadataVoprCampaign(std.testing.allocator, .{
        .seed = 0xA17F_0002,
        .operation_count = 48,
        .metadata_group_id = 6200,
        .table_id = 6201,
        .range_group_id = 6202,
        .split_group_id = 6203,
        .split_transition_id = 6204,
        .workload = .expanded,
    });
}

fn startMetadataAdminServers(
    comptime N: usize,
    alloc: std.mem.Allocator,
    cluster: *MetadataHttpClusterSimulation,
    shared_io: *std.Io.Threaded,
    listeners: *[N]std_http_listener.StdHttpListener,
    servers: *[N]metadata_http_server.MetadataHttpServer,
    sources: *[N]MetadataAdminSimSource,
    base_uris: *[N][]const u8,
) !void {
    var started: usize = 0;
    errdefer {
        for (0..started) |i| listeners[i].deinit();
        for (0..started) |i| servers[i].deinit();
    }

    for (0..N) |i| {
        sources[i] = .{ .node = cluster.node(i) };
        servers[i] = metadata_http_server.MetadataHttpServer.init(alloc, .{}, sources[i].iface());
        listeners[i] = std_http_listener.StdHttpListener.initShared(alloc, lean_sim_http_listener_cfg, servers[i].executor(), shared_io);
        try listeners[i].start();
        started += 1;
    }

    var uri_count: usize = 0;
    errdefer for (0..uri_count) |i| alloc.free(base_uris[i]);
    for (0..N) |i| {
        base_uris[i] = try listeners[i].baseUri(alloc);
        uri_count += 1;
    }
}

fn deinitMetadataAdminServers(comptime N: usize, servers: *[N]metadata_http_server.MetadataHttpServer) void {
    for (servers) |*server| server.deinit();
}

fn requestNodeShutdownViaSimAdmin(
    cluster: *MetadataHttpClusterSimulation,
    source_index: usize,
    node_id: u64,
) !void {
    var source = MetadataAdminSimSource{ .node = cluster.node(source_index) };
    var server = metadata_http_server.MetadataHttpServer.init(cluster.alloc, .{}, source.iface());
    defer server.deinit();

    const path = try std.fmt.allocPrint(cluster.alloc, "/internal/v1/nodes/{d}/shutdown", .{node_id});
    defer cluster.alloc.free(path);

    var response = try server.handle(.{
        .method = .PUT,
        .uri = path,
        .body = "{\"type\":\"remove\",\"reason\":\"sim\"}",
    });
    defer response.deinit(cluster.alloc);
    try std.testing.expectEqual(@as(u16, 202), response.status);
}

test "metadata http cluster simulation drives table placement convergence" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4000, root_a, cat_a),
        makeHostSimConfig(2, 4000, root_b, cat_b),
        makeHostSimConfig(3, 4000, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4000, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const summary = try workflow.createTable(&cluster.node(leader_index), .{
        .table_id = 41,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 4101,
        .table_id = 41,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try std.testing.expectEqual(@as(usize, 3), summary.placement_upserts);

    try std.testing.expect(try cluster.waitForGroupStatus(4101, .active, 32));

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
    try std.testing.expectEqual(@as(usize, 3), intents.len);

    const nodes = try cluster.node(leader_index).listProjectedNodes(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedNodes(std.testing.allocator, nodes);
    try std.testing.expectEqual(@as(usize, 3), nodes.len);
}

test "metadata http cluster simulation serves public lifecycle from a non-host node after public create" {
    var sim_alloc_state: LeanSimAllocator = .init;
    defer _ = sim_alloc_state.deinit();
    const sim_alloc = sim_alloc_state.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_d.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-lifecycle-a", .{tmp.sub_path});
    defer sim_alloc.free(root_a);
    const root_b = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-lifecycle-b", .{tmp.sub_path});
    defer sim_alloc.free(root_b);
    const root_c = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-lifecycle-c", .{tmp.sub_path});
    defer sim_alloc.free(root_c);
    const root_d = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-lifecycle-d", .{tmp.sub_path});
    defer sim_alloc.free(root_d);
    const cat_a = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-lifecycle-a.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_a);
    const cat_b = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-lifecycle-b.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_b);
    const cat_c = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-lifecycle-c.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_c);
    const cat_d = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-lifecycle-d.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4860, root_a, cat_a),
        makeHostSimConfig(2, 4860, root_b, cat_b),
        makeHostSimConfig(3, 4860, root_c, cat_c),
        makeHostSimConfig(4, 4860, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try MetadataHttpClusterSimulation.init(sim_alloc, 4860, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var public_api: PublicApiTestRig(4) = undefined;
    try public_api.initLeaderBackedInPlace(sim_alloc, &cluster, roots);
    defer public_api.deinit();
    api_base_uris = public_api.api_base_uris;
    var client = public_api.client;

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "clustered public docs");
    defer std.heap.page_allocator.free(create_body);
    var created = try client.createTable(api_base_uris[0], "docs", create_body);
    defer created.deinit(std.heap.page_allocator);
    var created_table = try std.json.parseFromSlice(struct { name: []const u8 }, std.heap.page_allocator, created.body, .{ .ignore_unknown_fields = true });
    defer created_table.deinit();
    try std.testing.expectEqualStrings("docs", created_table.value.name);

    const created_range = try waitForFirstProjectedRange(&cluster, leader_index, null, 3, true, 48);
    const group_id = created_range.group_id;
    try std.testing.expectEqual(@as(usize, 3), created_range.active_count);
    const client_index = created_range.non_host_index orelse return error.TestExpectedEqual;
    const client_base = api_base_uris[client_index];

    const group_leader_index = (try waitForGroupLeaderIndex(&cluster, group_id, 96)) orelse return error.TestExpectedEqual;
    try ensureGroupTextIndex(&cluster, roots[group_leader_index], group_id, api_tables.default_full_text_index_name, 40);

    const TableListEntry = struct { name: []const u8 };

    var listed_tables = try client.fetchTables(client_base, null);
    defer listed_tables.deinit(std.heap.page_allocator);
    var parsed_tables = try std.json.parseFromSlice([]TableListEntry, std.heap.page_allocator, listed_tables.body, .{ .ignore_unknown_fields = true });
    defer parsed_tables.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_tables.value.len);
    try std.testing.expectEqualStrings("docs", parsed_tables.value[0].name);

    var prefixed_tables = try client.fetchTables(client_base, "do");
    defer prefixed_tables.deinit(std.heap.page_allocator);
    var parsed_prefixed_tables = try std.json.parseFromSlice([]TableListEntry, std.heap.page_allocator, prefixed_tables.body, .{ .ignore_unknown_fields = true });
    defer parsed_prefixed_tables.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed_prefixed_tables.value.len);

    var updated_schema = try client.updateTableSchema(client_base, "docs",
        \\{"default_type":"doc","enforce_types":true,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"body":{"type":"text"},"status":{"type":"keyword"}}}}}}
    );
    defer updated_schema.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, updated_schema.body, "\"document_schemas\"") != null);
    try std.testing.expect(try waitForProjectedTableFieldContains(&cluster, "docs", .schema_json, "\"body\":{\"type\":\"text\"}", true, 40));
    try std.testing.expect(try waitForNodeProjectedTableFieldContains(&cluster, client_index, "docs", .indexes_json, "\"full_text_index_v0\"", true, 96));

    const create_index_body = try test_contract_helpers.encodeCreateIndexRequest(std.heap.page_allocator, "embed_idx");
    defer std.heap.page_allocator.free(create_index_body);
    var created_index = try client.createTableIndex(client_base, "docs", "embed_idx", create_index_body);
    defer created_index.deinit(std.heap.page_allocator);
    try std.testing.expect(try waitForProjectedTableFieldContains(&cluster, "docs", .indexes_json, "\"embed_idx\"", true, 40));

    var listed_indexes = try client.fetchTableIndexes(client_base, "docs");
    defer listed_indexes.deinit(std.heap.page_allocator);
    var parsed_indexes = try std.json.parseFromSlice([]std.json.Value, std.heap.page_allocator, listed_indexes.body, .{});
    defer parsed_indexes.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed_indexes.value.len);

    var batch = try client.fetchBatch(client_base, "docs",
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello clustered world","status":"published"},"doc:b":{"title":"beta","body":"secondary clustered document","status":"draft"}}}
    );
    defer batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, batch.body, "\"inserted\":2") != null);

    var query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"clustered"}},"fields":["title","body"],"count":true,"profile":true,"limit":10}
    );
    defer query.deinit(std.heap.page_allocator);
    var query_responses = try std.json.parseFromSlice(metadata_openapi.QueryResponses, std.heap.page_allocator, query.body, .{});
    defer query_responses.deinit();
    const query_result = query_responses.value.responses.?[0];
    try std.testing.expectEqual(@as(i64, 2), query_result.hits.?.total.?);
    try std.testing.expectEqual(@as(usize, 0), query_result.hits.?.hits.?.len);
    try std.testing.expect(query_result.profile != null);
    try std.testing.expectEqual(@as(i64, 1), query_result.profile.?.object.get("shards").?.object.get("total").?.integer);
    try std.testing.expectEqual(true, query_result.profile.?.object.get("merge") != null);

    var deleted = try client.fetchBatch(client_base, "docs",
        \\{"deletes":["doc:a"]}
    );
    defer deleted.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, deleted.body, "\"deleted\":1") != null);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:a", null));

    var dropped_index = try client.deleteTableIndex(client_base, "docs", "embed_idx");
    defer dropped_index.deinit(std.heap.page_allocator);

    var listed_indexes_after_drop = try client.fetchTableIndexes(client_base, "docs");
    defer listed_indexes_after_drop.deinit(std.heap.page_allocator);
    var parsed_indexes_after_drop = try std.json.parseFromSlice([]std.json.Value, std.heap.page_allocator, listed_indexes_after_drop.body, .{});
    defer parsed_indexes_after_drop.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_indexes_after_drop.value.len);

    var dropped_table = try client.dropTable(client_base, "docs");
    defer dropped_table.deinit(std.heap.page_allocator);
    var listed_tables_after_drop = try client.fetchTables(client_base, null);
    defer listed_tables_after_drop.deinit(std.heap.page_allocator);
    var parsed_tables_after_drop = try std.json.parseFromSlice([]TableListEntry, std.heap.page_allocator, listed_tables_after_drop.body, .{ .ignore_unknown_fields = true });
    defer parsed_tables_after_drop.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed_tables_after_drop.value.len);
}

test "metadata http cluster simulation seeds default admin for auth-enabled public api" {
    var sim_alloc_state: LeanSimAllocator = .init;
    defer _ = sim_alloc_state.deinit();
    const sim_alloc = sim_alloc_state.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-auth-seed-a", .{tmp.sub_path});
    defer sim_alloc.free(root_a);
    const root_b = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-auth-seed-b", .{tmp.sub_path});
    defer sim_alloc.free(root_b);
    const root_c = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-auth-seed-c", .{tmp.sub_path});
    defer sim_alloc.free(root_c);
    const cat_a = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-auth-seed-a.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_a);
    const cat_b = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-auth-seed-b.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_b);
    const cat_c = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-auth-seed-c.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4868, root_a, cat_a),
        makeHostSimConfig(2, 4868, root_b, cat_b),
        makeHostSimConfig(3, 4868, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(sim_alloc, 4868, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();
    _ = try startBootstrappedMetadataCluster(&cluster, 24, true);

    var auth_managers: [3]SimAuthManager = undefined;
    var auth_count: usize = 0;
    errdefer for (auth_managers[0..auth_count]) |*auth| auth.deinit();
    for (&auth_managers) |*auth| {
        auth.* = try SimAuthManager.init(sim_alloc);
        auth_count += 1;
    }
    defer for (auth_managers[0..auth_count]) |*auth| auth.deinit();

    const roots = [_][]const u8{ root_a, root_b, root_c };
    var public_api: PublicApiTestRig(3) = undefined;
    try public_api.initLeaderBackedWithAuthInPlace(sim_alloc, &cluster, roots, &auth_managers);
    defer public_api.deinit();

    const admin_auth = try encodeBasicAuthorization(std.heap.page_allocator, "admin", "admin");
    defer std.heap.page_allocator.free(admin_auth);

    for (public_api.api_base_uris) |base_uri| {
        const status_uri = try raft_transport.Routes.join(std.heap.page_allocator, base_uri, api_http_routes.Routes.status);
        defer std.heap.page_allocator.free(status_uri);

        var unauthorized = try public_api.client_executor.executor().execute(std.heap.page_allocator, .{
            .method = .GET,
            .uri = status_uri,
        });
        defer unauthorized.deinit(std.heap.page_allocator);
        try std.testing.expectEqual(@as(u16, 401), unauthorized.status);

        var authorized = try public_api.client_executor.executor().execute(std.heap.page_allocator, .{
            .method = .GET,
            .uri = status_uri,
            .authorization = admin_auth,
        });
        defer authorized.deinit(std.heap.page_allocator);
        try std.testing.expectEqual(@as(u16, 200), authorized.status);
    }
}

test "metadata http cluster simulation forwards public split flow from a non-host node after public create" {
    var sim_alloc_state: LeanSimAllocator = .init;
    defer _ = sim_alloc_state.deinit();
    const sim_alloc = sim_alloc_state.allocator();

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_d.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-split-a", .{tmp.sub_path});
    defer sim_alloc.free(root_a);
    const root_b = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-split-b", .{tmp.sub_path});
    defer sim_alloc.free(root_b);
    const root_c = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-split-c", .{tmp.sub_path});
    defer sim_alloc.free(root_c);
    const root_d = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-split-d", .{tmp.sub_path});
    defer sim_alloc.free(root_d);
    const cat_a = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-split-a.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_a);
    const cat_b = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-split-b.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_b);
    const cat_c = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-split-c.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_c);
    const cat_d = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-split-d.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4861, root_a, cat_a),
        makeHostSimConfig(2, 4861, root_b, cat_b),
        makeHostSimConfig(3, 4861, root_c, cat_c),
        makeHostSimConfig(4, 4861, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try MetadataHttpClusterSimulation.init(sim_alloc, 4861, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var http_io = std.Io.Threaded.init(sim_alloc, .{ .stack_size = lean_sim_thread_stack_size });
    defer http_io.deinit();

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(
        4,
        sim_alloc,
        &cluster,
        &http_io,
        &metadata_admin_listeners,
        &metadata_admin_servers,
        &metadata_admin_sources,
        &metadata_apis,
    );
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer deinitMetadataAdminServers(4, &metadata_admin_servers);
    defer for (metadata_apis) |uri| sim_alloc.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        sim_alloc,
        &cluster,
        &http_io,
        &roots,
        .local,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        .{},
        &api_base_uris,
    );
    defer for (api_base_uris) |uri| sim_alloc.free(uri);
    defer deinitPublicApiStack(4, &listeners, &servers, &write_sources);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "split public docs");
    defer std.heap.page_allocator.free(create_body);
    var created = try client.createTable(api_base_uris[0], "docs", create_body);
    defer created.deinit(std.heap.page_allocator);
    var created_table = try std.json.parseFromSlice(struct { name: []const u8 }, std.heap.page_allocator, created.body, .{ .ignore_unknown_fields = true });
    defer created_table.deinit();
    try std.testing.expectEqualStrings("docs", created_table.value.name);

    const created_range = try waitForFirstProjectedRange(&cluster, leader_index, null, 3, false, 96);
    const table_id = created_range.table_id;
    const source_group_id = created_range.group_id;
    try std.testing.expect(table_id != 0);
    try std.testing.expect(source_group_id != 0);
    try ensureGroupTextIndexOnActiveReplicas(&cluster, roots[0..], source_group_id, api_tables.default_full_text_index_name, 40);
    try reportSplitCandidateStatus(&cluster.node(leader_index), source_group_id, 12, 4096, "doc:m");

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":486101,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        split_body,
    );

    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, 486101, null, leader_index, 48));
    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const reconcile_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    try workflow.bootstrapDesiredFromCommitted(&cluster.node(reconcile_index));
    _ = try requireLeasedReconcile(cluster.node(reconcile_index), workflow.controlLoop());
    try cluster.stepAll();

    const split_route = try waitForPublicSplitRoute(&cluster, catalog_sources[0..], "docs", leader_index, 48);
    const left_group = split_route.left_group;
    const right_group = split_route.right_group;
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);
    const routed_client_index = split_route.client_index orelse return error.TestExpectedEqual;
    const client_base = api_base_uris[routed_client_index];

    const left_leader_index = (try waitForGroupLeaderIndex(&cluster, left_group, 96)) orelse return error.TestExpectedEqual;
    const right_leader_index = (try waitForGroupLeaderIndex(&cluster, right_group, 96)) orelse return error.TestExpectedEqual;
    try ensureGroupTextIndex(&cluster, roots[left_leader_index], left_group, api_tables.default_full_text_index_name, 40);
    try ensureGroupTextIndex(&cluster, roots[right_leader_index], right_group, api_tables.default_full_text_index_name, 40);
    try reportMergeCandidateStatuses(&cluster.node(leader_index), left_group, 16, 4096, right_group, 12, 3072);

    var batch = try client.fetchBatch(client_base, "docs",
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello left side","status":"published"},"doc:b":{"title":"beta","body":"hello left beta","status":"published"},"doc:y":{"title":"gamma","body":"hello right gamma","status":"published"},"doc:z":{"title":"zeta","body":"hello right side","status":"published"}}}
    );
    defer batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, batch.body, "\"inserted\":4") != null);

    var lookup = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer lookup.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, lookup.body, "\"zeta\"") != null);

    var query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"hello"}},"fields":["title","body"],"limit":10}
    );
    defer query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "\"_id\":\"doc:z\"") != null);

    try expectHelloCountProfile(&client, client_base, "docs", 4, 2, true);

    var deleted = try client.fetchBatch(client_base, "docs",
        \\{"deletes":["doc:y","doc:z"]}
    );
    defer deleted.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, deleted.body, "\"deleted\":2") != null);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:z", null));
}

test "metadata http cluster simulation forwards public merge flow from a non-host node after public create" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var sim_alloc_state: LeanSimAllocator = .init;
    defer _ = sim_alloc_state.deinit();
    const sim_alloc = sim_alloc_state.allocator();

    var store_a = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(sim_alloc);
    defer store_d.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = TestDescriptorFactory{ .alloc = sim_alloc, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-merge-a", .{tmp.sub_path});
    defer sim_alloc.free(root_a);
    const root_b = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-merge-b", .{tmp.sub_path});
    defer sim_alloc.free(root_b);
    const root_c = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-merge-c", .{tmp.sub_path});
    defer sim_alloc.free(root_c);
    const root_d = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-merge-d", .{tmp.sub_path});
    defer sim_alloc.free(root_d);
    const cat_a = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-merge-a.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_a);
    const cat_b = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-merge-b.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_b);
    const cat_c = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-merge-c.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_c);
    const cat_d = try std.fmt.allocPrint(sim_alloc, ".zig-cache/tmp/{s}/meta-sim-public-merge-d.txt", .{tmp.sub_path});
    defer sim_alloc.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4862, root_a, cat_a),
        makeHostSimConfig(2, 4862, root_b, cat_b),
        makeHostSimConfig(3, 4862, root_c, cat_c),
        makeHostSimConfig(4, 4862, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try MetadataHttpClusterSimulation.init(sim_alloc, 4862, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var http_io = std.Io.Threaded.init(sim_alloc, .{ .stack_size = lean_sim_thread_stack_size });
    defer http_io.deinit();

    var metadata_admin_listeners: [4]std_http_listener.StdHttpListener = undefined;
    var metadata_admin_servers: [4]metadata_http_server.MetadataHttpServer = undefined;
    var metadata_admin_sources: [4]MetadataAdminSimSource = undefined;
    var metadata_apis: [4][]const u8 = undefined;
    try startMetadataAdminServers(
        4,
        sim_alloc,
        &cluster,
        &http_io,
        &metadata_admin_listeners,
        &metadata_admin_servers,
        &metadata_admin_sources,
        &metadata_apis,
    );
    defer for (&metadata_admin_listeners) |*listener| listener.deinit();
    defer deinitMetadataAdminServers(4, &metadata_admin_servers);
    defer for (metadata_apis) |uri| sim_alloc.free(uri);

    var listeners: [4]std_http_listener.StdHttpListener = undefined;
    var servers: [4]api_http_server.ApiHttpServer = undefined;
    var status_sources: [4]PublicApiStatusSource = undefined;
    var catalog_sources: [4]PublicApiCatalogSource = undefined;
    var routers: [4]PublicApiRouter(4) = undefined;
    var read_sources: [4]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [4]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [4][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer forward_executor.deinit();
    try startPublicApiServers(
        4,
        sim_alloc,
        &cluster,
        &http_io,
        &roots,
        .local,
        &forward_executor,
        &listeners,
        &servers,
        &status_sources,
        &catalog_sources,
        &routers,
        &read_sources,
        &write_sources,
        .{},
        &api_base_uris,
    );
    defer for (api_base_uris) |uri| sim_alloc.free(uri);
    defer deinitPublicApiStack(4, &listeners, &servers, &write_sources);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    var metadata_client = metadata_http_client.MetadataHttpClient.init(std.heap.page_allocator, client_executor.executor());

    const create_body = try test_contract_helpers.encodeCreateTableRequest(std.heap.page_allocator, "merge public docs");
    defer std.heap.page_allocator.free(create_body);
    _ = try client.createTable(api_base_uris[0], "docs", create_body);

    const created_range = try waitForFirstProjectedRange(&cluster, leader_index, 1, null, false, 96);
    const table_id = created_range.table_id;
    const source_group_id = created_range.group_id;
    try std.testing.expect(table_id != 0);
    try std.testing.expect(source_group_id != 0);
    try ensureGroupTextIndexOnActiveReplicas(&cluster, roots[0..], source_group_id, api_tables.default_full_text_index_name, 40);
    try reportSplitCandidateStatus(&cluster.node(leader_index), source_group_id, 12, 4096, "doc:m");

    const split_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":486201,\"source_group_id\":{d},\"destination_group_id\":{d},\"split_key\":\"doc:m\"}}", .{
        source_group_id,
        source_group_id + 1,
    });
    defer std.testing.allocator.free(split_body);
    try metadata_client.requestTableSplit(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        split_body,
    );

    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, 486201, null, leader_index, 48));
    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const reconcile_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    try workflow.bootstrapDesiredFromCommitted(&cluster.node(reconcile_index));
    _ = try requireLeasedReconcile(cluster.node(reconcile_index), workflow.controlLoop());
    try cluster.stepAll();

    const split_route = try waitForPublicSplitRoute(&cluster, catalog_sources[0..], "docs", leader_index, 48);
    const left_group = split_route.left_group;
    const right_group = split_route.right_group;
    try std.testing.expect(left_group != 0);
    try std.testing.expect(right_group != 0);
    try std.testing.expect(left_group != right_group);
    try std.testing.expect(try cluster.waitForGroupStatusCount(left_group, .active, 3, 48));
    try std.testing.expect(try cluster.waitForGroupStatusCount(right_group, .active, 3, 48));

    const left_leader_index = (try waitForGroupLeaderIndex(&cluster, left_group, 96)) orelse return error.TestExpectedEqual;
    const right_leader_index = (try waitForGroupLeaderIndex(&cluster, right_group, 96)) orelse return error.TestExpectedEqual;
    try ensureGroupTextIndex(&cluster, roots[left_leader_index], left_group, api_tables.default_full_text_index_name, 40);
    try ensureGroupTextIndex(&cluster, roots[right_leader_index], right_group, api_tables.default_full_text_index_name, 40);
    try reportMergeCandidateStatuses(&cluster.node(leader_index), left_group, 16, 4096, right_group, 12, 3072);

    var pre_merge_batch = try client.fetchBatch(api_base_uris[0], "docs",
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello left side"},"doc:z":{"title":"zeta","body":"hello right side"}}}
    );
    defer pre_merge_batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, pre_merge_batch.body, "\"inserted\":2") != null);

    const merge_body = try std.fmt.allocPrint(std.testing.allocator, "{{\"transition_id\":486202,\"donor_group_id\":{d},\"receiver_group_id\":{d}}}", .{
        right_group,
        left_group,
    });
    defer std.testing.allocator.free(merge_body);
    try metadata_client.requestTableMerge(
        metadata_apis[currentMetadataLeaderIndex(&cluster) orelse leader_index],
        "docs",
        merge_body,
    );

    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, 486202, null, leader_index, 48));
    var merge_workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer merge_workflow.deinit();
    const merge_reconcile_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    try merge_workflow.bootstrapDesiredFromCommitted(&cluster.node(merge_reconcile_index));
    _ = try requireLeasedReconcile(cluster.node(merge_reconcile_index), merge_workflow.controlLoop());
    try cluster.stepAll();
    try std.testing.expect(try cluster.waitForGroupStatusCount(left_group, .active, 3, 48));
    try std.testing.expect(try cluster.waitForGroupStatus(right_group, .absent, 48));

    const routed_client_index = try waitForPublicMergedRoute(&cluster, catalog_sources[0].iface(), "docs", "doc:z", left_group, 48);
    const client_base = api_base_uris[routed_client_index];

    const merged_leader_index = (try waitForGroupLeaderIndex(&cluster, left_group, 48)) orelse return error.TestExpectedEqual;
    try ensureGroupTextIndex(&cluster, roots[merged_leader_index], left_group, api_tables.default_full_text_index_name, 40);

    var post_merge_batch = try client.fetchBatch(client_base, "docs",
        \\{"inserts":{"doc:z":{"title":"zeta","body":"hello merged right side"}}}
    );
    defer post_merge_batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, post_merge_batch.body, "\"inserted\":1") != null);
    try mirrorGroupBatchToActiveReplicas(&cluster, &client, api_base_uris[0..], left_group, "docs",
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello left side"},"doc:z":{"title":"zeta","body":"hello merged right side"}}}
    );

    var lookup = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer lookup.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, lookup.body, "\"zeta\"") != null);

    var query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"hello"}},"fields":["title","body"],"limit":10}
    );
    defer query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "\"_id\":\"doc:z\"") != null);

    try expectHelloCountProfile(&client, client_base, "docs", 2, 1, true);

    var deleted = try client.fetchBatch(client_base, "docs",
        \\{"deletes":["doc:z"]}
    );
    defer deleted.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, deleted.body, "\"deleted\":1") != null);
    try mirrorGroupBatchToActiveReplicas(&cluster, &client, api_base_uris[0..], left_group, "docs",
        \\{"deletes":["doc:z"]}
    );
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:z", null));
}

test "metadata http cluster simulation survives metadata leader restart during placement reconcile" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-r-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-r-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-r-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-r-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-r-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-r-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4100, root_a, cat_a),
        makeHostSimConfig(2, 4100, root_b, cat_b),
        makeHostSimConfig(3, 4100, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4100, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    _ = try workflow.createTable(&cluster.node(leader_index), .{
        .table_id = 42,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 4201,
        .table_id = 42,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    try cluster.stepAll();
    try cluster.restartNode(leader_index);
    _ = try cluster.waitForMetadataLeader(24);
    try std.testing.expect(try cluster.waitForGroupStatus(4201, .active, 40));

    const tables = try cluster.node(leader_index).listProjectedTables(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
}

test "metadata http cluster simulation drops table topology across leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-drop-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-drop-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-drop-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-drop-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-drop-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-drop-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4200, root_a, cat_a),
        makeHostSimConfig(2, 4200, root_b, cat_b),
        makeHostSimConfig(3, 4200, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4200, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    _ = try workflow.createTable(&cluster.node(leader_index), .{
        .table_id = 43,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 4301,
        .table_id = 43,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try std.testing.expect(try cluster.waitForGroupStatus(4301, .active, 32));

    const drop_summary = try workflow.dropTable(&cluster.node(leader_index), 43);
    try std.testing.expectEqual(@as(usize, 1), drop_summary.table_removals);
    try std.testing.expectEqual(@as(usize, 1), drop_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 3), drop_summary.placement_removals);

    try cluster.stepAll();
    try cluster.restartNode(leader_index);
    const new_leader = (try cluster.waitForMetadataLeader(32)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try cluster.waitForGroupStatus(4301, .absent, 48));

    const tables = try cluster.node(new_leader).listProjectedTables(std.testing.allocator);
    defer cluster.node(new_leader).freeProjectedTables(std.testing.allocator, tables);
    try std.testing.expectEqual(@as(usize, 0), tables.len);

    const ranges = try cluster.node(new_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(new_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 0), ranges.len);

    const intents = try cluster.node(new_leader).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(new_leader).freeProjectedPlacementIntents(std.testing.allocator, intents);
    try std.testing.expectEqual(@as(usize, 0), intents.len);
}

test "metadata http cluster simulation converges placement after candidate churn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-churn-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-churn-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-churn-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-churn-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-churn-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-churn-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4300, root_a, cat_a),
        makeHostSimConfig(2, 4300, root_b, cat_b),
        makeHostSimConfig(3, 4300, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4300, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try workflow.setPlacementCandidates(&.{ 1, 2 });
    const create_summary = try workflow.createTable(&cluster.node(leader_index), .{
        .table_id = 44,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, .{
        .group_id = 4401,
        .table_id = 44,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try std.testing.expectEqual(@as(usize, 2), create_summary.placement_upserts);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 4401, .active, 32));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 4401, .active, 1));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(4401));

    try workflow.setPlacementCandidates(&.{ 2, 3 });
    const churn_summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), churn_summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), churn_summary.placement_removals);

    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 4401, .absent, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 4401, .active, 1));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 4401, .active, 40));

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
    try std.testing.expectEqual(@as(usize, 2), intents.len);
    var saw_two = false;
    var saw_three = false;
    for (intents) |intent| {
        if (intent.record.group_id != 4401) continue;
        if (intent.record.local_node_id == 2) saw_two = true;
        if (intent.record.local_node_id == 3) saw_three = true;
        try std.testing.expect(intent.record.local_node_id != 1);
    }
    try std.testing.expect(saw_two);
    try std.testing.expect(saw_three);
}

test "metadata http cluster simulation drives split intent through the control loop" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-split-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-split-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-split-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-split-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-split-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-split-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4400, root_a, cat_a),
        makeHostSimConfig(2, 4400, root_b, cat_b),
        makeHostSimConfig(3, 4400, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4400, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    _ = try workflow.createTable(&cluster.node(leader_index), .{
        .table_id = 45,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 4501,
        .table_id = 45,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try std.testing.expect(try cluster.waitForGroupStatus(4501, .active, 32));
    try reportSplitCandidateStatus(&cluster.node(leader_index), 4501, 12, 4096, "doc:m");

    const split_summary = try workflow.requestSplit(&cluster.node(leader_index), .{
        .transition_id = 45001,
        .table_id = 45,
        .source_group_id = 4501,
        .destination_group_id = 4502,
        .split_key = "doc:m",
    });
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    var split_ctx = MetadataSplitTransitionProgressContext{ .transition_id = 45001 };
    try cluster.assertProgress("split-intent-progress", 24, &split_ctx, metadataSplitTransitionProgressPredicate);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    try std.testing.expectEqual(@as(u64, 45001), transitions[0].transition_id);

    const observation = (try cluster.node(query_index).observeSplitTransition(45001)) orelse return error.TestExpectedEqual;
    try std.testing.expect(observation.status.phase != .prepare or observation.status.source_split_phase != .prepare);
}

test "metadata http cluster simulation drives merge intent through the control loop" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-merge-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-merge-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-merge-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-merge-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-merge-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-merge-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4500, root_a, cat_a),
        makeHostSimConfig(2, 4500, root_b, cat_b),
        makeHostSimConfig(3, 4500, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4500, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    _ = try workflow.createTable(&cluster.node(leader_index), .{
        .table_id = 46,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, .{
        .group_id = 4601,
        .table_id = 46,
        .start_key = "doc:a",
        .end_key = "doc:m",
    });
    _ = try workflow.addRange(&cluster.node(leader_index), .{
        .group_id = 4602,
        .table_id = 46,
        .start_key = "doc:m",
        .end_key = "doc:z",
    });
    try std.testing.expect(try cluster.waitForGroupStatus(4601, .active, 32));
    try std.testing.expect(try cluster.waitForGroupStatus(4602, .active, 32));
    try reportMergeCandidateStatuses(&cluster.node(leader_index), 4601, 16, 4096, 4602, 12, 3072);

    const merge_summary = try workflow.requestMerge(&cluster.node(leader_index), .{
        .transition_id = 46001,
        .table_id = 46,
        .donor_group_id = 4602,
        .receiver_group_id = 4601,
    });
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    var merge_ctx = MetadataMergeTransitionProgressContext{ .transition_id = 46001 };
    try cluster.assertProgress("merge-intent-progress", 24, &merge_ctx, metadataMergeTransitionProgressPredicate);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    try std.testing.expectEqual(@as(u64, 46001), transitions[0].transition_id);

    const observation = (try cluster.node(query_index).observeMergeTransition(46001)) orelse return error.TestExpectedEqual;
    try std.testing.expect(observation.donor.phase != .prepare or observation.receiver.phase != .prepare);
}

test "metadata http cluster simulation drives automatic split through the control loop" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4510, root_a, cat_a),
        makeHostSimConfig(2, 4510, root_b, cat_b),
        makeHostSimConfig(3, 4510, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4510, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4511,
        .table_id = 451,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 451,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);
    const roots = [_][]const u8{ root_a, root_b, root_c };
    try seedDefaultSplitCandidateDocs(&cluster, cluster.node(leader_index), roots[0..], 4511, 64);

    try reportSplitCandidateStatus(cluster.node(leader_index), 4511, 256, 180, "doc:m");
    try cluster.stepAll();

    _ = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;
    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, null, query_index, 32));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse query_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);

    const remaining_splits = try cluster.node(finalize_leader).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedSplitTransitions(std.testing.allocator, remaining_splits);
    try std.testing.expectEqual(@as(usize, 0), remaining_splits.len);
}

test "metadata http cluster simulation uses live median key for automatic split planning" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-live-median-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-live-median-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-live-median-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-live-median-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-live-median-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-live-median-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4510, root_a, cat_a),
        makeHostSimConfig(2, 4510, root_b, cat_b),
        makeHostSimConfig(3, 4510, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4510, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();
    const leader_index = try startBootstrappedMetadataCluster(&cluster, 48, true);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45112,
        .table_id = 4512,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4512,
        .name = "docs",
        .indexes_json = api_tables.default_indexes_json,
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    const roots = [_][]const u8{ root_a, root_b, root_c };
    var public_api: PublicApiTestRig(3) = undefined;
    try public_api.initLeaderBackedInPlace(std.testing.allocator, &cluster, roots);
    defer public_api.deinit();
    var client = public_api.client;
    const client_index = (try waitForGroupLeaderIndex(&cluster, 45112, 64)) orelse return error.TestExpectedEqual;
    const client_base = public_api.api_base_uris[client_index];
    try ensureGroupTextIndex(&cluster, roots[client_index], 45112, api_tables.default_full_text_index_name, 40);

    var batch = try client.fetchBatch(client_base, "docs",
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:m":{"title":"mid","body":"middle"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, batch.body, "\"inserted\":3") != null);

    var lookup = try client.fetchLookup(client_base, "docs", "doc:m", null);
    defer lookup.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, lookup.body, "\"mid\"") != null);

    try reportSplitCandidateStatus(cluster.node(leader_index), 45112, 256, 180, "doc:t");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    try std.testing.expectEqualStrings("doc:m", transitions[0].split_key.?);
}

test "metadata http cluster simulation uses remote live median key when metadata leader is not a shard replica" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-remote-median-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-remote-median-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-remote-median-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-remote-median-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    factory_d.split_runtime.replica_root_dir = root_d;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-remote-median-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-remote-median-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-remote-median-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-remote-median-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4510, root_a, cat_a),
        makeHostSimConfig(2, 4510, root_b, cat_b),
        makeHostSimConfig(3, 4510, root_c, cat_c),
        makeHostSimConfig(4, 4510, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4510, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();
    const initial_leader = try startBootstrappedMetadataCluster(&cluster, 48, true);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45113,
        .table_id = 4513,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, initial_leader, .{
        .table_id = 4513,
        .name = "docs",
        .indexes_json = api_tables.default_indexes_json,
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 48);

    var remote_metadata_leader: ?usize = null;
    for (0..4) |i| {
        if (cluster.node(i).status(45113) != .active) {
            remote_metadata_leader = i;
            break;
        }
    }
    const forced_leader = remote_metadata_leader orelse return error.TestExpectedEqual;

    try cluster.node(forced_leader).campaignMetadataGroup();
    try std.testing.expect(try waitForMetadataLeaderIndex(&cluster, forced_leader, 96));
    try std.testing.expect(cluster.node(forced_leader).status(45113) != .active);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(forced_leader), &auto_loop);

    const roots = [_][]const u8{ root_a, root_b, root_c, root_d };
    var public_api: PublicApiTestRig(4) = undefined;
    try public_api.initLeaderBackedInPlace(std.testing.allocator, &cluster, roots);
    defer public_api.deinit();
    var client = public_api.client;
    const client_index = (try waitForGroupLeaderIndex(&cluster, 45113, 64)) orelse return error.TestExpectedEqual;
    const client_base = public_api.api_base_uris[client_index];
    try ensureGroupTextIndex(&cluster, roots[client_index], 45113, api_tables.default_full_text_index_name, 40);

    var batch = try client.fetchBatch(client_base, "docs",
        \\{"inserts":{"doc:a":{"title":"alpha","body":"left"},"doc:m":{"title":"mid","body":"middle"},"doc:z":{"title":"zeta","body":"right"}}}
    );
    defer batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, batch.body, "\"inserted\":3") != null);

    var lookup = try client.fetchLookup(client_base, "docs", "doc:m", null);
    defer lookup.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, lookup.body, "\"mid\"") != null);

    try reportSplitCandidateStatus(cluster.node(forced_leader), 45113, 256, 180, "doc:t");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(forced_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse forced_leader;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    try std.testing.expectEqualStrings("doc:m", transitions[0].split_key.?);
}

test "metadata http cluster simulation completes automatic split after metadata leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4515, root_a, cat_a),
        makeHostSimConfig(2, 4515, root_b, cat_b),
        makeHostSimConfig(3, 4515, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4515, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45151,
        .table_id = 4515,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4515,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);
    const roots = [_][]const u8{ root_a, root_b, root_c };
    try seedDefaultSplitCandidateDocs(&cluster, cluster.node(leader_index), roots[0..], 45151, 64);

    try reportSplitCandidateStatus(cluster.node(leader_index), 45151, 256, 180, "doc:m");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    try cluster.restartNode(query_index);
    _ = (try cluster.waitForMetadataLeader(32)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, null, leader_index, 48));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
}

test "metadata http cluster simulation completes automatic split after metadata leader partition" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-partition-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-partition-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-partition-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-partition-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-partition-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-partition-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4516, root_a, cat_a),
        makeHostSimConfig(2, 4516, root_b, cat_b),
        makeHostSimConfig(3, 4516, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4516, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45161,
        .table_id = 4516,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4516,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);
    const roots = [_][]const u8{ root_a, root_b, root_c };
    try seedDefaultSplitCandidateDocs(&cluster, cluster.node(leader_index), roots[0..], 45161, 64);

    try reportSplitCandidateStatus(cluster.node(leader_index), 45161, 256, 180, "doc:m");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    try isolateMetadataNode(&cluster, query_index);
    const new_leader = (try waitForMetadataLeaderExcluding(&cluster, query_index, 64)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, new_leader, new_leader, 64));

    const finalize_summary = try requireLeasedReconcile(cluster.node(new_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(new_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(new_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
}

test "metadata http cluster simulation completes automatic split under delayed raft transport" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    var delayed_a = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_a.deinit();
    var delayed_b = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_b.deinit();
    var delayed_c = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_c.deinit();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4517, root_a, cat_a),
        makeHostSimConfig(2, 4517, root_b, cat_b),
        makeHostSimConfig(3, 4517, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDepsWithTransportExecutor(&factory_a, delayed_a.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_b, delayed_b.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_c, delayed_c.executor()),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4517, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(48)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45171,
        .table_id = 4517,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4517,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 64);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);
    const roots = [_][]const u8{ root_a, root_b, root_c };
    try seedDefaultSplitCandidateDocs(&cluster, cluster.node(leader_index), roots[0..], 45171, 64);

    try reportSplitCandidateStatus(cluster.node(leader_index), 45171, 256, 180, "doc:m");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;
    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, null, query_index, 128));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse query_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
}

test "metadata http cluster simulation completes automatic split after leader restart under delayed raft transport" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    var delayed_a = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_a.deinit();
    var delayed_b = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_b.deinit();
    var delayed_c = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_c.deinit();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4519, root_a, cat_a),
        makeHostSimConfig(2, 4519, root_b, cat_b),
        makeHostSimConfig(3, 4519, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDepsWithTransportExecutor(&factory_a, delayed_a.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_b, delayed_b.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_c, delayed_c.executor()),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4519, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(48)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45191,
        .table_id = 4519,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4519,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 64);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);
    const roots = [_][]const u8{ root_a, root_b, root_c };
    try seedDefaultSplitCandidateDocs(&cluster, cluster.node(leader_index), roots[0..], 45191, 64);

    try reportSplitCandidateStatus(cluster.node(leader_index), 45191, 256, 180, "doc:m");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    try cluster.restartNode(query_index);
    _ = (try cluster.waitForMetadataLeader(64)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, null, leader_index, 128));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
}

test "metadata http cluster simulation completes automatic split after source group leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-source-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-source-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-source-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-source-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-source-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-source-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4544, root_a, cat_a),
        makeHostSimConfig(2, 4544, root_b, cat_b),
        makeHostSimConfig(3, 4544, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4544, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45441,
        .table_id = 4544,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4544,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);
    const roots = [_][]const u8{ root_a, root_b, root_c };
    try seedDefaultSplitCandidateDocs(&cluster, cluster.node(leader_index), roots[0..], 45441, 64);

    try reportSplitCandidateStatus(cluster.node(leader_index), 45441, 256, 180, "doc:m");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    const source_leader_index = (try waitForGroupLeaderIndex(&cluster, 45441, 64)) orelse return error.TestExpectedEqual;
    try cluster.restartNode(source_leader_index);
    _ = (try cluster.waitForMetadataLeader(64)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, null, query_index, 64));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse query_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
}

test "metadata http cluster simulation completes automatic split after destination group leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-destination-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-destination-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-destination-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-destination-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-destination-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-destination-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4545, root_a, cat_a),
        makeHostSimConfig(2, 4545, root_b, cat_b),
        makeHostSimConfig(3, 4545, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4545, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45451,
        .table_id = 4545,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4545,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    const roots = [_][]const u8{ root_a, root_b, root_c };
    try seedGroupDocsAcrossReplicaRoots(&cluster, roots[0..], 45451, &.{
        .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"left\"}" },
        .{ .key = "doc:m", .value = "{\"title\":\"mid\",\"body\":\"middle\"}" },
        .{ .key = "doc:z", .value = "{\"title\":\"zeta\",\"body\":\"right\"}" },
    });
    try std.testing.expect(try waitForMedianKeyEquals(&cluster, cluster.node(leader_index), 45451, "doc:m", 64));

    try reportSplitCandidateStatus(cluster.node(leader_index), 45451, 256, 180, "doc:m");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;
    const destination_group_id = transitions[0].destination_group_id;

    var destination_progress = MetadataSplitFinalizedOrGroupReadyProgressContext{
        .transition_id = transition_id,
        .group_id = destination_group_id,
        .fallback_index = query_index,
    };
    try std.testing.expect(try cluster.runUntil(160, &destination_progress, metadataSplitFinalizedOrGroupReadyProgressPredicate));

    if (destination_progress.group_index) |index| {
        try cluster.restartNode(index);
        _ = (try cluster.waitForMetadataLeader(64)) orelse return error.TestExpectedEqual;

        try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, null, query_index, 96));
    } else {
        try std.testing.expect(destination_progress.finalized);
    }

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse query_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    if (destination_progress.group_index == null) {
        const post_finalize_destination = (try waitForGroupLeaderIndex(&cluster, destination_group_id, 96)) orelse return error.TestExpectedEqual;
        try cluster.restartNode(post_finalize_destination);
        _ = (try cluster.waitForMetadataLeader(64)) orelse return error.TestExpectedEqual;
        try cluster.stepAll();
    }

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
}

test "metadata http cluster simulation completes automatic split after leader partition under delayed raft transport" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    var delayed_a = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_a.deinit();
    var delayed_b = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_b.deinit();
    var delayed_c = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_c.deinit();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-partition-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-partition-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-partition-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    factory_a.split_runtime.replica_root_dir = root_a;
    factory_b.split_runtime.replica_root_dir = root_b;
    factory_c.split_runtime.replica_root_dir = root_c;
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-partition-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-partition-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-split-delay-partition-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4521, root_a, cat_a),
        makeHostSimConfig(2, 4521, root_b, cat_b),
        makeHostSimConfig(3, 4521, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDepsWithTransportExecutor(&factory_a, delayed_a.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_b, delayed_b.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_c, delayed_c.executor()),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4521, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();
    const leader_index = try startBootstrappedMetadataCluster(&cluster, 48, true);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 45211,
        .table_id = 4521,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4521,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 64);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);
    const roots = [_][]const u8{ root_a, root_b, root_c };
    try seedDefaultSplitCandidateDocs(&cluster, cluster.node(leader_index), roots[0..], 45211, 64);

    try reportSplitCandidateStatus(cluster.node(leader_index), 45211, 256, 180, "doc:m");
    try cluster.stepAll();

    const split_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    try isolateMetadataNode(&cluster, query_index);
    const new_leader = (try waitForMetadataLeaderExcluding(&cluster, query_index, 96)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, transition_id, new_leader, new_leader, 160));

    const finalize_summary = try requireLeasedReconcile(cluster.node(new_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 2), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.split_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(new_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(new_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
}

test "metadata http cluster simulation serves public traffic across automatic split under delayed raft transport" {
    try runAutomaticSplitPublicTrafficScenario(.{
        .table_id = 4518,
        .path_prefix = "metadata-sim-auto-split-public-delay",
        .description = "automatic split public delay docs",
        .delayed_transport = true,
        .bootstrap_rounds = 48,
        .range_create_rounds = 64,
        .projected_index_rounds = 64,
        .finalize_rounds = 128,
        .verify = .{
            .route_rounds = 64,
            .active_rounds = 64,
            .leader_rounds = 192,
            .lookup_rounds = 160,
            .count_profile_rounds = 160,
        },
    });
}

test "metadata http cluster simulation serves public traffic across automatic split after leader restart under delayed raft transport" {
    try runAutomaticSplitPublicTrafficScenario(.{
        .table_id = 4523,
        .path_prefix = "metadata-sim-auto-split-public-delay-restart",
        .description = "automatic split public delay restart docs",
        .delayed_transport = true,
        .bootstrap_rounds = 48,
        .range_create_rounds = 64,
        .projected_index_rounds = 96,
        .finalize_rounds = 160,
        .post_failure_leader_wait_rounds = 96,
        .ensure_source_group_text_index_before_transition = true,
        .failure_mode = .restart_metadata_leader,
        .verify = .{
            .route_rounds = 96,
            .active_rounds = 96,
            .leader_rounds = 192,
            .lookup_rounds = 160,
            .count_profile_rounds = 160,
        },
    });
}

test "metadata http cluster simulation serves public traffic across automatic split after source leader restart under delayed raft transport" {
    try runAutomaticSplitPublicTrafficScenario(.{
        .table_id = 4548,
        .path_prefix = "metadata-sim-auto-split-public-source-restart",
        .description = "automatic split public source restart docs",
        .delayed_transport = true,
        .bootstrap_rounds = 48,
        .range_create_rounds = 64,
        .projected_index_rounds = 96,
        .finalize_rounds = 160,
        .post_failure_leader_wait_rounds = 96,
        .failure_mode = .restart_source_group_leader,
        .verify = .{
            .route_rounds = 96,
            .active_rounds = 96,
            .leader_rounds = 192,
            .lookup_rounds = 160,
            .expect_mid_doc = false,
        },
    });
}

test "metadata http cluster simulation serves public traffic across automatic split after leader partition under delayed raft transport" {
    try runAutomaticSplitPublicTrafficScenario(.{
        .table_id = 4522,
        .path_prefix = "metadata-sim-auto-split-public-delay-partition",
        .description = "automatic split public delay partition docs",
        .delayed_transport = true,
        .bootstrap_rounds = 48,
        .range_create_rounds = 64,
        .projected_index_rounds = 96,
        .finalize_rounds = 160,
        .post_failure_leader_wait_rounds = 96,
        .failure_mode = .partition_metadata_leader,
        .verify = .{
            .route_rounds = 96,
            .active_rounds = 96,
            .leader_rounds = 192,
            .lookup_rounds = 160,
            .right_active_count = 2,
            .count_profile_rounds = 160,
        },
    });
}

test "metadata http cluster simulation serves public traffic across automatic split after metadata leader partition" {
    try runAutomaticSplitPublicTrafficScenario(.{
        .table_id = 4527,
        .path_prefix = "metadata-sim-auto-split-public-partition",
        .description = "automatic split public partition docs",
        .delayed_transport = false,
        .bootstrap_rounds = 24,
        .range_create_rounds = 32,
        .projected_index_rounds = 96,
        .finalize_rounds = 64,
        .post_failure_leader_wait_rounds = 64,
        .failure_mode = .partition_metadata_leader,
        .verify = .{
            .route_rounds = 48,
            .active_rounds = 48,
            .leader_rounds = 160,
            .lookup_rounds = 160,
            .right_active_count = 2,
            .count_profile_rounds = 160,
        },
    });
}

test "metadata http cluster simulation drives automatic merge through the control loop" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4520, root_a, cat_a),
        makeHostSimConfig(2, 4520, root_b, cat_b),
        makeHostSimConfig(3, 4520, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4520, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 4521,
            .table_id = 452,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 4522,
            .table_id = 452,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 452,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    try reportMergeCandidateStatuses(cluster.node(leader_index), 4521, 16, 10, 4522, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;
    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, null, query_index, 32));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse query_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);

    const remaining_merges = try cluster.node(finalize_leader).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedMergeTransitions(std.testing.allocator, remaining_merges);
    try std.testing.expectEqual(@as(usize, 0), remaining_merges.len);
}

test "metadata http cluster simulation completes automatic merge after metadata leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4525, root_a, cat_a),
        makeHostSimConfig(2, 4525, root_b, cat_b),
        makeHostSimConfig(3, 4525, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4525, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 45251,
            .table_id = 4525,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 45252,
            .table_id = 4525,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4525,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    try reportMergeCandidateStatuses(cluster.node(leader_index), 45251, 16, 10, 45252, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    try cluster.restartNode(query_index);
    _ = (try cluster.waitForMetadataLeader(32)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, null, leader_index, 48));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
}

test "metadata http cluster simulation completes automatic merge after donor group leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-donor-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-donor-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-donor-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-donor-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-donor-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-donor-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4546, root_a, cat_a),
        makeHostSimConfig(2, 4546, root_b, cat_b),
        makeHostSimConfig(3, 4546, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4546, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 45461,
            .table_id = 4546,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 45462,
            .table_id = 4546,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4546,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    try reportMergeCandidateStatuses(cluster.node(leader_index), 45461, 16, 10, 45462, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    const donor_leader_index = (try waitForGroupLeaderIndex(&cluster, 45462, 64)) orelse return error.TestExpectedEqual;
    try cluster.restartNode(donor_leader_index);
    _ = (try cluster.waitForMetadataLeader(64)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, null, query_index, 64));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse query_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
}

test "metadata http cluster simulation completes automatic merge after receiver group leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-receiver-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-receiver-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-receiver-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-receiver-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-receiver-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-receiver-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4547, root_a, cat_a),
        makeHostSimConfig(2, 4547, root_b, cat_b),
        makeHostSimConfig(3, 4547, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4547, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 45471,
            .table_id = 4547,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 45472,
            .table_id = 4547,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4547,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    try reportMergeCandidateStatuses(cluster.node(leader_index), 45471, 16, 10, 45472, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    const receiver_leader_index = (try waitForGroupLeaderIndex(&cluster, 45471, 64)) orelse return error.TestExpectedEqual;
    try cluster.restartNode(receiver_leader_index);
    _ = (try cluster.waitForMetadataLeader(64)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, null, query_index, 64));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse query_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
}

test "metadata http cluster simulation completes automatic merge after metadata leader partition" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-partition-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-partition-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-partition-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-partition-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-partition-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-partition-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4526, root_a, cat_a),
        makeHostSimConfig(2, 4526, root_b, cat_b),
        makeHostSimConfig(3, 4526, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4526, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 45261,
            .table_id = 4526,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 45262,
            .table_id = 4526,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4526,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    try reportMergeCandidateStatuses(cluster.node(leader_index), 45261, 16, 10, 45262, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    try isolateMetadataNode(&cluster, query_index);
    const new_leader = (try waitForMetadataLeaderExcluding(&cluster, query_index, 64)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, new_leader, new_leader, 64));

    const finalize_summary = try requireLeasedReconcile(cluster.node(new_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(new_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(new_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
}

test "metadata http cluster simulation completes automatic merge under delayed raft transport" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    var delayed_a = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_a.deinit();
    var delayed_b = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_b.deinit();
    var delayed_c = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_c.deinit();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4529, root_a, cat_a),
        makeHostSimConfig(2, 4529, root_b, cat_b),
        makeHostSimConfig(3, 4529, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDepsWithTransportExecutor(&factory_a, delayed_a.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_b, delayed_b.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_c, delayed_c.executor()),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4529, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(48)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 45291,
            .table_id = 4529,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 45292,
            .table_id = 4529,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4529,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 64);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    try reportMergeCandidateStatuses(cluster.node(leader_index), 45291, 16, 10, 45292, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;
    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, null, query_index, 128));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse query_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
}

test "metadata http cluster simulation completes automatic merge after leader restart under delayed raft transport" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    var delayed_a = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_a.deinit();
    var delayed_b = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_b.deinit();
    var delayed_c = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_c.deinit();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4531, root_a, cat_a),
        makeHostSimConfig(2, 4531, root_b, cat_b),
        makeHostSimConfig(3, 4531, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDepsWithTransportExecutor(&factory_a, delayed_a.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_b, delayed_b.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_c, delayed_c.executor()),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4531, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(48)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 45311,
            .table_id = 4531,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 45312,
            .table_id = 4531,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4531,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 64);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    try reportMergeCandidateStatuses(cluster.node(leader_index), 45311, 16, 10, 45312, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    try cluster.restartNode(query_index);
    _ = (try cluster.waitForMetadataLeader(64)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, null, leader_index, 128));

    const finalize_leader = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const finalize_summary = try requireLeasedReconcile(cluster.node(finalize_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(finalize_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(finalize_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
}

test "metadata http cluster simulation completes automatic merge after leader partition under delayed raft transport" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    var delayed_a = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_a.deinit();
    var delayed_b = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_b.deinit();
    var delayed_c = raft_sim.DelayingRequestExecutor.init(std.testing.allocator, 2 * std.time.ns_per_ms);
    defer delayed_c.deinit();

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-partition-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-partition-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-partition-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-partition-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-partition-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-merge-delay-partition-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4532, root_a, cat_a),
        makeHostSimConfig(2, 4532, root_b, cat_b),
        makeHostSimConfig(3, 4532, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDepsWithTransportExecutor(&factory_a, delayed_a.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_b, delayed_b.executor()),
        makeHostSimDepsWithTransportExecutor(&factory_c, delayed_c.executor()),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4532, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(48)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 45321,
            .table_id = 4532,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 45322,
            .table_id = 4532,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 4532,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 64);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .min_shard_size_bytes = 30,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);

    try reportMergeCandidateStatuses(cluster.node(leader_index), 45321, 16, 10, 45322, 12, 12);
    try cluster.stepAll();

    const merge_summary = try requireLeasedReconcile(cluster.node(leader_index), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), merge_summary.merge_upserts);

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const transitions = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, transitions);
    try std.testing.expectEqual(@as(usize, 1), transitions.len);
    const transition_id = transitions[0].transition_id;

    try isolateMetadataNode(&cluster, query_index);
    const new_leader = (try waitForMetadataLeaderExcluding(&cluster, query_index, 96)) orelse return error.TestExpectedEqual;
    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, transition_id, new_leader, new_leader, 160));

    const finalize_summary = try requireLeasedReconcile(cluster.node(new_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), finalize_summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(new_leader).listProjectedRanges(std.testing.allocator);
    defer cluster.node(new_leader).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
}

test "metadata http cluster simulation serves public traffic across automatic merge under delayed raft transport" {
    try runAutomaticMergePublicTrafficScenario(.{
        .table_id = 4530,
        .path_prefix = "metadata-sim-auto-merge-public-delay",
        .description = "automatic merge public delay docs",
        .delayed_transport = true,
        .bootstrap_rounds = 48,
        .range_create_rounds = 64,
        .projected_index_rounds = 64,
        .finalize_rounds = 128,
        .verify = .{
            .active_rounds = 64,
            .absent_rounds = 64,
            .route_rounds = 64,
            .leader_rounds = 128,
            .lookup_rounds = 160,
            .expect_profile = true,
        },
    });
}

test "metadata http cluster simulation serves public traffic across automatic merge after leader restart under delayed raft transport" {
    try runAutomaticMergePublicTrafficScenario(.{
        .table_id = 4534,
        .path_prefix = "metadata-sim-auto-merge-public-delay-restart",
        .description = "automatic merge public delay restart docs",
        .delayed_transport = true,
        .bootstrap_rounds = 48,
        .range_create_rounds = 64,
        .projected_index_rounds = 96,
        .finalize_rounds = 160,
        .post_failure_leader_wait_rounds = 96,
        .failure_mode = .restart_metadata_leader,
        .verify = .{
            .active_rounds = 96,
            .absent_rounds = 96,
            .route_rounds = 96,
            .leader_rounds = 160,
            .lookup_rounds = 160,
            .expect_profile = true,
        },
    });
}

test "metadata http cluster simulation serves public traffic across automatic merge after donor leader restart under delayed raft transport" {
    try runAutomaticMergePublicTrafficScenario(.{
        .table_id = 4549,
        .path_prefix = "metadata-sim-auto-merge-public-donor-restart",
        .description = "automatic merge public donor restart docs",
        .delayed_transport = true,
        .bootstrap_rounds = 48,
        .range_create_rounds = 64,
        .projected_index_rounds = 96,
        .finalize_rounds = 160,
        .post_failure_leader_wait_rounds = 96,
        .failure_mode = .restart_donor_group_leader,
        .verify = .{
            .active_rounds = 96,
            .absent_rounds = 96,
            .route_rounds = 96,
            .leader_rounds = 160,
            .lookup_rounds = 160,
        },
    });
}

test "metadata http cluster simulation serves public traffic across automatic merge after leader partition under delayed raft transport" {
    try runAutomaticMergePublicTrafficScenario(.{
        .table_id = 4533,
        .path_prefix = "metadata-sim-auto-merge-public-delay-partition",
        .description = "automatic merge public delay partition docs",
        .delayed_transport = true,
        .bootstrap_rounds = 48,
        .range_create_rounds = 64,
        .projected_index_rounds = 96,
        .finalize_rounds = 160,
        .post_failure_leader_wait_rounds = 96,
        .failure_mode = .partition_metadata_leader,
        .verify = .{
            .active_rounds = 96,
            .absent_rounds = 96,
            .route_rounds = 96,
            .leader_rounds = 160,
            .lookup_rounds = 160,
            .removed_absent_count = 2,
            .expect_profile = true,
        },
    });
}

test "metadata http cluster simulation serves public traffic across automatic merge after metadata leader partition" {
    try runAutomaticMergePublicTrafficScenario(.{
        .table_id = 4528,
        .path_prefix = "metadata-sim-auto-merge-public-partition",
        .description = "automatic merge public partition docs",
        .delayed_transport = false,
        .bootstrap_rounds = 24,
        .range_create_rounds = 32,
        .projected_index_rounds = 64,
        .finalize_rounds = 64,
        .post_failure_leader_wait_rounds = 64,
        .failure_mode = .partition_metadata_leader,
        .verify = .{
            .active_rounds = 48,
            .absent_rounds = 48,
            .route_rounds = 48,
            .leader_rounds = 96,
            .lookup_rounds = 160,
            .removed_absent_count = 2,
            .expect_profile = true,
        },
    });
}

test "metadata http cluster simulation survives leader restart before forced automatic split reconcile" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-reallocate-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-reallocate-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-reallocate-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const roots = [_][]const u8{ root_a, root_b, root_c };
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-reallocate-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-reallocate-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-auto-reallocate-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4530, root_a, cat_a),
        makeHostSimConfig(2, 4530, root_b, cat_b),
        makeHostSimConfig(3, 4530, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4530, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();
    const leader_index = try startBootstrappedMetadataCluster(&cluster, 24, true);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4531,
        .table_id = 453,
        .start_key = "doc:a",
        .end_key = null,
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 453,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    var auto_loop = metadata_control_loop.MetadataControlLoop.initWithConfig(std.testing.allocator, .{
        .max_shard_size_bytes = 100,
        .disable_shard_alloc = true,
    });
    defer auto_loop.deinit();
    try bootstrapDesiredLoop(cluster.node(leader_index), &auto_loop);
    try seedDefaultSplitCandidateDocs(&cluster, cluster.node(leader_index), roots[0..], 4531, 64);

    try reportSplitCandidateStatus(cluster.node(leader_index), 4531, 384, 180, "doc:m");
    try cluster.stepAll();

    try cluster.node(leader_index).requestReallocation(1);
    try std.testing.expect((try cluster.node(leader_index).getProjectedReallocationRequest()) != null);

    try cluster.restartNode(leader_index);
    const new_leader = (try cluster.waitForMetadataLeader(32)) orelse return error.TestExpectedEqual;

    const summary = try requireLeasedReconcile(cluster.node(new_leader), &auto_loop);
    try std.testing.expectEqual(@as(usize, 1), summary.split_upserts);
    try std.testing.expect((try cluster.node(new_leader).getProjectedReallocationRequest()) == null);
}

test "metadata http cluster simulation publishes split topology after finalize" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-final-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-final-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-final-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-final-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-final-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-final-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4700, root_a, cat_a),
        makeHostSimConfig(2, 4700, root_b, cat_b),
        makeHostSimConfig(3, 4700, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4700, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4701,
        .table_id = 47,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 47,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);
    try reportSplitCandidateStatus(&cluster.node(leader_index), 4701, 12, 4096, "doc:m");

    _ = try workflow.requestSplit(&cluster.node(leader_index), .{
        .transition_id = 47001,
        .table_id = 47,
        .source_group_id = 4701,
        .destination_group_id = 4702,
        .split_key = "doc:m",
    });

    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, 47001, null, leader_index, 32));

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const summary = try requireLeasedReconcile(cluster.node(query_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.split_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 2), ranges.len);

    const splits = try cluster.node(query_index).listProjectedSplitTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 0), splits.len);
}

test "metadata http cluster simulation publishes merge topology after finalize" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-final-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-final-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-final-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-final-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-final-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-final-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4800, root_a, cat_a),
        makeHostSimConfig(2, 4800, root_b, cat_b),
        makeHostSimConfig(3, 4800, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4800, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 4801,
            .table_id = 48,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 4802,
            .table_id = 48,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 48,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 32);
    try reportMergeCandidateStatuses(&cluster.node(leader_index), 4801, 16, 4096, 4802, 12, 3072);

    _ = try workflow.requestMerge(&cluster.node(leader_index), .{
        .transition_id = 48001,
        .table_id = 48,
        .donor_group_id = 4802,
        .receiver_group_id = 4801,
    });

    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, 48001, null, leader_index, 32));

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const summary = try requireLeasedReconcile(cluster.node(query_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 1), summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), summary.merge_removals);
    try cluster.stepAll();

    const ranges = try cluster.node(query_index).listProjectedRanges(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);

    const merges = try cluster.node(query_index).listProjectedMergeTransitions(std.testing.allocator);
    defer cluster.node(query_index).freeProjectedMergeTransitions(std.testing.allocator, merges);
    try std.testing.expectEqual(@as(usize, 0), merges.len);
}

test "metadata http cluster simulation provisions split destination replicas across nodes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-multi-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-multi-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-multi-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-multi-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-multi-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-split-multi-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4810, root_a, cat_a),
        makeHostSimConfig(2, 4810, root_b, cat_b),
        makeHostSimConfig(3, 4810, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4810, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4811,
        .table_id = 481,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 481,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 40);
    try std.testing.expectEqual(@as(usize, 3), create_summary.placement_upserts);
    try reportSplitCandidateStatus(&cluster.node(leader_index), 4811, 12, 4096, "doc:m");

    _ = try workflow.requestSplit(&cluster.node(leader_index), .{
        .transition_id = 48101,
        .table_id = 481,
        .source_group_id = 4811,
        .destination_group_id = 4812,
        .split_key = "doc:m",
    });

    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, 48101, null, leader_index, 40));

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const summary = try requireLeasedReconcile(cluster.node(query_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.split_removals);
    try cluster.stepAll();

    try std.testing.expect(try cluster.waitForGroupStatus(4811, .active, 40));
    try std.testing.expect(try cluster.waitForGroupStatus(4812, .active, 40));
}

test "metadata http cluster simulation retires merge donor replicas across nodes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-multi-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-multi-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-multi-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-multi-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-multi-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-merge-multi-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4820, root_a, cat_a),
        makeHostSimConfig(2, 4820, root_b, cat_b),
        makeHostSimConfig(3, 4820, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4820, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 4821,
            .table_id = 482,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 4822,
            .table_id = 482,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 482,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 2,
    }, initial_ranges[0..], 40);
    try std.testing.expectEqual(@as(usize, 2), create_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 6), create_summary.placement_upserts);
    try reportMergeCandidateStatuses(&cluster.node(leader_index), 4821, 16, 4096, 4822, 12, 3072);

    _ = try workflow.requestMerge(&cluster.node(leader_index), .{
        .transition_id = 48201,
        .table_id = 482,
        .donor_group_id = 4822,
        .receiver_group_id = 4821,
    });

    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, 48201, null, leader_index, 40));

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    const summary = try requireLeasedReconcile(cluster.node(query_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 1), summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.range_removals);
    try std.testing.expectEqual(@as(usize, 1), summary.merge_removals);
    try cluster.stepAll();

    try std.testing.expect(try cluster.waitForGroupStatus(4821, .active, 40));
    try std.testing.expect(try cluster.waitForGroupStatus(4822, .absent, 40));
}

test "metadata http cluster simulation forwards public table io from a non-host node" {
    const TestStatusSource = struct {
        node: MetadataHttpNodeSimulation,

        fn iface(self: *@This()) api_http_server.StatusSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .status = status,
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.metadataStatus();
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.adminSnapshot();
        }

        fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.node.freeAdminSnapshot(snapshot);
        }
    };

    const TestCatalogSource = struct {
        node: MetadataHttpNodeSimulation,

        fn iface(self: *@This()) api_table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.adminSnapshot();
        }

        fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.node.freeAdminSnapshot(snapshot);
        }
    };

    const TestRouter = struct {
        node: MetadataHttpNodeSimulation,
        cluster: *MetadataHttpClusterSimulation,
        api_base_uris: *const [3][]const u8,

        fn iface(self: *@This()) api_table_router.HostedGroupRouter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .local_node_id = localNodeId,
                    .local_status = localStatus,
                    .group_leader_node_id = groupLeaderNodeId,
                    .node_status = nodeStatus,
                    .node_base_uri = nodeBaseUri,
                },
            };
        }

        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @as(u64, @intCast(self.node.index + 1));
        }

        fn localStatus(ptr: *anyopaque, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.node.status(group_id);
        }

        fn groupLeaderNodeId(ptr: *anyopaque, group_id: u64) ?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (self.cluster.cluster.nodes) |*sim| {
                if (sim.leaderId(group_id)) |leader_id| return leader_id;
            }
            return null;
        }

        fn nodeStatus(ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.cluster.cluster.nodes.len) return .absent;
            return self.cluster.node(@intCast(node_id - 1)).status(group_id);
        }

        fn nodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.api_base_uris.len) return null;
            return try alloc.dupe(u8, self.api_base_uris[node_id - 1]);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-forward-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-forward-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-forward-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-forward-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-forward-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-forward-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4830, root_a, cat_a),
        makeHostSimConfig(2, 4830, root_b, cat_b),
        makeHostSimConfig(3, 4830, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4830, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4831,
        .table_id = 483,
        .start_key = "doc:a",
        .end_key = null,
    }};
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 483,
        .name = "docs",
        .description = "forwarded docs",
        .indexes_json = api_tables.default_indexes_json,
        .desired_replica_count = 1,
        .min_ranges = 1,
    }, initial_ranges[0..], 40);
    try std.testing.expectEqual(@as(usize, 1), create_summary.placement_upserts);

    const actual_host_index = try waitForSingleActiveGroupHost(&cluster, 4831, 40);
    const client_index: usize = if (actual_host_index == 0) 1 else 0;
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(client_index).status(4831));

    var http_io = std.Io.Threaded.init(std.testing.allocator, .{ .stack_size = lean_sim_thread_stack_size });
    defer http_io.deinit();

    var listeners: [3]std_http_listener.StdHttpListener = undefined;
    var servers: [3]api_http_server.ApiHttpServer = undefined;
    var status_sources: [3]TestStatusSource = undefined;
    var catalog_sources: [3]TestCatalogSource = undefined;
    var routers: [3]TestRouter = undefined;
    var read_sources: [3]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [3]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [3][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer forward_executor.deinit();

    for (0..3) |i| {
        status_sources[i] = .{ .node = cluster.node(i) };
        catalog_sources[i] = .{ .node = cluster.node(i) };
        routers[i] = .{ .node = cluster.node(i), .cluster = &cluster, .api_base_uris = &api_base_uris };
        read_sources[i] = api_table_reads.HostedProvisionedTableReadSource.init(
            roots[i],
            catalog_sources[i].iface(),
            cluster.cluster.node(i).runtime.svc.readableLeaseRequester(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        _ = read_sources[i].withIo(&http_io);
        write_sources[i] = api_table_writes.HostedProvisionedTableWriteSource.init(
            roots[i],
            catalog_sources[i].iface(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        attachHostedSourcesBackendRuntimeForSimulation(&read_sources[i], &write_sources[i], cluster.backendRuntime(i));
        servers[i] = api_http_server.ApiHttpServer.init(
            std.testing.allocator,
            .{},
            status_sources[i].iface(),
            read_sources[i].source(),
            write_sources[i].source(),
        );
        listeners[i] = std_http_listener.StdHttpListener.initShared(std.testing.allocator, lean_sim_http_listener_cfg, servers[i].executor(), &http_io);
        try listeners[i].start();
    }
    for (0..3) |i| api_base_uris[i] = try listeners[i].baseUri(std.testing.allocator);
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);
    defer deinitPublicApiStack(3, &listeners, &servers, &write_sources);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    const client_base = api_base_uris[client_index];

    var batch = try client.fetchBatch(client_base, "docs",
        \\{"inserts":{"doc:z":{"title":"zeta","body":"hello forwarded world","status":"published","created_at":"2026-03-01T00:00:00Z"},"doc:y":{"title":"gamma","body":"hello forwarded extra","status":"published","created_at":"2026-03-20T00:00:00Z"},"doc:x":{"title":"beta","body":"hello hidden","status":"draft","created_at":"2026-03-10T00:00:00Z"},"doc:w":{"title":"alpine","body":"wild forwarded token","status":"published","created_at":"2026-03-12T00:00:00Z"}}}
    );
    defer batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, batch.body, "\"inserted\":4") != null);

    try ensureGroupTextIndex(&cluster, roots[actual_host_index], 4831, api_tables.default_full_text_index_name, 40);

    var lookup = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer lookup.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, lookup.body, "\"zeta\"") != null);

    var query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"forwarded"}},"fields":["title","body"],"limit":10}
    );
    defer query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "\"_id\":\"doc:z\"") != null);

    var filtered_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"hello"}},"filter_query":{"term":"published","field":"status"},"exclusion_query":{"term":"gamma","field":"title"},"fields":["title","body","status"],"limit":10}
    );
    defer filtered_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"_id\":\"doc:z\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"_id\":\"doc:y\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"_id\":\"doc:x\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"status\":\"published\"") != null);

    var phrase_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match_phrase":"forwarded world","field":"body"},"fields":["title","body"],"limit":10}
    );
    defer phrase_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, phrase_query.body, "\"_id\":\"doc:z\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, phrase_query.body, "\"_id\":\"doc:y\"") == null);

    var date_range_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"field":"created_at","start":"2026-03-15T00:00:00Z","end":"2026-03-25T00:00:00Z","inclusive_end":true},"fields":["title","created_at"],"limit":10}
    );
    defer date_range_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, date_range_query.body, "\"_id\":\"doc:y\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, date_range_query.body, "\"_id\":\"doc:z\"") == null);

    var wildcard_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"wildcard":"alp*","field":"title"},"fields":["title"],"limit":10}
    );
    defer wildcard_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, wildcard_query.body, "\"_id\":\"doc:w\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, wildcard_query.body, "\"_id\":\"doc:z\"") == null);

    var regexp_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"regexp":"^g.*a$","field":"title"},"fields":["title"],"limit":10}
    );
    defer regexp_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, regexp_query.body, "\"_id\":\"doc:y\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, regexp_query.body, "\"_id\":\"doc:w\"") == null);

    try expectCountProfile(&client, client_base, "docs", "forwarded", 3, 1, true);

    var deleted = try client.fetchBatch(client_base, "docs",
        \\{"deletes":["doc:z","doc:y","doc:w"]}
    );
    defer deleted.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, deleted.body, "\"deleted\":3") != null);

    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:z", null));
}

test "metadata http cluster simulation forwards public table io across split ranges from a non-host node" {
    const TestStatusSource = struct {
        node: MetadataHttpNodeSimulation,
        fn iface(self: *@This()) api_http_server.StatusSource {
            return .{ .ptr = self, .vtable = &.{ .status = status, .admin_snapshot = adminSnapshot, .free_admin_snapshot = freeAdminSnapshot } };
        }
        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.metadataStatus();
        }
        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.adminSnapshot();
        }
        fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.node.freeAdminSnapshot(snapshot);
        }
    };
    const TestCatalogSource = struct {
        node: MetadataHttpNodeSimulation,
        fn iface(self: *@This()) api_table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{ .admin_snapshot = adminSnapshot, .free_admin_snapshot = freeAdminSnapshot } };
        }
        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.adminSnapshot();
        }
        fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.node.freeAdminSnapshot(snapshot);
        }
    };
    const TestRouter = struct {
        node: MetadataHttpNodeSimulation,
        cluster: *MetadataHttpClusterSimulation,
        api_base_uris: *const [3][]const u8,
        fn iface(self: *@This()) api_table_router.HostedGroupRouter {
            return .{ .ptr = self, .vtable = &.{ .local_node_id = localNodeId, .local_status = localStatus, .group_leader_node_id = groupLeaderNodeId, .node_status = nodeStatus, .node_base_uri = nodeBaseUri } };
        }
        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @as(u64, @intCast(self.node.index + 1));
        }
        fn localStatus(ptr: *anyopaque, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.node.status(group_id);
        }
        fn groupLeaderNodeId(ptr: *anyopaque, group_id: u64) ?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (self.cluster.cluster.nodes) |*sim| {
                if (sim.leaderId(group_id)) |leader_id| return leader_id;
            }
            return null;
        }
        fn nodeStatus(ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.cluster.cluster.nodes.len) return .absent;
            return self.cluster.node(@intCast(node_id - 1)).status(group_id);
        }
        fn nodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.api_base_uris.len) return null;
            return try alloc.dupe(u8, self.api_base_uris[node_id - 1]);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-split-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-split-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-split-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-split-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-split-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-split-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4840, root_a, cat_a),
        makeHostSimConfig(2, 4840, root_b, cat_b),
        makeHostSimConfig(3, 4840, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4840, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4841,
        .table_id = 484,
        .start_key = "doc:a",
        .end_key = null,
    }};
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 484,
        .name = "docs",
        .description = "split forwarded docs",
        .indexes_json = api_tables.default_indexes_json,
        .desired_replica_count = 1,
        .min_ranges = 1,
    }, initial_ranges[0..], 40);
    try std.testing.expectEqual(@as(usize, 1), create_summary.placement_upserts);
    try reportSplitCandidateStatus(&cluster.node(leader_index), 4841, 12, 4096, "doc:m");

    _ = try workflow.requestSplit(&cluster.node(leader_index), .{
        .transition_id = 48401,
        .table_id = 484,
        .source_group_id = 4841,
        .destination_group_id = 4842,
        .split_key = "doc:m",
    });

    try std.testing.expect(try waitForSplitTransitionFinalized(&cluster, 48401, null, leader_index, 40));

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    _ = try requireLeasedReconcile(cluster.node(query_index), workflow.controlLoop());
    try cluster.stepAll();

    var left_host: ?usize = null;
    var right_host: ?usize = null;
    for (0..3) |i| {
        if (cluster.node(i).status(4841) == .active) left_host = i;
        if (cluster.node(i).status(4842) == .active) right_host = i;
    }
    const left = left_host orelse return error.TestExpectedEqual;
    const right = right_host orelse return error.TestExpectedEqual;
    try std.testing.expect(left != right);
    const client_index: usize = for (0..3) |i| {
        if (i != left and i != right) break i;
    } else return error.TestExpectedEqual;

    var http_io = std.Io.Threaded.init(std.testing.allocator, .{ .stack_size = lean_sim_thread_stack_size });
    defer http_io.deinit();

    var listeners: [3]std_http_listener.StdHttpListener = undefined;
    var servers: [3]api_http_server.ApiHttpServer = undefined;
    var status_sources: [3]TestStatusSource = undefined;
    var catalog_sources: [3]TestCatalogSource = undefined;
    var routers: [3]TestRouter = undefined;
    var read_sources: [3]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [3]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [3][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer forward_executor.deinit();
    for (0..3) |i| {
        status_sources[i] = .{ .node = cluster.node(i) };
        catalog_sources[i] = .{ .node = cluster.node(i) };
        routers[i] = .{ .node = cluster.node(i), .cluster = &cluster, .api_base_uris = &api_base_uris };
        read_sources[i] = api_table_reads.HostedProvisionedTableReadSource.init(
            roots[i],
            catalog_sources[i].iface(),
            cluster.cluster.node(i).runtime.svc.readableLeaseRequester(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        _ = read_sources[i].withIo(&http_io);
        write_sources[i] = api_table_writes.HostedProvisionedTableWriteSource.init(
            roots[i],
            catalog_sources[i].iface(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        attachHostedSourcesBackendRuntimeForSimulation(&read_sources[i], &write_sources[i], cluster.backendRuntime(i));
        servers[i] = api_http_server.ApiHttpServer.init(std.testing.allocator, .{}, status_sources[i].iface(), read_sources[i].source(), write_sources[i].source());
        listeners[i] = std_http_listener.StdHttpListener.initShared(std.testing.allocator, lean_sim_http_listener_cfg, servers[i].executor(), &http_io);
        try listeners[i].start();
    }
    for (0..3) |i| api_base_uris[i] = try listeners[i].baseUri(std.testing.allocator);
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);
    defer deinitPublicApiStack(3, &listeners, &servers, &write_sources);

    const routed_groups = try waitForSplitResolvedGroups(&cluster, catalog_sources[client_index].iface(), "docs", 40);
    try std.testing.expectEqual(@as(u64, 4841), routed_groups.left_group);
    try std.testing.expectEqual(@as(u64, 4842), routed_groups.right_group);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    const client_base = api_base_uris[client_index];

    var batch = try client.fetchBatch(client_base, "docs",
        \\{"inserts":{"doc:a":{"title":"alpha","body":"hello left","status":"published","score":10},"doc:b":{"title":"beta","body":"hello draft","status":"draft","score":3},"doc:y":{"title":"gamma","body":"hello extra right","status":"published","score":8},"doc:z":{"title":"zeta","body":"hello right","status":"published","score":9},"doc:m":{"title":"alpine","body":"midpoint hello","status":"published","score":6}}}
    );
    defer batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, batch.body, "\"inserted\":5") != null);

    try ensureGroupTextIndex(&cluster, roots[left], 4841, api_tables.default_full_text_index_name, 40);
    try ensureGroupTextIndex(&cluster, roots[right], 4842, api_tables.default_full_text_index_name, 40);

    var lookup = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer lookup.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, lookup.body, "\"zeta\"") != null);

    var query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"hello"}},"fields":["title","body"],"limit":10}
    );
    defer query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "\"_id\":\"doc:z\"") != null);

    var filtered_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"hello"}},"filter_query":{"term":"published","field":"status"},"exclusion_query":{"term":"gamma","field":"title"},"fields":["title","body","status"],"limit":10}
    );
    defer filtered_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"_id\":\"doc:z\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"_id\":\"doc:y\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"_id\":\"doc:b\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, filtered_query.body, "\"status\":\"published\"") != null);

    var prefix_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"prefix":"alp","field":"title"},"fields":["title"],"limit":10}
    );
    defer prefix_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, prefix_query.body, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, prefix_query.body, "\"_id\":\"doc:m\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, prefix_query.body, "\"_id\":\"doc:z\"") == null);

    var term_range_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"field":"title","min":"alpha","max":"beta","inclusive_max":false},"fields":["title"],"limit":10}
    );
    defer term_range_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, term_range_query.body, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, term_range_query.body, "\"_id\":\"doc:m\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, term_range_query.body, "\"_id\":\"doc:b\"") == null);

    var numeric_range_query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"field":"score","min":9,"max":10,"inclusive_max":true},"fields":["title","score"],"limit":10}
    );
    defer numeric_range_query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, numeric_range_query.body, "\"_id\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, numeric_range_query.body, "\"_id\":\"doc:z\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, numeric_range_query.body, "\"_id\":\"doc:y\"") == null);

    try expectHelloCountProfile(&client, client_base, "docs", 5, 2, true);

    var deleted = try client.fetchBatch(client_base, "docs",
        \\{"deletes":["doc:y","doc:z","doc:m"]}
    );
    defer deleted.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, deleted.body, "\"deleted\":3") != null);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:z", null));
}

test "metadata http cluster simulation forwards public table io after merge finalization from a non-host node" {
    const TestStatusSource = struct {
        node: MetadataHttpNodeSimulation,
        fn iface(self: *@This()) api_http_server.StatusSource {
            return .{ .ptr = self, .vtable = &.{ .status = status, .admin_snapshot = adminSnapshot, .free_admin_snapshot = freeAdminSnapshot } };
        }
        fn status(ptr: *anyopaque) !metadata_api.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.metadataStatus();
        }
        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.adminSnapshot();
        }
        fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.node.freeAdminSnapshot(snapshot);
        }
    };
    const TestCatalogSource = struct {
        node: MetadataHttpNodeSimulation,
        fn iface(self: *@This()) api_table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{ .admin_snapshot = adminSnapshot, .free_admin_snapshot = freeAdminSnapshot } };
        }
        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try self.node.adminSnapshot();
        }
        fn freeAdminSnapshot(ptr: *anyopaque, snapshot: *metadata_api.AdminSnapshot) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.node.freeAdminSnapshot(snapshot);
        }
    };
    const TestRouter = struct {
        node: MetadataHttpNodeSimulation,
        cluster: *MetadataHttpClusterSimulation,
        api_base_uris: *const [3][]const u8,
        fn iface(self: *@This()) api_table_router.HostedGroupRouter {
            return .{ .ptr = self, .vtable = &.{ .local_node_id = localNodeId, .local_status = localStatus, .group_leader_node_id = groupLeaderNodeId, .node_status = nodeStatus, .node_base_uri = nodeBaseUri } };
        }
        fn localNodeId(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @as(u64, @intCast(self.node.index + 1));
        }
        fn localStatus(ptr: *anyopaque, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.node.status(group_id);
        }
        fn groupLeaderNodeId(ptr: *anyopaque, group_id: u64) ?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            for (self.cluster.cluster.nodes) |*sim| {
                if (sim.leaderId(group_id)) |leader_id| return leader_id;
            }
            return null;
        }
        fn nodeStatus(ptr: *anyopaque, node_id: u64, group_id: u64) raft_host.HostedReplicaStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.cluster.cluster.nodes.len) return .absent;
            return self.cluster.node(@intCast(node_id - 1)).status(group_id);
        }
        fn nodeBaseUri(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (node_id == 0 or node_id > self.api_base_uris.len) return null;
            return try alloc.dupe(u8, self.api_base_uris[node_id - 1]);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-merge-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-merge-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-merge-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-merge-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-merge-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/meta-sim-api-merge-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4850, root_a, cat_a),
        makeHostSimConfig(2, 4850, root_b, cat_b),
        makeHostSimConfig(3, 4850, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4850, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();
    try cluster.publishClusterNodes(leader_index);
    try cluster.publishClusterStores(leader_index);

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 4851,
            .table_id = 485,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 4852,
            .table_id = 485,
            .start_key = "doc:m",
            .end_key = null,
        },
    };
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 485,
        .name = "docs",
        .description = "merge forwarded docs",
        .indexes_json = api_tables.default_indexes_json,
        .desired_replica_count = 1,
        .min_ranges = 2,
    }, initial_ranges[0..], 40);
    try std.testing.expectEqual(@as(usize, 2), create_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 2), create_summary.placement_upserts);
    try reportMergeCandidateStatuses(&cluster.node(leader_index), 4851, 16, 4096, 4852, 12, 3072);

    _ = try workflow.requestMerge(&cluster.node(leader_index), .{
        .transition_id = 48501,
        .table_id = 485,
        .donor_group_id = 4852,
        .receiver_group_id = 4851,
    });

    try std.testing.expect(try waitForMergeTransitionFinalized(&cluster, 48501, null, leader_index, 40));

    const query_index = currentMetadataLeaderIndex(&cluster) orelse leader_index;
    _ = try requireLeasedReconcile(cluster.node(query_index), workflow.controlLoop());
    try cluster.stepAll();

    var receiver_host: ?usize = null;
    for (0..3) |i| {
        if (cluster.node(i).status(4851) == .active) receiver_host = i;
    }
    const host = receiver_host orelse return error.TestExpectedEqual;
    const client_index: usize = if (host == 0) 1 else 0;
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(client_index).status(4851));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(client_index).status(4852));

    var http_io = std.Io.Threaded.init(std.testing.allocator, .{ .stack_size = lean_sim_thread_stack_size });
    defer http_io.deinit();

    var listeners: [3]std_http_listener.StdHttpListener = undefined;
    var servers: [3]api_http_server.ApiHttpServer = undefined;
    var status_sources: [3]TestStatusSource = undefined;
    var catalog_sources: [3]TestCatalogSource = undefined;
    var routers: [3]TestRouter = undefined;
    var read_sources: [3]api_table_reads.HostedProvisionedTableReadSource = undefined;
    var write_sources: [3]api_table_writes.HostedProvisionedTableWriteSource = undefined;
    var api_base_uris: [3][]const u8 = undefined;
    const roots = [_][]const u8{ root_a, root_b, root_c };

    var forward_executor: std_http_executor.StdHttpExecutor = undefined;
    forward_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer forward_executor.deinit();
    for (0..3) |i| {
        status_sources[i] = .{ .node = cluster.node(i) };
        catalog_sources[i] = .{ .node = cluster.node(i) };
        routers[i] = .{ .node = cluster.node(i), .cluster = &cluster, .api_base_uris = &api_base_uris };
        read_sources[i] = api_table_reads.HostedProvisionedTableReadSource.init(
            roots[i],
            catalog_sources[i].iface(),
            cluster.cluster.node(i).runtime.svc.readableLeaseRequester(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        _ = read_sources[i].withIo(&http_io);
        write_sources[i] = api_table_writes.HostedProvisionedTableWriteSource.init(
            roots[i],
            catalog_sources[i].iface(),
            routers[i].iface(),
            forward_executor.executor(),
        );
        attachHostedSourcesBackendRuntimeForSimulation(&read_sources[i], &write_sources[i], cluster.backendRuntime(i));
        servers[i] = api_http_server.ApiHttpServer.init(std.testing.allocator, .{}, status_sources[i].iface(), read_sources[i].source(), write_sources[i].source());
        listeners[i] = std_http_listener.StdHttpListener.initShared(std.testing.allocator, lean_sim_http_listener_cfg, servers[i].executor(), &http_io);
        try listeners[i].start();
    }
    for (0..3) |i| api_base_uris[i] = try listeners[i].baseUri(std.testing.allocator);
    defer for (api_base_uris) |uri| std.testing.allocator.free(uri);
    defer deinitPublicApiStack(3, &listeners, &servers, &write_sources);

    _ = try waitForResolvedGroupForKey(&cluster, catalog_sources[client_index].iface(), "docs", "doc:z", 4851, 40);

    var client_executor: std_http_executor.StdHttpExecutor = undefined;
    client_executor.initSharedInPlace(std.heap.page_allocator, .{}, &http_io);
    defer client_executor.deinit();
    var client = api_http_client.ApiHttpClient.init(std.heap.page_allocator, client_executor.executor());
    const client_base = api_base_uris[client_index];

    var batch = try client.fetchBatch(client_base, "docs",
        \\{"inserts":{"doc:z":{"title":"zeta","body":"hello merged remote"}}}
    );
    defer batch.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, batch.body, "\"inserted\":1") != null);

    try ensureGroupTextIndex(&cluster, roots[host], 4851, api_tables.default_full_text_index_name, 40);

    var lookup = try client.fetchLookup(client_base, "docs", "doc:z", null);
    defer lookup.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, lookup.body, "\"zeta\"") != null);

    var query = try client.fetchQuery(client_base, "docs",
        \\{"full_text_search":{"match":{"field":"body","text":"merged"}},"fields":["title","body"],"limit":10}
    );
    defer query.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "\"_id\":\"doc:z\"") != null);

    var deleted = try client.fetchBatch(client_base, "docs",
        \\{"deletes":["doc:z"]}
    );
    defer deleted.deinit(std.heap.page_allocator);
    try std.testing.expect(std.mem.indexOf(u8, deleted.body, "\"deleted\":1") != null);
    try std.testing.expectError(error.UnexpectedHttpStatus, client.fetchLookup(client_base, "docs", "doc:z", null));
}

test "metadata http cluster simulation reconverges placement from committed node membership" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-nodes-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-nodes-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-nodes-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-nodes-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-nodes-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-nodes-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4600, root_a, cat_a),
        makeHostSimConfig(2, 4600, root_b, cat_b),
        makeHostSimConfig(3, 4600, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4600, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertNode(.{ .node_id = 1, .role = "data" });
    try cluster.node(leader_index).upsertNode(.{ .node_id = 2, .role = "data" });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4701,
        .table_id = 47,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 47,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);
    try std.testing.expectEqual(@as(usize, 2), create_summary.placement_upserts);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 4701, .active, 1));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(4701));

    try cluster.node(leader_index).removeNode(1);
    try cluster.node(leader_index).upsertNode(.{ .node_id = 3, .role = "data" });
    try cluster.stepAll();

    const reconcile_summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), reconcile_summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), reconcile_summary.placement_removals);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 4701, .absent, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 4701, .active, 1));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 4701, .active, 40));

    const nodes = try cluster.node(leader_index).listProjectedNodes(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedNodes(std.testing.allocator, nodes);
    try std.testing.expectEqual(@as(usize, 2), nodes.len);
    var saw_two = false;
    var saw_three = false;
    for (nodes) |record| {
        if (record.node_id == 2) saw_two = true;
        if (record.node_id == 3) saw_three = true;
        try std.testing.expect(record.node_id != 1);
    }
    try std.testing.expect(saw_two);
    try std.testing.expect(saw_three);
}

test "metadata http cluster simulation reconverges placement from committed live stores" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-stores-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-stores-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-stores-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-stores-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-stores-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-stores-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4700, root_a, cat_a),
        makeHostSimConfig(2, 4700, root_b, cat_b),
        makeHostSimConfig(3, 4700, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4700, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4801,
        .table_id = 48,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 48,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);
    try std.testing.expectEqual(@as(usize, 2), create_summary.placement_upserts);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 4801, .active, 1));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(4801));

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = false });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true });
    try cluster.stepAll();

    const reconcile_summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), reconcile_summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), reconcile_summary.placement_removals);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 4801, .absent, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 4801, .active, 1));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 4801, .active, 40));

    const stores = try cluster.node(leader_index).listProjectedStores(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedStores(std.testing.allocator, stores);
    try std.testing.expectEqual(@as(usize, 3), stores.len);
    var saw_live_two = false;
    var saw_live_three = false;
    for (stores) |record| {
        if (record.node_id == 2 and record.live) saw_live_two = true;
        if (record.node_id == 3 and record.live) saw_live_three = true;
        if (record.node_id == 1) try std.testing.expect(!record.live);
    }
    try std.testing.expect(saw_live_two);
    try std.testing.expect(saw_live_three);
}

test "metadata http cluster simulation drains node through shutdown API" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-shutdown-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-shutdown-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-shutdown-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-shutdown-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-shutdown-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-shutdown-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4720, root_a, cat_a),
        makeHostSimConfig(2, 4720, root_b, cat_b),
        makeHostSimConfig(3, 4720, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4720, configs[0..], deps[0..]);
    defer cluster.deinit();
    defer cluster.stopAll();
    const leader_index = try startBootstrappedMetadataCluster(&cluster, 24, false);

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true });

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4821,
        .table_id = 482,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 482,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);
    try std.testing.expectEqual(@as(usize, 2), create_summary.placement_upserts);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 4821, .active, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 4821, .active, 40));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(4821));

    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true });
    try requestNodeShutdownViaSimAdmin(&cluster, leader_index, 1);
    var drain_ctx = VoprStoreDrainProgressContext{ .store_id = 1, .expected_drain_requested = true };
    try cluster.assertProgress("metadata-sim-node-shutdown-drain-requested", 32, &drain_ctx, voprStoreDrainProgressPredicate);

    const reconcile_summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), reconcile_summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), reconcile_summary.placement_removals);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 4821, .absent, 64));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 4821, .active, 1));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 4821, .active, 64));

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
    var saw_two = false;
    var saw_three = false;
    for (intents) |intent| {
        if (intent.record.group_id != 4821) continue;
        try std.testing.expect(intent.record.local_node_id != 1);
        if (intent.record.local_node_id == 2) saw_two = true;
        if (intent.record.local_node_id == 3) saw_three = true;
    }
    try std.testing.expect(saw_two);
    try std.testing.expect(saw_three);
}

test "metadata http cluster simulation ignores live stores without available capacity" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-cap-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-cap-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-cap-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-cap-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-cap-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-cap-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4800, root_a, cat_a),
        makeHostSimConfig(2, 4800, root_b, cat_b),
        makeHostSimConfig(3, 4800, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4800, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{
        .store_id = 1,
        .node_id = 1,
        .role = "data",
        .live = true,
        .capacity_bytes = 1024,
        .available_bytes = 0,
    });
    try cluster.node(leader_index).upsertStore(.{
        .store_id = 2,
        .node_id = 2,
        .role = "data",
        .live = true,
        .capacity_bytes = 1024,
        .available_bytes = 800,
    });
    try cluster.node(leader_index).upsertStore(.{
        .store_id = 3,
        .node_id = 3,
        .role = "data",
        .live = true,
        .capacity_bytes = 1024,
        .available_bytes = 600,
    });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 4901,
        .table_id = 49,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    const summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 49,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(0).status(4901));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 4901, .active, 1));

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
    var saw_two = false;
    var saw_three = false;
    for (intents) |intent| {
        if (intent.record.group_id != 4901) continue;
        if (intent.record.local_node_id == 2) saw_two = true;
        if (intent.record.local_node_id == 3) saw_three = true;
        try std.testing.expect(intent.record.local_node_id != 1);
    }
    try std.testing.expect(saw_two);
    try std.testing.expect(saw_three);
}

test "metadata http cluster simulation rebalances after store capacity churn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-rebalance-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-rebalance-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-rebalance-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-rebalance-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-rebalance-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-rebalance-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4900, root_a, cat_a),
        makeHostSimConfig(2, 4900, root_b, cat_b),
        makeHostSimConfig(3, 4900, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4900, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 700 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 100 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 5001,
        .table_id = 50,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 50,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5001, .active, 1));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(5001));

    try std.testing.expectEqual(@as(usize, 2), try cluster.node(leader_index).reportStoreStatuses(&.{
        .{
            .store_id = 1,
            .live = true,
            .health_class = "healthy",
            .capacity_bytes = 1024,
            .available_bytes = 0,
        },
        .{
            .store_id = 3,
            .live = true,
            .health_class = "healthy",
            .capacity_bytes = 1024,
            .available_bytes = 850,
        },
    }));
    try cluster.stepAll();

    const summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.placement_removals);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 5001, .absent, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5001, .active, 1));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 5001, .active, 40));
}

test "metadata http cluster simulation survives leader restart after reported store status churn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-status-restart-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-status-restart-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-status-restart-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-status-restart-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-status-restart-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-status-restart-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4950, root_a, cat_a),
        makeHostSimConfig(2, 4950, root_b, cat_b),
        makeHostSimConfig(3, 4950, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4950, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 700 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 100 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 5201,
        .table_id = 52,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 52,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5201, .active, 1));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(5201));

    try cluster.node(leader_index).reportStoreStatus(.{
        .store_id = 1,
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 0,
    });
    try cluster.node(leader_index).reportStoreStatus(.{
        .store_id = 3,
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 850,
    });
    try cluster.stepAll();

    try cluster.restartNode(leader_index);
    const new_leader = (try cluster.waitForMetadataLeader(32)) orelse return error.TestExpectedEqual;
    const summary = try requireLeasedReconcile(cluster.node(new_leader), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.placement_removals);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 5201, .absent, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5201, .active, 1));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 5201, .active, 40));
}

test "metadata http cluster simulation transfers reconcile lease on leader restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-lease-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-lease-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-lease-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-lease-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-lease-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-lease-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4970, root_a, cat_a),
        makeHostSimConfig(2, 4970, root_b, cat_b),
        makeHostSimConfig(3, 4970, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4970, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try cluster.stepAll();

    const leader_status = try cluster.node(leader_index).metadataStatus();
    try std.testing.expect(leader_status.reconcile_lease_enabled);
    try std.testing.expect(leader_status.reconcile_lease_held_by_local);
    try std.testing.expectEqual(cluster.cluster.configs[leader_index].host.http.host.local_node_id, leader_status.reconcile_lease_owner_node_id);

    const follower_index = if (leader_index == 0) @as(usize, 1) else @as(usize, 0);
    const follower_status = try cluster.node(follower_index).metadataStatus();
    try std.testing.expect(!follower_status.reconcile_lease_held_by_local);
    try std.testing.expectEqual(leader_status.reconcile_lease_owner_node_id, follower_status.reconcile_lease_owner_node_id);

    try cluster.restartNode(leader_index);
    const new_leader = (try cluster.waitForMetadataLeader(32)) orelse return error.TestExpectedEqual;
    try cluster.node(new_leader).campaignMetadataGroup();
    rounds = 0;
    while (rounds < 8) : (rounds += 1) try cluster.stepAll();

    const new_leader_status = try cluster.node(new_leader).metadataStatus();
    try std.testing.expect(new_leader_status.reconcile_lease_held_by_local);
    try std.testing.expectEqual(cluster.cluster.configs[new_leader].host.http.host.local_node_id, new_leader_status.reconcile_lease_owner_node_id);
    const restarted_status = try cluster.node(leader_index).metadataStatus();
    try std.testing.expect(!restarted_status.reconcile_lease_held_by_local);
    try std.testing.expectEqual(new_leader_status.reconcile_lease_owner_node_id, restarted_status.reconcile_lease_owner_node_id);
}

test "metadata http cluster simulation skips reconcile work without lease ownership" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-no-lease-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-no-lease-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-no-lease-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-no-lease-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4971, root_a, cat_a),
        makeHostSimConfig(2, 4971, root_b, cat_b),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4971, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try workflow.controlLoop().stateRef().tableManager().upsertTable(.{
        .table_id = 77,
        .name = "docs",
        .desired_replica_count = 1,
        .min_ranges = 1,
    });
    try workflow.controlLoop().stateRef().tableManager().upsertRange(.{
        .group_id = 7701,
        .table_id = 77,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });

    try std.testing.expect((try cluster.node(0).reconcileOnceIfLeaseHeld(workflow.controlLoop())) == null);
    try std.testing.expect((try cluster.node(1).reconcileOnceIfLeaseHeld(workflow.controlLoop())) == null);

    const status = try cluster.node(0).metadataStatus();
    try std.testing.expect(status.reconcile_lease_enabled);
    try std.testing.expect(!status.reconcile_lease_held_by_local);
    try std.testing.expectEqual(@as(u64, 0), status.reconcile_lease_owner_node_id);

    const projected_tables = try cluster.node(0).listProjectedTables(std.testing.allocator);
    defer cluster.node(0).freeProjectedTables(std.testing.allocator, projected_tables);
    try std.testing.expectEqual(@as(usize, 0), projected_tables.len);
}

test "metadata http cluster simulation rebalances away from high lease pressure" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pressure-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pressure-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pressure-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pressure-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pressure-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pressure-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 4960, root_a, cat_a),
        makeHostSimConfig(2, 4960, root_b, cat_b),
        makeHostSimConfig(3, 4960, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 4960, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 950, .lease_pressure = 5, .read_load = 10, .write_load = 8 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900, .lease_pressure = 8, .read_load = 12, .write_load = 8 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880, .lease_pressure = 12, .read_load = 18, .write_load = 10 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 5301,
        .table_id = 53,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 53,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);

    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5301, .active, 1));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(5301));

    try std.testing.expectEqual(@as(usize, 2), try cluster.node(leader_index).reportStoreStatuses(&.{
        .{ .store_id = 1, .live = true, .health_class = "healthy", .capacity_bytes = 1024, .available_bytes = 950, .lease_pressure = 96, .read_load = 200, .write_load = 140 },
        .{ .store_id = 3, .live = true, .health_class = "healthy", .capacity_bytes = 1024, .available_bytes = 880, .lease_pressure = 12, .read_load = 18, .write_load = 10 },
    }));
    try cluster.stepAll();

    const summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.placement_removals);
    try std.testing.expectEqual(@as(usize, 0), summary.repair_placement_groups);
    try std.testing.expectEqual(@as(usize, 1), summary.rebalance_placement_groups);
    const status = try cluster.node(leader_index).metadataStatus();
    try std.testing.expectEqual(@as(usize, 0), status.rebalance_placement_groups);
    try std.testing.expectEqual(@as(usize, 0), status.repair_placement_groups);
    try std.testing.expectEqual(@as(usize, 1), status.overloaded_stores);
    var admin_snapshot = try cluster.node(leader_index).adminSnapshot();
    defer cluster.node(leader_index).freeAdminSnapshot(&admin_snapshot);
    try std.testing.expectEqual(status.rebalance_placement_groups, admin_snapshot.status.rebalance_placement_groups);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 5301, .absent, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5301, .active, 1));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 5301, .active, 40));
}

test "metadata http cluster simulation repairs replica count after store recovery" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-repair-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-repair-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-repair-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-repair-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-repair-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-repair-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5000, root_a, cat_a),
        makeHostSimConfig(2, 5000, root_b, cat_b),
        makeHostSimConfig(3, 5000, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5000, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 700 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 5101,
        .table_id = 51,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 51,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);
    try std.testing.expectEqual(@as(usize, 2), create_summary.placement_upserts);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5101, .active, 1));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(5101));

    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 800 });
    try cluster.stepAll();

    const repair_summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 3), repair_summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 0), repair_summary.placement_removals);
    try std.testing.expectEqual(@as(usize, 1), repair_summary.repair_placement_groups);
    try std.testing.expectEqual(@as(usize, 0), repair_summary.rebalance_placement_groups);
    const repair_status = try cluster.node(leader_index).metadataStatus();
    try std.testing.expectEqual(@as(usize, 0), repair_status.repair_placement_groups);
    try std.testing.expectEqual(@as(usize, 0), repair_status.rebalance_placement_groups);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 5101, .active, 40));

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
    var active_count: usize = 0;
    for (intents) |intent| {
        if (intent.record.group_id == 5101) active_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), active_count);
}

test "metadata http cluster simulation spreads multi-range placement across stores" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-spread-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-spread-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-spread-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-spread-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-spread-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-spread-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5100, root_a, cat_a),
        makeHostSimConfig(2, 5100, root_b, cat_b),
        makeHostSimConfig(3, 5100, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5100, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 800 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 5201,
            .table_id = 52,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 5202,
            .table_id = 52,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    };
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 52,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 2,
    }, initial_ranges[0..], 32);

    _ = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try cluster.stepAll();

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
    var counts = [_]usize{ 0, 0, 0 };
    var group_5201: usize = 0;
    var group_5202: usize = 0;
    for (intents) |intent| {
        if (intent.record.group_id != 5201 and intent.record.group_id != 5202) continue;
        if (intent.record.group_id == 5201) group_5201 += 1;
        if (intent.record.group_id == 5202) group_5202 += 1;
        if (intent.record.local_node_id >= 1 and intent.record.local_node_id <= 3) {
            counts[intent.record.local_node_id - 1] += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 2), group_5201);
    try std.testing.expectEqual(@as(usize, 2), group_5202);
    try std.testing.expect(counts[0] > 0);
    try std.testing.expect(counts[1] > 0);
    try std.testing.expect(counts[2] > 0);
}

test "metadata http cluster simulation preserves valid placement when a better store appears" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-sticky-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-sticky-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-sticky-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-sticky-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-sticky-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-sticky-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5200, root_a, cat_a),
        makeHostSimConfig(2, 5200, root_b, cat_b),
        makeHostSimConfig(3, 5200, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5200, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 700 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const initial_ranges = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 5301,
        .table_id = 53,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }};
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 53,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, initial_ranges[0..], 32);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5301, .active, 1));

    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try cluster.stepAll();

    const summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 0), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 0), summary.placement_removals);
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.active, cluster.node(0).status(5301));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.active, cluster.node(1).status(5301));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(5301));
}

test "metadata http cluster simulation rotates replica pairs across tables and ranges" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pairs-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pairs-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pairs-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pairs-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pairs-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-pairs-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5300, root_a, cat_a),
        makeHostSimConfig(2, 5300, root_b, cat_b),
        makeHostSimConfig(3, 5300, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5300, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 54,
        .name = "docs_a",
        .desired_replica_count = 2,
        .min_ranges = 2,
    }, &.{
        .{
            .group_id = 5401,
            .table_id = 54,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 5402,
            .table_id = 54,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
    }, 32);
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 55,
        .name = "docs_b",
        .desired_replica_count = 2,
        .min_ranges = 2,
    }, &.{
        .{
            .group_id = 5501,
            .table_id = 55,
            .start_key = "item:a",
            .end_key = "item:m",
        },
        .{
            .group_id = 5502,
            .table_id = 55,
            .start_key = "item:m",
            .end_key = "item:z",
        },
    }, 32);

    try cluster.stepAll();

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);

    var pairs = std.AutoHashMapUnmanaged(u128, usize).empty;
    defer pairs.deinit(std.testing.allocator);
    for ([_]u64{ 5401, 5402, 5501, 5502 }) |group_id| {
        var peers = std.ArrayListUnmanaged(u64).empty;
        defer peers.deinit(std.testing.allocator);
        for (intents) |intent| {
            if (intent.record.group_id != group_id) continue;
            try peers.append(std.testing.allocator, intent.record.local_node_id);
        }
        try std.testing.expectEqual(@as(usize, 2), peers.items.len);
        const lo = @min(peers.items[0], peers.items[1]);
        const hi = @max(peers.items[0], peers.items[1]);
        const entry = try pairs.getOrPut(std.testing.allocator, (@as(u128, hi) << 64) | @as(u128, lo));
        if (!entry.found_existing) entry.value_ptr.* = 0;
        entry.value_ptr.* += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), pairs.count());
    var it = pairs.iterator();
    while (it.next()) |entry| {
        try std.testing.expect(entry.value_ptr.* <= 2);
    }
}

test "metadata http cluster simulation rebalances one table while preserving another valid placement" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-rebalance-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-rebalance-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-rebalance-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-rebalance-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-rebalance-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-rebalance-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5400, root_a, cat_a),
        makeHostSimConfig(2, 5400, root_b, cat_b),
        makeHostSimConfig(3, 5400, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5400, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 800 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 56,
        .name = "docs_a",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 5601,
        .table_id = 56,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }}, 32);
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 57,
        .name = "docs_b",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 5702,
        .table_id = 57,
        .start_key = "item:a",
        .end_key = "item:z",
    }}, 32);

    const intents_before = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents_before);
    var before_5702 = std.ArrayListUnmanaged(u64).empty;
    defer before_5702.deinit(std.testing.allocator);
    for (intents_before) |intent| {
        if (intent.record.group_id == 5702) try before_5702.append(std.testing.allocator, intent.record.local_node_id);
    }
    try std.testing.expectEqual(@as(usize, 2), before_5702.items.len);

    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 0 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try cluster.stepAll();

    const summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.placement_removals);

    const intents_after = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents_after);
    var after_5601 = std.ArrayListUnmanaged(u64).empty;
    defer after_5601.deinit(std.testing.allocator);
    var after_5702 = std.ArrayListUnmanaged(u64).empty;
    defer after_5702.deinit(std.testing.allocator);
    for (intents_after) |intent| {
        if (intent.record.group_id == 5601) try after_5601.append(std.testing.allocator, intent.record.local_node_id);
        if (intent.record.group_id == 5702) try after_5702.append(std.testing.allocator, intent.record.local_node_id);
    }
    try std.testing.expectEqual(@as(usize, 2), after_5601.items.len);
    try std.testing.expectEqual(@as(usize, 2), after_5702.items.len);
    try std.testing.expect(!std.mem.containsAtLeast(u64, after_5601.items, 1, &.{2}));
    std.mem.sort(u64, before_5702.items, {}, std.sort.asc(u64));
    std.mem.sort(u64, after_5702.items, {}, std.sort.asc(u64));
    try std.testing.expectEqualSlices(u64, before_5702.items, after_5702.items);

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
    var saw_5601_1 = false;
    var saw_5601_3 = false;
    var saw_5702_1 = false;
    var saw_5702_3 = false;
    for (intents) |intent| {
        switch (intent.record.group_id) {
            5601 => {
                if (intent.record.local_node_id == 1) saw_5601_1 = true;
                if (intent.record.local_node_id == 3) saw_5601_3 = true;
                try std.testing.expect(intent.record.local_node_id != 2);
            },
            5702 => {
                if (intent.record.local_node_id == 1) saw_5702_1 = true;
                if (intent.record.local_node_id == 3) saw_5702_3 = true;
                try std.testing.expect(intent.record.local_node_id != 2);
            },
            else => {},
        }
    }
    try std.testing.expect(saw_5601_1);
    try std.testing.expect(saw_5601_3);
    try std.testing.expect(saw_5702_1);
    try std.testing.expect(saw_5702_3);
}

test "metadata http cluster simulation prefers healthy stores before degraded ones" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-health-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-health-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-health-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-health-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-health-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-health-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5500, root_a, cat_a),
        makeHostSimConfig(2, 5500, root_b, cat_b),
        makeHostSimConfig(3, 5500, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5500, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .health_class = "degraded", .live = true, .capacity_bytes = 1024, .available_bytes = 980 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .health_class = "healthy", .live = true, .capacity_bytes = 1024, .available_bytes = 700 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .health_class = "healthy", .live = true, .capacity_bytes = 1024, .available_bytes = 650 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 58,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 5801,
        .table_id = 58,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }}, 32);

    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(0).status(5801));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 5801, .active, 1));

    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .health_class = "healthy", .live = true, .capacity_bytes = 1024, .available_bytes = 0 });
    try cluster.stepAll();

    const summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.placement_removals);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 5801, .active, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 5801, .absent, 1));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 5801, .active, 1));
}

test "metadata http cluster simulation prefers cross-domain placement for a range" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-domain-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-domain-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-domain-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-domain-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-domain-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-domain-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5600, root_a, cat_a),
        makeHostSimConfig(2, 5600, root_b, cat_b),
        makeHostSimConfig(3, 5600, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5600, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 600 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 59,
        .name = "docs",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 5901,
        .table_id = 59,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }}, 32);

    try std.testing.expect(try cluster.waitForNodeGroupStatus(2, 5901, .active, 32));
    const other_on_a = try cluster.waitForNodeGroupStatus(0, 5901, .active, 1) or try cluster.waitForNodeGroupStatus(1, 5901, .active, 1);
    try std.testing.expect(other_on_a);

    const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
    var saw_three = false;
    var saw_one_or_two = false;
    for (intents) |intent| {
        if (intent.record.group_id != 5901) continue;
        if (intent.record.local_node_id == 3) saw_three = true;
        if (intent.record.local_node_id == 1 or intent.record.local_node_id == 2) saw_one_or_two = true;
    }
    try std.testing.expect(saw_three);
    try std.testing.expect(saw_one_or_two);
}

test "metadata http cluster simulation mixes health domain and minimal-movement policy" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-policy-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-policy-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-policy-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-policy-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-policy-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-mixed-policy-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5700, root_a, cat_a),
        makeHostSimConfig(2, 5700, root_b, cat_b),
        makeHostSimConfig(3, 5700, root_c, cat_c),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5700, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "data", .health_class = "healthy", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .health_class = "healthy", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "data", .health_class = "healthy", .failure_domain = "rack-c", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 60,
        .name = "docs_a",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 6001,
        .table_id = 60,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }}, 32);
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 61,
        .name = "docs_b",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 6101,
        .table_id = 61,
        .start_key = "item:a",
        .end_key = "item:z",
    }}, 32);

    const intents_before = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents_before);
    var before_6001 = std.ArrayListUnmanaged(u64).empty;
    defer before_6001.deinit(std.testing.allocator);
    var before_6101 = std.ArrayListUnmanaged(u64).empty;
    defer before_6101.deinit(std.testing.allocator);
    for (intents_before) |intent| {
        if (intent.record.group_id == 6001) try before_6001.append(std.testing.allocator, intent.record.local_node_id);
        if (intent.record.group_id == 6101) try before_6101.append(std.testing.allocator, intent.record.local_node_id);
    }
    try std.testing.expectEqual(@as(usize, 2), before_6001.items.len);
    try std.testing.expectEqual(@as(usize, 2), before_6101.items.len);

    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "data", .health_class = "degraded", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 0 });
    try cluster.stepAll();

    const summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.placement_removals);

    const intents_after = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
    defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents_after);
    var after_6001 = std.ArrayListUnmanaged(u64).empty;
    defer after_6001.deinit(std.testing.allocator);
    var after_6101 = std.ArrayListUnmanaged(u64).empty;
    defer after_6101.deinit(std.testing.allocator);
    for (intents_after) |intent| {
        if (intent.record.group_id == 6001) try after_6001.append(std.testing.allocator, intent.record.local_node_id);
        if (intent.record.group_id == 6101) try after_6101.append(std.testing.allocator, intent.record.local_node_id);
    }
    try std.testing.expectEqual(@as(usize, 2), after_6001.items.len);
    try std.testing.expectEqual(@as(usize, 2), after_6101.items.len);
    try std.testing.expect(!std.mem.containsAtLeast(u64, after_6001.items, 1, &.{2}));
    std.mem.sort(u64, before_6101.items, {}, std.sort.asc(u64));
    std.mem.sort(u64, after_6101.items, {}, std.sort.asc(u64));
    try std.testing.expectEqualSlices(u64, before_6101.items, after_6101.items);
}

test "metadata http cluster simulation respects table placement roles under churn" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();
    var store_e = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_e.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4, 5 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4, 5 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4, 5 } };
    var factory_d = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4, 5 } };
    var factory_e = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_e, .peers = &.{ 1, 2, 3, 4, 5 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const root_e = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-e", .{tmp.sub_path});
    defer std.testing.allocator.free(root_e);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);
    const cat_e = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-e.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_e);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5800, root_a, cat_a),
        makeHostSimConfig(2, 5800, root_b, cat_b),
        makeHostSimConfig(3, 5800, root_c, cat_c),
        makeHostSimConfig(4, 5800, root_d, cat_d),
        makeHostSimConfig(5, 5800, root_e, cat_e),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
        makeHostSimDeps(&factory_e),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5800, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "hot", .health_class = "healthy", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "hot", .health_class = "healthy", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "cold", .health_class = "healthy", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 920 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 4, .node_id = 4, .role = "cold", .health_class = "healthy", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 880 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 5, .node_id = 5, .role = "hot", .health_class = "healthy", .failure_domain = "rack-c", .live = true, .capacity_bytes = 1024, .available_bytes = 300 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 62,
        .name = "hot_docs",
        .placement_role = "hot",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 6201,
        .table_id = 62,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }}, 32);
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 63,
        .name = "cold_docs",
        .placement_role = "cold",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 6301,
        .table_id = 63,
        .start_key = "item:a",
        .end_key = "item:z",
    }}, 32);

    try cluster.stepAll();
    {
        const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
        defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
        var hot_count: usize = 0;
        var cold_count: usize = 0;
        var saw_hot_3 = false;
        var saw_hot_4 = false;
        var saw_cold_3 = false;
        var saw_cold_4 = false;
        var saw_cold_5 = false;
        for (intents) |intent| {
            if (intent.record.group_id == 6201) {
                hot_count += 1;
                if (intent.record.local_node_id == 3) saw_hot_3 = true;
                if (intent.record.local_node_id == 4) saw_hot_4 = true;
            }
            if (intent.record.group_id == 6301) {
                cold_count += 1;
                if (intent.record.local_node_id == 3) saw_cold_3 = true;
                if (intent.record.local_node_id == 4) saw_cold_4 = true;
                if (intent.record.local_node_id == 5) saw_cold_5 = true;
            }
        }
        try std.testing.expectEqual(@as(usize, 2), hot_count);
        try std.testing.expect(!saw_hot_3);
        try std.testing.expect(!saw_hot_4);
        try std.testing.expectEqual(@as(usize, 2), cold_count);
        try std.testing.expect(saw_cold_3);
        try std.testing.expect(saw_cold_4);
        try std.testing.expect(!saw_cold_5);
    }

    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "hot", .health_class = "degraded", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 0 });
    try cluster.stepAll();

    const summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.placement_removals);
    {
        const intents = try cluster.node(leader_index).listProjectedPlacementIntents(std.testing.allocator);
        defer cluster.node(leader_index).freeProjectedPlacementIntents(std.testing.allocator, intents);
        var saw_hot_1 = false;
        var saw_hot_2 = false;
        var saw_hot_5 = false;
        var saw_cold_3 = false;
        var saw_cold_4 = false;
        var saw_cold_5 = false;
        for (intents) |intent| {
            if (intent.record.group_id == 6201) {
                if (intent.record.local_node_id == 1) saw_hot_1 = true;
                if (intent.record.local_node_id == 2) saw_hot_2 = true;
                if (intent.record.local_node_id == 5) saw_hot_5 = true;
            }
            if (intent.record.group_id == 6301) {
                if (intent.record.local_node_id == 3) saw_cold_3 = true;
                if (intent.record.local_node_id == 4) saw_cold_4 = true;
                if (intent.record.local_node_id == 5) saw_cold_5 = true;
            }
        }
        try std.testing.expect(saw_hot_1);
        try std.testing.expect(!saw_hot_2);
        try std.testing.expect(saw_hot_5);
        try std.testing.expect(saw_cold_3);
        try std.testing.expect(saw_cold_4);
        try std.testing.expect(!saw_cold_5);
    }
}

test "metadata http cluster simulation repairs only when a matching placement role appears" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-repair-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-repair-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-repair-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-repair-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-repair-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-repair-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-repair-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-role-repair-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 5900, root_a, cat_a),
        makeHostSimConfig(2, 5900, root_b, cat_b),
        makeHostSimConfig(3, 5900, root_c, cat_c),
        makeHostSimConfig(4, 5900, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 5900, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "hot", .health_class = "healthy", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "cold", .health_class = "healthy", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "cold", .health_class = "healthy", .failure_domain = "rack-c", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    const create_summary = try createActiveTableRangesWithSummary(&workflow, &cluster, leader_index, .{
        .table_id = 64,
        .name = "hot_docs",
        .placement_role = "hot",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 6401,
        .table_id = 64,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }}, 40);

    try std.testing.expectEqual(@as(usize, 1), create_summary.placement_upserts);
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(1).status(6401));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(6401));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(3).status(6401));

    try cluster.node(leader_index).upsertStore(.{ .store_id = 4, .node_id = 4, .role = "hot", .health_class = "healthy", .failure_domain = "rack-d", .live = true, .capacity_bytes = 1024, .available_bytes = 800 });
    try cluster.stepAll();

    const repair_summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), repair_summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 0), repair_summary.placement_removals);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(3, 6401, .active, 40));
}

test "metadata http cluster simulation rebalances after store class promotion and demotion" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var store_c = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_c.deinit();
    var store_d = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_d.deinit();

    var factory_a = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_a, .peers = &.{ 1, 2, 3, 4 } };
    var factory_b = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_b, .peers = &.{ 1, 2, 3, 4 } };
    var factory_c = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_c, .peers = &.{ 1, 2, 3, 4 } };
    var factory_d = TestDescriptorFactory{ .alloc = std.testing.allocator, .store = &store_d, .peers = &.{ 1, 2, 3, 4 } };

    const root_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-class-a", .{tmp.sub_path});
    defer std.testing.allocator.free(root_a);
    const root_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-class-b", .{tmp.sub_path});
    defer std.testing.allocator.free(root_b);
    const root_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-class-c", .{tmp.sub_path});
    defer std.testing.allocator.free(root_c);
    const root_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-class-d", .{tmp.sub_path});
    defer std.testing.allocator.free(root_d);
    const cat_a = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-class-a.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_a);
    const cat_b = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-class-b.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_b);
    const cat_c = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-class-c.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_c);
    const cat_d = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sim-class-d.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(cat_d);

    const configs = [_]raft_sim.ManagedHttpHostSimulationConfig{
        makeHostSimConfig(1, 6000, root_a, cat_a),
        makeHostSimConfig(2, 6000, root_b, cat_b),
        makeHostSimConfig(3, 6000, root_c, cat_c),
        makeHostSimConfig(4, 6000, root_d, cat_d),
    };
    const deps = [_]raft_sim.ManagedHttpHostSimulationDeps{
        makeHostSimDeps(&factory_a),
        makeHostSimDeps(&factory_b),
        makeHostSimDeps(&factory_c),
        makeHostSimDeps(&factory_d),
    };

    var cluster = try MetadataHttpClusterSimulation.init(std.testing.allocator, 6000, configs[0..], deps[0..]);
    defer cluster.deinit();
    try cluster.startAll();
    defer cluster.stopAll();
    try cluster.bootstrapMetadataReplicas();
    const leader_index = (try cluster.waitForMetadataLeader(24)) orelse return error.TestExpectedEqual;
    try cluster.node(leader_index).campaignMetadataGroup();
    try cluster.stepAll();

    try cluster.node(leader_index).upsertStore(.{ .store_id = 1, .node_id = 1, .role = "serving", .health_class = "healthy", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "bulk", .health_class = "healthy", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 3, .node_id = 3, .role = "archive", .health_class = "healthy", .failure_domain = "rack-c", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 4, .node_id = 4, .role = "serving", .health_class = "healthy", .failure_domain = "rack-d", .live = true, .capacity_bytes = 1024, .available_bytes = 800 });
    try cluster.stepAll();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try createActiveTableRanges(&workflow, &cluster, leader_index, .{
        .table_id = 65,
        .name = "serving_docs",
        .placement_role = "serving",
        .desired_replica_count = 2,
        .min_ranges = 1,
    }, &.{.{
        .group_id = 6501,
        .table_id = 65,
        .start_key = "doc:a",
        .end_key = "doc:z",
    }}, 40);

    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(1).status(6501));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(6501));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(3, 6501, .active, 1));

    try cluster.node(leader_index).upsertStore(.{ .store_id = 2, .node_id = 2, .role = "serving", .health_class = "healthy", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try cluster.node(leader_index).upsertStore(.{ .store_id = 4, .node_id = 4, .role = "bulk", .health_class = "healthy", .failure_domain = "rack-d", .live = true, .capacity_bytes = 1024, .available_bytes = 800 });
    try cluster.stepAll();

    const summary = try requireLeasedReconcile(cluster.node(leader_index), workflow.controlLoop());
    try std.testing.expectEqual(@as(usize, 2), summary.placement_upserts);
    try std.testing.expectEqual(@as(usize, 1), summary.placement_removals);
    try std.testing.expect(try cluster.waitForNodeGroupStatus(0, 6501, .active, 40));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(1, 6501, .active, 1));
    try std.testing.expectEqual(raft_host.HostedReplicaStatus.absent, cluster.node(2).status(6501));
    try std.testing.expect(try cluster.waitForNodeGroupStatus(3, 6501, .absent, 40));
}
