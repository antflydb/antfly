// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral operations for internal group coordination.

const std = @import("std");
const db_mod = @import("../storage/db/mod.zig");
const metadata_mod = @import("../metadata/domain.zig");
const operation = @import("operation.zig");
const raft_mod = @import("../raft/mod.zig");
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_write_source.zig");

pub const Error = operation.ApiError || error{
    TopologyChanged,
    IdentityReadGenerationChanged,
    DocIdentityNamespaceMismatch,
    StorageReadTemporarilyUnavailable,
    GroupLeaderUnavailable,
};

pub const LookupInput = struct {
    group_id: u64,
    table_name: []const u8,
    key: []const u8,
    options: db_mod.types.LookupOptions = .{},
};

pub const Operations = struct {
    reads: ?table_reads.TableReadSource,
    shard_db_adapter: ?metadata_mod.ShardDbAdapter,
    writes: ?table_writes.TableWriteSource = null,
    shard_ops: ?raft_mod.ShardOperationAdapter = null,

    pub fn corruptEmbeddingArtifact(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) Error!void {
        try request.ensureActive();
        const writes = self.writes orelse return error.NotFound;
        _ = (writes.corruptEmbeddingArtifact(alloc, table_name, doc_key, index_name) catch |err| switch (err) {
            error.NotFound => return error.NotFound,
            else => return error.Internal,
        }) orelse return error.NotFound;
    }

    pub fn observeSplit(
        self: Operations,
        request: operation.RequestContext,
        group_id: u64,
        record: @import("../metadata/transition_state.zig").SplitTransitionRecord,
    ) Error!@import("../metadata/transition_state.zig").SplitObservation {
        try request.ensureActive();
        const ops = self.shard_ops orelse return error.NotFound;
        if (group_id != record.source_group_id and group_id != record.destination_group_id)
            return error.InvalidArgument;
        var observation = ops.observeSplit(record) catch |err| switch (err) {
            error.UnknownGroup, error.UnknownSplitRuntime, error.MissingSplitRuntime => return error.NotFound,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.LeaderUnavailable,
            error.GroupLeaderUnavailable,
            error.SplitSourceProjectionNotReady,
            error.DurableRootIncarnationUnavailable,
            error.AutoBulkIngestBusy,
            error.ApplyStoreGroupRetired,
            error.ApplyStoreShuttingDown,
            => return error.GroupLeaderUnavailable,
            else => return error.Internal,
        };
        if (group_id == record.source_group_id) observation.source_local_leader = true;
        if (group_id == record.destination_group_id) observation.destination_local_leader = true;
        return observation;
    }

    pub fn observeMerge(
        self: Operations,
        request: operation.RequestContext,
        group_id: u64,
        record: @import("../metadata/transition_state.zig").MergeTransitionRecord,
    ) Error!@import("../metadata/transition_state.zig").MergeObservation {
        try request.ensureActive();
        const ops = self.shard_ops orelse return error.NotFound;
        if (group_id != record.donor_group_id and group_id != record.receiver_group_id)
            return error.InvalidArgument;
        var observation = ops.observeMerge(record) catch |err| switch (err) {
            error.UnknownGroup, error.UnknownMergeRuntime, error.MissingMergeRuntime => return error.NotFound,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.LeaderUnavailable, error.GroupLeaderUnavailable => return error.GroupLeaderUnavailable,
            else => return error.Internal,
        };
        if (group_id == record.donor_group_id) observation.donor_local_leader = true;
        if (group_id == record.receiver_group_id) observation.receiver_local_leader = true;
        return observation;
    }

    pub fn executeTransition(
        self: Operations,
        request: operation.RequestContext,
        group_id: u64,
        action: @import("../metadata/domain.zig").TransitionAction,
    ) Error!void {
        try request.ensureActive();
        const ops = self.shard_ops orelse return error.NotFound;
        if (!transitionActionMatchesGroup(action, group_id)) return error.InvalidArgument;
        ops.execute(action) catch |err| switch (err) {
            error.UnknownGroup,
            error.UnknownSplitRuntime,
            error.UnknownMergeRuntime,
            error.MissingSplitRuntime,
            error.MissingMergeRuntime,
            => return error.NotFound,
            error.TopologyChanged => return error.TopologyChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.UnsupportedOperation => return error.Unsupported,
            error.LeaderUnavailable,
            error.GroupLeaderUnavailable,
            error.MetadataSnapshotUnavailable,
            error.SplitSourceProjectionNotReady,
            error.SplitSourceProjectionAdvanced,
            error.DurableRootIncarnationUnavailable,
            error.AutoBulkIngestBusy,
            error.ApplyStoreGroupRetired,
            error.ApplyStoreShuttingDown,
            error.BackgroundOwnerClosing,
            error.BackgroundOwnerClosed,
            error.TransitionOperationBusy,
            => return error.GroupLeaderUnavailable,
            else => return error.Internal,
        };
    }

    fn transitionActionMatchesGroup(action: @import("../metadata/domain.zig").TransitionAction, group_id: u64) bool {
        return switch (action) {
            .none => group_id == 0,
            .prepare_split_source => |op| group_id == op.source_group_id,
            .start_split_source => |op| group_id == op.source_group_id,
            .bootstrap_split_destination => |op| group_id == op.source_group_id or group_id == op.destination_group_id,
            .catch_up_split_destination => |op| group_id == op.source_group_id or group_id == op.destination_group_id,
            .finalize_split_source => |op| group_id == op.source_group_id,
            .rollback_split => |op| group_id == op.source_group_id,
            .accept_merge_receiver => |op| group_id == op.receiver_group_id,
            .catch_up_merge_receiver => |op| group_id == op.receiver_group_id,
            .finalize_merge => |op| group_id == op.receiver_group_id,
            .rollback_merge => |op| group_id == op.receiver_group_id,
        };
    }

    pub fn lookup(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        input: LookupInput,
    ) Error!table_reads.LookupResponse {
        try request.ensureActive();
        const reads = self.reads orelse return error.NotFound;
        const result = reads.lookupGroupLocal(
            alloc,
            input.group_id,
            input.table_name,
            input.key,
            input.options,
            .read_index,
        ) catch |err| switch (err) {
            error.TopologyChanged => return error.TopologyChanged,
            error.IdentityReadGenerationChanged => return error.IdentityReadGenerationChanged,
            error.DocIdentityNamespaceMismatch => return error.DocIdentityNamespaceMismatch,
            error.StorageReadTemporarilyUnavailable => return error.StorageReadTemporarilyUnavailable,
            else => return error.Internal,
        };
        return result orelse error.NotFound;
    }

    /// The returned key, when present, is owned by `alloc`.
    pub fn medianKey(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
    ) Error!?[]u8 {
        try request.ensureActive();
        const adapter = self.shard_db_adapter orelse return error.NotFound;
        return adapter.fetchMedianKey(alloc, group_id) catch |err| switch (err) {
            error.UnknownGroup => error.NotFound,
            error.UnsupportedOperation => error.Unsupported,
            else => error.Internal,
        };
    }
};

test "internal group reads are callable without an HTTP request" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        fn reads() table_reads.TableReadSource {
            return .{ .ptr = undefined, .vtable = &.{
                .lookup = publicLookup,
                .scan = scan,
                .query = query,
                .lookup_group_local = groupLookup,
            } };
        }

        fn shardDb() metadata_mod.ShardDbAdapter {
            return .{ .ptr = undefined, .vtable = &.{
                .fetch_median_key = medianKey,
                .schema_index_ready = schemaIndexReady,
            } };
        }

        fn publicLookup(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db_mod.types.LookupOptions, _: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            return null;
        }

        fn scan(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db_mod.types.ScanOptions, _: raft_mod.ReadConsistency) !?table_reads.ScanResponse {
            return null;
        }

        fn query(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.SearchRequest, _: raft_mod.ReadConsistency) !?@import("query.zig").QueryResponse {
            return null;
        }

        fn groupLookup(_: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8, _: db_mod.types.LookupOptions, consistency: raft_mod.ReadConsistency) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("documents", table_name);
            try std.testing.expectEqualStrings("doc:a", key);
            try std.testing.expectEqual(raft_mod.ReadConsistency.read_index, consistency);
            return .{ .json = try inner_alloc.dupe(u8, "{\"title\":\"alpha\"}"), .version = 42 };
        }

        fn medianKey(_: *anyopaque, inner_alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
            try std.testing.expectEqual(@as(u64, 7), group_id);
            return try inner_alloc.dupe(u8, "doc:m");
        }

        fn schemaIndexReady(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: u64, _: u32, _: u32) !bool {
            return true;
        }
    };

    const operations = Operations{ .reads = Fake.reads(), .shard_db_adapter = Fake.shardDb() };
    var lookup = try operations.lookup(alloc, .{}, .{
        .group_id = 7,
        .table_name = "documents",
        .key = "doc:a",
    });
    defer lookup.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 42), lookup.version);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", lookup.json);

    const median = (try operations.medianKey(alloc, .{}, 7)).?;
    defer alloc.free(median);
    try std.testing.expectEqualStrings("doc:m", median);
    try std.testing.expectError(
        error.NotFound,
        operations.corruptEmbeddingArtifact(alloc, .{}, "documents", "doc:a", "embedding"),
    );
}
