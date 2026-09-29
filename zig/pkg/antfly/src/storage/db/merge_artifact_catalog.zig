// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
//! Attempt-bound source layouts. The receiver installs these once, atomically
//! with its page receipt, rather than consulting a live donor during apply.
const std = @import("std");
const inventory = @import("artifact_inventory.zig");
const pages = @import("merge_page_contract.zig");
pub const key = "\x00\x00__metadata__:raftmerge:artifact_source_catalog";
pub const Record = struct {
    transition_id: u64,
    donor_group_id: u64,
    receiver_group_id: u64,
    receiver_namespace: @import("doc_identity_namespace.zig").Namespace,
    attempt: @import("relational_integrity_handoff_contract.zig").MergeCopyAttempt,
    source: pages.Source,
    catalogs: inventory.Catalogs,

    pub fn matches(self: Record, progress: pages.Progress) bool {
        return self.transition_id == progress.transition_id and self.donor_group_id == progress.donor_group_id and
            self.receiver_group_id == progress.receiver_group_id and self.receiver_namespace.eql(progress.receiver_namespace) and
            self.attempt.order(progress.attempt) == .eq and self.source.eql(progress.source);
    }
};

/// Owned, immutable generation map reused for the entire copy attempt. The
/// caller serializes preparation with a dedicated mutex; catalog drift creates a
/// new plan rather than mutating names/generations held by a previous page.
pub const Cache = struct {
    identity: ?Record = null,
    receiver: ?inventory.Binding = null,
    plan: ?@import("online_graph_artifacts.zig").Plan = null,

    pub fn clear(self: *Cache) void {
        if (self.plan) |*plan| plan.deinit();
        self.* = .{};
    }

    pub fn get(self: *Cache, alloc: std.mem.Allocator, txn: anytype, progress: pages.Progress, receiver: inventory.Binding, catalogs: inventory.Catalogs) !*const @import("online_graph_artifacts.zig").Plan {
        if (self.identity) |identity| if (identity.matches(progress) and std.meta.eql(self.receiver, @as(?inventory.Binding, receiver))) return &self.plan.?;
        var source_record = try load(alloc, txn, progress);
        defer source_record.deinit();
        var prepared = try @import("online_graph_artifacts.zig").Plan.init(alloc, source_record.value.catalogs, progress.source.artifact_catalog orelse return error.ArtifactCatalogDrift, catalogs, receiver);
        errdefer prepared.deinit();
        self.clear();
        self.identity = source_record.value;
        // Names are owned by Plan; identity retains only fixed-size fields.
        self.identity.?.catalogs = .{};
        self.receiver = receiver;
        self.plan = prepared;
        return &self.plan.?;
    }
};

pub fn validateCheckpoint(checkpoint: anytype) !void {
    const catalogs = checkpoint.page_source_catalogs orelse {
        if (checkpoint.page_source) |source| if (source.artifact_catalog) |binding|
            if (binding.effect_protocol == 15) return error.InvalidMergeCheckpoint;
        return;
    };
    if (checkpoint.kind != .accept and checkpoint.kind != .begin_copy) return error.InvalidMergeCheckpoint;
    const source = checkpoint.page_source orelse return error.InvalidMergeCheckpoint;
    const binding = source.artifact_catalog orelse return error.InvalidMergeCheckpoint;
    if (!binding.valid() or binding.effect_protocol != 15 or checkpoint.page_receiver_namespace == null or
        catalogs.indexes.len > 1024 * 1024 or catalogs.enrichments.len > 1024 * 1024 or catalogs.resolvers.len > 1024 * 1024 or
        catalogs.indexes.len + catalogs.enrichments.len + catalogs.resolvers.len > 1024 * 1024 or
        !std.mem.eql(u8, &catalogs.digest(), &binding.digest)) return error.InvalidMergeCheckpoint;
    const semantic = catalogs.semanticDigest(std.heap.page_allocator) catch return error.InvalidMergeCheckpoint;
    if (!std.mem.eql(u8, &semantic, &binding.semantic_digest)) return error.InvalidMergeCheckpoint;
}

pub fn encode(alloc: std.mem.Allocator, checkpoint: anytype, progress: pages.Progress) !?[]u8 {
    try validateCheckpoint(checkpoint);
    const catalogs = checkpoint.page_source_catalogs orelse return null;
    const record: Record = .{ .transition_id = progress.transition_id, .donor_group_id = progress.donor_group_id, .receiver_group_id = progress.receiver_group_id, .receiver_namespace = progress.receiver_namespace, .attempt = progress.attempt, .source = progress.source, .catalogs = catalogs };
    return try std.json.Stringify.valueAlloc(alloc, record, .{ .emit_strings_as_arrays = true });
}

pub fn load(alloc: std.mem.Allocator, txn: anytype, progress: pages.Progress) !std.json.Parsed(Record) {
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return error.ArtifactCatalogDrift,
        else => return err,
    };
    if (raw.len > 5 * 1024 * 1024) return error.ArtifactCatalogCorrupt;
    var parsed = std.json.parseFromSlice(Record, alloc, raw, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.ArtifactCatalogCorrupt,
    };
    errdefer parsed.deinit();
    const binding = progress.source.artifact_catalog orelse return error.ArtifactCatalogDrift;
    if (!parsed.value.matches(progress) or !std.mem.eql(u8, &parsed.value.catalogs.digest(), &binding.digest)) return error.ArtifactCatalogDrift;
    return parsed;
}

test "ordered artifact inventory source layout cache is owned and copy-attempt bound" {
    const alloc = std.testing.allocator;
    const catalogs: inventory.Catalogs = .{ .indexes = "AIDX\x02\x00\x00\x00\x00\x00\x00\x00", .enrichments = "[]", .resolvers = "[]" };
    const binding: inventory.Binding = .{ .epoch = 1, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(alloc), .effect_protocol = 15 };
    const source: pages.Source = .{ .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .pin_digest = @splat(1), .applied_index = 7, .retention = .{ .epoch = 1, .after_sequence = 0 }, .artifact_catalog = binding };
    const progress: pages.Progress = .{ .transition_id = 4, .donor_group_id = 2, .receiver_group_id = 3, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .attempt = .{ .donor_term = 2, .sequence = 1 }, .source = source };
    const checkpoint = .{ .kind = @as(enum { begin_copy, accept }, .begin_copy), .page_source = @as(?pages.Source, source), .page_receiver_namespace = @as(?@import("doc_identity_namespace.zig").Namespace, progress.receiver_namespace), .page_source_catalogs = @as(?inventory.Catalogs, catalogs) };
    const raw = (try encode(alloc, checkpoint, progress)).?;
    defer alloc.free(raw);
    const Txn = struct {
        raw: []const u8,
        reads: usize = 0,
        fn get(self: *@This(), target: []const u8) anyerror![]const u8 {
            if (!std.mem.eql(u8, target, key)) return error.NotFound;
            self.reads += 1;
            return self.raw;
        }
    };
    var txn: Txn = .{ .raw = raw };
    var cache: Cache = .{};
    defer cache.clear();
    _ = try cache.get(alloc, &txn, progress, binding, catalogs);
    _ = try cache.get(alloc, &txn, progress, binding, catalogs);
    try std.testing.expectEqual(@as(usize, 1), txn.reads);
    var next = progress;
    next.attempt.sequence += 1;
    try std.testing.expectError(error.ArtifactCatalogDrift, cache.get(alloc, &txn, next, binding, catalogs));
    // A failed new-attempt lookup leaves the old immutable map intact.
    _ = try cache.get(alloc, &txn, progress, binding, catalogs);
    try std.testing.expectEqual(@as(usize, 2), txn.reads);
}
