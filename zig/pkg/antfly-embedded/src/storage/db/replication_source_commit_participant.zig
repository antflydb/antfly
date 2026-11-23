// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Actual Source COMMIT marker participation. The accepting transaction owns
//! the prepared source input, real source stamp and terminal row/metadata batch.
//! No observer repairs a missing marker after the primary commit.
const std = @import("std");
const participant = @import("../commit_participant.zig");
const store = @import("replication_source_apply_store.zig");
const source = @import("../source_authority.zig");
const Position = @import("receipt_position.zig").Position;

pub const Prepared = struct {
    alloc: std.mem.Allocator,
    source_prepared: store.Prepared,
    original: ?participant.Participant = null,
    terminal: ?participant.Participant = null,
    staged: bool = false,

    pub fn asParticipant(self: *@This()) participant.Participant {
        return .{ .ptr = self, .stage_control_on_empty = true, .reset = reset, .observe = observe, .stage = stage };
    }
    fn reset(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.staged = false;
        if (self.original) |original| original.reset(original.ptr);
        if (self.terminal) |terminal| terminal.reset(terminal.ptr);
    }
    fn observe(raw: *anyopaque, key: []const u8, value: ?[]const u8) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.original) |original| original.observe(original.ptr, key, value);
    }
    fn stage(raw: *anyopaque, view: participant.View, encoded: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.staged) return error.InvalidCommitMetadata;
        const position = try Position.decode(encoded);
        const expected = self.source_prepared;
        try expected.validate(self.alloc);
        try position.requireNamespace(expected.receipt.permit.namespace);
        const actual = (try source.load(view)) orelse return error.OnlineSourceScopeChanged;
        if (!std.mem.eql(u8, &actual.namespace, &@import("online_source_contract.zig").namespaceBytes(expected.receipt.permit.namespace)) or
            ((position == .native) != (actual.kind == .native)) or
            (position == .native and actual.sequence != position.native.sequence)) return error.ReplicationCutoverAuthorityLost;
        if (self.original) |original| try original.stage(original.ptr, view, encoded);
        _ = try store.stageApplied(self.alloc, view, expected.receipt.permit, expected.receipt.image.physical_root_incarnation, expected.intent_revision, position);
        if (self.terminal) |terminal| try terminal.stage(terminal.ptr, view, encoded);
        self.staged = true;
    }
};

test "Source terminal participant stamps real empty read-guard integrity and row batches once and rolls back atomically" {
    const a = std.testing.allocator;
    const Store = @import("../docstore.zig").DocStore;
    const apply = @import("../../api/replication_source_apply.zig");
    const keys = @import("../internal_keys.zig");
    const reference: @import("../../api/replication_source_reference.zig").Reference = .{
        .owner = .{ .logical_subscription = .{ .database_id = 2, .subscription_id = 3 } },
        .birth = 1,
        .generation = 1,
        .source_digest = @splat(1),
        .event_digest = @splat(2),
        .intent_digest = @splat(3),
        .manifest_digest = @splat(4),
        .txn_id = @splat(5),
    };
    const permit: apply.Permit = .{
        .semantic = .{ .metadata_group = 1, .metadata_incarnation = @splat('1'), .owner = reference.owner, .birth = reference.birth, .source_digest = reference.source_digest, .event_digest = reference.event_digest, .transaction_id = reference.txn_id },
        .reference = reference,
        .decision_owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(6), .admission_digest = @splat(7), .expires_at_unix_ns = 1000 },
        .member = 0,
        .owner_group_id = 9,
        .topology_epoch = 1,
        .namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 12 },
        .schema_version = 0,
        .schema_digest = @splat(8),
        .target_custody_digest = @splat(9),
        .expected_image = null,
        .previous_marker_digest = null,
        .pregrant_body_digest = @splat(10),
        .request_digest = @splat(11),
    };
    const input: store.Preparation = .{ .permit = permit, .physical = .{ .physical_root_incarnation = 123, .namespace = permit.namespace, .schema_version = 0, .schema_digest = permit.schema_digest } };
    const ns = @import("online_source_contract.zig").namespaceBytes(permit.namespace);
    const Failure = struct {
        original: participant.Participant,
        fn reset(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.original.reset(self.original.ptr);
        }
        fn observe(raw: *anyopaque, key: []const u8, value: ?[]const u8) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.original.observe(self.original.ptr, key, value);
        }
        fn stage(raw: *anyopaque, view: participant.View, encoded: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.original.stage(self.original.ptr, view, encoded);
            return error.InjectedSourceCommitFailure;
        }
        fn callback(self: *@This()) participant.Participant {
            return .{ .ptr = self, .stage_control_on_empty = true, .reset = reset, .observe = observe, .stage = stage };
        }
    };
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    for (0..4) |shape| {
        const path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/source-terminal-{d}", .{ temp.sub_path, shape }, 0);
        defer a.free(path);
        var db = try Store.open(a, path, .{});
        defer db.close();
        var arena: std.heap.ArenaAllocator = .init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        var initial: store.Prepared = undefined;
        {
            var txn = try db.beginWriteTxn();
            errdefer txn.abort();
            try source.bind(&txn, .native, ns);
            _ = try @import("replication_source_reader_floor.zig").stage(scratch, &txn, .{});
            if (shape == 3) try @import("artifact_publication.zig").stageAuthority(&txn, .{ .mode = .activate, .namespace = ns, .authority_epoch = 1, .catalog_digest = @splat(20), .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
            const sequence = try source.advance(&txn, ns, null);
            initial = try store.stagePrepared(scratch, &txn, input, .{ .native = .{ .namespace = permit.namespace, .sequence = sequence } }, reference.txn_id, 1, &.{});
            try txn.commit();
        }
        const row = try keys.documentKeyAlloc(a, "doc:a");
        defer a.free(row);
        const change: @import("../docstore.zig").KVPair = .{ .key = switch (shape) {
            0 => "\x00\x00__txn_terminal_fixture__",
            1 => "\x00\x00__txn_read_guard_fixture__",
            2 => "\x00\x00__metadata__:relational_integrity:source-terminal-fixture",
            3 => row,
            else => unreachable,
        }, .value = "actual-terminal-postimage" };
        var terminal: Prepared = .{ .alloc = scratch, .source_prepared = initial };
        var failure: Failure = .{ .original = terminal.asParticipant() };
        try std.testing.expectError(error.InjectedSourceCommitFailure, db.putBatchWithReplayAndParticipant(null, &.{change}, &.{}, null, .{}, failure.callback()));
        {
            var read = try db.beginReadTxn();
            defer read.abort();
            try std.testing.expectError(error.NotFound, read.get(change.key));
            try std.testing.expect((try store.readApplied(scratch, &read, permit)) == null);
            try std.testing.expectEqual(@as(u64, 1), (try source.load(&read)).?.sequence);
        }
        try db.putBatchWithReplayAndParticipant(null, &.{change}, &.{}, null, .{}, terminal.asParticipant());
        var read = try db.beginReadTxn();
        defer read.abort();
        const actual = (try store.readApplied(scratch, &read, permit)).?;
        try std.testing.expectEqualStrings(change.value, try read.get(change.key));
        try std.testing.expectEqual(@as(u64, 2), actual.position.native.sequence);
        try std.testing.expectEqual(@as(u64, 2), (try source.load(&read)).?.sequence);
        try std.testing.expect(terminal.staged);
    }
}

pub const AbortInput = struct {
    permit: @import("../../api/replication_source_apply.zig").Permit,
    physical_root: u128,
    namespace: @import("doc_identity_namespace.zig").Namespace,
    catalog: ?@import("table_catalog.zig").Catalog = null,
    pub fn validate(self: @This()) !void {
        try self.permit.validate();
        if (self.physical_root == 0 or !self.namespace.eql(self.permit.namespace)) return error.ReplicationCutoverAuthorityLost;
        if (self.permit.expected_image) |image| if (image.physical_root_incarnation != self.physical_root) return error.ReplicationCutoverAuthorityLost;
    }
};
/// The transaction manager attaches this only to the actual irreversible
/// ABORT batch, including a genuine fence for a member that never PREPARED.
pub const Aborted = struct {
    alloc: std.mem.Allocator,
    input: AbortInput,
    terminal: ?participant.Participant = null,
    staged: bool = false,
    pub fn asParticipant(self: *@This()) participant.Participant {
        return .{ .ptr = self, .stage_control_on_empty = true, .reset = reset, .observe = observe, .stage = stage };
    }
    fn reset(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.staged = false;
        if (self.terminal) |terminal| terminal.reset(terminal.ptr);
    }
    fn observe(_: *anyopaque, _: []const u8, _: ?[]const u8) void {}
    fn stage(raw: *anyopaque, view: participant.View, encoded: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.staged) return error.InvalidCommitMetadata;
        try self.input.validate();
        const position = try Position.decode(encoded);
        try position.requireNamespace(self.input.namespace);
        const actual = (try source.load(view)) orelse return error.OnlineSourceScopeChanged;
        if (!std.mem.eql(u8, &actual.namespace, &@import("online_source_contract.zig").namespaceBytes(self.input.namespace)) or
            ((position == .native) != (actual.kind == .native)) or
            (position == .native and actual.sequence != position.native.sequence)) return error.ReplicationCutoverAuthorityLost;
        _ = try @import("replication_source_reader_floor.zig").stage(self.alloc, view, self.input.catalog);
        _ = try store.stageAborted(self.alloc, view, self.input.permit, self.input.physical_root, self.input.namespace, position);
        if (self.terminal) |terminal| try terminal.stage(terminal.ptr, view, encoded);
        self.staged = true;
    }
};

/// One composition point for a captured participant without inbound Source
/// semantics. The real artifact author and terminal HA outbox share its cut.
pub const Combined = struct {
    original: ?participant.Participant,
    terminal: participant.Participant,
    pub fn asParticipant(self: *@This()) participant.Participant {
        return .{ .ptr = self, .stage_control_on_empty = true, .reset = reset, .observe = observe, .stage = stage };
    }
    fn reset(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.original) |original| original.reset(original.ptr);
        self.terminal.reset(self.terminal.ptr);
    }
    fn observe(raw: *anyopaque, key: []const u8, value: ?[]const u8) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.original) |original| original.observe(original.ptr, key, value);
        self.terminal.observe(self.terminal.ptr, key, value);
    }
    fn stage(raw: *anyopaque, view: participant.View, encoded: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.original) |original| try original.stage(original.ptr, view, encoded);
        try self.terminal.stage(self.terminal.ptr, view, encoded);
    }
};

/// The native completion grant was independently materialized by Data. This
/// callback records one actual ACK control cut with the physical bitmap and
/// existing HA outbox, while preserving original applied/aborted evidence.
pub const Acknowledged = struct {
    alloc: std.mem.Allocator,
    completion: @import("../../api/replication_source_apply.zig").AckCompletion,
    physical_root: u128,
    catalog: ?@import("table_catalog.zig").Catalog = null,
    terminal: ?participant.Participant = null,
    staged: bool = false,
    pub fn asParticipant(self: *@This()) participant.Participant {
        return .{ .ptr = self, .stage_control_on_empty = true, .reset = reset, .observe = observe, .stage = stage };
    }
    fn reset(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.staged = false;
        if (self.terminal) |terminal| terminal.reset(terminal.ptr);
    }
    fn observe(_: *anyopaque, _: []const u8, _: ?[]const u8) void {}
    fn stage(raw: *anyopaque, view: participant.View, encoded: []const u8) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.staged) return error.InvalidCommitMetadata;
        const position = try Position.decode(encoded);
        try position.requireNamespace(self.completion.permit.namespace);
        const actual = (try source.load(view)) orelse return error.OnlineSourceScopeChanged;
        if (!std.mem.eql(u8, &actual.namespace, &@import("online_source_contract.zig").namespaceBytes(self.completion.permit.namespace)) or ((position == .native) != (actual.kind == .native)) or
            (position == .native and actual.sequence != position.native.sequence)) return error.ReplicationCutoverAuthorityLost;
        _ = try @import("replication_source_reader_floor.zig").stage(self.alloc, view, self.catalog);
        _ = try store.stageAcknowledged(self.alloc, view, self.completion, self.physical_root, position);
        if (self.terminal) |terminal| try terminal.stage(terminal.ptr, view, encoded);
        self.staged = true;
    }
};
