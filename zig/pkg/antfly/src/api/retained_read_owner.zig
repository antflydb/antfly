// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Transport-neutral owner operations. Ingress must derive Scope from current
//! authentication and routing authority on EVERY call. The lifetime registry
//! and its periodic expiry task must outlive every RPC task using this owner.
const std = @import("std");
const registry_mod = @import("../storage/retained_read_registry.zig");
const reads = @import("table_read_source.zig");
const types = @import("../storage/db/types.zig");
const metadata = @import("../metadata/api.zig");
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const time = @import("antfly_platform").time;

/// Stable owner lifetime with one bounded sweeper, never one timer per cursor.
/// Stop ingress and join in-flight RPCs before deinit; their borrows must have
/// returned before resources can be destroyed. Lost responses are reclaimed
/// without relying on another client request arriving.
pub const Runtime = struct {
    registry: registry_mod.Registry,
    tasks: std.Io.Group = .init,

    pub fn create(alloc: std.mem.Allocator, io: std.Io, incarnation: u128, capacity: usize, per_principal: usize, max_lease_ns: u64) !*Runtime {
        const self = try alloc.create(Runtime);
        errdefer alloc.destroy(self);
        self.* = .{ .registry = try registry_mod.Registry.init(alloc, io, incarnation, capacity, per_principal, max_lease_ns) };
        errdefer self.registry.deinit();
        try self.tasks.concurrent(io, maintain, .{self});
        return self;
    }

    pub fn deinit(self: *Runtime) void {
        const alloc = self.registry.alloc;
        self.tasks.cancel(self.registry.io);
        self.registry.deinit();
        alloc.destroy(self);
    }

    fn maintain(self: *Runtime) void {
        while (true) {
            self.registry.expire(time.monotonicNs(), 64);
            self.registry.io.sleep(.fromMilliseconds(10), .awake) catch return;
        }
    }
};

pub const Owner = struct {
    registry: *registry_mod.Registry,
    source: reads.TableReadSource,

    const Capture = struct {
        alloc: std.mem.Allocator,
        cancelled: std.atomic.Value(bool) = .init(false),
        fence: reads.StatementReadFence,

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.fence.deinit();
            self.alloc.destroy(self);
        }
    };
    const Cursor = struct {
        alloc: std.mem.Allocator,
        cancelled: std.atomic.Value(bool) = .init(false),
        view: reads.RelationalReadView,
        next_sequence: u64 = 0,

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.view.deinit();
            self.alloc.destroy(self);
        }
    };

    /// Coordinator must finish read-index admission on ALL participants before
    /// capturing any owner. This operation intentionally requests .stale only
    /// to avoid running another barrier while earlier captures freeze apply.
    pub fn capture(self: Owner, scope: registry_mod.Scope, connection: u128, route: metadata.CatalogRouteFence, table: []const u8, deadline_ns: u64) !?registry_mod.Token {
        if (scope.table_id != route.table_id or scope.group_id != route.route.group_id or scope.topology_revision != route.topology_epoch) return error.RetainedReadScopeChanged;
        const alloc = self.registry.alloc;
        const owned = try alloc.create(Capture);
        errdefer alloc.destroy(owned);
        owned.* = .{ .alloc = alloc, .fence = undefined };
        var source = self.source;
        source.route_fence = route;
        // No request-stack callback or borrowed request Io survives this call.
        source.route_fence.?.admission_cancellation = CancellationToken.fromAtomic(&owned.cancelled);
        source.route_fence.?.admission_deadline_ns = deadline_ns;
        source.route_fence.?.admission_deadline_io = null;
        const opts = types.ScanOptions{
            .execution_deadline_ns = deadline_ns,
            .cancellation = CancellationToken.fromAtomic(&owned.cancelled),
        };
        owned.fence = (try source.tryStatementReadFenceGroupLocal(alloc, scope.group_id, table, opts, .stale)) orelse {
            alloc.destroy(owned);
            return null;
        };
        errdefer owned.fence.deinit();
        return try self.registry.insert(scope, connection, time.monotonicNs(), deadline_ns, .{ .ptr = owned, .kind = .capture, .close = Capture.close, .cancellation = &owned.cancelled });
    }

    pub fn validateCapture(self: Owner, scope: registry_mod.Scope, token: registry_mod.Token) !void {
        var borrow = try self.registry.borrow(token, scope, .capture, time.monotonicNs());
        defer borrow.deinit();
        const capture_value: *Capture = @ptrCast(@alignCast(borrow.resource.ptr));
        try capture_value.fence.validate();
        try borrow.validate(time.monotonicNs());
    }

    /// Snapshot gets a separate owned cancellation capsule: releasing the
    /// short mutation capture must not cancel its long-lived paging cursors.
    pub fn open(self: Owner, scope: registry_mod.Scope, capture_token: registry_mod.Token, connection: u128, from: []const u8, to: []const u8, input: types.ScanOptions, deadline_ns: u64) !registry_mod.Token {
        var borrow = try self.registry.borrow(capture_token, scope, .capture, time.monotonicNs());
        defer borrow.deinit();
        const capture_value: *Capture = @ptrCast(@alignCast(borrow.resource.ptr));
        const alloc = self.registry.alloc;
        const owned = try alloc.create(Cursor);
        errdefer alloc.destroy(owned);
        owned.* = .{ .alloc = alloc, .view = undefined };
        var opts = input;
        var parsed: ?std.json.Parsed(types.RelationalRowQuery) = null;
        defer if (parsed) |*value| value.deinit();
        var query = input.relational_query orelse blk: {
            parsed = try std.json.parseFromSlice(types.RelationalRowQuery, alloc, input.relational_query_json, .{ .allocate = .alloc_always, .parse_numbers = false });
            break :blk parsed.?.value;
        };
        if (query.schema_version) |version| if (version != scope.schema_version) return error.RetainedReadScopeChanged;
        query.schema_version = scope.schema_version;
        opts.relational_query = query;
        opts.columnar_stats = null;
        opts.cancellation = CancellationToken.fromAtomic(&owned.cancelled);
        opts.execution_deadline_ns = deadline_ns;
        owned.view = try capture_value.fence.open(alloc, from, to, opts);
        errdefer owned.view.deinit();
        try capture_value.fence.validate();
        try borrow.validate(time.monotonicNs());
        return try self.registry.insert(scope, connection, time.monotonicNs(), deadline_ns, .{ .ptr = owned, .kind = .cursor, .close = Cursor.close, .cancellation = &owned.cancelled });
    }

    /// Sequence fencing prevents a retransmitted request from silently skipping
    /// a page. Response loss currently requires restarting the statement, not
    /// blindly replaying this stateful RPC against an advanced native cursor.
    pub fn next(self: Owner, alloc: std.mem.Allocator, scope: registry_mod.Scope, token: registry_mod.Token, sequence: u64, limit: u32) !reads.RelationalReadView.Page {
        if (limit == 0 or limit > 4096) return error.InvalidRetainedReadPageLimit;
        var borrow = try self.registry.borrow(token, scope, .cursor, time.monotonicNs());
        defer borrow.deinit();
        const cursor: *Cursor = @ptrCast(@alignCast(borrow.resource.ptr));
        if (sequence != cursor.next_sequence) return error.RetainedReadSequenceMismatch;
        errdefer self.registry.close(token, scope) catch {};
        var page = try cursor.view.next(alloc, limit);
        errdefer page.deinit();
        try borrow.validate(time.monotonicNs());
        // Native snapshots preserve the captured schema, never silently switch
        // decoder identity if publication raced the original capture.
        for (page.rows) |row| if (row.schema_version != scope.schema_version) return error.RetainedReadScopeChanged;
        cursor.next_sequence = std.math.add(u64, cursor.next_sequence, 1) catch return error.RetainedReadSequenceExhausted;
        return page;
    }
};

test "retained read owner owns controls across capture release and cursor expiry" {
    const Fixture = struct {
        capture_token: CancellationToken = .none,
        cursor_token: CancellationToken = .none,
        captures_closed: usize = 0,
        cursors_closed: usize = 0,

        fn capture(ptr: *anyopaque, _: std.mem.Allocator, route: metadata.CatalogRouteFence, _: u64, _: []const u8, opts: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.capture_token = opts.cancellation.?;
            try std.testing.expectEqual(self.capture_token.ptr, route.admission_cancellation.ptr);
            return .{ .ptr = self, .vtable = &.{ .validate = validate, .open = open, .release = release } };
        }
        fn validate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.capture_token.check();
        }
        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.captures_closed += 1;
        }
        fn open(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, opts: types.ScanOptions) !reads.RelationalReadView {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cursor_token = opts.cancellation.?;
            try std.testing.expectEqual(@as(?u32, 5), opts.relational_query.?.schema_version);
            try std.testing.expect(self.cursor_token.ptr != self.capture_token.ptr);
            return .{ .ptr = self, .vtable = &.{ .next = next, .close = close } };
        }
        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, _: u32) !reads.RelationalReadView.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.cursor_token.check();
            return .{ .arena = std.heap.ArenaAllocator.init(alloc), .rows = &.{}, .after = null };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cursors_closed += 1;
        }
    };
    var registry = try registry_mod.Registry.init(std.testing.allocator, std.testing.io, 1, 8, 8, 10 * std.time.ns_per_s);
    defer registry.deinit();
    var fixture: Fixture = .{};
    const owner = Owner{ .registry = &registry, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = Fixture.capture } } };
    const scope = registry_mod.Scope{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    const route = metadata.CatalogRouteFence{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
    const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
    const capture = (try owner.capture(scope, 1, route, "rows", deadline)).?;
    try owner.validateCapture(scope, capture);
    const cursor = try owner.open(scope, capture, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline);
    try registry.close(capture, scope);
    try std.testing.expectEqual(1, fixture.captures_closed);
    var page = try owner.next(std.testing.allocator, scope, cursor, 0, 10);
    page.deinit();
    try std.testing.expectError(error.RetainedReadSequenceMismatch, owner.next(std.testing.allocator, scope, cursor, 0, 10));
    try std.testing.expectEqual(0, fixture.cursors_closed);
    registry.expire(deadline, 8);
    try std.testing.expectEqual(1, fixture.cursors_closed);
    try std.testing.expectError(error.RetainedReadNotFound, owner.next(std.testing.allocator, scope, cursor, 1, 10));
}

test "retained read owner expiry runtime joins maintenance before freeing registry" {
    const runtime = try Runtime.create(std.testing.allocator, std.testing.io, 1, 8, 4, std.time.ns_per_s);
    runtime.deinit();
}
