// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Transport-neutral owner operations. Ingress must derive Scope from current
//! authentication and routing authority on EVERY call. The lifetime registry
//! and its periodic expiry task must outlive every RPC task using this owner.
const std = @import("std");
const registry_mod = @import("../storage/retained_read_registry.zig");
const reads = @import("antfly_local_sources").api_table_read_source;
const types = @import("antfly_local_sources").storage_db_types;
const metadata = @import("../metadata/api.zig");
const CancellationToken = @import("antfly_cancellation").CancellationToken;
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

const attempts = @import("../common/remote_attempts.zig");

/// Synchronous factory borrowed from the authenticated worker frame. It must
/// produce an independent owned reference to exactly id. No pointer to this
/// factory or its frame survives Pending.init; only the returned retention does.
pub const RequestParent = struct {
    ptr: *anyopaque,
    id: attempts.Id,
    check: *const fn (*anyopaque) anyerror!attempts.Id,
    retain: *const fn (*anyopaque) anyerror!attempts.Retention,
};

pub const Owner = struct {
    request_parent: ?RequestParent = null,
    registry: *registry_mod.Registry,
    source: reads.TableReadSource,

    const Capture = struct {
        alloc: std.mem.Allocator,
        cancelled: std.atomic.Value(bool) = .init(false),
        fence: ?reads.StatementReadFence = null,

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fence) |fence| fence.deinit();
            self.alloc.destroy(self);
        }
    };
    const Snapshot = struct {
        alloc: std.mem.Allocator,
        cancelled: std.atomic.Value(bool) = .init(false),
        snapshot: ?@import("antfly_local_sources").storage_statement_read_fence.Snapshot = null,

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.snapshot) |snapshot| snapshot.deinit();
            self.alloc.destroy(self);
        }
    };
    const Cursor = struct {
        alloc: std.mem.Allocator,
        cancelled: std.atomic.Value(bool) = .init(false),
        view: ?reads.RelationalReadView = null,
        next_sequence: u64 = 0,

        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.view) |view| view.deinit();
            self.alloc.destroy(self);
        }
    };

    /// Charge admission before native creation. The exclusive borrow pins the
    /// pending cancellation capsule through callbacks, expiry and cleanup.
    const Pending = struct {
        owner: Owner,
        scope: registry_mod.Scope,
        token: registry_mod.Token,
        borrow: registry_mod.Registry.Borrow,
        published: bool = false,
        fn init(owner: Owner, comptime T: type, kind: registry_mod.Kind, scope: registry_mod.Scope, connection: u128, deadline: u64) !Pending {
            try owner.checkParent();
            var retention: ?attempts.Retention = null;
            if (owner.request_parent) |parent| {
                retention = try parent.retain(parent.ptr);
                if (!retention.?.id.eql(parent.id)) {
                    retention.?.release();
                    return error.InvalidRemoteAttempt;
                }
            }
            defer if (retention) |parent| parent.release();
            const owned = try owner.registry.alloc.create(T);
            owned.* = .{ .alloc = owner.registry.alloc };
            const token = owner.registry.insert(scope, connection, time.monotonicNs(), deadline, .{ .ptr = owned, .kind = kind, .close = T.close, .cancellation = &owned.cancelled, .retention = retention }) catch |err| {
                owner.registry.alloc.destroy(owned);
                return err;
            };
            retention = null; // the occupied registry slot now owns this ref
            const borrow = owner.borrowResource(token, scope, kind, time.monotonicNs()) catch |err| {
                owner.registry.close(token, scope) catch {};
                return err;
            };
            return .{ .owner = owner, .scope = scope, .token = token, .borrow = borrow };
        }
        fn value(self: *Pending, comptime T: type) *T {
            return @ptrCast(@alignCast(self.borrow.resource.ptr));
        }
        fn publish(self: *Pending) !registry_mod.Token {
            try self.owner.validateBorrow(&self.borrow);
            self.published = true;
            return self.token;
        }
        fn deinit(self: *Pending) void {
            if (!self.published) self.owner.registry.close(self.token, self.scope) catch {};
            self.borrow.deinit();
        }
    };

    fn checkParent(self: Owner) !void {
        if (self.request_parent) |parent| {
            const actual = try parent.check(parent.ptr);
            if (!actual.eql(parent.id)) return error.InvalidRemoteAttempt;
        }
    }
    fn validateBorrow(self: Owner, borrowed: *const registry_mod.Registry.Borrow) !void {
        try self.checkParent();
        try borrowed.validate(time.monotonicNs());
    }
    fn borrowResource(self: Owner, token: registry_mod.Token, scope: registry_mod.Scope, kind: registry_mod.Kind, now: u64) !registry_mod.Registry.Borrow {
        try self.checkParent();
        return self.registry.borrowForAttempt(token, scope, kind, now, if (self.request_parent) |parent| parent.id else null);
    }

    /// Coordinator must finish read-index admission on ALL participants before
    /// capturing any owner. This operation intentionally requests .stale only
    /// to avoid running another barrier while earlier captures freeze apply.
    pub fn capture(self: Owner, scope: registry_mod.Scope, connection: u128, route: metadata.CatalogRouteFence, table: []const u8, deadline_ns: u64) !?registry_mod.Token {
        return self.captureWithAdmission(scope, connection, route, table, deadline_ns, null);
    }
    pub fn captureWithAdmission(self: Owner, scope: registry_mod.Scope, connection: u128, route: metadata.CatalogRouteFence, table: []const u8, deadline_ns: u64, original: ?@import("antfly_local_sources").common_read_admission.Port) !?registry_mod.Token {
        var retained_dependency: @import("antfly_local_sources").common_read_admission.RetainedDependency = .{};
        const control = retained_dependency.bind(original);
        if (control) |port| try port.checkpoint();
        if (scope.table_id != route.table_id or scope.group_id != route.route.group_id or scope.topology_revision != route.topology_epoch) return error.RetainedReadScopeChanged;
        const alloc = self.registry.alloc;
        var pending = try Pending.init(self, Capture, .capture, scope, connection, deadline_ns);
        defer pending.deinit();
        const owned = pending.value(Capture);
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
        owned.fence = (if (control) |port| try source.tryStatementReadFenceGroupLocalWithAdmission(alloc, scope.group_id, table, opts, .stale, port) else try source.tryStatementReadFenceGroupLocal(alloc, scope.group_id, table, opts, .stale)) orelse return null;
        if (control) |port| try port.checkpoint();
        return try pending.publish();
    }

    pub fn validateCapture(self: Owner, scope: registry_mod.Scope, token: registry_mod.Token) !void {
        return self.validateCaptureWithAdmission(scope, token, null);
    }
    pub fn validateCaptureWithAdmission(self: Owner, scope: registry_mod.Scope, token: registry_mod.Token, original: ?@import("antfly_local_sources").common_read_admission.Port) !void {
        var retained_dependency: @import("antfly_local_sources").common_read_admission.RetainedDependency = .{};
        const control = retained_dependency.bind(original);
        if (control) |borrowed| try borrowed.checkpoint();
        var borrow = try self.borrowResource(token, scope, .capture, time.monotonicNs());
        defer borrow.deinit();
        const capture_value: *Capture = @ptrCast(@alignCast(borrow.resource.ptr));
        if (control) |borrowed| try capture_value.fence.?.validateWithAdmission(borrowed) else try capture_value.fence.?.validate();
        try self.validateBorrow(&borrow);
        if (control) |borrowed| try borrowed.checkpoint();
    }

    /// Convert a short mutation fence into an independently leased immutable
    /// owner cut. The coordinator captures every owner while all fences are
    /// held, validates the cohort, then releases the fences before row work.
    pub fn captureSnapshot(self: Owner, scope: registry_mod.Scope, capture_token: registry_mod.Token, connection: u128, deadline_ns: u64) !registry_mod.Token {
        return self.captureSnapshotWithAdmission(scope, capture_token, connection, deadline_ns, null);
    }
    pub fn captureSnapshotWithAdmission(self: Owner, scope: registry_mod.Scope, capture_token: registry_mod.Token, connection: u128, deadline_ns: u64, original: ?@import("antfly_local_sources").common_read_admission.Port) !registry_mod.Token {
        var retained_dependency: @import("antfly_local_sources").common_read_admission.RetainedDependency = .{};
        const control = retained_dependency.bind(original);
        if (control) |port| try port.checkpoint();
        var borrow = try self.borrowResource(capture_token, scope, .capture, time.monotonicNs());
        defer borrow.deinit();
        const capture_value: *Capture = @ptrCast(@alignCast(borrow.resource.ptr));
        const alloc = self.registry.alloc;
        var pending = try Pending.init(self, Snapshot, .snapshot, scope, connection, deadline_ns);
        defer pending.deinit();
        const owned = pending.value(Snapshot);
        owned.snapshot = if (control) |port| try capture_value.fence.?.captureSnapshotWithAdmission(alloc, port) else try capture_value.fence.?.captureSnapshot(alloc);
        if (control) |port| try port.checkpoint();
        if (control) |borrowed| try capture_value.fence.?.validateWithAdmission(borrowed) else try capture_value.fence.?.validate();
        try self.validateBorrow(&borrow);
        if (control) |borrowed| try borrowed.checkpoint();
        return try pending.publish();
    }

    /// Snapshot gets a separate owned cancellation capsule: releasing the
    /// short mutation capture must not cancel its long-lived paging cursors.
    pub fn open(self: Owner, scope: registry_mod.Scope, capture_token: registry_mod.Token, connection: u128, from: []const u8, to: []const u8, input: types.ScanOptions, deadline_ns: u64) !registry_mod.Token {
        return self.openWithAdmission(scope, capture_token, connection, from, to, input, deadline_ns, null);
    }
    pub fn openWithAdmission(self: Owner, scope: registry_mod.Scope, capture_token: registry_mod.Token, connection: u128, from: []const u8, to: []const u8, input: types.ScanOptions, deadline_ns: u64, original: ?@import("antfly_local_sources").common_read_admission.Port) !registry_mod.Token {
        var retained_dependency: @import("antfly_local_sources").common_read_admission.RetainedDependency = .{};
        const control = retained_dependency.bind(original);
        if (control) |port| try port.checkpoint();
        var borrow = try self.borrowResource(capture_token, scope, .capture, time.monotonicNs());
        defer borrow.deinit();
        const capture_value: *Capture = @ptrCast(@alignCast(borrow.resource.ptr));
        const alloc = self.registry.alloc;
        var pending = try Pending.init(self, Cursor, .cursor, scope, connection, deadline_ns);
        defer pending.deinit();
        const owned = pending.value(Cursor);
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
        // A registry cursor outlives the incoming request execution owner.
        opts.execution_wait = null;
        opts.cancellation = CancellationToken.fromAtomic(&owned.cancelled);
        opts.execution_deadline_ns = deadline_ns;
        owned.view = if (control) |port| try capture_value.fence.?.openWithAdmission(alloc, from, to, opts, port) else try capture_value.fence.?.open(alloc, from, to, opts);
        if (control) |port| try port.checkpoint();
        // open() validates local ownership while pinning; the coordinator does
        // one final quorum validation per capture after ALL aliases are open.
        // Repeating quorum rounds per alias would inflate the apply-lock hold.
        try self.validateBorrow(&borrow);
        return try pending.publish();
    }

    pub fn openSnapshot(self: Owner, scope: registry_mod.Scope, snapshot_token: registry_mod.Token, connection: u128, from: []const u8, to: []const u8, input: types.ScanOptions, deadline_ns: u64) !registry_mod.Token {
        return self.openSnapshotWithAdmission(scope, snapshot_token, connection, from, to, input, deadline_ns, null);
    }
    pub fn openSnapshotWithAdmission(self: Owner, scope: registry_mod.Scope, snapshot_token: registry_mod.Token, connection: u128, from: []const u8, to: []const u8, input: types.ScanOptions, deadline_ns: u64, control: ?@import("antfly_local_sources").common_read_admission.Port) !registry_mod.Token {
        if (control) |port| try port.checkpoint();
        var borrow = try self.borrowResource(snapshot_token, scope, .snapshot, time.monotonicNs());
        defer borrow.deinit();
        const retained: *Snapshot = @ptrCast(@alignCast(borrow.resource.ptr));
        const alloc = self.registry.alloc;
        var pending = try Pending.init(self, Cursor, .cursor, scope, connection, deadline_ns);
        defer pending.deinit();
        const owned = pending.value(Cursor);
        var opts = input;
        var parsed: ?std.json.Parsed(types.RelationalRowQuery) = null;
        defer if (parsed) |*value| value.deinit();
        var query = input.relational_query orelse blk: {
            parsed = try std.json.parseFromSlice(types.RelationalRowQuery, alloc, input.relational_query_json, .{ .allocate = .alloc_always, .parse_numbers = false });
            break :blk parsed.?.value;
        };
        if (query.schema_version) |version| if (version != scope.schema_version) return error.RetainedReadScopeChanged;
        if (query.index != null or query.auto_index) return error.SqlStatementSnapshotRequired;
        query.schema_version = scope.schema_version;
        opts.relational_query = query;
        opts.columnar_stats = null;
        // A registry cursor outlives the incoming request execution owner.
        opts.execution_wait = null;
        opts.cancellation = CancellationToken.fromAtomic(&owned.cancelled);
        opts.execution_deadline_ns = deadline_ns;
        owned.view = if (control) |port| try retained.snapshot.?.openWithAdmission(alloc, from, to, opts, port) else try retained.snapshot.?.open(alloc, from, to, opts);
        if (control) |port| try port.checkpoint();
        try self.validateBorrow(&borrow);
        return try pending.publish();
    }

    /// Sequence fencing prevents a retransmitted request from silently skipping
    /// a page. Response loss currently requires restarting the statement, not
    /// blindly replaying this stateful RPC against an advanced native cursor.
    pub fn next(self: Owner, alloc: std.mem.Allocator, scope: registry_mod.Scope, token: registry_mod.Token, sequence: u64, limit: u32) !reads.RelationalReadView.Page {
        return self.nextWithAdmission(alloc, scope, token, sequence, limit, null);
    }
    pub fn nextWithAdmission(self: Owner, alloc: std.mem.Allocator, scope: registry_mod.Scope, token: registry_mod.Token, sequence: u64, limit: u32, control: ?@import("antfly_local_sources").common_read_admission.Port) !reads.RelationalReadView.Page {
        if (limit == 0 or limit > 4096) return error.InvalidRetainedReadPageLimit;
        var borrow = try self.borrowResource(token, scope, .cursor, time.monotonicNs());
        defer borrow.deinit();
        const cursor: *Cursor = @ptrCast(@alignCast(borrow.resource.ptr));
        if (sequence != cursor.next_sequence) return error.RetainedReadSequenceMismatch;
        errdefer self.registry.close(token, scope) catch {};
        var page = if (control) |port| try cursor.view.?.nextWithAdmission(alloc, limit, port) else try cursor.view.?.next(alloc, limit);
        errdefer page.deinit();
        try self.validateBorrow(&borrow);
        // Native snapshots preserve the captured schema, never silently switch
        // decoder identity if publication raced the original capture.
        for (page.rows) |row| if (row.schema_version != scope.schema_version) return error.RetainedReadScopeChanged;
        cursor.next_sequence = std.math.add(u64, cursor.next_sequence, 1) catch return error.RetainedReadSequenceExhausted;
        return page;
    }

    pub fn rangeProofs(self: Owner, alloc: std.mem.Allocator, scope: registry_mod.Scope, token: registry_mod.Token) ![]@import("antfly_local_sources").storage_range_protection.Proof {
        return self.rangeProofsWithAdmission(alloc, scope, token, null);
    }
    pub fn rangeProofsWithAdmission(self: Owner, alloc: std.mem.Allocator, scope: registry_mod.Scope, token: registry_mod.Token, control: ?@import("antfly_local_sources").common_read_admission.Port) ![]@import("antfly_local_sources").storage_range_protection.Proof {
        if (control) |port| try port.checkpoint();
        var borrow = try self.borrowResource(token, scope, .cursor, time.monotonicNs());
        defer borrow.deinit();
        const cursor: *Cursor = @ptrCast(@alignCast(borrow.resource.ptr));
        const result = if (control) |port| try cursor.view.?.rangeProofsWithAdmission(alloc, port) else try cursor.view.?.rangeProofs(alloc);
        errdefer alloc.free(result);
        try self.validateBorrow(&borrow);
        return result;
    }

    pub fn normalize(self: Owner, alloc: std.mem.Allocator, scope: registry_mod.Scope, token: registry_mod.Token, writes: []const types.BatchWrite) ![]types.BatchWrite {
        return self.normalizeWithAdmission(alloc, scope, token, writes, null);
    }
    pub fn normalizeWithAdmission(self: Owner, alloc: std.mem.Allocator, scope: registry_mod.Scope, token: registry_mod.Token, writes: []const types.BatchWrite, control: ?@import("antfly_local_sources").common_read_admission.Port) ![]types.BatchWrite {
        if (control) |port| try port.checkpoint();
        if (writes.len > 4096) return error.InvalidRetainedReadQuery;
        var borrow = try self.borrowResource(token, scope, .cursor, time.monotonicNs());
        defer borrow.deinit();
        const cursor: *Cursor = @ptrCast(@alignCast(borrow.resource.ptr));
        const result = if (control) |port| try cursor.view.?.normalizeWithAdmission(alloc, writes, port) else try cursor.view.?.normalize(alloc, writes);
        errdefer {
            for (result) |row| {
                alloc.free(row.key);
                alloc.free(row.value);
                for (row.json_null_fields) |field| alloc.free(field);
                if (row.json_null_fields.len != 0) alloc.free(row.json_null_fields);
            }
            alloc.free(result);
        }
        try self.validateBorrow(&borrow);
        return result;
    }
};

pub const consumer_tests = consumerTests();
comptime {
    if (@import("builtin").is_test) _ = consumer_tests;
}
fn consumerTests() type {
    if (!@import("builtin").is_test) return struct {};
    const root = @import("antfly_source_root");
    if (@hasDecl(root, "implementation_tests_only") and root.implementation_tests_only) return struct {};
    return struct {
        test "retained read owner fenced admission preserves original errors and retires late results once" {
            const admission = @import("antfly_local_sources").common_read_admission;
            const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
            const SnapshotType = @import("antfly_local_sources").storage_statement_read_fence.Snapshot;
            const Stage = enum { none, capture, validate, snapshot, open };
            const Fixture = struct {
                execution: *native.Execution,
                scheduler: *native.Scheduler,
                stage: Stage,
                cause: anyerror,
                failure: ?anyerror = null,
                held: bool = false,
                closed_captures: usize = 0,
                closed_snapshots: usize = 0,
                closed_views: usize = 0,
                lifetime_checks: usize = 0,
                poisoned: bool = false,
                fn cpu(_: *anyopaque) !void {
                    return error.UnexpectedFencedOwnerHandoff;
                }
                fn life(raw: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (self.poisoned) return error.TestOwnerBorrowEscaped;
                    self.lifetime_checks += 1;
                    try self.execution.checkLifetime();
                    if (self.failure) |cause| {
                        self.failure = null;
                        return cause;
                    }
                }
                fn verify(self: *@This(), control: admission.Port, stage: Stage) !void {
                    try control.checkpoint();
                    try std.testing.expect(try control.executionWait() == null);
                    try std.testing.expectError(error.Backpressured, control.waitForProgress());
                    try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().runnable_slots);
                    try std.testing.expectEqual(@as(usize, 1), self.scheduler.stats().operations);
                    try std.testing.expectEqual(@as(u64, 17), self.scheduler.stats().retained_bytes);
                    var health: native.Execution = .{};
                    try health.start(self.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                    health.finish();
                    if (self.stage == stage) {
                        self.failure = self.cause;
                        self.stage = .none;
                    }
                }
                fn capture(raw: *anyopaque, _: std.mem.Allocator, _: metadata.CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency, control: admission.Port) !?reads.StatementReadFence {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try self.verify(control, .capture);
                    self.held = true;
                    return .{ .ptr = raw, .vtable = &.{ .validate = undefined, .validate_with_admission = validate, .open = undefined, .open_with_admission = open, .capture_snapshot_with_admission = snapshot, .release = release } };
                }
                fn validate(raw: *anyopaque, control: admission.Port) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expect(self.held);
                    try self.verify(control, .validate);
                }
                fn snapshot(raw: *anyopaque, _: std.mem.Allocator, control: admission.Port) !SnapshotType {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expect(self.held);
                    try self.verify(control, .snapshot);
                    return .{ .ptr = raw, .vtable = &.{ .open = undefined, .release = closeSnapshot } };
                }
                fn open(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, opts: types.ScanOptions, control: admission.Port) !reads.RelationalReadView {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    try std.testing.expect(self.held);
                    try std.testing.expect(opts.execution_wait == null);
                    try self.verify(control, .open);
                    return .{ .ptr = raw, .vtable = &.{ .next = undefined, .close = closeView } };
                }
                fn release(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    std.debug.assert(self.held);
                    self.held = false;
                    self.closed_captures += 1;
                }
                fn closeSnapshot(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.closed_snapshots += 1;
                }
                fn closeView(raw: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    self.closed_views += 1;
                }
            };
            const scope: registry_mod.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
            const route: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
            for ([_]Stage{ .capture, .validate, .snapshot, .open }) |stage| {
                for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.RowPolicyCatalogChanged, error.NotFound }) |cause| {
                    var scheduler = try native.Scheduler.init(.{ .slots = 2 });
                    var execution: native.Execution = .{};
                    try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 17 }, .query);
                    defer execution.finish();
                    var registry = try registry_mod.Registry.init(std.testing.allocator, std.testing.io, 1, 4, 4, 10 * std.time.ns_per_s);
                    defer registry.deinit();
                    var fixture: Fixture = .{ .scheduler = &scheduler, .execution = &execution, .stage = stage, .cause = cause };
                    const payload: admission.Payload = .{ .ptr = &fixture, .vtable = &.{ .checkpoint = Fixture.cpu, .check_lifetime = Fixture.life, .wait_for_progress = Fixture.cpu }, .execution_wait = .{ .ptr = &fixture, .begin = Fixture.cpu, .end = Fixture.cpu } };
                    const port: admission.Port = .init(&payload);
                    const owner: Owner = .{ .registry = &registry, .source = .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_with_admission = Fixture.capture } } };
                    const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
                    if (stage == .capture) {
                        try std.testing.expectError(cause, owner.captureWithAdmission(scope, 1, route, "rows", deadline, port));
                        try std.testing.expect(!fixture.held);
                    }
                    const captured = (try owner.captureWithAdmission(scope, 1, route, "rows", deadline, port)).?;
                    if (stage == .validate) try std.testing.expectError(cause, owner.validateCaptureWithAdmission(scope, captured, port));
                    try owner.validateCaptureWithAdmission(scope, captured, port);
                    if (stage == .snapshot) try std.testing.expectError(cause, owner.captureSnapshotWithAdmission(scope, captured, 1, deadline, port));
                    const snapshot = try owner.captureSnapshotWithAdmission(scope, captured, 1, deadline, port);
                    if (stage == .open) try std.testing.expectError(cause, owner.openWithAdmission(scope, captured, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline, port));
                    const cursor = try owner.openWithAdmission(scope, captured, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline, port);
                    const checks = fixture.lifetime_checks;
                    fixture.poisoned = true;
                    try registry.close(captured, scope);
                    try registry.close(snapshot, scope);
                    try registry.close(cursor, scope);
                    try std.testing.expectEqual(checks, fixture.lifetime_checks);
                    try std.testing.expectEqual(@as(usize, if (stage == .capture) 2 else 1), fixture.closed_captures);
                    try std.testing.expectEqual(@as(usize, if (stage == .snapshot) 2 else 1), fixture.closed_snapshots);
                    try std.testing.expectEqual(@as(usize, if (stage == .open) 2 else 1), fixture.closed_views);
                    for (registry.slots) |slot| try std.testing.expect(slot.entry == null);
                    execution.finish();
                    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
                    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
                }
            }
        }

        test "retained read owner owns controls across capture release and cursor expiry" {
            const Fixture = struct {
                capture_token: CancellationToken = .none,
                cursor_token: CancellationToken = .none,
                registry: ?*registry_mod.Registry = null,
                scope: registry_mod.Scope = undefined,
                capture_calls: usize = 0,
                snapshot_calls: usize = 0,
                open_calls: usize = 0,
                missing_capture: bool = false,
                fail_capture: bool = false,
                revoke_capture: bool = false,
                captures_closed: usize = 0,
                snapshots_closed: usize = 0,
                cursors_closed: usize = 0,

                fn noop(_: *anyopaque) void {}
                fn checkAdmission(self: *@This()) !void {
                    if (self.registry) |registry| {
                        try std.testing.expectError(error.RetainedReadAdmissionExceeded, registry.insert(self.scope, 2, time.monotonicNs(), time.monotonicNs() + std.time.ns_per_s, .{ .ptr = self, .close = noop, .kind = .capture }));
                    }
                }
                fn capture(ptr: *anyopaque, _: std.mem.Allocator, route: metadata.CatalogRouteFence, _: u64, _: []const u8, opts: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.capture_calls += 1;
                    try self.checkAdmission();
                    self.capture_token = opts.cancellation.?;
                    try std.testing.expectEqual(self.capture_token.ptr, route.admission_cancellation.ptr);
                    if (self.revoke_capture) self.registry.?.disconnect(1);
                    if (self.missing_capture) return null;
                    if (self.fail_capture) return error.TestCaptureFailed;
                    return .{ .ptr = self, .vtable = &.{ .validate = validate, .open = open, .capture_snapshot = captureSnapshot, .release = release } };
                }
                fn captureSnapshot(ptr: *anyopaque, _: std.mem.Allocator) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.snapshot_calls += 1;
                    try self.checkAdmission();
                    try self.capture_token.check();
                    return .{ .ptr = self, .vtable = &.{ .open = open, .release = releaseSnapshot } };
                }
                fn releaseSnapshot(ptr: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.snapshots_closed += 1;
                }
                pub fn validate(ptr: *anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    try self.capture_token.check();
                }
                fn release(ptr: *anyopaque) void {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.captures_closed += 1;
                }
                fn open(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, opts: types.ScanOptions) !reads.RelationalReadView {
                    const self: *@This() = @ptrCast(@alignCast(ptr));
                    self.open_calls += 1;
                    try self.checkAdmission();
                    self.cursor_token = opts.cancellation.?;
                    try std.testing.expectEqual(@as(?u32, 5), opts.relational_query.?.schema_version);
                    try std.testing.expect(opts.execution_wait == null);
                    try std.testing.expect(self.cursor_token.ptr != self.capture_token.ptr);
                    return .{ .ptr = self, .vtable = &.{ .next = next, .close = close } };
                }
                fn borrowedWait(_: *anyopaque) !void {
                    return error.BorrowedExecutionMustNotEscape;
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
            fixture.registry = &registry;
            fixture.scope = scope;
            registry.max_per_principal = 1;
            const capture = (try owner.capture(scope, 1, route, "rows", deadline)).?;
            try owner.validateCapture(scope, capture);
            registry.max_per_principal = 2;
            const snapshot = try owner.captureSnapshot(scope, capture, 1, deadline);
            const borrowed_wait: @import("antfly_local_sources").common_execution_wait.Boundary = .{ .ptr = &fixture, .begin = Fixture.borrowedWait, .end = Fixture.borrowedWait };
            registry.max_per_principal = 3;
            const cursor = try owner.open(scope, capture, 1, "", "", .{ .relational_query = .{ .fields = &.{} }, .execution_wait = borrowed_wait }, deadline);
            try registry.close(capture, scope);
            try std.testing.expectEqual(1, fixture.captures_closed);
            var wrong_scope = scope;
            wrong_scope.group_id += 1;
            try std.testing.expectError(error.RetainedReadScopeChanged, owner.openSnapshot(wrong_scope, snapshot, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline));
            const delayed = try owner.openSnapshot(scope, snapshot, 1, "", "", .{ .relational_query = .{ .fields = &.{} }, .execution_wait = borrowed_wait }, deadline);
            try registry.close(snapshot, scope);
            try std.testing.expectEqual(1, fixture.snapshots_closed);
            var delayed_page = try owner.next(std.testing.allocator, scope, delayed, 0, 10);
            delayed_page.deinit();
            var page = try owner.next(std.testing.allocator, scope, cursor, 0, 10);
            page.deinit();
            try std.testing.expectError(error.RetainedReadSequenceMismatch, owner.next(std.testing.allocator, scope, cursor, 0, 10));
            try std.testing.expectEqual(0, fixture.cursors_closed);
            registry.expire(deadline, 8);
            try std.testing.expectEqual(2, fixture.cursors_closed);
            try std.testing.expectError(error.RetainedReadNotFound, owner.next(std.testing.allocator, scope, cursor, 1, 10));
            registry.max_per_principal = 1;
            const sole = (try owner.capture(scope, 1, route, "rows", deadline)).?;
            const calls = .{ fixture.capture_calls, fixture.snapshot_calls, fixture.open_calls };
            try std.testing.expectError(error.RetainedReadAdmissionExceeded, owner.capture(scope, 1, route, "rows", deadline));
            try std.testing.expectError(error.RetainedReadAdmissionExceeded, owner.captureSnapshot(scope, sole, 1, deadline));
            try std.testing.expectError(error.RetainedReadAdmissionExceeded, owner.open(scope, sole, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline));
            try std.testing.expectEqual(calls, .{ fixture.capture_calls, fixture.snapshot_calls, fixture.open_calls });
            try registry.close(sole, scope);
            fixture.missing_capture = true;
            try std.testing.expect((try owner.capture(scope, 1, route, "rows", deadline)) == null);
            fixture.missing_capture = false;
            fixture.fail_capture = true;
            try std.testing.expectError(error.TestCaptureFailed, owner.capture(scope, 1, route, "rows", deadline));
            fixture.fail_capture = false;
            fixture.revoke_capture = true;
            const closed_before = fixture.captures_closed;
            try std.testing.expectError(error.RetainedReadExpired, owner.capture(scope, 1, route, "rows", deadline));
            try std.testing.expectEqual(closed_before + 1, fixture.captures_closed);
            fixture.revoke_capture = false;
            // All rejected and missing creations return their reservation.
            const final_capture = (try owner.capture(scope, 1, route, "rows", deadline)).?;
            try registry.close(final_capture, scope);
        }

        test "retained read owner expiry runtime joins maintenance before freeing registry" {
            const runtime = try Runtime.create(std.testing.allocator, std.testing.io, 1, 8, 4, std.time.ns_per_s);
            runtime.deinit();
        }
    };
}

test "retained read owner worker parent fences terminal evidence through physical token destruction" {
    const worker = @import("../common/remote_worker.zig");
    const membership = @import("../common/remote_membership.zig");
    const wire = @import("../common/remote_attempt_wire.zig");
    const store_mod = @import("../common/remote_fence_store.zig");
    const Fixture = struct {
        worker: *worker.Registry,
        binding: membership.Binding,
        cancel_on_capture: ?wire.Verified = null,
        captures: usize = 0,
        snapshots: usize = 0,
        opens: usize = 0,
        pages: usize = 0,
        closed: std.atomic.Value(usize) = .init(0),
        block_once: std.atomic.Value(bool) = .init(false),
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        const scope: registry_mod.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
        const route: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 2, .topology_epoch = 4, .route = .{ .group_id = 3, .range_id = 3, .identity_namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 3 } } };
        fn source(self: *@This()) reads.TableReadSource {
            return .{ .ptr = self, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .try_statement_read_fence_group_local_routed = capture } };
        }
        fn capture(raw: *anyopaque, _: std.mem.Allocator, _: metadata.CatalogRouteFence, _: u64, _: []const u8, _: types.ScanOptions, _: @import("../raft/read_gate.zig").ReadConsistency) !?reads.StatementReadFence {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.captures += 1;
            if (self.cancel_on_capture) |input| _ = try self.worker.cancelWithWait(input, self.binding, null);
            return .{ .ptr = raw, .vtable = &.{ .validate = validate, .capture_snapshot = snapshot, .open = open, .release = close } };
        }
        fn validate(_: *anyopaque) !void {}
        fn snapshot(raw: *anyopaque, _: std.mem.Allocator) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.snapshots += 1;
            return .{ .ptr = raw, .vtable = &.{ .open = open, .release = close } };
        }
        fn open(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.ScanOptions) !reads.RelationalReadView {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.opens += 1;
            return .{ .ptr = raw, .vtable = &.{ .next = next, .close = close } };
        }
        fn next(raw: *anyopaque, alloc: std.mem.Allocator, _: u32) !reads.RelationalReadView.Page {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pages += 1;
            return .{ .arena = .init(alloc), .rows = &.{}, .after = null };
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.block_once.swap(false, .acq_rel)) {
                self.entered.set(std.testing.io);
                self.release.waitUncancelable(std.testing.io);
            }
            _ = self.closed.fetchAdd(1, .acq_rel);
        }
    };
    const Clock = struct {
        fn now() u64 {
            return 100;
        }
    };
    const Frame = struct {
        lease: *worker.Lease,
        poisoned: bool = false,
        retain_from: ?*worker.Lease = null,
        fn check(raw: *anyopaque) !attempts.Id {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.poisoned) return error.TestRequestFrameEscaped;
            try self.lease.check();
            return self.lease.id;
        }
        fn retain(raw: *anyopaque) !attempts.Retention {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = try check(raw);
            return (self.retain_from orelse self.lease).retainRemoteChild();
        }
        fn parent(self: *@This()) RequestParent {
            return .{ .ptr = self, .id = self.lease.id, .check = check, .retain = retain };
        }
    };
    const Ready = struct {
        fn quiescent(_: *const anyopaque) bool {
            return true; // freshly created isolated test store, no prior work
        }
    };
    const local: membership.Local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
    const coordinator = try membership.coordinatorIdentity(local.metadata_incarnation, 7);
    const binding: membership.Binding = .{ .coordinator_node_id = 7, .worker_node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
    const Input = struct {
        fn request(id: attempts.Id, kind: wire.Kind) wire.Verified {
            return .{ .context = .{ .kind = kind, .id = id, .worker_incarnation = 11, .remaining_ns = 1000, .charged_bytes = if (kind == .read) 1024 else 0, .request_digest = @splat(7) }, .local_deadline_ns = 1100 };
        }
        fn borrowOwned(registry: *registry_mod.Registry, token: registry_mod.Token, parent: attempts.Id) !void {
            var borrowed = try registry.borrowForAttempt(token, Fixture.scope, .capture, time.monotonicNs(), parent);
            defer borrowed.deinit();
        }
    };
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try store_mod.Store.create(a, std.testing.io, tmp.dir, 11, 2);
    defer store.deinit();
    var workers = try worker.Registry.init(a, &store, local, .{ .coordinators = 2, .attempts = 8, .per_coordinator_attempts = 8, .duplicate_window = 16, .bytes = 16 << 10, .per_coordinator_bytes = 8 << 10, .resource_references = 2 }, Clock.now);
    defer workers.deinit() catch unreachable;
    try workers.completeRecovery(.{ .context = &local, .quiescent = Ready.quiescent });
    _ = try workers.fence(Input.request(.{ .coordinator = coordinator, .generation = 1, .operation = 0, .sequence = 0 }, .fence), binding);
    var resources = try registry_mod.Registry.init(a, std.testing.io, 99, 8, 8, 10 * std.time.ns_per_s);
    defer resources.deinit();
    var fixture: Fixture = .{ .worker = &workers, .binding = binding };
    const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
    var id: attempts.Id = .{ .coordinator = coordinator, .generation = 1, .operation = 71, .sequence = 1 };
    var lease1 = (try workers.begin(Input.request(id, .read), binding)).started;
    defer lease1.close();
    var frame1: Frame = .{ .lease = &lease1 };
    var owner1: Owner = .{ .registry = &resources, .source = fixture.source(), .request_parent = frame1.parent() };
    owner1.request_parent.?.id.operation += 1;
    try std.testing.expectError(error.InvalidRemoteAttempt, owner1.capture(Fixture.scope, 1, Fixture.route, "rows", deadline));
    try std.testing.expectEqual(@as(usize, 1), workers.snapshot().references);
    try std.testing.expectEqual(@as(usize, 0), fixture.captures);
    owner1.request_parent.?.id = id;
    const foreign_id: attempts.Id = .{ .coordinator = coordinator, .generation = 1, .operation = 72, .sequence = 6 };
    var foreign_lease = (try workers.begin(Input.request(foreign_id, .read), binding)).started;
    defer foreign_lease.close();
    frame1.retain_from = &foreign_lease;
    try std.testing.expectError(error.InvalidRemoteAttempt, owner1.capture(Fixture.scope, 1, Fixture.route, "rows", deadline));
    try std.testing.expectEqual(@as(usize, 2), workers.snapshot().references);
    try std.testing.expectEqual(@as(usize, 0), fixture.captures);
    frame1.retain_from = null;
    foreign_lease.close();
    {
        var no_bytes: [0]u8 = .{};
        var exhausted = std.heap.FixedBufferAllocator.init(&no_bytes);
        const allocator = resources.alloc;
        resources.alloc = exhausted.allocator();
        defer resources.alloc = allocator;
        try std.testing.expectError(error.OutOfMemory, owner1.capture(Fixture.scope, 1, Fixture.route, "rows", deadline));
        try std.testing.expectEqual(@as(usize, 1), workers.snapshot().references);
        try std.testing.expectEqual(@as(usize, 0), fixture.captures);
    }
    {
        const Empty = struct {
            closed: usize = 0,
            fn close(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.closed += 1;
            }
        };
        var empty: Empty = .{};
        var occupied: [8]registry_mod.Token = undefined;
        var held: usize = 0;
        defer for (occupied[0..held]) |value| resources.close(value, Fixture.scope) catch {};
        for (&occupied) |*value| {
            value.* = try resources.insert(Fixture.scope, 1, time.monotonicNs(), deadline, .{ .ptr = &empty, .close = Empty.close, .kind = .capture });
            held += 1;
        }
        try std.testing.expectError(error.RetainedReadAdmissionExceeded, owner1.capture(Fixture.scope, 1, Fixture.route, "rows", deadline));
        try std.testing.expectEqual(@as(usize, 1), workers.snapshot().references);
        try std.testing.expectEqual(@as(usize, 0), fixture.captures);
        for (occupied) |value| try resources.close(value, Fixture.scope);
        held = 0;
        try std.testing.expectEqual(@as(usize, 8), empty.closed);
    }
    const capture = (try owner1.capture(Fixture.scope, 1, Fixture.route, "rows", deadline)).?;
    try std.testing.expectError(error.RemoteResourceCapacity, owner1.capture(Fixture.scope, 1, Fixture.route, "rows", deadline));
    try std.testing.expectEqual(@as(usize, 1), fixture.captures);
    lease1.close();
    frame1.poisoned = true;
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try workers.status(Input.request(id, .status), binding));
    try std.testing.expectEqual(@as(usize, 1024), workers.snapshot().bytes);
    const capture_id = id;
    var same_operation = id;
    same_operation.sequence = 99;
    try Input.borrowOwned(&resources, capture, same_operation);
    for (0..3) |dimension| {
        var changed = same_operation;
        switch (dimension) {
            0 => changed.coordinator += 1,
            1 => changed.generation += 1,
            2 => changed.operation += 1,
            else => unreachable,
        }
        try std.testing.expectError(error.RetainedReadScopeChanged, Input.borrowOwned(&resources, capture, changed));
    }
    id.sequence = 2;
    var lease2 = (try workers.begin(Input.request(id, .read), binding)).started;
    defer lease2.close();
    var frame2: Frame = .{ .lease = &lease2 };
    const owner2: Owner = .{ .registry = &resources, .source = fixture.source(), .request_parent = frame2.parent() };
    const snapshot = try owner2.captureSnapshot(Fixture.scope, capture, 1, deadline);
    lease2.close();
    frame2.poisoned = true;
    id.sequence = 3;
    var lease3 = (try workers.begin(Input.request(id, .read), binding)).started;
    defer lease3.close();
    var frame3: Frame = .{ .lease = &lease3 };
    const owner3: Owner = .{ .registry = &resources, .source = fixture.source(), .request_parent = frame3.parent() };
    const cursor = try owner3.openSnapshot(Fixture.scope, snapshot, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline);
    lease3.close();
    frame3.poisoned = true;
    const cursor_id = id;
    id.sequence = 4;
    var lease4 = (try workers.begin(Input.request(id, .read), binding)).started;
    defer lease4.close();
    var frame4: Frame = .{ .lease = &lease4 };
    const owner4: Owner = .{ .registry = &resources, .source = fixture.source(), .request_parent = frame4.parent() };
    var page = try owner4.next(a, Fixture.scope, cursor, 0, 1);
    page.deinit();
    lease4.close();
    frame4.poisoned = true;
    id.sequence = 5;
    id.operation = 72;
    var lease5 = (try workers.begin(Input.request(id, .read), binding)).started;
    defer lease5.close();
    var frame5: Frame = .{ .lease = &lease5 };
    const other: Owner = .{ .registry = &resources, .source = fixture.source(), .request_parent = frame5.parent() };
    const legacy: Owner = .{ .registry = &resources, .source = fixture.source() };
    try std.testing.expectError(error.RetainedReadScopeChanged, legacy.validateCapture(Fixture.scope, capture));
    try std.testing.expectError(error.RetainedReadScopeChanged, other.validateCapture(Fixture.scope, capture));
    try std.testing.expectError(error.RetainedReadScopeChanged, other.captureSnapshot(Fixture.scope, capture, 1, deadline));
    try std.testing.expectError(error.RetainedReadScopeChanged, other.open(Fixture.scope, capture, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline));
    try std.testing.expectError(error.RetainedReadScopeChanged, other.openSnapshot(Fixture.scope, snapshot, 1, "", "", .{ .relational_query = .{ .fields = &.{} } }, deadline));
    try std.testing.expectError(error.RetainedReadScopeChanged, other.next(a, Fixture.scope, cursor, 1, 1));
    try std.testing.expectError(error.RetainedReadScopeChanged, other.rangeProofs(a, Fixture.scope, cursor));
    try std.testing.expectError(error.RetainedReadScopeChanged, other.normalize(a, Fixture.scope, cursor, &.{}));
    var forged = other;
    forged.request_parent.?.id = capture_id;
    try std.testing.expectError(error.InvalidRemoteAttempt, forged.next(a, Fixture.scope, cursor, 1, 1));
    try std.testing.expectEqual(@as(usize, 1), fixture.snapshots);
    try std.testing.expectEqual(@as(usize, 1), fixture.opens);
    try std.testing.expectEqual(@as(usize, 1), fixture.pages);
    // A signed operation cannot silently adopt an unowned legacy capture.
    const legacy_capture = (try legacy.capture(Fixture.scope, 1, Fixture.route, "rows", deadline)).?;
    try std.testing.expectError(error.RetainedReadScopeChanged, other.captureSnapshot(Fixture.scope, legacy_capture, 1, deadline));
    try std.testing.expectEqual(registry_mod.Retirement.retired, try resources.closeAcknowledged(legacy_capture, Fixture.scope));
    const initial_closed = fixture.closed.load(.acquire);
    lease5.close();
    frame5.poisoned = true;
    try std.testing.expectError(error.RemoteWorkerBusy, workers.deinit());
    const fence = Input.request(.{ .coordinator = coordinator, .generation = 2, .operation = 0, .sequence = 0 }, .fence);
    try std.testing.expectError(error.RemoteFencePending, workers.fence(fence, binding));
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try workers.status(Input.request(cursor_id, .status), binding));
    // The native destructor is a real physical fence: while it is blocked,
    // neither the token ACK nor the durable worker generation may retire.
    const Close = struct {
        fn run(registry: *registry_mod.Registry, token: registry_mod.Token) !void {
            try std.testing.expectEqual(registry_mod.Retirement.retired, try registry.closeAcknowledged(token, Fixture.scope));
        }
    };
    var closers = std.Io.Threaded.init(a, .{ .concurrent_limit = .limited(1) });
    defer closers.deinit();
    fixture.block_once.store(true, .release);
    var closing = try closers.io().concurrent(Close.run, .{ &resources, cursor });
    var closing_live = true;
    defer if (closing_live) {
        fixture.release.set(std.testing.io);
        closing.await(closers.io()) catch {};
    };
    try fixture.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(registry_mod.Retirement.pending, try resources.closeAcknowledged(cursor, Fixture.scope));
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try workers.status(Input.request(cursor_id, .status), binding));
    try std.testing.expectEqual(@as(usize, 3 * 1024), workers.snapshot().bytes);
    // Independent cleanup progresses while another native destructor blocks.
    try std.testing.expectEqual(registry_mod.Retirement.retired, try resources.closeAcknowledged(snapshot, Fixture.scope));
    try std.testing.expectEqual(@as(usize, 2 * 1024), workers.snapshot().bytes);
    try std.testing.expectError(error.RemoteFencePending, workers.fence(fence, binding));
    fixture.release.set(std.testing.io);
    try closing.await(closers.io());
    closing_live = false;
    try std.testing.expectEqual(@as(usize, 1024), workers.snapshot().bytes);
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try workers.status(Input.request(capture_id, .status), binding));
    try std.testing.expectEqual(registry_mod.Retirement.retired, try resources.closeAcknowledged(capture, Fixture.scope));
    _ = try workers.fence(fence, binding);
    try std.testing.expectEqual(@as(usize, 0), workers.snapshot().attempts);
    try std.testing.expectEqual(@as(usize, 0), workers.snapshot().bytes);
    try std.testing.expectEqual(initial_closed + 3, fixture.closed.load(.acquire));
    // A late real worker cancellation retires an unreturned native result,
    // preserving the worker's charge until the request frame itself closes.
    id = .{ .coordinator = coordinator, .generation = 2, .operation = 71, .sequence = 1 };
    var late_lease = (try workers.begin(Input.request(id, .read), binding)).started;
    defer late_lease.close();
    var late_frame: Frame = .{ .lease = &late_lease };
    fixture.cancel_on_capture = Input.request(id, .cancel);
    const late: Owner = .{ .registry = &resources, .source = fixture.source(), .request_parent = late_frame.parent() };
    try std.testing.expectError(error.Canceled, late.capture(Fixture.scope, 1, Fixture.route, "rows", deadline));
    try std.testing.expectEqual(initial_closed + 4, fixture.closed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), workers.snapshot().references);
    late_lease.close();
    late_frame.poisoned = true;
    try std.testing.expectEqual(@as(usize, 0), workers.snapshot().attempts);
}
