// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native retained-relation snapshot. One immutable source cut covers every
//! page and the initial replay cursor. A caller must bound session lifetime and
//! retain its native owner lease until close; no session pins source TTL.
const std = @import("std");
const series = @import("retained_series.zig");
const capture = @import("retained_series_capture.zig");
const control = @import("retained_series_control.zig");
const store = @import("docstore.zig");
const A = std.mem.Allocator;
pub const Query = @import("retained_series_read_contract.zig").Query;
pub const Control = @import("retained_series_read_contract.zig").Control;
pub const memory_limit = 32 * 1024 * 1024;
const Budget = @import("antfly_platform").allocator.BoundedAllocator;
pub const Request = @import("retained_series_read_contract.zig").Request;
pub const Session = struct {
    alloc: A,
    memory: Budget,
    definition_arena: std.heap.ArenaAllocator,
    page_arena: std.heap.ArenaAllocator,
    txn: store.DocStore.Txn,
    spec: series.Spec,
    query: Query,
    root_incarnation: u128,
    expires_at_ns: u64,
    cursor: series.Cursor,
    after: ?[]u8 = null,
    done: bool = false,
    row_policy_lease: ?@import("db/row_policy_gate.zig").Gate.Lease = null,

    /// Ownership of txn transfers on invocation, including failure paths.
    pub fn open(a: A, txn_owned: store.DocStore.Txn, query: Query, root_incarnation: u128) !*Session {
        return openControlled(a, txn_owned, query, root_incarnation, .{});
    }
    pub fn openControlled(a: A, txn_owned: store.DocStore.Txn, query: Query, root_incarnation: u128, operation: Control) !*Session {
        var txn = txn_owned;
        errdefer txn.abort();
        try operation.check();
        const self = try a.create(Session);
        errdefer a.destroy(self);
        self.memory = .{ .backing = a, .limit = memory_limit };
        self.alloc = self.memory.allocator();
        return self.initialize(&txn, query, root_incarnation, operation) catch |err| {
            std.debug.assert(self.memory.live == 0);
            return if (err == error.OutOfMemory and self.memory.denied) error.DatasetPageBudgetExceeded else err;
        };
    }
    fn initialize(self: *Session, snapshot: *store.DocStore.Txn, query: Query, root_incarnation: u128, operation: Control) !*Session {
        var arena = std.heap.ArenaAllocator.init(self.alloc);
        errdefer arena.deinit();
        const temp = arena.allocator();
        var retained_query = query;
        if (query.dimensions_json) |dimensions| retained_query.dimensions_json = try temp.dupe(u8, dimensions);
        const bindings = try capture.load(temp, snapshot);
        const spec = for (bindings) |binding| {
            if (std.mem.eql(u8, &binding.spec.id, &query.fence.id) and binding.spec.generation == query.fence.generation) break binding.spec;
        } else return error.RetainedSeriesDefinitionMismatch;
        try control.requireFence(query.fence, spec);
        const engine = series.Engine{ .alloc = self.alloc, .spec = spec, .read_control = operation };
        const cursor = try engine.snapshotCursor(snapshot);
        // Validation scratch belongs to this call, not to the retained binding.
        var validation = std.heap.ArenaAllocator.init(self.alloc);
        defer validation.deinit();
        if (query.archive) |archive| {
            _ = try engine.scanArchive(validation.allocator(), snapshot, archive, null, 1);
        } else {
            _ = try engine.scanWhere(validation.allocator(), snapshot, query.start_ns, query.end_ns, query.admitted_ns, null, 1, query.dimensions_json, query.after);
        }
        try operation.check();
        self.definition_arena = arena;
        self.page_arena = std.heap.ArenaAllocator.init(self.alloc);
        self.txn = snapshot.*;
        self.spec = spec;
        self.query = retained_query;
        self.root_incarnation = root_incarnation;
        self.expires_at_ns = @import("antfly_platform").time.monotonicNs() +| (60 * std.time.ns_per_s);
        self.cursor = cursor;
        self.after = null;
        self.done = false;
        self.row_policy_lease = null;
        return self;
    }
    /// Rows borrow the page arena until the next call. Errors close admission
    /// at the host; callers must not retry an ambiguously delivered page.
    pub fn next(self: *Session, limit: usize) !series.Page {
        return self.nextControlled(limit, .{});
    }
    pub fn nextControlled(self: *Session, limit: usize, input: Control) !series.Page {
        return self.nextInner(limit, input) catch |err| {
            return if (err == error.OutOfMemory and self.memory.denied) error.DatasetPageBudgetExceeded else err;
        };
    }
    fn nextInner(self: *Session, limit: usize, input: Control) !series.Page {
        var operation = input;
        operation.deadline_ns = @min(operation.deadline_ns, self.expires_at_ns);
        if (@import("antfly_platform").time.monotonicNs() >= self.expires_at_ns) return error.RetainedSeriesSnapshotExpired;
        try operation.check();
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
        if (limit == 0 or limit > series.max_changes) return error.RetainedSeriesBatchTooLarge;
        _ = self.page_arena.reset(.free_all);
        if (self.done) return .{ .rows = &.{}, .after = null, .cursor = self.cursor };
        const engine = series.Engine{ .alloc = self.alloc, .spec = self.spec, .read_control = operation };
        const page = if (self.query.archive) |archive|
            try engine.scanArchive(self.page_arena.allocator(), &self.txn, archive, self.after, limit)
        else
            try engine.scanWhere(self.page_arena.allocator(), &self.txn, self.query.start_ns, self.query.end_ns, self.query.admitted_ns, self.after, limit, self.query.dimensions_json, self.query.after);
        if (!std.meta.eql(page.cursor, self.cursor)) return error.RetainedSeriesResetRequired;
        const after = if (page.after) |value| try self.alloc.dupe(u8, value) else null;
        if (self.after) |value| self.alloc.free(value);
        self.after = after;
        self.done = page.after == null;
        if (self.row_policy_lease) |*lease| try lease.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s)));
        return page;
    }
    /// Serialization and the outgoing ABI copy overlap inside one admission.
    pub fn nextJson(self: *Session, limit: usize, operation: Control) ![]u8 {
        return self.encodeNext(limit, operation) catch |err| {
            if ((err == error.OutOfMemory or err == error.WriteFailed) and self.memory.denied) return error.DatasetPageBudgetExceeded;
            return if (err == error.WriteFailed) error.OutOfMemory else err;
        };
    }
    fn encodeNext(self: *Session, limit: usize, input: Control) ![]u8 {
        var operation = input;
        operation.deadline_ns = @min(operation.deadline_ns, self.expires_at_ns);
        const page = try self.nextControlled(limit, operation);
        var output: std.Io.Writer.Allocating = .init(self.alloc);
        defer output.deinit();
        var json: std.json.Stringify = .{ .writer = &output.writer };
        try json.beginObject();
        try json.objectField("root_incarnation");
        var root: [40]u8 = undefined;
        try json.write(try std.fmt.bufPrint(&root, "{d}", .{self.root_incarnation}));
        try json.objectField("page");
        try json.beginObject();
        inline for (@typeInfo(series.Page).@"struct".field_names) |name| {
            try operation.check();
            try json.objectField(name);
            if (comptime std.mem.eql(u8, name, "rows")) {
                try json.beginArray();
                for (page.rows) |row| {
                    try operation.check();
                    try json.write(row);
                }
                try json.endArray();
            } else try json.write(@field(page, name));
        }
        try json.endObject();
        try json.endObject();
        try operation.check();
        const bytes = output.written();
        if (bytes.len > self.memory.limit - self.memory.live) return error.DatasetPageBudgetExceeded;
        return self.memory.backing.dupe(u8, bytes);
    }
    pub fn close(self: *Session) void {
        const a = self.memory.backing;
        self.txn.abort();
        if (self.row_policy_lease) |*lease| lease.release();
        if (self.after) |value| self.alloc.free(value);
        self.page_arena.deinit();
        self.definition_arena.deinit();
        std.debug.assert(self.memory.live == 0);
        a.destroy(self);
    }
};

fn seededStore(backend: *@import("mem_backend.zig").Backend, count: usize) !store.DocStore {
    const a = std.testing.allocator;
    var db = try store.DocStore.openRuntime(a, try backend.runtimeStore(a, .{}));
    errdefer db.close();
    var txn = try db.beginWriteTxn();
    errdefer txn.abort();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const binding: capture.Binding = .{ .spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .retention_ns = 100_000_000, .laws = &.{.count} }, .timestamp = "ts", .fields = &.{null} };
    try capture.install(arena.allocator(), &txn, binding, .{ .sequence = 0, .admitted_ns = 0 }, 0);
    const engine = series.Engine{ .alloc = a, .spec = binding.spec };
    try engine.backfill(&txn, "", &.{}, true);
    var first: usize = 0;
    var sequence: u64 = 0;
    while (first < count) {
        _ = arena.reset(.free_all);
        const temp = arena.allocator();
        const end = @min(count, first + 128);
        const changes = try temp.alloc(series.Change, end - first);
        for (changes, first..) |*change, i| change.* = .{ .key = try std.fmt.allocPrint(temp, "row-{d}", .{i}), .after = .{ .timestamp_ns = @intCast(i * 10), .dimensions = "[\"present\"]", .values = &.{null} } };
        sequence += 1;
        try engine.apply(&txn, sequence, 0, .mutation, changes);
        first = end;
    }
    try txn.commit();
    return db;
}
fn testQuery(dimensions: ?[]const u8) Query {
    return .{ .fence = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .partition = @splat(3), .incarnation = 1 }, .start_ns = 0, .end_ns = 1_100_000, .admitted_ns = 0, .dimensions_json = dimensions };
}

test "retained series native reader sparse seeks retain bounded memory and interrupt within scans" {
    const a = std.testing.allocator;
    var backend = @import("mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var db = try seededStore(&backend, 100_000);
    defer db.close();
    {
        const reader = try Session.open(a, try db.beginReadTxn(), testQuery("[\"absent\"]"), 7);
        defer reader.close();
        const definition_bytes = reader.memory.live;
        const bytes = try reader.nextJson(1, .{});
        defer a.free(bytes);
        const decoded = try std.json.parseFromSlice(struct { page: series.Page }, a, bytes, .{ .ignore_unknown_fields = true });
        defer decoded.deinit();
        try std.testing.expectEqual(@as(usize, 0), decoded.value.page.rows.len);
        // Neither validation nor one hundred thousand unsuccessful seeks may
        // accumulate heap keys in the retained definition/page arenas.
        try std.testing.expect(definition_bytes < 64 * 1024);
        try std.testing.expect(reader.memory.peak < 256 * 1024);
    }
    const Probe = struct {
        calls: usize = 0,
        stop_at: usize = 8,
        expire_at: ?u64 = null,
        fn canceled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls != self.stop_at) return false;
            if (self.expire_at) |deadline| {
                while (@import("antfly_platform").time.monotonicNs() < deadline) std.atomic.spinLoopHint();
                return false;
            }
            return true;
        }
    };
    {
        const reader = try Session.open(a, try db.beginReadTxn(), testQuery(null), 7);
        defer reader.close();
        var encoding: Probe = .{ .stop_at = 150 };
        try std.testing.expectError(error.Canceled, reader.nextJson(128, .{ .cancellation_ctx = &encoding, .is_cancelled_fn = Probe.canceled }));
        try std.testing.expectEqual(@as(usize, 150), encoding.calls);
        // The page was read successfully; cancellation occurred while writing
        // its rows, and the failed response must not leave any outgoing copy.
        try std.testing.expect(reader.after != null);
    }
    var opening: Probe = .{};
    try std.testing.expectError(error.Canceled, Session.openControlled(a, try db.beginReadTxn(), testQuery("[\"absent\"]"), 7, .{ .cancellation_ctx = &opening, .is_cancelled_fn = Probe.canceled }));
    try std.testing.expectEqual(@as(usize, 8), opening.calls);
    {
        const reader = try Session.open(a, try db.beginReadTxn(), testQuery(null), 7);
        defer reader.close();
        var paging: Probe = .{};
        try std.testing.expectError(error.Canceled, reader.nextControlled(128, .{ .cancellation_ctx = &paging, .is_cancelled_fn = Probe.canceled }));
        try std.testing.expectEqual(@as(usize, 8), paging.calls);
        try std.testing.expectEqual(@as(?[]u8, null), reader.after);
    }
    {
        const reader = try Session.open(a, try db.beginReadTxn(), testQuery(null), 7);
        defer reader.close();
        const deadline = @import("antfly_platform").time.monotonicNs() + 250 * std.time.ns_per_ms;
        var paging: Probe = .{ .expire_at = deadline };
        try std.testing.expectError(error.Timeout, reader.nextControlled(128, .{ .deadline_ns = deadline, .cancellation_ctx = &paging, .is_cancelled_fn = Probe.canceled }));
        try std.testing.expectEqual(@as(usize, 8), paging.calls);
    }
}

fn allocationScenario(backing: A) !void {
    const a = std.testing.allocator;
    var backend = @import("mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var db = try seededStore(&backend, 2);
    defer db.close();
    const reader = try Session.open(backing, try db.beginReadTxn(), testQuery(null), 7);
    defer reader.close();
    const bytes = try reader.nextJson(1, .{});
    defer backing.free(bytes);
}
test "retained series native reader allocation failures release snapshots arenas and output" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, allocationScenario, .{});
    const a = std.testing.allocator;
    var backend = @import("mem_backend.zig").Backend.init(a, .{});
    defer backend.close();
    var db = try seededStore(&backend, 2);
    defer db.close();
    const reader = try Session.open(a, try db.beginReadTxn(), testQuery(null), 7);
    defer reader.close();
    reader.memory.limit = reader.memory.live + 16;
    try std.testing.expectError(error.DatasetPageBudgetExceeded, reader.nextJson(128, .{}));
}
