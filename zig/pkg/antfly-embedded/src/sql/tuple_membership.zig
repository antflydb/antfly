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

//! NULL-aware row membership. A shared-prefix hash trie retains each distinct
//! right-hand tuple once. Exact non-NULL hits take one lookup per column; only
//! ambiguous NULL probes walk compatible branches. No 2^arity mask expansion,
//! per-probe allocation, serialized JSON keys, or per-outer-row source scan.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const A = std.mem.Allocator;

pub const Truth = enum { yes, no, unknown };
pub fn negate(value: Truth) Truth {
    return switch (value) {
        .yes => .no,
        .no => .yes,
        .unknown => .unknown,
    };
}
pub const Limits = struct {
    rows: usize = 1_000_000,
    bytes: usize = 64 * 1024 * 1024,
    work: usize = 16_000_000,
};
pub const Checkpoint = struct {
    ptr: *anyopaque,
    call: *const fn (*anyopaque) anyerror!void,
};

pub const Index = struct {
    const none = std.math.maxInt(usize);
    const Edge = struct { parent: usize, hash: u64 };
    const Node = struct {
        value: Datum = .{},
        first: usize = none,
        next: usize = none,
        null_child: usize = none,
        collision: usize = none,
    };
    backing: A,
    budget: @import("memory_budget.zig"),
    arena: std.heap.ArenaAllocator,
    nodes: std.ArrayList(Node) = .empty,
    edges: std.AutoHashMapUnmanaged(Edge, usize) = .empty,
    arity: usize,
    limits: Limits,
    checkpoint: ?Checkpoint,
    rows: usize = 0,
    work: usize = 0,
    sealed: bool = false,
    failed: bool = false,

    pub fn create(a: A, arity: usize, limits: Limits, checkpoint: ?Checkpoint) !*Index {
        if (arity == 0 or arity > 256) return error.InvalidSqlParameters;
        if (limits.bytes <= @sizeOf(Index)) return error.SqlProgramLimitExceeded;
        const self = try a.create(Index);
        self.* = .{ .backing = a, .budget = .{ .backing = a, .limit = limits.bytes - @sizeOf(Index) }, .arena = undefined, .arity = arity, .limits = limits, .checkpoint = checkpoint };
        self.arena = .init(self.budget.allocator());
        errdefer self.deinit();
        self.nodes.append(self.budget.allocator(), .{}) catch |err| return self.failure(err);
        return self;
    }

    pub fn deinit(self: *Index) void {
        const a = self.backing;
        self.edges.deinit(self.budget.allocator());
        self.nodes.deinit(self.budget.allocator());
        self.arena.deinit();
        std.debug.assert(self.budget.live == 0);
        a.destroy(self);
    }

    fn failure(self: *Index, err: anyerror) anyerror {
        self.failed = true;
        return if (err == error.OutOfMemory and self.budget.exhausted) error.SqlProgramLimitExceeded else err;
    }

    fn step(self: *Index) !void {
        if (self.work >= self.limits.work) return error.SqlProgramLimitExceeded;
        if (self.work % 64 == 0) if (self.checkpoint) |check| try check.call(check.ptr);
        self.work += 1;
    }

    fn find(self: *Index, parent: usize, value: Datum, hash: u64) !usize {
        if (value.sql_null) return self.nodes.items[parent].null_child;
        var candidate = self.edges.get(.{ .parent = parent, .hash = hash }) orelse return none;
        while (candidate != none) {
            try self.step();
            if (try scalar.compareDatums(self.nodes.items[candidate].value, value) == .eq) return candidate;
            candidate = self.nodes.items[candidate].collision;
        }
        return none;
    }

    /// The binder must establish one common typed column contract for both
    /// sides, exactly as for ordinary hash joins. Values are cloned before the
    /// source page can retire. Any failed build poisons the index: a partially
    /// retained prefix can never be mistaken for an admitted tuple.
    pub fn add(self: *Index, values: []const Datum) !void {
        if (self.failed or self.sealed or values.len != self.arity) return error.InvalidSqlBackendResponse;
        self.addInner(values) catch |err| return self.failure(err);
    }

    fn addInner(self: *Index, values: []const Datum) !void {
        if (self.rows >= self.limits.rows) return error.SqlProgramLimitExceeded;
        var parent: usize = 0;
        for (values) |value| {
            try self.step();
            if (value.patterns != null) return error.SqlTypeMismatch;
            const hash = try scalar.semanticHashDatum(value);
            var child = try self.find(parent, value, hash);
            if (child == none) {
                const owned = if (value.sql_null) Datum{} else try @import("operators.zig").cloneDatum(self.arena.allocator(), value);
                child = self.nodes.items.len;
                const edge: Edge = .{ .parent = parent, .hash = hash };
                const collision = if (value.sql_null) none else self.edges.get(edge) orelse none;
                try self.nodes.append(self.budget.allocator(), .{ .value = owned, .next = self.nodes.items[parent].first, .collision = collision });
                self.nodes.items[parent].first = child;
                if (value.sql_null) self.nodes.items[parent].null_child = child else try self.edges.put(self.budget.allocator(), edge, child);
            }
            parent = child;
        }
        self.rows += 1;
    }

    pub fn seal(self: *Index) !void {
        if (self.failed) return error.InvalidSqlBackendResponse;
        self.sealed = true;
    }

    /// SQL NOT IN negates yes/no but retains unknown. An empty right side is
    /// false even for an all-NULL left tuple. A definitive mismatch in any
    /// position defeats NULLs elsewhere in that candidate row.
    pub fn probe(self: *Index, values: []const Datum) !Truth {
        if (self.failed or !self.sealed or values.len != self.arity) return error.InvalidSqlBackendResponse;
        try self.step();
        if (self.rows == 0) return .no;
        var hashes: [256]u64 = undefined;
        var all_nonnull = true;
        for (values, hashes[0..self.arity]) |value, *hash| {
            try self.step();
            if (value.patterns != null) return error.SqlTypeMismatch;
            hash.* = try scalar.semanticHashDatum(value);
            all_nonnull = all_nonnull and !value.sql_null;
        }
        if (all_nonnull) {
            var parent: usize = 0;
            for (values, hashes[0..self.arity]) |value, hash| {
                parent = try self.find(parent, value, hash);
                if (parent == none) break;
            } else return .yes;
        }
        // Iterative depth-first compatibility search. At a non-NULL probe
        // position only its exact edge and the NULL edge are candidates. A
        // NULL position accepts all edges, but subsequent concrete positions
        // still prune incompatible prefixes. Stack size depends only on arity.
        const Frame = struct { parent: usize, next: usize, exact_done: bool = false };
        var stack: [256]Frame = undefined;
        stack[0] = .{ .parent = 0, .next = self.nodes.items[0].first };
        var depth: usize = 0;
        while (true) {
            try self.step();
            const frame = &stack[depth];
            const value = values[depth];
            var child: usize = none;
            if (value.sql_null) {
                child = frame.next;
                if (child != none) frame.next = self.nodes.items[child].next;
            } else if (!frame.exact_done) {
                frame.exact_done = true;
                child = try self.find(frame.parent, value, hashes[depth]);
                if (child == none) {
                    child = self.nodes.items[frame.parent].null_child;
                    frame.next = none;
                } else frame.next = self.nodes.items[frame.parent].null_child;
            } else {
                child = frame.next;
                frame.next = none;
            }
            if (child == none) {
                if (depth == 0) return .no;
                depth -= 1;
                continue;
            }
            if (depth + 1 == self.arity) return .unknown;
            depth += 1;
            stack[depth] = .{ .parent = child, .next = self.nodes.items[child].first };
        }
    }
};

test "SQL tuple membership keeps NULL ambiguity distinct from definite mismatch" {
    const a = std.testing.allocator;
    const index = try Index.create(a, 2, .{}, null);
    defer index.deinit();
    const one = Datum.json(.{ .integer = 1 });
    const two = Datum.json(.{ .integer = 2 });
    try index.add(&.{ one, two });
    try index.add(&.{ two, .{} });
    try index.add(&.{ one, two });
    try index.seal();
    try std.testing.expectEqual(Truth.yes, try index.probe(&.{ one, two }));
    try std.testing.expectEqual(Truth.unknown, try index.probe(&.{ two, one }));
    try std.testing.expectEqual(Truth.no, try index.probe(&.{ one, one }));
    try std.testing.expectEqual(Truth.unknown, try index.probe(&.{ .{}, two }));
    try std.testing.expectEqual(Truth.unknown, try index.probe(&.{ .{}, .{} }));
    const empty = try Index.create(a, 2, .{}, null);
    defer empty.deinit();
    try empty.seal();
    try std.testing.expectEqual(Truth.no, try empty.probe(&.{ .{}, .{} }));
}

test "SQL tuple membership owns build payloads and unwinds every allocation failure" {
    const Fixture = struct {
        fn run(a: A) !void {
            const index = try Index.create(a, 2, .{}, null);
            defer index.deinit();
            var text = [_]u8{ 'o', 'l', 'd' };
            try index.add(&.{ Datum.json(.{ .string = &text }), Datum.json(.{ .integer = 9007199254740993 }) });
            @memset(&text, 'x');
            try index.add(&.{ .{}, Datum.json(.{ .integer = 2 }) });
            try index.seal();
            try std.testing.expectEqual(Truth.yes, try index.probe(&.{ Datum.json(.{ .string = "old" }), Datum.json(.{ .integer = 9007199254740993 }) }));
            try std.testing.expectEqual(Truth.no, try index.probe(&.{ Datum.json(.{ .string = "old" }), Datum.json(.{ .integer = 9007199254740992 }) }));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL tuple membership has linear exact-hit work and bounded NULL search" {
    const index = try Index.create(std.testing.allocator, 2, .{}, null);
    defer index.deinit();
    for (0..4096) |n| try index.add(&.{ Datum.json(.{ .integer = @intCast(n) }), Datum.json(.{ .integer = @intCast(n + 1) }) });
    try index.seal();
    const start = index.work;
    for (0..4096) |n| try std.testing.expectEqual(Truth.yes, try index.probe(&.{ Datum.json(.{ .integer = @intCast(n) }), Datum.json(.{ .integer = @intCast(n + 1) }) }));
    try std.testing.expect(index.work - start <= 4096 * 6);
    index.limits.work = index.work + 3;
    try std.testing.expectError(error.SqlProgramLimitExceeded, index.probe(&.{ .{}, .{} }));
}

test "SQL tuple membership fails closed after quota exhaustion and cancellation" {
    const Canceled = struct {
        fn checkpoint(_: *anyopaque) !void {
            return error.Canceled;
        }
    };
    var sentinel: u8 = 0;
    const canceled = try Index.create(std.testing.allocator, 1, .{}, .{ .ptr = &sentinel, .call = Canceled.checkpoint });
    defer canceled.deinit();
    try std.testing.expectError(error.Canceled, canceled.add(&.{.{}}));
    try std.testing.expectError(error.InvalidSqlBackendResponse, canceled.seal());
    const limited = try Index.create(std.testing.allocator, 1, .{ .rows = 1 }, null);
    defer limited.deinit();
    try limited.add(&.{.{}});
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.add(&.{.{}}));
    try std.testing.expectError(error.InvalidSqlBackendResponse, limited.seal());
}

test "SQL tuple membership matches independent PostgreSQL row IN and NOT IN truth tables" {
    const Reference = struct { cases: []const struct { width: usize, rhs: []const usize, truth: []const u8 } };
    const parsed = try std.json.parseFromSlice(Reference, std.testing.allocator, @embedFile("fixtures/sql_tuple_reference.json"), .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const Domain = struct {
        fn tuple(out: []Datum, index: usize) void {
            var value = index;
            var position = out.len;
            while (position != 0) {
                position -= 1;
                out[position] = if (value % 3 == 0) .{} else Datum.json(.{ .integer = @intCast(value % 3) });
                value /= 3;
            }
            std.debug.assert(value == 0);
        }
    };
    var checks: usize = 0;
    for (parsed.value.cases) |case| {
        try std.testing.expect(case.width >= 1 and case.width <= 3);
        const index = try Index.create(std.testing.allocator, case.width, .{}, null);
        defer index.deinit();
        var row: [3]Datum = undefined;
        for (case.rhs) |source| {
            Domain.tuple(row[0..case.width], source);
            try index.add(row[0..case.width]);
        }
        try index.seal();
        for (case.truth, 0..) |expected, probe| {
            Domain.tuple(row[0..case.width], probe);
            const actual = try index.probe(row[0..case.width]);
            const want: Truth = switch (expected) {
                'y' => .yes,
                'n' => .no,
                'u' => .unknown,
                else => return error.InvalidPostgresReference,
            };
            try std.testing.expectEqual(want, actual);
            try std.testing.expectEqual(negate(want), negate(actual));
            checks += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 110), parsed.value.cases.len);
    try std.testing.expectEqual(@as(usize, 1740), checks);
}

test "SQL tuple membership distinguishes JSONB null and owns typed array keys" {
    const a = std.testing.allocator;
    const index = try Index.create(a, 2, .{}, null);
    defer index.deinit();
    var elements = [_]Datum{ Datum.json(.{ .integer = 9007199254740993 }), .{} };
    var dimensions = [_]@import("array_value.zig").Dimension{.{ .length = 2, .lower = -1 }};
    var array = try @import("array_value.zig").Value.init(.int64, &dimensions, &elements, .{});
    try index.add(&.{ Datum.json(.null), Datum.typedArray(&array) });
    try index.seal();
    try std.testing.expectEqual(Truth.yes, try index.probe(&.{ Datum.json(.null), Datum.typedArray(&array) }));
    try std.testing.expectEqual(Truth.unknown, try index.probe(&.{ .{}, Datum.typedArray(&array) }));
    elements[0] = Datum.json(.{ .integer = 9007199254740992 });
    try std.testing.expectEqual(Truth.no, try index.probe(&.{ Datum.json(.null), Datum.typedArray(&array) }));
    elements[0] = Datum.json(.{ .integer = 9007199254740993 });
    dimensions[0].lower = 1;
    try std.testing.expectEqual(Truth.no, try index.probe(&.{ Datum.json(.null), Datum.typedArray(&array) }));
}

test "SQL tuple membership admits maximum width without mask expansion and bounds retained bytes" {
    const a = std.testing.allocator;
    const index = try Index.create(a, 256, .{}, null);
    defer index.deinit();
    const row: [256]Datum = @splat(.{});
    try index.add(&row);
    try index.seal();
    try std.testing.expectEqual(@as(usize, 257), index.nodes.items.len);
    try std.testing.expectEqual(Truth.unknown, try index.probe(&row));
    const limited = try Index.create(a, 1, .{ .bytes = 4096 }, null);
    defer limited.deinit();
    const text: [8192]u8 = @splat('x');
    try std.testing.expectError(error.SqlProgramLimitExceeded, limited.add(&.{Datum.json(.{ .string = &text })}));
    try std.testing.expectError(error.InvalidSqlBackendResponse, limited.seal());
}
