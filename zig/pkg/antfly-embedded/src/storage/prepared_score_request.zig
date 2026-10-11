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

//! Owned immutable graph request. Inputs are copied at construction; callers
//! keep the request alive during delivery and never mutate its owned plan.
const std = @import("std");
const Plan = @import("prepared_score_plan.zig").PreparedScorePlan;

pub const PreparedScoreRequest = struct {
    // The pointee is immutable outside this owner. Moving the handle is fine;
    // copying it does not transfer ownership and must not produce two deinits.
    handle: *const State,
    const Canonical = struct {
        bytes: []u8,
        keys: [][]const u8,
        identity: u64,
        retain_decoded: bool = false,
    };
    const State = struct { allocator: std.mem.Allocator, payload: union(enum) { full: Plan, canonical: Canonical } };

    /// A compact owner has no mutable input matching or output fanout. Only
    /// this constructor can create its sorted, unique canonical identity.
    fn initCanonical(a: std.mem.Allocator, prefix: []const u8, nodes: []const []const u8) !PreparedScoreRequest {
        const encoding = @import("internal_keys.zig");
        if (nodes.len == 0 or nodes.len > Plan.max_keys) return error.GraphMetricQueryBudgetExceeded;
        var size = try std.math.mul(usize, nodes.len, @sizeOf([]const u8));
        const descriptors = size;
        for (nodes, 0..) |node, i| {
            if (i != 0 and std.mem.order(u8, nodes[i - 1], node) != .lt) return error.InvalidBatch;
            size = std.math.add(usize, size, prefix.len) catch return error.GraphMetricQueryBudgetExceeded;
            size = std.math.add(usize, size, encoding.encodedComponentLen(node)) catch return error.GraphMetricQueryBudgetExceeded;
            if (size > Plan.max_bytes) return error.GraphMetricQueryBudgetExceeded;
        }
        const state = try a.create(State);
        errdefer a.destroy(state);
        const bytes = try a.alloc(u8, size - descriptors);
        errdefer a.free(bytes);
        const keys = try a.alloc([]const u8, nodes.len);
        var offset: usize = 0;
        for (nodes, keys) |node, *key| {
            const len = prefix.len + encoding.encodedComponentLen(node);
            key.* = bytes[offset..][0..len];
            @memcpy(bytes[offset..][0..prefix.len], prefix);
            _ = encoding.encodeComponent(bytes[offset + prefix.len ..][0 .. len - prefix.len], node);
            offset += len;
        }
        state.* = .{ .allocator = a, .payload = .{ .canonical = .{ .bytes = bytes, .keys = keys, .identity = @import("prepared_score_plan.zig").newCanonicalIdentity() } } };
        return .{ .handle = state };
    }

    pub fn readKeys(self: *const PreparedScoreRequest) []const []const u8 {
        return switch (self.handle.payload) {
            .full => |*p| p.read_keys,
            .canonical => |*c| c.keys,
        };
    }
    pub fn canonicalIdentity(self: *const PreparedScoreRequest) u64 {
        return switch (self.handle.payload) {
            .full => |*p| if (p.valid) p.identity else 0,
            .canonical => |*c| c.identity,
        };
    }
    pub fn retainDecoded(self: *const PreparedScoreRequest) bool {
        return switch (self.handle.payload) {
            .full => |*p| p.retain_decoded,
            .canonical => |*c| c.retain_decoded,
        };
    }
    pub fn ownedKeyBytes(self: *const PreparedScoreRequest) usize {
        return switch (self.handle.payload) {
            .full => |*p| p.storage.?.len,
            .canonical => |*c| c.bytes.len + c.keys.len * @sizeOf([]const u8),
        };
    }

    pub fn init(allocator: std.mem.Allocator, prefixes: []const ?[]const u8, nodes: []const []const u8) !PreparedScoreRequest {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{ .allocator = allocator, .payload = .{ .full = .{} } };
        errdefer state.payload.full.deinit();
        if (!try state.payload.full.prepare(allocator, prefixes, nodes)) return error.GraphMetricQueryBudgetExceeded;
        return .{ .handle = state };
    }
    /// Clone while the source scope is exclusively leased. Independent lifetime
    /// ownership preserves its canonical identity and any validated score cache.
    pub fn fromPlan(allocator: std.mem.Allocator, source: *const Plan) !PreparedScoreRequest {
        const state = try allocator.create(State);
        errdefer allocator.destroy(state);
        state.* = .{ .allocator = allocator, .payload = .{ .full = try source.clone(allocator) } };
        return .{ .handle = state };
    }
    /// Full plans are supplied by init/fromPlan and combinedRequest(). Compact
    /// chunks expose readKeys()/canonicalIdentity() instead of mutable mappings.
    pub fn plan(self: *const PreparedScoreRequest) *const Plan {
        return &self.handle.payload.full;
    }
    pub fn deinit(self: *PreparedScoreRequest) void {
        const state = @constCast(self.handle);
        const allocator = state.allocator;
        switch (state.payload) {
            .full => |*p| p.deinit(),
            .canonical => |*c| {
                allocator.free(c.keys);
                allocator.free(c.bytes);
            },
        }
        allocator.destroy(state);
        self.* = undefined;
    }
};

/// Query-owned compiled chunks. Each canonical batch obeys the existing 4096-key/256 KiB
/// limits; native workspace retention is unchanged. Total ownership is charged
/// to the caller allocator. Global duplicate fanout is compiled once, including
/// duplicates crossing chunk boundaries; no input bytes are compared on reads.
pub const PreparedScoreChunks = struct {
    handle: *const State,
    const State = struct {
        allocator: std.mem.Allocator,
        rows: usize,
        row_order_identity: bool,
        column_owners: []const usize,
        row_heads: []const usize,
        duplicate_next: []const usize,
        chunks: []const Chunk,
        combined: ?PreparedScoreRequest = null,
        unique_rows: usize = 0,
    };
    const absent = std.math.maxInt(usize);
    pub const Chunk = struct {
        request: PreparedScoreRequest,
        column: usize,
        first: usize,
    };

    pub fn init(a: std.mem.Allocator, prefixes: []const ?[]const u8, nodes: []const []const u8) !@This() {
        const state = try a.create(State);
        errdefer a.destroy(state);
        // Preserve the decoded cache and full fanout when the entire request
        // fits the bounded plan, regardless of the number of metric columns.
        const has_dependencies = blk: {
            for (prefixes) |prefix| if (prefix != null) break :blk true;
            break :blk false;
        };
        const combined = if (nodes.len != 0 and has_dependencies) PreparedScoreRequest.init(a, prefixes, nodes) catch |err| switch (err) {
            error.GraphMetricQueryBudgetExceeded => null,
            else => return err,
        } else null;
        if (combined) |request| {
            state.* = .{ .allocator = a, .rows = nodes.len, .row_order_identity = true, .column_owners = &.{}, .row_heads = &.{}, .duplicate_next = &.{}, .chunks = &.{}, .combined = request };
            return .{ .handle = state };
        }
        const owners = try a.alloc(usize, prefixes.len);
        errdefer a.free(owners);
        @memset(owners, absent);
        var prefix_map = std.StringHashMap(usize).init(a);
        defer prefix_map.deinit();
        var columns: std.ArrayList(usize) = .empty;
        defer columns.deinit(a);
        for (prefixes, 0..) |prefix, column| if (prefix) |value| {
            const entry = try prefix_map.getOrPut(value);
            if (!entry.found_existing) {
                entry.value_ptr.* = column;
                try columns.append(a, column);
            }
            owners[column] = entry.value_ptr.*;
        };
        if (nodes.len == 0 or columns.items.len == 0) {
            state.* = .{ .allocator = a, .rows = nodes.len, .row_order_identity = true, .column_owners = owners, .row_heads = &.{}, .duplicate_next = &.{}, .chunks = &.{} };
            return .{ .handle = state };
        }
        const next = try a.alloc(usize, nodes.len);
        errdefer a.free(next);
        @memset(next, absent);
        const order = try a.alloc(usize, nodes.len);
        defer a.free(order);
        for (order, 0..) |*row, i| row.* = i;
        std.mem.sort(usize, order, nodes, struct {
            fn less(inputs: []const []const u8, x: usize, y: usize) bool {
                const cmp = std.mem.order(u8, inputs[x], inputs[y]);
                return cmp == .lt or (cmp == .eq and x < y);
            }
        }.less);
        var unique: std.ArrayList([]const u8) = .empty;
        defer unique.deinit(a);
        var heads: std.ArrayList(usize) = .empty;
        errdefer heads.deinit(a);
        for (order) |row| {
            if (unique.items.len == 0 or !std.mem.eql(u8, unique.items[unique.items.len - 1], nodes[row])) {
                try unique.append(a, nodes[row]);
                try heads.append(a, absent);
            }
            const head = &heads.items[heads.items.len - 1];
            next[row] = head.*;
            head.* = row;
        }
        var chunks: std.ArrayList(Chunk) = .empty;
        errdefer {
            for (chunks.items) |*chunk| chunk.request.deinit();
            chunks.deinit(a);
        }
        for (columns.items) |column| {
            var first: usize = 0;
            while (first < unique.items.len) {
                var count = @min(Plan.max_keys, unique.items.len - first);
                const request = while (true) {
                    const candidate = PreparedScoreRequest.initCanonical(a, prefixes[column].?, unique.items[first..][0..count]) catch |err| switch (err) {
                        error.GraphMetricQueryBudgetExceeded => {
                            if (count == 1) return err;
                            count = @max(1, count / 2);
                            continue;
                        },
                        else => return err,
                    };
                    break candidate;
                };
                chunks.append(a, .{ .request = request, .column = column, .first = first }) catch |err| {
                    var owned = request;
                    owned.deinit();
                    return err;
                };
                first += count;
            }
        }
        // A duplicate-heavy logical request may exceed the mutable plan's
        // target budget yet deduplicate to one canonical batch. It can reuse
        // decoded scores just like a combined full plan.
        if (chunks.items.len == 1) @constCast(chunks.items[0].request.handle).payload.canonical.retain_decoded = true;
        var row_order_identity = heads.items.len == nodes.len;
        for (heads.items, 0..) |head, i| row_order_identity = row_order_identity and head == i;
        const unique_rows = heads.items.len;
        const owned_heads = try heads.toOwnedSlice(a);
        errdefer a.free(owned_heads);
        state.* = .{ .allocator = a, .rows = nodes.len, .row_order_identity = row_order_identity, .column_owners = owners, .row_heads = if (row_order_identity) &.{} else owned_heads, .duplicate_next = if (row_order_identity) &.{} else next, .chunks = try chunks.toOwnedSlice(a), .unique_rows = unique_rows };
        if (row_order_identity) {
            a.free(next);
            a.free(owned_heads);
        }
        return .{ .handle = state };
    }

    pub fn deinit(self: *@This()) void {
        const state = self.handle;
        if (@constCast(state).combined) |*request| request.deinit();
        for (@constCast(state.chunks)) |*chunk| chunk.request.deinit();
        const a = state.allocator;
        a.free(state.chunks);
        a.free(state.column_owners);
        a.free(state.row_heads);
        a.free(state.duplicate_next);
        a.destroy(state);
        self.* = undefined;
    }

    pub fn combinedRequest(self: *const @This()) ?*const PreparedScoreRequest {
        if (self.handle.combined) |*request| return request;
        return null;
    }

    pub fn chunkPlans(self: *const @This()) []const Chunk {
        return self.handle.chunks;
    }
    pub fn uniqueRowCount(self: *const @This()) usize {
        if (self.combinedRequest()) |request| {
            const plan = request.plan();
            var count: usize = 0;
            // Hash heads are unique when lookup construction completed. The
            // rare adversarial-collision plan falls back to its sorted targets.
            if (plan.row_lookup_valid) {
                for (plan.row_heads) |row| {
                    if (row != 0) count += 1;
                }
            } else {
                const column = for (plan.prefixes, 0..) |prefix, i| {
                    if (prefix != null) break i;
                } else return 0;
                var previous: ?[]const u8 = null;
                for (plan.targets) |target| if (target.column == column) {
                    const node = plan.nodes[target.row];
                    if (previous == null or !std.mem.eql(u8, previous.?, node)) count += 1;
                    previous = node;
                };
            }
            return count;
        }
        return self.handle.unique_rows;
    }
    pub fn validateColumns(self: *const @This(), columns: []const []?f64) !void {
        const count = if (self.combinedRequest()) |request| request.plan().prefixes.len else self.handle.column_owners.len;
        if (columns.len != count) return error.InvalidBatch;
        for (columns) |column| if (column.len != self.handle.rows) return error.InvalidBatch;
    }

    pub const Output = struct {
        owner: *const PreparedScoreChunks,
        chunk: *const Chunk,
        columns: []const []?f64,
        pub fn sink(self: *@This()) @import("backend_types.zig").CompleteScoreSink {
            return .{ .context = self, .write = write, .write_decoded = decoded };
        }
        fn write(ctx: *anyopaque, i: usize, score: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.owner.handle.row_order_identity) {
                self.columns[self.chunk.column][self.chunk.first + i] = score;
                return;
            }
            var row = self.owner.handle.row_heads[self.chunk.first + i];
            while (row != absent) {
                self.columns[self.chunk.column][row] = score;
                row = self.owner.handle.duplicate_next[row];
            }
        }
        fn decoded(ctx: *anyopaque, scores: []const f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.owner.handle.row_order_identity) {
                const column = self.columns[self.chunk.column][self.chunk.first..][0..scores.len];
                for (scores, column) |score, *value| value.* = if (std.math.isNan(score)) null else score;
            } else for (scores, 0..) |score, i| write(ctx, i, if (std.math.isNan(score)) null else score);
        }
    };

    pub fn clearAbsentColumns(self: *const @This(), columns: []const []?f64) void {
        for (self.handle.column_owners, columns) |owner, column| if (owner == absent) {
            @memset(column, null);
        };
    }

    pub fn finishColumns(self: *const @This(), columns: []const []?f64) void {
        for (self.handle.column_owners, 0..) |owner, column| {
            if (owner != absent and owner != column) @memcpy(columns[column], columns[owner]);
        }
    }
};

test "prepared score request owns immutable inputs and unique identity" {
    var node: [1]u8 = .{'a'};
    var prefix: [2]u8 = .{ 'p', '/' };
    var request = try PreparedScoreRequest.init(std.testing.allocator, &.{&prefix}, &.{&node});
    defer request.deinit();
    node[0] = 'b';
    prefix[0] = 'q';
    try std.testing.expectEqualStrings("a", request.plan().nodes[0]);
    try std.testing.expectEqualStrings("p/", request.plan().prefixes[0].?);
    var next = try PreparedScoreRequest.init(std.testing.allocator, &.{"p/"}, &.{"a"});
    defer next.deinit();
    try std.testing.expect(request.plan().identity != next.plan().identity);
}

fn allocationFailureCase(a: std.mem.Allocator) !void {
    var request = try PreparedScoreRequest.init(a, &.{ "p/", null, "p/" }, &.{ "a", "b", "a" });
    defer request.deinit();
    try std.testing.expect(request.plan().valid);
}

test "prepared score request allocation failures release ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailureCase, .{});
}

fn cloneFailureCase(a: std.mem.Allocator, source: *const Plan) !void {
    var request = try PreparedScoreRequest.fromPlan(a, source);
    defer request.deinit();
    try std.testing.expectEqual(source.identity, request.plan().identity);
}

test "prepared score request cloning allocation failures retain source ownership" {
    var source = Plan{};
    defer source.deinit();
    try std.testing.expect(try source.prepare(std.testing.allocator, &.{ "", null, "p/" }, &.{ "", "a", "a" }));
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cloneFailureCase, .{&source});
    try std.testing.expect(source.matches(&.{ "", null, "p/" }, &.{ "", "a", "a" }));
}

const StableAllocator = struct {
    backing: std.mem.Allocator,
    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
    }
    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        return self.backing.rawAlloc(len, alignment, ra);
    }
    fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(bytes, alignment, ra);
    }
};

fn chunkAllocationFailureCase(a: std.mem.Allocator) !void {
    var stable = StableAllocator{ .backing = a };
    var request = try PreparedScoreChunks.init(stable.allocator(), &.{ "p/", null, "p/", "q/" }, &.{ "b", "a", "b", "" });
    defer request.deinit();
    try std.testing.expect(request.combinedRequest() != null);
    try std.testing.expectEqual(@as(usize, 3), request.uniqueRowCount());
}

test "prepared score chunks allocation failures release partial ownership" {
    try chunkAllocationFailureCase(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, chunkAllocationFailureCase, .{});
}

test "prepared score chunks enforce byte bounds and skip empty dependencies" {
    const a = std.testing.allocator;
    var storage: [260][1024]u8 = undefined;
    var nodes: [260][]const u8 = undefined;
    for (&storage, &nodes, 0..) |*key, *node, i| {
        @memset(key, 'x');
        _ = try std.fmt.bufPrint(key[0..8], "{d:0>8}", .{i});
        node.* = key;
    }
    var compiled = try PreparedScoreChunks.init(a, &.{ "p/", "p/", null }, &nodes);
    defer compiled.deinit();
    try std.testing.expect(compiled.chunkPlans().len > 1);
    for (compiled.chunkPlans()) |chunk| {
        try std.testing.expect(chunk.request.ownedKeyBytes() <= Plan.max_bytes);
        try std.testing.expect(chunk.request.readKeys().len <= Plan.max_keys);
        try std.testing.expect(!chunk.request.retainDecoded());
    }
    var empty = try PreparedScoreChunks.init(a, &.{ null, null }, &nodes);
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.chunkPlans().len);
    try std.testing.expectEqual(@as(usize, 0), empty.handle.duplicate_next.len);
    var zero_rows = try PreparedScoreChunks.init(a, &.{"p/"}, &.{});
    defer zero_rows.deinit();
    try std.testing.expectEqual(@as(usize, 0), zero_rows.chunkPlans().len);
}

fn canonicalAllocationFailureCase(a: std.mem.Allocator) !void {
    var node: [3]u8 = .{ 'a', 0, 'b' };
    var prefix: [2]u8 = .{ 'p', '/' };
    var request = try PreparedScoreRequest.initCanonical(a, &prefix, &.{ "", &node, "z" });
    defer request.deinit();
    node[0] = 'x';
    prefix[0] = 'q';
    try std.testing.expectEqualStrings("p/a\x00\xffb\x00\x00", request.readKeys()[1]);
    try std.testing.expect(!request.retainDecoded());
    try std.testing.expect(request.canonicalIdentity() != 0);
    var full = try PreparedScoreRequest.init(a, &.{"p/"}, &.{"a"});
    defer full.deinit();
    try std.testing.expect(full.canonicalIdentity() != request.canonicalIdentity());
}

test "prepared score canonical ownership validates keys and allocation cleanup" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, canonicalAllocationFailureCase, .{});
    try std.testing.expectError(error.InvalidBatch, PreparedScoreRequest.initCanonical(std.testing.allocator, "p/", &.{ "b", "a" }));
    try std.testing.expectError(error.InvalidBatch, PreparedScoreRequest.initCanonical(std.testing.allocator, "p/", &.{ "a", "a" }));
}

fn largeChunkAllocationFailureCase(a: std.mem.Allocator) !void {
    var stable = StableAllocator{ .backing = a };
    var nodes: [4097][]const u8 = undefined;
    var storage: [4097][8]u8 = undefined;
    for (&storage, &nodes, 0..) |*bytes, *node, i| {
        _ = try std.fmt.bufPrint(bytes, "{d:0>8}", .{i});
        node.* = bytes;
    }
    var compiled = try PreparedScoreChunks.init(stable.allocator(), &.{"p/"}, &nodes);
    defer compiled.deinit();
    try std.testing.expect(compiled.combinedRequest() == null);
    try std.testing.expectEqual(@as(usize, 4097), compiled.uniqueRowCount());
    try std.testing.expectEqual(@as(usize, 0), compiled.handle.row_heads.len);
    try std.testing.expectEqual(@as(usize, 0), compiled.handle.duplicate_next.len);
    for (compiled.chunkPlans()) |chunk| try std.testing.expect(chunk.request.ownedKeyBytes() <= Plan.max_bytes);
}

test "prepared score compact chunks release partial ownership under allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, largeChunkAllocationFailureCase, .{});
}
