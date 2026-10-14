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

// Borrowed only while encoding; indexed inputs avoid a second descriptor slab.
const NodeView = struct {
    nodes: []const []const u8,
    order: ?[]const usize = null,
    fn len(self: @This()) usize {
        return if (self.order) |order| order.len else self.nodes.len;
    }
    fn at(self: @This(), i: usize) []const u8 {
        return self.nodes[if (self.order) |order| order[i] else i];
    }
    fn range(self: @This(), first: usize, count: usize) @This() {
        return if (self.order) |order| .{ .nodes = self.nodes, .order = order[first..][0..count] } else .{ .nodes = self.nodes[first..][0..count] };
    }
};

pub const PreparedScoreRequest = struct {
    // The pointee is immutable outside this owner. Moving the handle is fine;
    // copying it does not transfer ownership and must not produce two deinits.
    handle: *const State,
    pub const CanonicalTarget = struct {
        key_offset: u32,
        key_len: u32,
        column: u32,
        row: u32,
        fn key(self: @This(), bytes: []const u8) []const u8 {
            return bytes[self.key_offset..][0..self.key_len];
        }
    };
    const Canonical = struct {
        bytes: []u8,
        keys: [][]const u8,
        count: usize,
        targets: []CanonicalTarget = &.{},
        ends: []usize = &.{},
        identity: u64,
        retain_decoded: bool = false,
    };
    const State = struct { allocator: std.mem.Allocator, payload: union(enum) { full: Plan, canonical: Canonical } };

    /// A compact owner has no mutable input matching or output fanout. Only
    /// this constructor can create its sorted, unique canonical identity.
    fn initCanonical(a: std.mem.Allocator, prefix: []const u8, nodes: NodeView) !PreparedScoreRequest {
        const encoding = @import("internal_keys.zig");
        if (nodes.len() == 0 or nodes.len() > Plan.max_keys) return error.GraphMetricQueryBudgetExceeded;
        var size = try std.math.mul(usize, nodes.len(), @sizeOf([]const u8));
        const descriptors = size;
        for (0..nodes.len()) |i| {
            const node = nodes.at(i);
            if (i != 0 and std.mem.order(u8, nodes.at(i - 1), node) != .lt) return error.InvalidBatch;
            size = std.math.add(usize, size, prefix.len) catch return error.GraphMetricQueryBudgetExceeded;
            size = std.math.add(usize, size, encoding.encodedComponentLen(node)) catch return error.GraphMetricQueryBudgetExceeded;
            if (size > Plan.max_bytes) return error.GraphMetricQueryBudgetExceeded;
        }
        const state = try a.create(State);
        errdefer a.destroy(state);
        const bytes = try a.alloc(u8, size - descriptors);
        errdefer a.free(bytes);
        const keys = try a.alloc([]const u8, nodes.len());
        var offset: usize = 0;
        for (keys, 0..) |*key, i| {
            const node = nodes.at(i);
            const len = prefix.len + encoding.encodedComponentLen(node);
            key.* = bytes[offset..][0..len];
            @memcpy(bytes[offset..][0..prefix.len], prefix);
            _ = encoding.encodeComponent(bytes[offset + prefix.len ..][0 .. len - prefix.len], node);
            offset += len;
        }
        state.* = .{ .allocator = a, .payload = .{ .canonical = .{ .bytes = bytes, .keys = keys, .count = keys.len, .identity = @import("prepared_score_plan.zig").newCanonicalIdentity() } } };
        return .{ .handle = state };
    }

    // Global row/column deduplication has already happened. Retain one bounded
    // canonical identity and compact routes for all eligible metric columns.
    // Overlapping prefixes are sorted and coalesced, preserving every route.
    fn initCanonicalColumns(a: std.mem.Allocator, prefixes: []const ?[]const u8, columns: []const usize, nodes: NodeView) !PreparedScoreRequest {
        const encoding = @import("internal_keys.zig");
        const count = std.math.mul(usize, columns.len, nodes.len()) catch return error.GraphMetricQueryBudgetExceeded;
        if (count == 0 or count > Plan.max_keys) return error.GraphMetricQueryBudgetExceeded;
        var byte_count: usize = 0;
        for (columns) |column| {
            if (column > std.math.maxInt(u32)) return error.GraphMetricQueryBudgetExceeded;
            const prefix = prefixes[column] orelse return error.InvalidBatch;
            for (0..nodes.len()) |i| {
                const node = nodes.at(i);
                byte_count = std.math.add(usize, byte_count, prefix.len) catch return error.GraphMetricQueryBudgetExceeded;
                byte_count = std.math.add(usize, byte_count, encoding.encodedComponentLen(node)) catch return error.GraphMetricQueryBudgetExceeded;
                if (byte_count > Plan.max_bytes) return error.GraphMetricQueryBudgetExceeded;
            }
        }
        const required = byte_count + count * (@sizeOf(CanonicalTarget) + @sizeOf([]const u8) + @sizeOf(usize));
        if (required > Plan.max_bytes) return error.GraphMetricQueryBudgetExceeded;
        const state = try a.create(State);
        errdefer a.destroy(state);
        const bytes = try a.alloc(u8, byte_count);
        errdefer a.free(bytes);
        const targets = try a.alloc(CanonicalTarget, count);
        errdefer a.free(targets);
        const keys = try a.alloc([]const u8, count);
        errdefer a.free(keys);
        const ends = try a.alloc(usize, count);
        var offset: usize = 0;
        var index: usize = 0;
        for (columns) |column| {
            const prefix = prefixes[column].?;
            for (0..nodes.len()) |row| {
                const node = nodes.at(row);
                const len = prefix.len + encoding.encodedComponentLen(node);
                @memcpy(bytes[offset..][0..prefix.len], prefix);
                _ = encoding.encodeComponent(bytes[offset + prefix.len ..][0 .. len - prefix.len], node);
                targets[index] = .{ .key_offset = @intCast(offset), .key_len = @intCast(len), .column = @intCast(column), .row = @intCast(row) };
                offset += len;
                index += 1;
            }
        }
        std.mem.sort(CanonicalTarget, targets, bytes, struct {
            fn less(encoded: []const u8, x: CanonicalTarget, y: CanonicalTarget) bool {
                return std.mem.order(u8, x.key(encoded), y.key(encoded)) == .lt;
            }
        }.less);
        var unique: usize = 0;
        for (targets, 0..) |target, i| {
            const key = target.key(bytes);
            if (unique == 0 or !std.mem.eql(u8, keys[unique - 1], key)) {
                keys[unique] = key;
                unique += 1;
            }
            ends[unique - 1] = i + 1;
        }
        state.* = .{ .allocator = a, .payload = .{ .canonical = .{ .bytes = bytes, .keys = keys, .count = unique, .targets = targets, .ends = ends, .identity = @import("prepared_score_plan.zig").newCanonicalIdentity(), .retain_decoded = true } } };
        return .{ .handle = state };
    }

    fn hasRoutes(self: *const PreparedScoreRequest) bool {
        return switch (self.handle.payload) {
            .full => false,
            .canonical => |*c| c.targets.len != 0,
        };
    }
    fn uniqueRoutes(self: *const PreparedScoreRequest) bool {
        const c = &self.handle.payload.canonical;
        return c.count == c.targets.len;
    }

    fn routes(self: *const PreparedScoreRequest, i: usize) []const CanonicalTarget {
        const c = &self.handle.payload.canonical;
        return c.targets[if (i == 0) 0 else c.ends[i - 1]..c.ends[i]];
    }

    pub fn readKeys(self: *const PreparedScoreRequest) []const []const u8 {
        return switch (self.handle.payload) {
            .full => |*p| p.read_keys,
            .canonical => |*c| c.keys[0..c.count],
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
            .canonical => |*c| c.bytes.len + c.keys.len * @sizeOf([]const u8) + c.targets.len * @sizeOf(CanonicalTarget) + c.ends.len * @sizeOf(usize),
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
                allocator.free(c.targets);
                allocator.free(c.ends);
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
var next_request_owner_identity = std.atomic.Value(u64).init(1);
fn newRequestOwnerIdentity() u64 {
    const id = next_request_owner_identity.fetchAdd(1, .monotonic);
    if (id == 0 or id == std.math.maxInt(u64)) @panic("score request owner identity exhausted");
    return id;
}

/// Output aliasing is allowed only for identical spans. Partial overlap cannot
/// represent independent logical rows; reject it before any delivery or clear.
pub fn validateScoreColumns(rows: usize, count: usize, columns: anytype) !void {
    if (columns.len != count) return error.InvalidBatch;
    for (columns) |column| if (column.len != rows) return error.InvalidBatch;
    if (columns.len <= 1 or rows == 0) return;
    for (columns, 0..) |column, i| {
        const start = @intFromPtr(column.ptr);
        const end = std.math.add(usize, start, try std.math.mul(usize, rows, @sizeOf(@TypeOf(column[0])))) catch return error.InvalidBatch;
        for (columns[0..i]) |prior| {
            const other = @intFromPtr(prior.ptr);
            if (other == start or rows == 0) continue;
            const other_end = std.math.add(usize, other, try std.math.mul(usize, rows, @sizeOf(@TypeOf(column[0])))) catch return error.InvalidBatch;
            if (start < other_end and other < end) return error.InvalidBatch;
        }
    }
}

pub const PreparedScoreChunks = struct {
    handle: *const State,
    const State = struct {
        allocator: std.mem.Allocator,
        rows: usize,
        row_order_identity: bool,
        column_owners: []const usize,
        column_next: []const usize = &.{},
        row_heads: []const usize,
        duplicate_next: []const usize,
        chunks: []const Chunk,
        combined: ?PreparedScoreRequest = null,
        unique_rows: usize = 0,
        references: usize = 1,
        identity: u64,
    };
    const absent = std.math.maxInt(usize);
    pub const Chunk = struct {
        request: PreparedScoreRequest,
        column: usize,
        first: usize,
    };

    pub fn init(a: std.mem.Allocator, prefixes: []const ?[]const u8, nodes: []const []const u8) !@This() {
        const identity = newRequestOwnerIdentity();
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
            state.* = .{ .allocator = a, .identity = identity, .rows = nodes.len, .row_order_identity = true, .column_owners = &.{}, .row_heads = &.{}, .duplicate_next = &.{}, .chunks = &.{}, .combined = request };
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
        const column_next = try a.alloc(usize, owners.len);
        errdefer a.free(column_next);
        @memset(column_next, absent);
        var reverse = owners.len;
        while (reverse != 0) {
            reverse -= 1;
            const owner = owners[reverse];
            if (owner != absent and owner != reverse) {
                column_next[reverse] = column_next[owner];
                column_next[owner] = reverse;
            }
        }
        if (nodes.len == 0 or columns.items.len == 0) {
            state.* = .{ .allocator = a, .identity = identity, .rows = nodes.len, .row_order_identity = true, .column_owners = owners, .column_next = column_next, .row_heads = &.{}, .duplicate_next = &.{}, .chunks = &.{} };
            return .{ .handle = state };
        }
        // Prove sorted uniqueness before allocating sort/fanout scratch.
        // Borrow caller descriptors only during construction; encoded bytes are
        // always independently owned by the completed requests.
        const row_order_identity = blk: {
            for (nodes[1..], nodes[0 .. nodes.len - 1]) |right, left| {
                if (std.mem.order(u8, left, right) != .lt) break :blk false;
            }
            break :blk true;
        };
        var order: []usize = if (row_order_identity) &.{} else try a.alloc(usize, nodes.len);
        errdefer a.free(order);
        var next: []usize = &.{};
        errdefer a.free(next);
        var unique_count = nodes.len;
        if (!row_order_identity) {
            for (order, 0..) |*row, i| row.* = i;
            std.mem.sort(usize, order, nodes, struct {
                fn less(inputs: []const []const u8, x: usize, y: usize) bool {
                    const cmp = std.mem.order(u8, inputs[x], inputs[y]);
                    return cmp == .lt or (cmp == .eq and x < y);
                }
            }.less);
            var duplicates = false;
            for (order[1..], order[0 .. order.len - 1]) |right, left| {
                if (std.mem.eql(u8, nodes[left], nodes[right])) {
                    duplicates = true;
                    break;
                }
            }
            if (duplicates) {
                next = try a.alloc(usize, nodes.len);
                @memset(next, absent);
                unique_count = 0;
                // Compact canonical heads in the sort allocation itself. Reads
                // only touch the unconsumed suffix, so no descriptor copy is needed.
                for (order) |row| {
                    if (unique_count == 0 or !std.mem.eql(u8, nodes[order[unique_count - 1]], nodes[row])) {
                        order[unique_count] = row;
                        unique_count += 1;
                    } else {
                        next[row] = order[unique_count - 1];
                        order[unique_count - 1] = row;
                    }
                }
            }
        }
        if (order.len != unique_count and !row_order_identity) order = try a.realloc(order, unique_count);
        const unique_nodes = NodeView{ .nodes = nodes, .order = if (row_order_identity) null else order[0..unique_count] };
        var chunks: std.ArrayList(Chunk) = .empty;
        errdefer {
            for (chunks.items) |*chunk| chunk.request.deinit();
            chunks.deinit(a);
        }
        if (columns.items.len > 1) {
            const combined_keys = PreparedScoreRequest.initCanonicalColumns(a, prefixes, columns.items, unique_nodes) catch |err| switch (err) {
                error.GraphMetricQueryBudgetExceeded => null,
                else => return err,
            };
            if (combined_keys) |request| chunks.append(a, .{ .request = request, .column = absent, .first = 0 }) catch |err| {
                var owned = request;
                owned.deinit();
                return err;
            };
        }
        if (chunks.items.len == 0) for (columns.items) |column| {
            var first: usize = 0;
            while (first < unique_nodes.len()) {
                var count = @min(Plan.max_keys, unique_nodes.len() - first);
                const request = while (true) {
                    const candidate = PreparedScoreRequest.initCanonical(a, prefixes[column].?, unique_nodes.range(first, count)) catch |err| switch (err) {
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
        };
        // A duplicate-heavy logical request may exceed the mutable plan's
        // target budget yet deduplicate to one canonical batch. It can reuse
        // decoded scores just like a combined full plan.
        if (chunks.items.len == 1) @constCast(chunks.items[0].request.handle).payload.canonical.retain_decoded = true;
        state.* = .{ .allocator = a, .identity = identity, .rows = nodes.len, .row_order_identity = row_order_identity, .column_owners = owners, .column_next = column_next, .row_heads = order, .duplicate_next = next, .chunks = try chunks.toOwnedSlice(a), .unique_rows = unique_count };
        return .{ .handle = state };
    }

    pub fn retain(self: *const @This()) @This() {
        @constCast(self.handle).references += 1;
        return self.*;
    }
    pub fn deinit(self: *@This()) void {
        const state = @constCast(self.handle);
        self.* = undefined;
        state.references -= 1;
        if (state.references != 0) return;
        if (@constCast(state).combined) |*request| request.deinit();
        for (@constCast(state.chunks)) |*chunk| chunk.request.deinit();
        const a = state.allocator;
        a.free(state.chunks);
        a.free(state.column_owners);
        a.free(state.column_next);
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
        try validateScoreColumns(self.handle.rows, count, columns);
    }

    pub const Output = struct {
        owner: *const PreparedScoreChunks,
        chunk: *const Chunk,
        columns: []const []?f64,
        pub fn sink(self: *@This()) @import("backend_types.zig").CompleteScoreSink {
            const routed_keys = self.chunk.request.hasRoutes();
            const unique = routed_keys and self.chunk.request.uniqueRoutes();
            return .{ .context = self, .write = if (unique) routedUnique else if (routed_keys) routed else write, .write_decoded = if (unique) decodedUniqueRoutes else if (routed_keys) decodedRouted else decoded };
        }
        fn routedUnique(ctx: *anyopaque, i: usize, score: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const target = self.chunk.request.handle.payload.canonical.targets[i];
            self.writeRow(target.column, target.row, score);
        }
        fn decodedUniqueRoutes(ctx: *anyopaque, scores: []const f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            for (scores, self.chunk.request.handle.payload.canonical.targets) |score, target| {
                self.writeRow(target.column, target.row, if (std.math.isNan(score)) null else score);
            }
        }
        fn routed(ctx: *anyopaque, i: usize, score: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            for (self.chunk.request.routes(i)) |target| self.writeRow(target.column, target.row, score);
        }
        fn decodedRouted(ctx: *anyopaque, scores: []const f64) void {
            for (scores, 0..) |score, i| routed(ctx, i, if (std.math.isNan(score)) null else score);
        }
        fn writeRow(self: *@This(), column: usize, i: usize, score: ?f64) void {
            if (self.owner.handle.row_order_identity) {
                self.columns[column][i] = score;
                return;
            }
            var row = self.owner.handle.row_heads[i];
            while (row != absent) {
                self.columns[column][row] = score;
                row = if (self.owner.handle.duplicate_next.len == 0) absent else self.owner.handle.duplicate_next[row];
            }
        }
        fn write(ctx: *anyopaque, i: usize, score: ?f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.writeRow(self.chunk.column, self.chunk.first + i, score);
        }
        fn decoded(ctx: *anyopaque, scores: []const f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.owner.handle.row_order_identity) {
                const column = self.columns[self.chunk.column][self.chunk.first..][0..scores.len];
                for (scores, column) |score, *value| value.* = if (std.math.isNan(score)) null else score;
            } else for (scores, 0..) |score, i| write(ctx, i, if (std.math.isNan(score)) null else score);
        }
    };

    pub fn denseColumnRange(self: *const @This(), column: usize) ?struct { first: usize, count: usize } {
        if (self.combinedRequest()) |request| {
            const plan = request.plan();
            if (!plan.dense_columns or !plan.row_order_identity) return null;
            for (plan.column_sorted[0..plan.column_count], 0..) |col, ordinal| {
                if ((if (plan.column_order_identity) col else plan.column_remap[col]) == column)
                    return .{ .first = ordinal * self.handle.rows, .count = self.handle.rows };
            }
            return null;
        }
        if (!self.handle.row_order_identity or column >= self.handle.column_owners.len) return null;
        const owner = self.handle.column_owners[column];
        if (owner == absent) return null;
        var offset: usize = 0;
        var first: ?usize = null;
        var count: usize = 0;
        for (self.handle.chunks) |chunk| {
            if (chunk.request.hasRoutes()) return null;
            const len = chunk.request.readKeys().len;
            if (chunk.column == owner) {
                if (first == null) first = offset;
                count += len;
            }
            offset += len;
        }
        return if (first) |start| .{ .first = start, .count = count } else null;
    }

    pub const ScoreVisitor = struct {
        context: *anyopaque,
        write: *const fn (*anyopaque, usize, usize, ?f64) void,
    };

    /// Visit every logical cell exactly once without materializing output slabs.
    /// Order is canonical, not input-row order. NaN is an internal missing sentinel.
    pub fn visitDecoded(self: *const @This(), scores: []const f64, visitor: ScoreVisitor) void {
        if (self.combinedRequest()) |request| {
            const plan = request.plan();
            for (plan.prefixes, 0..) |prefix, column| if (prefix == null) {
                const col = if (plan.column_order_identity) column else plan.column_remap[column];
                for (0..self.handle.rows) |row| visitor.write(visitor.context, col, row, null);
            };
            var first: usize = 0;
            for (scores, plan.ends) |score, end| {
                const value: ?f64 = if (std.math.isNan(score)) null else score;
                for (plan.targets[first..end]) |target| visitor.write(visitor.context, if (plan.column_order_identity) target.column else plan.column_remap[target.column], if (plan.row_order_identity) target.row else plan.row_remap[target.row], value);
                first = end;
            }
            return;
        }
        for (self.handle.column_owners, 0..) |owner, column| if (owner == absent) {
            for (0..self.handle.rows) |row| visitor.write(visitor.context, column, row, null);
        };
        var offset: usize = 0;
        for (self.handle.chunks) |*chunk| {
            const count = chunk.request.readKeys().len;
            for (scores[offset..][0..count], 0..) |score, i| {
                const value: ?f64 = if (std.math.isNan(score)) null else score;
                if (chunk.request.hasRoutes()) {
                    for (chunk.request.routes(i)) |target| self.visitRow(visitor, target.column, target.row, value);
                } else self.visitRow(visitor, chunk.column, chunk.first + i, value);
            }
            offset += count;
        }
    }
    fn visitRow(self: *const @This(), visitor: ScoreVisitor, column: usize, canonical_row: usize, value: ?f64) void {
        var row = if (self.handle.row_order_identity) canonical_row else self.handle.row_heads[canonical_row];
        while (row != absent) {
            // Duplicate columns share the canonical value, never a copied slab.
            var col = column;
            while (col != absent) {
                visitor.write(visitor.context, col, row, value);
                col = self.handle.column_next[col];
            }
            row = if (self.handle.row_order_identity or self.handle.duplicate_next.len == 0) absent else self.handle.duplicate_next[row];
        }
    }

    pub fn clearAbsentColumns(self: *const @This(), columns: []const []?f64) void {
        for (self.handle.column_owners, columns) |owner, column| if (owner == absent) {
            @memset(column, null);
        };
    }

    pub fn finishColumns(self: *const @This(), columns: []const []?f64) void {
        for (self.handle.column_owners, 0..) |owner, column| {
            if (owner != absent and owner != column and columns[column].ptr != columns[owner].ptr) @memcpy(columns[column], columns[owner]);
        }
    }
};

/// Independently owned score-to-logical-row routing, containing no key bytes,
/// storage handles, or borrowed descriptors. Constructed once per view cache.
pub const ScoreRouting = struct {
    const absent = std.math.maxInt(usize);
    pub const Column = struct { first: usize = absent, indices: []usize = &.{} };
    allocator: std.mem.Allocator,
    rows: usize,
    columns: []Column,
    row_map: []usize = &.{},

    pub fn init(a: std.mem.Allocator, request: *const PreparedScoreChunks) !@This() {
        const count = if (request.combinedRequest()) |r| r.plan().prefixes.len else request.handle.column_owners.len;
        var self = @This(){ .allocator = a, .rows = request.handle.rows, .columns = try a.alloc(Column, count) };
        @memset(self.columns, .{});
        errdefer self.deinit();
        if (request.combinedRequest()) |r| {
            const plan = r.plan();
            if (plan.dense_columns and plan.row_order_identity) {
                for (plan.column_sorted[0..plan.column_count], 0..) |column, ordinal| {
                    self.columns[if (plan.column_order_identity) column else plan.column_remap[column]].first = ordinal * self.rows;
                }
            } else {
                for (plan.prefixes, 0..) |prefix, col| if (prefix != null) {
                    self.columns[if (plan.column_order_identity) col else plan.column_remap[col]].indices = try a.alloc(usize, self.rows);
                };
                var first: usize = 0;
                for (plan.ends, 0..) |end, key| {
                    for (plan.targets[first..end]) |target| {
                        const col = if (plan.column_order_identity) target.column else plan.column_remap[target.column];
                        const row = if (plan.row_order_identity) target.row else plan.row_remap[target.row];
                        self.columns[col].indices[row] = key;
                    }
                    first = end;
                }
            }
            return self;
        }
        if (!request.handle.row_order_identity) {
            self.row_map = try a.alloc(usize, self.rows);
            for (request.handle.row_heads[0..request.handle.unique_rows], 0..) |head, unique| {
                var row = head;
                while (row != absent) {
                    self.row_map[row] = unique;
                    row = if (request.handle.duplicate_next.len == 0) absent else request.handle.duplicate_next[row];
                }
            }
        }
        var offset: usize = 0;
        for (request.chunkPlans()) |chunk| {
            const keys = chunk.request.readKeys().len;
            if (chunk.request.hasRoutes()) {
                for (0..keys) |key| for (chunk.request.routes(key)) |target| {
                    const col = &self.columns[target.column];
                    if (col.indices.len == 0) col.indices = try a.alloc(usize, request.handle.unique_rows);
                    col.indices[target.row] = offset + key;
                };
            } else if (self.columns[chunk.column].first == absent) {
                self.columns[chunk.column].first = offset;
            }
            offset += keys;
        }
        // Alias descriptors borrow the owner's index slice; only owners free it.
        for (request.handle.column_owners, 0..) |owner, col| if (owner != absent and owner != col) {
            self.columns[col] = self.columns[owner];
        };
        return self;
    }
    pub fn deinit(self: *@This()) void {
        for (self.columns, 0..) |column, i| {
            if (column.indices.len == 0) continue;
            var shared = false;
            for (self.columns[0..i]) |prior| if (prior.indices.ptr == column.indices.ptr) {
                shared = true;
                break;
            };
            if (!shared) self.allocator.free(column.indices);
        }
        self.allocator.free(self.row_map);
        self.allocator.free(self.columns);
        self.* = undefined;
    }
    pub fn scoreIndex(self: *const @This(), col: usize, row: usize) ?usize {
        const column = self.columns[col];
        const canonical = if (self.row_map.len == 0) row else self.row_map[row];
        if (column.indices.len != 0) return column.indices[canonical];
        return if (column.first == absent) null else column.first + canonical;
    }
    pub fn denseColumnRange(self: *const @This(), col: usize) ?struct { first: usize, count: usize } {
        if (col >= self.columns.len or self.row_map.len != 0) return null;
        const column = self.columns[col];
        if (column.first == absent or column.indices.len != 0) return null;
        return .{ .first = column.first, .count = self.rows };
    }
    pub fn visitDecoded(self: *const @This(), scores: []const f64, visitor: PreparedScoreChunks.ScoreVisitor) void {
        for (self.columns, 0..) |_, col| for (0..self.rows) |row| {
            const value = if (self.scoreIndex(col, row)) |index| scores[index] else std.math.nan(f64);
            visitor.write(visitor.context, col, row, if (std.math.isNan(value)) null else value);
        };
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
    var request = try PreparedScoreRequest.initCanonical(a, &prefix, .{ .nodes = &.{ "", &node, "z" } });
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
    try std.testing.expectError(error.InvalidBatch, PreparedScoreRequest.initCanonical(std.testing.allocator, "p/", .{ .nodes = &.{ "b", "a" } }));
    try std.testing.expectError(error.InvalidBatch, PreparedScoreRequest.initCanonical(std.testing.allocator, "p/", .{ .nodes = &.{ "a", "a" } }));
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

fn canonicalColumnsFailureCase(a: std.mem.Allocator) !void {
    var request = try PreparedScoreRequest.initCanonicalColumns(a, &.{ "p", "pa" }, &.{ 0, 1 }, .{ .nodes = &.{ "", "a" } });
    defer request.deinit();
    // p + a and pa + empty produce the same physical key, with two routes.
    try std.testing.expectEqual(@as(usize, 3), request.readKeys().len);
    try std.testing.expectEqual(@as(usize, 2), request.routes(1).len);
    try std.testing.expect(request.retainDecoded());
    for (request.readKeys()[1..], request.readKeys()[0 .. request.readKeys().len - 1]) |right, left| try std.testing.expect(std.mem.order(u8, left, right) == .lt);
}

test "prepared score combined canonical routes coalesce overlapping prefixes and release allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, canonicalColumnsFailureCase, .{});
}

fn duplicateColumnsFailureCase(a: std.mem.Allocator) !void {
    var stable = StableAllocator{ .backing = a };
    var nodes: [5000][]const u8 = @splat("a");
    nodes[0] = "";
    var request = try PreparedScoreChunks.init(stable.allocator(), &.{ "p", "pa", null, "p" }, &nodes);
    defer request.deinit();
    try std.testing.expect(request.combinedRequest() == null);
    try std.testing.expectEqual(@as(usize, 1), request.chunkPlans().len);
    try std.testing.expectEqual(@as(usize, 3), request.chunkPlans()[0].request.readKeys().len);
    var columns: [4][5000]?f64 = undefined;
    request.clearAbsentColumns(&.{ &columns[0], &columns[1], &columns[2], &columns[3] });
    var output = PreparedScoreChunks.Output{ .owner = &request, .chunk = &request.chunkPlans()[0], .columns = &.{ &columns[0], &columns[1], &columns[2], &columns[3] } };
    output.sink().deliverDecoded(&.{ 1, 2, 3 });
    request.finishColumns(&.{ &columns[0], &columns[1], &columns[2], &columns[3] });
    try std.testing.expectEqual(@as(?f64, 1), columns[0][0]);
    try std.testing.expectEqual(@as(?f64, 2), columns[1][0]);
    for (columns[0][1..]) |value| try std.testing.expectEqual(@as(?f64, 2), value);
    for (columns[1][1..]) |value| try std.testing.expectEqual(@as(?f64, 3), value);
    for (columns[2]) |value| try std.testing.expect(value == null);
    try std.testing.expectEqualSlices(?f64, &columns[0], &columns[3]);
}

test "prepared score duplicate multi column routes preserve fanout under construction failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, duplicateColumnsFailureCase, .{});
}

test "prepared score duplicate columns accept shared output storage" {
    const nodes: [5000][]const u8 = @splat("a");
    var request = try PreparedScoreChunks.init(std.testing.allocator, &.{ "p/", "p/" }, &nodes);
    defer request.deinit();
    var column: [5000]?f64 = @splat(3);
    const columns = [_][]?f64{ &column, &column };
    try request.validateColumns(&columns);
    request.finishColumns(&columns);
    for (column) |value| try std.testing.expectEqual(@as(?f64, 3), value);
}

fn permutationAllocationFailureCase(a: std.mem.Allocator) !void {
    var stable = StableAllocator{ .backing = a };
    var nodes: [4097][]const u8 = undefined;
    var storage: [4097][8]u8 = undefined;
    for (&storage, 0..) |*bytes, i| _ = try std.fmt.bufPrint(bytes, "{d:0>8}", .{i});
    for (&nodes, 0..) |*node, i| node.* = &storage[nodes.len - i - 1];
    var request = try PreparedScoreChunks.init(stable.allocator(), &.{"p/"}, &nodes);
    defer request.deinit();
    try std.testing.expectEqual(nodes.len, request.uniqueRowCount());
    try std.testing.expectEqual(nodes.len, request.handle.row_heads.len);
    try std.testing.expectEqual(@as(usize, 0), request.handle.duplicate_next.len);
    var scores: [4097]?f64 = undefined;
    for (request.chunkPlans()) |*chunk| {
        var output = PreparedScoreChunks.Output{ .owner = &request, .chunk = chunk, .columns = &.{&scores} };
        for (chunk.request.readKeys(), 0..) |_, i| output.sink().write(output.sink().context, i, @floatFromInt(chunk.first + i));
    }
    for (scores, 0..) |score, i| try std.testing.expectEqual(@as(?f64, @floatFromInt(nodes.len - i - 1)), score);
}

test "prepared score unique permutations omit duplicate chains and release allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, permutationAllocationFailureCase, .{});
}

fn routingFailureCase(a: std.mem.Allocator, request: *const PreparedScoreChunks) !void {
    var routing = try ScoreRouting.init(a, request);
    defer routing.deinit();
    try std.testing.expectEqual(request.handle.rows, routing.rows);
}
test "prepared score detached routing releases allocation failures" {
    const a = std.testing.allocator;
    var small = try PreparedScoreChunks.init(a, &.{ "p/", null, "q/", "p/" }, &.{ "b", "a", "b" });
    defer small.deinit();
    try std.testing.checkAllAllocationFailures(a, routingFailureCase, .{&small});
    const nodes: [5000][]const u8 = @splat("a");
    var large = try PreparedScoreChunks.init(a, &.{ "p/", null, "q/", "p/" }, &nodes);
    defer large.deinit();
    try std.testing.checkAllAllocationFailures(a, routingFailureCase, .{&large});
}
