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

//! Streaming internal expansion decoder. Temporary parsing is retired per
//! node/hit/status; exported data owns its bytes independently of the wire.
const std = @import("std");
const graph = @import("graph.zig");
const query = graph.graph_query_mod;
const types = graph.db_mod.types;
const Allocator = std.mem.Allocator;

const NodeBuilder = struct {
    a: Allocator,
    nodes: std.ArrayListUnmanaged(query.GraphResultNode) = .empty,
    owner: ?*query.GraphMetricColumns = null,
    total_rows: usize,
    values: std.ArrayListUnmanaged(query.GraphMetricValue) = .empty,
    counts: std.ArrayListUnmanaged(usize) = .empty,
    names: std.ArrayListUnmanaged([]u8) = .empty,
    interned: std.StringHashMapUnmanaged([]u8) = .empty,

    fn deinit(self: *@This()) void {
        for (self.nodes.items) |*node| node.deinit(self.a);
        self.nodes.deinit(self.a);
        if (self.owner) |owner| owner.deinit(self.a);
        self.values.deinit(self.a);
        self.counts.deinit(self.a);
        self.interned.deinit(self.a);
        for (self.names.items) |name| self.a.free(name);
        self.names.deinit(self.a);
    }
    fn intern(self: *@This(), name: []const u8) ![]u8 {
        if (self.interned.get(name)) |owned| return owned;
        try self.interned.ensureUnusedCapacity(self.a, 1);
        try self.names.ensureUnusedCapacity(self.a, 1);
        const owned = try self.a.dupe(u8, name);
        self.interned.putAssumeCapacity(owned, owned);
        self.names.appendAssumeCapacity(owned);
        return owned;
    }
    fn initColumns(self: *@This(), node: query.GraphResultNode) !void {
        const owner = try self.a.create(query.GraphMetricColumns);
        errdefer self.a.destroy(owner);
        const names = try self.a.alloc([]u8, node.metricCount());
        var initialized: usize = 0;
        errdefer {
            for (names[0..initialized]) |name| self.a.free(name);
            self.a.free(names);
        }
        for (names, 0..) |*name, column| {
            name.* = try self.a.dupe(u8, node.metricAt(column).name);
            initialized += 1;
        }
        const scores = try self.a.alloc(f64, try std.math.mul(usize, self.total_rows, names.len));
        owner.* = .{ .names = names, .scores = scores, .rows = self.total_rows };
        self.owner = owner;
    }
    fn promote(self: *@This()) !void {
        const owner = self.owner orelse return;
        try self.counts.ensureUnusedCapacity(self.a, self.nodes.items.len);
        try self.values.ensureUnusedCapacity(self.a, try std.math.mul(usize, self.nodes.items.len, owner.names.len));
        for (self.nodes.items, 0..) |_, row| {
            for (0..owner.names.len) |column| {
                const metric = owner.at(row, column);
                const name = try self.intern(metric.name);
                self.values.appendAssumeCapacity(.{ .name = name, .name_owned = false, .score = metric.score });
            }
            self.counts.appendAssumeCapacity(owner.names.len);
        }
        // No fallible operations after borrowing nodes lose the column owner.
        for (self.nodes.items) |*node| node.metric_columns = null;
        owner.deinit(self.a);
        self.owner = null;
    }
    fn resizeColumns(self: *@This(), rows: usize) !void {
        const owner = self.owner orelse return;
        if (owner.rows == rows) return;
        const scores = try self.a.alloc(f64, try std.math.mul(usize, rows, owner.names.len));
        const count = @min(rows, self.nodes.items.len);
        for (0..owner.names.len) |column| @memcpy(scores[column * rows ..][0..count], owner.scores[column * owner.rows ..][0..count]);
        self.a.free(owner.scores);
        owner.scores = scores;
        owner.rows = rows;
        self.total_rows = rows;
    }
    fn append(self: *@This(), source: query.GraphResultNode) !void {
        if (self.nodes.items.len >= std.math.maxInt(u32)) return error.InvalidRemoteResponse;
        try self.nodes.ensureUnusedCapacity(self.a, 1);
        if (self.owner == null and self.nodes.items.len == 0) self.total_rows = self.nodes.capacity;
        if (self.nodes.items.len == 0 and source.metricCount() > 0 and source.metricCount() <= query.graph_metric_projection_limit) try self.initColumns(source);
        if (self.owner) |owner| {
            var same = source.metricCount() == owner.names.len;
            if (same) for (owner.names, 0..) |name, column| {
                same = same and std.mem.eql(u8, name, source.metricAt(column).name);
            };
            if (!same) try self.promote();
        }
        var payload = source;
        payload.metrics = &.{};
        payload.metric_columns = null;
        var node = try graph.cloneGraphNode(self.a, payload);
        errdefer node.deinit(self.a);
        node.metrics_owned = false;
        if (self.owner != null and self.nodes.items.len == self.total_rows) try self.resizeColumns(self.nodes.capacity);
        if (self.owner) |owner| {
            const row = self.nodes.items.len;
            for (0..owner.names.len) |column| owner.scores[column * self.total_rows + row] = source.metricAt(column).score orelse std.math.nan(f64);
            node.metric_columns = owner;
            node.metric_row = @intCast(row);
        } else {
            try self.counts.ensureUnusedCapacity(self.a, 1);
            try self.values.ensureUnusedCapacity(self.a, source.metricCount());
            for (0..source.metricCount()) |column| {
                const metric = source.metricAt(column);
                const name = try self.intern(metric.name);
                self.values.appendAssumeCapacity(.{ .name = name, .name_owned = false, .score = metric.score });
            }
            self.counts.appendAssumeCapacity(source.metricCount());
        }
        self.nodes.appendAssumeCapacity(node);
    }
    fn finish(self: *@This()) !graph.OwnedGraphNodes {
        try self.resizeColumns(self.nodes.items.len);
        const nodes = try self.nodes.toOwnedSlice(self.a);
        errdefer {
            for (nodes) |*node| node.deinit(self.a);
            self.a.free(nodes);
        }
        if (self.owner) |owner| {
            self.owner = null;
            return .{ .nodes = nodes, .metrics = .{ .columns = owner } };
        }
        const values = try self.values.toOwnedSlice(self.a);
        errdefer self.a.free(values);
        const names = try self.names.toOwnedSlice(self.a);
        var offset: usize = 0;
        for (nodes, self.counts.items) |*node, count| {
            node.metrics = values[offset..][0..count];
            offset += count;
        }
        return .{ .nodes = nodes, .metrics = .{ .values = values, .names = names } };
    }
};

// This pass counts only structure; typed parsing remains authoritative. It
// skips quoted bytes without converting numbers or allocating token stacks.
fn countNodes(input: []const u8) !usize {
    var depth: usize = 0;
    var quoted = false;
    var count: usize = 0;
    var i: usize = 0;
    while (i < input.len) : (i += 1) {
        const byte = input[i];
        if (quoted) {
            if (byte == '\\') {
                i += 1;
                continue;
            }
            if (byte == '"') quoted = false;
            continue;
        }
        switch (byte) {
            '"' => quoted = true,
            '{', '[' => {
                if (byte == '{' and depth == 1) {
                    if (count == std.math.maxInt(u32)) return error.InvalidRemoteResponse;
                    count += 1;
                }
                depth += 1;
            },
            '}', ']' => {
                if (depth == 0) return error.SyntaxError;
                depth -= 1;
                if (depth == 0) {
                    if (byte != ']') return error.SyntaxError;
                    // A valid node needs key, depth and distance (at least 33
                    // encoded bytes). Reject impossible counts before reserving
                    // output for malformed arrays full of tiny empty objects.
                    if (count > (i + 1) / 33) return error.MissingField;
                    return count;
                }
            },
            else => {},
        }
    }
    return error.SyntaxError;
}

const Parser = struct {
    a: Allocator,
    scanner: std.json.Scanner,
    scratch: std.heap.ArenaAllocator,
    options: std.json.ParseOptions,
    fn reset(self: *@This()) void {
        _ = self.scratch.reset(.{ .retain_with_limit = 256 * 1024 });
    }
    fn expect(self: *@This(), kind: std.json.TokenType) !void {
        if (try self.scanner.peekNextTokenType() != kind) return error.UnexpectedToken;
        _ = try self.scanner.next();
    }
    fn read(self: *@This(), comptime T: type) !T {
        return std.json.innerParse(T, self.scratch.allocator(), &self.scanner, self.options);
    }
    fn field(self: *@This(), comptime names: []const []const u8, seen: *u16) !?usize {
        self.reset();
        if (try self.scanner.peekNextTokenType() == .object_end) {
            _ = try self.scanner.next();
            return null;
        }
        const name = try self.read([]const u8);
        inline for (names, 0..) |expected, i| if (std.mem.eql(u8, name, expected)) {
            const bit: u16 = @as(u16, 1) << i;
            if (seen.* & bit != 0) return error.DuplicateField;
            seen.* |= bit;
            return i;
        };
        return error.UnknownField;
    }
    fn nodes(self: *@This(), hint: ?u32) !graph.OwnedGraphNodes {
        if (try self.scanner.peekNextTokenType() != .array_begin) return error.UnexpectedToken;
        // Current workers send total before nodes. Treat it only as a bounded
        // allocation hint; arbitrary field order and inaccurate totals remain
        // valid, and exported columns always use the actual row count.
        const remaining = self.scanner.input.len - self.scanner.cursor;
        const count = if (hint) |total| @min(@as(usize, total), remaining / 33) else try countNodes(self.scanner.input[self.scanner.cursor..]);
        try self.expect(.array_begin);
        var builder = NodeBuilder{ .a = self.a, .total_rows = count };
        defer builder.deinit();
        try builder.nodes.ensureTotalCapacityPrecise(self.a, count);
        while (try self.scanner.peekNextTokenType() != .array_end) {
            self.reset();
            try builder.append(try self.read(query.GraphResultNode));
        }
        try self.expect(.array_end);
        return builder.finish();
    }
    fn ownedArray(self: *@This(), comptime T: type) ![]T {
        try self.expect(.array_begin);
        var list = std.ArrayListUnmanaged(T).empty;
        errdefer {
            for (list.items) |*item| item.deinit(self.a);
            list.deinit(self.a);
        }
        while (try self.scanner.peekNextTokenType() != .array_end) {
            self.reset();
            try list.ensureUnusedCapacity(self.a, 1);
            const raw = try self.read(T);
            const owned = if (T == types.SearchHit) try raw.clone(self.a) else try raw.cloneAlloc(self.a);
            list.appendAssumeCapacity(owned);
        }
        try self.expect(.array_end);
        return list.toOwnedSlice(self.a);
    }
    fn expansion(self: *@This()) !graph.GraphExpansion {
        try self.expect(.object_begin);
        var result = graph.GraphExpansion{ .frontier_id = 0, .frontier_key = &.{}, .graph_result = .{ .name = &.{}, .nodes = &.{}, .hits = &.{}, .total_hits = 0 } };
        errdefer result.deinit(self.a);
        var seen: u16 = 0;
        while (try self.field(&.{ "frontier_id", "frontier_key", "name", "total", "nodes", "hits", "metric_status" }, &seen)) |index| switch (index) {
            0 => result.frontier_id = try self.read(u32),
            1 => result.frontier_key = try self.a.dupe(u8, try self.read([]const u8)),
            2 => result.graph_result.name = try self.a.dupe(u8, try self.read([]const u8)),
            3 => result.graph_result.total_hits = try self.read(u32),
            4 => {
                const owned = try self.nodes(if (seen & 8 != 0) result.graph_result.total_hits else null);
                result.graph_result.nodes = owned.nodes;
                result.graph_result.metric_columns = owned.metrics.columns;
                result.graph_result.metric_values_slab = owned.metrics.values;
                result.graph_result.metric_value_names = owned.metrics.names;
            },
            5 => result.graph_result.hits = try self.ownedArray(types.SearchHit),
            6 => result.graph_result.metric_status = try self.ownedArray(types.GraphMetricStatus),
            else => unreachable,
        };
        if (seen & 31 != 31) return error.MissingField;
        return result;
    }
};

pub fn parse(a: Allocator, body: []const u8) !graph.GraphExpandResponse {
    var parser = Parser{ .a = a, .scanner = std.json.Scanner.initCompleteInput(a, body), .scratch = std.heap.ArenaAllocator.init(a), .options = .{ .allocate = .alloc_if_needed, .max_value_len = body.len } };
    defer parser.scanner.deinit();
    defer parser.scratch.deinit();
    try parser.expect(.object_begin);
    var expansions = std.ArrayListUnmanaged(graph.GraphExpansion).empty;
    errdefer {
        for (expansions.items) |*expansion| expansion.deinit(a);
        expansions.deinit(a);
    }
    var scanned: ?u32 = null;
    var seen: u16 = 0;
    while (try parser.field(&.{ "expansions", "scanned_rows" }, &seen)) |index| switch (index) {
        0 => {
            try parser.expect(.array_begin);
            while (try parser.scanner.peekNextTokenType() != .array_end) {
                try expansions.ensureUnusedCapacity(a, 1);
                expansions.appendAssumeCapacity(try parser.expansion());
            }
            try parser.expect(.array_end);
        },
        1 => scanned = try parser.read(?u32),
        else => unreachable,
    };
    if (seen & 1 == 0) return error.MissingField;
    try parser.expect(.end_of_document);
    return .{ .expansions = try expansions.toOwnedSlice(a), .scanned_rows = scanned orelse @intCast(graph.graph_pattern_mod.default_max_explored_edges) };
}
