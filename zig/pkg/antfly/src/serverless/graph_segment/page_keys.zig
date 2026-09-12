// Copyright 2026 Antfly, Inc.
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

//! Stable graph identities for the ordered page tree. Adjacency is node-first;
//! the second, local-only topology index is type-first. A metric selecting one
//! type therefore does not read unrelated edges or a global node dictionary.
//! Physical duplicates retain multiplicity through a per-equal-edge occurrence
//! ordinal, independent of input ordering and of every other source document.
const std = @import("std");
const Allocator = std.mem.Allocator;
const tree = @import("page_tree.zig");

pub const Direction = enum(u8) { member = 0, outgoing = 1, incoming = 2 };
const adjacency_tag: u8 = 16;
const topology_tag: u8 = 32;

pub const Edge = struct {
    source: []const u8,
    target: []const u8,
    kind: []const u8,
    table: ?[]const u8 = null,
    weight: f32 = 1,
    occurrence: u32 = 0,
};

pub const Key = struct {
    bytes: std.ArrayListUnmanaged(u8) = .empty,

    pub fn deinit(self: *Key, alloc: Allocator) void {
        self.bytes.deinit(alloc);
    }

    fn append(self: *Key, alloc: Allocator, bytes: []const u8) !void {
        if (bytes.len > tree.max_key_bytes - self.bytes.items.len) return error.GraphPageRecordTooLarge;
        try self.bytes.appendSlice(alloc, bytes);
    }

    fn component(self: *Key, alloc: Allocator, value: []const u8) !void {
        for (value) |byte| {
            if (byte == 0) try self.append(alloc, &.{ 0, 255 }) else try self.append(alloc, &.{byte});
        }
        try self.append(alloc, &.{ 0, 0 });
    }

    pub fn node(alloc: Allocator, id: []const u8) !Key {
        if (id.len == 0) return error.InvalidGraphPageKey;
        var key: Key = .{};
        errdefer key.deinit(alloc);
        try key.append(alloc, &.{adjacency_tag});
        try key.component(alloc, id);
        return key;
    }

    pub fn adjacency(alloc: Allocator, id: []const u8, direction: Direction, kind: ?[]const u8) !Key {
        var key = try node(alloc, id);
        errdefer key.deinit(alloc);
        try key.append(alloc, &.{@intFromEnum(direction)});
        if (kind) |value| try key.component(alloc, value);
        return key;
    }

    pub fn topology(alloc: Allocator, kind: ?[]const u8) !Key {
        var key: Key = .{};
        errdefer key.deinit(alloc);
        try key.append(alloc, &.{topology_tag});
        if (kind) |value| try key.component(alloc, value);
        return key;
    }

    pub fn edge(alloc: Allocator, value: Edge, direction: Direction) !Key {
        try @import("../../graph/edge_type.zig").validateStored(value.kind);
        if (value.table) |table| if (table.len == 0) return error.InvalidGraphPageKey;
        if (direction == .member or value.source.len == 0 or value.target.len == 0 or
            !std.math.isFinite(value.weight) or (direction == .incoming and value.table != null))
            return error.InvalidGraphPageKey;
        var key = try adjacency(alloc, if (direction == .incoming) value.target else value.source, direction, value.kind);
        errdefer key.deinit(alloc);
        try key.component(alloc, if (direction == .incoming) value.source else value.target);
        try key.finishEdge(alloc, value);
        return key;
    }

    pub fn neighborPrefix(alloc: Allocator, source: []const u8, kind: []const u8, target: []const u8) !Key {
        var key = try adjacency(alloc, source, .outgoing, kind);
        errdefer key.deinit(alloc);
        try key.component(alloc, target);
        return key;
    }

    pub fn topologyEdge(alloc: Allocator, value: Edge) !Key {
        try @import("../../graph/edge_type.zig").validateStored(value.kind);
        if (value.table != null or value.source.len == 0 or value.target.len == 0 or !std.math.isFinite(value.weight))
            return error.InvalidGraphPageKey;
        var key = try topology(alloc, value.kind);
        errdefer key.deinit(alloc);
        try key.component(alloc, value.source);
        try key.component(alloc, value.target);
        try key.finishEdge(alloc, value);
        return key;
    }

    fn finishEdge(self: *Key, alloc: Allocator, value: Edge) !void {
        // Numeric ascending order, including negative weights. Normalize signed
        // zero, which is semantically equal and must not split duplicate groups.
        const bits: u32 = @bitCast(if (value.weight == 0) @as(f32, 0) else value.weight);
        const ordered = if (bits & 0x80000000 != 0) ~bits else bits ^ 0x80000000;
        var weight: [4]u8 = undefined;
        std.mem.writeInt(u32, &weight, ordered, .big);
        try self.append(alloc, &weight);
        try self.component(alloc, value.table orelse "");
        var occurrence: [4]u8 = undefined;
        std.mem.writeInt(u32, &occurrence, value.occurrence, .big);
        try self.append(alloc, &occurrence);
    }

    /// Every key with this prefix lies in [prefix, successor). The caller owns
    /// both boundaries, so cursors can borrow them across page transitions.
    pub fn successorAlloc(self: Key, alloc: Allocator) ![]u8 {
        var len = self.bytes.items.len;
        while (len != 0 and self.bytes.items[len - 1] == 255) len -= 1;
        if (len == 0) return error.InvalidGraphPageKey;
        const upper = try alloc.dupe(u8, self.bytes.items[0..len]);
        upper[len - 1] += 1;
        return upper;
    }
};

/// Decoding uses caller-owned scratch, not one allocation per string. Returned
/// components remain valid until the scratch is reused. No global intern table
/// is part of the persisted format.
pub const Decoded = struct {
    topology: bool,
    direction: Direction,
    node: []const u8,
    edge: ?Edge,
};

pub fn decode(bytes: []const u8, scratch: []u8) !Decoded {
    if (bytes.len == 0 or bytes.len > tree.max_key_bytes) return error.InvalidGraphPageKey;
    if (scratch.len < bytes.len) return error.GraphPageKeyScratchTooSmall;
    const Parser = struct {
        bytes: []const u8,
        scratch: []u8,
        pos: usize = 1,
        used: usize = 0,

        fn component(self: *@This()) ![]const u8 {
            const begin = self.used;
            while (self.pos < self.bytes.len) {
                const byte = self.bytes[self.pos];
                self.pos += 1;
                if (byte == 0) {
                    if (self.pos == self.bytes.len) return error.InvalidGraphPageKey;
                    const escaped = self.bytes[self.pos];
                    self.pos += 1;
                    if (escaped == 0) return self.scratch[begin..self.used];
                    if (escaped != 255) return error.InvalidGraphPageKey;
                }
                self.scratch[self.used] = byte;
                self.used += 1;
            }
            return error.InvalidGraphPageKey;
        }
    };
    var parser: Parser = .{ .bytes = bytes, .scratch = scratch };
    var direction: Direction = .outgoing;
    var node: []const u8 = undefined;
    var kind: []const u8 = undefined;
    const is_topology = bytes[0] == topology_tag;
    if (is_topology) {
        kind = try parser.component();
        node = try parser.component();
    } else {
        if (bytes[0] != adjacency_tag) return error.InvalidGraphPageKey;
        node = try parser.component();
        if (parser.pos == bytes.len) return error.InvalidGraphPageKey;
        direction = switch (bytes[parser.pos]) {
            0 => .member,
            1 => .outgoing,
            2 => .incoming,
            else => return error.InvalidGraphPageKey,
        };
        parser.pos += 1;
        if (direction == .member) {
            if (node.len == 0 or parser.pos != bytes.len) return error.InvalidGraphPageKey;
            return .{ .topology = false, .direction = .member, .node = node, .edge = null };
        }
        kind = try parser.component();
    }
    @import("../../graph/edge_type.zig").validateStored(kind) catch return error.InvalidGraphPageKey;
    const neighbor = try parser.component();
    if (node.len == 0 or neighbor.len == 0 or bytes.len - parser.pos < 4) return error.InvalidGraphPageKey;
    const ordered = std.mem.readInt(u32, bytes[parser.pos..][0..4], .big);
    parser.pos += 4;
    const bits = if (ordered & 0x80000000 != 0) ordered ^ 0x80000000 else ~ordered;
    const weight: f32 = @bitCast(bits);
    if (!std.math.isFinite(weight) or bits == 0x80000000) return error.InvalidGraphPageKey;
    const table = try parser.component();
    if (bytes.len - parser.pos != 4 or ((direction == .incoming or is_topology) and table.len != 0))
        return error.InvalidGraphPageKey;
    return .{
        .topology = is_topology,
        .direction = direction,
        .node = node,
        .edge = .{
            .source = if (direction == .incoming) neighbor else node,
            .target = if (direction == .incoming) node else neighbor,
            .kind = kind,
            .table = if (table.len == 0) null else table,
            .weight = weight,
            .occurrence = std.mem.readInt(u32, bytes[parser.pos..][0..4], .big),
        },
    };
}

test "serverless graph page keys roundtrip binary identities qualification weights and duplicates" {
    const alloc = std.testing.allocator;
    var scratch: [tree.max_key_bytes]u8 = undefined;
    const original: Edge = .{ .source = "a\x00b", .target = "x\xff", .kind = "k\x00", .weight = -2.5, .occurrence = 3 };
    for ([_]Direction{ .outgoing, .incoming }) |direction| {
        var key = try Key.edge(alloc, original, direction);
        defer key.deinit(alloc);
        const parsed = try decode(key.bytes.items, &scratch);
        try std.testing.expectEqual(direction, parsed.direction);
        try std.testing.expectEqualDeep(original, parsed.edge.?);
    }
    var topology = try Key.topologyEdge(alloc, original);
    defer topology.deinit(alloc);
    try std.testing.expectEqualDeep(original, (try decode(topology.bytes.items, &scratch)).edge.?);
    var qualified = original;
    qualified.table = "remote";
    var key = try Key.edge(alloc, qualified, .outgoing);
    defer key.deinit(alloc);
    try std.testing.expectEqualDeep(qualified, (try decode(key.bytes.items, &scratch)).edge.?);
    try std.testing.expectError(error.InvalidGraphPageKey, Key.topologyEdge(alloc, qualified));
    try std.testing.expectError(error.InvalidGraphPageKey, Key.edge(alloc, qualified, .incoming));
    qualified.table = "";
    try std.testing.expectError(error.InvalidGraphPageKey, Key.edge(alloc, qualified, .outgoing));
    qualified.table = null;
    qualified.kind = "";
    try std.testing.expectError(error.InvalidGraphEdges, Key.edge(alloc, qualified, .outgoing));
    qualified.kind = "\xff";
    try std.testing.expectError(error.InvalidGraphEdges, Key.topologyEdge(alloc, qualified));
}

test "serverless graph page key prefixes route exact nodes and preserve numeric edge order" {
    const alloc = std.testing.allocator;
    var prefix = try Key.node(alloc, "a");
    defer prefix.deinit(alloc);
    const upper = try prefix.successorAlloc(alloc);
    defer alloc.free(upper);
    var previous: Key = .{};
    defer previous.deinit(alloc);
    for ([_]f32{ -100, -1, 0, 1, 100 }) |weight| {
        var key = try Key.edge(alloc, .{ .source = "a", .target = "b", .kind = "k", .weight = weight }, .outgoing);
        errdefer key.deinit(alloc);
        try std.testing.expect(std.mem.startsWith(u8, key.bytes.items, prefix.bytes.items));
        try std.testing.expect(std.mem.order(u8, key.bytes.items, upper) == .lt);
        if (previous.bytes.items.len != 0) try std.testing.expect(std.mem.order(u8, previous.bytes.items, key.bytes.items) == .lt);
        previous.deinit(alloc);
        previous = key;
    }
    var longer = try Key.node(alloc, "aa");
    defer longer.deinit(alloc);
    try std.testing.expect(std.mem.order(u8, longer.bytes.items, upper) == .gt);
}

test "serverless graph page keys admit the stored edge type byte limit" {
    const alloc = std.testing.allocator;
    const kind = [_]u8{'k'} ** @import("../../graph/edge_type.zig").max_bytes;
    var key = try Key.edge(alloc, .{ .source = "source", .target = "target", .kind = &kind }, .outgoing);
    defer key.deinit(alloc);
    var scratch: [tree.max_key_bytes]u8 = undefined;
    try std.testing.expectEqualStrings(&kind, (try decode(key.bytes.items, &scratch)).edge.?.kind);
}
