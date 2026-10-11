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

//! Bounded logical plans: own generation prefixes and node identities, never
//! file offsets, decoded scores, or caller output buffers. Values are always
//! resolved through the current scope's pinned checkpoint.
const std = @import("std");
const keys = @import("internal_keys.zig");
const ScoreSink = @import("backend_types.zig").ScoreSink;

pub const PreparedScorePlan = struct {
    pub const max_bytes = 256 * 1024;
    pub const max_keys = 4096;
    const Target = struct {
        key_offset: u32,
        key_len: u32,
        column: u16,
        row: u16,
        fn key(target: @This(), bytes: []const u8) []const u8 {
            return bytes[target.key_offset..][0..target.key_len];
        }
    };
    backing: ?std.mem.Allocator = null,
    storage: ?[]u8 = null,
    nodes: []const []const u8 = &.{},
    prefixes: []const ?[]const u8 = &.{},
    read_keys: []const []const u8 = &.{},
    targets: []const Target = &.{},
    ends: []const usize = &.{},
    valid: bool = false,
    // Targets keep canonical row identities; requests only update this mapping.
    row_remap: []u16 = &.{},
    row_identity: []u16 = &.{},
    row_heads: []u16 = &.{},
    row_cursors: []u16 = &.{},
    duplicate_next: []u16 = &.{},
    row_lookup_valid: bool = false,
    row_order_identity: bool = true,

    pub fn deinit(self: *@This()) void {
        if (self.storage) |storage| self.backing.?.free(storage);
        self.* = .{};
    }

    pub fn prepare(self: *@This(), backing: std.mem.Allocator, prefixes: []const ?[]const u8, nodes: []const []const u8) !bool {
        if (self.matches(prefixes, nodes)) return true;
        if (self.reusePermutation(prefixes, nodes)) return true;
        const estimate = requiredBytes(prefixes, nodes) orelse {
            self.deinit();
            return false;
        };
        errdefer self.deinit();
        const same_backing = if (self.backing) |old| old.ptr == backing.ptr and old.vtable == backing.vtable else false;
        if (!same_backing or self.storage.?.len < estimate) {
            self.deinit();
            self.backing = backing;
            const capacity = @min(max_bytes, std.math.ceilPowerOfTwo(usize, estimate) catch max_bytes);
            self.storage = try backing.alloc(u8, capacity);
        }
        self.valid = false;
        var buffer = std.heap.FixedBufferAllocator.init(self.storage.?);
        const a = buffer.allocator();
        var count: usize = 0;
        for (prefixes) |prefix| if (prefix != null) {
            count += nodes.len;
        };
        const owned_nodes = try a.alloc([]const u8, nodes.len);
        var node_bytes: usize = 0;
        for (nodes) |node| node_bytes += node.len;
        const identities = try a.alloc(u8, node_bytes);
        var identity_offset: usize = 0;
        for (owned_nodes, nodes) |*out, node| {
            out.* = identities[identity_offset..][0..node.len];
            @memcpy(identities[identity_offset..][0..node.len], node);
            identity_offset += node.len;
        }
        const owned_prefixes = try a.alloc(?[]const u8, prefixes.len);
        for (owned_prefixes, prefixes) |*out, prefix| out.* = if (prefix) |p| try a.dupe(u8, p) else null;
        const rows = try a.alloc(usize, owned_nodes.len);
        for (rows, 0..) |*row, i| row.* = i;
        const RowOrder = struct {
            nodes: []const []const u8,
            fn less(orderer: @This(), x: usize, y: usize) bool {
                const order = std.mem.order(u8, orderer.nodes[x], orderer.nodes[y]);
                return order == .lt or (order == .eq and x < y);
            }
        };
        var ascending = true;
        var descending = true;
        for (owned_nodes, 0..) |node, i| {
            if (i == 0) continue;
            const order = std.mem.order(u8, owned_nodes[i - 1], node);
            ascending = ascending and order != .gt;
            descending = descending and order != .lt;
            if (!ascending and !descending) break;
        }
        if (!ascending) {
            if (descending) std.mem.reverse(usize, rows) else std.mem.sort(usize, rows, RowOrder{ .nodes = owned_nodes }, RowOrder.less);
        }
        const columns = try a.alloc(usize, owned_prefixes.len);
        var column_count: usize = 0;
        for (owned_prefixes, 0..) |prefix, i| if (prefix != null) {
            columns[column_count] = i;
            column_count += 1;
        };
        const ColumnOrder = struct {
            prefixes: []const ?[]const u8,
            fn less(orderer: @This(), x: usize, y: usize) bool {
                const order = std.mem.order(u8, orderer.prefixes[x].?, orderer.prefixes[y].?);
                return order == .lt or (order == .eq and x < y);
            }
        };
        std.mem.sort(usize, columns[0..column_count], ColumnOrder{ .prefixes = owned_prefixes }, ColumnOrder.less);
        const targets = try a.alloc(Target, count);
        var index: usize = 0;
        var position: usize = 0;
        var previous_prefix: ?[]const u8 = null;
        var overlapping_prefixes = false;
        while (position < column_count) {
            const p = owned_prefixes[columns[position]].?;
            if (previous_prefix) |previous| {
                if (std.mem.startsWith(u8, p, previous)) overlapping_prefixes = true;
            }
            previous_prefix = p;
            var end = position + 1;
            while (end < column_count and std.mem.eql(u8, p, owned_prefixes[columns[end]].?)) : (end += 1) {}
            for (rows) |row| {
                const node = owned_nodes[row];
                const encoded = try a.alloc(u8, p.len + keys.encodedComponentLen(node));
                @memcpy(encoded[0..p.len], p);
                _ = keys.encodeComponent(encoded[p.len..], node);
                for (columns[position..end]) |column| {
                    targets[index] = .{ .key_offset = @intCast(@intFromPtr(encoded.ptr) - @intFromPtr(self.storage.?.ptr)), .key_len = @intCast(encoded.len), .column = @intCast(column), .row = @intCast(row) };
                    index += 1;
                }
            }
            position = end;
        }
        // Nonoverlapping prefixes and the component encoding preserve row
        // order. Only ambiguous prefix ranges need a full physical-key sort.
        if (overlapping_prefixes) {
            const Order = struct {
                fn less(bytes: []const u8, x: Target, y: Target) bool {
                    return std.mem.order(u8, x.key(bytes), y.key(bytes)) == .lt;
                }
            };
            std.mem.sort(Target, targets, @as([]const u8, self.storage.?), Order.less);
        }
        const read_keys = try a.alloc([]const u8, count);
        const ends = try a.alloc(usize, count);
        var unique: usize = 0;
        for (targets, 0..) |target, i| {
            const key = target.key(self.storage.?);
            if (i == 0 or !std.mem.eql(u8, key, targets[i - 1].key(self.storage.?))) {
                read_keys[unique] = key;
                unique += 1;
            }
            ends[unique - 1] = i + 1;
        }
        self.row_remap = try a.alloc(u16, nodes.len);
        self.row_identity = try a.alloc(u16, nodes.len);
        self.duplicate_next = try a.alloc(u16, nodes.len);
        const slots = std.math.ceilPowerOfTwo(usize, @max(@as(usize, 1), nodes.len * 2)) catch return error.OutOfMemory;
        self.row_heads = try a.alloc(u16, slots);
        self.row_cursors = try a.alloc(u16, slots);
        @memset(self.row_heads, 0);
        for (0..nodes.len) |row| {
            self.row_remap[row] = @intCast(row);
            self.row_identity[row] = @intCast(row);
        }
        self.row_order_identity = true;
        self.row_lookup_valid = true;
        lookup: for (owned_nodes, 0..) |node, row| {
            var probes: usize = 0;
            var slot = @as(usize, @truncate(std.hash.Wyhash.hash(0, node))) & (slots - 1);
            while (self.row_heads[slot] != 0 and !std.mem.eql(u8, node, owned_nodes[self.row_heads[slot] - 1])) {
                probes += 1;
                if (probes >= 32) {
                    // Adversarial collisions use the bounded sort path.
                    self.row_lookup_valid = false;
                    break :lookup;
                }
                slot = (slot + 1) & (slots - 1);
            }
            self.duplicate_next[row] = self.row_heads[slot];
            self.row_heads[slot] = @intCast(row + 1);
        }
        self.nodes = owned_nodes;
        self.prefixes = owned_prefixes;
        self.targets = targets;
        self.read_keys = read_keys[0..unique];
        self.ends = ends[0..unique];
        self.valid = true;
        return true;
    }

    // Include copied identities, target fanout, key spans, and alignment.
    // Caller inputs must not borrow this plan's storage across preparation.
    fn requiredBytes(prefixes: []const ?[]const u8, nodes: []const []const u8) ?usize {
        if (nodes.len >= std.math.maxInt(u16)) return null;
        const slots = std.math.ceilPowerOfTwo(usize, @max(@as(usize, 1), nodes.len * 2)) catch return null;
        var estimate: usize = 1024 + nodes.len * 3 * @sizeOf(u16) + slots * 2 * @sizeOf(u16);
        var count: usize = 0;
        for (nodes) |node| {
            estimate = std.math.add(usize, estimate, node.len +| @sizeOf([]const u8) +| @sizeOf(usize)) catch return null;
            if (estimate > max_bytes) return null;
        }
        for (prefixes) |prefix| {
            estimate = std.math.add(usize, estimate, @sizeOf(?[]const u8) + @sizeOf(usize)) catch return null;
            if (prefix) |p| {
                estimate = std.math.add(usize, estimate, p.len) catch return null;
                for (nodes) |node| {
                    count += 1;
                    if (count > max_keys) return null;
                    const bytes = std.math.add(usize, p.len, keys.encodedComponentLen(node)) catch return null;
                    estimate = std.math.add(usize, estimate, bytes +| @sizeOf(Target) +| @sizeOf([]const u8) +| @sizeOf(usize)) catch return null;
                    if (estimate > max_bytes) return null;
                }
            }
            if (estimate > max_bytes) return null;
        }
        return estimate;
    }

    fn prefixesMatch(self: *const @This(), prefixes: []const ?[]const u8) bool {
        if (!self.valid or prefixes.len != self.prefixes.len) return false;
        for (prefixes, self.prefixes) |prefix, old| {
            if (prefix) |p| {
                if (old == null or !std.mem.eql(u8, p, old.?)) return false;
            } else if (old != null) return false;
        }
        return true;
    }

    fn matches(self: *const @This(), prefixes: []const ?[]const u8, nodes: []const []const u8) bool {
        if (!self.prefixesMatch(prefixes) or nodes.len != self.nodes.len) return false;
        if (self.row_order_identity) {
            for (nodes, self.nodes) |node, old| if (!std.mem.eql(u8, node, old)) return false;
        } else {
            for (nodes, self.row_identity) |node, identity| if (!std.mem.eql(u8, node, self.nodes[identity])) return false;
        }
        return true;
    }

    /// Match a multiset in linear expected time without re-encoding or sorting.
    /// Hashes select a probe slot only; exact bytes and duplicate counts decide.
    fn reusePermutation(self: *@This(), prefixes: []const ?[]const u8, nodes: []const []const u8) bool {
        if (!self.row_lookup_valid or !self.prefixesMatch(prefixes) or nodes.len != self.nodes.len) return false;
        @memcpy(self.row_cursors, self.row_heads);
        const mask = self.row_heads.len - 1;
        var identity_mapping = true;
        for (nodes, 0..) |node, row| {
            var probes: usize = 0;
            var slot = @as(usize, @truncate(std.hash.Wyhash.hash(0, node))) & mask;
            while (self.row_heads[slot] != 0 and !std.mem.eql(u8, node, self.nodes[self.row_heads[slot] - 1])) {
                probes += 1;
                if (probes >= 32) return false;
                slot = (slot + 1) & mask;
            }
            const next = self.row_cursors[slot];
            if (next == 0) return false;
            const identity = next - 1;
            self.row_cursors[slot] = self.duplicate_next[identity];
            self.row_remap[identity] = @intCast(row);
            self.row_identity[row] = identity;
            identity_mapping = identity_mapping and identity == row;
        }
        self.row_order_identity = identity_mapping;
        return true;
    }

    pub const Output = struct {
        plan: *const PreparedScorePlan,
        columns: []const []?f64,
        pub fn sink(self: *@This()) ScoreSink {
            return .{ .context = self, .write = if (self.plan.row_order_identity) writeCanonical else writeRemapped };
        }
        fn writeCanonical(ctx: *anyopaque, index: usize, score: f64) void {
            writeRows(ctx, index, score, false);
        }
        fn writeRemapped(ctx: *anyopaque, index: usize, score: f64) void {
            writeRows(ctx, index, score, true);
        }
        fn writeRows(ctx: *anyopaque, index: usize, score: f64, comptime remapped: bool) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.plan.read_keys.len == self.plan.targets.len) {
                const target = self.plan.targets[index];
                self.columns[target.column][if (remapped) self.plan.row_remap[target.row] else target.row] = score;
                return;
            }
            const start = if (index == 0) 0 else self.plan.ends[index - 1];
            for (self.plan.targets[start..self.plan.ends[index]]) |target| self.columns[target.column][if (remapped) self.plan.row_remap[target.row] else target.row] = score;
        }
    };
};

test "prepared score plan canonical remapping owns identities and duplicate fanout" {
    var plan = PreparedScorePlan{};
    defer plan.deinit();
    var owned: [4][1]u8 = .{ .{'c'}, .{'a'}, .{'b'}, .{'a'} };
    const nodes: [4][]const u8 = .{ &owned[0], &owned[1], &owned[2], &owned[3] };
    const prefixes = [_]?[]const u8{ "", "p/", "", null };
    try std.testing.expect(try plan.prepare(std.testing.allocator, &prefixes, &nodes));
    const storage = plan.storage.?.ptr;
    const physical = plan.read_keys.ptr;
    owned[0][0] = 'z';
    const requests = [_][4][]const u8{
        .{ "a", "c", "a", "b" },
        .{ "b", "a", "c", "a" },
        .{ "b", "a", "c", "a" },
        .{ "a", "c", "b", "b" }, // Different duplicate counts rebuild.
        .{ "d", "c", "b", "a" }, // Different identities rebuild.
    };
    for (requests, 0..) |request, iteration| {
        try std.testing.expect(try plan.prepare(std.testing.allocator, &prefixes, &request));
        if (iteration < 3) {
            try std.testing.expectEqual(storage, plan.storage.?.ptr);
            try std.testing.expectEqual(physical, plan.read_keys.ptr);
        }
        var scores: [4][4]?f64 = @splat(@splat(null));
        const columns = [_][]?f64{ &scores[0], &scores[1], &scores[2], &scores[3] };
        var output = PreparedScorePlan.Output{ .plan = &plan, .columns = &columns };
        const sink = output.sink();
        for (plan.read_keys, 0..) |key, i| sink.write(sink.context, i, @floatFromInt(key[key.len - 3]));
        for (scores, prefixes) |column, prefix| for (column, request) |score, node| {
            try std.testing.expectEqual(if (prefix != null) @as(?f64, @floatFromInt(node[0])) else null, score);
        };
    }
}

fn canonicalAllocationScenario(a: std.mem.Allocator) !void {
    var plan = PreparedScorePlan{};
    defer plan.deinit();
    try std.testing.expect(try plan.prepare(a, &.{"p/"}, &.{ "a", "b", "a" }));
    try std.testing.expect(try plan.prepare(a, &.{"p/"}, &.{ "b", "a", "a" }));
    try std.testing.expect(try plan.prepare(a, &.{"q/"}, &.{ "c", "b", "a" }));
}

test "prepared score plan canonical allocation failure releases ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, canonicalAllocationScenario, .{});
}

test "prepared score plan bounds hash collision work and retains sorting fallback" {
    var plan = PreparedScorePlan{};
    defer plan.deinit();
    var storage: [33][16]u8 = undefined;
    var nodes: [33][]const u8 = undefined;
    var candidate: usize = 0;
    for (&storage, &nodes) |*buf, *node| {
        while (true) {
            const key = try std.fmt.bufPrint(buf, "{d}", .{candidate});
            candidate += 1;
            if (std.hash.Wyhash.hash(0, key) & 127 == 0) {
                node.* = key;
                break;
            }
        }
    }
    try std.testing.expect(try plan.prepare(std.testing.allocator, &.{"p/"}, &nodes));
    try std.testing.expect(!plan.row_lookup_valid);
    std.mem.reverse([]const u8, &nodes);
    try std.testing.expect(try plan.prepare(std.testing.allocator, &.{"p/"}, &nodes));
    try std.testing.expect(!plan.row_lookup_valid);
    try std.testing.expectEqual(@as(usize, 33), plan.read_keys.len);
}
