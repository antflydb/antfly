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

    pub fn deinit(self: *@This()) void {
        if (self.storage) |storage| self.backing.?.free(storage);
        self.* = .{};
    }

    pub fn prepare(self: *@This(), backing: std.mem.Allocator, prefixes: []const ?[]const u8, nodes: []const []const u8) !bool {
        if (self.matches(prefixes, nodes)) return true;
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
        var estimate: usize = 1024;
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

    fn matches(self: *const @This(), prefixes: []const ?[]const u8, nodes: []const []const u8) bool {
        if (!self.valid or prefixes.len != self.prefixes.len or nodes.len != self.nodes.len) return false;
        for (nodes, self.nodes) |node, old| if (!std.mem.eql(u8, node, old)) return false;
        for (prefixes, self.prefixes) |prefix, old| {
            if (prefix) |p| {
                if (old == null or !std.mem.eql(u8, p, old.?)) return false;
            } else if (old != null) return false;
        }
        return true;
    }

    pub const Output = struct {
        plan: *const PreparedScorePlan,
        columns: []const []?f64,
        pub fn sink(self: *@This()) ScoreSink {
            return .{ .context = self, .write = write };
        }
        fn write(ctx: *anyopaque, index: usize, score: f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.plan.read_keys.len == self.plan.targets.len) {
                const target = self.plan.targets[index];
                self.columns[target.column][target.row] = score;
                return;
            }
            const start = if (index == 0) 0 else self.plan.ends[index - 1];
            for (self.plan.targets[start..self.plan.ends[index]]) |target| self.columns[target.column][target.row] = score;
        }
    };
};
