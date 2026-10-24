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

//! Extendible radix directory over fixed typed hash slots. Shared statement
//! pages evict independently; probes decode payload keys only after a complete
//! 64-bit hash match. Bounded tiles share directory and leaf page pins.
const std = @import("std");
const paging = @import("execution_pager.zig");
const spill = @import("spill.zig");
const width = 512;
const Slot = struct { hash: u64 = 0, head: u64 = spill.none };
const Node = struct { table: u64, left: u64 = 0, right: u64 = 0, used: u32 = 0, depth: u8 = 0 };
pub const Index = struct {
    nodes: paging.Array(Node),
    slots: paging.Array(Slot),
    splits: u64 = 0,
    pub fn init(manager: *spill.Manager) !Index {
        const pager = try manager.pager();
        var self: Index = .{ .nodes = .{ .pager = pager }, .slots = .{ .pager = pager } };
        errdefer self.deinit();
        try self.slots.appendRepeated(.{}, width);
        try self.nodes.append(.{ .table = 0 });
        return self;
    }
    pub fn deinit(self: *Index) void {
        self.slots.deinit();
        self.nodes.deinit();
    }
    fn leaf(self: *const Index, hash: u64) !usize {
        var id: usize = 0;
        while (true) {
            const node = try self.nodes.get(id);
            if (node.table != spill.none) return id;
            if (node.depth >= 64) return error.InvalidSqlSpill;
            id = @intCast(if (hash & (@as(u64, 1) << @as(u6, @intCast(63 - node.depth))) == 0) node.left else node.right);
        }
    }
    fn position(values: []const Slot, hash: u64) usize {
        var i: usize = @intCast(hash & (width - 1));
        while (values[i].head != spill.none and values[i].hash != hash) i = (i + 1) % width;
        return i;
    }
    fn window(self: *const Index, table: u64, write: bool) !paging.Array(Slot).Window {
        return self.slots.pinPage(@as(usize, @intCast(table)) * width, write);
    }
    fn tableOffset(table: u64, begin: usize) usize {
        return @as(usize, @intCast(table)) * width - begin;
    }
    pub fn get(self: *const Index, hash: u64) !u64 {
        const node = try self.nodes.get(try self.leaf(hash));
        const held = try self.window(node.table, false);
        defer held.lease.release();
        const values = held.values[tableOffset(node.table, held.begin)..][0..width];
        return values[position(values, hash)].head;
    }
    pub fn set(self: *Index, hash: u64, head: u64) !void {
        if (head == spill.none) return error.InvalidSqlSpill;
        while (true) {
            const id = try self.leaf(hash);
            var node = try self.nodes.get(id);
            var inserted = false;
            {
                const held = try self.window(node.table, true);
                defer held.lease.release();
                const values = held.values[tableOffset(node.table, held.begin)..][0..width];
                const at = position(values, hash);
                if (values[at].head != spill.none or node.used < width * 3 / 4) {
                    inserted = values[at].head == spill.none;
                    values[at] = .{ .hash = hash, .head = head };
                } else {
                    // No pointer survives a split's allocation or eviction.
                    inserted = false;
                }
                if (!inserted and values[at].hash == hash and values[at].head == head) return;
                if (!inserted) {
                    // The leaf is full; mutation happens after releasing it.
                }
            }
            if (inserted) {
                node.used += 1;
                try self.nodes.set(id, node);
                return;
            }
            try self.split(id, node);
        }
    }
    fn writeTable(self: *Index, table: u64, values: *const [width]Slot) !void {
        const held = try self.window(table, true);
        defer held.lease.release();
        @memcpy(held.values[tableOffset(table, held.begin)..][0..width], values);
    }
    fn split(self: *Index, id: usize, node: Node) !void {
        if (node.depth >= 64) return error.InvalidSqlSpill;
        var left: [width]Slot = @splat(.{});
        var right: [width]Slot = @splat(.{});
        var right_count: u32 = 0;
        {
            const held = try self.window(node.table, false);
            defer held.lease.release();
            for (held.values[tableOffset(node.table, held.begin)..][0..width]) |slot| {
                if (slot.head == spill.none) continue;
                const high = slot.hash & (@as(u64, 1) << @as(u6, @intCast(63 - node.depth))) != 0;
                const values = if (high) &right else &left;
                values[position(values, slot.hash)] = slot;
                right_count += @intFromBool(high);
            }
        }
        const table = self.slots.len / width;
        try self.slots.appendRepeated(.{}, width);
        try self.writeTable(node.table, &left);
        try self.writeTable(table, &right);
        const child = self.nodes.len;
        try self.nodes.append(.{ .table = node.table, .used = node.used - right_count, .depth = node.depth + 1 });
        try self.nodes.append(.{ .table = table, .used = right_count, .depth = node.depth + 1 });
        try self.nodes.set(id, .{ .table = spill.none, .left = child, .right = child + 1, .depth = node.depth });
        self.splits += 1;
    }
    pub fn getMany(self: *const Index, hashes: []const ?u64, output: []u64) !void {
        if (hashes.len != output.len) return error.InvalidSqlSpill;
        var begin: usize = 0;
        while (begin < hashes.len) {
            const count = @min(128, hashes.len - begin);
            var ids: [128]usize = @splat(0);
            var active: [128]bool = undefined;
            for (hashes[begin..][0..count], active[0..count], output[begin..][0..count]) |hash, *live, *head| {
                live.* = hash != null;
                head.* = spill.none;
            }
            var tables: [128]u64 = @splat(spill.none);
            while (true) {
                var selected: [128]usize = undefined;
                var lanes: [128]usize = undefined;
                var nodes: [128]Node = undefined;
                var pending: usize = 0;
                for (active[0..count], 0..) |live, i| if (live) {
                    selected[pending] = ids[i];
                    lanes[pending] = i;
                    pending += 1;
                };
                if (pending == 0) break;
                try self.nodes.getMany(selected[0..pending], nodes[0..pending]);
                for (nodes[0..pending], lanes[0..pending]) |node, i| {
                    if (node.table != spill.none) {
                        tables[i] = node.table;
                        active[i] = false;
                    } else {
                        if (node.depth >= 64) return error.InvalidSqlSpill;
                        const high = hashes[begin + i].? & (@as(u64, 1) << @as(u6, @intCast(63 - node.depth))) != 0;
                        ids[i] = @intCast(if (high) node.right else node.left);
                    }
                }
            }
            var done: [128]bool = @splat(false);
            for (tables[0..count], 0..) |table, i| {
                if (table == spill.none or done[i]) continue;
                const held = try self.window(table, false);
                defer held.lease.release();
                for (tables[0..count], 0..) |candidate, j| {
                    if (candidate == spill.none or done[j] or self.slots.pageStart(@as(usize, @intCast(candidate)) * width) != held.begin) continue;
                    const values = held.values[tableOffset(candidate, held.begin)..][0..width];
                    output[begin + j] = values[position(values, hashes[begin + j].?)].head;
                    done[j] = true;
                }
            }
            begin += count;
        }
    }
};

test "SQL partitioned spill hash slots preserve full hashes through one-frame eviction and batched splits" {
    const a = std.testing.allocator;
    var marker: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .page_residency_bytes = paging.page_bytes, .async_writes = false };
    defer manager.deinit();
    var index = try Index.init(&manager);
    defer index.deinit();
    const count = 1000;
    for (0..count) |i| try index.set((@as(u64, i) *% 0x9e3779b97f4a7c15 & ~@as(u64, 4095)) | 7, i + 1);
    try std.testing.expect(index.splits != 0);
    var hashes: [129]?u64 = undefined;
    var heads: [129]u64 = undefined;
    for (&hashes, 0..) |*hash, i| hash.* = if (i == 128) null else (@as(u64, count - 1 - i) *% 0x9e3779b97f4a7c15 & ~@as(u64, 4095)) | 7;
    try index.getMany(&hashes, &heads);
    for (heads[0..128], 0..) |head, i| try std.testing.expectEqual(@as(u64, count - i), head);
    try std.testing.expectEqual(spill.none, heads[128]);
    try index.set(hashes[0].?, 9999);
    try std.testing.expectEqual(@as(u64, 9999), try index.get(hashes[0].?));
    try std.testing.expectEqual(spill.none, try index.get(8));
    try std.testing.expect((try manager.pager()).evictions != 0);
    try std.testing.expect((try manager.pager()).restores != 0);
    try std.testing.expect((try manager.pager()).peak <= paging.page_bytes);
}

test "SQL partitioned spill directory unwinds allocation failures and cancellation" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var canceled = false;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &canceled, .checkpoint = struct {
                fn check(raw: *anyopaque) !void {
                    if (@as(*bool, @ptrCast(@alignCast(raw))).*) return error.Canceled;
                }
            }.check, .page_residency_bytes = paging.page_bytes, .async_writes = false };
            defer manager.deinit();
            var index = try Index.init(&manager);
            defer index.deinit();
            try index.set(7, 99);
            try std.testing.expectEqual(@as(u64, 99), try index.get(7));
            canceled = true;
            try std.testing.expectError(error.Canceled, index.get(7));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
