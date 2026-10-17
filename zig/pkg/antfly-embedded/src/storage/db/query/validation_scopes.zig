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

const std = @import("std");
const Selector = @import("json_projection_plan.zig").Selector;

/// Owned, immutable descriptions of visited-key contracts. Never retain a
/// selector pointer: a prepared view can outlive the plan that validated it.
/// Bounds are per response; exhaustion merely disables structural shortcuts.
pub const Scopes = struct {
    const Node = struct { start: u32, len: u32, next: u32 };
    const Field = struct { start: u32, len: u32, child: u32 };
    const Pending = struct { name: []const u8, child: u32 };
    nodes: std.ArrayListUnmanaged(Node) = .empty,
    fields: std.ArrayListUnmanaged(Field) = .empty,
    names: std.ArrayListUnmanaged(u8) = .empty,
    heads: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    const max_nodes = 1024;
    const max_fields = 4096;
    const max_names = 64 * 1024;
    const max_steps = 65536;
    const Work = struct { steps: usize = 0, active_fields: usize = 0 };

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.nodes.deinit(alloc);
        self.fields.deinit(alloc);
        self.names.deinit(alloc);
        self.heads.deinit(alloc);
        self.* = .{};
    }
    pub fn intern(self: *@This(), alloc: std.mem.Allocator, selector: *const Selector) !?u32 {
        var work: Work = .{};
        return self.internValue(alloc, selector, &work, 0);
    }
    fn internValue(self: *@This(), alloc: std.mem.Allocator, selector: *const Selector, work: *Work, depth: usize) error{OutOfMemory}!?u32 {
        const count = selector.fields.count();
        if (count == 0) return 0; // A terminal selection imposes no child checks.
        if (depth > @import("json_projection_plan.zig").max_path_depth or work.steps >= max_steps or count > max_fields - work.active_fields) return null;
        work.steps += 1;
        work.active_fields += count;
        defer work.active_fields -= count;
        var inline_fields: [8]Pending = undefined;
        const fields = if (count <= inline_fields.len) inline_fields[0..count] else try alloc.alloc(Pending, count);
        defer if (count > inline_fields.len) alloc.free(fields);
        var it = selector.fields.iterator();
        var i: usize = 0;
        while (it.next()) |entry| : (i += 1) {
            if (work.steps >= max_steps) return null;
            work.steps += 1;
            if (std.mem.eql(u8, entry.key_ptr.*, "*")) return null;
            fields[i] = .{ .name = entry.key_ptr.*, .child = (try self.internValue(alloc, entry.value_ptr.*, work, depth + 1)) orelse return null };
        }
        std.mem.sort(Pending, fields, {}, struct {
            fn less(_: void, a: Pending, b: Pending) bool {
                return std.mem.lessThan(u8, a.name, b.name);
            }
        }.less);
        var hash = std.hash.Wyhash.init(0);
        var bytes: usize = 0;
        for (fields) |field| {
            bytes = std.math.add(usize, bytes, field.name.len) catch return null;
            hash.update(std.mem.asBytes(&field.name.len));
            hash.update(field.name);
            hash.update(std.mem.asBytes(&field.child));
        }
        const key = hash.final();
        var found = self.heads.get(key) orelse 0;
        while (found != 0) {
            const node = self.nodes.items[found - 1];
            const owned = self.fields.items[node.start..][0..node.len];
            const equal = matched: {
                if (owned.len != fields.len) break :matched false;
                for (owned, fields) |left, right| if (left.child != right.child or !std.mem.eql(u8, self.name(left), right.name)) break :matched false;
                break :matched true;
            };
            if (equal) return found;
            found = node.next;
        }
        if (self.nodes.items.len == max_nodes or fields.len > max_fields - self.fields.items.len or bytes > max_names - self.names.items.len) return null;
        try reserve(Node, &self.nodes, alloc, self.nodes.items.len + 1, max_nodes);
        try reserve(Field, &self.fields, alloc, self.fields.items.len + fields.len, max_fields);
        try reserve(u8, &self.names, alloc, self.names.items.len + bytes, max_names);
        const head = try self.heads.getOrPut(alloc, key);
        const id: u32 = @intCast(self.nodes.items.len + 1);
        const start: u32 = @intCast(self.fields.items.len);
        for (fields) |field| {
            self.fields.appendAssumeCapacity(.{ .start = @intCast(self.names.items.len), .len = @intCast(field.name.len), .child = field.child });
            self.names.appendSliceAssumeCapacity(field.name);
        }
        self.nodes.appendAssumeCapacity(.{ .start = start, .len = @intCast(fields.len), .next = if (head.found_existing) head.value_ptr.* else 0 });
        head.value_ptr.* = id;
        return id;
    }
    fn reserve(comptime T: type, list: *std.ArrayListUnmanaged(T), alloc: std.mem.Allocator, needed: usize, limit: usize) !void {
        if (needed > list.capacity) try list.ensureTotalCapacityPrecise(alloc, @min(limit, @max(needed, @max(8, list.capacity * 2))));
    }
    fn name(self: *const @This(), field: Field) []const u8 {
        return self.names.items[field.start..][0..field.len];
    }
    pub fn covers(self: *const @This(), id: u32, required: *const Selector) bool {
        var steps: usize = 0;
        return self.coversValue(id, required, &steps, 0);
    }
    fn coversValue(self: *const @This(), id: u32, required: *const Selector, steps: *usize, depth: usize) bool {
        if (required.fields.count() == 0) return true;
        if (id == 0 or id > self.nodes.items.len or depth > @import("json_projection_plan.zig").max_path_depth) return false;
        const node = self.nodes.items[id - 1];
        const fields = self.fields.items[node.start..][0..node.len];
        var it = required.fields.iterator();
        while (it.next()) |entry| {
            if (steps.* >= max_steps) return false;
            steps.* += 1;
            if (std.mem.eql(u8, entry.key_ptr.*, "*")) return false;
            var lo: usize = 0;
            var hi: usize = fields.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                switch (std.mem.order(u8, self.name(fields[mid]), entry.key_ptr.*)) {
                    .lt => lo = mid + 1,
                    .gt => hi = mid,
                    .eq => {
                        if (!self.coversValue(fields[mid].child, entry.value_ptr.*, steps, depth + 1)) return false;
                        break;
                    },
                }
            } else return false;
        }
        return true;
    }
};

test "public graph metric borrowed wire scope snapshots outlive selectors and preserve coverage" {
    const a = std.testing.allocator;
    var scopes: Scopes = .{};
    defer scopes.deinit(a);
    var id: u32 = undefined;
    {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const selected = try @import("json_projection_plan.zig").compile(arena.allocator(), &.{ .{ .parts = &.{ "x", "a" } }, .{ .parts = &.{ "x", "b" } }, .{ .parts = &.{"y"} } });
        id = (try scopes.intern(a, selected)).?;
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const compile = @import("json_projection_plan.zig").compile;
    const narrower = try compile(arena.allocator(), &.{.{ .parts = &.{ "x", "a" } }});
    const stronger = try compile(arena.allocator(), &.{.{ .parts = &.{ "x", "a", "deep" } }});
    const different = try compile(arena.allocator(), &.{.{ .parts = &.{"z"} }});
    const reordered = try compile(arena.allocator(), &.{ .{ .parts = &.{"y"} }, .{ .parts = &.{ "x", "b" } }, .{ .parts = &.{ "x", "a" } } });
    try std.testing.expect(scopes.covers(id, narrower));
    try std.testing.expect(!scopes.covers(id, stronger));
    try std.testing.expect(!scopes.covers(id, different));
    try std.testing.expectEqual(id, (try scopes.intern(a, reordered)).?);
    const Check = struct {
        fn run(alloc: std.mem.Allocator, selected: *const Selector) !void {
            var owned: Scopes = .{};
            defer owned.deinit(alloc);
            const captured = (try owned.intern(alloc, selected)).?;
            try std.testing.expect(owned.covers(captured, selected));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{reordered});
}

test "public graph metric borrowed wire scope budgets fall back while retaining earlier proofs" {
    const a = std.testing.allocator;
    var budget: @import("../../lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 512 * 1024 };
    {
        var scopes: Scopes = .{};
        defer scopes.deinit(budget.allocator());
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const first = try @import("json_projection_plan.zig").compile(arena.allocator(), &.{.{ .parts = &.{"first"} }});
        const original = (try scopes.intern(budget.allocator(), first)).?;
        for (0..Scopes.max_nodes) |i| {
            const name = try std.fmt.allocPrint(arena.allocator(), "field{d}", .{i});
            const selected = try @import("json_projection_plan.zig").compile(arena.allocator(), &.{.{ .parts = &.{name} }});
            const result = try scopes.intern(budget.allocator(), selected);
            if (i == Scopes.max_nodes - 1) try std.testing.expect(result == null);
        }
        try std.testing.expect(scopes.covers(original, first));
        try std.testing.expectEqual(original, (try scopes.intern(budget.allocator(), first)).?);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.peak < 256 * 1024);
}
