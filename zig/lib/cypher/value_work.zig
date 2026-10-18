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

//! Query-owned metadata for immutable data containers. Cache by allocation, not
//! by expanded contents: reduce() can build a DAG with exponentially many paths.
//! Every recursive operation reserves its expanded work before traversing it.
//! The allocator is the query arena. Backend values remain immutable and alive
//! through execution, so cached allocation keys cannot be recycled mid-query.
const std = @import("std");
const v = @import("value.zig");
const A = std.mem.Allocator;
const Key = struct { address: usize, length: usize, kind: enum { list, map, string } };
pub const Info = struct { hash: u64, depth: usize = 0, work: usize = 1, comparison_work: usize = 1, entity_work: usize = 1, unknown: bool = false, entities: bool = false };
pub const Work = struct {
    a: A,
    ptr: *anyopaque,
    charge: *const fn (*anyopaque, usize) anyerror!void,
    // Execution bindings must preserve representation: 1 and 1.0 compare equal
    // in Cypher but behave differently in integer division and nested values.
    // This mode has its own Work/cache and never changes GROUP BY semantics.
    binding_identity: bool = false,
    cache: std.AutoHashMapUnmanaged(Key, Info) = .empty,
    fields: std.AutoHashMapUnmanaged(Key, std.StringHashMapUnmanaged(usize)) = .empty,

    fn key(value: v.Value) ?Key {
        return switch (value) {
            .list => |items| .{ .address = @intFromPtr(items.ptr), .length = items.len, .kind = .list },
            .map => |items| .{ .address = @intFromPtr(items.ptr), .length = items.len, .kind = .map },
            .string => |item| .{ .address = @intFromPtr(item.ptr), .length = item.len, .kind = .string },
            else => null,
        };
    }
    pub fn lookup(self: *Work, entries: []const v.Entry, name: []const u8) !?v.Value {
        try self.charge(self.ptr, 1 + name.len / 64);
        // Tiny maps need no separate allocation; their bounded scans still pay
        // work. Larger immutable maps build a name index once per allocation.
        if (entries.len <= 32) {
            for (entries) |entry| {
                try self.charge(self.ptr, 1 + entry.name.len / 64);
                if (std.mem.eql(u8, entry.name, name)) return entry.value;
            }
            return null;
        }
        const cache_key = key(.{ .map = entries }).?;
        var index = self.fields.get(cache_key);
        if (index == null) {
            var built: std.StringHashMapUnmanaged(usize) = .empty;
            errdefer built.deinit(self.a);
            for (entries, 0..) |entry, position| {
                try self.charge(self.ptr, 1 + entry.name.len / 64);
                const field = try built.getOrPut(self.a, entry.name);
                if (!field.found_existing) field.value_ptr.* = position;
            }
            try self.fields.put(self.a, cache_key, built);
            index = built;
        }
        const position = index.?.get(name) orelse return null;
        return entries[position].value;
    }
    pub fn property(self: *Work, value: v.Value, name: []const u8) !v.Value {
        const entries = switch (value) {
            .null => return .null,
            .map => value.map,
            .node => value.node.properties,
            .relationship => value.relationship.properties,
            else => return error.CypherTypeMismatch,
        };
        return (try self.lookup(entries, name)) orelse .null;
    }
    fn mix(left: u64, right: u64) u64 {
        return std.hash.Wyhash.hash(left, std.mem.asBytes(&right));
    }
    fn text(seed: u64, input: []const u8) u64 {
        return std.hash.Wyhash.hash(mix(seed, input.len), input);
    }
    fn identity(seed: u64, id: v.Identity) u64 {
        return text(text(seed, id.table), id.key);
    }
    pub fn info(self: *Work, value: v.Value) anyerror!Info {
        return self.infoDepth(value, 0);
    }
    fn infoDepth(self: *Work, value: v.Value, depth: usize) anyerror!Info {
        if (depth > 64) return error.CypherValueDepthExceeded;
        try self.charge(self.ptr, 1);
        const cache_key = key(value);
        if (cache_key) |k| if (self.cache.get(k)) |cached| {
            if (depth + cached.depth > 64) return error.CypherValueDepthExceeded;
            return cached;
        };
        var result: Info = .{ .hash = if (self.binding_identity) @backingInt(std.meta.activeTag(value)) else v.Value.rank(value) };
        switch (value) {
            .null => result.unknown = true,
            .boolean => |item| result.hash = mix(result.hash, @intFromBool(item)),
            .integer => |number| result.hash = mix(result.hash, @bitCast(number)),
            .number => |number| {
                // Canonicalize exactly integral, in-range floats to INTEGER.
                // Casting every integer to f64 would collapse 512+ distinct
                // values near i64's boundary into each bucket.
                if (self.binding_identity) {
                    result.hash = mix(result.hash, @bitCast(number));
                } else if (number >= -9223372036854775808.0 and number < 9223372036854775808.0 and @trunc(number) == number) {
                    const integer: i64 = @intFromFloat(number);
                    result.hash = mix(result.hash, @bitCast(integer));
                } else result.hash = mix(mix(result.hash, 1), @bitCast(number));
            },
            .string => |item| {
                try self.charge(self.ptr, item.len / 64);
                result.hash = text(result.hash, item);
                result.work +|= item.len / 64;
                result.comparison_work = result.work;
            },
            .node => |node| {
                result.hash = identity(result.hash, node.identity);
                result.entities = true;
            },
            .relationship => |edge| {
                result.hash = text(identity(result.hash, edge.identity), edge.index);
                result.entities = true;
            },
            .path => |path| {
                result.entities = true;
                for (path.nodes) |node| result.hash = identity(result.hash, node.identity);
                for (path.relationships) |edge| result.hash = text(identity(result.hash, edge.identity), edge.index);
                result.work +|= path.nodes.len +| path.relationships.len;
                result.comparison_work = result.work;
                result.entity_work = result.work;
            },
            .list => |items| {
                result.hash = mix(result.hash, items.len);
                for (items) |item| {
                    const child = try self.infoDepth(item, depth + 1);
                    result.hash = mix(result.hash, child.hash);
                    result.depth = @max(result.depth, child.depth + 1);
                    result.work +|= child.work;
                    result.comparison_work +|= child.comparison_work;
                    result.entity_work +|= if (child.entities) child.entity_work else 1;
                    result.unknown = result.unknown or child.unknown;
                    result.entities = result.entities or child.entities;
                }
            },
            .map => |items| {
                // Map equality is independent of insertion order. Hash each
                // named value, then combine commutatively without sorting.
                var entries: u64 = 0;
                for (items) |item| {
                    const child = try self.infoDepth(item.value, depth + 1);
                    try self.charge(self.ptr, item.name.len / 64);
                    entries +%= text(child.hash, item.name);
                    result.depth = @max(result.depth, child.depth + 1);
                    result.work +|= child.work +| (item.name.len / 64);
                    result.comparison_work +|= child.comparison_work +| (item.name.len / 64);
                    result.entity_work +|= if (child.entities) child.entity_work else 1;
                    result.unknown = result.unknown or child.unknown;
                    result.entities = result.entities or child.entities;
                }
                result.hash = mix(mix(result.hash, items.len), entries);
                // The existing total map ordering compares sorted keys using
                // repeated selection. Reserve that work as well as traversal.
                result.comparison_work +|= items.len *| items.len;
            },
        }
        if (cache_key) |k| try self.cache.put(self.a, k, result);
        return result;
    }
    pub fn traversal(self: *Work, value: v.Value) !void {
        try self.charge(self.ptr, (try self.info(value)).work);
    }
    /// Wire depth includes entity property maps and path members. Entity
    /// identities are stable, but their properties can change during writes;
    /// inspect current contents here instead of caching them with identity hashes.
    pub fn output(self: *Work, value: v.Value) !void {
        try self.outputDepth(value, 0);
    }
    pub fn properties(self: *Work, entries: []const v.Entry) !void {
        try self.outputDepth(.{ .map = entries }, 1);
    }
    fn outputDepth(self: *Work, value: v.Value, depth: usize) anyerror!void {
        if (depth > 64) return error.CypherValueDepthExceeded;
        const metadata = try self.info(value);
        if (!metadata.entities) {
            if (depth + metadata.depth > 64) return error.CypherValueDepthExceeded;
            return self.charge(self.ptr, metadata.work);
        }
        try self.charge(self.ptr, 1);
        switch (value) {
            .node => |node| try self.outputDepth(.{ .map = node.properties }, depth + 1),
            .relationship => |edge| try self.outputDepth(.{ .map = edge.properties }, depth + 1),
            .path => |path| {
                for (path.nodes) |node| try self.outputDepth(.{ .node = node }, depth + 1);
                for (path.relationships) |edge| try self.outputDepth(.{ .relationship = edge }, depth + 1);
            },
            .list => |items| for (items) |item| try self.outputDepth(item, depth + 1),
            .map => |items| for (items) |item| try self.outputDepth(item.value, depth + 1),
            else => unreachable,
        }
    }
    pub fn entityTraversal(self: *Work, value: v.Value) !void {
        try self.charge(self.ptr, (try self.info(value)).entity_work);
    }
    fn identical(a: v.Value, b: v.Value) bool {
        const left = key(a) orelse return false;
        const right = key(b) orelse return false;
        return std.meta.eql(left, right);
    }
    pub fn equal(self: *Work, a: v.Value, b: v.Value) !?bool {
        try self.charge(self.ptr, 1);
        // Missing properties and incompatible shapes cannot match. Do not
        // traverse a large catalog predicate for each obvious nonmember.
        if (a == .null or b == .null) return null;
        if (v.Value.rank(a) != v.Value.rank(b)) return false;
        if (a == .map and a.map.len != b.map.len) return false;
        if (a == .list and a.list.len != b.list.len) return false;
        const left = try self.info(a);
        const right = try self.info(b);
        if (identical(a, b)) return if (left.unknown) null else true;
        try self.charge(self.ptr, left.work +| right.work);
        return self.equalInner(a, b, false);
    }
    fn equalInner(self: *Work, a: v.Value, b: v.Value, nulls_equal: bool) anyerror!?bool {
        try self.charge(self.ptr, 1);
        if (self.binding_identity) {
            if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
            if (a == .number) return @as(u64, @bitCast(a.number)) == @as(u64, @bitCast(b.number));
        }
        if (a == .null or b == .null) return if (nulls_equal) a == .null and b == .null else null;
        if (a == .map and b == .map) {
            if (a.map.len != b.map.len) return false;
            var unknown = false;
            for (a.map) |entry| {
                const other = (try self.lookup(b.map, entry.name)) orelse return false;
                if (try self.equalInner(entry.value, other, nulls_equal)) |same| {
                    if (!same) return false;
                } else unknown = true;
            }
            return if (unknown) null else true;
        }
        if (a == .list and b == .list) {
            if (a.list.len != b.list.len) return false;
            var unknown = false;
            for (a.list, b.list) |left, right| {
                if (try self.equalInner(left, right, nulls_equal)) |same| {
                    if (!same) return false;
                } else unknown = true;
            }
            return if (unknown) null else true;
        }
        // Scalar/path equality has bounded traversal, with checkpoints above
        // each container member. Structural metadata reserves byte/path work.
        try self.charge(self.ptr, (try self.info(a)).work +| (try self.info(b)).work);
        return if (nulls_equal) v.Value.eql(a, b) else v.Value.equal(a, b);
    }
    pub fn eql(self: *Work, a: v.Value, b: v.Value) !bool {
        try self.charge(self.ptr, 1);
        const left = try self.info(a);
        const right = try self.info(b);
        if (left.hash != right.hash) return false;
        if (identical(a, b)) return true;
        try self.charge(self.ptr, left.work +| right.work);
        return (try self.equalInner(a, b, true)).?;
    }
    pub fn order(self: *Work, a: v.Value, b: v.Value) !std.math.Order {
        try self.charge(self.ptr, 1);
        if (identical(a, b)) return .eq;
        try self.charge(self.ptr, (try self.info(a)).comparison_work);
        try self.charge(self.ptr, (try self.info(b)).comparison_work);
        return v.Value.order(a, b);
    }
    pub fn compare(self: *Work, a: v.Value, b: v.Value) !?std.math.Order {
        try self.charge(self.ptr, 1);
        const left = try self.info(a);
        if (identical(a, b)) return if (left.unknown) null else .eq;
        try self.charge(self.ptr, left.comparison_work);
        try self.charge(self.ptr, (try self.info(b)).comparison_work);
        return v.Value.compare(a, b);
    }
};

/// Hash collisions retain chains of candidate row indices. Equality remains
/// fallible so collision handling cannot bypass cancellation or work ceilings.
pub const Index = struct {
    const Link = struct { index: usize, next: ?usize };
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    links: std.ArrayList(Link) = .empty,
    pub fn head(self: Index, hash: u64) ?usize {
        return self.heads.get(hash);
    }
    pub fn insert(self: *Index, a: A, hash: u64, index: usize) !void {
        const next = self.head(hash);
        const position = self.links.items.len;
        try self.links.append(a, .{ .index = index, .next = next });
        try self.heads.put(a, hash, position);
    }
};

test "binding identity preserves nested numeric representations independently of Cypher equality" {
    const Noop = struct {
        fn charge(_: *anyopaque, _: usize) !void {}
    };
    var state: u8 = 0;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const integer: v.Value = .{ .map = &.{.{ .name = "x", .value = .{ .list = &.{.{ .integer = 1 }} } }} };
    const number: v.Value = .{ .map = &.{.{ .name = "x", .value = .{ .list = &.{.{ .number = 1 }} } }} };
    var semantic: Work = .{ .a = arena.allocator(), .ptr = &state, .charge = Noop.charge };
    var bindings: Work = .{ .a = arena.allocator(), .ptr = &state, .charge = Noop.charge, .binding_identity = true };
    try std.testing.expect(try semantic.eql(integer, number));
    try std.testing.expect(!try bindings.eql(integer, number));
    try std.testing.expect((try bindings.info(integer)).hash != (try bindings.info(number)).hash);
    try std.testing.expect(try bindings.eql(integer, try integer.clone(arena.allocator())));
    try std.testing.expect(!try bindings.eql(.{ .number = -0.0 }, .{ .number = 0.0 }));
}

test "cached comparisons and hash collision checks still honor cancellation" {
    const Counter = struct {
        canceled: bool = false,
        fn charge(ptr: *anyopaque, _: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.canceled) return error.Canceled;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var counter: Counter = .{};
    var work: Work = .{ .a = arena.allocator(), .ptr = &counter, .charge = Counter.charge };
    const data: v.Value = .{ .map = &.{.{ .name = "a", .value = .{ .integer = 1 } }} };
    _ = try work.info(data);
    counter.canceled = true;
    try std.testing.expectError(error.Canceled, work.equal(data, data));
    try std.testing.expectError(error.Canceled, work.eql(data, data));
    try std.testing.expectError(error.Canceled, work.order(data, data));
    try std.testing.expectError(error.Canceled, work.compare(data, data));
    try std.testing.expectError(error.Canceled, work.traversal(data));
}

test "large immutable property maps are indexed once and preserve first-key and null semantics" {
    const Counter = struct {
        units: usize = 0,
        canceled: bool = false,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.canceled) return error.Canceled;
            self.units += amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const entries = try a.alloc(v.Entry, 4000);
    for (entries, 0..) |*entry, index| entry.* = .{ .name = try std.fmt.allocPrint(a, "field:{d}", .{index}), .value = .{ .integer = @intCast(index) } };
    entries[entries.len - 1] = .{ .name = "field:0", .value = .null };
    var counter: Counter = .{};
    var work: Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    for (0..1000) |_| {
        try std.testing.expectEqual(@as(i64, 3998), (try work.property(.{ .map = entries }, "field:3998")).integer);
        try std.testing.expectEqual(@as(i64, 0), (try work.property(.{ .map = entries }, "field:0")).integer);
        try std.testing.expectEqual(@as(?v.Value, null), try work.lookup(entries, "missing"));
    }
    try std.testing.expect(counter.units < 10_000);
    try std.testing.expectEqual(@as(usize, 1), work.fields.count());
    counter.canceled = true;
    try std.testing.expectError(error.Canceled, work.property(.{ .map = entries }, "field:0"));
}
