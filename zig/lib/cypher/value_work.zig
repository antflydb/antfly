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
pub const Info = struct { hash: u64, depth: usize = 0, work: usize = 1, steps: usize = 1, entity_work: usize = 1, unknown: bool = false, entities: bool = false };
/// Structural certificates never inspect string bodies or compute equality hashes.
/// They share the same immutable allocation/epoch contract as hash metadata.
pub const Shape = struct {
    depth: usize = 0,
    work: usize = 1,
    steps: usize = 1,
    entity_work: usize = 1,
    unknown: bool = false,
    entities: bool = false,
    fn fromInfo(value: Info) Shape {
        return .{ .depth = value.depth, .work = value.work, .steps = value.steps, .entity_work = value.entity_work, .unknown = value.unknown, .entities = value.entities };
    }
};
pub const Work = struct {
    a: A,
    ptr: *anyopaque,
    charge: *const fn (*anyopaque, usize) anyerror!void,
    // Execution bindings must preserve representation: 1 and 1.0 compare equal
    // in Cypher but behave differently in integer division and nested values.
    // This mode has its own Work/cache and never changes GROUP BY semantics.
    binding_identity: bool = false,
    /// Only immutable statement-owned metadata may be inherited by a shorter
    /// evaluation scope. Transient probe metadata is never inserted there.
    parent: ?*Work = null,
    cache: std.AutoHashMapUnmanaged(Key, Info) = .empty,
    shapes: std.AutoHashMapUnmanaged(Key, Shape) = .empty,
    fields: std.AutoHashMapUnmanaged(Key, std.StringHashMapUnmanaged(usize)) = .empty,
    order_depths: std.AutoHashMapUnmanaged(Key, usize) = .empty,
    order_entries: std.AutoHashMapUnmanaged(Key, []const v.Entry) = .empty,

    fn key(value: v.Value) ?Key {
        return switch (value) {
            .list => |items| .{ .address = @intFromPtr(items.ptr), .length = items.len, .kind = .list },
            .map => |items| .{ .address = @intFromPtr(items.ptr), .length = items.len, .kind = .map },
            .string => |item| .{ .address = @intFromPtr(item.ptr), .length = item.len, .kind = .string },
            else => null,
        };
    }
    /// Observe an existing certificate without traversing an unevaluated value.
    /// Parent epochs only contain immutable values whose lifetime encloses us.
    pub fn cachedInfo(self: *const Work, value: v.Value) ?Info {
        const identity_key = key(value) orelse return null;
        var owner: ?*const Work = self;
        while (owner) |current| : (owner = current.parent) if (current.cache.get(identity_key)) |known| return known;
        return null;
    }
    pub fn hasMetadata(self: *const Work, value: v.Value) bool {
        return if (key(value)) |k| (self.cache.contains(k) or self.shapes.contains(k)) else false;
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
    pub fn cachedShape(self: *const Work, value: v.Value) ?Shape {
        const k = key(value) orelse return null;
        var owner: ?*const Work = self;
        while (owner) |current| : (owner = current.parent) {
            if (current.shapes.get(k)) |known| return known;
            if (current.cache.get(k)) |known| return Shape.fromInfo(known);
        }
        return null;
    }
    pub fn shape(self: *Work, value: v.Value) anyerror!Shape {
        return self.shapeDepth(value, 0);
    }
    fn shapeDepth(self: *Work, value: v.Value, depth: usize) anyerror!Shape {
        try self.charge(self.ptr, 1);
        if (depth > 64) return error.CypherValueDepthExceeded;
        if (self.cachedShape(value)) |known| {
            if (depth + known.depth > 64) return error.CypherValueDepthExceeded;
            return known;
        }
        var result: Shape = .{};
        switch (value) {
            .null => result.unknown = true,
            .string => |bytes| result.work +|= bytes.len / 64,
            .node, .relationship => result.entities = true,
            .path => |path| {
                result.entities = true;
                result.work +|= path.nodes.len +| path.relationships.len;
                result.entity_work = result.work;
                result.steps = result.work;
            },
            .list => |items| for (items) |item| {
                const child = try self.shapeDepth(item, depth + 1);
                mergeShape(&result, child);
            },
            .map => |entries| for (entries) |entry| {
                const child = try self.shapeDepth(entry.value, depth + 1);
                mergeShape(&result, child);
                result.work +|= entry.name.len / 64;
            },
            else => {},
        }
        if (key(value)) |k| try self.shapes.put(self.a, k, result);
        return result;
    }
    fn mergeShape(result: *Shape, child: Shape) void {
        result.depth = @max(result.depth, child.depth + 1);
        result.work +|= child.work;
        result.steps +|= child.steps;
        result.entity_work +|= if (child.entities) child.entity_work else 1;
        result.unknown = result.unknown or child.unknown;
        result.entities = result.entities or child.entities;
    }
    pub fn rememberShape(self: *Work, value: v.Value, metadata: Shape) !void {
        if (key(value)) |k| try self.shapes.put(self.a, k, metadata);
    }
    pub fn info(self: *Work, value: v.Value) anyerror!Info {
        return self.infoDepth(value, 0);
    }
    fn infoDepth(self: *Work, value: v.Value, depth: usize) anyerror!Info {
        if (depth > 64) return error.CypherValueDepthExceeded;
        try self.charge(self.ptr, 1);
        const cache_key = key(value);
        if (cache_key) |k| {
            var owner: ?*Work = self;
            while (owner) |current| : (owner = current.parent) if (current.cache.get(k)) |cached| {
                if (depth + cached.depth > 64) return error.CypherValueDepthExceeded;
                return cached;
            };
            owner = self.parent;
            while (owner) |current| : (owner = current.parent) {
                if (current.shapes.contains(k)) return current.infoDepth(value, depth);
            }
        }
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

                result.entity_work = result.work;
                result.steps = result.work;
            },
            .list => |items| {
                result.hash = mix(result.hash, items.len);
                for (items) |item| {
                    const child = try self.infoDepth(item, depth + 1);
                    result.hash = mix(result.hash, child.hash);
                    result.depth = @max(result.depth, child.depth + 1);
                    result.work +|= child.work;
                    result.steps +|= child.steps;
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
                    result.steps +|= child.steps;
                    result.entity_work +|= if (child.entities) child.entity_work else 1;
                    result.unknown = result.unknown or child.unknown;
                    result.entities = result.entities or child.entities;
                }
                result.hash = mix(mix(result.hash, items.len), entries);
            },
        }
        if (cache_key) |k| try self.cache.put(self.a, k, result);
        return result;
    }
    /// Transfer root metadata with an ownership clone. The caller proves that
    /// both roots have identical contents and owns this cache for that clone's
    /// lifetime; source allocation addresses never enter the new cache.
    pub fn remember(self: *Work, value: v.Value, metadata: Info) !void {
        if (key(value)) |cache_key| try self.cache.put(self.a, cache_key, metadata);
    }
    pub fn traversal(self: *Work, value: v.Value) !void {
        try self.charge(self.ptr, (try self.shape(value)).work);
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
        const metadata = try self.shape(value);
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
        try self.charge(self.ptr, (try self.shape(value)).entity_work);
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
        if (a == .string and b == .string) return try self.equalText(a.string, b.string);
        const left = try self.shape(a);
        const right = try self.shape(b);
        if (identical(a, b)) return if (left.unknown) null else true;
        try self.charge(self.ptr, left.steps +| right.steps);
        return self.equalInner(a, b, false);
    }
    fn equalInner(self: *Work, a: v.Value, b: v.Value, nulls_equal: bool) anyerror!?bool {
        try self.charge(self.ptr, 1);
        if (self.binding_identity) {
            if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
            if (a == .number) return @as(u64, @bitCast(a.number)) == @as(u64, @bitCast(b.number));
        }
        if (a == .null or b == .null) return if (nulls_equal) a == .null and b == .null else null;
        if (a == .string and b == .string) return try self.equalText(a.string, b.string);
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
        try self.charge(self.ptr, (try self.shape(a)).work +| (try self.shape(b)).work);
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
    /// Ordering does not need equality hashes. Validate container depth without
    /// reading string bodies, then meter the members/byte prefixes actually
    /// compared. All metadata has the same owner/lifetime as this Work.
    pub fn order(self: *Work, a: v.Value, b: v.Value) !std.math.Order {
        _ = try self.orderDepth(a, 0);
        _ = try self.orderDepth(b, 0);
        return self.orderInner(a, b);
    }
    fn orderDepth(self: *Work, value: v.Value, depth: usize) anyerror!usize {
        try self.charge(self.ptr, 1);
        if (depth > 64) return error.CypherValueDepthExceeded;
        if (value != .list and value != .map) return 0;
        const k = key(value).?;
        if (self.order_depths.get(k)) |known| {
            if (depth + known > 64) return error.CypherValueDepthExceeded;
            return known;
        }
        var result: usize = 0;
        switch (value) {
            .list => |items| for (items) |item| {
                result = @max(result, 1 + try self.orderDepth(item, depth + 1));
            },
            .map => |entries| for (entries) |entry| {
                result = @max(result, 1 + try self.orderDepth(entry.value, depth + 1));
            },
            else => unreachable,
        }
        try self.order_depths.put(self.a, k, result);
        return result;
    }
    fn orderText(self: *Work, a: []const u8, b: []const u8) !std.math.Order {
        var offset: usize = 0;
        const common = @min(a.len, b.len);
        while (offset < common) {
            try self.charge(self.ptr, 1);
            const end = offset + @min(64, common - offset);
            const comparison = std.mem.order(u8, a[offset..end], b[offset..end]);
            if (comparison != .eq) return comparison;
            offset = end;
        }
        return std.math.order(a.len, b.len);
    }
    fn equalText(self: *Work, a: []const u8, b: []const u8) !bool {
        if (a.len != b.len) return false;
        if (a.ptr == b.ptr) return true;
        return try self.orderText(a, b) == .eq;
    }
    fn orderIdentity(self: *Work, a: v.Identity, b: v.Identity) !std.math.Order {
        const table = try self.orderText(a.table, b.table);
        return if (table != .eq) table else self.orderText(a.key, b.key);
    }
    fn sortedEntries(self: *Work, entries: []const v.Entry) ![]const v.Entry {
        const k = key(.{ .map = entries }).?;
        if (self.order_entries.get(k)) |known| return known;
        const sorted = try self.a.dupe(v.Entry, entries);
        const scratch = try self.a.alloc(v.Entry, entries.len);
        var width: usize = 1;
        while (width < sorted.len) : (width *= 2) {
            var start: usize = 0;
            while (start < sorted.len) : (start += width * 2) {
                const middle = @min(start + width, sorted.len);
                const end = @min(start + width * 2, sorted.len);
                var l = start;
                var r = middle;
                for (scratch[start..end]) |*out| {
                    try self.charge(self.ptr, 1);
                    if (r == end or (l < middle and try self.orderText(sorted[l].name, sorted[r].name) != .gt)) {
                        out.* = sorted[l];
                        l += 1;
                    } else {
                        out.* = sorted[r];
                        r += 1;
                    }
                }
            }
            @memcpy(sorted, scratch);
        }
        try self.order_entries.put(self.a, k, sorted);
        return sorted;
    }
    fn orderInner(self: *Work, a: v.Value, b: v.Value) anyerror!std.math.Order {
        try self.charge(self.ptr, 1);
        if (identical(a, b)) return .eq;
        const ar = v.Value.rank(a);
        const br = v.Value.rank(b);
        if (ar != br) return std.math.order(ar, br);
        return switch (a) {
            .null, .integer, .number, .boolean => v.Value.order(a, b),
            .string => self.orderText(a.string, b.string),
            .node => self.orderIdentity(a.node.identity, b.node.identity),
            .relationship => blk: {
                const identity_order = try self.orderIdentity(a.relationship.identity, b.relationship.identity);
                break :blk if (identity_order != .eq) identity_order else try self.orderText(a.relationship.index, b.relationship.index);
            },
            .list => blk: {
                for (a.list[0..@min(a.list.len, b.list.len)], b.list[0..@min(a.list.len, b.list.len)]) |left, right| {
                    const comparison = try self.orderInner(left, right);
                    if (comparison != .eq) break :blk comparison;
                }
                break :blk std.math.order(a.list.len, b.list.len);
            },
            .path => blk: {
                for (0..@min(a.path.nodes.len, b.path.nodes.len)) |i| {
                    try self.charge(self.ptr, 1);
                    const node_order = try self.orderIdentity(a.path.nodes[i].identity, b.path.nodes[i].identity);
                    if (node_order != .eq) break :blk node_order;
                    if (i < @min(a.path.relationships.len, b.path.relationships.len)) {
                        const edge_order = try self.orderInner(.{ .relationship = a.path.relationships[i] }, .{ .relationship = b.path.relationships[i] });
                        if (edge_order != .eq) break :blk edge_order;
                    }
                }
                break :blk std.math.order(a.path.nodes.len, b.path.nodes.len);
            },
            .map => blk: {
                if (a.map.len != b.map.len) break :blk std.math.order(a.map.len, b.map.len);
                const left = try self.sortedEntries(a.map);
                const right = try self.sortedEntries(b.map);
                // Cypher compares the complete sorted key sets before values.
                for (left, right) |x, y| {
                    const comparison = try self.orderText(x.name, y.name);
                    if (comparison != .eq) break :blk comparison;
                }
                for (left, right) |x, y| {
                    const comparison = try self.orderInner(x.value, y.value);
                    if (comparison != .eq) break :blk comparison;
                }
                break :blk .eq;
            },
        };
    }
    pub fn compare(self: *Work, a: v.Value, b: v.Value) !?std.math.Order {
        try self.charge(self.ptr, 1);
        if (a == .null or b == .null) return null;
        if (a == .string and b == .string) {
            // Shared immutable spans prove their common prefix without a scan.
            if (a.string.ptr == b.string.ptr) return std.math.order(a.string.len, b.string.len);
            return try self.orderText(a.string, b.string);
        }
        const left = try self.shape(a);
        if (identical(a, b)) return if (left.unknown) null else .eq;
        const right = try self.shape(b);
        try self.charge(self.ptr, left.steps +| right.steps);
        return self.compareInner(a, b);
    }
    fn compareInner(self: *Work, a: v.Value, b: v.Value) anyerror!?std.math.Order {
        try self.charge(self.ptr, 1);
        if (a == .null or b == .null or v.Value.rank(a) != v.Value.rank(b)) return null;
        if (a == .list) {
            for (a.list[0..@min(a.list.len, b.list.len)], b.list[0..@min(a.list.len, b.list.len)]) |left, right| {
                const comparison = (try self.compareInner(left, right)) orelse return null;
                if (comparison != .eq) return comparison;
            }
            return std.math.order(a.list.len, b.list.len);
        }
        if (a == .map) {
            if (a.map.len != b.map.len) return std.math.order(a.map.len, b.map.len);
            const left = try self.sortedEntries(a.map);
            const right = try self.sortedEntries(b.map);
            for (left, right) |x, y| {
                const comparison = try self.orderText(x.name, y.name);
                if (comparison != .eq) return comparison;
            }
            for (left, right) |x, y| {
                const comparison = (try self.compareInner(x.value, y.value)) orelse return null;
                if (comparison != .eq) return comparison;
            }
            return .eq;
        }
        if (a == .string and a.string.ptr == b.string.ptr) return std.math.order(a.string.len, b.string.len);
        return try self.orderInner(a, b);
    }
};

/// Hash collisions retain chains of candidate row indices. Equality remains
/// fallible so collision handling cannot bypass cancellation or work ceilings.
pub const Index = struct {
    const Link = struct { index: usize, next: ?usize };
    heads: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    links: std.ArrayList(Link) = .empty,
    free: ?usize = null,
    pub fn deinit(self: *Index, a: A) void {
        self.heads.deinit(a);
        self.links.deinit(a);
    }
    pub fn remove(self: *Index, hash: u64, index: usize) void {
        var previous: ?usize = null;
        var candidate = self.head(hash);
        while (candidate) |position| {
            const link = self.links.items[position];
            if (link.index == index) {
                if (previous) |prior| self.links.items[prior].next = link.next else if (link.next) |next| {
                    self.heads.getPtr(hash).?.* = next;
                } else {
                    _ = self.heads.remove(hash);
                }
                self.links.items[position].next = self.free;
                self.free = position;
                return;
            }
            previous = position;
            candidate = link.next;
        }
        unreachable;
    }
    pub fn head(self: Index, hash: u64) ?usize {
        return self.heads.get(hash);
    }
    pub fn insert(self: *Index, a: A, hash: u64, index: usize) !void {
        const next = self.head(hash);
        if (self.free) |position| {
            try self.heads.put(a, hash, position);
            self.free = self.links.items[position].next;
            self.links.items[position] = .{ .index = index, .next = next };
        } else {
            const position = self.links.items.len;
            try self.links.append(a, .{ .index = index, .next = next });
            try self.heads.put(a, hash, position);
        }
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

test "mutable equality index reuses collision links after eviction" {
    var index: Index = .{};
    defer index.deinit(std.testing.allocator);
    try index.insert(std.testing.allocator, 7, 0);
    try index.insert(std.testing.allocator, 7, 1);
    try index.insert(std.testing.allocator, 7, 2);
    index.remove(7, 1);
    index.remove(7, 2);
    try std.testing.expectEqual(@as(usize, 0), index.links.items[index.head(7).?].index);
    for (0..1000) |i| {
        try index.insert(std.testing.allocator, @intCast(i + 8), 1);
        index.remove(@intCast(i + 8), 1);
    }
    try std.testing.expectEqual(@as(usize, 3), index.links.items.len);
    index.remove(7, 0);
    try std.testing.expect(index.head(7) == null);
}

test "metered ordering reads decisive prefixes and bounds equal tails and shared containers" {
    const Counter = struct {
        left: usize,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (amount > self.left) return error.CypherStateBudgetExceeded;
            self.left -= amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try a.alloc(u8, 65536);
    const right = try a.alloc(u8, left.len);
    @memset(left, 'x');
    @memset(right, 'x');
    left[0] = 'a';
    right[0] = 'b';
    var counter: Counter = .{ .left = 10 };
    var work: Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    try std.testing.expectEqual(std.math.Order.lt, try work.order(.{ .string = left }, .{ .string = right }));
    right[0] = 'a';
    counter.left = 10;
    try std.testing.expectError(error.CypherStateBudgetExceeded, work.order(.{ .string = left }, .{ .string = right }));
    // Separate allocation DAGs with equal leaves require real comparisons and
    // must stop at the work ceiling, without an unmetered recursive traversal.
    var x: v.Value = .{ .integer = 1 };
    var y = x;
    for (0..20) |_| {
        const xs = try a.alloc(v.Value, 2);
        const ys = try a.alloc(v.Value, 2);
        xs[0..2].* = .{ x, x };
        ys[0..2].* = .{ y, y };
        x = .{ .list = xs };
        y = .{ .list = ys };
    }
    counter.left = 1000;
    try std.testing.expectError(error.CypherStateBudgetExceeded, work.order(x, y));
}

test "metered ordering matches total Cypher ordering across scalar and container types" {
    const Counter = struct {
        fn charge(_: *anyopaque, _: usize) !void {}
    };
    var state: u8 = 0;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var work: Work = .{ .a = arena.allocator(), .ptr = &state, .charge = Counter.charge };
    const values = [_]v.Value{
        .null,                                                   .{ .boolean = false },                                                                                        .{ .boolean = true },                                                                             .{ .integer = 9007199254740993 },                                                                             .{ .number = 9007199254740992.0 },
        .{ .integer = -1 },                                      .{ .number = -0.0 },                                                                                          .{ .string = "a" },                                                                               .{ .string = "b" },                                                                                           .{ .list = &.{ .{ .integer = 1 }, .null } },
        .{ .list = &.{ .{ .number = 1 }, .{ .string = "x" } } }, .{ .map = &.{ .{ .name = "b", .value = .{ .integer = 1 } }, .{ .name = "a", .value = .{ .integer = 2 } } } }, .{ .map = &.{ .{ .name = "a", .value = .{ .integer = 2 } }, .{ .name = "b", .value = .null } } }, .{ .map = &.{ .{ .name = "z", .value = .{ .integer = 1 } }, .{ .name = "a", .value = .{ .integer = 2 } } } },
    };
    for (values) |x| for (values) |y| try std.testing.expectEqual(try v.Value.order(x, y), try work.order(x, y));
}

test "metered scalar equality only reads equal-length string prefixes" {
    const Counter = struct {
        left: usize,
        fn charge(ptr: *anyopaque, count: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (count > self.left) return error.CypherStateBudgetExceeded;
            self.left -= count;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try a.alloc(u8, 2 << 20);
    const right = try a.alloc(u8, left.len);
    @memset(left, 'x');
    @memset(right, 'x');
    var counter: Counter = .{ .left = 100 };
    var work: Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    for (0..32) |_| try std.testing.expectEqual(@as(?bool, false), try work.equal(.{ .string = left }, .{ .string = "short" }));
    right[0] = 'y';
    try std.testing.expectEqual(@as(?bool, false), try work.equal(.{ .string = left }, .{ .string = right }));
    right[0] = 'x';
    try std.testing.expectError(error.CypherStateBudgetExceeded, work.equal(.{ .string = left }, .{ .string = right }));
    counter.left = 100000;
    try std.testing.expectEqual(@as(?bool, true), try work.equal(.{ .string = left }, .{ .string = right }));
    try std.testing.expectEqual(@as(usize, 0), work.cache.count());
}

test "nullable string comparison meters decisive prefixes and equal tails" {
    const Counter = struct {
        left: usize,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (amount > self.left) return error.CypherStateBudgetExceeded;
            self.left -= amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const text = try a.alloc(u8, 2 << 20);
    @memset(text, 'x');
    var counter: Counter = .{ .left = 256 };
    var work: Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    for (0..32) |_| try std.testing.expectEqual(std.math.Order.gt, (try work.compare(.{ .string = text }, .{ .string = "7" })).?);
    counter.left = 2;
    try std.testing.expectEqual(std.math.Order.eq, (try work.compare(.{ .string = text }, .{ .string = text })).?);
    try std.testing.expectEqual(std.math.Order.gt, (try work.compare(.{ .string = text }, .{ .string = text[0..1] })).?);
    counter.left = 256;
    const copy = try a.dupe(u8, text);
    try std.testing.expectError(error.CypherStateBudgetExceeded, work.compare(.{ .string = text }, .{ .string = copy }));
    counter.left = 256;
    try std.testing.expectEqual(@as(?std.math.Order, null), try work.compare(.null, .{ .string = text }));
}

test "container predicates validate shape and meter only compared byte prefixes" {
    const Counter = struct {
        left: usize,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (amount > self.left) return error.CypherStateBudgetExceeded;
            self.left -= amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bytes = try a.alloc(u8, 2 << 20);
    @memset(bytes, 'x');
    const left: v.Value = .{ .list = &.{.{ .string = bytes }} };
    const right: v.Value = .{ .list = &.{.{ .string = "7" }} };
    var counter: Counter = .{ .left = 2000 };
    var work: Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    for (0..32) |_| {
        try std.testing.expectEqual(std.math.Order.gt, (try work.compare(left, right)).?);
        try std.testing.expectEqual(@as(?bool, false), try work.equal(left, right));
    }
    try std.testing.expectEqual(@as(usize, 0), work.cache.count());
    const nullable: v.Value = .{ .list = &.{ .null, .{ .integer = 1 } } };
    const different: v.Value = .{ .list = &.{ .null, .{ .integer = 2 } } };
    try std.testing.expectEqual(@as(?std.math.Order, null), try work.compare(nullable, different));
    try std.testing.expectEqual(@as(?bool, false), try work.equal(nullable, different));
    const copy: v.Value = .{ .list = &.{.{ .string = try a.dupe(u8, bytes) }} };
    counter.left = 16;
    try std.testing.expectError(error.CypherStateBudgetExceeded, work.compare(left, copy));
}

test "wide nullable map ordering sorts key views and immutable hashes stay lazy" {
    const Counter = struct {
        left: usize,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (amount > self.left) return error.CypherStateBudgetExceeded;
            self.left -= amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const left = try a.alloc(v.Entry, 2048);
    const right = try a.alloc(v.Entry, left.len);
    for (left, 0..) |*entry, i| entry.* = .{ .name = try std.fmt.allocPrint(a, "{d:0>4}", .{i}), .value = .{ .integer = @intCast(i) } };
    for (right, 0..) |*entry, i| entry.* = left[left.len - i - 1];
    right[right.len - 1].value = .{ .integer = 1 };
    var counter: Counter = .{ .left = 100000 };
    var work: Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    try std.testing.expectEqual(std.math.Order.lt, (try work.compare(.{ .map = left }, .{ .map = right })).?);
    try std.testing.expectEqual(@as(usize, 0), work.cache.count());
    const bytes = try a.alloc(u8, 2 << 20);
    @memset(bytes, 'x');
    const value: v.Value = .{ .list = &.{.{ .string = bytes }} };
    counter.left = 40000;
    var owner: Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    _ = try owner.shape(value);
    try std.testing.expectEqual(@as(usize, 0), owner.cache.count());
    for (0..32) |_| {
        var epoch: Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge, .parent = &owner };
        _ = try epoch.info(value);
        try std.testing.expectEqual(@as(usize, 0), epoch.cache.count());
    }
    try std.testing.expect(owner.cache.count() == 2);
}
