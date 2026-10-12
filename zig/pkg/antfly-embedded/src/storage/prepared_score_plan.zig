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
const platform = @import("antfly_platform");
const keys = @import("internal_keys.zig");
const ScoreSink = @import("backend_types.zig").ScoreSink;

// A rebuild gets a process-unique identity, including after deinit and address
// reuse. Row/column permutations keep the canonical physical key identity.
var next_identity = platform.atomic.Value(u64).init(1);
pub fn newCanonicalIdentity() u64 {
    const identity = next_identity.fetchAdd(1, .monotonic);
    if (identity == 0 or identity == std.math.maxInt(u64)) @panic("prepared read identity exhausted");
    return identity;
}

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
    storage: ?[]align(@alignOf(usize)) u8 = null,
    nodes: []const []const u8 = &.{},
    prefixes: []const ?[]const u8 = &.{},
    read_keys: []const []const u8 = &.{},
    targets: []const Target = &.{},
    ends: []const usize = &.{},
    valid: bool = false,
    identity: u64 = 0,
    // Targets keep canonical row identities; requests only update this mapping.
    row_remap: []u16 = &.{},
    row_identity: []u16 = &.{},
    row_heads: []u16 = &.{},
    row_cursors: []u16 = &.{},
    duplicate_next: []u16 = &.{},
    row_lookup_valid: bool = false,
    row_order_identity: bool = true,
    column_remap: []u16 = &.{},
    column_identity: []u16 = &.{},
    column_consumed: []u16 = &.{},
    column_sorted: []const usize = &.{},
    column_count: usize = 0,
    column_order_identity: bool = true,
    dense_columns: bool = false,
    // Immutable multi-chunk scans do not churn the single-entry decoded cache.
    // This is a read admission hint, not part of canonical key identity.
    retain_decoded: bool = true,

    pub fn deinit(self: *@This()) void {
        if (self.storage) |storage| self.backing.?.free(storage);
        self.* = .{};
    }

    /// Deep, independently owned snapshot of a valid plan. Canonical bytes and
    /// their identity are unchanged; all descriptors and mappings are relocated.
    /// Storage alignment is explicit so offsets remain valid on any allocator.
    pub fn clone(self: *const @This(), a: std.mem.Allocator) !@This() {
        if (!self.valid or self.identity == 0) return error.InvalidBatch;
        const old = self.storage orelse return error.InvalidBatch;
        const storage = try a.alignedAlloc(u8, .of(usize), old.len);
        @memcpy(storage, old);
        var copy = self.*;
        copy.backing = a;
        copy.storage = storage;
        const nodes = relocate([]const u8, self.nodes, old, storage);
        for (nodes) |*node| node.* = relocate(u8, node.*, old, storage);
        copy.nodes = nodes;
        const prefixes = relocate(?[]const u8, self.prefixes, old, storage);
        for (prefixes) |*prefix| if (prefix.*) |value| {
            prefix.* = relocate(u8, value, old, storage);
        };
        copy.prefixes = prefixes;
        const keys_copy = relocate([]const u8, self.read_keys, old, storage);
        for (keys_copy) |*key| key.* = relocate(u8, key.*, old, storage);
        copy.read_keys = keys_copy;
        copy.targets = relocate(Target, self.targets, old, storage);
        copy.ends = relocate(usize, self.ends, old, storage);
        inline for (.{ "row_remap", "row_identity", "row_heads", "row_cursors", "duplicate_next", "column_remap", "column_identity", "column_consumed" }) |field| {
            @field(copy, field) = relocate(u16, @field(self, field), old, storage);
        }
        copy.column_sorted = relocate(usize, self.column_sorted, old, storage);
        return copy;
    }

    fn relocate(comptime T: type, values: []const T, old: []const u8, storage: []u8) []T {
        if (values.len == 0) return &.{};
        const offset = @intFromPtr(values.ptr) - @intFromPtr(old.ptr);
        std.debug.assert(offset <= old.len and values.len * @sizeOf(T) <= old.len - offset);
        const ptr: [*]T = @ptrCast(@alignCast(storage.ptr + offset));
        return ptr[0..values.len];
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
            self.storage = try backing.alignedAlloc(u8, .of(usize), capacity);
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
        self.column_remap = try a.alloc(u16, prefixes.len);
        self.column_identity = try a.alloc(u16, prefixes.len);
        self.column_consumed = try a.alloc(u16, prefixes.len);
        var null_column = column_count;
        for (owned_prefixes, 0..) |prefix, column| {
            self.column_remap[column] = @intCast(column);
            self.column_identity[column] = @intCast(column);
            if (prefix == null) {
                columns[null_column] = column;
                null_column += 1;
            }
        }
        self.column_sorted = columns;
        self.column_count = column_count;
        self.column_order_identity = true;
        self.nodes = owned_nodes;
        self.prefixes = owned_prefixes;
        self.targets = targets;
        self.read_keys = read_keys[0..unique];
        self.ends = ends[0..unique];
        // Prove once that canonical scores consist of whole contiguous columns.
        // Duplicate keys, interleaved prefixes and unsorted rows use fanout.
        self.dense_columns = nodes.len != 0 and unique == targets.len;
        if (self.dense_columns) for (targets, 0..) |target, i| {
            if (target.row != i % nodes.len or target.column != columns[i / nodes.len]) {
                self.dense_columns = false;
                break;
            }
        };
        self.identity = newCanonicalIdentity();
        self.valid = true;
        return true;
    }

    // Include copied identities, target fanout, key spans, and alignment.
    // Caller inputs must not borrow this plan's storage across preparation.
    fn requiredBytes(prefixes: []const ?[]const u8, nodes: []const []const u8) ?usize {
        if (nodes.len >= std.math.maxInt(u16) or prefixes.len >= std.math.maxInt(u16)) return null;
        const slots = std.math.ceilPowerOfTwo(usize, @max(@as(usize, 1), nodes.len * 2)) catch return null;
        var estimate: usize = 1024 + nodes.len * 3 * @sizeOf(u16) + slots * 2 * @sizeOf(u16);
        var count: usize = 0;
        for (nodes) |node| {
            estimate = std.math.add(usize, estimate, node.len +| @sizeOf([]const u8) +| @sizeOf(usize)) catch return null;
            if (estimate > max_bytes) return null;
        }
        for (prefixes) |prefix| {
            estimate = std.math.add(usize, estimate, @sizeOf(?[]const u8) + @sizeOf(usize) + 3 * @sizeOf(u16)) catch return null;
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
        for (prefixes, 0..) |prefix, column| {
            const old = self.prefixes[if (self.column_order_identity) column else self.column_identity[column]];
            if (prefix) |p| {
                if (old == null or !std.mem.eql(u8, p, old.?)) return false;
            } else if (old != null) return false;
        }
        return true;
    }

    /// Exact logical input comparison, respecting current row/column remaps.
    pub fn matches(self: *const @This(), prefixes: []const ?[]const u8, nodes: []const []const u8) bool {
        return self.prefixesMatch(prefixes) and self.nodesMatch(nodes);
    }

    fn nodesMatch(self: *const @This(), nodes: []const []const u8) bool {
        if (!self.valid or nodes.len != self.nodes.len) return false;
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
        if (!self.valid or prefixes.len != self.prefixes.len or nodes.len != self.nodes.len) return false;
        if (!self.prefixesMatch(prefixes) and !self.reuseColumns(prefixes)) return false;
        if (self.nodesMatch(nodes)) return true;
        if (!self.row_lookup_valid) return false;
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

    // Canonical non-null columns are sorted once. Binary lookup plus a
    // duplicate cursor verifies the full multiset with bounded O(n log n) work.
    fn reuseColumns(self: *@This(), prefixes: []const ?[]const u8) bool {
        @memset(self.column_consumed, 0);
        var identity_mapping = true;
        for (prefixes, 0..) |prefix, column| {
            var low: usize = 0;
            if (prefix) |wanted| {
                var high = self.column_count;
                while (low < high) {
                    const mid = low + (high - low) / 2;
                    if (std.mem.order(u8, self.prefixes[self.column_sorted[mid]].?, wanted) == .lt) low = mid + 1 else high = mid;
                }
                if (low == self.column_count) return false;
            } else low = self.column_count;
            if (low == self.column_sorted.len) return false;
            const first = self.column_sorted[low];
            const position = low + self.column_consumed[first];
            if (position >= self.column_sorted.len) return false;
            const identity = self.column_sorted[position];
            const old = self.prefixes[identity];
            if (prefix) |wanted| {
                if (old == null or !std.mem.eql(u8, old.?, wanted)) return false;
            } else if (old != null) return false;
            self.column_consumed[first] += 1;
            self.column_remap[identity] = @intCast(column);
            self.column_identity[column] = @intCast(identity);
            identity_mapping = identity_mapping and identity == column;
        }
        self.column_order_identity = identity_mapping;
        return true;
    }

    pub const Output = struct {
        plan: *const PreparedScorePlan,
        columns: []const []?f64,
        pub fn sink(self: *@This()) ScoreSink {
            return .{ .context = self, .write = if (self.plan.column_order_identity)
                (if (self.plan.row_order_identity) writeCanonical else writeRemapped)
            else
                (if (self.plan.row_order_identity) writeColumns else writeBoth) };
        }
        pub fn completeSink(self: *@This()) @import("backend_types.zig").CompleteScoreSink {
            return .{ .context = self, .write = if (self.plan.column_order_identity)
                (if (self.plan.row_order_identity) completeCanonical else completeRemapped)
            else
                (if (self.plan.row_order_identity) completeColumns else completeBoth), .write_decoded = if (self.plan.dense_columns and self.plan.row_order_identity and self.plan.nodes.len >= 8)
                decodedDense
            else if (self.plan.column_order_identity)
                (if (self.plan.row_order_identity) decodedCanonical else decodedRemapped)
            else
                (if (self.plan.row_order_identity) decodedColumns else decodedBoth) };
        }
        fn decodedDense(ctx: *anyopaque, scores: []const f64) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const rows = self.plan.nodes.len;
            std.debug.assert(scores.len == self.plan.read_keys.len);
            for (self.plan.column_sorted[0..self.plan.column_count], 0..) |column, ordinal| {
                const output = self.columns[if (self.plan.column_order_identity) column else self.plan.column_remap[column]];
                for (scores[ordinal * rows ..][0..rows], output) |score, *value| {
                    value.* = if (std.math.isNan(score)) null else score;
                }
            }
        }
        fn decodedCanonical(ctx: *anyopaque, scores: []const f64) void {
            writeDecoded(ctx, scores, false, false);
        }
        fn decodedRemapped(ctx: *anyopaque, scores: []const f64) void {
            writeDecoded(ctx, scores, true, false);
        }
        fn decodedColumns(ctx: *anyopaque, scores: []const f64) void {
            writeDecoded(ctx, scores, false, true);
        }
        fn decodedBoth(ctx: *anyopaque, scores: []const f64) void {
            writeDecoded(ctx, scores, true, true);
        }
        fn writeDecoded(ctx: *anyopaque, scores: []const f64, comptime remapped: bool, comptime columns_remapped: bool) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.debug.assert(scores.len == self.plan.read_keys.len);
            // Lift the unique-target decision out of the per-score delivery.
            if (scores.len == self.plan.targets.len) {
                for (scores, self.plan.targets) |score, target| {
                    self.columns[if (columns_remapped) self.plan.column_remap[target.column] else target.column][if (remapped) self.plan.row_remap[target.row] else target.row] = if (std.math.isNan(score)) null else score;
                }
            } else {
                var start: usize = 0;
                for (scores, self.plan.ends) |score, end| {
                    const value: ?f64 = if (std.math.isNan(score)) null else score;
                    for (self.plan.targets[start..end]) |target| {
                        self.columns[if (columns_remapped) self.plan.column_remap[target.column] else target.column][if (remapped) self.plan.row_remap[target.row] else target.row] = value;
                    }
                    start = end;
                }
            }
        }
        fn completeCanonical(ctx: *anyopaque, index: usize, score: ?f64) void {
            writeRows(ctx, index, score, false, false);
        }
        fn completeRemapped(ctx: *anyopaque, index: usize, score: ?f64) void {
            writeRows(ctx, index, score, true, false);
        }
        fn completeColumns(ctx: *anyopaque, index: usize, score: ?f64) void {
            writeRows(ctx, index, score, false, true);
        }
        fn completeBoth(ctx: *anyopaque, index: usize, score: ?f64) void {
            writeRows(ctx, index, score, true, true);
        }
        fn writeCanonical(ctx: *anyopaque, index: usize, score: f64) void {
            writeRows(ctx, index, score, false, false);
        }
        fn writeRemapped(ctx: *anyopaque, index: usize, score: f64) void {
            writeRows(ctx, index, score, true, false);
        }
        fn writeColumns(ctx: *anyopaque, index: usize, score: f64) void {
            writeRows(ctx, index, score, false, true);
        }
        fn writeBoth(ctx: *anyopaque, index: usize, score: f64) void {
            writeRows(ctx, index, score, true, true);
        }
        fn writeRows(ctx: *anyopaque, index: usize, score: ?f64, comptime remapped: bool, comptime columns_remapped: bool) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.plan.read_keys.len == self.plan.targets.len) {
                const target = self.plan.targets[index];
                self.columns[if (columns_remapped) self.plan.column_remap[target.column] else target.column][if (remapped) self.plan.row_remap[target.row] else target.row] = score;
                return;
            }
            const start = if (index == 0) 0 else self.plan.ends[index - 1];
            for (self.plan.targets[start..self.plan.ends[index]]) |target| self.columns[if (columns_remapped) self.plan.column_remap[target.column] else target.column][if (remapped) self.plan.row_remap[target.row] else target.row] = score;
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

test "prepared score plan column permutations preserve duplicate null and row fanout" {
    var plan = PreparedScorePlan{};
    defer plan.deinit();
    const a = std.testing.allocator;
    try std.testing.expect(try plan.prepare(a, &.{ "p/", null, "q/", "p/" }, &.{ "a", "b", "a" }));
    const storage = plan.storage.?.ptr;
    const physical = plan.read_keys.ptr;
    const requests = [_][4]?[]const u8{
        .{ "q/", "p/", "p/", null },
        .{ null, "p/", "q/", "p/" },
        .{ "p/", null, "q/", "p/" },
    };
    for (requests) |prefixes| {
        const nodes = [_][]const u8{ "b", "a", "a" };
        try std.testing.expect(try plan.prepare(a, &prefixes, &nodes));
        try std.testing.expectEqual(storage, plan.storage.?.ptr);
        try std.testing.expectEqual(physical, plan.read_keys.ptr);
        var scores: [4][3]?f64 = @splat(@splat(null));
        var output = PreparedScorePlan.Output{ .plan = &plan, .columns = &.{ &scores[0], &scores[1], &scores[2], &scores[3] } };
        const sink = output.sink();
        for (plan.read_keys, 0..) |key, i| sink.write(sink.context, i, @floatFromInt(key[0] + key[key.len - 3]));
        for (prefixes, scores) |prefix, column| for (nodes, column) |node, value| {
            try std.testing.expectEqual(if (prefix) |p| @as(?f64, @floatFromInt(p[0] + node[0])) else null, value);
        };
    }
    // A changed multiset must rebuild, even after a partial mapping attempt.
    try std.testing.expect(try plan.prepare(a, &.{ "q/", "p/", "q/", null }, &.{ "b", "a", "a" }));
    try std.testing.expectEqualStrings("q/", plan.prefixes[0].?);
}

test "prepared score plan identity survives permutations and changes after rebuild and deinit" {
    var plan = PreparedScorePlan{};
    defer plan.deinit();
    try std.testing.expect(try plan.prepare(std.testing.allocator, &.{"p"}, &.{ "a", "b" }));
    const identity = plan.identity;
    try std.testing.expect(try plan.prepare(std.testing.allocator, &.{"p"}, &.{ "b", "a" }));
    try std.testing.expectEqual(identity, plan.identity);
    try std.testing.expect(try plan.prepare(std.testing.allocator, &.{"q"}, &.{ "a", "b" }));
    try std.testing.expect(plan.identity != identity);
    const rebuilt = plan.identity;
    plan.deinit();
    try std.testing.expect(try plan.prepare(std.testing.allocator, &.{"q"}, &.{ "a", "b" }));
    try std.testing.expect(plan.identity != rebuilt);
}

test "prepared score bulk delivery preserves null duplicate and permutation fanout" {
    const a = std.testing.allocator;
    var plan = PreparedScorePlan{};
    defer plan.deinit();
    const prefixes = [_]?[]const u8{ "p/", null, "q/", "p/" };
    const reordered = [_]?[]const u8{ "q/", "p/", null, "p/" };
    const nodes = [_][]const u8{ "a", "b", "a" };
    const permuted = [_][]const u8{ "b", "a", "a" };
    for (0..4) |mode| {
        try std.testing.expect(try plan.prepare(a, if (mode & 1 == 0) &prefixes else &reordered, if (mode & 2 == 0) &nodes else &permuted));
        var expected: [4][3]?f64 = @splat(@splat(null));
        var actual: [4][3]?f64 = @splat(@splat(null));
        var first = PreparedScorePlan.Output{ .plan = &plan, .columns = &.{ &expected[0], &expected[1], &expected[2], &expected[3] } };
        var second = PreparedScorePlan.Output{ .plan = &plan, .columns = &.{ &actual[0], &actual[1], &actual[2], &actual[3] } };
        var values: [4]f64 = undefined;
        for (values[0..plan.read_keys.len], 0..) |*score, i| {
            score.* = if (i == 1) std.math.nan(f64) else @floatFromInt(i);
            const sink = first.completeSink();
            sink.write(sink.context, i, if (std.math.isNan(score.*)) null else score.*);
        }
        second.completeSink().deliverDecoded(values[0..plan.read_keys.len]);
        for (expected, actual) |want, got| try std.testing.expectEqualSlices(?f64, &want, &got);
    }
}

test "prepared score dense columns preserve missing scores null columns and remaps" {
    var plan = PreparedScorePlan{};
    defer plan.deinit();
    const prefixes = [_]?[]const u8{ "q/", null, "p/" };
    const nodes = [_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" };
    const reordered = [_]?[]const u8{ "p/", "q/", null };
    const reversed = [_][]const u8{ "h", "g", "f", "e", "d", "c", "b", "a" };
    for (0..4) |mode| {
        try std.testing.expect(try plan.prepare(std.testing.allocator, if (mode & 1 == 0) &prefixes else &reordered, if (mode & 2 == 0) &nodes else &reversed));
        try std.testing.expect(plan.dense_columns);
        var expected: [3][8]?f64 = @splat(@splat(null));
        var actual: [3][8]?f64 = @splat(@splat(null));
        var scalar = PreparedScorePlan.Output{ .plan = &plan, .columns = &.{ &expected[0], &expected[1], &expected[2] } };
        var bulk = PreparedScorePlan.Output{ .plan = &plan, .columns = &.{ &actual[0], &actual[1], &actual[2] } };
        const values = [_]f64{ 3, std.math.nan(f64), 5, 8, 1, 2, 3, 4, 9, 8, 7, std.math.nan(f64), 5, 4, 3, 2 };
        const sink = scalar.completeSink();
        for (values, 0..) |value, i| sink.write(sink.context, i, if (std.math.isNan(value)) null else value);
        bulk.completeSink().deliverDecoded(&values);
        for (expected, actual) |first, second| try std.testing.expectEqualSlices(?f64, &first, &second);
    }
    try std.testing.expect(try plan.prepare(std.testing.allocator, &.{ "p/", "p/" }, &nodes));
    try std.testing.expect(!plan.dense_columns);
    try std.testing.expect(try plan.prepare(std.testing.allocator, &prefixes, &.{ "a", "a" }));
    try std.testing.expect(!plan.dense_columns);
}

test "prepared score plan clone owns remapped descriptors and preserves canonical identity" {
    const a = std.testing.allocator;
    var source = PreparedScorePlan{};
    defer source.deinit();
    const prefixes = [_]?[]const u8{ "p/", null, "p/", "q/" };
    const reordered = [_]?[]const u8{ "q/", "p/", null, "p/" };
    const nodes = [_][]const u8{ "", "a\x00", "b", "a\x00" };
    const permuted = [_][]const u8{ "b", "a\x00", "", "a\x00" };
    try std.testing.expect(try source.prepare(a, &prefixes, &nodes));
    try std.testing.expect(try source.prepare(a, &reordered, &permuted));
    var copy = try source.clone(a);
    defer copy.deinit();
    try std.testing.expectEqual(source.identity, copy.identity);
    var expected: [4][4]?f64 = @splat(@splat(null));
    var actual: [4][4]?f64 = @splat(@splat(null));
    var output = PreparedScorePlan.Output{ .plan = &source, .columns = &.{ &expected[0], &expected[1], &expected[2], &expected[3] } };
    var values: [6]f64 = undefined;
    for (&values, 0..) |*value, i| value.* = if (i == 2) std.math.nan(f64) else @floatFromInt(i);
    output.completeSink().deliverDecoded(&values);
    source.deinit();
    try std.testing.expect(copy.matches(&reordered, &permuted));
    var cloned = PreparedScorePlan.Output{ .plan = &copy, .columns = &.{ &actual[0], &actual[1], &actual[2], &actual[3] } };
    cloned.completeSink().deliverDecoded(&values);
    for (expected, actual) |first, second| try std.testing.expectEqualSlices(?f64, &first, &second);
    // Rebuild the original and mutate the independent copy's mapping.
    try std.testing.expect(try source.prepare(a, &.{"other"}, &.{"different"}));
    try std.testing.expect(source.identity != copy.identity);
    const identity = copy.identity;
    try std.testing.expect(try copy.prepare(a, &prefixes, &nodes));
    try std.testing.expectEqual(identity, copy.identity);
}
