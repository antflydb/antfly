// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Exact normalized-key indexes over immutable aggregate blocks. Hash/radix
//! reduction ownership remains independent of the ordered serving forest.
const std = @import("std");
const local = @import("antfly_local_sources");
const keys = @import("lake_index_aggregate_keys.zig");
const directory = @import("lake_index_native_directory.zig");
const stores = @import("../serverless/artifacts/store.zig");
const tree = directory.tree;
const A = std.mem.Allocator;
const Token = @import("antfly_cancellation").CancellationToken;
/// Inline pages use hexadecimal text so the root remains portable JSON.
pub const Index = struct { domain: [32]u8, page: tree.Ref, inline_page: []const u8 = &.{} };

/// A flat reduction changes at most its own serving tree. Immutable sibling
/// indexes survive parent reduction/path-copy updates without a global rebuild.
pub const Builder = struct {
    store: *stores.ArtifactStore,
    token: Token,
    recipe: local.sql_aggregate_materialization.Recipe,
    page: ?tree.Ref = null,
    disabled: bool = false,
    allow_inline: bool = false,
    inline_page: ?struct { a: A, bytes: []u8 } = null,
    reads: u64 = 512 * 1024 * 1024,
    writes: u64 = 512 * 1024 * 1024,
    pub fn deinit(self: *Builder) void {
        if (self.inline_page) |page| page.a.free(page.bytes);
    }
    pub fn result(self: *const Builder) ?Index {
        if (self.disabled or self.page == null) return null;
        return .{ .domain = self.store.upload_scope.?.domain, .page = self.page.?, .inline_page = if (self.inline_page) |page| page.bytes else &.{} };
    }
    pub fn add(self: *Builder, a: A, rows: []const local.sql_operators.Row, block: u32) !void {
        self.addAdmitted(a, rows, block) catch |err| {
            // Navigation is optional. A deterministic local tree limit must
            // not reject a complete, otherwise admissible materialization.
            if (err == error.GraphPageWriteBudgetExceeded or err == error.GraphPageRecordTooLarge or err == error.GraphPageTreeTooDeep) {
                self.disabled = true;
                return;
            }
            return err;
        };
    }
    fn addAdmitted(self: *Builder, a: A, rows: []const local.sql_operators.Row, block: u32) !void {
        if (self.disabled) return;
        if (self.store.upload_scope == null or self.recipe.keys.len == 0 or (try keys.rowBoundsTyped(a, rows, self.recipe)) == null) {
            self.disabled = true;
            return;
        }
        var scratch = std.heap.ArenaAllocator.init(local.sql_memory_budget.reclaimingAllocator(a));
        defer scratch.deinit();
        const sa = scratch.allocator();
        const edits = try sa.alloc(tree.Mutation, rows.len);
        for (edits, rows, 0..) |*edit, row, ordinal| {
            const coordinate = try sa.alloc(u8, 6);
            std.mem.writeInt(u32, coordinate[0..4], block, .big);
            std.mem.writeInt(u16, coordinate[4..6], @intCast(ordinal), .big);
            edit.* = .{ .key = try keys.encode(sa, row.keys), .value = coordinate };
        }
        std.mem.sort(tree.Mutation, edits, {}, struct {
            fn less(_: void, l: tree.Mutation, r: tree.Mutation) bool {
                return std.mem.lessThan(u8, l.key, r.key);
            }
        }.less);
        var access: directory.Access = undefined;
        access.init(self.store.*, self.store.upload_scope.?.domain, self.token, null);
        access.reads = self.reads;
        access.work = &self.writes;
        if (self.allow_inline and self.page == null) {
            var capture: Capture = .{ .a = a, .underlying = access.backing() };
            defer if (capture.pending) |page| a.free(page.bytes);
            var backing = capture.underlying;
            backing.ptr = &capture;
            backing.get = Capture.get;
            backing.put = Capture.put;
            backing.put_many = null;
            backing.check = Capture.check;
            self.page = try tree.apply(a, backing, null, edits);
            if (capture.pending) |page| {
                const encoded = try a.alloc(u8, page.bytes.len * 2);
                const digits = "0123456789abcdef";
                for (page.bytes, 0..) |byte, i| {
                    encoded[i * 2] = digits[byte >> 4];
                    encoded[i * 2 + 1] = digits[byte & 15];
                }
                self.inline_page = .{ .a = a, .bytes = encoded };
            }
        } else self.page = try tree.apply(a, access.backing(), self.page, edits);
        self.reads = access.reads;
    }
    // Packs embed their single navigation leaf, preserving the one-object
    // small-cohort format. Larger indexes flush every page to the usual store.
    const Capture = struct {
        a: A,
        underlying: tree.Store,
        pending: ?struct { ref: tree.Ref, bytes: []u8 } = null,
        external: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.underlying.check(self.underlying.ptr);
        }
        fn get(raw: *anyopaque, a: A, ref: tree.Ref) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.pending) |page| if (page.ref.eql(ref)) return a.dupe(u8, page.bytes);
            return self.underlying.get(self.underlying.ptr, a, ref);
        }
        fn put(raw: *anyopaque, ref: tree.Ref, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.external and self.pending == null) {
                self.pending = .{ .ref = ref, .bytes = try self.a.dupe(u8, bytes) };
                return;
            }
            if (self.pending) |page| {
                try self.underlying.put(self.underlying.ptr, page.ref, page.bytes);
                self.a.free(page.bytes);
                self.pending = null;
            }
            self.external = true;
            try self.underlying.put(self.underlying.ptr, ref, bytes);
        }
    };
};

/// A bounded merge retains each exact index cursor across result pulls. No
/// payload scan or TopK is restarted at page boundaries. Admission completes
/// before the provider attests ordering; excessive navigation uses SQL's
/// existing one-pass external sort instead.
pub fn Plan(comptime Reader: type) type {
    return struct {
        const Self = @This();
        const Leaf = struct {
            reader: *Reader,
            borrowed: bool,
            previous_raw: bool = false,
            access: directory.Access = undefined,
            cursor: ?tree.Cursor = null,
            record: ?tree.Cursor.Record = null,
            fn backing(self: *Leaf) tree.Store {
                var store = self.access.backing();
                if (self.reader.root.key_index.?.inline_page.len != 0) {
                    store.ptr = self;
                    store.get = get;
                    store.check = check;
                    store.put_many = null;
                }
                return store;
            }
            fn check(raw: *anyopaque) !void {
                const self: *Leaf = @ptrCast(@alignCast(raw));
                try self.reader.cancellation.check();
                if (self.reader.cached) |cached| try cached.context.ensureActive();
            }
            fn get(raw: *anyopaque, a: A, ref: tree.Ref) ![]u8 {
                const self: *Leaf = @ptrCast(@alignCast(raw));
                try check(raw);
                const index = self.reader.root.key_index.?;
                if (!index.page.eql(ref)) return error.InvalidNativeAggregateArtifact;
                const bytes = try a.alloc(u8, index.page.bytes);
                errdefer a.free(bytes);
                _ = std.fmt.hexToBytes(bytes, index.inline_page) catch return error.InvalidNativeAggregateArtifact;
                return bytes;
            }
        };
        fn compare(reverse: bool, l: *Leaf, r: *Leaf) std.math.Order {
            const order = std.mem.order(u8, l.record.?.key, r.record.?.key);
            return if (reverse) order.invert() else order;
        }
        a: A,
        root: *Reader,
        budget: local.sql_memory_budget,
        leaves: std.ArrayList(*Leaf) = .empty,
        queue: std.PriorityQueue(*Leaf, bool, compare),
        current: ?*Leaf = null,
        emitted: u64 = 0,
        pub fn prepare(root: *Reader, bytes: usize) !?*Self {
            const self = try root.a.create(Self);
            self.* = .{ .a = root.a, .root = root, .budget = .{ .backing = local.sql_memory_budget.reclaimingAllocator(root.a), .limit = bytes }, .queue = .initContext(root.reverse) };
            self.collect(root, true) catch |err| {
                const decline = err == error.AggregateOrderedIndexUnavailable or (err == error.OutOfMemory and self.budget.isLocallyExhausted());
                self.deinit();
                if (decline) return null;
                return err;
            };
            return self;
        }
        fn collect(self: *Self, reader: *Reader, borrowed: bool) anyerror!void {
            var retained = borrowed;
            defer if (!retained) reader.cursor().close(reader);
            if (self.root.key_range) |range| if (!range.overlaps(reader.root.bounds)) return;
            if (reader.root.partitions.len != 0) {
                for (reader.root.partitions, 0..) |part, index| {
                    if (self.root.key_range) |range| if (!range.overlaps(part.bounds)) continue;
                    if (reader.point_hash) |hash| {
                        const depth = reader.root.radix_depth;
                        const shift: u6 = if (depth == 0) 0 else @intCast(6 + (depth - 1) * 2);
                        if (part.bucket != (hash >> shift) % (if (depth == 0) @as(u64, 64) else 4)) continue;
                    }
                    const child = try reader.partitionReaderAllocated(index, self.budget.allocator());
                    try self.collect(child, false);
                }
                return;
            }
            if (reader.root.groups == 0) return;
            const index = reader.root.key_index orelse return error.AggregateOrderedIndexUnavailable;
            const leased = if (reader.pack_metadata) |owner| owner.lease.item.budget.live else if (reader.metadata) |owner| owner.lease.item.budget.live else 0;
            if (!borrowed) {
                if (leased > self.budget.limit -| self.budget.live) return error.AggregateOrderedIndexUnavailable;
                self.budget.limit -= leased;
            }
            const a = self.budget.allocator();
            const leaf = try a.create(Leaf);
            var adopted = false;
            errdefer if (!adopted) {
                if (leaf.cursor) |*cursor| cursor.deinit();
                a.destroy(leaf);
            };
            leaf.* = .{ .reader = reader, .borrowed = borrowed, .previous_raw = reader.raw_states };
            leaf.access.init(reader.store, index.domain, reader.cancellation, reader.cached);
            const range = self.root.key_range orelse keys.Range{};
            leaf.cursor = if (self.root.reverse) try tree.Cursor.initReverse(a, leaf.backing(), index.page, range.lower, range.upper) else try tree.Cursor.init(a, leaf.backing(), index.page, range.lower, range.upper);
            leaf.record = try leaf.cursor.?.next();
            if (leaf.record == null) {
                // A narrow range can overlap every hash-directory fence while
                // matching only one serving leaf. Do not retain empty cursors
                // or their immutable leases through the rest of preflight.
                leaf.cursor.?.deinit();
                a.destroy(leaf);
                if (!borrowed) self.budget.limit += leased;
                return;
            }
            try self.leaves.append(a, leaf);
            adopted = true;
            retained = true;
            // Payload residency is separate from the admitted navigation. At
            // most one leaf's bounded block is retained by this merge owner.
            reader.page.child_allocator = self.a;
            reader.raw_states = true;
            if (leaf.record != null) try self.queue.push(a, leaf);
        }
        pub fn next(self: *Self, a: A, maximum: u32) !?[]const local.sql_operators.GroupResult {
            try self.root.cancellation.check();
            if (self.root.cached) |cached| try cached.context.ensureActive();
            if (maximum == 0) return error.InvalidSqlLimit;
            if (self.queue.items.len == 0) return null;
            if (self.root.selection_pages_left == 0) return error.SqlProgramLimitExceeded;
            self.root.selection_pages_left -= 1;
            const count = @min(maximum, 256);
            const result = try a.alloc(local.sql_operators.GroupResult, count);
            var n: usize = 0;
            while (n < count) {
                const leaf = self.queue.pop() orelse break;
                if (self.root.selection_rows_left == 0) return error.SqlProgramLimitExceeded;
                self.root.selection_rows_left -= 1;
                if (self.current != leaf) {
                    if (self.current) |old| old.reader.discardBlock();
                    self.current = leaf;
                }
                const record = leaf.record.?;
                if (record.value.len != 6) return error.InvalidNativeAggregateArtifact;
                const block = std.mem.readInt(u32, record.value[0..4], .big);
                const row = std.mem.readInt(u16, record.value[4..6], .big);
                var scratch = std.heap.ArenaAllocator.init(self.a);
                defer scratch.deinit();
                const before = leaf.reader.blocks_decoded;
                const input = try leaf.reader.readIndexed(scratch.allocator(), block, row);
                if (!leaf.borrowed) self.root.blocks_decoded += leaf.reader.blocks_decoded - before;
                const encoded = try keys.encode(scratch.allocator(), input.keys);
                if (!std.mem.eql(u8, encoded, record.key)) return error.InvalidNativeAggregateArtifact;
                const key_cells = try a.alloc(local.sql_scalar.Datum, input.keys.len);
                for (key_cells, input.keys) |*cell, value| cell.* = try local.sql_operators.cloneDatum(a, value);
                const width = try local.sql_aggregate_partial.partialCount(input);
                const states = try a.alloc(local.sql_operators.Aggregate, width);
                for (states, 0..) |*state, column| {
                    const physical = leaf.reader.physicalSlot(column);
                    const spec = (leaf.reader.root.state_recipe orelse leaf.reader.root.recipe).inputs[physical].spec;
                    state.* = try local.sql_aggregate_partial.cloneBorrowed(a, try local.sql_aggregate_partial.read(scratch.allocator(), input, column, spec));
                }
                result[n] = .{ .keys = key_cells, .aggregates = &.{}, .typed_states = states, .aggregate_slots = self.root.output_slots, .ordinal = self.emitted };
                n += 1;
                self.emitted += 1;
                leaf.record = try leaf.cursor.?.next();
                if (leaf.record != null) try self.queue.push(self.budget.allocator(), leaf);
            }
            return result[0..n];
        }
        pub fn deinit(self: *Self) void {
            const a = self.budget.allocator();
            self.queue.deinit(a);
            for (self.leaves.items) |leaf| {
                if (leaf.cursor) |*cursor| cursor.deinit();
                leaf.reader.raw_states = leaf.previous_raw;
                if (!leaf.borrowed) {
                    leaf.reader.cursor().close(leaf.reader);
                }
                a.destroy(leaf);
            }
            self.leaves.deinit(a);
            std.debug.assert(self.budget.live == 0);
            self.a.destroy(self);
        }
    };
}
