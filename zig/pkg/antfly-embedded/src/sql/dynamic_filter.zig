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

//! Immutable build-side evidence installed before a probe scan is pulled.
//! Bloom collisions retain candidates; exact join/residual checks remain.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const A = std.mem.Allocator;
pub const Column = struct {
    name: []const u8,
    type: @import("ast.zig").ColumnType,
    element_type: ?@import("array_value.zig").ElementType = null,
    pub fn datum(self: Column, a: A, raw: Datum) !Datum {
        if (raw.sql_null) return .{};
        if (self.element_type == .numeric) {
            if (raw.numeric != null) return raw;
            var ctx: @import("numeric_value.zig").Context = .{ .alloc = a };
            const number = try @import("numeric_storage.zig").fromJson(&ctx, raw.value);
            const value = try a.create(@import("numeric_value.zig").Value);
            value.* = number.value;
            return Datum.typedNumeric(value);
        }
        return .{ .value = try @import("describe.zig").coerceAlloc(a, raw.value, self.type), .sql_null = false };
    }
};
pub const Domain = struct {
    minimum: ?Datum = null,
    maximum: ?Datum = null,
    min_owner: std.heap.ArenaAllocator,
    max_owner: std.heap.ArenaAllocator,
    fn add(self: *Domain, value: Datum) !void {
        if (value.sql_null) return;
        if (self.minimum == null or (try scalar.compareDatums(value, self.minimum.?)) == .lt) {
            _ = self.min_owner.reset(.free_all);
            self.minimum = try @import("operators.zig").cloneDatum(self.min_owner.allocator(), value);
        }
        if (self.maximum == null or (try scalar.compareDatums(value, self.maximum.?)) == .gt) {
            _ = self.max_owner.reset(.free_all);
            self.maximum = try @import("operators.zig").cloneDatum(self.max_owner.allocator(), value);
        }
    }
};
pub const Filter = struct {
    pub const max_exact_keys = 4096;
    pub const ExactKey = struct { hash: u64, values: []const Datum, next: ?usize };
    a: A,
    columns: []Column,
    domains: []Domain,
    bits: []u64,
    rows: usize = 0,
    sealed: bool = false,
    failed: bool = false,
    exact_complete: bool = true,
    spill_manager: ?*@import("spill.zig").Manager = null,
    exact_sort: ?@import("spill.zig").Sort = null,
    exact_orders: []@import("operators.zig").Order = &.{},
    exact_budget: @import("memory_budget.zig"),
    exact_arena: std.heap.ArenaAllocator = undefined,
    exact_keys: std.ArrayList(ExactKey) = .empty,
    exact_buckets: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    pub fn create(a: A, columns: []const Column, bytes: usize) !*Filter {
        const self = try a.create(Filter);
        errdefer a.destroy(self);
        const keys = try a.dupe(Column, columns);
        errdefer a.free(keys);
        const domains = try a.alloc(Domain, columns.len);
        errdefer a.free(domains);
        const bits = try a.alloc(u64, @max(16, @min(8192, bytes / 8)));
        @memset(bits, 0);
        for (domains) |*domain| domain.* = .{ .min_owner = .init(a), .max_owner = .init(a) };
        self.* = .{ .a = a, .columns = keys, .domains = domains, .bits = bits, .exact_budget = .{ .backing = a, .limit = 1024 * 1024 } };
        self.exact_arena = .init(self.exact_budget.allocator());
        return self;
    }
    pub fn createWithSpill(a: A, columns: []const Column, bytes: usize, manager: ?*@import("spill.zig").Manager) !*Filter {
        const self = try create(a, columns, bytes);
        self.spill_manager = manager;
        return self;
    }
    pub fn close(self: *Filter) void {
        if (self.exact_sort) |*sort| sort.deinit();
        self.a.free(self.exact_orders);
        self.exact_arena.deinit();
        for (self.domains) |*domain| {
            domain.min_owner.deinit();
            domain.max_owner.deinit();
        }
        self.a.free(self.domains);
        self.a.free(self.columns);
        self.a.free(self.bits);
        self.a.destroy(self);
    }
    fn mask(self: *const Filter, hash: u64) struct { index: usize, bit: u64 } {
        const position = hash % (self.bits.len * 64);
        return .{ .index = @intCast(position / 64), .bit = @as(u64, 1) << @as(u6, @intCast(position % 64)) };
    }
    pub fn add(self: *Filter, values: []const Datum) !void {
        if (self.failed or self.sealed or values.len != self.columns.len) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        const hash = (try @import("operators.zig").HashJoin.keyHash(values)) orelse return;
        for (self.domains, values) |*domain, value| try domain.add(value);
        const first = self.mask(hash);
        const second = self.mask(std.math.rotr(u64, hash, 23));
        self.bits[first.index] |= first.bit;
        self.bits[second.index] |= second.bit;
        self.rows += 1;
        if (self.exact_complete) self.addExact(hash, values) catch |err| switch (err) {
            error.OutOfMemory => {
                if (self.exact_sort != null) {
                    self.discardExact();
                    return;
                }
                self.spillExact() catch |spill_error| switch (spill_error) {
                    error.OutOfMemory, error.SqlProgramLimitExceeded => self.discardExact(),
                    else => return spill_error,
                };
                if (self.exact_sort) |*sort| sort.add(.{ .keys = values, .values = &.{}, .ordinal = self.rows }) catch |spill_error| switch (spill_error) {
                    error.OutOfMemory, error.SqlProgramLimitExceeded => self.discardExact(),
                    else => return spill_error,
                };
            },
            error.SqlProgramLimitExceeded => self.discardExact(),
            else => return err,
        };
    }
    fn discardExact(self: *Filter) void {
        self.exact_complete = false;
        if (self.exact_sort) |*sort| sort.deinit();
        self.exact_sort = null;
        self.a.free(self.exact_orders);
        self.exact_orders = &.{};
        _ = self.exact_arena.reset(.free_all);
        self.exact_keys = .empty;
        self.exact_buckets = .empty;
    }
    fn spillExact(self: *Filter) !void {
        if (self.exact_sort != null) return;
        const manager = self.spill_manager orelse {
            self.discardExact();
            return;
        };
        const orders = try self.a.alloc(@import("operators.zig").Order, self.columns.len);
        errdefer self.a.free(orders);
        @memset(orders, .{});
        var sort = @import("spill.zig").Sort.init(self.a, manager, orders, 1024 * 1024);
        errdefer sort.deinit();
        sort.shared_runs = true;
        sort.parallel_runs = false;
        for (self.exact_keys.items, 0..) |key, index| try sort.add(.{ .keys = key.values, .values = &.{}, .ordinal = index });
        self.exact_sort = sort;
        self.exact_orders = orders;
        _ = self.exact_arena.reset(.free_all);
        self.exact_keys = .empty;
        self.exact_buckets = .empty;
    }
    fn addExact(self: *Filter, hash: u64, values: []const Datum) !void {
        if (self.exact_sort) |*sort| return sort.add(.{ .keys = values, .values = &.{}, .ordinal = self.rows });
        var next = self.exact_buckets.get(hash);
        while (next) |index| {
            const key = self.exact_keys.items[index];
            var equal = true;
            for (key.values, values) |l, r| if ((try scalar.compareDatums(l, r)) != .eq) {
                equal = false;
                break;
            };
            if (equal) return;
            next = key.next;
        }
        if (self.exact_keys.items.len == max_exact_keys) {
            try self.spillExact();
            if (self.exact_sort) |*sort| try sort.add(.{ .keys = values, .values = &.{}, .ordinal = self.rows });
            return;
        }
        const a = self.exact_arena.allocator();
        try self.exact_keys.ensureUnusedCapacity(a, 1);
        try self.exact_buckets.ensureUnusedCapacity(a, 1);
        const owned = try a.alloc(Datum, values.len);
        for (owned, values) |*out, value| out.* = try @import("operators.zig").cloneDatum(a, value);
        const index = self.exact_keys.items.len;
        self.exact_keys.appendAssumeCapacity(.{ .hash = hash, .values = owned, .next = self.exact_buckets.get(hash) });
        self.exact_buckets.putAssumeCapacity(hash, index);
    }
    /// Complete, deduplicated build tuples. Absence means retain the bounded
    /// Bloom/domain path; a partial exact set must never restrict a scan.
    pub fn exactKeys(self: *const Filter) ?[]const ExactKey {
        if (!self.sealed or self.failed or !self.exact_complete or self.exact_sort != null) return null;
        return self.exact_keys.items;
    }
    pub fn hasExact(self: *const Filter) bool {
        return self.sealed and !self.failed and self.exact_complete;
    }
    /// Statement-owned replayable sorted tuples. Duplicates are allowed; native
    /// consumers deduplicate their projected prefixes with a bounded merge.
    pub fn exactStream(self: *const Filter) ?*@import("spill.zig").Sort {
        if (!self.hasExact()) return null;
        const mutable: *Filter = @constCast(self);
        return if (mutable.exact_sort) |*sort| sort else null;
    }
    pub fn applyBatch(self: *const Filter, a: A, batch: @import("../storage/rowsource/types.zig").ColumnBatch, selected: []bool) !void {
        if (self.failed or !self.sealed or selected.len != batch.rowCount()) return error.InvalidSqlBackendResponse;
        if (self.rows == 0) {
            @memset(selected, false);
            return;
        }
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        // Keep hash lanes on the stack, independent of scan width/cardinality.
        // Primitive columns dispatch once; exact/complex coercions reuse one
        // scalar lane whose temporary payload expires immediately after hashing.
        var begin: usize = 0;
        while (begin < selected.len) {
            const count = @min(128, selected.len - begin);
            var hashes: [128]std.hash.Wyhash = undefined;
            for (hashes[0..count]) |*hash| hash.* = .init(0);
            const keep = selected[begin..][0..count];
            for (self.columns, self.domains) |definition, domain| {
                if (batch.findColumn(definition.name)) |vector| {
                    if (vector.rowCount() != selected.len) return error.InvalidSqlBackendResponse;
                    vector.nulls.validate(selected.len) catch return error.InvalidSqlBackendResponse;
                    if (try nativeColumn(vector, definition, domain, begin, keep, hashes[0..count])) continue;
                }
                for (keep, hashes[0..count], begin..) |*active, *hash, row| {
                    if (!active.*) continue;
                    _ = scratch.reset(.retain_capacity);
                    const one: @import("catalog.zig").ColumnPage = .{ .batch = batch, .selection = &.{row} };
                    const value = try definition.datum(scratch.allocator(), try one.typedCell(scratch.allocator(), 0, definition.name, definition.element_type == .numeric));
                    if (value.sql_null or (try scalar.compareDatums(value, domain.minimum.?)) == .lt or (try scalar.compareDatums(value, domain.maximum.?)) == .gt) {
                        active.* = false;
                        continue;
                    }
                    var bytes: [8]u8 = undefined;
                    std.mem.writeInt(u64, &bytes, try scalar.semanticHashDatum(value), .little);
                    hash.update(&bytes);
                }
            }
            for (keep, hashes[0..count]) |*active, *state| {
                if (!active.*) continue;
                const hash = state.final();
                const first = self.mask(hash);
                const second = self.mask(std.math.rotr(u64, hash, 23));
                active.* = self.bits[first.index] & first.bit != 0 and self.bits[second.index] & second.bit != 0;
            }
            begin += count;
        }
    }
    fn nativeColumn(vector: @import("../storage/rowsource/types.zig").ColumnVector, definition: Column, domain: Domain, begin: usize, keep: []bool, hashes: []std.hash.Wyhash) !bool {
        if (definition.element_type == .numeric) return false;
        const lower = domain.minimum orelse return error.InvalidSqlBackendResponse;
        const upper = domain.maximum orelse return error.InvalidSqlBackendResponse;
        const supported = switch (vector.values) {
            .i64, .dictionary_i64 => definition.type == .integer and lower.value == .integer and upper.value == .integer,
            .f64, .dictionary_f64 => definition.type == .number and lower.value == .float and upper.value == .float,
            .bytes, .dictionary_bytes => definition.type == .string and lower.value == .string and upper.value == .string,
            .bool => definition.type == .boolean and lower.value == .bool and upper.value == .bool,
            else => false,
        };
        if (!supported) return false;
        if (lower.value == .float and (std.math.isNan(lower.value.float) or std.math.isNan(upper.value.float))) return false;
        // Memoize page-local dictionary identities without allocator pressure.
        // Only selected nonnull IDs are inspected; malformed excluded lanes
        // preserve the scalar lazy boundary.
        var used: [256]u8 = @splat(0);
        var ids: [256]u32 = undefined;
        var semantic: [256]u64 = undefined;
        switch (vector.values) {
            inline .i64, .dictionary_i64, .f64, .dictionary_f64, .bytes, .dictionary_bytes, .bool => |values, tag| {
                const dictionary = tag == .dictionary_i64 or tag == .dictionary_f64 or tag == .dictionary_bytes;
                for (keep, hashes, begin..) |*active, *hash, row| {
                    if (!active.*) continue;
                    if (vector.nulls.isNull(row)) {
                        active.* = false;
                        continue;
                    }
                    const id: u32 = if (dictionary) values.indices[row] else 0;
                    var slot: usize = (id *% 2654435761) & 255;
                    if (dictionary) {
                        if (id >= values.values.len) return error.InvalidSqlBackendResponse;
                        while (used[slot] != 0 and ids[slot] != id) slot = (slot + 1) & 255;
                        if (used[slot] != 0) {
                            active.* = used[slot] == 2;
                            if (active.*) {
                                var bytes: [8]u8 = undefined;
                                std.mem.writeInt(u64, &bytes, semantic[slot], .little);
                                hash.update(&bytes);
                            }
                            continue;
                        }
                    }
                    const value = if (dictionary) values.values[id] else values[row];
                    const datum = Datum.json(switch (tag) {
                        .i64, .dictionary_i64 => .{ .integer = value },
                        .f64, .dictionary_f64 => .{ .float = value },
                        .bytes, .dictionary_bytes => .{ .string = value },
                        .bool => .{ .bool = value },
                        else => unreachable,
                    });
                    const allowed = switch (tag) {
                        .i64, .dictionary_i64 => value >= lower.value.integer and value <= upper.value.integer,
                        .f64, .dictionary_f64 => blk: {
                            if (!std.math.isFinite(value)) return error.SqlTypeMismatch;
                            break :blk value >= lower.value.float and value <= upper.value.float;
                        },
                        .bytes, .dictionary_bytes => std.mem.order(u8, value, lower.value.string) != .lt and std.mem.order(u8, value, upper.value.string) != .gt,
                        .bool => @intFromBool(value) >= @intFromBool(lower.value.bool) and @intFromBool(value) <= @intFromBool(upper.value.bool),
                        else => unreachable,
                    };
                    active.* = allowed;
                    const key = if (allowed) try scalar.semanticHashDatum(datum) else 0;
                    if (dictionary) {
                        ids[slot] = id;
                        used[slot] = if (allowed) 2 else 1;
                        semantic[slot] = key;
                    }
                    if (allowed) {
                        var bytes: [8]u8 = undefined;
                        std.mem.writeInt(u64, &bytes, key, .little);
                        hash.update(&bytes);
                    }
                }
            },
            else => unreachable,
        }
        return true;
    }
    pub fn contains(self: *const Filter, values: []const Datum) !bool {
        if (self.failed or !self.sealed or values.len != self.columns.len) return error.InvalidSqlBackendResponse;
        if (self.rows == 0) return false;
        const hash = (try @import("operators.zig").HashJoin.keyHash(values)) orelse return false;
        const first = self.mask(hash);
        const second = self.mask(std.math.rotr(u64, hash, 23));
        if (self.bits[first.index] & first.bit == 0 or self.bits[second.index] & second.bit == 0) return false;
        for (self.domains, values) |domain, value| {
            if ((try scalar.compareDatums(value, domain.minimum.?)) == .lt or (try scalar.compareDatums(value, domain.maximum.?)) == .gt) return false;
        }
        return true;
    }
};
test "SQL scan dynamic filters preserve composite numeric equality and SQL nulls" {
    const a = std.testing.allocator;
    const filter = try Filter.create(a, &.{ .{ .name = "n", .type = .integer }, .{ .name = "s", .type = .string } }, 256);
    defer filter.close();
    for (0..1000) |i| try filter.add(&.{ Datum.json(.{ .integer = @intCast(i * 3) }), Datum.json(.{ .string = "x" }) });
    try filter.add(&.{ .{}, Datum.json(.{ .string = "null" }) });
    filter.sealed = true;
    for (0..1000) |i| try std.testing.expect(try filter.contains(&.{ Datum.json(.{ .float = @floatFromInt(i * 3) }), Datum.json(.{ .string = "x" }) }));
    try std.testing.expect(!try filter.contains(&.{ .{}, Datum.json(.{ .string = "x" }) }));
    try std.testing.expect(!try filter.contains(&.{ Datum.json(.{ .integer = 4000 }), Datum.json(.{ .string = "x" }) }));
    try std.testing.expectError(error.InvalidSqlBackendResponse, filter.add(&.{ .{}, .{} }));
}

test "SQL batch dynamic masks match scalar membership for dictionaries nulls and selections" {
    const a = std.testing.allocator;
    const filter = try Filter.create(a, &.{ .{ .name = "s", .type = .string }, .{ .name = "n", .type = .integer } }, 256);
    defer filter.close();
    try filter.add(&.{ Datum.json(.{ .string = "yes" }), Datum.json(.{ .integer = 9007199254740993 }) });
    filter.sealed = true;
    const batch: @import("../storage/rowsource/types.zig").ColumnBatch = .{
        .snapshot = .{ .table_id = "t", .snapshot_id = "s" },
        .row_refs = &.{ .{ .relational_key = "1" }, .{ .relational_key = "2" }, .{ .relational_key = "3" }, .{ .relational_key = "4" } },
        .columns = &.{
            .{ .name = "s", .values = .{ .dictionary_bytes = .{ .values = &.{ "no", "yes" }, .indices = &.{ 1, 0, 99, 1 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
            .{ .name = "n", .values = .{ .dictionary_i64 = .{ .values = &.{9007199254740993}, .indices = &.{ 0, 0, 99, 0 } } }, .nulls = .{ .bytes = &.{ 0, 0, 1, 0 } } },
        },
    };
    var selected = [_]bool{ true, true, true, false };
    try filter.applyBatch(a, batch, &selected);
    try std.testing.expectEqualSlices(bool, &.{ true, false, false, false }, &selected);
}

test "SQL dynamic exact keys own values deduplicate semantic tuples and abandon incomplete sets" {
    const a = std.testing.allocator;
    const filter = try Filter.create(a, &.{.{ .name = "k", .type = .integer }}, 256);
    defer filter.close();
    try filter.add(&.{Datum.json(.{ .integer = 42 })});
    for (0..1000) |_| try filter.add(&.{Datum.json(.{ .float = 42 })});
    try filter.add(&.{.{}});
    filter.sealed = true;
    try std.testing.expectEqual(@as(usize, 1), filter.exactKeys().?.len);
    try std.testing.expectEqual(@as(i64, 42), filter.exactKeys().?[0].values[0].value.integer);
    filter.sealed = false;
    for (0..Filter.max_exact_keys + 1) |i| try filter.add(&.{Datum.json(.{ .integer = @intCast(i + 100) })});
    filter.sealed = true;
    try std.testing.expect(filter.exactKeys() == null);
    try std.testing.expectEqual(@as(usize, 0), filter.exact_budget.live);
    for (0..Filter.max_exact_keys + 1) |i| try std.testing.expect(try filter.contains(&.{Datum.json(.{ .integer = @intCast(i + 100) })}));
    const strings = try Filter.create(a, &.{.{ .name = "s", .type = .string }}, 256);
    defer strings.close();
    var borrowed = [_]u8{ 'a', 'b', 'c' };
    try strings.add(&.{Datum.json(.{ .string = &borrowed })});
    borrowed[0] = 'x';
    strings.sealed = true;
    try std.testing.expectEqualStrings("abc", strings.exactKeys().?[0].values[0].value.string);
}

test "SQL dynamic exact keys spill complete tuples and abandon resource limited streams safely" {
    const a = std.testing.allocator;
    const Check = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check };
    defer manager.deinit();
    const filter = try Filter.createWithSpill(a, &.{.{ .name = "k", .type = .integer }}, 256, &manager);
    defer filter.close();
    const count = 20_000;
    for (0..count) |i| try filter.add(&.{Datum.json(.{ .integer = @intCast(count - i) })});
    try filter.add(&.{Datum.json(.{ .integer = 100 })});
    filter.sealed = true;
    try std.testing.expect(filter.exactKeys() == null);
    try std.testing.expect(filter.hasExact());
    try std.testing.expectEqual(@as(usize, 0), filter.exact_budget.live);
    const stream = filter.exactStream().?;
    for (0..2) |_| {
        try stream.rewind();
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        var n: usize = 0;
        var previous: i64 = 0;
        while (true) {
            _ = scratch.reset(.retain_capacity);
            const row = try stream.next(scratch.allocator()) orelse break;
            const key = row.keys[0].value.integer;
            try std.testing.expect(key >= previous);
            try std.testing.expect(try filter.contains(row.keys));
            previous = key;
            n += 1;
        }
        try std.testing.expectEqual(@as(usize, count + 1), n);
    }
    // Disk admission failure must never leave a restrictive partial exact set.
    var limited: @import("spill.zig").Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Check.check, .max_bytes = 1 };
    defer limited.deinit();
    const fallback = try Filter.createWithSpill(a, &.{.{ .name = "k", .type = .integer }}, 256, &limited);
    defer fallback.close();
    for (0..count) |i| try fallback.add(&.{Datum.json(.{ .integer = @intCast(i) })});
    fallback.sealed = true;
    try std.testing.expect(!fallback.hasExact());
    try std.testing.expectEqual(@as(u64, 0), limited.live_bytes);
    for (0..count) |i| try std.testing.expect(try fallback.contains(&.{Datum.json(.{ .integer = @intCast(i) })}));
}

test "lake SQL native dynamic filter masks use bounded allocation-free primitive lanes" {
    const a = std.testing.allocator;
    const filter = try Filter.create(a, &.{ .{ .name = "n", .type = .integer }, .{ .name = "s", .type = .string } }, 256);
    defer filter.close();
    try filter.add(&.{ Datum.json(.{ .integer = 7 }), Datum.json(.{ .string = "yes" }) });
    filter.sealed = true;
    var integers: [4096]i64 = @splat(7);
    integers[2048] = 9;
    var ids: [4096]u32 = @splat(0);
    ids[2049] = 1;
    const batch: @import("../storage/rowsource/types.zig").ColumnBatch = .{ .snapshot = .{ .table_id = "t", .snapshot_id = "s" }, .external_span = .{ .first = .{ .source_id = "t", .snapshot_id = "s", .file_id = "f", .row_group_ordinal = 0, .row_ordinal = 0 }, .count = integers.len }, .columns = &.{ .{ .name = "n", .values = .{ .i64 = &integers } }, .{ .name = "s", .values = .{ .dictionary_bytes = .{ .values = &.{ "yes", "no" }, .indices = &ids } } } } };
    var selected: [4096]bool = @splat(true);
    try filter.applyBatch(std.testing.failing_allocator, batch, &selected);
    for (selected, 0..) |keep, i| try std.testing.expectEqual(i != 2048 and i != 2049, keep);
}
