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

//! Column-oriented retained SQL state. Primitive values never retain Datum
//! tags; complex or heterogeneous columns retain native JSON values.
//! Datum rows are reconstructed only at an expression/result boundary. Validity is a packed SQL-null bitmap.
const std = @import("std");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const A = std.mem.Allocator;
/// Conservative physical payload estimate before cardinality is known.
/// Allocation admission still accounts for actual vector/hash capacity growth.
pub fn retainedCellBytes(value: Datum) !usize {
    return 1 +| (if (value.patterns != null) @sizeOf(?*scalar.PatternSet) else @as(usize, 0)) +| switch (value.value) {
        .integer, .float => @as(usize, 8),
        .bool => @as(usize, 1),
        .string, .number_string => |text| @sizeOf([]const u8) +| text.len +| 32,
        else => try @import("operators.zig").datumBytes(value),
    };
}
pub fn columnMetadataBytes(width: usize) usize {
    return width *| @sizeOf(Column);
}
const Dictionary = struct {
    values: std.ArrayList([]const u8) = .empty,
    indices: std.ArrayList(u32) = .empty,
    lookup: std.StringHashMapUnmanaged(u32) = .empty,
    flat: ?std.ArrayList([]const u8) = null,
    const empty: Dictionary = .{};
    fn growthBytes(self: *const Dictionary, additional: usize) usize {
        if (self.flat != null or additional <= self.lookup.available) return 0;
        // A hash-table resize allocates the replacement before releasing the
        // old table. Account for that capacity jump rather than one cell's
        // payload, including metadata, keys, values and alignment/header space.
        const entries = self.lookup.count() +| additional;
        const required = entries *| 100 / std.hash_map.default_max_load_percentage +| 1;
        const capacity = std.math.ceilPowerOfTwo(usize, @max(8, required)) catch return std.math.maxInt(usize);
        return capacity *| (@sizeOf(u8) + @sizeOf([]const u8) + @sizeOf(u32)) +| 64;
    }
    fn deinit(self: *Dictionary, a: A) void {
        self.values.deinit(a);
        self.indices.deinit(a);
        self.lookup.deinit(a);
        if (self.flat) |*values| values.deinit(a);
    }
    fn append(self: *Dictionary, a: A, owned: A, value: ?[]const u8) !void {
        // High-cardinality columns revert to flat storage after a bounded
        // sample; dictionary overhead must not penalize unique identifiers.
        if (self.flat == null and self.values.items.len >= 512 and self.values.items.len > self.indices.items.len / 2) {
            var flat: std.ArrayList([]const u8) = .empty;
            errdefer flat.deinit(a);
            try flat.ensureTotalCapacity(a, self.indices.items.len + 1);
            for (self.indices.items) |index| flat.appendAssumeCapacity(self.values.items[index]);
            self.values.clearAndFree(a);
            self.indices.clearAndFree(a);
            self.lookup.clearAndFree(a);
            self.flat = flat;
        }
        if (self.flat) |*values| {
            try values.append(a, if (value) |bytes| try owned.dupe(u8, bytes) else &.{});
            return;
        }
        const text = value orelse {
            try self.indices.append(a, 0);
            return;
        };
        if (self.lookup.get(text)) |index| {
            try self.indices.append(a, index);
            return;
        }
        const index = std.math.cast(u32, self.values.items.len) orelse return error.SqlProgramLimitExceeded;
        const bytes = try owned.dupe(u8, text);
        try self.values.append(a, bytes);
        try self.lookup.put(a, bytes, index);
        try self.indices.append(a, index);
    }
    fn resizeNulls(self: *Dictionary, a: A, count: usize) !void {
        try self.indices.resize(a, count);
        @memset(self.indices.items, 0);
    }
    fn getText(self: Dictionary, row: usize) []const u8 {
        return if (self.flat) |values| values.items[row] else self.values.items[self.indices.items[row]];
    }
};
// Sample once; retain repeated numeric values by ID without penalizing unique
// keys with a hash lookup on every subsequent append. Float bit patterns keep
// signed zero and NaN payloads distinct at this physical ownership boundary.
fn Numeric(comptime T: type) type {
    return struct {
        flat: std.ArrayList(T) = .empty,
        values: std.ArrayList(T) = .empty,
        indices: std.ArrayList(u32) = .empty,
        lookup: std.AutoHashMapUnmanaged(u64, u32) = .empty,
        encoded: bool = false,
        sampled: bool = false,
        const empty: @This() = .{};
        fn key(value: T) u64 {
            return @bitCast(value);
        }
        fn deinit(self: *@This(), a: A) void {
            self.flat.deinit(a);
            self.values.deinit(a);
            self.indices.deinit(a);
            self.lookup.deinit(a);
        }
        fn get(self: @This(), row: usize) T {
            return if (self.encoded) self.values.items[self.indices.items[row]] else self.flat.items[row];
        }
        fn resize(self: *@This(), a: A, count: usize) !void {
            try self.flat.resize(a, count);
            @memset(self.flat.items, 0);
        }
        fn encodeOne(self: *@This(), a: A, value: T) !void {
            const slot = try self.lookup.getOrPut(a, key(value));
            if (!slot.found_existing) {
                slot.value_ptr.* = @intCast(self.values.items.len);
                try self.values.append(a, value);
            }
            try self.indices.append(a, slot.value_ptr.*);
        }
        fn repeatedSample(self: *const @This()) bool {
            var keys: [512]u64 = undefined;
            var used: [8]u64 = @splat(0);
            var count: usize = 0;
            for (self.flat.items[0..256]) |value| {
                const bits = key(value);
                var slot: usize = @intCast((bits *% 0x9e3779b97f4a7c15) >> 55);
                while (true) : (slot = (slot + 1) & 511) {
                    const mask = @as(u64, 1) << @as(u6, @intCast(slot % 64));
                    if (used[slot / 64] & mask == 0) {
                        used[slot / 64] |= mask;
                        keys[slot] = bits;
                        count += 1;
                        if (count > 128) return false;
                        break;
                    }
                    if (keys[slot] == bits) break;
                }
            }
            return true;
        }
        fn append(self: *@This(), a: A, value: T) !void {
            if (!self.sampled and self.flat.items.len >= 256 and !@call(.never_inline, repeatedSample, .{self})) self.sampled = true;
            if (!self.sampled and self.flat.items.len >= 256) {
                // Build a candidate before publishing its representation.
                var candidate: @This() = .{ .sampled = true, .encoded = true };
                errdefer candidate.deinit(a);
                for (self.flat.items) |prior| try candidate.encodeOne(a, prior);
                self.sampled = true;
                if (candidate.values.items.len <= self.flat.items.len / 2) {
                    self.flat.deinit(a);
                    self.* = candidate;
                } else candidate.deinit(a);
            }
            if (self.encoded and self.values.items.len >= 512 and self.values.items.len > self.indices.items.len / 2) {
                var flat: std.ArrayList(T) = .empty;
                errdefer flat.deinit(a);
                try flat.ensureTotalCapacity(a, self.indices.items.len + 1);
                for (self.indices.items) |id| flat.appendAssumeCapacity(self.values.items[id]);
                self.values.clearAndFree(a);
                self.indices.clearAndFree(a);
                self.lookup.clearAndFree(a);
                self.flat = flat;
                self.encoded = false;
            }
            if (self.encoded) try self.encodeOne(a, value) else try self.flat.append(a, value);
        }
    };
}
// Shared per retained store, with stable addresses for managed JSON arrays.
// Cached trees have a strict aggregate cap; large/cold values decode into the
// consumer's scratch instead. Failed speculative caches release all capacity.
const JsonCache = struct {
    const limit = 512 * 1024;
    budget: @import("memory_budget.zig"),
    text_arena: std.heap.ArenaAllocator,
    entries: ?*Entry = null,
    mutex: std.atomic.Mutex = .unlocked,
    admission: ?*std.atomic.Mutex = null,
    const Entry = struct { arena: std.heap.ArenaAllocator, datum: Datum, next: ?*Entry };
    fn deinit(self: *JsonCache) void {
        while (self.entries) |entry| {
            self.entries = entry.next;
            entry.arena.deinit();
            self.budget.allocator().destroy(entry);
        }
        self.text_arena.deinit();
    }
    fn lock(self: *JsonCache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn materialize(self: *JsonCache, payload: *JsonPayload, a: A) !Datum {
        self.lock();
        const entry = blk: {
            defer self.mutex.unlock();
            if (!payload.attempted) {
                if (self.admission) |gate| while (!gate.tryLock()) std.atomic.spinLoopHint();
                defer if (self.admission) |gate| gate.unlock();
                payload.cached = try self.cache(payload.*);
                payload.attempted = true;
            }
            break :blk payload.cached;
        };
        if (entry) |cached| return cached.datum;
        return payload.decode(a);
    }
    fn encoded(self: *JsonCache, payload: *JsonPayload) !?[]const u8 {
        self.lock();
        defer self.mutex.unlock();
        if (payload.text) |text| return text;
        const frozen = payload.frozen orelse return null;
        if (!frozen.lossless_text) return null;
        payload.text = try @import("spill.zig").frozenJsonText(self.text_arena.allocator(), frozen);
        return payload.text;
    }
    fn cache(self: *JsonCache, payload: JsonPayload) !?*Entry {
        // Avoid speculative decoding for a value that cannot plausibly fit.
        const bytes = if (payload.frozen) |v| v.bytes else payload.text.?;
        if (bytes.len > self.budget.limit -| self.budget.live) return null;
        // A cache is optional within the operator's admission too. Bound its
        // speculative allocations by remaining known parent capacity so an
        // operator quota miss falls back to the consumer's scratch region.
        const ceiling = self.budget.limit;
        if (@import("memory_budget.zig").headroom(self.budget.backing)) |available|
            self.budget.limit = @min(ceiling, self.budget.live +| available);
        defer self.budget.limit = ceiling;
        self.budget.exhausted = false;
        const entry = self.budget.allocator().create(Entry) catch |err| {
            if (self.budget.exhausted) return null;
            return err;
        };
        entry.* = .{ .arena = .init(self.budget.allocator()), .datum = .{}, .next = null };
        var keep = false;
        defer if (!keep) {
            entry.arena.deinit();
            self.budget.allocator().destroy(entry);
        };
        entry.datum = payload.decode(entry.arena.allocator()) catch |err| {
            if (err == error.OutOfMemory and self.budget.exhausted) return null;
            return err;
        };
        entry.next = self.entries;
        self.entries = entry;
        keep = true;
        return entry;
    }
};
const JsonPayload = struct {
    text: ?[]const u8 = null,
    frozen: ?@import("execution_batch.zig").FrozenJson = null,
    cached: ?*JsonCache.Entry = null,
    attempted: bool = false,
    shared: ?*Store.SharedJson = null,
    fn decode(self: JsonPayload, a: A) !Datum {
        if (self.frozen) |value| return @import("spill.zig").materializeFrozenJson(a, value);
        return Datum.json(try std.json.parseFromSliceLeaky(std.json.Value, a, self.text.?, .{ .parse_numbers = false }));
    }
};
const Column = struct {
    const Values = union(enum) {
        unknown,
        integers: Numeric(i64),
        numbers: Numeric(f64),
        booleans: std.ArrayList(bool),
        strings: Dictionary,
        decimals: Dictionary,
        encoded: std.ArrayList(std.json.Value),
    };
    values: Values = .unknown,
    json_bytes: ?*std.ArrayList(?*JsonPayload) = null,
    json_cache: ?*JsonCache = null,
    nulls: std.ArrayList(u64) = .empty,
    patterns: std.ArrayList(?*scalar.PatternSet) = .empty,
    arrays: std.ArrayList(?*const @import("array_value.zig").Value) = .empty,
    numerics: std.ArrayList(?*const @import("numeric_value.zig").Value) = .empty,
    fn deinit(self: *Column, a: A) void {
        if (self.json_bytes) |bytes| {
            bytes.deinit(a);
            a.destroy(bytes);
        }

        switch (self.values) {
            .unknown => {},
            inline else => |*values| values.deinit(a),
        }
        self.nulls.deinit(a);
        self.patterns.deinit(a);
        self.arrays.deinit(a);
        self.numerics.deinit(a);
    }
    fn isNull(self: Column, row: usize) bool {
        return self.nulls.items[row / 64] & (@as(u64, 1) << @as(u6, @intCast(row % 64))) != 0;
    }
    fn cell(self: Column, a: A, row: usize) !Datum {
        if (self.isNull(row)) return .{};
        if (self.json_bytes) |bytes| if (bytes.items[row]) |json| {
            return (if (json.shared) |shared| &shared.cache else self.json_cache.?).materialize(json, a);
        };
        const value: std.json.Value = switch (self.values) {
            .unknown => return error.InvalidSqlBackendResponse,
            .integers => |v| .{ .integer = v.get(row) },
            .numbers => |v| .{ .float = v.get(row) },
            .booleans => |v| .{ .bool = v.items[row] },
            .strings => |v| .{ .string = v.getText(row) },
            .decimals => |v| .{ .number_string = v.getText(row) },
            .encoded => |v| v.items[row],
        };
        return .{ .value = value, .sql_null = false, .patterns = if (self.patterns.items.len == 0) null else self.patterns.items[row], .array = if (self.arrays.items.len == 0) null else self.arrays.items[row], .numeric = if (self.numerics.items.len == 0) null else self.numerics.items[row] };
    }
    fn appendDictionary(self: *Column, a: A, owned: A, begin: usize, batch: @import("execution_batch.zig").Batch) !bool {
        if (self.json_bytes != null or batch != .dictionary or self.patterns.items.len != 0 or self.arrays.items.len != 0 or self.numerics.items.len != 0) return false;
        const source = batch.dictionary;
        for (source.indices) |id| if (id >= source.values.len) return error.InvalidSqlBackendResponse;
        const remap = try a.alloc(u32, source.values.len);
        defer a.free(remap);
        @memset(remap, std.math.maxInt(u32));
        for (source.indices) |id| {
            remap[id] = 0;
        }
        var tag: ?std.meta.Tag(std.json.Value) = null;
        var referenced: usize = 0;
        for (source.values, remap) |value, id| {
            if (id == std.math.maxInt(u32)) continue;
            if (value.patterns != null or value.array != null or value.numeric != null) return false;
            if (value.sql_null) continue;
            const actual = std.meta.activeTag(value.value);
            if (actual != .integer and actual != .float and actual != .string and actual != .number_string) return false;
            if (tag != null and tag.? != actual) return false;
            tag = actual;
            referenced += 1;
        }
        if (tag == null) return false;
        // Preserve encoding only when it saves numeric storage. Parquet may
        // dictionary-encode unique IDs; importing that representation must not
        // override the retained store's flat/high-cardinality policy.
        const flat_numeric = switch (self.values) {
            .unknown => tag.? == .integer or tag.? == .float,
            inline .integers, .numbers => |v| !v.encoded,
            else => false,
        };
        if (flat_numeric and referenced > source.indices.len / 2) return false;
        if (self.values != .unknown) {
            const compatible = switch (self.values) {
                .integers => |v| tag.? == .integer and (v.encoded or !v.sampled),
                .numbers => |v| tag.? == .float and (v.encoded or !v.sampled),
                .strings => |v| tag.? == .string and v.flat == null,
                .decimals => |v| tag.? == .number_string and v.flat == null,
                else => false,
            };
            if (!compatible) return false;
        } else {
            self.values = switch (tag.?) {
                .integer => .{ .integers = .empty },
                .float => .{ .numbers = .empty },
                .string => .{ .strings = .empty },
                .number_string => .{ .decimals = .empty },
                else => unreachable,
            };
            switch (self.values) {
                inline .integers, .numbers => |*v| try v.resize(a, begin),
                .strings, .decimals => |*v| try v.resizeNulls(a, begin),
                else => unreachable,
            }
        }
        switch (self.values) {
            inline .integers, .numbers => |*v| {
                if (!v.encoded) {
                    for (v.flat.items) |value| try v.encodeOne(a, value);
                    v.flat.clearAndFree(a);
                    v.encoded = true;
                    v.sampled = true;
                }
                for (source.values, remap) |value, *id| {
                    if (id.* == std.math.maxInt(u32)) continue;
                    id.* = 0;
                    if (value.sql_null) continue;
                    const number = if (@TypeOf(v.*) == Numeric(i64)) value.value.integer else value.value.float;
                    const entry = try v.lookup.getOrPut(a, @as(u64, @bitCast(number)));
                    if (!entry.found_existing) {
                        entry.value_ptr.* = std.math.cast(u32, v.values.items.len) orelse return error.SqlProgramLimitExceeded;
                        try v.values.append(a, number);
                    }
                    id.* = entry.value_ptr.*;
                }
                try v.indices.ensureUnusedCapacity(a, source.indices.len);
                for (source.indices) |id| v.indices.appendAssumeCapacity(remap[id]);
                if (v.values.items.len >= 512 and v.values.items.len > v.indices.items.len / 2) {
                    var flat: @TypeOf(v.flat) = .empty;
                    errdefer flat.deinit(a);
                    try flat.ensureTotalCapacity(a, v.indices.items.len);
                    for (v.indices.items) |id| flat.appendAssumeCapacity(v.values.items[id]);
                    v.values.clearAndFree(a);
                    v.indices.clearAndFree(a);
                    v.lookup.clearAndFree(a);
                    v.flat = flat;
                    v.encoded = false;
                }
            },
            .strings, .decimals => |*v| {
                for (source.values, remap) |value, *id| {
                    if (id.* == std.math.maxInt(u32)) continue;
                    id.* = 0;
                    if (value.sql_null) continue;
                    const text = if (value.value == .string) value.value.string else value.value.number_string;
                    if (v.lookup.get(text)) |prior| {
                        id.* = prior;
                        continue;
                    }
                    const bytes = try owned.dupe(u8, text);
                    id.* = std.math.cast(u32, v.values.items.len) orelse return error.SqlProgramLimitExceeded;
                    try v.values.append(a, bytes);
                    try v.lookup.put(a, bytes, id.*);
                }
                try v.indices.ensureUnusedCapacity(a, source.indices.len);
                for (source.indices) |id| v.indices.appendAssumeCapacity(remap[id]);
                if (v.values.items.len >= 512 and v.values.items.len > v.indices.items.len / 2) {
                    var flat: std.ArrayList([]const u8) = .empty;
                    errdefer flat.deinit(a);
                    try flat.ensureTotalCapacity(a, v.indices.items.len);
                    for (v.indices.items) |id| flat.appendAssumeCapacity(if (v.values.items.len == 0) &.{} else v.values.items[id]);
                    v.values.clearAndFree(a);
                    v.indices.clearAndFree(a);
                    v.lookup.clearAndFree(a);
                    v.flat = flat;
                }
            },
            else => unreachable,
        }
        for (source.indices, 0..) |id, offset| {
            const row = begin + offset;
            if (row % 64 == 0) try self.nulls.append(a, 0);
            if (source.values[id].sql_null) self.nulls.items[row / 64] |= @as(u64, 1) << @as(u6, @intCast(row % 64));
        }
        return true;
    }
    fn appendJson(self: *Column, a: A, owned: A, scratch: A, row: usize, payload: *JsonPayload) !void {
        if (self.json_bytes == null) {
            const values = try a.create(std.ArrayList(?*JsonPayload));
            values.* = .empty;
            self.json_bytes = values;
            try values.resize(a, row);
            @memset(values.items, null);
        }
        try self.append(a, owned, scratch, row, Datum.json(.null));
        self.json_bytes.?.items[row] = payload;
    }
    fn append(self: *Column, a: A, owned: A, scratch: A, row: usize, value: Datum) !void {
        if (self.json_bytes) |bytes| try bytes.append(a, null);
        if (self.numerics.items.len != 0 or value.numeric != null) {
            if (self.numerics.items.len == 0) {
                try self.numerics.resize(a, row);
                @memset(self.numerics.items, null);
            }
            try self.numerics.append(a, if (value.numeric != null) (try @import("operators.zig").cloneDatum(owned, value)).numeric else null);
        }
        if (self.arrays.items.len != 0 or value.array != null) {
            if (self.arrays.items.len == 0) {
                try self.arrays.resize(a, row);
                @memset(self.arrays.items, null);
            }
            try self.arrays.append(a, if (value.array != null) (try @import("operators.zig").cloneDatum(owned, value)).array else null);
        }
        if (row % 64 == 0) try self.nulls.append(a, 0);
        if (value.sql_null) self.nulls.items[row / 64] |= @as(u64, 1) << @as(u6, @intCast(row % 64));
        if (self.patterns.items.len != 0 or value.patterns != null) {
            if (self.patterns.items.len == 0) {
                try self.patterns.resize(a, row);
                @memset(self.patterns.items, null);
            }
            try self.patterns.append(a, value.patterns);
        }
        if (self.values == .unknown and !value.sql_null) {
            self.values = switch (value.value) {
                .integer => .{ .integers = .empty },
                .float => .{ .numbers = .empty },
                .bool => .{ .booleans = .empty },
                .string => .{ .strings = .empty },
                .number_string => .{ .decimals = .empty },
                else => .{ .encoded = .empty },
            };
            switch (self.values) {
                .unknown => unreachable,
                .strings, .decimals => |*values| try values.resizeNulls(a, row),
                inline .integers, .numbers => |*values| try values.resize(a, row),
                inline else => |*values| {
                    try values.resize(a, row);
                    @memset(values.items, switch (@typeInfo(@TypeOf(values.items)).pointer.child) {
                        i64, f64 => 0,
                        bool => false,
                        std.json.Value => .null,
                        else => &.{},
                    });
                },
            }
        }
        const compatible = value.sql_null or switch (self.values) {
            .unknown => true,
            .integers => value.value == .integer,
            .numbers => value.value == .float,
            .booleans => value.value == .bool,
            .strings => value.value == .string,
            .decimals => value.value == .number_string,
            .encoded => true,
        };
        if (!compatible) {
            var encoded: std.ArrayList(std.json.Value) = .empty;
            errdefer encoded.deinit(a);
            try encoded.ensureTotalCapacity(a, row + 1);
            for (0..row) |i| {
                const prior = try self.cell(scratch, i);
                encoded.appendAssumeCapacity(if (prior.sql_null) .null else prior.value);
            }
            switch (self.values) {
                .unknown => unreachable,
                inline else => |*values| values.deinit(a),
            }
            self.values = .{ .encoded = encoded };
        }
        switch (self.values) {
            .unknown => {},
            .integers => |*v| try v.append(a, if (value.sql_null) 0 else value.value.integer),
            .numbers => |*v| try v.append(a, if (value.sql_null) 0 else value.value.float),
            .booleans => |*v| try v.append(a, !value.sql_null and value.value.bool),
            .strings => |*v| try v.append(a, owned, if (value.sql_null) null else value.value.string),
            .decimals => |*v| try v.append(a, owned, if (value.sql_null) null else value.value.number_string),
            .encoded => |*v| try v.append(a, if (value.sql_null or value.array != null or value.numeric != null) .null else (try @import("operators.zig").cloneDatum(owned, value)).value),
        }
    }
};
pub const Store = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    columns: []Column = &.{},
    len: usize = 0,
    initialized: bool = false,
    failed: bool = false,
    json_cache: ?*JsonCache = null,
    json_pool: ?*JsonPool = null,
    // Content keys borrow only this store's owned immutable payloads. Small
    // values avoid dictionary metadata; large fan-out rows share bytes/cache.
    json_payloads: std.HashMapUnmanaged(JsonKey, *JsonPayload, JsonKeyContext, 80) = .empty,
    const JsonKey = struct { representation: u8, bytes: []const u8, hash: u64 };
    /// Stable, reference-counted bodies shared across independently retired
    /// stores. Keys own their bytes; the last store releases the body and its
    /// lazy cache immediately. One cache budget bounds all pooled DOMs.
    /// The retaining operator sequences ownership mutations; parallel readers
    /// use the synchronized lazy caches without changing ownership counts.
    pub const JsonPool = struct {
        budget: @import("memory_budget.zig"),
        cache_budget: @import("memory_budget.zig"),
        bodies: std.HashMapUnmanaged(JsonKey, *SharedJson, JsonKeyContext, 80) = .empty,
        cache_mutex: std.atomic.Mutex = .unlocked,
        fn growthBytes(self: *const JsonPool, additional: usize) usize {
            if (additional <= self.bodies.available) return 0;
            const entries = self.bodies.count() +| additional;
            const required = entries *| 100 / 80 +| 1;
            const capacity = std.math.ceilPowerOfTwo(usize, @max(8, required)) catch return std.math.maxInt(usize);
            return capacity *| (@sizeOf(u8) + @sizeOf(JsonKey) + @sizeOf(*SharedJson)) +| 64;
        }
        pub fn create(a: A) !*JsonPool {
            const self = try a.create(JsonPool);
            self.* = .{ .budget = .{ .backing = a, .limit = std.math.maxInt(usize) }, .cache_budget = .{ .backing = undefined, .limit = JsonCache.limit } };
            self.cache_budget.backing = self.budget.allocator();
            return self;
        }
        pub fn deinit(self: *JsonPool) void {
            std.debug.assert(self.bodies.count() == 0 and self.cache_budget.live == 0);
            const a = self.budget.backing;
            self.bodies.deinit(self.budget.allocator());
            std.debug.assert(self.budget.live == 0);
            a.destroy(self);
        }
        fn acquire(self: *JsonPool, key: JsonKey, payload: JsonPayload) !*JsonPayload {
            if (self.bodies.get(key)) |owner| {
                owner.references += 1;
                return &owner.payload;
            }
            const a = self.budget.allocator();
            const owner = try a.create(SharedJson);
            errdefer a.destroy(owner);
            const bytes = try a.dupe(u8, key.bytes);
            errdefer a.free(bytes);
            var owned_key = key;
            owned_key.bytes = bytes;
            owner.* = .{ .pool = self, .key = owned_key, .payload = payload, .cache = .{ .budget = .{ .backing = self.cache_budget.allocator(), .limit = JsonCache.limit }, .text_arena = .init(a), .admission = &self.cache_mutex } };
            owner.payload.shared = owner;
            if (payload.frozen != null) owner.payload.frozen.?.bytes = bytes else owner.payload.text = bytes;
            try self.bodies.put(a, owned_key, owner);
            return &owner.payload;
        }
        fn release(self: *JsonPool, payload: *JsonPayload) void {
            const owner = payload.shared.?;
            std.debug.assert(owner.pool == self and owner.references != 0);
            owner.references -= 1;
            if (owner.references != 0) return;
            std.debug.assert(self.bodies.remove(owner.key));
            owner.cache.deinit();
            const a = self.budget.allocator();
            a.free(owner.key.bytes);
            a.destroy(owner);
        }
    };
    const SharedJson = struct {
        pool: *JsonPool,
        key: JsonKey,
        payload: JsonPayload,
        cache: JsonCache,
        references: usize = 1,
    };
    /// A memo belongs to one destination store and immutable producer lease,
    /// and is discarded before another pull. Admission and retention may share it; borrowed addresses
    /// must never become persistent keys in the destination store.
    pub const JsonLease = struct {
        a: A,
        identities: std.AutoHashMapUnmanaged(Identity, Resolution) = .empty,
        // Local counters make hash work measurable without global test hooks.
        hashed_bytes: usize = 0,
        generation: usize = 0,
        const Identity = struct { address: usize, len: usize, representation: u8 };
        const Resolution = struct { key: JsonKey, owned: ?*JsonPayload, estimated: usize = 0 };
        pub fn init(a: A) JsonLease {
            return .{ .a = @import("memory_budget.zig").reclaimingAllocator(a) };
        }
        pub fn deinit(self: *JsonLease) void {
            self.identities.deinit(self.a);
        }
        /// A flush retires destination ownership, but producer identities and
        /// their hashes remain valid until the current immutable batch ends.
        pub fn forgetOwned(self: *JsonLease) void {
            var entries = self.identities.valueIterator();
            while (entries.next()) |entry| entry.owned = null;
        }
        fn resolve(self: *JsonLease, store: *const Store, payload: JsonPayload) !*Resolution {
            const bytes = if (payload.frozen) |value| value.bytes else payload.text.?;
            const representation: u8 = if (payload.frozen) |value| (if (value.lossless_text) @as(u8, 3) else 2) else 1;
            const slot = try self.identities.getOrPut(self.a, .{ .address = @intFromPtr(bytes.ptr), .len = bytes.len, .representation = representation });
            if (!slot.found_existing) {
                const key: JsonKey = .{ .representation = representation, .bytes = bytes, .hash = std.hash.Wyhash.hash(representation, bytes) };
                self.hashed_bytes +|= bytes.len;
                slot.value_ptr.* = .{ .key = key, .owned = store.json_payloads.get(key) };
            }
            return slot.value_ptr;
        }
    };
    const JsonKeyContext = struct {
        pub fn hash(_: @This(), key: JsonKey) u64 {
            return key.hash;
        }
        pub fn eql(_: @This(), left: JsonKey, right: JsonKey) bool {
            return left.representation == right.representation and std.mem.eql(u8, left.bytes, right.bytes);
        }
    };
    fn ownJson(self: *Store, payload: JsonPayload, lease: *JsonLease) !*JsonPayload {
        const source = if (payload.frozen) |value| value.bytes else payload.text.?;
        const resolved = if (source.len >= 256) try lease.resolve(self, payload) else null;
        if (resolved) |entry| {
            if (entry.owned) |existing| return existing;
            // A different source address with equal content may have been
            // retained since admission. Cached hashes keep this lookup cheap.
            if (self.json_payloads.get(entry.key)) |existing| {
                entry.owned = existing;
                return existing;
            }
        }
        if (resolved) |entry| if (self.json_pool) |pool| {
            const result = try pool.acquire(entry.key, payload);
            errdefer pool.release(result);
            try self.json_payloads.put(self.a, result.shared.?.key, result);
            entry.owned = result;
            return result;
        };
        const owned = self.arena.allocator();
        const result = try owned.create(JsonPayload);
        result.* = payload;
        const bytes = try owned.dupe(u8, source);
        if (payload.frozen != null) result.frozen.?.bytes = bytes else result.text = bytes;
        if (resolved) |entry| {
            var key = entry.key;
            key.bytes = bytes;
            try self.json_payloads.put(self.a, key, result);
            entry.owned = result;
        }
        return result;
    }
    /// Bytes that retire when this store drops the final reference to a
    /// pooled body. Hash-table capacity stays with the pool and is excluded.
    pub fn exclusiveJsonBytes(self: *const Store) usize {
        var bytes: usize = 0;
        var payloads = self.json_payloads.valueIterator();
        while (payloads.next()) |payload| if (payload.*.shared) |shared| if (shared.references == 1) {
            bytes +|= @sizeOf(SharedJson) +| shared.key.bytes.len +| shared.cache.budget.live;
            bytes +|= shared.cache.text_arena.queryCapacity();
            inline for (.{ shared.cache.text_arena.state.used_list, shared.cache.text_arena.state.free_list }) |head| {
                var node = head;
                while (node) |value| : (node = value.next) bytes +|= @sizeOf(@TypeOf(value.*));
            }
        };
        return bytes;
    }
    pub fn init(a: A) Store {
        return .{ .a = a, .arena = .init(a) };
    }
    pub fn deinit(self: *Store) void {
        for (self.columns) |*column| column.deinit(self.a);
        self.a.free(self.columns);
        if (self.json_pool) |pool| {
            var payloads = self.json_payloads.valueIterator();
            while (payloads.next()) |payload| pool.release(payload.*);
        }
        self.json_payloads.deinit(self.a);
        self.arena.deinit();
        if (self.json_cache) |cache| {
            cache.deinit();
            self.a.destroy(cache);
        }
    }
    fn jsonCache(self: *Store) !*JsonCache {
        if (self.json_cache == null) {
            const cache = try self.a.create(JsonCache);
            cache.* = .{ .budget = .{ .backing = @import("memory_budget.zig").reclaimingAllocator(self.a), .limit = JsonCache.limit }, .text_arena = .init(self.a) };
            self.json_cache = cache;
        }
        return self.json_cache.?;
    }
    /// Conservative admission estimate that recognizes already retained
    /// dictionary bytes. Capacity growth is still enforced by the allocator.
    /// Representation identity for expression reuse, including a distinct NULL
    /// lane. No semantic comparison or coercion is implied by dictionary IDs.
    pub fn dictionaryId(self: *const Store, row_index: usize, column: usize) !?u64 {
        if (row_index >= self.len or column >= self.columns.len) return error.InvalidSqlBackendResponse;
        const stored = self.columns[column];
        if (stored.patterns.items.len != 0 or stored.arrays.items.len != 0 or stored.numerics.items.len != 0) return null;
        const id: u32 = switch (stored.values) {
            inline .integers, .numbers => |v| if (v.encoded) v.indices.items[row_index] else return null,
            .strings, .decimals => |v| if (v.flat == null) v.indices.items[row_index] else return null,
            else => return null,
        };
        return if (stored.isNull(row_index)) 0 else @as(u64, id) + 1;
    }
    pub fn dictionaryBatch(self: *const Store, a: A, column: usize, begin: usize, count: usize) !?@import("execution_batch.zig").Batch {
        if (self.failed or column >= self.columns.len or begin > self.len or count > self.len - begin) return error.InvalidSqlBackendResponse;
        const stored = self.columns[column];
        if (stored.patterns.items.len != 0 or stored.arrays.items.len != 0 or stored.numerics.items.len != 0) return null;
        switch (stored.values) {
            inline .integers, .numbers => |v| if (!v.encoded) return null,
            .strings, .decimals => |v| if (v.flat != null) return null,
            else => return null,
        }
        // Export only referenced entries. A small delivery slice must not
        // retain or reconstruct a relation's entire dictionary.
        const values = try a.alloc(Datum, count + 1);
        errdefer a.free(values);
        values[0] = .{};
        const indices = try a.alloc(u32, count);
        errdefer a.free(indices);
        var ids: std.AutoHashMapUnmanaged(u64, u32) = .empty;
        defer ids.deinit(a);
        var used: usize = 1;
        for (indices, 0..) |*id, offset| {
            const physical = (try self.dictionaryId(begin + offset, column)).?;
            if (physical == 0) {
                id.* = 0;
                continue;
            }
            const entry = try ids.getOrPut(a, physical);
            if (!entry.found_existing) {
                entry.value_ptr.* = @intCast(used);
                values[used] = try stored.cell(a, begin + offset);
                used += 1;
            }
            id.* = entry.value_ptr.*;
        }
        return .{ .dictionary = .{ .values = try a.realloc(values, used), .indices = indices } };
    }
    pub fn appendBytes(self: *const Store, values: []const Datum) !usize {
        var bytes: usize = if (self.initialized) 0 else columnMetadataBytes(values.len);
        for (values, 0..) |value, column| bytes +|= try self.cellAppendBytes(column, value);
        return bytes;
    }
    fn cellAppendBytes(self: *const Store, index: usize, value: Datum) !usize {
        var bytes: usize = 0;
        var repeated = false;
        if (!value.sql_null and index < self.columns.len) {
            const column = self.columns[index];
            if (column.values == .strings and value.value == .string) repeated = column.values.strings.flat == null and column.values.strings.lookup.contains(value.value.string);
            if (column.values == .decimals and value.value == .number_string) repeated = column.values.decimals.flat == null and column.values.decimals.lookup.contains(value.value.number_string);
            if (!repeated) {
                if (column.values == .strings and value.value == .string) bytes +|= column.values.strings.growthBytes(1);
                if (column.values == .decimals and value.value == .number_string) bytes +|= column.values.decimals.growthBytes(1);
            }
        }
        bytes +|= if (repeated) @sizeOf(u32) + 1 else try retainedCellBytes(value);
        if (repeated and value.patterns != null) bytes +|= @sizeOf(?*scalar.PatternSet);
        return bytes;
    }
    /// Estimate only new native JSON bodies, including duplicates within this
    /// batch. Keys borrow the producer's immutable lease for this call only.
    /// Per-cell descriptors and allocator growth remain separate reservations.
    pub fn batchAppendBytes(self: *const Store, a: A, batch: @import("execution_batch.zig").Batch, maximum: usize) !usize {
        var lease = JsonLease.init(a);
        defer lease.deinit();
        return self.batchAppendBytesLeased(a, batch, maximum, &lease);
    }
    /// Resolutions survive retries within one producer lease. Each estimate
    /// gets a separate counting generation so declined prefixes cannot hide
    /// payload bytes in a later attempt.
    pub fn batchAppendBytesLeased(self: *const Store, a: A, batch: @import("execution_batch.zig").Batch, maximum: usize, lease: *JsonLease) !usize {
        var region = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(a));
        defer region.deinit();
        const work = region.allocator();
        var incoming: std.HashMapUnmanaged(JsonKey, void, JsonKeyContext, 80) = .empty;
        var cell_region = std.heap.ArenaAllocator.init(work);
        defer cell_region.deinit();
        lease.generation +%= 1;
        if (lease.generation == 0) {
            var entries = lease.identities.valueIterator();
            while (entries.next()) |entry| entry.estimated = 0;
            lease.generation = 1;
        }
        var bytes: usize = 0;
        for (0..batch.width()) |column| for (0..batch.len()) |index| {
            const payload: ?JsonPayload = if (try batch.frozenJson(index, column)) |frozen|
                .{ .frozen = frozen }
            else if (try batch.encodedJson(index, column)) |text|
                .{ .text = text }
            else
                null;
            if (payload) |native| {
                bytes +|= @sizeOf(?*JsonPayload) + 1;
                if (bytes > maximum) return bytes;
                const source = if (native.frozen) |frozen| frozen.bytes else native.text.?;
                if (source.len >= 256) {
                    const resolved = try lease.resolve(self, native);
                    if (resolved.estimated == lease.generation) continue;
                    resolved.estimated = lease.generation;
                    if (resolved.owned == null) resolved.owned = self.json_payloads.get(resolved.key);
                    if (resolved.owned != null) continue;
                    if (self.json_pool) |pool| if (pool.bodies.contains(resolved.key)) continue;
                    const prior_growth = if (self.json_pool) |pool| pool.growthBytes(incoming.count()) else @as(usize, 0);
                    const entry = try incoming.getOrPut(work, resolved.key);
                    if (entry.found_existing) continue;
                    if (self.json_pool) |pool| bytes +|= pool.growthBytes(incoming.count()) -| prior_growth;
                }
                bytes +|= source.len +| @sizeOf(JsonPayload) +| 64;
            } else {
                _ = cell_region.reset(.retain_capacity);
                bytes +|= try self.cellAppendBytes(column, try batch.cell(cell_region.allocator(), index, column));
            }
            // Reject before temporary identity state can outgrow admission.
            if (bytes > maximum) return bytes;
        };
        return bytes;
    }

    /// Reserve dictionary replacement capacity before admitting a batch. Treat
    /// its incoming keys as distinct until their cardinality is established.
    pub fn dictionaryGrowthBytes(self: *const Store, rows: usize) usize {
        var bytes: usize = 0;
        for (self.columns) |column| switch (column.values) {
            .strings, .decimals => |dictionary| bytes +|= dictionary.growthBytes(rows),
            else => {},
        };
        return bytes;
    }
    /// Column-major payload admission. No per-row Datum slices or arenas;
    /// primitive and dictionary state is retained directly in each column.
    pub fn appendBatch(self: *Store, batch: @import("execution_batch.zig").Batch) !void {
        var lease = JsonLease.init(self.a);
        defer lease.deinit();
        return self.appendBatchLeased(batch, &lease);
    }
    pub fn appendBatchLeased(self: *Store, batch: @import("execution_batch.zig").Batch, lease: *JsonLease) !void {
        if (self.failed or (self.initialized and self.columns.len != batch.width())) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (!self.initialized) {
            self.columns = try self.a.alloc(Column, batch.width());
            @memset(self.columns, .{});
            self.initialized = true;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (self.columns, 0..) |*column, ordinal| {
            _ = scratch.reset(.free_all);
            if (try batch.dictionaryColumn(scratch.allocator(), ordinal)) |encoded| {
                if (encoded.len() != batch.len()) return error.InvalidSqlBackendResponse;
                if (try column.appendDictionary(self.a, self.arena.allocator(), self.len, encoded)) continue;
            }
            _ = scratch.reset(.free_all);
            for (0..batch.len()) |index| {
                _ = scratch.reset(.retain_capacity);
                if (try batch.frozenJson(index, ordinal)) |frozen| {
                    column.json_cache = try self.jsonCache();
                    try column.appendJson(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, try self.ownJson(.{ .frozen = frozen }, lease));
                    continue;
                }
                if (try batch.encodedJson(index, ordinal)) |bytes| {
                    column.json_cache = try self.jsonCache();
                    try column.appendJson(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, try self.ownJson(.{ .text = bytes }, lease));
                    continue;
                }
                const value = try batch.cell(scratch.allocator(), index, ordinal);
                try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, value);
            }
        }
        self.len += batch.len();
    }
    pub fn append(self: *Store, values: []const Datum) !usize {
        if (self.failed or (self.initialized and values.len != self.columns.len)) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (!self.initialized) {
            self.columns = try self.a.alloc(Column, values.len);
            @memset(self.columns, .{});
            self.initialized = true;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (self.columns, values) |*column, value| try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len, value);
        const index = self.len;
        self.len += 1;
        return index;
    }
    pub fn appendRagged(self: *Store, values: []const Datum) !usize {
        if (!self.initialized or values.len == self.columns.len) return self.append(values);
        errdefer self.failed = true;
        if (values.len > self.columns.len) {
            const wider = try self.a.alloc(Column, values.len);
            @memcpy(wider[0..self.columns.len], self.columns);
            @memset(wider[self.columns.len..], .{});
            const prior_width = self.columns.len;
            self.a.free(self.columns);
            self.columns = wider;
            for (self.columns[prior_width..]) |*column| for (0..self.len) |index| try column.append(self.a, self.arena.allocator(), self.a, index, .{});
        }
        const padded = try self.a.alloc(Datum, self.columns.len);
        defer self.a.free(padded);
        @memset(padded, .{});
        @memcpy(padded[0..values.len], values);
        return self.append(padded);
    }
    pub fn rowWidth(self: *const Store, a: A, index: usize, width: usize) ![]const Datum {
        if (self.failed or index >= self.len or width > self.columns.len) return error.InvalidSqlBackendResponse;
        const result = try a.alloc(Datum, width);
        errdefer a.free(result);
        for (result, 0..) |*value, column| value.* = try self.cell(a, index, column);
        return result;
    }
    pub fn encodedJson(self: *const Store, index: usize, column: usize) !?[]const u8 {
        if (self.failed or index >= self.len or column >= self.columns.len) return error.InvalidSqlBackendResponse;
        const values = self.columns[column].json_bytes orelse return null;
        const payload = values.items[index] orelse return null;
        return (if (payload.shared) |shared| &shared.cache else self.json_cache.?).encoded(payload);
    }
    pub fn frozenJson(self: *const Store, index: usize, column: usize) !?@import("execution_batch.zig").FrozenJson {
        if (self.failed or index >= self.len or column >= self.columns.len) return error.InvalidSqlBackendResponse;
        const values = self.columns[column].json_bytes orelse return null;
        const payload = values.items[index] orelse return null;
        return payload.frozen;
    }
    pub fn cellBytes(self: *const Store, a: A, index: usize, column: usize) !usize {
        if (try self.frozenJson(index, column)) |value| return value.bytes.len;
        if (try self.encodedJson(index, column)) |bytes| return bytes.len;
        return @import("operators.zig").datumBytes(try self.cell(a, index, column));
    }
    pub fn cell(self: *const Store, a: A, index: usize, column: usize) !Datum {
        if (self.failed or index >= self.len or column >= self.columns.len) return error.InvalidSqlBackendResponse;
        return self.columns[column].cell(a, index);
    }
    pub fn row(self: *const Store, a: A, index: usize) ![]const Datum {
        if (self.failed or index >= self.len) return error.InvalidSqlBackendResponse;
        const result = try a.alloc(Datum, self.columns.len);
        errdefer a.free(result);
        for (result, 0..) |*value, column| value.* = try self.cell(a, index, column);
        return result;
    }
    pub fn equal(self: *const Store, a: A, row_index: usize, values: []const Datum, null_equal: bool) !bool {
        if (self.failed or values.len != self.columns.len or row_index >= self.len) return error.InvalidSqlBackendResponse;
        for (values, 0..) |value, column| {
            const stored = self.columns[column];
            const is_null = stored.isNull(row_index);
            if (is_null or value.sql_null) {
                if (!null_equal or is_null != value.sql_null) return false;
                continue;
            }
            if (value.array != null or value.numeric != null or (stored.arrays.items.len != 0 and stored.arrays.items[row_index] != null) or (stored.numerics.items.len != 0 and stored.numerics.items[row_index] != null)) {
                if ((try scalar.compareDatums(try stored.cell(a, row_index), value)) != .eq) return false;
                continue;
            }
            // Dispatch on the retained physical type, without constructing a
            // Datum or invoking JSON comparison for homogeneous primitive keys.
            const equal_ = switch (stored.values) {
                .integers => |v| if (value.value == .integer) v.get(row_index) == value.value.integer else null,
                .numbers => |v| if (value.value == .float and std.math.isFinite(v.get(row_index)) and std.math.isFinite(value.value.float)) v.get(row_index) == value.value.float else null,
                .booleans => |v| if (value.value == .bool) v.items[row_index] == value.value.bool else null,
                .strings => |v| if (value.value == .string) std.mem.eql(u8, v.getText(row_index), value.value.string) else null,
                else => null,
            };
            if (equal_) |matches| {
                if (!matches) return false;
            } else if ((try scalar.compareDatums(try stored.cell(a, row_index), value)) != .eq) return false;
        }
        return true;
    }
};
test "SQL exact NUMERIC retained columns own limbs preserve scale and reject placeholder equality" {
    const Harness = struct {
        fn run(a: A) !void {
            const numeric = @import("numeric_value.zig");
            var context: numeric.Context = .{ .alloc = a };
            var original = try numeric.parse(&context, "9007199254740993.1200");
            defer original.deinit();
            var store = Store.init(a);
            defer store.deinit();
            _ = try store.append(&.{.{}});
            _ = try store.append(&.{Datum.typedNumeric(&original.value)});
            _ = try store.append(&.{Datum.json(.null)});
            _ = try store.append(&.{Datum.json(.{ .integer = 7 })});
            const retained = try store.cell(a, 1, 0);
            try std.testing.expect(retained.numeric != null and !retained.sql_null);
            try std.testing.expect(retained.numeric.?.digits.ptr != original.value.digits.ptr);
            try std.testing.expectEqual(@as(u16, 4), retained.numeric.?.scale);
            try std.testing.expect(try store.equal(a, 1, &.{Datum.typedNumeric(&original.value)}, true));
            try std.testing.expectError(error.SqlTypeMismatch, store.equal(a, 1, &.{Datum.json(.null)}, true));
            try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
            try std.testing.expect((try store.cell(a, 2, 0)).numeric == null);
            try std.testing.expectEqual(@as(i64, 7), (try store.cell(a, 3, 0)).value.integer);
            try std.testing.expect(try store.dictionaryId(1, 0) == null);
        }
    };
    try Harness.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL typed array retained columns preserve bounds ownership and type separation under allocation faults" {
    const Harness = struct {
        fn run(a: A) !void {
            const arrays = @import("array_value.zig");
            var text: [3]u8 = "abc".*;
            var dimensions = [_]arrays.Dimension{.{ .length = 3, .lower = -2 }};
            const value = try arrays.Value.init(.text, &dimensions, &.{ Datum.json(.{ .string = &text }), .{}, Datum.json(.{ .string = "tail" }) }, .{});
            var store = Store.init(a);
            defer store.deinit();
            _ = try store.append(&.{.{}});
            _ = try store.append(&.{Datum.typedArray(&value)});
            _ = try store.append(&.{Datum.json(.null)});
            _ = try store.append(&.{Datum.json(.{ .integer = 7 })});
            _ = try store.append(&.{Datum.typedArray(&value)});
            @memset(&text, 'z');
            dimensions[0].lower = 1;
            const cell = try store.cell(a, 1, 0);
            try std.testing.expect(!cell.sql_null and cell.array != null);
            try std.testing.expectEqual(@as(i32, -2), cell.array.?.dimensions[0].lower);
            try std.testing.expectEqualStrings("abc", cell.array.?.flatCell(0).value.string);
            try std.testing.expect(cell.array.?.flatCell(1).sql_null);
            try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
            const json_null = try store.cell(a, 2, 0);
            try std.testing.expect(!json_null.sql_null and json_null.array == null);
            try std.testing.expectEqual(@as(i64, 7), (try store.cell(a, 3, 0)).value.integer);
            try std.testing.expect(try store.equal(a, 4, &.{cell}, true));
            try std.testing.expectError(error.SqlTypeMismatch, store.equal(a, 1, &.{json_null}, true));
            const shifted = try arrays.Value.init(.text, &dimensions, cell.array.?.elements, .{});
            try std.testing.expect(!try store.equal(a, 1, &.{Datum.typedArray(&shifted)}, true));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL typed store preserves bitmaps ownership mixed exact numerics and JSON null" {
    const a = std.testing.allocator;
    var store = Store.init(a);
    defer store.deinit();
    for (0..130) |i| _ = try store.append(&.{ if (i % 7 == 0) .{} else Datum.json(.{ .integer = @intCast(i) }), Datum.json(.{ .string = "owned" }) });
    _ = try store.append(&.{ Datum.json(.{ .float = 1.5 }), Datum.json(.null) });
    _ = try store.append(&.{ Datum.json(.{ .integer = 9007199254740993 }), .{} });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (0..130) |i| {
        const value = try store.cell(arena.allocator(), i, 0);
        try std.testing.expectEqual(i % 7 == 0, value.sql_null);
        if (!value.sql_null) try std.testing.expectEqual(@as(i64, @intCast(i)), value.value.integer);
    }
    try std.testing.expect(!(try store.cell(arena.allocator(), 130, 1)).sql_null);
    try std.testing.expect((try store.cell(arena.allocator(), 131, 1)).sql_null);
    try std.testing.expect(try store.equal(arena.allocator(), 131, &.{ Datum.json(.{ .integer = 9007199254740993 }), .{} }, true));
}

test "SQL typed store promotions and ragged widths release every allocation failure" {
    const Harness = struct {
        fn run(a: A) !void {
            var store = Store.init(a);
            defer store.deinit();
            _ = try store.appendRagged(&.{ .{}, Datum.json(.{ .string = "owned" }) });
            _ = try store.appendRagged(&.{ Datum.json(.{ .integer = 9007199254740993 }), .{}, Datum.json(.{ .bool = true }) });
            _ = try store.appendRagged(&.{ Datum.json(.{ .float = 2.5 }), Datum.json(.null) });
            const cells = try store.rowWidth(a, 0, 2);
            defer a.free(cells);
            try std.testing.expect(cells[0].sql_null);
            try std.testing.expectEqualStrings("owned", cells[1].value.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
}

test "SQL typed dictionaries retain repeated payloads once and preserve promotion" {
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 256 * 1024 };
    const a = budget.allocator();
    {
        var store = Store.init(a);
        defer store.deinit();
        var payload: [1024]u8 = @splat('x');
        for (0..10_000) |index| _ = try store.append(&.{if (index % 7 == 0) Datum{} else Datum.json(.{ .string = &payload })});
        try std.testing.expectEqual(@as(usize, 1), store.columns[0].values.strings.values.items.len);
        payload[0] = 'y';
        try std.testing.expectEqual(@as(u8, 'x'), (try store.cell(a, 1, 0)).value.string[0]);
        try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
}

fn batchAllocationScenario(a: A) !void {
    var store = Store.init(a);
    defer store.deinit();
    const rows = [_][]const Datum{
        &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .string = "shared" }) },
        &.{ .{}, Datum.json(.{ .string = "shared" }) },
        &.{ Datum.json(.{ .float = 2.5 }), Datum.json(.{ .string = "unique" }) },
    };
    try store.appendBatch(.{ .rows = &rows });
    for (rows, 0..) |row, index| for (row, 0..) |expected, column| {
        const actual = try store.cell(a, index, column);
        try std.testing.expectEqualDeep(expected.value, actual.value);
        try std.testing.expectEqual(expected.sql_null, actual.sql_null);
    };
}
test "SQL retained column batch promotion unwinds every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, batchAllocationScenario, .{});
}

fn numericRetentionScenario(a: A) !void {
    var store = Store.init(a);
    defer store.deinit();
    for (0..1024) |row| _ = try store.append(&.{
        if (row % 7 == 0) Datum{} else Datum.json(.{ .integer = 9007199254740993 + @as(i64, @intCast(row % 3)) }),
        Datum.json(.{ .float = if (row % 2 == 0) -0.0 else 2.5 }),
    });
    try std.testing.expect(store.columns[0].values.integers.encoded);
    try std.testing.expect(store.columns[1].values.numbers.encoded);
    for (0..1024) |row| {
        const value = try store.cell(a, row, 0);
        try std.testing.expectEqual(row % 7 == 0, value.sql_null);
        if (!value.sql_null) try std.testing.expectEqual(9007199254740993 + @as(i64, @intCast(row % 3)), value.value.integer);
        if (row % 2 == 0) try std.testing.expect(std.math.signbit((try store.cell(a, row, 1)).value.float));
    }
    // A later high-cardinality suffix returns to flat storage safely.
    for (0..4096) |row| _ = try store.append(&.{ Datum.json(.{ .integer = @intCast(row) }), Datum.json(.{ .float = @floatFromInt(row) }) });
    try std.testing.expect(!store.columns[0].values.integers.encoded);
    try std.testing.expectEqual(@as(i64, 4095), (try store.cell(a, store.len - 1, 0)).value.integer);
}
test "SQL retained numeric dictionaries preserve exact values nulls and allocation failure ownership" {
    // Force allocate/copy growth so the exhaustive fault sequence does not
    // depend on whether the backing allocator happens to grow in place.
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(fixed.allocator(), numericRetentionScenario, .{});
}

fn dictionaryHandoffScenario(a: A) !void {
    const Batch = @import("execution_batch.zig").Batch;
    var store = Store.init(a);
    defer store.deinit();
    _ = try store.append(&.{.{}});
    const source: Batch = .{ .dictionary = .{ .values = &.{ .{}, Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .string = "unused incompatible entry" }), Datum.json(.{ .integer = -7 }) }, .indices = &.{ 1, 0, 3, 1, 3, 0 } } };
    try store.appendBatch(source);
    try std.testing.expect(store.columns[0].values.integers.encoded);
    for (0..source.len()) |index| try std.testing.expectEqualDeep(try source.cell(a, index, 0), try store.cell(a, index + 1, 0));
    const exported = (try store.dictionaryBatch(a, 0, 1, source.len())).?;
    defer a.free(exported.dictionary.values);
    defer a.free(exported.dictionary.indices);
    try std.testing.expectEqual(@as(usize, 3), exported.dictionary.values.len);
    var target = Store.init(a);
    defer target.deinit();
    try target.appendBatch(exported);
    for (0..source.len()) |index| try std.testing.expectEqualDeep(try source.cell(a, index, 0), try target.cell(a, index, 0));
    try std.testing.expect((try store.cell(a, 0, 0)).sql_null);
    var unique = Store.init(a);
    defer unique.deinit();
    var unique_values: [64]Datum = undefined;
    var unique_indices: [64]u32 = undefined;
    for (&unique_values, &unique_indices, 0..) |*value, *id, index| {
        value.* = Datum.json(.{ .integer = @intCast(index) });
        id.* = @intCast(index);
    }
    try unique.appendBatch(.{ .dictionary = .{ .values = &unique_values, .indices = &unique_indices } });
    try std.testing.expect(!unique.columns[0].values.integers.encoded);
    try std.testing.expectEqual(@as(i64, 63), (try unique.cell(a, 63, 0)).value.integer);
    try std.testing.expectError(error.InvalidSqlBackendResponse, target.appendBatch(.{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 1 })}, .indices = &.{9} } }));
}
test "SQL dictionary handoff remaps referenced entries preserves NULL prefixes and unwinds failures" {
    try dictionaryHandoffScenario(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, dictionaryHandoffScenario, .{});
}

test "SQL encoded JSON bounded cache reuses scalar trees and releases failed attempts" {
    const Fixture = struct {
        fn run(a: A) !void {
            const Probe = struct {
                fn json(_: *anyopaque, _: usize, _: usize) !?[]const u8 {
                    return "{\"x\":[1,2,3],\"s\":\"value\"}";
                }
                fn cell(_: *anyopaque, _: A, _: usize, _: usize) !Datum {
                    return error.UnexpectedScalarMaterialization;
                }
            };
            var raw: u8 = 0;
            var store = Store.init(a);
            defer store.deinit();
            try store.appendBatch(.{ .reader = .{ .ptr = &raw, .read = Probe.cell, .read_json = Probe.json, .count = 3, .width = 1 } });
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const first = try store.cell(scratch.allocator(), 0, 0);
            const live = store.json_cache.?.budget.live;
            _ = scratch.reset(.free_all);
            for (0..16) |_| {
                const again = try store.cell(scratch.allocator(), 0, 0);
                try std.testing.expect(first.value.object.values().ptr == again.value.object.values().ptr);
                try std.testing.expectEqual(live, store.json_cache.?.budget.live);
            }
            // Policy exhaustion is an optimization miss, while a genuine
            // backing allocator failure must still propagate to the caller.
            store.json_cache.?.budget.limit = live + @sizeOf(JsonCache.Entry) + 32;
            const second = try store.cell(scratch.allocator(), 1, 0);
            try std.testing.expectEqualStrings("1", second.value.object.get("x").?.array.items[0].number_string);
            try std.testing.expectEqual(live, store.json_cache.?.budget.live);
            try std.testing.expect(store.columns[0].json_bytes.?.items[1].?.cached == null);
            try std.testing.expect(store.columns[0].json_bytes.?.items[1].?.attempted);
            var parent: @import("memory_budget.zig") = .{ .backing = a, .limit = 0 };
            const original = store.json_cache.?.budget.backing;
            store.json_cache.?.budget.backing = parent.allocator();
            defer store.json_cache.?.budget.backing = original;
            store.json_cache.?.budget.limit = JsonCache.limit;
            _ = try store.cell(scratch.allocator(), 2, 0);
            try std.testing.expect(!parent.exhausted);
            try std.testing.expectEqual(@as(usize, 0), parent.live);
            try std.testing.expectEqual(live, store.json_cache.?.budget.live);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "native projection refinements benchmark repeated retained JSON scalar reads" {
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer source.deinit();
    var input: std.Io.Writer.Allocating = .init(source.allocator());
    try input.writer.writeByte('[');
    for (0..if (@import("builtin").mode == .debug) @as(usize, 512) else 1024) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"x\":1,\"s\":\"nested value\"}");
    }
    try input.writer.writeByte(']');
    const Probe = struct {
        bytes: []const u8,
        fn json(raw: *anyopaque, _: usize, _: usize) !?[]const u8 {
            return @as(*@This(), @ptrCast(@alignCast(raw))).bytes;
        }
        fn cell(_: *anyopaque, _: A, _: usize, _: usize) !Datum {
            return error.UnexpectedScalarMaterialization;
        }
    };
    var probe: Probe = .{ .bytes = input.written() };
    var store = Store.init(std.testing.allocator);
    defer store.deinit();
    try store.appendBatch(.{ .reader = .{ .ptr = &probe, .read = Probe.cell, .read_json = Probe.json, .count = 1, .width = 1 } });
    var baseline: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 128 << 20 };
    var prior = std.heap.ArenaAllocator.init(baseline.allocator());
    defer prior.deinit();
    for (0..16) |_| _ = try std.json.parseFromSliceLeaky(std.json.Value, prior.allocator(), probe.bytes, .{ .parse_numbers = false });
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 128 << 20 };
    var results = std.heap.ArenaAllocator.init(budget.allocator());
    defer results.deinit();
    for (0..16) |_| _ = try store.cell(results.allocator(), 0, 0);
    try std.testing.expect(store.json_cache.?.budget.live <= JsonCache.limit);
    try std.testing.expect(store.json_cache.?.budget.peak + budget.peak < baseline.peak);
    std.debug.print("RETAINED_JSON_CACHE before_peak={d} after_scratch_peak={d} cache_peak={d} wire={d} reads=16\n", .{ baseline.peak, budget.peak, store.json_cache.?.budget.peak, probe.bytes.len });
}

const SharedJsonFixture = struct {
    text: []const u8,
    frozen: ?@import("execution_batch.zig").FrozenJson = null,
    fn cell(_: *anyopaque, _: A, _: usize, _: usize) !Datum {
        return error.UnexpectedScalarMaterialization;
    }
    fn json(raw: *anyopaque, _: usize, _: usize) !?[]const u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (self.frozen == null) self.text else null;
    }
    fn tokens(raw: *anyopaque, _: usize, _: usize) !?@import("execution_batch.zig").FrozenJson {
        return @as(*@This(), @ptrCast(@alignCast(raw))).frozen;
    }
    fn populate(self: *@This(), store: *Store, count: usize, reference: bool) !void {
        if (!reference) return store.appendBatch(.{ .reader = .{ .ptr = self, .read = cell, .read_json = json, .read_frozen = tokens, .count = count, .width = 1 } });
        // Reference the former per-row payload-copy policy with the same
        // retained column/cache structures, without content interning.
        store.columns = try store.a.alloc(Column, 1);
        store.columns[0] = .{};
        store.initialized = true;
        store.columns[0].json_cache = try store.jsonCache();
        const owned = store.arena.allocator();
        for (0..count) |row| {
            const value = try owned.create(JsonPayload);
            value.* = if (self.frozen) |frozen| .{ .frozen = .{ .bytes = try owned.dupe(u8, frozen.bytes), .lossless_text = frozen.lossless_text } } else .{ .text = try owned.dupe(u8, self.text) };
            try store.columns[0].appendJson(store.a, owned, owned, row, value);
            store.len += 1;
        }
    }
};

test "SQL shared JSON owns payloads and scalar caches across producers and representations" {
    const Fixture = struct {
        fn run(a: A, frozen_mode: bool) !void {
            var store = Store.init(a);
            defer store.deinit();
            {
                var source = std.heap.ArenaAllocator.init(a);
                defer source.deinit();
                const sa = source.allocator();
                const padding: [512]u8 = @splat('x');
                const text = try std.fmt.allocPrint(sa, "{{\"x\":9007199254740993,\"payload\":\"{s}\"}}", .{padding});
                var fixture: SharedJsonFixture = .{ .text = text };
                if (frozen_mode) {
                    const root = try std.json.parseFromSliceLeaky(std.json.Value, sa, text, .{});
                    const wire = try @import("spill.zig").encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(root)}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
                    const block = try @import("spill.zig").decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
                    defer block.deinit();
                    fixture.frozen = (try block.frozenJson(0, 0, false)).?;
                    try fixture.populate(&store, 3, false);
                    // Text bytes and frozen numeric tags must never share a
                    // cache, even when their rendered JSON means the same thing.
                    fixture.frozen = null;
                    try fixture.populate(&store, 1, false);
                } else try fixture.populate(&store, 4, false);
            }
            try std.testing.expect(store.columns[0].json_bytes.?.items[0].? == store.columns[0].json_bytes.?.items[2].?);
            if (frozen_mode) try std.testing.expect(store.columns[0].json_bytes.?.items[0].? != store.columns[0].json_bytes.?.items[3].?);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const first = try store.cell(scratch.allocator(), 0, 0);
            const repeated = try store.cell(scratch.allocator(), 2, 0);
            try std.testing.expect(first.value.object.values().ptr == repeated.value.object.values().ptr);
            const number = first.value.object.get("x").?;
            if (frozen_mode) try std.testing.expectEqual(@as(i64, 9007199254740993), number.integer) else try std.testing.expectEqualStrings("9007199254740993", number.number_string);
            try std.testing.expectEqual(@as(usize, 512), first.value.object.get("payload").?.string.len);
        }
    };
    for ([_]bool{ false, true }) |frozen| {
        try Fixture.run(std.testing.allocator, frozen);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{frozen});
    }
}

test "native projection refinements benchmark shared retained JSON payloads" {
    var source = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer source.deinit();
    var text: std.Io.Writer.Allocating = .init(source.allocator());
    try text.writer.writeAll("{\"payload\":\"");
    try text.writer.splatByteAll('x', 12000);
    try text.writer.writeAll("\"}");
    var fixture: SharedJsonFixture = .{ .text = text.written() };
    var peaks: [2]usize = undefined;
    var sums: [2]u64 = undefined;
    for ([_]bool{ true, false }, 0..) |reference, index| {
        var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 64 << 20 };
        var store = Store.init(budget.allocator());
        defer store.deinit();
        try fixture.populate(&store, 64, reference);
        var hash = std.hash.Wyhash.init(0);
        for (0..store.len) |row| hash.update((try store.encodedJson(row, 0)).?);
        sums[index] = hash.final();
        peaks[index] = budget.peak;
        if (!reference) try std.testing.expectEqual(@as(usize, 1), store.json_payloads.count());
    }
    try std.testing.expectEqual(sums[0], sums[1]);
    try std.testing.expect(peaks[1] * 8 < peaks[0]);
    std.debug.print("SHARED_RETAINED_JSON rows=64 source_bytes={d} before_peak={d} after_peak={d}\n", .{ text.written().len, peaks[0], peaks[1] });
}

test "SQL native JSON admission counts new payloads once and preserves representation identity" {
    const Fixture = struct {
        fn run(a: A, frozen_mode: bool) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const padding: [512]u8 = @splat('x');
            const text = try std.fmt.allocPrint(sa, "{{\"payload\":\"{s}\"}}", .{padding});
            var fixture: SharedJsonFixture = .{ .text = text };
            if (frozen_mode) {
                const root = try std.json.parseFromSliceLeaky(std.json.Value, sa, text, .{});
                const wire = try @import("spill.zig").encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(root)}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
                const block = try @import("spill.zig").decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
                defer block.deinit();
                fixture.frozen = (try block.frozenJson(0, 0, false)).?;
            }
            var store = Store.init(a);
            defer store.deinit();
            const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &fixture, .read = SharedJsonFixture.cell, .read_json = SharedJsonFixture.json, .read_frozen = SharedJsonFixture.tokens, .count = 8, .width = 1 } };
            // A candidate beyond its remaining allowance must decline before
            // allocating even the first temporary dictionary entry.
            var none = std.heap.FixedBufferAllocator.init(&.{});
            try std.testing.expectEqual(@as(usize, @sizeOf(?*JsonPayload) + 1), try store.batchAppendBytes(none.allocator(), batch, 0));
            const before = try store.batchAppendBytes(a, batch, std.math.maxInt(usize));
            try fixture.populate(&store, 8, false);
            const after = try store.batchAppendBytes(a, batch, std.math.maxInt(usize));
            try std.testing.expectEqual(@as(usize, 8 * (@sizeOf(?*JsonPayload) + 1)), after);
            try std.testing.expect(before > after + 512 and before < after + 1024);
            if (frozen_mode) {
                fixture.frozen.?.lossless_text = !fixture.frozen.?.lossless_text;
                try std.testing.expectEqual(before, try store.batchAppendBytes(a, batch, std.math.maxInt(usize)));
            }
        }
    };
    for ([_]bool{ false, true }) |frozen| {
        try Fixture.run(std.testing.allocator, frozen);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{frozen});
    }
}

test "SQL native JSON lease hashes each body once across admission and retention" {
    const Fixture = struct {
        fn run(a: A, frozen_mode: bool) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const padding: [1024]u8 = @splat('x');
            const text = try std.fmt.allocPrint(sa, "{{\"payload\":\"{s}\"}}", .{padding});
            var fixture: SharedJsonFixture = .{ .text = text };
            if (frozen_mode) {
                const root = try std.json.parseFromSliceLeaky(std.json.Value, sa, text, .{});
                const wire = try @import("spill.zig").encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(root)}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
                const block = try @import("spill.zig").decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
                defer block.deinit();
                fixture.frozen = (try block.frozenJson(0, 0, false)).?;
            }
            var store = Store.init(a);
            defer store.deinit();
            const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &fixture, .read = SharedJsonFixture.cell, .read_json = SharedJsonFixture.json, .read_frozen = SharedJsonFixture.tokens, .count = 64, .width = 1 } };
            var lease = Store.JsonLease.init(a);
            defer lease.deinit();
            const estimate = try store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &lease);
            for (0..3) |_| try std.testing.expectEqual(estimate, try store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &lease));
            lease.generation = std.math.maxInt(usize);
            try std.testing.expectEqual(estimate, try store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &lease));
            const length = if (fixture.frozen) |frozen| frozen.bytes.len else text.len;
            try std.testing.expectEqual(length, lease.hashed_bytes);
            try store.appendBatchLeased(batch, &lease);
            try std.testing.expectEqual(length, lease.hashed_bytes);
            try std.testing.expectEqual(@as(usize, 1), store.json_payloads.count());
            try std.testing.expectEqual(@as(usize, 64), store.len);
            // A new lease must resolve content afresh, even if the producer
            // reuses the same address for a different payload on the next pull.
            var again = Store.JsonLease.init(a);
            defer again.deinit();
            try std.testing.expectEqual(@as(usize, 64 * (@sizeOf(?*JsonPayload) + 1)), try store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &again));
            try std.testing.expect(estimate > 64 * (@sizeOf(?*JsonPayload) + 1));
            try store.appendBatchLeased(batch, &again);
            try std.testing.expectEqual(length, again.hashed_bytes);
            try std.testing.expectEqual(@as(usize, 1), store.json_payloads.count());
        }
    };
    for ([_]bool{ false, true }) |frozen| {
        try Fixture.run(std.testing.allocator, frozen);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{frozen});
    }
}

test "SQL native JSON lease scopes reused addresses and deduplicates distinct addresses" {
    const Fixture = struct {
        fn run(a: A) !void {
            var original: [1024]u8 = @splat('x');
            @memcpy(original[0..6], "{\"p\":\"");
            @memcpy(original[original.len - 2 ..], "\"}");
            var other = original;
            var fixture: SharedJsonFixture = .{ .text = &original };
            var store = Store.init(a);
            defer store.deinit();
            const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &fixture, .read = SharedJsonFixture.cell, .read_json = SharedJsonFixture.json, .count = 8, .width = 1 } };
            {
                var lease = Store.JsonLease.init(a);
                defer lease.deinit();
                const first = try store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &lease);
                // A separate immutable range with the same bytes shares content
                // storage, but has its own lease identity and content hash.
                fixture.text = &other;
                try store.appendBatchLeased(batch, &lease);
                fixture.text = &original;
                try store.appendBatchLeased(batch, &lease);
                try std.testing.expect(first > original.len);
                try std.testing.expectEqual(@as(usize, original.len * 2), lease.hashed_bytes);
                try std.testing.expectEqual(@as(usize, 1), store.json_payloads.count());
            }
            // The producer can overwrite an expired lease's address. The new
            // lease must hash the new bytes and retain a different payload.
            original[6] = 'y';
            var next = Store.JsonLease.init(a);
            defer next.deinit();
            _ = try store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &next);
            try store.appendBatchLeased(batch, &next);
            try std.testing.expectEqual(@as(usize, original.len), next.hashed_bytes);
            try std.testing.expectEqual(@as(usize, 2), store.json_payloads.count());
            try std.testing.expectEqual(@as(u8, 'x'), (try store.encodedJson(0, 0)).?[6]);
            try std.testing.expectEqual(@as(u8, 'y'), (try store.encodedJson(16, 0)).?[6]);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "native projection refinements benchmark shared JSON lease hash work" {
    const a = std.testing.allocator;
    var source = std.heap.ArenaAllocator.init(a);
    defer source.deinit();
    var text: std.Io.Writer.Allocating = .init(source.allocator());
    try text.writer.writeAll("{\"payload\":\"");
    try text.writer.splatByteAll('x', 1 << 20);
    try text.writer.writeAll("\"}");
    var fixture: SharedJsonFixture = .{ .text = text.written() };
    var store = Store.init(a);
    defer store.deinit();
    const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &fixture, .read = SharedJsonFixture.cell, .read_json = SharedJsonFixture.json, .count = 64, .width = 1 } };
    var lease = Store.JsonLease.init(a);
    defer lease.deinit();
    _ = try store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &lease);
    try store.appendBatchLeased(batch, &lease);
    try std.testing.expectEqual(text.written().len, lease.hashed_bytes);
    std.debug.print("SHARED_JSON_HASH rows=64 wire={d} before_hash_input={d} after_hash_input={d} unique_payloads={d}\n", .{ text.written().len, 128 * text.written().len, lease.hashed_bytes, store.json_payloads.count() });
}
