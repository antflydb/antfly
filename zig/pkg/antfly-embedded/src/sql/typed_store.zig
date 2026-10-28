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
const Work = @import("operator_work.zig");
/// Conservative physical payload estimate before cardinality is known.
/// Allocation admission still accounts for actual vector/hash capacity growth.
pub fn retainedCellBytes(value: Datum) !usize {
    return retainedCellBytesControlled(value, null);
}
pub fn retainedCellBytesControlled(value: Datum, control: ?Work.Control) !usize {
    try Work.metadataPoll(control);
    if (fixedCoefficient(value) != null) return 19;
    return 1 +| (if (value.patterns != null) @sizeOf(?*scalar.PatternSet) else @as(usize, 0)) +| switch (value.value) {
        .integer, .float => @as(usize, 8),
        .bool => @as(usize, 1),
        .string, .number_string => |text| @sizeOf([]const u8) +| text.len +| 32,
        .null => if (value.numeric == null and value.array == null) @sizeOf(Datum) else try @import("operators.zig").datumBytesWithControl(value, control),
        else => try @import("operators.zig").datumBytesWithControl(value, control),
    };
}
pub fn columnMetadataBytes(width: usize) usize {
    return width *| @sizeOf(Column);
}
const Dictionary = struct {
    values: std.ArrayList([]const u8) = .empty,
    indices: std.ArrayList(u32) = .empty,
    lookup: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    next: std.ArrayList(?u32) = .empty,
    flat: ?std.ArrayList([]const u8) = null,
    const empty: Dictionary = .{};
    fn growthBytes(self: *const Dictionary, additional: usize) usize {
        if (self.flat != null) return 0;
        const links = if (additional <= self.next.capacity - self.next.items.len) 0 else (self.next.items.len +| additional +| 8) *| 2 *| @sizeOf(?u32);
        if (additional <= self.lookup.available) return links;
        // A hash-table resize allocates the replacement before releasing the
        // old table. Account for that capacity jump rather than one cell's
        // payload, including metadata, keys, values and alignment/header space.
        const entries = self.lookup.count() +| additional;
        const required = entries *| 100 / std.hash_map.default_max_load_percentage +| 1;
        const capacity = std.math.ceilPowerOfTwo(usize, @max(8, required)) catch return std.math.maxInt(usize);
        return links +| capacity *| (@sizeOf(u8) + @sizeOf(u64) + @sizeOf(u32)) +| 64;
    }
    fn deinit(self: *Dictionary, a: A) void {
        self.values.deinit(a);
        self.indices.deinit(a);
        self.lookup.deinit(a);
        self.next.deinit(a);
        if (self.flat) |*values| values.deinit(a);
    }
    fn append(self: *Dictionary, a: A, owned: A, value: ?[]const u8, control: ?Work.Control) !void {
        try Work.poll(control);
        // High-cardinality columns revert to flat storage after a bounded
        // sample; dictionary overhead must not penalize unique identifiers.
        if (self.flat == null and self.values.items.len >= 512 and self.values.items.len > self.indices.items.len / 2) {
            var flat: std.ArrayList([]const u8) = .empty;
            errdefer flat.deinit(a);
            try flat.ensureTotalCapacity(a, self.indices.items.len + 1);
            for (self.indices.items) |index| {
                try Work.metadataPoll(control);
                flat.appendAssumeCapacity(self.values.items[index]);
            }
            self.values.clearAndFree(a);
            self.indices.clearAndFree(a);
            self.lookup.clearAndFree(a);
            self.next.clearAndFree(a);
            self.flat = flat;
        }
        if (self.flat) |*values| {
            try values.append(a, if (value) |bytes| try Work.dupe(u8, owned, bytes, control) else &.{});
            return;
        }
        const text = value orelse {
            try self.indices.append(a, 0);
            return;
        };
        const index = try self.intern(a, owned, text, control);
        try self.indices.append(a, index);
    }
    fn findHashed(self: *const Dictionary, text: []const u8, hash: u64, control: ?Work.Control) !?u32 {
        var slot = self.lookup.get(hash);
        while (slot) |index| {
            if (try Work.equalBytes(control, self.values.items[index], text)) return index;
            slot = self.next.items[index];
        }
        return null;
    }
    fn find(self: *const Dictionary, text: []const u8, control: ?Work.Control) !?u32 {
        // A small fixed-width index has a bounded probe domain. Larger maps
        // retain their direct check; text hashing/equality meter their bytes.
        if (self.lookup.capacity() <= 64) try Work.metadataPoll(control) else try Work.poll(control);
        if (self.lookup.count() == 0) return null;
        return self.findHashed(text, try Work.hashBytes(control, 0, text), control);
    }
    fn intern(self: *Dictionary, a: A, owned: A, text: []const u8, control: ?Work.Control) !u32 {
        const hash = try Work.hashBytes(control, 0, text);
        if (try self.findHashed(text, hash, control)) |index| return index;
        const index = std.math.cast(u32, self.values.items.len) orelse return error.SqlProgramLimitExceeded;
        // All fallible byte work precedes publication. Map growth hashes only
        // fixed-width integers, never whole retained keys under a callback.
        try self.values.ensureUnusedCapacity(a, 1);
        try self.next.ensureUnusedCapacity(a, 1);
        try self.lookup.ensureUnusedCapacity(a, 1);
        const bytes = try Work.dupe(u8, owned, text, control);
        self.values.appendAssumeCapacity(bytes);
        self.next.appendAssumeCapacity(self.lookup.get(hash));
        self.lookup.putAssumeCapacity(hash, index);
        return index;
    }
    fn resizeNulls(self: *Dictionary, a: A, count: usize, control: ?Work.Control) !void {
        try Work.resizeFill(u32, a, &self.indices, count, 0, control);
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
        fn resize(self: *@This(), a: A, count: usize, control: ?Work.Control) !void {
            try Work.resizeFill(T, a, &self.flat, count, 0, control);
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
fn fixedCoefficient(value: Datum) ?struct { coefficient: i128, scale: u16 } {
    const number = value.numeric orelse return null;
    if (number.kind != .finite or number.scale > 38) return null;
    const coefficient = @import("numeric_value.zig").coefficientAtScale(number.*, number.scale) orelse return null;
    if (@abs(coefficient) >= 100000000000000000000000000000000000000) return null;
    return .{ .coefficient = coefficient, .scale = number.scale };
}
const Fixed = struct {
    const Cells = std.AutoHashMapUnmanaged(usize, *const @import("numeric_value.zig").Value);
    cells: *Cells,
    values: std.ArrayList(i128) = .empty,
    scales: std.ArrayList(u16) = .empty,
    uniform: ?u16 = null,
    mixed: bool = false,
    fn deinit(self: *Fixed, a: A) void {
        self.values.deinit(a);
        self.scales.deinit(a);
    }
    fn cell(self: Fixed, a: A, row: usize) !Datum {
        if (self.cells.get(row)) |number| return Datum.typedNumeric(number);
        var context: @import("numeric_value.zig").Context = .{ .alloc = a };
        var owned = try @import("numeric_value.zig").fromScaledInteger(&context, self.values.items[row], self.scales.items[row]);
        errdefer owned.deinit();
        const number = try a.create(@import("numeric_value.zig").Value);
        number.* = owned.value;
        try self.cells.put(a, row, number);
        return Datum.typedNumeric(number);
    }
};
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
    decoding: bool = false,
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
        return self.materializeControlled(payload, a, null);
    }
    fn materializeControlled(self: *JsonCache, payload: *JsonPayload, a: A, control: ?Work.Control) !Datum {
        // Only constant-time metadata is protected by this physical mutex.
        // Decode may yield. Other readers decline the optional decode token
        // rather than waiting on a decoder that needs their execution slot.
        try Work.poll(control);
        while (!self.mutex.tryLock()) {
            try Work.poll(control);
            std.atomic.spinLoopHint();
        }
        if (payload.cached) |cached| {
            self.mutex.unlock();
            return cached.datum;
        }
        if (payload.attempted or self.decoding) {
            const snapshot = payload.*;
            self.mutex.unlock();
            return snapshot.decodeControlled(a, control);
        }
        if (self.admission) |gate| if (!gate.tryLock()) {
            const snapshot = payload.*;
            self.mutex.unlock();
            return snapshot.decodeControlled(a, control);
        };
        self.decoding = true;
        self.mutex.unlock();
        var claimed = true;
        defer if (claimed) self.endDecodeToken();
        const candidate = try self.cache(payload.*, control);
        self.lock();
        payload.cached = candidate;
        payload.attempted = true;
        const snapshot = payload.*;
        self.mutex.unlock();
        self.endDecodeToken();
        claimed = false;
        if (candidate) |cached| return cached.datum;
        return snapshot.decodeControlled(a, control);
    }
    fn endDecodeToken(self: *JsonCache) void {
        // Publication/rollback cannot yield or be canceled while holding
        // metadata. The optional token has no blocking consumer.
        self.lock();
        self.decoding = false;
        self.mutex.unlock();
        if (self.admission) |gate| gate.unlock();
    }
    fn encoded(self: *JsonCache, payload: *JsonPayload) !?[]const u8 {
        self.lock();
        if (payload.text) |text| {
            self.mutex.unlock();
            return text;
        }
        const frozen = payload.frozen orelse {
            self.mutex.unlock();
            return null;
        };
        if (!frozen.lossless_text or self.decoding) {
            self.mutex.unlock();
            return null;
        }
        if (self.admission) |gate| if (!gate.tryLock()) {
            self.mutex.unlock();
            return null;
        };
        self.decoding = true;
        self.mutex.unlock();
        defer self.endDecodeToken();
        // Encoded text is an optional capability. A busy cache declines it;
        // callers can use frozen bytes or scalar access without waiting.
        const text = try @import("spill.zig").frozenJsonText(self.text_arena.allocator(), frozen);
        self.lock();
        payload.text = text;
        self.mutex.unlock();
        return text;
    }
    fn cache(self: *JsonCache, payload: JsonPayload, control: ?Work.Control) !?*Entry {
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
        entry.datum = payload.decodeControlled(entry.arena.allocator(), control) catch |err| {
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
        return self.decodeControlled(a, null);
    }
    fn decodeControlled(self: JsonPayload, a: A, control: ?Work.Control) !Datum {
        if (self.frozen) |value| return @import("spill.zig").materializeFrozenJsonControlled(a, value, control);
        return Datum.json(try @import("controlled_json_text.zig").parseLeaky(a, self.text.?, control));
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
    fixed: ?Fixed = null,
    json_bytes: ?*std.ArrayList(?*JsonPayload) = null,
    json_cache: ?*JsonCache = null,
    nulls: std.ArrayList(u64) = .empty,
    patterns: std.ArrayList(?*scalar.PatternSet) = .empty,
    arrays: std.ArrayList(?*const @import("array_value.zig").Value) = .empty,
    numerics: std.ArrayList(?*const @import("numeric_value.zig").Value) = .empty,
    fn deinit(self: *Column, a: A) void {
        if (self.fixed) |*fixed| fixed.deinit(a);
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
        return self.cellControlled(a, row, null);
    }
    fn cellControlled(self: Column, a: A, row: usize, control: ?Work.Control) !Datum {
        try Work.metadataPoll(control);
        if (self.isNull(row)) return .{};
        if (self.fixed) |fixed| return fixed.cell(a, row);
        if (self.json_bytes) |bytes| if (bytes.items[row]) |json| {
            return (if (json.shared) |shared| &shared.cache else self.json_cache.?).materializeControlled(json, a, control);
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
    fn appendDictionary(self: *Column, a: A, owned: A, begin: usize, batch: @import("execution_batch.zig").Batch, control: ?Work.Control) !bool {
        if (self.fixed != null or self.json_bytes != null or batch != .dictionary or self.patterns.items.len != 0 or self.arrays.items.len != 0 or self.numerics.items.len != 0) return false;
        const source = batch.dictionary;
        for (source.indices) |id| {
            try Work.poll(control);
            if (id >= source.values.len) return error.InvalidSqlBackendResponse;
        }
        const remap = try a.alloc(u32, source.values.len);
        defer a.free(remap);
        @memset(remap, std.math.maxInt(u32));
        for (source.indices) |id| {
            try Work.poll(control);
            remap[id] = 0;
        }
        var tag: ?std.meta.Tag(std.json.Value) = null;
        var referenced: usize = 0;
        for (source.values, remap) |value, id| {
            try Work.poll(control);
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
                inline .integers, .numbers => |*v| try v.resize(a, begin, control),
                .strings, .decimals => |*v| try v.resizeNulls(a, begin, control),
                else => unreachable,
            }
        }
        switch (self.values) {
            inline .integers, .numbers => |*v| {
                if (!v.encoded) {
                    for (v.flat.items) |value| {
                        try Work.poll(control);
                        try v.encodeOne(a, value);
                    }
                    v.flat.clearAndFree(a);
                    v.encoded = true;
                    v.sampled = true;
                }
                for (source.values, remap) |value, *id| {
                    try Work.poll(control);
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
                for (source.indices) |id| {
                    try Work.poll(control);
                    v.indices.appendAssumeCapacity(remap[id]);
                }
                if (v.values.items.len >= 512 and v.values.items.len > v.indices.items.len / 2) {
                    var flat: @TypeOf(v.flat) = .empty;
                    errdefer flat.deinit(a);
                    try flat.ensureTotalCapacity(a, v.indices.items.len);
                    for (v.indices.items) |id| {
                        try Work.poll(control);
                        flat.appendAssumeCapacity(v.values.items[id]);
                    }
                    v.values.clearAndFree(a);
                    v.indices.clearAndFree(a);
                    v.lookup.clearAndFree(a);
                    v.flat = flat;
                    v.encoded = false;
                }
            },
            .strings, .decimals => |*v| {
                for (source.values, remap) |value, *id| {
                    try Work.poll(control);
                    if (id.* == std.math.maxInt(u32)) continue;
                    id.* = 0;
                    if (value.sql_null) continue;
                    const text = if (value.value == .string) value.value.string else value.value.number_string;
                    id.* = try v.intern(a, owned, text, control);
                }
                try v.indices.ensureUnusedCapacity(a, source.indices.len);
                for (source.indices) |id| {
                    try Work.poll(control);
                    v.indices.appendAssumeCapacity(remap[id]);
                }
                if (v.values.items.len >= 512 and v.values.items.len > v.indices.items.len / 2) {
                    var flat: std.ArrayList([]const u8) = .empty;
                    errdefer flat.deinit(a);
                    try flat.ensureTotalCapacity(a, v.indices.items.len);
                    for (v.indices.items) |id| {
                        try Work.metadataPoll(control);
                        flat.appendAssumeCapacity(if (v.values.items.len == 0) &.{} else v.values.items[id]);
                    }
                    v.values.clearAndFree(a);
                    v.indices.clearAndFree(a);
                    v.lookup.clearAndFree(a);
                    v.next.clearAndFree(a);
                    v.flat = flat;
                }
            },
            else => unreachable,
        }
        for (source.indices, 0..) |id, offset| {
            try Work.poll(control);
            const row = begin + offset;
            if (row % 64 == 0) try self.nulls.append(a, 0);
            if (source.values[id].sql_null) self.nulls.items[row / 64] |= @as(u64, 1) << @as(u6, @intCast(row % 64));
        }
        return true;
    }
    fn appendJson(self: *Column, a: A, owned: A, scratch: A, row: usize, payload: *JsonPayload, control: ?Work.Control) !void {
        if (self.json_bytes == null) {
            const values = try a.create(std.ArrayList(?*JsonPayload));
            values.* = .empty;
            self.json_bytes = values;
            try Work.resizeFill(?*JsonPayload, a, values, row, null, control);
        }
        try self.append(a, owned, scratch, row, Datum.json(.null), control);
        self.json_bytes.?.items[row] = payload;
    }
    // Only fixed-size header updates and primitive appends with spare storage
    // share metadata credit. Growth, widening and complex payloads retain the
    // original direct owner check before their variable work.
    fn metadataAppend(self: Column, row: usize, value: Datum) bool {
        if (self.fixed != null or self.json_bytes != null or value.numeric != null or value.array != null or value.patterns != null or self.numerics.items.len != 0 or self.arrays.items.len != 0 or self.patterns.items.len != 0) return false;
        if (row % 64 == 0 and self.nulls.items.len == self.nulls.capacity and self.nulls.items.len != 0) return false;
        return switch (self.values) {
            .unknown => value.sql_null or (row == 0 and (value.value == .integer or value.value == .float or value.value == .bool)),
            .integers => |v| (value.sql_null or value.value == .integer) and !v.encoded and (v.sampled or v.flat.items.len < 256) and (v.flat.items.len == 0 or v.flat.items.len < v.flat.capacity),
            .numbers => |v| (value.sql_null or value.value == .float) and !v.encoded and (v.sampled or v.flat.items.len < 256) and (v.flat.items.len == 0 or v.flat.items.len < v.flat.capacity),
            .booleans => |v| (value.sql_null or value.value == .bool) and (v.items.len == 0 or v.items.len < v.capacity),
            // Dictionary.append checks the original owner before any byte
            // copy, lookup growth or representation change. Only this fixed
            // header prefix shares metadata credit with the enclosing batch.
            .strings => value.sql_null or value.value == .string,
            .decimals => value.sql_null or value.value == .number_string,
            else => false,
        };
    }
    fn append(self: *Column, a: A, owned: A, scratch: A, row: usize, value: Datum, control: ?Work.Control) !void {
        if (self.metadataAppend(row, value)) try Work.metadataPoll(control) else try Work.poll(control);
        if (self.json_bytes) |bytes| try bytes.append(a, null);
        const coefficient = fixedCoefficient(value);
        if (self.fixed == null and self.json_bytes == null and self.values == .unknown and self.numerics.items.len == 0 and self.arrays.items.len == 0 and self.patterns.items.len == 0 and coefficient != null) {
            const cells = try owned.create(Fixed.Cells);
            cells.* = .empty;
            self.fixed = .{ .cells = cells };
            try Work.resizeFill(i128, a, &self.fixed.?.values, row, 0, control);
            try Work.resizeFill(u16, a, &self.fixed.?.scales, row, 0, control);
        }
        if (self.fixed) |*fixed| {
            if (value.sql_null or (coefficient != null and value.array == null and value.patterns == null)) {
                if (row % 64 == 0) try self.nulls.append(a, 0);
                if (value.sql_null) self.nulls.items[row / 64] |= @as(u64, 1) << @as(u6, @intCast(row % 64));
                if (!value.sql_null) if (coefficient) |c| {
                    if (fixed.uniform != null and fixed.uniform.? != c.scale) fixed.mixed = true;
                    fixed.uniform = c.scale;
                };
                try fixed.values.append(a, if (coefficient) |c| c.coefficient else 0);
                try fixed.scales.append(a, if (coefficient) |c| c.scale else 0);
                return;
            }
            // Widen once, retaining exact display scales and SQL NULLs.
            try Work.ensureTotalCapacity(@TypeOf(self.numerics.items[0]), a, &self.numerics, row, control);
            for (0..row) |i| {
                try Work.poll(control);
                const number = if (self.isNull(i)) null else blk: {
                    // Convert the bounded fixed coefficient directly. Populating
                    // the reader cache here would grow a hash table and retain a
                    // second copy of every value merely to widen this column.
                    const numeric = @import("numeric_value.zig");
                    var context: numeric.Context = .{ .alloc = owned, .checkpoint = if (control) |c| c.checkpoint else null, .ptr = if (control) |c| c.ptr else null };
                    var converted = try numeric.fromScaledInteger(&context, fixed.values.items[i], fixed.scales.items[i]);
                    errdefer converted.deinit();
                    const result = try owned.create(numeric.Value);
                    result.* = converted.value;
                    break :blk result;
                };
                self.numerics.appendAssumeCapacity(number);
            }
            fixed.deinit(a);
            self.fixed = null;
            self.values = .{ .encoded = .empty };
            try Work.resizeFill(@TypeOf(self.values.encoded.items[0]), a, &self.values.encoded, row, .null, control);
        }
        if (self.numerics.items.len != 0 or value.numeric != null) {
            if (self.numerics.items.len == 0) {
                try Work.resizeFill(@TypeOf(self.numerics.items[0]), a, &self.numerics, row, null, control);
            }
            try self.numerics.append(a, if (value.numeric != null) (try @import("operators.zig").cloneDatumControlled(owned, value, control)).numeric else null);
        }
        if (self.arrays.items.len != 0 or value.array != null) {
            if (self.arrays.items.len == 0) {
                try Work.resizeFill(@TypeOf(self.arrays.items[0]), a, &self.arrays, row, null, control);
            }
            try self.arrays.append(a, if (value.array != null) (try @import("operators.zig").cloneDatumControlled(owned, value, control)).array else null);
        }
        if (row % 64 == 0) try self.nulls.append(a, 0);
        if (value.sql_null) self.nulls.items[row / 64] |= @as(u64, 1) << @as(u6, @intCast(row % 64));
        if (self.patterns.items.len != 0 or value.patterns != null) {
            if (self.patterns.items.len == 0) {
                try Work.resizeFill(@TypeOf(self.patterns.items[0]), a, &self.patterns, row, null, control);
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
                .strings, .decimals => |*values| try values.resizeNulls(a, row, control),
                inline .integers, .numbers => |*values| try values.resize(a, row, control),
                inline else => |*values| {
                    try Work.resizeFill(@typeInfo(@TypeOf(values.items)).pointer.child, a, values, row, switch (@typeInfo(@TypeOf(values.items)).pointer.child) {
                        i64, f64 => 0,
                        bool => false,
                        std.json.Value => .null,
                        else => &.{},
                    }, control);
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
            try Work.ensureTotalCapacity(std.json.Value, a, &encoded, row + 1, control);
            for (0..row) |i| {
                try Work.poll(control);
                const prior = try self.cellControlled(scratch, i, control);
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
            .strings => |*v| try v.append(a, owned, if (value.sql_null) null else value.value.string, control),
            .decimals => |*v| try v.append(a, owned, if (value.sql_null) null else value.value.number_string, control),
            .encoded => |*v| try v.append(a, if (value.sql_null or value.array != null or value.numeric != null) .null else (try @import("operators.zig").cloneDatumControlled(owned, value, control)).value),
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
    /// Scalar materialization shares the retained owner's arena. Physical
    /// consumers remain allocation-free; concurrent readers serialize only
    /// the uncommon cache miss / canonical cell path.
    reader_mutex: std.atomic.Mutex = .unlocked,
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
        fn acquire(self: *JsonPool, key: JsonKey, payload: JsonPayload, control: ?Work.Control) !*JsonPayload {
            if (try jsonGet(&self.bodies, key, control)) |owner| {
                owner.references += 1;
                return &owner.payload;
            }
            const a = self.budget.allocator();
            const owner = try a.create(SharedJson);
            errdefer a.destroy(owner);
            const bytes = try Work.dupe(u8, a, key.bytes, control);
            errdefer a.free(bytes);
            var owned_key = key;
            owned_key.bytes = bytes;
            owner.* = .{ .pool = self, .key = owned_key, .payload = payload, .cache = .{ .budget = .{ .backing = self.cache_budget.allocator(), .limit = JsonCache.limit }, .text_arena = .init(a), .admission = &self.cache_mutex } };
            owner.payload.shared = owner;
            if (payload.frozen != null) owner.payload.frozen.?.bytes = bytes else owner.payload.text = bytes;
            _ = try jsonPut(&self.bodies, a, owned_key, owner, control);
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
        /// Synchronous producer-lease borrow. Never copied into retained data.
        control: ?Work.Control = null,
        const Identity = struct { address: usize, len: usize, representation: u8 };
        const Resolution = struct { key: JsonKey, owned: ?*JsonPayload, estimated: usize = 0 };
        pub fn init(a: A) JsonLease {
            return initControlled(a, null);
        }
        pub fn initControlled(a: A, control: ?Work.Control) JsonLease {
            return .{ .a = @import("memory_budget.zig").reclaimingAllocator(a), .control = control };
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
            try Work.poll(self.control);
            const bytes = if (payload.frozen) |value| value.bytes else payload.text.?;
            const representation: u8 = if (payload.frozen) |value| (if (value.lossless_text) @as(u8, 3) else 2) else 1;
            const identity: Identity = .{ .address = @intFromPtr(bytes.ptr), .len = bytes.len, .representation = representation };
            if (self.identities.getPtr(identity)) |existing| return existing;
            // Finish interruptible work before publishing an initialized memo.
            const key: JsonKey = .{ .representation = representation, .bytes = bytes, .hash = try Work.hashBytes(self.control, representation, bytes) };
            self.hashed_bytes +|= bytes.len;
            const owned = try jsonGet(&store.json_payloads, key, self.control);
            const slot = try self.identities.getOrPut(self.a, identity);
            slot.value_ptr.* = .{ .key = key, .owned = owned };
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
    // Hash-map equality cannot return an error. A failed comparison stops the
    // probe as a provisional match; callers check the original error before
    // consuming a value or publishing a key. No adapter escapes this call.
    const JsonKeyAdapter = struct {
        control: ?Work.Control,
        failure: *?anyerror,
        pub fn hash(_: @This(), key: JsonKey) u64 {
            return key.hash;
        }
        pub fn eql(self: @This(), left: JsonKey, right: JsonKey) bool {
            if (left.representation != right.representation) return false;
            return Work.equalBytes(self.control, left.bytes, right.bytes) catch |err| {
                self.failure.* = err;
                return true;
            };
        }
    };
    fn jsonGet(map: anytype, key: JsonKey, control: ?Work.Control) !@TypeOf(map.get(key)) {
        try Work.poll(control);
        var failure: ?anyerror = null;
        const result = map.getAdapted(key, JsonKeyAdapter{ .control = control, .failure = &failure });
        if (failure) |err| return err;
        return result;
    }
    fn jsonPut(map: anytype, a: A, key: JsonKey, value: anytype, control: ?Work.Control) !bool {
        try Work.poll(control);
        var failure: ?anyerror = null;
        const slot = map.getOrPutAdapted(a, key, JsonKeyAdapter{ .control = control, .failure = &failure }) catch |err| return failure orelse err;
        if (failure) |err| return err;
        if (!slot.found_existing) slot.key_ptr.* = key;
        slot.value_ptr.* = value;
        return !slot.found_existing;
    }
    fn ownJson(self: *Store, payload: JsonPayload, lease: *JsonLease) !*JsonPayload {
        const source = if (payload.frozen) |value| value.bytes else payload.text.?;
        const resolved = if (source.len >= 256) try lease.resolve(self, payload) else null;
        if (resolved) |entry| {
            if (entry.owned) |existing| return existing;
            // A different source address with equal content may have been
            // retained since admission. Cached hashes keep this lookup cheap.
            if (try jsonGet(&self.json_payloads, entry.key, lease.control)) |existing| {
                entry.owned = existing;
                return existing;
            }
        }
        if (resolved) |entry| if (self.json_pool) |pool| {
            const result = try pool.acquire(entry.key, payload, lease.control);
            errdefer pool.release(result);
            _ = try jsonPut(&self.json_payloads, self.a, result.shared.?.key, result, lease.control);
            entry.owned = result;
            return result;
        };
        const owned = self.arena.allocator();
        const result = try owned.create(JsonPayload);
        result.* = payload;
        const bytes = try Work.dupe(u8, owned, source, lease.control);
        if (payload.frozen != null) result.frozen.?.bytes = bytes else result.text = bytes;
        if (resolved) |entry| {
            var key = entry.key;
            key.bytes = bytes;
            _ = try jsonPut(&self.json_payloads, self.a, key, result, lease.control);
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
    pub fn decimalColumn(self: *const Store, column: usize, begin: usize, count: usize) !?@import("execution_batch.zig").DecimalColumn {
        if (self.failed or column >= self.columns.len or begin > self.len or count > self.len - begin) return error.InvalidSqlBackendResponse;
        const stored = self.columns[column];
        const fixed = stored.fixed orelse return null;
        return .{ .scales = if (fixed.mixed) fixed.scales.items[begin..][0..count] else &.{}, .physical = .{ .vector = .{ .name = "", .values = .{ .decimal128 = .{ .values = fixed.values.items[begin..][0..count], .precision = 38, .scale = fixed.uniform orelse 0 } }, .nulls = .{ .bits = .{ .words = stored.nulls.items, .offset = begin } } }, .range = .{ .count = count }, .kind = .number, .element = .numeric } };
    }

    pub fn physicalColumn(self: *const Store, _: A, column: usize, begin: usize, count: usize) !?@import("execution_batch.zig").PhysicalColumn {
        if (self.failed or column >= self.columns.len or begin > self.len or count > self.len - begin) return error.InvalidSqlBackendResponse;
        const stored = self.columns[column];
        if (stored.patterns.items.len != 0 or stored.arrays.items.len != 0 or stored.numerics.items.len != 0) return null;
        const types = @import("../storage/rowsource/types.zig");
        const values: types.ColumnValues = switch (stored.values) {
            .integers => |v| if (v.encoded) .{ .dictionary_i64 = .{ .values = v.values.items, .indices = v.indices.items[begin..][0..count] } } else .{ .i64 = v.flat.items[begin..][0..count] },
            .numbers => |v| if (v.encoded) .{ .dictionary_f64 = .{ .values = v.values.items, .indices = v.indices.items[begin..][0..count] } } else .{ .f64 = v.flat.items[begin..][0..count] },
            .booleans => |v| .{ .bool = v.items[begin..][0..count] },
            .strings => |v| if (v.flat) |flat| .{ .bytes = flat.items[begin..][0..count] } else .{ .dictionary_bytes = .{ .values = v.values.items, .indices = v.indices.items[begin..][0..count] } },
            else => return null,
        };
        return .{ .vector = .{ .name = "", .values = values, .nulls = .{ .bits = .{ .words = stored.nulls.items, .offset = begin } } }, .range = .{ .count = count }, .kind = switch (values) {
            .i64, .dictionary_i64 => .integer,
            .f64, .dictionary_f64 => .number,
            .bool => .boolean,
            .bytes, .dictionary_bytes => .string,
            else => unreachable,
        }, .element = if (stored.values == .numbers) .float64 else null };
    }
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
        return self.dictionaryBatchControlled(a, column, begin, count, null);
    }
    pub fn dictionaryBatchControlled(self: *const Store, a: A, column: usize, begin: usize, count: usize, control: ?Work.Control) !?@import("execution_batch.zig").Batch {
        try Work.metadataPoll(control);
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
            try Work.metadataPoll(control);
            const physical = (try self.dictionaryId(begin + offset, column)).?;
            if (physical == 0) {
                id.* = 0;
                continue;
            }
            const entry = try ids.getOrPut(a, physical);
            if (!entry.found_existing) {
                entry.value_ptr.* = @intCast(used);
                values[used] = try stored.cellControlled(a, begin + offset, control);
                used += 1;
            }
            id.* = entry.value_ptr.*;
        }
        return .{ .dictionary = .{ .values = try a.realloc(values, used), .indices = indices } };
    }
    pub fn appendBytes(self: *const Store, values: []const Datum) !usize {
        return self.appendBytesControlled(values, null);
    }
    pub fn appendBytesControlled(self: *const Store, values: []const Datum, control: ?Work.Control) !usize {
        try Work.poll(control);
        // Estimate retained column storage, not a reconstructed Datum row.
        // Callers reserve growth headroom separately; the allocator remains
        // the authority for actual admission, including representation changes.
        var bytes: usize = if (self.initialized) 0 else columnMetadataBytes(values.len);
        for (values, 0..) |value, column| bytes +|= try self.cellAppendBytesControlled(column, value, control);
        return bytes;
    }
    fn cellAppendBytes(self: *const Store, index: usize, value: Datum) !usize {
        return self.cellAppendBytesControlled(index, value, null);
    }
    fn cellAppendBytesControlled(self: *const Store, index: usize, value: Datum, control: ?Work.Control) !usize {
        try Work.metadataPoll(control);
        var bytes: usize = 0;
        var repeated = false;
        if (!value.sql_null and index < self.columns.len) {
            const column = self.columns[index];
            if (column.fixed != null and fixedCoefficient(value) != null) return 19;
            if (column.fixed != null and fixedCoefficient(value) == null) bytes +|= self.len *| 256;
            if (column.values == .strings and value.value == .string) repeated = (try column.values.strings.find(value.value.string, control)) != null;
            if (column.values == .decimals and value.value == .number_string) repeated = (try column.values.decimals.find(value.value.number_string, control)) != null;
            if (!repeated) {
                if (column.values == .strings and value.value == .string) bytes +|= column.values.strings.growthBytes(1);
                if (column.values == .decimals and value.value == .number_string) bytes +|= column.values.decimals.growthBytes(1);
            }
        }
        bytes +|= if (repeated) @sizeOf(u32) + 1 else try retainedCellBytesControlled(value, control);
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
        var scope: Work.MetadataScope = .{ .original = lease.control };
        const control = scope.view();
        try Work.poll(control);
        const bytes = try self.batchAppendBytesInScope(a, batch, maximum, lease, control);
        try Work.poll(control);
        return bytes;
    }
    fn batchAppendBytesInScope(self: *const Store, a: A, batch: @import("execution_batch.zig").Batch, maximum: usize, lease: *JsonLease, control: ?Work.Control) !usize {
        var region = std.heap.ArenaAllocator.init(@import("memory_budget.zig").reclaimingAllocator(a));
        defer region.deinit();
        const work = region.allocator();
        var incoming: std.HashMapUnmanaged(JsonKey, void, JsonKeyContext, 80) = .empty;
        var cell_region = std.heap.ArenaAllocator.init(work);
        defer cell_region.deinit();
        lease.generation +%= 1;
        if (lease.generation == 0) {
            var entries = lease.identities.valueIterator();
            while (entries.next()) |entry| {
                try Work.metadataPoll(control);
                entry.estimated = 0;
            }
            lease.generation = 1;
        }
        var bytes: usize = 0;
        for (0..batch.width()) |column| for (0..batch.len()) |index| {
            try Work.metadataPoll(control);
            const payload: ?JsonPayload = if (try batch.frozenJsonControlled(index, column, control)) |frozen|
                .{ .frozen = frozen }
            else if (try batch.encodedJsonControlled(index, column, control)) |text|
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
                    if (resolved.owned == null) resolved.owned = try jsonGet(&self.json_payloads, resolved.key, lease.control);
                    if (resolved.owned != null) continue;
                    if (self.json_pool) |pool| if (try jsonGet(&pool.bodies, resolved.key, lease.control) != null) continue;
                    const prior_growth = if (self.json_pool) |pool| pool.growthBytes(incoming.count()) else @as(usize, 0);
                    if (!try jsonPut(&incoming, work, resolved.key, {}, lease.control)) continue;
                    if (self.json_pool) |pool| bytes +|= pool.growthBytes(incoming.count()) -| prior_growth;
                }
                bytes +|= source.len +| @sizeOf(JsonPayload) +| 64;
            } else {
                _ = cell_region.reset(.retain_capacity);
                bytes +|= try self.cellAppendBytesControlled(column, try batch.cellControlled(cell_region.allocator(), index, column, control), control);
            }
            // Reject before temporary identity state can outgrow admission.
            if (bytes > maximum) return bytes;
        };
        return bytes;
    }

    fn listGrowth(values: anytype, additional: usize) usize {
        const required = values.items.len +| additional;
        if (required <= values.capacity) return 0;
        return (required *| 2 +| 8) *| @sizeOf(@TypeOf(values.items[0]));
    }
    fn tableGrowth(table: anytype, additional: usize) usize {
        if (additional <= table.available) return 0;
        const required = (table.count() +| additional) *| 100 / std.hash_map.default_max_load_percentage +| 1;
        const capacity = std.math.ceilPowerOfTwo(usize, @max(8, required)) catch return std.math.maxInt(usize);
        return capacity *| 32 +| 64;
    }
    /// Include replacement vector/hash capacities, not just retained cells.
    /// Numeric/string dictionary sampling may temporarily own both formats.
    pub fn appendGrowthBytes(self: *const Store, additional: usize) usize {
        if (additional == 0) return 0;
        var bytes: usize = 0;
        for (self.columns) |column| {
            const words = (self.len +| additional +| 63) / 64;
            bytes +|= listGrowth(column.nulls, words -| column.nulls.items.len);
            if (column.patterns.items.len != 0) bytes +|= listGrowth(column.patterns, additional);
            if (column.arrays.items.len != 0) bytes +|= listGrowth(column.arrays, additional);
            if (column.numerics.items.len != 0) bytes +|= listGrowth(column.numerics, additional);
            if (column.fixed) |fixed| bytes +|= listGrowth(fixed.values, additional) +| listGrowth(fixed.scales, additional);
            switch (column.values) {
                .unknown => {},
                inline .booleans, .encoded => |values| bytes +|= listGrowth(values, additional),
                inline .integers, .numbers => |values| {
                    if (!values.encoded) {
                        bytes +|= listGrowth(values.flat, additional);
                        if (!values.sampled and values.flat.items.len +| additional > 256) bytes +|= 256 * 64;
                    } else {
                        bytes +|= listGrowth(values.values, additional);
                        bytes +|= listGrowth(values.indices, additional);
                        bytes +|= tableGrowth(values.lookup, additional);
                        if (values.values.items.len +| additional >= 512) bytes +|= (self.len +| additional) *| 16;
                    }
                },
                .strings, .decimals => |values| {
                    if (values.flat) |flat| bytes +|= listGrowth(flat, additional) else {
                        bytes +|= listGrowth(values.values, additional);
                        bytes +|= listGrowth(values.indices, additional);
                        bytes +|= values.growthBytes(additional);
                        if (values.values.items.len +| additional >= 512) bytes +|= (self.len +| additional) *| 32;
                    }
                },
            }
        }
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
        var scope: Work.MetadataScope = .{ .original = lease.control };
        const control = scope.view();
        try Work.poll(control);
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
            try Work.metadataPoll(control);
            _ = scratch.reset(.free_all);
            if (column.fixed != null or (column.values == .unknown and column.numerics.items.len == 0 and column.arrays.items.len == 0 and column.patterns.items.len == 0)) {
                if (try batch.decimalColumnSelectedControlled(scratch.allocator(), ordinal, null, control)) |decimal| {
                    for (0..batch.len()) |index| {
                        var coefficient: @import("numeric_value.zig").ScaledInteger = undefined;
                        var number: @import("numeric_value.zig").Value = undefined;
                        const value = try decimal.cell(index, &coefficient, &number);
                        try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, value, control);
                    }
                    continue;
                }
            }
            if (try batch.dictionaryColumnControlled(scratch.allocator(), ordinal, control)) |encoded| {
                if (encoded.len() != batch.len()) return error.InvalidSqlBackendResponse;
                if (try column.appendDictionary(self.a, self.arena.allocator(), self.len, encoded, control)) continue;
            }
            _ = scratch.reset(.free_all);
            for (0..batch.len()) |index| {
                try Work.metadataPoll(control);
                _ = scratch.reset(.retain_capacity);
                if (try batch.frozenJsonControlled(index, ordinal, control)) |frozen| {
                    column.json_cache = try self.jsonCache();
                    try column.appendJson(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, try self.ownJson(.{ .frozen = frozen }, lease), control);
                    continue;
                }
                if (try batch.encodedJsonControlled(index, ordinal, control)) |bytes| {
                    column.json_cache = try self.jsonCache();
                    try column.appendJson(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, try self.ownJson(.{ .text = bytes }, lease), control);
                    continue;
                }
                const value = try batch.cellControlled(scratch.allocator(), index, ordinal, control);
                try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len + index, value, control);
            }
        }
        try Work.poll(control);
        self.len += batch.len();
    }
    pub fn append(self: *Store, values: []const Datum) !usize {
        return self.appendControlled(values, null);
    }
    pub fn appendControlled(self: *Store, values: []const Datum, control: ?Work.Control) !usize {
        try Work.poll(control);
        if (self.failed or (self.initialized and values.len != self.columns.len)) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        if (!self.initialized) {
            self.columns = try self.a.alloc(Column, values.len);
            @memset(self.columns, .{});
            self.initialized = true;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (self.columns, values) |*column, value| try column.append(self.a, self.arena.allocator(), scratch.allocator(), self.len, value, control);
        try Work.poll(control);
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
            for (self.columns[prior_width..]) |*column| for (0..self.len) |index| try column.append(self.a, self.arena.allocator(), self.a, index, .{}, null);
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
        return self.cellBytesControlled(a, index, column, null);
    }
    pub fn cellBytesControlled(self: *const Store, a: A, index: usize, column: usize, control: ?Work.Control) !usize {
        try Work.poll(control);
        if (try self.frozenJson(index, column)) |value| return value.bytes.len;
        if (try self.encodedJson(index, column)) |bytes| return bytes.len;
        return @import("operators.zig").datumBytesWithControl(try self.cellControlled(a, index, column, control), control);
    }
    pub fn cell(self: *const Store, a: A, index: usize, column: usize) !Datum {
        return self.cellControlled(a, index, column, null);
    }
    pub fn cellControlled(self: *const Store, a: A, index: usize, column: usize, control: ?Work.Control) !Datum {
        if (self.failed or index >= self.len or column >= self.columns.len) return error.InvalidSqlBackendResponse;
        if (self.columns[column].fixed != null) {
            const owner = @constCast(self);
            try Work.poll(control);
            while (!owner.reader_mutex.tryLock()) {
                try Work.poll(control);
                std.atomic.spinLoopHint();
            }
            defer owner.reader_mutex.unlock();
            // Fixed coefficients need only bounded header/digit allocation.
            // No borrowed owner callback may yield while this arena is locked.
            return self.columns[column].cellControlled(owner.arena.allocator(), index, null);
        }
        return self.columns[column].cellControlled(a, index, control);
    }
    pub fn row(self: *const Store, a: A, index: usize) ![]const Datum {
        if (self.failed or index >= self.len) return error.InvalidSqlBackendResponse;
        const result = try a.alloc(Datum, self.columns.len);
        errdefer a.free(result);
        for (result, 0..) |*value, column| value.* = try self.cell(a, index, column);
        return result;
    }
    pub fn equal(self: *const Store, a: A, row_index: usize, values: []const Datum, null_equal: bool) !bool {
        return self.equalControlled(a, row_index, values, null_equal, null);
    }
    pub fn equalControlled(self: *const Store, a: A, row_index: usize, values: []const Datum, null_equal: bool, control: ?@import("operator_work.zig").Control) !bool {
        try @import("operator_work.zig").poll(control);
        if (self.failed or values.len != self.columns.len or row_index >= self.len) return error.InvalidSqlBackendResponse;
        for (values, 0..) |value, column| {
            try @import("operator_work.zig").poll(control);
            const stored = self.columns[column];
            const is_null = stored.isNull(row_index);
            if (is_null or value.sql_null) {
                if (!null_equal or is_null != value.sql_null) return false;
                continue;
            }
            if (stored.fixed) |fixed| {
                var coefficient = try @import("numeric_value.zig").ScaledInteger.init(fixed.values.items[row_index], fixed.scales.items[row_index]);
                const number = coefficient.view();
                if ((try scalar.compareDatums(Datum.typedNumeric(&number), value)) != .eq) return false;
                continue;
            }
            if (value.array != null or value.numeric != null or (stored.arrays.items.len != 0 and stored.arrays.items[row_index] != null) or (stored.numerics.items.len != 0 and stored.numerics.items[row_index] != null)) {
                if ((try scalar.compareDatumsControlled(try stored.cellControlled(a, row_index, control), value, control)) != .eq) return false;
                continue;
            }
            // Dispatch on the retained physical type, without constructing a
            // Datum or invoking JSON comparison for homogeneous primitive keys.
            const equal_ = switch (stored.values) {
                .integers => |v| if (value.value == .integer) v.get(row_index) == value.value.integer else null,
                .numbers => |v| if (value.value == .float and std.math.isFinite(v.get(row_index)) and std.math.isFinite(value.value.float)) v.get(row_index) == value.value.float else null,
                .booleans => |v| if (value.value == .bool) v.items[row_index] == value.value.bool else null,
                .strings => |v| if (value.value == .string) try @import("operator_work.zig").equalBytes(control, v.getText(row_index), value.value.string) else null,
                else => null,
            };
            if (equal_) |matches| {
                if (!matches) return false;
            } else if ((try scalar.compareDatumsControlled(try stored.cellControlled(a, row_index, control), value, control)) != .eq) return false;
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

fn nativeFixedScenario(a: A) !void {
    var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = 8 * 1024 * 1024 };
    var store = Store.init(budget.allocator());
    defer {
        store.deinit();
        std.debug.assert(budget.live == 0);
    }
    const batch = blk: {
        const values = try a.dupe(i128, &.{ 12300, -70000, 0 });
        errdefer a.free(values);
        const nulls = try a.dupe(u8, &.{ 0, 0, 1 });
        errdefer a.free(nulls);
        break :blk try @import("execution_batch.zig").Batch.fromDecimal(a, values, nulls, 4);
    };
    defer batch.reader.free_owned.?(batch.reader.ptr, a);
    try store.appendBatch(batch);
    try std.testing.expect(store.columns[0].fixed != null);
    try std.testing.expectEqual(@as(usize, 0), store.columns[0].numerics.items.len);
    const native = (try store.decimalColumn(0, 0, 3)).?;
    try std.testing.expectEqual(@as(i128, 12300), native.physical.vector.values.decimal128.values[0]);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    var context: @import("numeric_value.zig").Context = .{ .alloc = sa };
    const before = try store.cell(budget.allocator(), 0, 0);
    try std.testing.expectEqualStrings("1.2300", try @import("numeric_value.zig").format(&context, before.numeric.?.*));
    var work: @import("array_value.zig").Budget = .{};
    const wide = try scalar.numericTextLeaky(sa, "1e100", &work);
    _ = try store.append(&.{wide});
    try std.testing.expect(store.columns[0].fixed == null);
    try std.testing.expectEqualStrings("1.2300", try @import("numeric_value.zig").format(&context, (try store.cell(budget.allocator(), 0, 0)).numeric.?.*));
    try std.testing.expect((try store.cell(budget.allocator(), 2, 0)).sql_null);
    try std.testing.expectEqual(@as(usize, 4), store.len);
}
test "SQL retained decimal coefficients stay native then widen exact values without borrowing temporary limbs" {
    try nativeFixedScenario(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, nativeFixedScenario, .{});
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
            try store.columns[0].appendJson(store.a, owned, owned, row, value, null);
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

test "SQL grouping work retained dictionary collision chains compare full keys cooperatively" {
    const a = std.testing.allocator;
    var first: [8192]u8 = @splat('x');
    var second = first;
    first[first.len - 1] = 'a';
    second[second.len - 1] = 'b';
    var dictionary: Dictionary = .{};
    defer dictionary.deinit(a);
    try dictionary.values.appendSlice(a, &.{ &first, &second });
    try dictionary.next.appendSlice(a, &.{ null, 0 });
    // Force a raw hash collision to exercise the exact chain, independently
    // of the hash implementation's resistance to naturally colliding keys.
    try dictionary.lookup.put(a, 42, 1);
    const Poll = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == self.stop) return error.OriginalKeyCanceled;
        }
        fn control(self: *@This()) Work.Control {
            return .{ .ptr = self, .checkpoint = check };
        }
    };
    var poll: Poll = .{};
    try std.testing.expectEqual(@as(?u32, 0), try dictionary.findHashed(&first, 42, poll.control()));
    try std.testing.expect(poll.calls > 32);
    poll = .{ .stop = 8 };
    try std.testing.expectError(error.OriginalKeyCanceled, dictionary.findHashed(&first, 42, poll.control()));
    try std.testing.expectEqual(@as(usize, 2), dictionary.values.items.len);
}

const JsonLeasePoll = struct {
    calls: usize = 0,
    stop: usize = std.math.maxInt(usize),
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        if (self.calls >= self.stop) return error.OriginalJsonLeaseCanceled;
    }
    fn control(self: *@This()) Work.Control {
        return .{ .ptr = self, .checkpoint = check };
    }
};

test "SQL native JSON lease cancellation interrupts hashing before memo publication and permits retry" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 64 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    text[0] = '"';
    text[text.len - 1] = '"';
    const Source = struct {
        text: []const u8,
        fn json(raw: *anyopaque, _: usize, _: usize) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.text;
        }
        fn cell(_: *anyopaque, _: A, _: usize, _: usize) !Datum {
            return error.UnexpectedScalarMaterialization;
        }
    };
    var source: Source = .{ .text = text };
    const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &source, .read = Source.cell, .read_json = Source.json, .count = 2, .width = 1 } };
    var store = Store.init(a);
    defer store.deinit();
    var poll: JsonLeasePoll = .{ .stop = 6 };
    var lease = Store.JsonLease.initControlled(a, poll.control());
    defer lease.deinit();
    try std.testing.expectError(error.OriginalJsonLeaseCanceled, store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &lease));
    try std.testing.expectEqual(@as(usize, 0), lease.identities.count());
    try std.testing.expectEqual(@as(usize, 0), store.len);
    poll = .{};
    _ = try store.batchAppendBytesLeased(a, batch, std.math.maxInt(usize), &lease);
    try std.testing.expectEqual(text.len, lease.hashed_bytes);
    try std.testing.expectEqual(@as(usize, 1), lease.identities.count());
    try store.appendBatchLeased(batch, &lease);
    try std.testing.expectEqual(@as(usize, 2), store.len);
    try std.testing.expectEqual(text.len, lease.hashed_bytes);
    // Retained state borrows no work authority after the synchronous lease.
    poll.stop = 1;
    try std.testing.expectEqualStrings(text, (try store.encodedJson(1, 0)).?);
}

test "SQL native JSON lease content collisions preserve cancellation and do not publish provisional matches" {
    const a = std.testing.allocator;
    var left: [16 * 1024]u8 = @splat('x');
    var right = left;
    right[right.len - 1] = 'y';
    const original: Store.JsonKey = .{ .representation = 1, .hash = 42, .bytes = &left };
    const collision: Store.JsonKey = .{ .representation = 1, .hash = 42, .bytes = &right };
    var map: std.HashMapUnmanaged(Store.JsonKey, u32, Store.JsonKeyContext, 80) = .empty;
    defer map.deinit(a);
    _ = try Store.jsonPut(&map, a, original, @as(u32, 7), null);
    var poll: JsonLeasePoll = .{ .stop = 8 };
    try std.testing.expectError(error.OriginalJsonLeaseCanceled, Store.jsonGet(&map, collision, poll.control()));
    poll = .{ .stop = 8 };
    try std.testing.expectError(error.OriginalJsonLeaseCanceled, Store.jsonPut(&map, a, collision, @as(u32, 9), poll.control()));
    try std.testing.expectEqual(@as(usize, 1), map.count());
    try std.testing.expectEqual(@as(?u32, 7), map.get(original));
    poll = .{};
    try std.testing.expectEqual(@as(?u32, null), try Store.jsonGet(&map, collision, poll.control()));
    try std.testing.expect(try Store.jsonPut(&map, a, collision, @as(u32, 9), poll.control()));
    try std.testing.expectEqual(@as(usize, 2), map.count());
}

test "SQL native JSON lease interrupted pooled copy releases all unpublished ownership" {
    const a = std.testing.allocator;
    const pool = try Store.JsonPool.create(a);
    defer pool.deinit();
    var text: [16 * 1024]u8 = @splat('x');
    const key: Store.JsonKey = .{ .representation = 1, .hash = std.hash.Wyhash.hash(1, &text), .bytes = &text };
    var poll: JsonLeasePoll = .{ .stop = 4 };
    try std.testing.expectError(error.OriginalJsonLeaseCanceled, pool.acquire(key, .{ .text = &text }, poll.control()));
    try std.testing.expectEqual(@as(usize, 0), pool.bodies.count());
    try std.testing.expectEqual(@as(usize, 0), pool.budget.live);
    poll = .{};
    const payload = try pool.acquire(key, .{ .text = &text }, poll.control());
    try std.testing.expectEqualStrings(&text, payload.text.?);
    pool.release(payload);
}

test "SQL consumer JSON decoding cancels text refills rolls back cache and retries with a new owner" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 96 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    text[0] = '"';
    text[text.len - 1] = '"';
    var source: SharedJsonFixture = .{ .text = text };
    var store = Store.init(a);
    defer store.deinit();
    try source.populate(&store, 1, false);
    var first: JsonLeasePoll = .{ .stop = 12 };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    try std.testing.expectError(error.OriginalJsonLeaseCanceled, store.cellControlled(scratch.allocator(), 0, 0, first.control()));
    try std.testing.expectEqual(@as(usize, 0), store.json_cache.?.budget.live);
    var second: JsonLeasePoll = .{};
    const value = try store.cellControlled(scratch.allocator(), 0, 0, second.control());
    try std.testing.expectEqualStrings(text[1 .. text.len - 1], value.value.string);
    const old_calls = first.calls;
    const current_calls = second.calls;
    // A cached read belongs to this new consumer, not the canceled producer.
    const again = try store.cellControlled(scratch.allocator(), 0, 0, second.control());
    try std.testing.expectEqualStrings(value.value.string, again.value.string);
    try std.testing.expectEqual(old_calls, first.calls);
    try std.testing.expect(second.calls > current_calls);
}

test "SQL consumer JSON decoding cancels frozen byte copies and preserves native scalar tags on retry" {
    const a = std.testing.allocator;
    var source_arena = std.heap.ArenaAllocator.init(a);
    defer source_arena.deinit();
    const sa = source_arena.allocator();
    const large = try sa.alloc(u8, 64 * 1024);
    @memset(large, 'x');
    var object: std.json.ObjectMap = .empty;
    try object.put(sa, "body", .{ .string = large });
    try object.put(sa, "integer", .{ .integer = 9007199254740993 });
    try object.put(sa, "float", .{ .float = 1.25 });
    try object.put(sa, "lexeme", .{ .number_string = "2.00" });
    const spill = @import("spill.zig");
    const wire = try spill.encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(.{ .object = object })}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
    const block = try spill.decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
    defer block.deinit();
    var source: SharedJsonFixture = .{ .text = "", .frozen = (try block.frozenJson(0, 0, false)).? };
    var store = Store.init(a);
    defer store.deinit();
    try source.populate(&store, 1, false);
    var poll: JsonLeasePoll = .{ .stop = 12 };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    try std.testing.expectError(error.OriginalJsonLeaseCanceled, store.cellControlled(scratch.allocator(), 0, 0, poll.control()));
    try std.testing.expectEqual(@as(usize, 0), store.json_cache.?.budget.live);
    poll = .{};
    const result = (try store.cellControlled(scratch.allocator(), 0, 0, poll.control())).value.object;
    try std.testing.expectEqualStrings(large, result.get("body").?.string);
    try std.testing.expectEqual(@as(i64, 9007199254740993), result.get("integer").?.integer);
    try std.testing.expectEqual(@as(f64, 1.25), result.get("float").?.float);
    try std.testing.expectEqualStrings("2.00", result.get("lexeme").?.number_string);
}

test "SQL consumer JSON cache mutex waits preserve the caller error and another owners lock" {
    const a = std.testing.allocator;
    var source: SharedJsonFixture = .{ .text = "{\"x\":1}" };
    var store = Store.init(a);
    defer store.deinit();
    try source.populate(&store, 1, false);
    const cache = store.json_cache.?;
    try std.testing.expect(cache.mutex.tryLock());
    var locked = true;
    defer if (locked) cache.mutex.unlock();
    var poll: JsonLeasePoll = .{ .stop = 6 };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    try std.testing.expectError(error.OriginalJsonLeaseCanceled, store.cellControlled(scratch.allocator(), 0, 0, poll.control()));
    try std.testing.expect(!cache.mutex.tryLock());
    cache.mutex.unlock();
    locked = false;
    poll = .{};
    try std.testing.expectEqualStrings("1", (try store.cellControlled(scratch.allocator(), 0, 0, poll.control())).value.object.get("x").?.number_string);
}

test "SQL consumer JSON busy cache token permits reentrant scratch progress without physical mutex ownership" {
    const a = std.testing.allocator;
    const pool = try Store.JsonPool.create(a);
    defer pool.deinit();
    const text = try a.alloc(u8, 16 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    text[0] = '"';
    text[text.len - 1] = '"';
    var source: SharedJsonFixture = .{ .text = text };
    var first = Store.init(a);
    first.json_pool = pool;
    defer first.deinit();
    try source.populate(&first, 1, false);
    text[1] = 'y';
    var second = Store.init(a);
    second.json_pool = pool;
    defer second.deinit();
    try source.populate(&second, 1, false);
    const cache = &first.columns[0].json_bytes.?.items[0].?.shared.?.cache;
    const Fixture = struct {
        first_cache: *JsonCache,
        other: *Store,
        nested: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            // A checkpoint is never called with the physical cache lock held.
            try std.testing.expect(self.first_cache.mutex.tryLock());
            const decoding = self.first_cache.decoding;
            self.first_cache.mutex.unlock();
            if (decoding and !self.nested) {
                self.nested = true;
                var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
                defer scratch.deinit();
                var poll: JsonLeasePoll = .{};
                const result = try self.other.cellControlled(scratch.allocator(), 0, 0, poll.control());
                try std.testing.expectEqual(@as(u8, 'y'), result.value.string[0]);
            }
        }
    };
    var fixture: Fixture = .{ .first_cache = cache, .other = &second };
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const result = try first.cellControlled(scratch.allocator(), 0, 0, .{ .ptr = &fixture, .checkpoint = Fixture.check });
    try std.testing.expect(fixture.nested);
    try std.testing.expectEqual(@as(u8, 'x'), result.value.string[0]);
}

test "SQL consumer JSON projected selected native columns use the current reader without retaining its authority" {
    const a = std.testing.allocator;
    var source: SharedJsonFixture = .{ .text = "{\"x\":[9007199254740993,1.25]}" };
    var store = Store.init(a);
    defer store.deinit();
    try source.populate(&store, 2, false);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    const Batch = @import("execution_batch.zig").Batch;
    const retained: Batch = .{ .retained = .{ .store = &store, .count = 2 } };
    const projected = try retained.project(sa, 0);
    const selected = try projected.select(sa, &.{1});
    const page: @import("catalog.zig").ColumnPage = .{ .native = .{ .values = &selected, .names = &.{"payload"} }, .selection = &.{0} };
    var canceled: JsonLeasePoll = .{ .stop = 8 };
    try std.testing.expectError(error.OriginalJsonLeaseCanceled, page.cellControlled(sa, 0, "payload", canceled.control()));
    var reader: JsonLeasePoll = .{};
    const result = try page.cellControlled(sa, 0, "payload", reader.control());
    try std.testing.expectEqualStrings("9007199254740993", result.value.object.get("x").?.array.items[0].number_string);
    const stopped_calls = canceled.calls;
    _ = try selected.cellControlled(sa, 0, 0, reader.control());
    try std.testing.expectEqual(stopped_calls, canceled.calls);
    try std.testing.expect(reader.calls > 0);
}

test "SQL consumer JSON representation capabilities do not bypass an opaque readers precise error" {
    const Fixture = struct {
        fn cell(_: *anyopaque, _: A, _: usize, _: usize) !Datum {
            return error.OriginalRowPolicyRejected;
        }
        fn json(_: *anyopaque, _: usize, _: usize) !?[]const u8 {
            return "{\"x\":1}";
        }
    };
    var raw: u8 = 0;
    const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &raw, .read = Fixture.cell, .read_json = Fixture.json, .count = 1, .width = 1 } };
    var poll: JsonLeasePoll = .{};
    try std.testing.expectError(error.OriginalRowPolicyRejected, batch.cellControlled(std.testing.allocator, 0, 0, poll.control()));
}

test "SQL consumer JSON streaming matches complete input escapes exact numbers and validation errors" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    const prefix = "{\"body\":\"";
    const suffix = "\\uD83D\\uDE80\",\"n\":9007199254740993,\"a\":[null,true,1.25,\"λ\"]}";
    const text = try sa.alloc(u8, 4093 + suffix.len);
    @memcpy(text[0..prefix.len], prefix);
    @memset(text[prefix.len..4093], 'x');
    @memcpy(text[4093..], suffix);
    const expected = try std.json.parseFromSliceLeaky(std.json.Value, sa, text, .{ .parse_numbers = false });
    var poll: JsonLeasePoll = .{};
    const actual = try @import("controlled_json_text.zig").parseLeaky(sa, text, poll.control());
    // The DOM must own tokens even when a refill reuses the scanner's buffer.
    @memset(text, '?');
    try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(sa, expected, .{}), try std.json.Stringify.valueAlloc(sa, actual, .{}));
    for ([_][]const u8{ "{\"x\":1} garbage", "{\"x\":1,\"x\":2}", "\"unterminated", "\"\\uD800\"" }) |bad| {
        const wanted = error_case: {
            _ = std.json.parseFromSliceLeaky(std.json.Value, sa, bad, .{ .parse_numbers = false }) catch |err| break :error_case err;
            return error.TestExpectedError;
        };
        try std.testing.expectError(wanted, @import("controlled_json_text.zig").parseLeaky(sa, bad, poll.control()));
    }
}

test "SQL consumer JSON controlled cache and scanner unwind every allocation failure" {
    const Fixture = struct {
        fn run(a: A) !void {
            const text = try a.alloc(u8, 8192);
            defer a.free(text);
            @memset(text, 'x');
            text[0] = '"';
            text[text.len - 1] = '"';
            var source: SharedJsonFixture = .{ .text = text };
            var store = Store.init(a);
            defer store.deinit();
            try source.populate(&store, 1, false);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            var poll: JsonLeasePoll = .{};
            const value = try store.cellControlled(scratch.allocator(), 0, 0, poll.control());
            try std.testing.expectEqualStrings(text[1 .. text.len - 1], value.value.string);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}
