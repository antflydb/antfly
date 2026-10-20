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

//! Exact-size canonical primitive/text array construction. Ingress parsers
//! count shape and payload first, then write cells without a Datum vector.
const std = @import("std");
const arrays = @import("array_value.zig");
const layout = @import("../common/sql_array_layout.zig");
const A = std.mem.Allocator;

pub fn supported(kind: arrays.ElementType) bool {
    return switch (kind) {
        .uuid, .numeric, .jsonb => false,
        else => true,
    };
}

pub const Builder = struct {
    bytes: []u8,
    kind: arrays.ElementType,
    count: usize,
    index: usize = 0,
    non_null: usize = 0,
    payload_at: usize = 0,
    bitmap: usize,
    slots: usize,
    values: usize,
    payload: usize,
    compact: bool,

    pub fn init(a: A, kind: arrays.ElementType, dimensions: []const arrays.Dimension, count: usize, non_null: usize, text_bytes: usize, limits: arrays.Limits) !Builder {
        if (!supported(kind) or count > limits.elements or non_null > count) return error.SqlProgramLimitExceeded;
        const rank: usize = if (count == 0) 0 else dimensions.len;
        const section = try layout.encodedSectionSize(kind, rank, count, non_null);
        const size = try std.math.add(usize, section, if (kind == .text) text_bytes else 0);
        if (size > limits.bytes or size > std.math.maxInt(u32)) return error.SqlProgramLimitExceeded;
        const bytes = try a.alloc(u8, size);
        @memset(bytes, 0);
        bytes[0] = layout.version;
        bytes[1] = @intCast(rank);
        const compact = layout.usesCompact(kind, count, non_null);
        bytes[2] = @intFromBool(compact);
        std.mem.writeInt(u32, bytes[4..8], @intCast(count), .little);
        for (dimensions[0..rank], 0..) |axis, i| {
            std.mem.writeInt(u32, bytes[8 + i * 8 ..][0..4], axis.length, .little);
            std.mem.writeInt(i32, bytes[12 + i * 8 ..][0..4], axis.lower, .little);
        }
        const bitmap = 8 + rank * 8;
        const slots = bitmap + layout.bitmapSize(count);
        return .{ .bytes = bytes, .kind = kind, .count = count, .bitmap = bitmap, .slots = slots, .values = slots + if (compact) layout.checkpointSize(count) else @as(usize, 0), .payload = section, .compact = compact };
    }

    pub fn append(self: *Builder, cell: arrays.Element) !void {
        if (self.index >= self.count) return error.InvalidSqlArrayShape;
        const i = self.index;
        self.index += 1;
        if (self.compact and i % 64 == 0) std.mem.writeInt(u32, self.bytes[self.slots + i / 64 * 4 ..][0..4], @intCast(self.non_null), .little);
        if (cell.sql_null) self.bytes[self.bitmap + i / 8] |= @as(u8, 1) << @intCast(i % 8);
        if (self.kind == .text) {
            std.mem.writeInt(u32, self.bytes[self.slots + i * 4 ..][0..4], @intCast(self.payload_at), .little);
            if (!cell.sql_null) {
                const text = cell.value.string;
                if (text.len > self.bytes.len - self.payload - self.payload_at) return error.InvalidSqlArrayShape;
                @memcpy(self.bytes[self.payload + self.payload_at ..][0..text.len], text);
                self.payload_at += text.len;
                self.non_null += 1;
            }
            return;
        }
        if (cell.sql_null) return;
        const width = layout.width(self.kind);
        const at = self.values + (if (self.compact) self.non_null else i) * width;
        self.non_null += 1;
        const out = self.bytes[at..][0..width];
        switch (self.kind) {
            .boolean => out[0] = @intFromBool(cell.value.bool),
            .int16 => std.mem.writeInt(i16, out[0..2], @intCast(cell.value.integer), .little),
            .int32 => std.mem.writeInt(i32, out[0..4], @intCast(cell.value.integer), .little),
            .int64 => std.mem.writeInt(i64, out[0..8], cell.value.integer, .little),
            .float32 => {
                const f: f32 = @floatCast(cell.value.float);
                std.mem.writeInt(u32, out[0..4], if (std.math.isNan(f)) 0x7fc00000 else @bitCast(f), .little);
            },
            .float64 => std.mem.writeInt(u64, out[0..8], if (std.math.isNan(cell.value.float)) 0x7ff8000000000000 else @bitCast(cell.value.float), .little),
            else => unreachable,
        }
    }

    fn finishHeaders(self: *Builder) !void {
        if (self.index != self.count) return error.InvalidSqlArrayShape;
        if (self.compact) std.mem.writeInt(u32, self.bytes[self.slots + (self.count + 63) / 64 * 4 ..][0..4], @intCast(self.non_null), .little);
        if (self.kind == .text and self.count != 0) {
            std.mem.writeInt(u32, self.bytes[self.slots + self.count * 4 ..][0..4], @intCast(self.payload_at), .little);
            if (self.payload + self.payload_at != self.bytes.len) return error.InvalidSqlArrayShape;
        }
    }
    pub fn finish(self: *Builder, a: A, limits: arrays.Limits) !arrays.Value {
        try self.finishHeaders();
        return @import("array_storage.zig").borrowLeaky(a, self.kind, self.bytes, .{ .values = limits, .wire_bytes = limits.bytes });
    }
};

/// A separate producer type keeps unchecked codecs unchanged. Every append
/// validates under the caller's work owner; proof summaries reuse the format
/// counters instead of adding bookkeeping to each emitted fixed-width cell.
pub const CheckedBuilder = struct {
    state: Builder,
    pub fn init(a: A, kind: arrays.ElementType, dimensions: []const arrays.Dimension, count: usize, non_null: usize, text_bytes: usize, limits: arrays.Limits) !CheckedBuilder {
        return .{ .state = try Builder.init(a, kind, dimensions, count, non_null, text_bytes, limits) };
    }
    pub inline fn appendChecked(self: *CheckedBuilder, cell: arrays.Element, kind: arrays.ElementType, limits: arrays.Limits, work: *arrays.Budget) !void {
        // Keep the immutable schema kind separate from the mutable emitter so
        // scalar admission can specialize without reloading an aliased field.
        if (kind != self.state.kind) return error.SqlTypeMismatch;
        _ = try arrays.Value.initWithBudget(kind, &.{.{ .length = 1 }}, &.{cell}, limits, work);
        try self.state.append(cell);
    }
    pub fn finish(self: *CheckedBuilder, a: A, limits: arrays.Limits) !arrays.Value {
        const state = &self.state;
        try state.finishHeaders();
        // Header/shape checks and the constructor's size promises remain
        // mandatory. The text NULL count scans only its tiny validity bitmap.
        var view = try layout.View.openAuthenticated(state.kind, state.bytes, .{ .bytes = limits.bytes, .elements = limits.elements });
        var present = state.non_null;
        if (state.kind == .text) {
            var nulls: usize = 0;
            for (state.bytes[state.bitmap..][0..layout.bitmapSize(state.count)]) |byte| nulls += @popCount(byte);
            present = state.count - nulls;
        }
        const text_bytes = if (state.kind == .text) state.payload_at else 0;
        const expected = try std.math.add(usize, try layout.encodedSectionSize(state.kind, view.rank, state.count, present), text_bytes);
        if (expected != state.bytes.len or state.compact != layout.usesCompact(state.kind, state.count, present)) return error.InvalidSqlArrayShape;
        view.validation = .{
            .element_work = state.count + text_bytes,
            .non_null = present,
            .binary_wire_bytes = 12 + @as(usize, view.rank) * 8 + state.count * 4 + if (state.kind == .text) text_bytes else present * layout.width(state.kind),
        };
        return @import("array_storage.zig").borrowValidatedLeaky(a, view);
    }
};

/// Region-owned computed results. Fixed-width arrays and size-planned text
/// avoid a Datum vector; small/unsized results keep borrowed-cell semantics.
/// The region owns partial and completed allocations, including unused dense
/// capacity after sparse finalization. No borrowed allocator escapes finish.
pub const ResultBuilder = union(enum) {
    cells: struct { kind: arrays.ElementType, dimensions: []const arrays.Dimension, elements: []arrays.Element, index: usize = 0 },
    fixed: struct { builder: CheckedBuilder, expected_non_null: ?usize, capacity_bytes: usize },

    pub fn usesFixed(kind: arrays.ElementType, count: usize) bool {
        return count >= 32 and supported(kind) and kind != .text;
    }
    pub fn allocationBytes(kind: arrays.ElementType, count: usize, rank: usize) !usize {
        return allocationBytesKnown(kind, count, rank, null);
    }
    /// Counts come from immutable validated native views or producer semantics.
    /// Unknown counts reserve dense staging; known counts reserve exact bytes.
    pub fn allocationBytesKnown(kind: arrays.ElementType, count: usize, rank: usize, non_null: ?usize) !usize {
        return allocationBytesPlanned(kind, count, rank, non_null, null);
    }
    pub fn allocationBytesPlanned(kind: arrays.ElementType, count: usize, rank: usize, non_null: ?usize, text_bytes: ?usize) !usize {
        if (rank > 6 or count > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        if ((rank == 0) != (count == 0)) return error.InvalidSqlArrayShape;
        if (non_null) |present| if (present > count) return error.InvalidSqlArrayShape;
        const metadata = @sizeOf(arrays.Value) + rank * @sizeOf(arrays.Dimension);
        if (usesFixed(kind, count) or (kind == .text and count >= 32 and text_bytes != null)) {
            const section = try layout.encodedSectionSize(kind, rank, count, non_null orelse count);
            const size = std.math.add(usize, section, text_bytes orelse 0) catch return error.SqlProgramLimitExceeded;
            return std.math.add(usize, metadata + @sizeOf(layout.View), size) catch error.SqlProgramLimitExceeded;
        }
        return std.math.add(usize, metadata, std.math.mul(usize, count, @sizeOf(arrays.Element)) catch return error.SqlProgramLimitExceeded) catch error.SqlProgramLimitExceeded;
    }
    pub fn init(a: A, kind: arrays.ElementType, dimensions: []const arrays.Dimension, count: usize, limits: arrays.Limits) !ResultBuilder {
        return initKnown(a, kind, dimensions, count, null, limits);
    }
    pub fn initKnown(a: A, kind: arrays.ElementType, dimensions: []const arrays.Dimension, count: usize, non_null: ?usize, limits: arrays.Limits) !ResultBuilder {
        return initPlanned(a, kind, dimensions, count, non_null, null, limits);
    }
    pub fn initPlanned(a: A, kind: arrays.ElementType, dimensions: []const arrays.Dimension, count: usize, non_null: ?usize, text_bytes: ?usize, limits: arrays.Limits) !ResultBuilder {
        const empty = try arrays.validateShape(dimensions, count, limits);
        const axes = if (empty) &.{} else dimensions;
        const capacity_bytes = try allocationBytesPlanned(kind, count, axes.len, non_null, text_bytes);
        if (capacity_bytes > limits.bytes) return error.SqlProgramLimitExceeded;
        if (usesFixed(kind, count) or (kind == .text and count >= 32 and text_bytes != null)) return .{ .fixed = .{ .builder = try CheckedBuilder.init(a, kind, axes, count, non_null orelse count, text_bytes orelse 0, limits), .expected_non_null = non_null, .capacity_bytes = capacity_bytes } };
        const owned_axes = try a.dupe(arrays.Dimension, axes);
        const elements = try a.alloc(arrays.Element, count);
        return .{ .cells = .{ .kind = kind, .dimensions = owned_axes, .elements = elements } };
    }
    pub fn append(self: *ResultBuilder, element: arrays.Element, work: *arrays.Budget) !void {
        switch (self.*) {
            .cells => |*cells| {
                if (cells.index == cells.elements.len) return error.InvalidSqlArrayShape;
                cells.elements[cells.index] = element;
                cells.index += 1;
            },
            .fixed => |*fixed| {
                try arrays.validateElement(fixed.builder.state.kind, element, work);
                // Reject an inconsistent plan before a packed payload write.
                if (fixed.builder.state.compact and !element.sql_null) if (fixed.expected_non_null) |expected| if (fixed.builder.state.non_null >= expected) return error.InvalidSqlArrayShape;
                try fixed.builder.state.append(element);
            },
        }
    }
    /// Copy immutable, validated native inputs in physical order. Each block
    /// ends at a destination checkpoint; mixed dense/packed layouts copy runs.
    /// Authentication-only inputs retain the checked scalar fallback.
    pub fn appendValue(self: *ResultBuilder, value: arrays.Value, work: *arrays.Budget) !void {
        if (self.* == .fixed) if (value.stored) |view| if (view.validation != null and view.kind == self.fixed.builder.state.kind) {
            _ = try value.knownNonNullCount();
            try self.appendNative(view.*, 0, view.count, view.validation.?.non_null, work);
            return;
        };
        for (0..value.cardinality()) |i| try self.append(value.flatCell(i), work);
    }

    /// Range boundary for transformed runs. Only validated native storage
    /// skips scalar decoding; prepared/authenticated values keep the fallback.
    pub fn appendRange(self: *ResultBuilder, value: arrays.Value, begin: usize, count: usize, work: *arrays.Budget) !void {
        if (begin > value.cardinality() or count > value.cardinality() - begin) return error.SqlArraySubscriptOutOfRange;
        try work.consume(0);
        if (begin == 0 and count == value.cardinality()) return self.appendValue(value, work);
        if (self.* == .fixed) if (value.stored) |view| if (view.validation != null and view.kind == self.fixed.builder.state.kind) {
            _ = try value.knownNonNullCount();
            var present: usize = 0;
            var at: usize = begin;
            while (at < begin + count) {
                const length = @min(64, begin + count - at);
                // Emission below charges these cells once; metadata scans
                // retain bounded cancellation without a second work charge.
                if ((at - begin) % 4096 == 0) try work.poll();
                present += length - @popCount(nullBits(view.*, at, length));
                at += length;
            }
            return self.appendNative(view.*, begin, count, present, work);
        };
        for (begin..begin + count) |i| try self.append(value.flatCell(i), work);
    }

    /// Primitive native conversions keep a physical cursor and visit only
    /// present slots. The scalar codec remains the range/rounding oracle.
    pub fn appendConverted(self: *ResultBuilder, value: arrays.Value, work: *arrays.Budget) !bool {
        return self.appendConvertedWithWork(value, .cast_and_emission, work);
    }
    pub const ConversionWork = enum(u2) { prepaid = 0, emission = 1, cast_and_emission = 2 };
    /// Fused callers charge both logical cast visits before evaluating later
    /// operands. Emit once for concat, or only poll when replacing a cast's
    /// identity result; conversion still polls at each bounded native block.
    pub fn appendConvertedWithWork(self: *ResultBuilder, value: arrays.Value, accounting: ConversionWork, work: *arrays.Budget) !bool {
        if (self.* != .fixed) return false;
        const view = value.stored orelse return false;
        const target = self.fixed.builder.state.kind;
        const casts = @import("builtin_cast.zig");
        if (view.validation == null or !supported(view.kind) or view.kind == .text or !supported(target) or target == .text or !casts.allowed(view.kind, target)) return false;
        _ = try value.knownNonNullCount();
        switch (view.kind) {
            .boolean => try self.convertFrom(bool, view.*, accounting, work),
            .int16 => try self.convertFrom(i16, view.*, accounting, work),
            .int32 => try self.convertFrom(i32, view.*, accounting, work),
            .int64 => try self.convertFrom(i64, view.*, accounting, work),
            .float32 => try self.convertFrom(f32, view.*, accounting, work),
            .float64 => try self.convertFrom(f64, view.*, accounting, work),
            else => unreachable,
        }
        return true;
    }
    fn convertFrom(self: *ResultBuilder, comptime Source: type, view: layout.View, accounting: ConversionWork, work: *arrays.Budget) !void {
        switch (self.fixed.builder.state.kind) {
            .boolean => try self.convertTo(Source, bool, view, accounting, work),
            .int16 => try self.convertTo(Source, i16, view, accounting, work),
            .int32 => try self.convertTo(Source, i32, view, accounting, work),
            .int64 => try self.convertTo(Source, i64, view, accounting, work),
            .float32 => try self.convertTo(Source, f32, view, accounting, work),
            .float64 => try self.convertTo(Source, f64, view, accounting, work),
            else => unreachable,
        }
    }
    fn primitiveKind(comptime T: type) arrays.ElementType {
        return if (T == bool) .boolean else if (T == i16) .int16 else if (T == i32) .int32 else if (T == i64) .int64 else if (T == f32) .float32 else .float64;
    }
    fn converted(comptime Source: type, comptime Target: type, value: Source) !Target {
        if (Source == Target) return value;
        if (Source == bool) {
            if (Target == i32) return @intFromBool(value);
            return error.SqlCannotCoerce;
        }
        if (Target == bool) {
            if (Source == i32) return value != 0;
            return error.SqlCannotCoerce;
        }
        if (Target == f32 or Target == f64) {
            if (Source == f32 or Source == f64) {
                const result: Target = @floatCast(value);
                if (std.math.isFinite(value) and (!std.math.isFinite(result) or (result == 0 and value != 0))) return error.SqlNumericOutOfRange;
                return result;
            }
            return @floatFromInt(value);
        }
        if (Source == f32 or Source == f64) return @intCast(try @import("builtin_cast.zig").floatingInteger(value, primitiveKind(Target)));
        return std.math.cast(Target, value) orelse error.SqlNumericOutOfRange;
    }
    fn convertTo(self: *ResultBuilder, comptime Source: type, comptime Target: type, view: layout.View, accounting: ConversionWork, work: *arrays.Budget) !void {
        const state = &self.fixed.builder.state;
        const present = view.validation.?.non_null;
        if (view.count > state.count - state.index) return error.InvalidSqlArrayShape;
        if (self.fixed.expected_non_null) |expected| if (present > expected -| state.non_null) return error.InvalidSqlArrayShape;
        const source_width = if (Source == bool) 1 else @sizeOf(Source);
        const target_width = if (Target == bool) 1 else @sizeOf(Target);
        var at: usize = 0;
        var physical: usize = 0;
        while (at < view.count) {
            const count = @min(@as(usize, view.count) - at, 64 - state.index % 64);
            // Preserve scalar cast + emission work, including skipped NULLs.
            if (accounting == .prepaid) try work.poll();
            try work.consume(count * @backingInt(accounting));
            const bits = nullBits(view, at, count);
            const mask = if (count == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(count)) - 1;
            var candidates = ~bits & mask;
            writeNulls(state, bits, count);
            var packed_at: usize = 0;
            while (candidates != 0) {
                const bit = @ctz(candidates);
                candidates &= candidates - 1;
                const source_slot = physical + if (view.compact) packed_at else bit;
                const input = view.bytes[view.payload_start + source_slot * source_width ..][0..source_width];
                const number: Source = if (Source == bool) input[0] != 0 else if (Source == f32 or Source == f64) @bitCast(std.mem.readInt(@Int(.unsigned, @bitSizeOf(Source)), input[0..source_width], .little)) else std.mem.readInt(Source, input[0..source_width], .little);
                const result = try converted(Source, Target, number);
                const target_slot = if (state.compact) state.non_null + packed_at else state.index + bit;
                const output = state.bytes[state.values + target_slot * target_width ..][0..target_width];
                if (Target == bool) output[0] = @intFromBool(result) else if (Target == f32) std.mem.writeInt(u32, output[0..4], if (std.math.isNan(result)) 0x7fc00000 else @bitCast(result), .little) else if (Target == f64) std.mem.writeInt(u64, output[0..8], if (std.math.isNan(result)) 0x7ff8000000000000 else @bitCast(result), .little) else std.mem.writeInt(Target, output[0..target_width], result, .little);
                packed_at += 1;
            }
            physical += if (view.compact) packed_at else count;
            state.index += count;
            state.non_null += packed_at;
            at += count;
        }
    }

    const CopyWork = enum { scheduled, payload, prepaid };
    fn copyBytes(destination: []u8, source: []const u8, work: *arrays.Budget, accounting: CopyWork) !void {
        if (destination.len != source.len) return error.InvalidSqlArrayShape;
        var at: usize = 0;
        while (at < source.len) {
            const end = at + @min(4096, source.len - at);
            switch (accounting) {
                .payload => try work.consume(end - at),
                .prepaid => try work.poll(),
                // Native blocks already charge at most 64 cells before each
                // short copy, preserving the owner's normal polling cadence.
                .scheduled => try work.consume(0),
            }
            @memcpy(destination[at..end], source[at..end]);
            at = end;
        }
    }

    /// Internal native-plan primitive: caller bounds count to 64 and the
    /// requested range to the pinned, validated view before reaching here.
    pub fn nullBits(view: layout.View, begin: usize, count: usize) u64 {
        if (count == 64) {
            const offset = view.bitmap_start + begin / 8;
            const word = std.mem.readInt(u64, view.bytes[offset..][0..8], .little);
            const shift: u6 = @intCast(begin % 8);
            if (shift == 0) return word;
            return (word >> shift) | (@as(u64, view.bytes[offset + 8]) << @intCast(64 - @as(usize, shift)));
        }
        var bits: u64 = 0;
        var at: usize = 0;
        while (at < count) {
            const index = begin + at;
            const take = @min(count - at, 8 - index % 8);
            const mask: u8 = @intCast((@as(u16, 1) << @intCast(take)) - 1);
            bits |= @as(u64, (view.bytes[view.bitmap_start + index / 8] >> @intCast(index % 8)) & mask) << @intCast(at);
            at += take;
        }
        return bits;
    }

    fn writeNulls(state: *Builder, bits: u64, count: usize) void {
        if (state.compact and state.index % 64 == 0) std.mem.writeInt(u32, state.bytes[state.slots + state.index / 64 * 4 ..][0..4], @intCast(state.non_null), .little);
        if (count == 64 and state.index % 8 == 0) {
            const destination = state.bytes[state.bitmap + state.index / 8 ..][0..8];
            std.mem.writeInt(u64, destination, std.mem.readInt(u64, destination, .little) | bits, .little);
            return;
        }
        var bit_at: usize = 0;
        while (bit_at < count) {
            const index = state.index + bit_at;
            const take = @min(count - bit_at, 8 - index % 8);
            const selected = (bits >> @intCast(bit_at)) & ((@as(u64, 1) << @intCast(take)) - 1);
            state.bytes[state.bitmap + index / 8] |= @as(u8, @intCast(selected)) << @intCast(index % 8);
            bit_at += take;
        }
    }

    fn appendNative(self: *ResultBuilder, view: layout.View, begin: usize, range_length: usize, present: usize, work: *arrays.Budget) !void {
        const fixed = &self.fixed;
        const state = &fixed.builder.state;
        if (range_length > state.count - state.index) return error.InvalidSqlArrayShape;
        if (fixed.expected_non_null) |expected| if (present > expected -| state.non_null) return error.InvalidSqlArrayShape;
        const text = view.kind == .text;
        if (range_length == 0) return work.consume(0);
        if (text) {
            const first = std.mem.readInt(u32, view.bytes[view.slots_start + begin * 4 ..][0..4], .little);
            const last = std.mem.readInt(u32, view.bytes[view.slots_start + (begin + range_length) * 4 ..][0..4], .little);
            if (last - first > state.bytes.len - state.payload - state.payload_at) return error.InvalidSqlArrayShape;
        }
        const width = layout.width(view.kind);
        // Fully present dense spans need neither bitmap writes nor rank
        // checkpoints. Copy payload in bounded chunks under the same work owner.
        if (!text and !view.compact and !state.compact and present == range_length) {
            try work.consume(range_length);
            try copyBytes(state.bytes[state.values + state.index * width ..][0 .. range_length * width], view.bytes[view.payload_start + begin * width ..][0 .. range_length * width], work, .prepaid);
            state.index += range_length;
            state.non_null += present;
            return;
        }
        var source_index: usize = begin;
        var source_physical: usize = if (view.compact) std.mem.readInt(u32, view.bytes[view.slots_start + begin / 64 * 4 ..][0..4], .little) + begin % 64 - @popCount(nullBits(view, begin / 64 * 64, begin % 64)) else begin;
        if (!text and view.compact == state.compact) {
            // Matching physical layouts have one contiguous payload, even
            // across NULLs. Rebuild only destination validity/checkpoints.
            try work.consume(range_length);
            const slots = if (view.compact) present else range_length;
            const destination = if (state.compact) state.non_null else state.index;
            try copyBytes(state.bytes[state.values + destination * width ..][0 .. slots * width], view.bytes[view.payload_start + source_physical * width ..][0 .. slots * width], work, .prepaid);
            while (source_index < begin + range_length) {
                if ((source_index - begin) % 4096 < 64) try work.poll();
                const count = @min(begin + range_length - source_index, 64 - state.index % 64);
                const bits = nullBits(view, source_index, count);
                writeNulls(state, bits, count);
                state.index += count;
                state.non_null += count - @popCount(bits);
                source_index += count;
            }
            return;
        }
        while (source_index < begin + range_length) {
            const count = @min(begin + range_length - source_index, 64 - state.index % 64);
            try work.consume(count);
            const bits = nullBits(view, source_index, count);
            const non_null = count - @popCount(bits);
            writeNulls(state, bits, count);
            if (text) {
                const base = std.mem.readInt(u32, view.bytes[view.slots_start + source_index * 4 ..][0..4], .little);
                const end = std.mem.readInt(u32, view.bytes[view.slots_start + (source_index + count) * 4 ..][0..4], .little);
                for (0..count) |i| {
                    const offset = std.mem.readInt(u32, view.bytes[view.slots_start + (source_index + i) * 4 ..][0..4], .little);
                    std.mem.writeInt(u32, state.bytes[state.slots + (state.index + i) * 4 ..][0..4], @intCast(state.payload_at + offset - base), .little);
                }
                const length = end - base;
                try copyBytes(state.bytes[state.payload + state.payload_at ..][0..length], view.bytes[view.payload_start + base ..][0..length], work, .payload);
                state.payload_at += length;
            } else {
                // Consecutive present runs permit memcpy even when one side
                // has NULL slots and the other stores only present values.
                var i: usize = 0;
                var packed_at: usize = 0;
                while (i < count) {
                    if (bits & (@as(u64, 1) << @intCast(i)) != 0) {
                        i += 1;
                        continue;
                    }
                    const run_begin = i;
                    while (i < count and bits & (@as(u64, 1) << @intCast(i)) == 0) : (i += 1) {}
                    const length = (i - run_begin) * width;
                    const destination = if (state.compact) state.non_null + packed_at else state.index + run_begin;
                    const source = source_physical + if (view.compact) packed_at else run_begin;
                    try copyBytes(state.bytes[state.values + destination * width ..][0..length], view.bytes[view.payload_start + source * width ..][0..length], work, .scheduled);
                    packed_at += i - run_begin;
                }
            }
            state.index += count;
            state.non_null += non_null;
            source_index += count;
            source_physical += if (view.compact) non_null else count;
        }
    }

    pub fn finish(self: *ResultBuilder, a: A, limits: arrays.Limits, work: *arrays.Budget) !arrays.Value {
        try work.consume(0);
        return switch (self.*) {
            .cells => |cells| blk: {
                if (cells.index != cells.elements.len) return error.InvalidSqlArrayShape;
                break :blk try arrays.Value.initWithBudget(cells.kind, cells.dimensions, cells.elements, limits, work);
            },
            .fixed => |*fixed| blk: {
                const state = &fixed.builder.state;
                if (state.index != state.count) return error.InvalidSqlArrayShape;
                if (fixed.capacity_bytes > limits.bytes) return error.SqlProgramLimitExceeded;
                if (fixed.expected_non_null) |expected| if (state.non_null != expected) return error.InvalidSqlArrayShape;
                if (!state.compact and layout.usesCompact(state.kind, state.count, state.non_null)) {
                    // Pack into the dense prefix first, then shift right for
                    // checkpoints. Forward packing cannot overwrite unread
                    // slots; the final overlapping move proceeds backwards.
                    const width = layout.width(state.kind);
                    var present: usize = 0;
                    for (0..state.count) |i| {
                        if (i % 1024 == 0) try work.consume(@min(1024, state.count - i));
                        const is_null = state.bytes[state.bitmap + i / 8] & (@as(u8, 1) << @intCast(i % 8)) != 0;
                        if (is_null) continue;
                        if (present != i) @memcpy(state.bytes[state.values + present * width ..][0..width], state.bytes[state.values + i * width ..][0..width]);
                        present += 1;
                    }
                    const directory = layout.checkpointSize(state.count);
                    const packed_bytes = present * width;
                    std.mem.copyBackwards(u8, state.bytes[state.values + directory ..][0..packed_bytes], state.bytes[state.values..][0..packed_bytes]);
                    var prefix: u32 = 0;
                    var begin: usize = 0;
                    while (begin < state.count) : (begin += 64) {
                        try work.consume(1);
                        std.mem.writeInt(u32, state.bytes[state.slots + begin / 64 * 4 ..][0..4], prefix, .little);
                        const end = @min(state.count, begin + 64);
                        var nulls: usize = 0;
                        for (state.bytes[state.bitmap + begin / 8 ..][0 .. (end - begin + 7) / 8]) |byte| nulls += @popCount(byte);
                        prefix += @intCast(end - begin - nulls);
                    }
                    const size = state.values + directory + packed_bytes;
                    _ = a.resize(state.bytes, size);
                    state.bytes = state.bytes[0..size];
                    state.values += directory;
                    state.compact = true;
                    state.bytes[2] = 1;
                }
                var value = try fixed.builder.finish(a, limits);
                value.prepared_footprint = fixed.capacity_bytes;
                break :blk value;
            },
        };
    }
};

test "SQL compact ingress preserves codecs shape nulls and isolated ownership" {
    const a = std.testing.allocator;
    const text = @import("array_text.zig");
    const binary = @import("array_binary.zig");
    for ([_]struct { kind: arrays.ElementType, value: []const u8 }{
        .{ .kind = .int64, .value = "[-1:1]={9007199254740993,NULL,-7}" },
        .{ .kind = .int32, .value = "[0:1][2:3]={{1,NULL},{3,-2147483648}}" },
        .{ .kind = .int16, .value = "{-32768,NULL,32767}" },
        .{ .kind = .boolean, .value = "{t,NULL,f}" },
        .{ .kind = .float32, .value = "{1.25,NaN,Infinity,-Infinity,NULL}" },
        .{ .kind = .float64, .value = "{1.25,NaN,Infinity,-Infinity,NULL}" },
        .{ .kind = .text, .value = "[-1:1]={\"é\\\"x\",NULL,\"retained\"}" },
        .{ .kind = .text, .value = "{\"\",NULL,\"NULL\"}" },
        .{ .kind = .int64, .value = "{}" },
        .{ .kind = .int64, .value = "{NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,1}" },
    }) |case| {
        var original = try text.decode(a, case.kind, case.value, .{});
        defer original.deinit();
        var wire: std.Io.Writer.Allocating = .init(a);
        defer wire.deinit();
        try binary.encode(original.value, &wire.writer, .{});
        for ([_]bool{ false, true }) |is_binary| {
            var owner = std.heap.ArenaAllocator.init(a);
            defer owner.deinit();
            const value = if (is_binary) (try binary.decodeLeaky(owner.allocator(), case.kind, wire.written(), .{ .compact = true })).value else (try text.decodeLeaky(owner.allocator(), case.kind, case.value, .{ .compact = true })).value;
            try std.testing.expect(value.stored != null and value.elements.len == 0);
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try original.value.compare(value, &work));
            const stored = try @import("array_storage.zig").encodeAlloc(a, value, .{});
            defer a.free(stored);
            try std.testing.expectEqualSlices(u8, value.stored.?.bytes, stored);
            var frame = try @import("parameter_frame.zig").Frame.prepare(a, &.{.{ .kind = .array, .element_type = case.kind }}, &.{.{ .datum = .typedArray(&value) }}, .{});
            defer frame.deinit();
            _ = owner.reset(.free_all);
            try std.testing.expect(frame.values[0].array.?.stored != null);
            try std.testing.expectEqual(std.math.Order.eq, try original.value.compare(frame.values[0].array.?.*, &work));
        }
    }
}

test "SQL compact ingress rejects truncation and unwinds allocation failures" {
    const Probe = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const value = (try @import("array_text.zig").decodeLeaky(arena.allocator(), .text, "[-1:1]={\"é\\\"x\",NULL,\"retained\"}", .{ .compact = true })).value;
            var wire: std.Io.Writer.Allocating = .init(a);
            defer wire.deinit();
            @import("array_binary.zig").encode(value, &wire.writer, .{}) catch |err| return if (err == error.WriteFailed) error.OutOfMemory else err;
            const copied = (try @import("array_binary.zig").decodeLeaky(arena.allocator(), .text, wire.written(), .{ .compact = true })).value;
            var work: arrays.Budget = .{};
            try std.testing.expectEqual(std.math.Order.eq, try value.compare(copied, &work));
            for (0..wire.written().len) |end| {
                if (@import("array_binary.zig").decodeLeaky(arena.allocator(), .text, wire.written()[0..end], .{ .compact = true })) |_| return error.ExpectedInvalidBinary else |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => {},
                }
            }
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "SQL compact validation proof preserves quota work cancellation and binary wire size" {
    const a = std.testing.allocator;
    const binary = @import("array_binary.zig");
    for ([_]struct { kind: arrays.ElementType, text: []const u8 }{
        .{ .kind = .text, .text = "[-1:1]={\"é\\\"x\",NULL,\"kept\"}" },
        .{ .kind = .int64, .text = "{1,NULL,9007199254740993}" },
        .{ .kind = .float32, .text = "{NaN,Infinity,NULL,1.25}" },
        .{ .kind = .boolean, .text = "{t,NULL,f}" },
        .{ .kind = .int16, .text = "{}" },
    }) |case| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const value = (try @import("array_text.zig").decodeLeaky(arena.allocator(), case.kind, case.text, .{ .compact = true })).value;
        const proof = value.stored.?.validation.?;
        var copy = value.stored.?.*;
        copy.validation = null;
        var fallback = value;
        fallback.stored = &copy;
        var fast_work: arrays.Budget = .{ .remaining = 10000 };
        var slow_work = fast_work;
        _ = try value.validateWithBudget(.{}, &fast_work);
        _ = try fallback.validateWithBudget(.{}, &slow_work);
        try std.testing.expectEqual(slow_work.remaining, fast_work.remaining);
        if (proof.element_work > 0) {
            var denied: arrays.Budget = .{ .remaining = proof.element_work - 1 };
            try std.testing.expectError(error.SqlProgramLimitExceeded, value.validateWithBudget(.{}, &denied));
        }
        const Cancel = struct {
            fn poll(_: ?*anyopaque) !void {
                return error.Canceled;
            }
        };
        var parent: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Cancel.poll };
        var canceled: arrays.Budget = .{ .remaining = 10000, .shared = &parent };
        try std.testing.expectError(error.Canceled, value.validateWithBudget(.{}, &canceled));
        var fast: std.Io.Writer.Allocating = .init(a);
        defer fast.deinit();
        var slow: std.Io.Writer.Allocating = .init(a);
        defer slow.deinit();
        try binary.encode(value, &fast.writer, .{});
        try binary.encode(fallback, &slow.writer, .{});
        try std.testing.expectEqualSlices(u8, slow.written(), fast.written());
        try std.testing.expectEqual(proof.binary_wire_bytes, fast.written().len);
        var denied_output: std.Io.Writer = .fixed(&.{});
        try std.testing.expectError(error.SqlProgramLimitExceeded, binary.encode(value, &denied_output, .{ .wire_bytes = proof.binary_wire_bytes - 1 }));
        try std.testing.expectEqual(@as(usize, 0), denied_output.end);
        const authenticated = try layout.View.openAuthenticated(case.kind, value.stored.?.bytes, .{});
        try std.testing.expect(authenticated.validation == null);
    }
}

test "SQL checked array builders publish exact proofs and preserve unchecked validation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]struct { kind: arrays.ElementType, text: []const u8 }{
        .{ .kind = .int64, .text = "{9007199254740993,NULL,-7}" },
        .{ .kind = .float32, .text = "{NaN,NULL,Infinity,1.25}" },
        .{ .kind = .boolean, .text = "{t,NULL,f}" },
        .{ .kind = .text, .text = "{hello,NULL,world}" },
        .{ .kind = .int16, .text = "{}" },
    }) |case| {
        const original = (try @import("array_text.zig").decodeLeaky(a, case.kind, case.text, .{})).value;
        var present: usize = 0;
        var text_bytes: usize = 0;
        for (original.elements) |cell| if (!cell.sql_null) {
            present += 1;
            if (case.kind == .text) text_bytes += cell.value.string.len;
        };
        var builder = try CheckedBuilder.init(a, case.kind, original.dimensions, original.cardinality(), present, text_bytes, .{});
        var work: arrays.Budget = .{};
        for (original.elements) |cell| try builder.appendChecked(cell, case.kind, .{}, &work);
        const result = try builder.finish(a, .{});
        const scanned = try @import("array_storage.zig").validateCanonical(a, case.kind, result.stored.?.bytes, .{});
        try std.testing.expectEqual(scanned.validation.?, result.stored.?.validation.?);
        try std.testing.expectEqualSlices(u8, try @import("array_storage.zig").encodeAlloc(a, original, .{}), result.stored.?.bytes);
    }
    var wrong_kind = try CheckedBuilder.init(a, .int64, &.{.{ .length = 1 }}, 1, 1, 0, .{});
    var kind_work: arrays.Budget = .{};
    try std.testing.expectError(error.SqlTypeMismatch, wrong_kind.appendChecked(.json(.{ .bool = true }), .boolean, .{}, &kind_work));
    try std.testing.expectEqual(@as(usize, 0), wrong_kind.state.index);
    var dense_hint = try CheckedBuilder.init(a, .int64, &.{.{ .length = 65 }}, 65, 65, 0, .{});
    var dense_work: arrays.Budget = .{};
    for (0..65) |_| try dense_hint.appendChecked(.{}, .int64, .{}, &dense_work);
    try std.testing.expectError(error.InvalidSqlArrayShape, dense_hint.finish(a, .{}));
    var wrong_hint = try CheckedBuilder.init(a, .int64, &.{.{ .length = 65 }}, 65, 2, 0, .{});
    var hint_work: arrays.Budget = .{};
    for (0..65) |_| try wrong_hint.appendChecked(.{}, .int64, .{}, &hint_work);
    try std.testing.expectError(error.NonCanonicalSqlArrayStorage, wrong_hint.finish(a, .{}));
    var invalid = try Builder.init(a, .text, &.{.{ .length = 1 }}, 1, 1, 1, .{});
    try invalid.append(.json(.{ .string = "\x00" }));
    try std.testing.expectError(error.SqlInvalidTextEncoding, invalid.finish(a, .{}));
}

test "SQL computed array builder matches canonical storage and binary wire across widths shapes and null densities" {
    const a = std.testing.allocator;
    const binary = @import("array_binary.zig");
    for ([_]arrays.ElementType{ .boolean, .int16, .int32, .int64, .float32, .float64 }) |kind| {
        for ([_]usize{ 0, 1, 31, 32, 33, 63, 64, 65, 129, 1024 }) |count| {
            for (0..4) |density| {
                var arena = std.heap.ArenaAllocator.init(a);
                defer arena.deinit();
                const region = arena.allocator();
                const dimensions: [1]arrays.Dimension = .{.{ .length = @intCast(count), .lower = -7 }};
                const axes = dimensions[0..@intFromBool(count != 0)];
                const cells = try region.alloc(arrays.Element, count);
                var present: usize = 0;
                for (cells, 0..) |*cell, i| {
                    const missing = switch (density) {
                        0 => false,
                        1 => i % 16 == 0,
                        2 => i % 16 != 0,
                        else => true,
                    };
                    cell.* = if (missing) .{} else arrays.Element.json(switch (kind) {
                        .boolean => .{ .bool = i % 2 == 0 },
                        .float32, .float64 => .{ .float = switch (i % 8) {
                            0 => -0.0,
                            1 => std.math.inf(f64),
                            2 => -std.math.inf(f64),
                            3 => std.math.nan(f64),
                            else => @floatFromInt(i),
                        } },
                        else => .{ .integer = @intCast(i) },
                    });
                    present += @intFromBool(!cell.sql_null);
                }
                for ([_]?usize{ null, present }) |known| {
                    var output = try ResultBuilder.initKnown(region, kind, axes, count, known, .{});
                    var work: arrays.Budget = .{};
                    for (cells) |cell| try output.append(cell, &work);
                    const value = try output.finish(region, .{}, &work);
                    try std.testing.expectEqual(ResultBuilder.usesFixed(kind, count), value.stored != null);
                    if (value.stored) |view| {
                        try std.testing.expect(view.validation != null);
                        _ = try layout.View.open(kind, view.bytes, .{});
                        const source = try arrays.Value.init(kind, axes, cells, .{});
                        const canonical = try @import("array_storage.zig").encodeAlloc(region, source, .{});
                        try std.testing.expectEqualSlices(u8, canonical, view.bytes);
                    }
                    const source = try arrays.Value.init(kind, axes, cells, .{});
                    var expected: std.Io.Writer.Allocating = .init(a);
                    defer expected.deinit();
                    var actual: std.Io.Writer.Allocating = .init(a);
                    defer actual.deinit();
                    try binary.encode(source, &expected.writer, .{});
                    try binary.encode(value, &actual.writer, .{});
                    try std.testing.expectEqualSlices(u8, expected.written(), actual.written());
                }
            }
        }
    }
}

test "SQL computed array builder unwinds faults without resize and retains shared cancellation quotas" {
    const Fixture = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            var nr: @import("../storage/lite/test_allocator.zig").NoResizeAllocator = .{ .backing = arena.allocator() };
            const region = nr.allocator();
            var output = try ResultBuilder.init(region, .int64, &.{ .{ .length = 3, .lower = -3 }, .{ .length = 43, .lower = 7 } }, 129, .{});
            var work: arrays.Budget = .{};
            for (0..129) |i| try output.append(if (i % 64 == 0) .json(.{ .integer = @intCast(i) }) else .{}, &work);
            const value = try output.finish(region, .{}, &work);
            const view = value.stored.?;
            try std.testing.expectEqual(@as(usize, 3), view.validation.?.non_null);
            try std.testing.expectEqual(@as(i32, -3), value.dimensions[0].lower);
            try std.testing.expectEqual(@as(i32, 7), value.dimensions[1].lower);
            _ = try layout.View.open(.int64, view.bytes, .{});
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const region = arena.allocator();
    try std.testing.expectError(error.SqlProgramLimitExceeded, ResultBuilder.init(region, .int64, &.{.{ .length = 64 }}, 64, .{ .elements = 63 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, ResultBuilder.init(region, .int64, &.{.{ .length = 64 }}, 64, .{ .bytes = 128 }));
    try std.testing.expectError(error.InvalidSqlArrayShape, ResultBuilder.init(region, .int64, &.{.{ .length = 63 }}, 64, .{}));
    var output = try ResultBuilder.init(region, .int64, &.{.{ .length = 64 }}, 64, .{});
    var work: arrays.Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, output.append(.{}, &work));
    work = .{};
    try std.testing.expectError(error.SqlTypeMismatch, output.append(.json(.{ .string = "bad" }), &work));
    for (0..64) |_| try output.append(.{}, &work);
    const Cancel = struct {
        fn poll(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    var parent: @import("numeric_value.zig").Context = .{ .alloc = region, .checkpoint = Cancel.poll };
    work = .{ .shared = &parent };
    try std.testing.expectError(error.Canceled, output.finish(region, .{}, &work));
    // Cancel during sparse finalization too, after append validation succeeded.
    parent = .{ .alloc = region, .checkpoint = Cancel.poll, .since_poll = 0 };
    work = .{ .shared = &parent };
    var larger = try ResultBuilder.init(region, .int64, &.{.{ .length = 1024 }}, 1024, .{});
    var local: arrays.Budget = .{};
    for (0..1024) |_| try larger.append(.{}, &local);
    try std.testing.expectError(error.Canceled, larger.finish(region, .{}, &work));
}

test "SQL computed array builder direct preparation and binary output measurement" {
    const a = std.testing.allocator;
    const cells = try a.alloc(arrays.Element, 65536);
    defer a.free(cells);
    for (cells, 0..) |*cell, i| cell.* = if (i % 16 == 0) .{} else .json(.{ .integer = @intCast(i) });
    const source = try arrays.Value.init(.int64, &.{.{ .length = 65536, .lower = -7 }}, cells, .{});
    for (0..2) |mode| {
        var counter: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = a };
        {
            var arena = std.heap.ArenaAllocator.init(counter.allocator());
            defer arena.deinit();
            const region = arena.allocator();
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            const value = if (mode == 0) (try @import("operators.zig").cloneDatum(region, .typedArray(&source))).array.?.* else compact: {
                var output = try ResultBuilder.init(region, .int64, source.dimensions, source.cardinality(), .{});
                var work: arrays.Budget = .{};
                for (cells) |cell| try output.append(cell, &work);
                break :compact try output.finish(region, .{}, &work);
            };
            const prepare_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
            const peak = counter.peak;
            const encode_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            var discard: std.Io.Writer.Discarding = .init(&.{});
            for (0..20) |_| try @import("array_binary.zig").encode(value, &discard.writer, .{});
            const encode_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - encode_start;
            std.debug.print("SQL_COMPUTED_ARRAY direct={} elements=65536 prepare_ns={} retained={} peak={} encode20_ns={} wire_bytes={}\n", .{ mode == 1, prepare_ns, try value.retainedBytes(), peak, encode_ns, discard.count });
        }
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
}

test "SQL sparse native array plans validate counts before writes and publication" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const region = arena.allocator();
    var work: arrays.Budget = .{};
    try std.testing.expectError(error.InvalidSqlArrayShape, ResultBuilder.initKnown(std.testing.failing_allocator, .int64, &.{.{ .length = 64 }}, 64, 65, .{}));
    var zero = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 64 }}, 64, 0, .{});
    try std.testing.expectError(error.InvalidSqlArrayShape, zero.append(.json(.{ .integer = 7 }), &work));
    for (0..64) |_| try zero.append(.{}, &work);
    const zero_value = try zero.finish(region, .{}, &work);
    _ = try layout.View.open(.int64, zero_value.stored.?.bytes, .{});
    var one = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 64 }}, 64, 1, .{});
    for (0..64) |_| try one.append(.{}, &work);
    try std.testing.expectError(error.InvalidSqlArrayShape, one.finish(region, .{}, &work));
    // A nearly dense plan can use dense storage; publication must still
    // reject a count mismatch even when no packed write could overflow.
    var dense = try ResultBuilder.initKnown(region, .boolean, &.{.{ .length = 64 }}, 64, 63, .{});
    for (0..64) |_| try dense.append(.json(.{ .bool = true }), &work);
    try std.testing.expectError(error.InvalidSqlArrayShape, dense.finish(region, .{}, &work));
    var packed_result = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 64 }}, 64, 1, .{});
    try packed_result.append(.json(.{ .integer = 7 }), &work);
    try std.testing.expectError(error.InvalidSqlArrayShape, packed_result.append(.json(.{ .integer = 9 }), &work));
    for (1..64) |_| try packed_result.append(.{}, &work);
    const exact = try ResultBuilder.allocationBytesKnown(.int64, 64, 1, 1);
    try std.testing.expectError(error.SqlProgramLimitExceeded, packed_result.finish(region, .{ .bytes = exact - 1 }, &work));
    const value = try packed_result.finish(region, .{ .bytes = exact }, &work);
    try std.testing.expectEqual(@as(i64, 7), value.flatCell(0).value.integer);
    try std.testing.expect(value.flatCell(1).sql_null);
    _ = try layout.View.open(.int64, value.stored.?.bytes, .{});
}

test "SQL sparse native array proofs and construction admission survive ownership copies and faults" {
    const Fixture = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const region = arena.allocator();
            var output = try ResultBuilder.init(region, .int64, &.{.{ .length = 129, .lower = -7 }}, 129, .{});
            var work: arrays.Budget = .{};
            for (0..129) |_| try output.append(.{}, &work);
            const value = try output.finish(region, .{}, &work);
            try std.testing.expect(try value.admissionBytes() > try value.retainedBytes());
            try std.testing.expectEqual(@as(?usize, 0), try value.knownNonNullCount());
            const copy = (try @import("operators.zig").cloneDatum(region, .typedArray(&value))).array.?;
            try std.testing.expectEqual(try copy.retainedBytes(), try copy.admissionBytes());
            var unproven_view = value.stored.?.*;
            unproven_view.validation = null;
            var unproven = value;
            unproven.stored = &unproven_view;
            try std.testing.expectEqual(@as(?usize, null), try unproven.knownNonNullCount());
            var wrong_shape = value;
            wrong_shape.dimensions = &.{.{ .length = 129, .lower = 1 }};
            try std.testing.expectError(error.InvalidSqlArrayShape, wrong_shape.knownNonNullCount());
            var wrong_kind = value;
            wrong_kind.element_type = .float64;
            try std.testing.expectError(error.InvalidSqlArrayShape, wrong_kind.knownNonNullCount());
            // Exact native plans retain no stack allocator or source payload.
            var result = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 129, .lower = -7 }}, 129, try value.knownNonNullCount(), .{});
            for (0..129) |i| try result.append(value.flatCell(i), &work);
            const exact = try result.finish(region, .{}, &work);
            try std.testing.expectEqual(try exact.retainedBytes(), try exact.admissionBytes());
            var denied: arrays.Budget = .{ .remaining = 0 };
            var canceled = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 64 }}, 64, 0, .{});
            try std.testing.expectError(error.SqlProgramLimitExceeded, canceled.append(.{}, &denied));
            const Cancel = struct {
                fn poll(_: ?*anyopaque) !void {
                    return error.Canceled;
                }
            };
            var parent: @import("numeric_value.zig").Context = .{ .alloc = region, .checkpoint = Cancel.poll };
            denied = .{ .shared = &parent };
            try std.testing.expectError(error.Canceled, canceled.append(.{}, &denied));
        }
    };
    try Fixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL sparse native array construction peak and admission measurement" {
    const a = std.testing.allocator;
    var source_arena = std.heap.ArenaAllocator.init(a);
    defer source_arena.deinit();
    for (0..3) |density| {
        const source_count = 32768;
        const non_null: usize = switch (density) {
            0 => 0,
            1 => source_count / 16,
            else => source_count,
        };
        var source_builder = try CheckedBuilder.init(source_arena.allocator(), .int64, &.{.{ .length = source_count, .lower = -7 }}, source_count, non_null, 0, .{});
        var source_work: arrays.Budget = .{};
        for (0..source_count) |i| try source_builder.appendChecked(if (density == 2 or (density == 1 and i % 16 == 0)) .json(.{ .integer = @intCast(i) }) else .{}, .int64, .{}, &source_work);
        const source = try source_builder.finish(source_arena.allocator(), .{});
        var unproven_view = source.stored.?.*;
        unproven_view.validation = null;
        var unproven = source;
        unproven.stored = &unproven_view;
        var oracle: ?[]u8 = null;
        defer if (oracle) |bytes| a.free(bytes);
        for ([_]usize{ 2 * 1024 * 1024, 64 * 1024 }) |limit| {
            if (limit == 64 * 1024 and density != 0) continue;
            for ([_]arrays.Value{ unproven, source }, 0..) |input, mode| {
                var counter: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = limit };
                {
                    var arena = std.heap.ArenaAllocator.init(counter.allocator());
                    defer arena.deinit();
                    var work: arrays.Budget = .{};
                    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
                    const result = input.concatenate(arena.allocator(), input, .{ .bytes = limit }, &work);
                    const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
                    if (limit == 64 * 1024 and mode == 0) {
                        try std.testing.expectError(error.SqlProgramLimitExceeded, result);
                    } else {
                        const value = try result;
                        if (oracle) |bytes| try std.testing.expectEqualSlices(u8, bytes, value.stored.?.bytes) else oracle = try a.dupe(u8, value.stored.?.bytes);
                        std.debug.print("SQL_SPARSE_NATIVE density={} known={} limit={} peak={} retained={} admission={} prepare_ns={} identical_canonical=true\n", .{ density, mode == 1, limit, counter.peak, try value.retainedBytes(), try value.admissionBytes(), elapsed });
                    }
                }
                try std.testing.expectEqual(@as(usize, 0), counter.live);
            }
        }
    }
}

test "SQL native span construction preserves mixed layouts offsets ownership and canonical wire" {
    const a = std.testing.allocator;
    for ([_]arrays.ElementType{ .boolean, .int16, .int32, .int64, .float32, .float64, .text }) |kind| {
        for ([_]usize{ 1, 7, 31, 63, 65, 129 }) |count| {
            for (0..3) |density| {
                for ([_]usize{ 0, 1, 7, 63 }) |prefix| {
                    var arena = std.heap.ArenaAllocator.init(a);
                    defer arena.deinit();
                    var source_arena = std.heap.ArenaAllocator.init(a);
                    defer source_arena.deinit();
                    const region = arena.allocator();
                    var work: arrays.Budget = .{ .remaining = 1_000_000 };
                    var sources: [2]arrays.Value = undefined;
                    var present: usize = 0;
                    var text_bytes: usize = 0;
                    for (&sources, 0..) |*source, which| {
                        const cells = try source_arena.allocator().alloc(arrays.Element, count);
                        var source_present: usize = 0;
                        var source_text: usize = 0;
                        for (cells, 0..) |*cell, i| {
                            const missing = switch ((density + which) % 3) {
                                0 => false,
                                1 => i % 16 != 0,
                                else => true,
                            };
                            cell.* = if (missing) .{} else .json(switch (kind) {
                                .boolean => .{ .bool = i % 2 == 0 },
                                .float32, .float64 => .{ .float = if (i % 3 == 0) -0.0 else if (i % 3 == 1) std.math.nan(f64) else std.math.inf(f64) },
                                .text => .{ .string = if (i % 2 == 0) "é☺" else "" },
                                else => .{ .integer = @intCast(i) },
                            });
                            if (!cell.sql_null) {
                                source_present += 1;
                                if (kind == .text) source_text += cell.value.string.len;
                            }
                        }
                        var builder = try CheckedBuilder.init(source_arena.allocator(), kind, &.{.{ .length = @intCast(count), .lower = -7 }}, count, source_present, source_text, .{});
                        for (cells) |cell| try builder.appendChecked(cell, kind, .{}, &work);
                        source.* = try builder.finish(source_arena.allocator(), .{});
                        present += source_present;
                        text_bytes += source_text;
                    }
                    const total = prefix + 2 * count + 1;
                    const shape = &[_]arrays.Dimension{.{ .length = @intCast(total), .lower = -13 }};
                    var fast = try ResultBuilder.initPlanned(region, kind, shape, total, present, if (kind == .text) text_bytes else null, .{});
                    // Scalar reconstruction remains an independent path.
                    var slow = try ResultBuilder.initKnown(region, kind, shape, total, present, .{});
                    for (0..prefix) |_| {
                        try fast.append(.{}, &work);
                        try slow.append(.{}, &work);
                    }
                    for (sources) |source| {
                        try fast.appendValue(source, &work);
                        for (0..source.cardinality()) |i| try slow.append(source.flatCell(i), &work);
                    }
                    try fast.append(.{}, &work);
                    try slow.append(.{}, &work);
                    const result = try fast.finish(region, .{}, &work);
                    const reference = try slow.finish(region, .{}, &work);
                    const expected = try @import("array_storage.zig").encodeAlloc(region, reference, .{});
                    if (result.stored != null) _ = source_arena.reset(.free_all);
                    const actual = try @import("array_storage.zig").encodeAlloc(region, result, .{});
                    try std.testing.expectEqualSlices(u8, expected, actual);
                    _ = try layout.View.open(kind, actual, .{});
                    var fast_wire: std.Io.Writer.Allocating = .init(a);
                    defer fast_wire.deinit();
                    var slow_wire: std.Io.Writer.Allocating = .init(a);
                    defer slow_wire.deinit();
                    // Reference text payloads borrow sources; decode the owned oracle.
                    const decoded = try @import("array_storage.zig").borrowLeaky(region, kind, expected, .{});
                    try @import("array_binary.zig").encode(decoded, &slow_wire.writer, .{});
                    try @import("array_binary.zig").encode(result, &fast_wire.writer, .{});
                    try std.testing.expectEqualSlices(u8, slow_wire.written(), fast_wire.written());
                }
            }
        }
    }
}

test "SQL native span construction retains work cancellation count and text payload bounds" {
    const Fixture = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const region = arena.allocator();
            var work: arrays.Budget = .{};
            var source_builder = try CheckedBuilder.init(region, .text, &.{.{ .length = 65 }}, 65, 65, 65 * 5, .{});
            for (0..65) |_| try source_builder.appendChecked(.json(.{ .string = "é☺" }), .text, .{}, &work);
            const source = try source_builder.finish(region, .{});
            var short = try ResultBuilder.initPlanned(region, .text, source.dimensions, 65, 65, 0, .{});
            try std.testing.expectError(error.InvalidSqlArrayShape, short.appendValue(source, &work));
            var count = try ResultBuilder.initPlanned(region, .text, source.dimensions, 65, 0, 325, .{});
            try std.testing.expectError(error.InvalidSqlArrayShape, count.appendValue(source, &work));
            var output = try ResultBuilder.initPlanned(region, .text, source.dimensions, 65, 65, 325, .{});
            var denied: arrays.Budget = .{ .remaining = 64 };
            try std.testing.expectError(error.SqlProgramLimitExceeded, output.appendValue(source, &denied));
            try std.testing.expectError(error.InvalidSqlArrayShape, output.finish(region, .{}, &work));
            const Cancel = struct {
                fn poll(_: ?*anyopaque) !void {
                    return error.Canceled;
                }
            };
            var parent: @import("numeric_value.zig").Context = .{ .alloc = region, .checkpoint = Cancel.poll };
            var canceled: arrays.Budget = .{ .shared = &parent };
            try std.testing.expectError(error.Canceled, output.appendValue(source, &canceled));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL native span construction text and fixed measurement" {
    const a = std.testing.allocator;
    for ([_]arrays.ElementType{ .int64, .text }) |kind| {
        var source_arena = std.heap.ArenaAllocator.init(a);
        defer source_arena.deinit();
        const count = 8192;
        var builder = try CheckedBuilder.init(source_arena.allocator(), kind, &.{.{ .length = count }}, count, count / 16, if (kind == .text) count / 16 * 5 else 0, .{});
        var work: arrays.Budget = .{};
        for (0..count) |i| try builder.appendChecked(if (i % 16 != 0) .{} else if (kind == .text) .json(.{ .string = "é☺" }) else .json(.{ .integer = @intCast(i) }), kind, .{}, &work);
        const source = try builder.finish(source_arena.allocator(), .{});
        var oracle: ?[]u8 = null;
        defer if (oracle) |bytes| a.free(bytes);
        for (0..2) |mode| {
            var counter: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 8 * 1024 * 1024 };
            var elapsed: i96 = 0;
            for (0..20) |_| {
                var arena = std.heap.ArenaAllocator.init(counter.allocator());
                defer arena.deinit();
                var operation_work: arrays.Budget = .{};
                const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
                const result = if (mode == 1) try source.concatenate(arena.allocator(), source, .{}, &operation_work) else scalar: {
                    var output = try ResultBuilder.initKnown(arena.allocator(), kind, &.{.{ .length = count * 2 }}, count * 2, count / 8, .{});
                    for (0..2) |_| for (0..count) |i| try output.append(source.flatCell(i), &operation_work);
                    break :scalar try output.finish(arena.allocator(), .{}, &operation_work);
                };
                elapsed += @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start);
                const bytes = try @import("array_storage.zig").encodeAlloc(a, result, .{});
                defer a.free(bytes);
                if (oracle) |expected| try std.testing.expectEqualSlices(u8, expected, bytes) else oracle = try a.dupe(u8, bytes);
            }
            try std.testing.expectEqual(@as(usize, 0), counter.live);
            std.debug.print("SQL_NATIVE_SPANS kind={s} native={} output_cells={} peak={} repetitions=20 elapsed_ns={} identical_canonical=true\n", .{ @tagName(kind), mode == 1, count * 2, counter.peak, elapsed });
        }
    }
}

test "SQL native transformed ranges preserve unaligned text and mixed packed layouts" {
    const a = std.testing.allocator;
    for ([_]arrays.ElementType{ .boolean, .int64, .text }) |kind| for (0..2) |density| {
        var owner = std.heap.ArenaAllocator.init(a);
        defer owner.deinit();
        const cells = try owner.allocator().alloc(arrays.Element, 129);
        var present: usize = 0;
        var payload: usize = 0;
        for (cells, 0..) |*cell, i| {
            cell.* = if (density == 1 and i % 7 != 0) .{} else .json(switch (kind) {
                .text => .{ .string = "é☺" },
                .boolean => .{ .bool = i % 2 == 0 },
                else => .{ .integer = @intCast(i) },
            });
            if (!cell.sql_null) {
                present += 1;
                if (kind == .text) payload += 5;
            }
        }
        var builder = try CheckedBuilder.init(owner.allocator(), kind, &.{.{ .length = 129, .lower = -7 }}, 129, present, payload, .{});
        var source_work: arrays.Budget = .{};
        for (cells) |cell| try builder.appendChecked(cell, kind, .{}, &source_work);
        const source = try builder.finish(owner.allocator(), .{});
        for ([_]usize{ 0, 1, 7, 63, 64, 128, 129 }) |begin| for ([_]usize{ 0, 1, 7, 31, 63, 65 }) |count| {
            if (count > 129 - begin) continue;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const region = arena.allocator();
            var expected_present: usize = 0;
            var expected_text: usize = 0;
            for (cells[begin..][0..count]) |cell| if (!cell.sql_null) {
                expected_present += 1;
                if (kind == .text) expected_text += 5;
            };
            const shape = &[_]arrays.Dimension{.{ .length = @intCast(64 + count) }};
            var fast = try ResultBuilder.initPlanned(region, kind, shape, 64 + count, expected_present, if (kind == .text) expected_text else null, .{});
            var slow = try ResultBuilder.initKnown(region, kind, shape, 64 + count, expected_present, .{});
            var work: arrays.Budget = .{};
            for (0..7) |_| {
                try fast.append(.{}, &work);
                try slow.append(.{}, &work);
            }
            try fast.appendRange(source, begin, count, &work);
            for (cells[begin..][0..count]) |cell| try slow.append(cell, &work);
            for (0..57) |_| {
                try fast.append(.{}, &work);
                try slow.append(.{}, &work);
            }
            const result = try fast.finish(region, .{}, &work);
            const reference = try slow.finish(region, .{}, &work);
            const actual = try @import("array_storage.zig").encodeAlloc(region, result, .{});
            const expected = try @import("array_storage.zig").encodeAlloc(region, reference, .{});
            try std.testing.expectEqualSlices(u8, expected, actual);
            _ = try layout.View.open(kind, actual, .{});
        };
    };
}

test "SQL native block conversion and ranges unwind faults and retain shared cancellation" {
    const Fixture = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const region = arena.allocator();
            var builder = try CheckedBuilder.init(region, .int32, &.{.{ .length = 64 }}, 64, 64, 0, .{});
            var work: arrays.Budget = .{};
            for (0..64) |i| try builder.appendChecked(.json(.{ .integer = @intCast(i) }), .int32, .{}, &work);
            const source = try builder.finish(region, .{});
            var output = try ResultBuilder.initKnown(region, .int16, source.dimensions, 64, 64, .{});
            var denied: arrays.Budget = .{ .remaining = 127 };
            try std.testing.expectError(error.SqlProgramLimitExceeded, output.appendConverted(source, &denied));
            try std.testing.expectEqual(@as(usize, 0), output.fixed.builder.state.index);
            const Cancel = struct {
                fn poll(_: ?*anyopaque) !void {
                    return error.Canceled;
                }
            };
            var owner: @import("numeric_value.zig").Context = .{ .alloc = region, .checkpoint = Cancel.poll };
            var canceled: arrays.Budget = .{ .shared = &owner };
            try std.testing.expectError(error.Canceled, output.appendConverted(source, &canceled));
            try std.testing.expect(try output.appendConverted(source, &work));
            const result = try output.finish(region, .{}, &work);
            _ = try layout.View.open(.int16, result.stored.?.bytes, .{});
            var range = try ResultBuilder.initKnown(region, .int32, &.{.{ .length = 32 }}, 32, 32, .{});
            try std.testing.expectError(error.SqlArraySubscriptOutOfRange, range.appendRange(source, 65, 0, &work));
            try std.testing.expectError(error.InvalidSqlArrayShape, range.appendRange(source, 0, 64, &work));
            try std.testing.expectError(error.Canceled, range.appendRange(source, 1, 32, &canceled));
            try range.appendRange(source, 1, 32, &work);
            const part = try range.finish(region, .{}, &work);
            try std.testing.expectEqual(@as(i64, 1), part.flatCell(0).value.integer);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL native dense bulk spans preserve partial NULL bytes work and cancellation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const region = arena.allocator();
    var source_builder = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 8192 }}, 8192, 8192, .{});
    var source_work: arrays.Budget = .{};
    for (0..8192) |i| try source_builder.append(.json(.{ .integer = @intCast(i) }), &source_work);
    const source = try source_builder.finish(region, .{}, &source_work);
    var fast = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 129, .lower = -7 }}, 129, 128, .{});
    var slow = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 129, .lower = -7 }}, 129, 128, .{});
    var fast_work: arrays.Budget = .{};
    var slow_work: arrays.Budget = .{};
    try fast.append(.{}, &fast_work);
    try slow.append(.{}, &slow_work);
    try fast.appendRange(source, 7, 93, &fast_work);
    for (7..100) |i| try slow.append(.json(.{ .integer = @intCast(i) }), &slow_work);
    for (0..35) |_| {
        try fast.append(.json(.{ .integer = -1 }), &fast_work);
        try slow.append(.json(.{ .integer = -1 }), &slow_work);
    }
    const actual = try fast.finish(region, .{}, &fast_work);
    const expected = try slow.finish(region, .{}, &slow_work);
    try std.testing.expectEqual(slow_work.remaining, fast_work.remaining);
    try std.testing.expect(actual.flatCell(0).sql_null);
    for (1..94) |i| try std.testing.expectEqual(@as(i64, @intCast(i + 6)), actual.flatCell(i).value.integer);
    for (94..129) |i| try std.testing.expectEqual(@as(i64, -1), actual.flatCell(i).value.integer);
    try std.testing.expectEqualSlices(u8, expected.stored.?.bytes, actual.stored.?.bytes);
    _ = try layout.View.open(.int64, actual.stored.?.bytes, .{});
    var denied_output = try ResultBuilder.initKnown(region, .int64, source.dimensions, 8192, 8192, .{});
    var denied: arrays.Budget = .{ .remaining = 8191 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, denied_output.appendValue(source, &denied));
    try std.testing.expectEqual(@as(usize, 0), denied_output.fixed.builder.state.index);
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 3) return error.Canceled;
        }
    };
    var cancel: Cancel = .{};
    var context: @import("numeric_value.zig").Context = .{ .alloc = region, .checkpoint = Cancel.check, .ptr = &cancel };
    var work: arrays.Budget = .{ .shared = &context };
    var output = try ResultBuilder.initKnown(region, .int64, source.dimensions, 8192, 8192, .{});
    try std.testing.expectError(error.Canceled, output.appendValue(source, &work));
    try std.testing.expectEqual(@as(usize, 3), cancel.calls);
    try std.testing.expectError(error.Canceled, work.consume(0));
}

test "SQL native bulk nullable spans preserve unaligned checkpoints and work" {
    const a = std.testing.allocator;
    for (0..4) |pattern| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const region = arena.allocator();
        const cells = try region.alloc(arrays.Element, 257);
        var present: usize = 0;
        for (cells, 0..) |*cell, i| {
            cell.* = if (pattern == 0 or (pattern == 1 and i % 7 != 0) or (pattern == 2 and i == 128)) .{} else .json(.{ .integer = @intCast(i) });
            present += @intFromBool(!cell.sql_null);
        }
        var source_builder = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = 257, .lower = -7 }}, 257, present, .{});
        var initial: arrays.Budget = .{};
        for (cells) |cell| try source_builder.append(cell, &initial);
        const source = try source_builder.finish(region, .{}, &initial);
        for ([_]usize{ 0, 1, 7, 63, 64, 65 }) |begin| for ([_]usize{ 0, 1, 7, 63, 64, 65 }) |prefix| {
            const count: usize = 129;
            var range_present: usize = 0;
            for (cells[begin..][0..count]) |cell| range_present += @intFromBool(!cell.sql_null);
            const axes = [_]arrays.Dimension{.{ .length = @intCast(prefix + count + 5), .lower = -3 }};
            var fast = try ResultBuilder.initKnown(region, .int64, &axes, prefix + count + 5, range_present + 5, .{});
            var slow = try ResultBuilder.initKnown(region, .int64, &axes, prefix + count + 5, range_present + 5, .{});
            var fw: arrays.Budget = .{};
            var sw: arrays.Budget = .{};
            for (0..prefix) |_| {
                try fast.append(.{}, &fw);
                try slow.append(.{}, &sw);
            }
            try fast.appendRange(source, begin, count, &fw);
            for (cells[begin..][0..count]) |cell| try slow.append(cell, &sw);
            for (0..5) |_| {
                try fast.append(.json(.{ .integer = -1 }), &fw);
                try slow.append(.json(.{ .integer = -1 }), &sw);
            }
            const actual = try fast.finish(region, .{}, &fw);
            const expected = try slow.finish(region, .{}, &sw);
            try std.testing.expectEqual(sw.remaining, fw.remaining);
            try std.testing.expectEqualSlices(u8, expected.stored.?.bytes, actual.stored.?.bytes);
            _ = try layout.View.open(.int64, actual.stored.?.bytes, .{});
        };
    }
}

test "SQL native nullable span copy measurement" {
    const a = std.testing.allocator;
    for ([_]usize{ 1, 16 }) |stride| {
        var source_arena = std.heap.ArenaAllocator.init(a);
        defer source_arena.deinit();
        const region = source_arena.allocator();
        const count: usize = 16384;
        const present = if (stride == 1) count - 1 else count / stride;
        var source_builder = try ResultBuilder.initKnown(region, .int64, &.{.{ .length = count }}, count, present, .{});
        var initial: arrays.Budget = .{};
        for (0..count) |i| try source_builder.append(if (if (stride == 1) i == 0 else i % stride != 0) .{} else .json(.{ .integer = @intCast(i) }), &initial);
        const source = try source_builder.finish(region, .{}, &initial);
        var counter: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 1024 * 1024 };
        var elapsed: i96 = 0;
        for (0..100) |_| {
            var arena = std.heap.ArenaAllocator.init(counter.allocator());
            defer arena.deinit();
            const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            var output = try ResultBuilder.initKnown(arena.allocator(), .int64, &.{.{ .length = count }}, count, present, .{});
            var work: arrays.Budget = .{};
            try output.appendValue(source, &work);
            const result = try output.finish(arena.allocator(), .{}, &work);
            elapsed += @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start);
            try std.testing.expectEqualSlices(u8, source.stored.?.bytes, result.stored.?.bytes);
        }
        std.debug.print("SQL_NULLABLE_SPAN compact={} repetitions=100 peak={} elapsed_ns={} canonical_equal=true\n", .{ source.stored.?.compact, counter.peak, elapsed });
    }
}
