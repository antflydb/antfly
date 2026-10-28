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

//! Immutable typed SQL arrays, distinct from JSON arrays. Elements are flat,
//! row-major prepared values or canonical storage views. Consumers use
//! cardinality()/flatCell(); bounds and SQL NULL provenance are authoritative.
//! Borrowed views require a pinned owner. Owned copies include nested payloads.
const std = @import("std");
const scalar = @import("scalar.zig");
const json_order = @import("json_order.zig");
const operators = @import("operators.zig");
const MemoryBudget = @import("memory_budget.zig");
const native_ops = @import("array_native.zig");
const Allocator = std.mem.Allocator;
pub const Element = scalar.Datum;
pub const Budget = json_order.Budget;

/// Element widths are part of the type, not inferred from JSON payload shape.
/// Text uses the binary/C collation. Other collations require a bound codec.
pub const ElementType = @import("../common/sql_builtin_type.zig").Type;

pub const Dimension = @import("../common/sql_array_layout.zig").Dimension;
pub const Limits = struct { elements: usize = 65536, bytes: usize = 8 * 1024 * 1024, work: usize = 1_048_576 };
pub const Comparison = enum {
    eq,
    ne,
    lt,
    le,
    gt,
    ge,

    pub fn accepts(self: Comparison, order: std.math.Order) bool {
        return switch (self) {
            .eq => order == .eq,
            .ne => order != .eq,
            .lt => order == .lt,
            .le => order != .gt,
            .gt => order == .gt,
            .ge => order != .lt,
        };
    }
};
pub const Quantifier = enum { any, all };

/// Allocation-free concatenation layout. Evaluators may fold layouts in the
/// original expression tree before committing a single output allocation.
pub const ConcatLayout = struct {
    axes: [6]Dimension = undefined,
    rank: usize = 0,
    count: usize = 0,
    pub fn from(value: Value) !ConcatLayout {
        if (value.dimensions.len > 6) return error.SqlProgramLimitExceeded;
        var result: ConcatLayout = .{ .rank = value.dimensions.len, .count = value.cardinality() };
        @memcpy(result.axes[0..result.rank], value.dimensions);
        return result;
    }
    pub fn appended(self: ConcatLayout, limits: Limits) !ConcatLayout {
        if (self.rank > 1) return error.SqlArrayAppendDimensions;
        var result = self;
        result.count = std.math.add(usize, self.count, 1) catch return error.SqlProgramLimitExceeded;
        const lower_bound: i32 = if (self.rank == 0) 1 else self.axes[0].lower;
        const count = std.math.cast(i64, result.count) orelse return error.SqlProgramLimitExceeded;
        if (@as(i64, lower_bound) + count > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
        result.rank = 1;
        result.axes[0] = .{ .length = std.math.cast(u32, result.count) orelse return error.SqlProgramLimitExceeded, .lower = lower_bound };
        _ = try validateShape(result.axes[0..1], result.count, limits);
        return result;
    }
    pub fn combine(left: ConcatLayout, right: ConcatLayout) !ConcatLayout {
        if (left.count == 0) return right;
        if (right.count == 0) return left;
        if (left.rank == 0 or right.rank == 0) return error.InvalidSqlArrayShape;
        if (@max(left.rank, right.rank) - @min(left.rank, right.rank) > 1) return error.SqlArrayConcatenationDimensions;
        var result = if (left.rank >= right.rank) left else right;
        const lower = if (left.rank >= right.rank) right else left;
        const tails = if (left.rank == right.rank) lower.axes[1..lower.rank] else lower.axes[0..lower.rank];
        for (result.axes[1..result.rank], tails) |a, b| if (a.length != b.length or a.lower != b.lower) return error.SqlArrayConcatenationDimensions;
        result.count = std.math.add(usize, left.count, right.count) catch return error.SqlProgramLimitExceeded;
        result.axes[0].length = std.math.add(u32, result.axes[0].length, if (left.rank == right.rank) right.axes[0].length else 1) catch return error.SqlProgramLimitExceeded;
        _ = try validateShape(result.axes[0..result.rank], result.count, .{ .elements = std.math.maxInt(i32) });
        return result;
    }
};

pub const Value = struct {
    element_type: ElementType,
    dimensions: []const Dimension,
    elements: []const Element = &.{},
    /// Canonical, page-owned storage bytes for primitive/text arrays.
    stored: ?*const @import("../common/sql_array_layout.zig").View = null,
    /// Physical footprint of immutable prepared cells, including this header
    /// and dimensions, excluding the enclosing Datum. Constructors compute it
    /// during their existing walk; prepared copies preserve it. This is sizing
    /// metadata, never a validation proof or permission to bypass quotas.
    /// Computed native results use this slot for their construction capacity;
    /// retainedBytes reads their view, while admissionBytes includes staging.
    /// Native ownership copies reset it to the new owner's actual extent.
    /// A caller replacing cells or dimensions must clear this cache.
    prepared_footprint: ?usize = null,

    pub fn retainedBytes(self: Value) !usize {
        const header = @sizeOf(Value) + self.dimensions.len * @sizeOf(Dimension);
        if (self.stored) |stored| return header + @sizeOf(@import("../common/sql_array_layout.zig").View) + stored.bytes.len;
        if (self.prepared_footprint) |bytes| return bytes;
        var bytes: usize = header;
        for (self.elements) |element| {
            if (element.array != null) return error.SqlTypeMismatch;
            bytes +|= try operators.datumBytes(element);
        }
        return bytes;
    }

    /// Dense staging may remain in the construction region even after its
    /// published view is sparse. Owned copies retain only canonical bytes.
    pub fn admissionBytes(self: Value) !usize {
        return @max(self.prepared_footprint orelse 0, try self.retainedBytes());
    }

    /// Only validated immutable native storage supplies reusable NULL counts.
    /// Authentication-only views and prepared cells keep the bounded fallback.
    pub fn knownNonNullCount(self: Value) !?usize {
        if (self.stored) |view| {
            const proof = view.validation orelse return null;
            if (self.elements.len != 0 or self.element_type != view.kind or self.dimensions.len != view.rank or proof.non_null > view.count) return error.InvalidSqlArrayShape;
            for (self.dimensions, 0..) |axis, i| {
                const expected = try view.dimension(i);
                if (axis.length != expected.length or axis.lower != expected.lower) return error.InvalidSqlArrayShape;
            }
            return proof.non_null;
        }
        return if (self.elements.len == 0 and self.dimensions.len == 0) 0 else null;
    }

    /// The validated text directory already identifies the complete payload.
    pub fn knownTextBytes(self: Value) !?usize {
        if (self.element_type != .text) return null;
        if (try self.knownNonNullCount() != null) {
            if (self.stored) |view| return view.bytes.len - view.payload_start;
            return 0;
        }
        return null;
    }

    pub fn flatCell(self: Value, index: usize) Element {
        if (self.stored) |view| {
            const raw = view.cell(index) catch unreachable;
            if (raw.sql_null) return .{};
            return Element.json(switch (self.element_type) {
                .boolean => .{ .bool = raw.bytes[0] == 1 },
                .int16 => .{ .integer = std.mem.readInt(i16, raw.bytes[0..2], .little) },
                .int32 => .{ .integer = std.mem.readInt(i32, raw.bytes[0..4], .little) },
                .int64 => .{ .integer = std.mem.readInt(i64, raw.bytes[0..8], .little) },
                .float32 => .{ .float = @as(f32, @bitCast(std.mem.readInt(u32, raw.bytes[0..4], .little))) },
                .float64 => .{ .float = @bitCast(std.mem.readInt(u64, raw.bytes[0..8], .little)) },
                .text => .{ .string = raw.bytes },
                .uuid, .numeric, .jsonb => unreachable,
            });
        }
        return self.elements[index];
    }

    pub fn validate(self: Value, limits: Limits) !Value {
        var work: Budget = .{ .remaining = limits.work };
        return self.validateWithBudget(limits, &work);
    }
    pub fn validateWithBudget(self: Value, limits: Limits, work: *Budget) !Value {
        if (self.stored) |stored| {
            if (self.elements.len != 0 or self.element_type != stored.kind or stored.rank != self.dimensions.len) return error.InvalidSqlArrayShape;
            if (stored.count > limits.elements or stored.bytes.len > limits.bytes) return error.SqlProgramLimitExceeded;
            for (self.dimensions, 0..) |axis, index| {
                const expected = try stored.dimension(index);
                if (axis.length != expected.length or axis.lower != expected.lower) return error.InvalidSqlArrayShape;
            }
            if (stored.validation) |proof| {
                // Recheck the caller's quota and cancellation owner without
                // rescanning immutable bytes admitted by the storage codec.
                try work.consume(proof.element_work);
            } else for (0..stored.count) |index| try validateElement(self.element_type, self.flatCell(index), work);
            return self;
        }
        return initWithBudget(self.element_type, self.dimensions, self.elements, limits, work);
    }

    /// Validate borrowed prepared cells before they enter an operator. Empty
    /// arrays have no dimensions, including an input shape containing a zero.
    pub fn init(kind: ElementType, dimensions: []const Dimension, elements: []const Element, limits: Limits) !Value {
        var work: Budget = .{ .remaining = limits.work };
        return initWithBudget(kind, dimensions, elements, limits, &work);
    }

    /// Ingress codecs share parsing and typed validation under one work budget.
    pub fn initWithBudget(kind: ElementType, dimensions: []const Dimension, elements: []const Element, limits: Limits, work: *Budget) !Value {
        const empty = try validateShape(dimensions, elements.len, limits);
        var bytes: usize = @sizeOf(Value) + dimensions.len * @sizeOf(Dimension);
        if (bytes > limits.bytes) return error.SqlProgramLimitExceeded;
        for (elements) |element| {
            try validateElement(kind, element, work);
            bytes = std.math.add(usize, bytes, try operators.datumBytesControlled(element, work)) catch return error.SqlProgramLimitExceeded;
            if (bytes > limits.bytes) return error.SqlProgramLimitExceeded;
        }
        return .{ .element_type = kind, .dimensions = if (empty) &.{} else dimensions, .elements = elements, .prepared_footprint = bytes - (if (empty) dimensions.len * @sizeOf(Dimension) else 0) };
    }

    pub fn length(self: Value, dimension: i32) ?u32 {
        if (dimension <= 0 or dimension > self.dimensions.len) return null;
        return self.dimensions[@intCast(dimension - 1)].length;
    }

    pub fn cardinality(self: Value) usize {
        return if (self.stored) |view| view.count else self.elements.len;
    }

    pub fn rank(self: Value) ?usize {
        return if (self.dimensions.len == 0) null else self.dimensions.len;
    }

    /// Strict scalar comparisons implement SQL three-valued ANY/ALL. A NULL
    /// array is handled by the caller, separately from this non-NULL view.
    /// Empty arrays retain their quantifier identity even for a NULL scalar.
    pub fn quantified(self: Value, scalar_value: Element, comparison: Comparison, quantifier: Quantifier, work: *Budget) !?bool {
        return (try self.quantifiedObserved(scalar_value, comparison, quantifier, work)).result;
    }

    /// Completed prefix length distinguishes expensive first-cell comparisons
    /// from scans that actually benefit from a reusable membership index.
    pub fn quantifiedObserved(self: Value, scalar_value: Element, comparison: Comparison, quantifier: Quantifier, work: *Budget) !QuantifiedResult {
        // The SQL probe can have a wider numeric type than the array cells:
        // comparing bigint against int[] must not narrow/overflow the probe.
        try validateElement(switch (self.element_type) {
            .int16, .int32 => .int64,
            .float32 => .float64,
            else => self.element_type,
        }, scalar_value, work);
        if (self.stored) |view| if (native_ops.supported(view.*)) {
            _ = try self.knownNonNullCount();
            return native_ops.quantified(view.*, scalar_value, comparison, quantifier, work);
        };
        var saw_null = false;
        for (0..self.cardinality()) |array_cell_0| {
            const element = self.flatCell(array_cell_0);
            try work.consume(1);
            if (scalar_value.sql_null or element.sql_null) {
                saw_null = true;
                continue;
            }
            const matched = comparison.accepts(try compareElement(self.element_type, scalar_value, element, work));
            if (quantifier == .any and matched) return .{ .result = true, .visited = array_cell_0 + 1 };
            if (quantifier == .all and !matched) return .{ .result = false, .visited = array_cell_0 + 1 };
        }
        return .{ .result = if (saw_null) null else quantifier == .all, .visited = self.cardinality() };
    }

    pub fn lower(self: Value, dimension: i32) ?i32 {
        if (dimension <= 0 or dimension > self.dimensions.len) return null;
        return self.dimensions[@intCast(dimension - 1)].lower;
    }

    pub fn upper(self: Value, dimension: i32) ?i32 {
        if (dimension <= 0 or dimension > self.dimensions.len) return null;
        const axis = self.dimensions[@intCast(dimension - 1)];
        return @intCast(@as(i64, axis.lower) + axis.length - 1);
    }

    pub fn at(self: Value, subscripts: []const i32) ?Element {
        if (subscripts.len == 0 or subscripts.len != self.dimensions.len) return null;
        var offset: usize = 0;
        for (self.dimensions, subscripts) |dimension, subscript| {
            const relative = @as(i64, subscript) - dimension.lower;
            if (relative < 0 or relative >= dimension.length) return null;
            offset = offset * dimension.length + @as(usize, @intCast(relative));
        }
        return self.flatCell(offset);
    }

    /// PostgreSQL search uses IS NOT DISTINCT FROM, not strict equality:
    /// SQL NULL matches SQL NULL, and NaN matches NaN. Subscripts, not flat
    /// offsets, are returned. Only the one-dimensional search overload exists.
    pub fn position(self: Value, needle: Element, start: ?i32, work: *Budget) !?i32 {
        if (self.dimensions.len > 1) return error.UnsupportedSqlShape;
        try validateElement(self.element_type, needle, work);
        if (self.dimensions.len == 0) return null;
        const lower_bound = self.dimensions[0].lower;
        const offset: usize = @intCast(@max(@as(i64, 0), @as(i64, start orelse lower_bound) - lower_bound));
        if (offset >= self.cardinality()) return null;
        if (self.nativeSearch()) |view| {
            _ = try self.knownNonNullCount();
            var physical: usize = if (view.compact) std.mem.readInt(u32, view.bytes[view.slots_start + offset / 64 * 4 ..][0..4], .little) + offset % 64 - @popCount(@import("array_compact.zig").ResultBuilder.nullBits(view, offset / 64 * 64, offset % 64)) else offset;
            var cursor = offset;
            while (cursor < view.count) {
                const block_count = @min(64, view.count - cursor);
                const bits = try searchNativeBlock(view, cursor, block_count, &physical, needle, work, true);
                if (bits != 0) return @intCast(@as(i64, lower_bound) + @as(i64, @intCast(cursor + @ctz(bits))));
                cursor += block_count;
            }
            return null;
        }
        for (offset..self.cardinality()) |i| {
            const element = self.flatCell(i);
            if (try compareElement(self.element_type, element, needle, work) == .eq) return @intCast(@as(i64, lower_bound) + @as(i64, @intCast(i)));
        }
        return null;
    }

    /// Two bounded passes allocate the exact result vector rather than a
    /// worst-case vector for every input row. The result always has int4 cells
    /// and a one-based lower bound, independent of the searched array's bounds.
    pub fn positions(self: Value, alloc: Allocator, needle: Element, limits: Limits, work: *Budget) !Value {
        if (self.dimensions.len > 1) return error.UnsupportedSqlShape;
        try validateElement(self.element_type, needle, work);
        var local: [8192]u8 = undefined;
        if (self.cardinality() <= local.len * 8) if (self.nativeSearch()) |view| {
            _ = try self.knownNonNullCount();
            const bitmap = local[0 .. (self.cardinality() + 7) / 8];
            var physical: usize = 0;
            var cursor: usize = 0;
            var count: usize = 0;
            const before = work.remaining;
            while (cursor < view.count) {
                const block_count = @min(64, view.count - cursor);
                const bits = try searchNativeBlock(view, cursor, block_count, &physical, needle, work, false);
                for (0..(block_count + 7) / 8) |byte| bitmap[cursor / 8 + byte] = @truncate(bits >> @intCast(byte * 8));
                count += @popCount(bits);
                cursor += block_count;
            }
            const dimensions: [1]Dimension = .{.{ .length = @intCast(count) }};
            var output = try @import("array_compact.zig").ResultBuilder.initKnown(alloc, .int32, dimensions[0..@intFromBool(count != 0)], count, count, limits);
            // Retain the second logical comparison pass without decoding or
            // comparing again. Emission keeps its own original work charges.
            try work.consume(before - work.remaining);
            cursor = nextBit(bitmap, 0, view.count, true);
            while (cursor < view.count) {
                try output.append(Element.json(.{ .integer = @as(i64, self.dimensions[0].lower) + @as(i64, @intCast(cursor)) }), work);
                cursor = nextBit(bitmap, cursor + 1, view.count, true);
            }
            return output.finish(alloc, limits, work);
        };
        var count: usize = 0;
        for (0..self.cardinality()) |i| if (try compareElement(self.element_type, self.flatCell(i), needle, work) == .eq) {
            count += 1;
        };
        const dimensions: [1]Dimension = .{.{ .length = @intCast(count) }};
        var output = try @import("array_compact.zig").ResultBuilder.initKnown(alloc, .int32, dimensions[0..@intFromBool(count != 0)], count, count, limits);
        for (0..self.cardinality()) |i| if (try compareElement(self.element_type, self.flatCell(i), needle, work) == .eq) {
            try output.append(Element.json(.{ .integer = @as(i64, self.dimensions[0].lower) + @as(i64, @intCast(i)) }), work);
        };
        return output.finish(alloc, limits, work);
    }

    /// Result vectors/dimensions belong to alloc; cell payloads borrow the
    /// pinned source/replacement owner, as scalar constructors do. Retained
    /// operator boundaries must clone Datums before releasing those owners.
    pub fn transform(self: Value, alloc: Allocator, needle: Element, replacement: ?Element, limits: Limits, work: *Budget) !Value {
        if (replacement == null and self.dimensions.len > 1) return error.UnsupportedSqlShape;
        try validateElement(self.element_type, needle, work);
        if (replacement) |value| {
            try validateElement(self.element_type, value, work);
            // SQL equality alone cannot establish representation identity:
            // floating zero signs and NUMERIC display scales may change.
            const identity = if (needle.sql_null or value.sql_null) needle.sql_null and value.sql_null else switch (self.element_type) {
                .boolean => needle.value.bool == value.value.bool,
                .int16, .int32, .int64 => needle.value.integer == value.value.integer,
                .text, .uuid => try work.orderBytes(needle.value.string, value.value.string) == .eq,
                else => false,
            };
            if (identity and self.cardinality() <= limits.elements and try self.admissionBytes() <= limits.bytes) return self.validateWithBudget(limits, work);
        }
        // A fixed bitmap covers the default maximum without heap allocation.
        // Custom larger inputs use bounded replay instead of growing scratch.
        var local: [8192]u8 = undefined;
        const plan_size = (self.cardinality() + 7) / 8;
        const matches: ?[]u8 = if (plan_size <= local.len) local[0..plan_size] else null;
        if (matches) |bitmap| @memset(bitmap, 0);
        var changed = false;
        var count: usize = 0;
        var non_null: usize = 0;
        var text_bytes: usize = 0;
        const native = if (self.stored) |view| view.validation != null and matches != null else false;
        if (native) {
            _ = try self.knownNonNullCount();
            const view = self.stored.?;
            var matched_count: usize = 0;
            var physical: usize = 0;
            var cursor: usize = 0;
            while (cursor < self.cardinality()) {
                const block_count = @min(64, self.cardinality() - cursor);
                const bits = @import("array_compact.zig").ResultBuilder.nullBits(view.*, cursor, block_count);
                var matched: u64 = 0;
                if (needle.sql_null) {
                    try work.consume(block_count);
                    matched = bits;
                } else {
                    matched = switch (self.element_type) {
                        inline .boolean, .int16, .int32, .int64, .float32, .float64, .text => |kind| try matchNativeBlock(kind, view.*, cursor, block_count, bits, &physical, needle, work),
                        else => unreachable,
                    };
                }
                for (0..(block_count + 7) / 8) |byte| matches.?[cursor / 8 + byte] = @truncate(matched >> @intCast(byte * 8));
                matched_count += @popCount(matched);
                cursor += block_count;
            }
            changed = matched_count != 0;
            count = self.cardinality() - if (replacement == null) matched_count else @as(usize, 0);
            non_null = view.validation.?.non_null - if (needle.sql_null) @as(usize, 0) else matched_count;
            if (replacement) |value| if (!value.sql_null) {
                non_null += matched_count;
            };
            if (self.element_type == .text) {
                text_bytes = (try self.knownTextBytes()).? - if (needle.sql_null) @as(usize, 0) else matched_count * needle.value.string.len;
                if (replacement) |value| if (!value.sql_null) {
                    const added = std.math.mul(usize, matched_count, value.value.string.len) catch return error.SqlProgramLimitExceeded;
                    text_bytes = std.math.add(usize, text_bytes, added) catch return error.SqlProgramLimitExceeded;
                };
            }
        } else {
            for (0..self.cardinality()) |i| {
                const element = self.flatCell(i);
                const matched = try compareElement(self.element_type, element, needle, work) == .eq;
                if (matched) {
                    changed = true;
                    if (matches) |bitmap| bitmap[i / 8] |= @as(u8, 1) << @intCast(i % 8);
                    if (replacement == null) continue;
                }
                const emitted = if (matched) replacement.? else element;
                count += 1;
                if (!emitted.sql_null) {
                    non_null += 1;
                    if (self.element_type == .text) text_bytes = std.math.add(usize, text_bytes, emitted.value.string.len) catch return error.SqlProgramLimitExceeded;
                }
            }
        }
        if (!changed and self.cardinality() <= limits.elements and try self.admissionBytes() <= limits.bytes) {
            return self.validateWithBudget(limits, work);
        }
        const rank_: usize = if (count == 0) 0 else self.dimensions.len;
        var axes: [6]Dimension = undefined;
        if (rank_ > axes.len) return error.SqlProgramLimitExceeded;
        const dimensions = axes[0..rank_];
        @memcpy(dimensions, self.dimensions[0..rank_]);
        if (replacement == null and count != 0) dimensions[0].length = @intCast(count);
        var output = try @import("array_compact.zig").ResultBuilder.initPlanned(alloc, self.element_type, dimensions, count, non_null, if (self.element_type == .text) text_bytes else null, limits);
        if (native) {
            try work.consume(self.cardinality());
            if (count == 0) return output.finish(alloc, limits, work);
            var cursor: usize = 0;
            while (cursor < self.cardinality()) {
                const next = nextBit(matches.?, cursor, self.cardinality(), true);
                if (next != cursor) try output.appendRange(self, cursor, next - cursor, work);
                if (next == self.cardinality()) break;
                if (replacement) |value| {
                    try output.append(value, work);
                    cursor = next + 1;
                } else {
                    cursor = nextBit(matches.?, next + 1, self.cardinality(), false);
                }
            }
        } else {
            for (0..self.cardinality()) |i| {
                try work.consume(1);
                const matched = if (matches) |bitmap| bitmap[i / 8] & (@as(u8, 1) << @intCast(i % 8)) != 0 else try compareElement(self.element_type, self.flatCell(i), needle, work) == .eq;
                if (matched and replacement == null) continue;
                try output.append(if (matched) replacement.? else self.flatCell(i), work);
            }
        }
        return output.finish(alloc, limits, work);
    }

    // A validated view permits sequential physical addressing. Packed inputs
    // never replay rank checkpoints for each non-NULL candidate; text reads its
    // offset pair directly. Keep the scalar comparator's work and NaN rules.
    fn nativeSearch(self: Value) ?@import("../common/sql_array_layout.zig").View {
        const view = self.stored orelse return null;
        return if (view.validation != null) view.* else null;
    }
    fn searchNativeBlock(view: @import("../common/sql_array_layout.zig").View, begin: usize, count: usize, physical: *usize, needle: Element, work: *Budget, comptime first_only: bool) !u64 {
        const nulls = @import("array_compact.zig").ResultBuilder.nullBits(view, begin, count);
        if (needle.sql_null) {
            try work.consume(if (first_only and nulls != 0) @ctz(nulls) + 1 else count);
            return nulls;
        }
        return switch (view.kind) {
            inline .boolean, .int16, .int32, .int64, .float32, .float64, .text => |kind| matchNativeBlockMode(kind, view, begin, count, nulls, physical, needle, work, first_only),
            else => error.InvalidSqlArrayShape,
        };
    }
    fn matchNativeBlock(comptime kind: ElementType, view: @import("../common/sql_array_layout.zig").View, begin: usize, block_count: usize, nulls: u64, physical: *usize, needle: Element, work: *Budget) !u64 {
        return matchNativeBlockMode(kind, view, begin, block_count, nulls, physical, needle, work, false);
    }
    fn matchNativeBlockMode(comptime kind: ElementType, view: @import("../common/sql_array_layout.zig").View, begin: usize, block_count: usize, nulls: u64, physical: *usize, needle: Element, work: *Budget, comptime first_only: bool) !u64 {
        if (!first_only) try work.consume(@popCount(nulls));
        const mask = if (block_count == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(block_count)) - 1;
        var candidates = ~nulls & mask;
        var packed_cursor = physical.*;
        var matched: u64 = 0;
        var charged: usize = 0;
        while (candidates != 0) {
            const bit = @ctz(candidates);
            candidates &= candidates - 1;
            const slot = if (view.compact) packed_cursor else begin + bit;
            try work.consume(if (first_only) bit + 1 - charged else 1);
            charged = bit + 1;
            const equal = switch (kind) {
                .text => text: {
                    const first = std.mem.readInt(u32, view.bytes[view.slots_start + slot * 4 ..][0..4], .little);
                    const last = std.mem.readInt(u32, view.bytes[view.slots_start + (slot + 1) * 4 ..][0..4], .little);
                    break :text try work.orderBytes(view.bytes[view.payload_start + first .. view.payload_start + last], needle.value.string) == .eq;
                },
                .boolean => (view.bytes[view.payload_start + slot] != 0) == needle.value.bool,
                .int16, .int32, .int64 => integer: {
                    const T = if (kind == .int16) i16 else if (kind == .int32) i32 else i64;
                    const width = @sizeOf(T);
                    break :integer std.mem.readInt(T, view.bytes[view.payload_start + slot * width ..][0..width], .little) == needle.value.integer;
                },
                .float32, .float64 => floating: {
                    const T = if (kind == .float32) f32 else f64;
                    const width = @sizeOf(T);
                    const number: T = @bitCast(std.mem.readInt(@Int(.unsigned, @bitSizeOf(T)), view.bytes[view.payload_start + slot * width ..][0..width], .little));
                    break :floating number == needle.value.float or (std.math.isNan(number) and std.math.isNan(needle.value.float));
                },
                else => unreachable,
            };
            packed_cursor += 1;
            if (equal) {
                matched |= @as(u64, 1) << @intCast(bit);
                if (first_only) return matched;
            }
        }
        if (first_only) try work.consume(block_count - charged);
        physical.* = packed_cursor;
        return matched;
    }

    fn nextBit(bitmap: []const u8, begin: usize, count: usize, matched: bool) usize {
        var cursor = begin;
        while (cursor < count) {
            const bits = if (matched) bitmap[cursor / 8] else ~bitmap[cursor / 8];
            const byte = bits & (@as(u8, 0xff) << @intCast(cursor % 8));
            if (byte != 0) return @min(count, cursor / 8 * 8 + @ctz(byte));
            cursor = (cursor / 8 + 1) * 8;
        }
        return count;
    }

    /// Append/prepend preserve an existing one-dimensional lower bound.
    /// Empty arrays acquire the ordinary one-based bound. Payloads borrow the
    /// pinned operand owners; structural storage belongs to the result arena.
    pub fn append(self: Value, alloc: Allocator, element: Element, prepend: bool, limits: Limits, work: *Budget) !Value {
        if (self.dimensions.len > 1) return error.SqlArrayAppendDimensions;
        try validateElement(self.element_type, element, work);
        const shape = try (try ConcatLayout.from(self)).appended(limits);
        const count = shape.count;
        const non_null = if (try self.knownNonNullCount()) |present| present + @intFromBool(!element.sql_null) else null;
        const text_bytes = if (try self.knownTextBytes()) |bytes| std.math.add(usize, bytes, if (element.sql_null) 0 else element.value.string.len) catch return error.SqlProgramLimitExceeded else null;
        var output = try @import("array_compact.zig").ResultBuilder.initPlanned(alloc, self.element_type, shape.axes[0..shape.rank], count, non_null, text_bytes, limits);
        if (prepend) try output.append(element, work);
        try output.appendValue(self, work);
        if (!prepend) try output.append(element, work);
        return output.finish(alloc, limits, work);
    }

    /// Concatenate whole first-axis slices. Equal-rank arrays may differ only
    /// in their first axis; adjacent ranks require the lower-rank shape to
    /// exactly match the higher-rank tail, including lower bounds. Identity
    /// results borrow all storage; nonidentity results allocate typed storage or
    /// a small/variable-width cell vector.
    pub fn concatenate(self: Value, alloc: Allocator, other: Value, limits: Limits, work: *Budget) !Value {
        if (self.element_type != other.element_type) return error.SqlTypeMismatch;
        if (self.cardinality() == 0) return other;
        if (other.cardinality() == 0) return self;
        const shape = try ConcatLayout.combine(try ConcatLayout.from(self), try ConcatLayout.from(other));
        const count = shape.count;
        const dimensions = shape.axes[0..shape.rank];
        var non_null: ?usize = null;
        if (try self.knownNonNullCount()) |left| if (try other.knownNonNullCount()) |right| {
            non_null = left + right;
        };
        var text_bytes: ?usize = null;
        if (try self.knownTextBytes()) |left| if (try other.knownTextBytes()) |right| {
            text_bytes = std.math.add(usize, left, right) catch return error.SqlProgramLimitExceeded;
        };
        var output = try @import("array_compact.zig").ResultBuilder.initPlanned(alloc, self.element_type, dimensions, count, non_null, text_bytes, limits);
        try output.appendValue(self, work);
        try output.appendValue(other, work);
        return output.finish(alloc, limits, work);
    }

    pub fn compare(self: Value, other: Value, budget: *Budget) !std.math.Order {
        try budget.consume(1);
        if (self.element_type != other.element_type) return error.SqlTypeMismatch;
        if (self.stored) |left| if (other.stored) |right| if (native_ops.supported(left.*) and native_ops.supported(right.*)) {
            _ = try self.knownNonNullCount();
            _ = try other.knownNonNullCount();
            const result = try native_ops.orderPrefix(left.*, right.*, budget, false);
            if (result != .eq) return result;
            return compareShape(self.cardinality(), other.cardinality(), self.dimensions, other.dimensions, budget);
        };
        for (0..@min(self.cardinality(), other.cardinality())) |index| {
            const a = self.flatCell(index);
            const b = other.flatCell(index);
            const order = try compareElement(self.element_type, a, b, budget);
            if (order != .eq) return order;
        }
        return compareShape(self.cardinality(), other.cardinality(), self.dimensions, other.dimensions, budget);
    }

    pub fn semanticHash(self: Value, budget: *Budget) !u64 {
        try budget.consume(1);
        var hash = std.hash.Wyhash.init(0x4152524159);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, self.element_type.oid(), .little);
        hash.update(&bytes);
        std.mem.writeInt(u64, &bytes, self.dimensions.len, .little);
        hash.update(&bytes);
        std.mem.writeInt(u64, &bytes, self.cardinality(), .little);
        hash.update(&bytes);
        for (self.dimensions) |dimension| {
            std.mem.writeInt(u32, bytes[0..4], dimension.length, .little);
            std.mem.writeInt(i32, bytes[4..8], dimension.lower, .little);
            hash.update(&bytes);
        }
        if (self.stored) |view| if (native_ops.supported(view.*)) {
            _ = try self.knownNonNullCount();
            try native_ops.hashElements(view.*, &hash, budget);
            return hash.final();
        };
        for (0..self.cardinality()) |array_cell_0| {
            const element = self.flatCell(array_cell_0);
            std.mem.writeInt(u64, &bytes, try hashElement(self.element_type, element, budget), .little);
            hash.update(&bytes);
        }
        return hash.final();
    }
};

/// Shared allocation-free shape gate for prepared and directly typed results.
pub fn validateShape(dimensions: []const Dimension, count_: usize, limits: Limits) !bool {
    if (dimensions.len > 6 or count_ > limits.elements) return error.SqlProgramLimitExceeded;
    var empty = dimensions.len == 0;
    for (dimensions) |dimension| {
        if (dimension.length == 0) empty = true;
        // PostgreSQL requires the exclusive upper end to fit int32,
        // not merely the last valid subscript. Dimension lengths and
        // intermediate row-major products are also signed-int32 bounded.
        if (dimension.length > std.math.maxInt(i32) or @as(i64, dimension.lower) + dimension.length > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
    }
    var count: usize = @intFromBool(dimensions.len != 0);
    for (dimensions) |dimension| {
        count = std.math.mul(usize, count, dimension.length) catch return error.SqlProgramLimitExceeded;
        if (count > std.math.maxInt(i32)) return error.SqlProgramLimitExceeded;
    }
    if (count != count_) return error.InvalidSqlArrayShape;
    return empty;
}

pub fn validateElement(kind: ElementType, element: Element, budget: *Budget) !void {
    _ = try validateElementHash(kind, element, budget);
}

/// JSONB semantic hashing is part of validation (including numeric tokens).
/// Return that hash to immediate lookup callers rather than traversing the
/// same probe again. Other types retain their existing validation/hash paths.
/// Only immediate callers receive it, after full validation succeeds.
fn validateElementHash(kind: ElementType, element: Element, budget: *Budget) !?u64 {
    try budget.consume(1);
    if (element.patterns != null or element.array != null) return error.SqlTypeMismatch;
    if (element.numeric != null and (kind != .numeric or element.sql_null or element.value != .null)) return error.SqlTypeMismatch;
    if (element.sql_null) {
        if (element.value != .null) return error.InvalidSqlArrayShape;
        return null;
    }
    const value = element.value;
    switch (kind) {
        .numeric => {
            var none = std.heap.FixedBufferAllocator.init(&.{});
            var ctx = budget.numericContext(none.allocator());
            defer budget.remaining = @intCast(ctx.remaining);
            try @import("numeric_value.zig").validateCanonical(&ctx, (element.numeric orelse return error.SqlTypeMismatch).*);
        },
        .text => {
            if (value != .string) return error.SqlTypeMismatch;
            try json_order.validateText(value.string, budget);
        },
        .uuid => {
            if (value != .string or value.string.len != 36) return error.SqlTypeMismatch;
            try budget.consume(value.string.len);
            if (!@import("../common/uuid.zig").isCanonical(value.string)) return error.SqlTypeMismatch;
        },
        .int16, .int32, .int64 => {
            if (value != .integer) return error.SqlTypeMismatch;
            if (kind == .int16 and std.math.cast(i16, value.integer) == null) return error.SqlNumericOutOfRange;
            if (kind == .int32 and std.math.cast(i32, value.integer) == null) return error.SqlNumericOutOfRange;
        },
        .float32, .float64 => {
            if (value != .float) return error.SqlTypeMismatch;
            if (kind == .float32 and std.math.isFinite(value.float)) {
                const narrowed: f32 = @floatCast(value.float);
                if (!std.math.isFinite(narrowed)) return error.SqlNumericOutOfRange;
                if (@as(f64, narrowed) != value.float) return error.SqlTypeMismatch;
            }
        },
        .boolean => if (value != .bool) return error.SqlTypeMismatch,
        .jsonb => {
            try json_order.validateTextDomain(value, budget, 0);
            return try json_order.hash(value, budget, 0);
        },
    }
    return null;
}

/// PostgreSQL's shape tie-break follows equal row-major elements. Reuse it
/// for materialized values and borrowed canonical row views.
pub fn compareShape(left_count: usize, right_count: usize, left: []const Dimension, right: []const Dimension, budget: *Budget) !std.math.Order {
    try budget.consume(1);
    const count = std.math.order(left_count, right_count);
    if (count != .eq) return count;
    const rank_order = std.math.order(left.len, right.len);
    if (rank_order != .eq) return rank_order;
    for (left, right) |a, b| {
        try budget.consume(1);
        const order = std.math.order(a.length, b.length);
        if (order != .eq) return order;
    }
    for (left, right) |a, b| {
        try budget.consume(1);
        const order = std.math.order(a.lower, b.lower);
        if (order != .eq) return order;
    }
    return .eq;
}

pub fn compareElement(kind: ElementType, a: Element, b: Element, budget: *Budget) !std.math.Order {
    try budget.consume(1);
    if (a.sql_null or b.sql_null) return if (a.sql_null == b.sql_null) .eq else if (a.sql_null) .gt else .lt;
    if (kind == .float32 or kind == .float64) {
        const an = std.math.isNan(a.value.float);
        const bn = std.math.isNan(b.value.float);
        if (an or bn) return if (an == bn) .eq else if (an) .gt else .lt;
        return std.math.order(a.value.float, b.value.float);
    }
    return switch (kind) {
        .numeric => blk: {
            var none = std.heap.FixedBufferAllocator.init(&.{});
            var ctx = budget.numericContext(none.allocator());
            defer budget.remaining = @intCast(ctx.remaining);
            break :blk try @import("numeric_value.zig").order(&ctx, a.numeric.?.*, b.numeric.?.*);
        },
        .int16, .int32, .int64 => std.math.order(a.value.integer, b.value.integer),
        .boolean => std.math.order(@intFromBool(a.value.bool), @intFromBool(b.value.bool)),
        .text, .uuid => budget.orderBytes(a.value.string, b.value.string),
        .jsonb => json_order.compare(a.value, b.value, budget, 0),
        .float32, .float64 => unreachable,
    };
}

fn hashElement(kind: ElementType, element: Element, budget: *Budget) !u64 {
    try budget.consume(1);
    if (element.sql_null) return 0x53514c4e554c4c;
    if (kind == .numeric) {
        var none = std.heap.FixedBufferAllocator.init(&.{});
        var ctx = budget.numericContext(none.allocator());
        defer budget.remaining = @intCast(ctx.remaining);
        var hash = std.hash.Wyhash.init(0);
        try @import("numeric_value.zig").hash(&ctx, element.numeric.?.*, &hash);
        return hash.final();
    }
    if (kind == .int16 or kind == .int32 or kind == .int64) {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(i64, &bytes, element.value.integer, .little);
        return std.hash.Wyhash.hash(2, &bytes);
    }
    if (kind == .float32 or kind == .float64) {
        const value = element.value.float;
        const bits: u64 = if (std.math.isNan(value)) 0x7ff8000000000000 else if (value == 0) 0 else @bitCast(value);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, bits, .little);
        return std.hash.Wyhash.hash(1, &bytes);
    }
    return json_order.hash(element.value, budget, 0);
}

pub const Owned = struct {
    arena: *std.heap.ArenaAllocator,
    budget: *MemoryBudget,
    value: Value,

    pub fn init(backing: Allocator, value: Value, limits: Limits) !Owned {
        const validated = try value.validate(limits);
        const budget = try backing.create(MemoryBudget);
        errdefer backing.destroy(budget);
        budget.* = .{ .backing = backing, .limit = limits.bytes };
        const arena = budget.allocator().create(std.heap.ArenaAllocator) catch |err| return allocationError(budget, err);
        errdefer budget.allocator().destroy(arena);
        arena.* = std.heap.ArenaAllocator.init(budget.allocator());
        errdefer arena.deinit();
        const owned = arena.allocator();
        const dimensions = owned.dupe(Dimension, validated.dimensions) catch |err| return allocationError(budget, err);
        if (validated.stored) |stored| {
            const copy = owned.create(@import("../common/sql_array_layout.zig").View) catch |err| return allocationError(budget, err);
            copy.* = stored.*;
            copy.bytes = owned.dupe(u8, stored.bytes) catch |err| return allocationError(budget, err);
            return .{ .arena = arena, .budget = budget, .value = .{ .element_type = validated.element_type, .dimensions = dimensions, .stored = copy } };
        }

        const elements = owned.alloc(Element, value.cardinality()) catch |err| return allocationError(budget, err);
        for (elements, 0..) |*out, index| out.* = operators.cloneDatum(owned, value.flatCell(index)) catch |err| return allocationError(budget, err);
        return .{ .arena = arena, .budget = budget, .value = .{ .element_type = validated.element_type, .dimensions = dimensions, .elements = elements, .prepared_footprint = validated.prepared_footprint } };
    }

    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.budget.allocator().destroy(self.arena);
        const backing = self.budget.backing;
        std.debug.assert(self.budget.live == 0);
        backing.destroy(self.budget);
        self.* = undefined;
    }
};

fn allocationError(budget: *MemoryBudget, err: anyerror) anyerror {
    return if (err == error.OutOfMemory and budget.exhausted) error.SqlProgramLimitExceeded else err;
}

test "SQL typed array NUMERIC kernels retain shared admission and cancellation" {
    const exact = @import("numeric_value.zig");
    const Operation = enum { validate, compare, hash };
    const Harness = struct {
        calls: usize = 0,
        fn poll(ptr: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.calls == 2) return error.Canceled;
        }
        fn run(op: Operation, value: Value, work: *Budget) !void {
            switch (op) {
                .validate => _ = try Value.initWithBudget(.numeric, value.dimensions, value.elements, .{}, work),
                .compare => _ = try value.compare(value, work),
                .hash => _ = try value.semanticHash(work),
            }
        }
    };
    const digits: [1000]u16 = @splat(1);
    const number: exact.Value = .{ .digits = &digits, .weight = 999 };
    const value: Value = .{
        .element_type = .numeric,
        .dimensions = &.{.{ .length = 1 }},
        .elements = &.{.{ .sql_null = false, .numeric = &number }},
    };
    var denied = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    for ([_]Operation{ .validate, .compare, .hash }) |op| {
        var parent: exact.Context = .{ .alloc = denied.allocator() };
        var work: Budget = .{ .shared = &parent };
        const local_before = work.remaining;
        const parent_before = parent.remaining;
        try Harness.run(op, value, &work);
        try std.testing.expectEqual(local_before - work.remaining, parent_before - parent.remaining);
        try std.testing.expect(local_before - work.remaining >= digits.len);

        var cancel: Harness = .{};
        parent = .{ .alloc = denied.allocator(), .checkpoint = Harness.poll, .ptr = &cancel };
        work = .{ .shared = &parent };
        try std.testing.expectError(error.Canceled, Harness.run(op, value, &work));
        try std.testing.expectEqual(@as(usize, 2), cancel.calls);
        parent.checkpoint = null;
        parent.remaining = 8 * 1024 * 1024;
        work.remaining = 1_048_576;
        try std.testing.expectError(error.Canceled, Harness.run(op, value, &work));

        parent = .{ .alloc = denied.allocator() };
        work = .{ .shared = &parent, .remaining = 128 };
        try std.testing.expectError(error.SqlProgramLimitExceeded, Harness.run(op, value, &work));
        work.remaining = 1_048_576;
        try std.testing.expectError(error.SqlProgramLimitExceeded, Harness.run(op, value, &work));
    }
    var parent: exact.Context = .{ .alloc = denied.allocator(), .max_groups = 1 };
    var work: Budget = .{ .shared = &parent };
    try std.testing.expectError(error.SqlProgramLimitExceeded, Harness.run(.validate, value, &work));
    try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
}

test "SQL typed array quantified comparisons match PostgreSQL reference contracts" {
    const Case = struct {
        sql: []const u8,
        values: []const ?i64,
        lower: i32 = 1,
        shape: ?[]const u32 = null,
        scalar: ?i64,
        comparison: Comparison,
        quantifier: Quantifier,
        expected: ?bool,
    };
    const Fixture = struct { reference: []const u8, scope: []const u8, entries: []const Case };
    const a = std.testing.allocator;
    const fixture = try std.json.parseFromSlice(Fixture, a, @embedFile("fixtures/sql_array_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", fixture.value.reference);
    try std.testing.expectEqual(@as(usize, 18), fixture.value.entries.len);
    for (fixture.value.entries) |case| {
        const elements = try a.alloc(Element, case.values.len);
        defer a.free(elements);
        for (case.values, elements) |value, *out| out.* = if (value) |integer| Element.json(.{ .integer = integer }) else .{};
        var dimensions: [6]Dimension = undefined;
        const rank_count: usize = if (case.shape) |shape| shape.len else 1;
        if (case.shape) |shape| {
            for (shape, dimensions[0..rank_count]) |length_value, *out| out.* = .{ .length = length_value, .lower = case.lower };
        } else dimensions[0] = .{ .length = @intCast(elements.len), .lower = case.lower };
        const value = try Value.init(.int32, dimensions[0..rank_count], elements, .{});
        var work: Budget = .{};
        const actual = try value.quantified(if (case.scalar) |integer| Element.json(.{ .integer = integer }) else .{}, case.comparison, case.quantifier, &work);
        try std.testing.expectEqual(case.expected, actual);
    }
}

test "SQL typed arrays preserve dimensions lower bounds widths and NULL provenance" {
    const elements: []const Element = &.{ Element.json(.{ .integer = 1 }), .{}, Element.json(.{ .integer = 3 }), Element.json(.{ .integer = 4 }) };
    const value = try Value.init(.int32, &.{ .{ .length = 2, .lower = -1 }, .{ .length = 2, .lower = 0 } }, elements, .{});
    try std.testing.expectEqual(@as(?u32, 2), value.length(1));
    try std.testing.expectEqual(@as(?i32, -1), value.lower(1));
    try std.testing.expectEqual(@as(?i32, 0), value.upper(1));
    try std.testing.expectEqual(@as(i64, 4), value.at(&.{ 0, 1 }).?.value.integer);
    try std.testing.expect(value.at(&.{ -1, 1 }).?.sql_null);
    try std.testing.expect(value.at(&.{ -2, 0 }) == null);
    try std.testing.expect(value.at(&.{0}) == null);
    const empty = try Value.init(.text, &.{.{ .length = 0 }}, &.{}, .{});
    try std.testing.expectEqual(@as(usize, 0), empty.dimensions.len);
    try std.testing.expectEqual(@as(?u32, null), empty.length(1));
    try std.testing.expectEqual(@as(?usize, null), empty.rank());
    try std.testing.expectEqual(@as(usize, 0), empty.cardinality());
    try std.testing.expectEqual(@as(?usize, 2), value.rank());
    try std.testing.expectEqual(@as(usize, 4), value.cardinality());
    try std.testing.expectError(error.InvalidSqlArrayShape, Value.init(.int32, &.{.{ .length = 3 }}, elements, .{}));
    try std.testing.expectError(error.SqlNumericOutOfRange, Value.init(.int16, &.{.{ .length = 1 }}, &.{Element.json(.{ .integer = 32768 })}, .{}));
    try std.testing.expectError(error.SqlNumericOutOfRange, Value.init(.int32, &.{.{ .length = 1 }}, &.{Element.json(.{ .integer = 2147483648 })}, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{.{ .length = 2, .lower = std.math.maxInt(i32) }}, elements[0..2], .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{.{ .length = 1, .lower = std.math.maxInt(i32) }}, elements[0..1], .{}));
    const last = try Value.init(.int32, &.{.{ .length = 1, .lower = std.math.maxInt(i32) - 1 }}, elements[0..1], .{});
    try std.testing.expectEqual(@as(?i32, std.math.maxInt(i32) - 1), last.upper(1));
    try std.testing.expectError(error.SqlTypeMismatch, Value.init(.text, &.{.{ .length = 1 }}, &.{Element.json(.null)}, .{}));
    try std.testing.expectError(error.SqlUntranslatableCharacter, Value.init(.text, &.{.{ .length = 1 }}, &.{Element.json(.{ .string = "a\x00b" })}, .{}));
    try std.testing.expectError(error.SqlUntranslatableCharacter, Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.{ .string = "a\x00b" })}, .{}));
    try std.testing.expectError(error.SqlInvalidTextEncoding, Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.{ .string = "\xff" })}, .{}));
    try std.testing.expectError(error.SqlNumericOutOfRange, Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.{ .float = std.math.inf(f64) })}, .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{ .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 }, .{ .length = 1 } }, elements[0..1], .{}));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{.{ .length = 4 }}, elements, .{ .work = 3 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Value.init(.int32, &.{.{ .length = 4 }}, elements, .{ .bytes = 1 }));
    const json_null = try Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.null)}, .{});
    const sql_null = try Value.init(.jsonb, &.{.{ .length = 1 }}, &.{.{}}, .{});
    var budget: Budget = .{};
    try std.testing.expectEqual(std.math.Order.lt, try json_null.compare(sql_null, &budget));
    try std.testing.expect(try json_null.semanticHash(&budget) != try sql_null.semanticHash(&budget));
}

test "SQL typed array owned copies and membership clean up under allocation faults" {
    const Harness = struct {
        fn run(a: Allocator) !void {
            var payload: [4]u8 = "éx!".*;
            const view = try Value.init(.text, &.{.{ .length = 3, .lower = -3 }}, &.{ Element.json(.{ .string = &payload }), .{}, Element.json(.{ .string = &payload }) }, .{});
            var owned = try Owned.init(a, view, .{});
            defer owned.deinit();
            @memset(&payload, 'z');
            var index = try Membership.init(a, owned.value, .{});
            defer index.deinit();
            var work: Budget = .{};
            try std.testing.expectEqual(@as(?i32, -3), try index.firstPosition(Element.json(.{ .string = "éx!" }), &work));
            try std.testing.expectEqual(@as(?i32, -2), try index.firstPosition(.{}, &work));
            try std.testing.expectEqual(@as(usize, 1), index.count);
            const query = try Value.init(.text, &.{.{ .length = 2 }}, &.{ Element.json(.{ .string = "éx!" }), Element.json(.{ .string = "éx!" }) }, .{});
            try std.testing.expect(try index.contains(query, &work));
            try std.testing.expect(try index.overlaps(query, &work));
            const null_query = try Value.init(.text, &.{.{ .length = 1 }}, &.{.{}}, .{});
            try std.testing.expect(!try index.contains(null_query, &work));
            try std.testing.expect(!try index.overlaps(null_query, &work));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    const value = try Value.init(.int64, &.{.{ .length = 2 }}, &.{ Element.json(.{ .integer = 1 }), Element.json(.{ .integer = 2 }) }, .{});
    try std.testing.expectError(error.SqlProgramLimitExceeded, Owned.init(std.testing.allocator, value, .{ .bytes = 128 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Membership.init(std.testing.allocator, value, .{ .bytes = 128 }));
    var index = try Membership.init(std.testing.allocator, value, .{});
    defer index.deinit();
    var no_work: Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, index.contains(value, &no_work));
}

test "SQL typed array hashing follows PostgreSQL float and shape equality" {
    const a = try Value.init(.float64, &.{.{ .length = 3 }}, &.{ Element.json(.{ .float = -0.0 }), Element.json(.{ .float = std.math.inf(f64) }), Element.json(.{ .float = std.math.nan(f64) }) }, .{});
    const b = try Value.init(.float64, &.{.{ .length = 3 }}, &.{ Element.json(.{ .float = 0.0 }), Element.json(.{ .float = std.math.inf(f64) }), Element.json(.{ .float = -std.math.nan(f64) }) }, .{});
    var work: Budget = .{};
    try std.testing.expectEqual(std.math.Order.eq, try a.compare(b, &work));
    try std.testing.expectEqual(try a.semanticHash(&work), try b.semanticHash(&work));
    const shifted = try Value.init(.float64, &.{.{ .length = 3, .lower = 2 }}, b.elements, .{});
    try std.testing.expectEqual(std.math.Order.lt, try a.compare(shifted, &work));
    try std.testing.expect(try a.semanticHash(&work) != try shifted.semanticHash(&work));
    var index = try Membership.init(std.testing.allocator, a, .{});
    defer index.deinit();
    try std.testing.expect(try index.contains(b, &work));
}

test "SQL typed array prepared membership bounds repeated probe work without allocations" {
    const a = std.testing.allocator;
    const source_cells = try a.alloc(Element, 4096);
    defer a.free(source_cells);
    for (source_cells, 0..) |*cell, ordinal| cell.* = Element.json(.{ .integer = @intCast(ordinal) });
    var probe_cells: [256]Element = undefined;
    for (&probe_cells, 0..) |*cell, ordinal| cell.* = Element.json(.{ .integer = @intCast(3840 + ordinal) });
    const source = try Value.init(.int32, &.{.{ .length = 4096 }}, source_cells, .{});
    const probes = try Value.init(.int32, &.{.{ .length = 256 }}, &probe_cells, .{});
    const prepare_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    var index = try Membership.init(a, source, .{ .bytes = 512 * 1024 });
    defer index.deinit();
    const prepared_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - prepare_start;
    const retained = index.budget.live;
    var work: Budget = .{ .remaining = 20_000_000 };
    const scan_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..10) |_| for (0..probes.cardinality()) |array_cell_0| {
        const probe = probes.flatCell(array_cell_0);
        var matched = false;
        for (0..source.cardinality()) |candidate_index| if (try compareElement(.int32, probe, source.flatCell(candidate_index), &work) == .eq) {
            matched = true;
            break;
        };
        try std.testing.expect(matched);
    };
    const scan_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - scan_start;
    const indexed_start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    work = .{ .remaining = 100000 };
    for (0..10) |_| try std.testing.expect(try index.contains(probes, &work));
    const indexed_ns = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - indexed_start;
    try std.testing.expectEqual(retained, index.budget.live);
    // Bucket collisions now spend work explicitly, unlike the former opaque
    // hash-map probes. Keep lookup work bounded per probe, far below a replay
    // of the 4096-cell source, without depending on a specific hash layout.
    const spent = 100000 - work.remaining;
    try std.testing.expect(spent <= 8 * probes.cardinality() * 10);
    std.debug.print("SQL array membership: source_elements=4096 probe_elements=256 iterations=10 prepare_ns={} scan_ns={} indexed_ns={} retained_bytes={} probe_work={} probe_allocated_bytes=0\n", .{ prepared_ns, scan_ns, indexed_ns, retained, spent });
}

/// One evaluation owns this scratch; immutable statement indexes use Membership
/// directly. Tiny probe sets scan borrowed storage without constructing a hash
/// table. Larger sets reclaim every growth buffer, even inside a result arena.
pub fn dynamicMembership(backing: Allocator, left: Value, right: Value, overlap: bool, limits: Limits, work: *Budget) !bool {
    try work.poll();
    if (left.element_type != right.element_type) return error.SqlTypeMismatch;
    // Shortcuts must retain ingress validation, quota and cancellation checks.
    const a = try left.validateWithBudget(limits, work);
    const b = try right.validateWithBudget(limits, work);
    if (b.cardinality() == 0) return !overlap;
    if (a.cardinality() == 0) return false;
    const scan_swapped = overlap and a.cardinality() < b.cardinality();
    const source = if (scan_swapped) b else a;
    const probes = if (scan_swapped) a else b;
    if (probes.cardinality() <= 4) {
        for (0..probes.cardinality()) |i| {
            const probe = probes.flatCell(i);
            try work.consume(1);
            // Containment equality is strict: NULL is never a matching cell.
            const found = !probe.sql_null and ((try source.quantified(probe, .eq, .any, work)) orelse false);
            if (overlap and found) return true;
            if (!overlap and !found) return false;
        }
        return !overlap;
    }
    // A small open-addressed table lives entirely on the stack. Equality
    // verifies collisions; duplicate probes share a single matched bit.
    if (probes.cardinality() <= 64) return smallMembership(source, probes, overlap, work);
    const swapped = b.cardinality() < a.cardinality();
    // Metadata is allocated too; include it in the temporary memory allowance.
    if (limits.bytes < @sizeOf(MemoryBudget)) return error.SqlProgramLimitExceeded;
    var scratch_limits = limits;
    scratch_limits.bytes -= @sizeOf(MemoryBudget);
    var index = try Membership.initValidated(MemoryBudget.reclaimingAllocator(backing), if (swapped) b else a, scratch_limits);
    defer index.deinit();
    try index.populate(work);
    if (overlap) return index.overlaps(if (swapped) a else b, work);
    if (!swapped) return index.contains(b, work);
    return index.containedBy(a, work);
}

fn smallMembership(source: Value, probes: Value, overlap: bool, work: *Budget) !bool {
    const Slot = struct { hash: u64 = 0, ordinal: ?u8 = null, matched: bool = false };
    var slots: [128]Slot = @splat(.{});
    var remaining: usize = 0;
    for (0..probes.cardinality()) |i| {
        const cell = probes.flatCell(i);
        try work.consume(1);
        if (cell.sql_null) {
            if (!overlap) return false;
            continue;
        }
        const hash = try hashElement(probes.element_type, cell, work);
        var bucket: usize = @intCast(hash & 127);
        while (slots[bucket].ordinal) |ordinal| {
            try work.consume(1);
            if (slots[bucket].hash == hash and try compareElement(probes.element_type, probes.flatCell(ordinal), cell, work) == .eq) break;
            bucket = (bucket + 1) & 127;
        } else {
            slots[bucket] = .{ .hash = hash, .ordinal = @intCast(i) };
            remaining += 1;
        }
    }
    if (remaining == 0) return !overlap;
    for (0..source.cardinality()) |i| {
        const cell = source.flatCell(i);
        try work.consume(1);
        if (cell.sql_null) continue;
        const hash = try hashElement(source.element_type, cell, work);
        var bucket: usize = @intCast(hash & 127);
        while (slots[bucket].ordinal) |ordinal| {
            const entry = &slots[bucket];
            try work.consume(1);
            if (entry.hash == hash and try compareElement(source.element_type, probes.flatCell(ordinal), cell, work) == .eq) {
                if (overlap) return true;
                if (!entry.matched) {
                    entry.matched = true;
                    remaining -= 1;
                }
                if (remaining == 0) return true;
                break;
            }
            bucket = (bucket + 1) & 127;
        }
    }
    return false;
}

pub const QuantifiedResult = struct { result: ?bool, visited: usize };

/// Reusable equality membership, O(n) build and expected O(m) probes. The
/// immutable array owner must outlive this index. NULLs never satisfy @>/&&;
/// firstPosition separately uses IS NOT DISTINCT FROM semantics.
pub const Membership = struct {
    const empty = std.math.maxInt(u32);
    budget: *MemoryBudget,
    value: Value,
    // Each occupied bucket borrows the first matching source ordinal. Array
    // cardinality is bounded by i32, leaving an unambiguous empty sentinel.
    // One allocation contains ordinals followed by byte fingerprints. The
    // fingerprints avoid decoding/comparing unrelated colliding text keys.
    buffer: []u32 = &.{},
    slots: []u32 = &.{},
    tags: []u8 = &.{},
    count: usize = 0,
    first_null: ?usize = null,

    pub fn init(backing: Allocator, value: Value, limits: Limits) !Membership {
        var work: Budget = .{ .remaining = limits.work };
        return initWithBudget(backing, value, limits, &work);
    }

    pub fn initWithBudget(backing: Allocator, value: Value, limits: Limits, work: *Budget) !Membership {
        return initBorrowedWithBudget(backing, value, limits, limits.bytes, work);
    }

    /// Source admission belongs to the immutable input owner; index_bytes
    /// bounds only new index storage. Validation and construction still share
    /// the caller's work/cancellation budget. The owner must outlive the index.
    pub fn initBorrowedWithBudget(backing: Allocator, value: Value, source_limits: Limits, index_bytes: usize, work: *Budget) !Membership {
        const validated = try value.validateWithBudget(source_limits, work);
        var index = try initValidated(backing, validated, .{ .bytes = index_bytes });
        errdefer index.deinit();
        try index.populate(work);
        return index;
    }

    /// The frame registry admits and owns this immutable source before it
    /// publishes the capability. Resolve it through that owner rather than
    /// accepting arbitrary Values or treating footprint metadata as proof.
    /// The frame must outlive the index; allocation and cancellation remain
    /// independently bounded by this construction's budget.
    pub fn initFrameOwnedWithBudget(backing: Allocator, source: *const @import("parameter_frame.zig").Frame.ArraySource, index_bytes: usize, work: *Budget) !Membership {
        try work.consume(1);
        try work.poll();
        const value = source.borrowValidated();
        var index = try initValidated(backing, value, .{ .bytes = index_bytes });
        errdefer index.deinit();
        try index.populate(work);
        return index;
    }

    fn initValidated(backing: Allocator, value: Value, limits: Limits) !Membership {
        const budget = try backing.create(MemoryBudget);
        errdefer backing.destroy(budget);
        budget.* = .{ .backing = backing, .limit = limits.bytes };
        return .{ .budget = budget, .value = value };
    }

    /// Memoize only an affordable prefix of expensive keys. Source ordinals
    /// address hashes directly; no map, per-bucket hash, or retained sidecar is
    /// needed. Require headroom for every table allocation even behind
    /// a monotonic region, so duplicates and tight quotas keep the small path.
    fn buildHashes(index: *Membership) ![]u64 {
        const n = index.value.cardinality();
        if (n < 32 or (index.value.element_type != .text and index.value.element_type != .jsonb)) return &.{};
        if (try index.value.retainedBytes() / n < 256) return &.{};
        const target = std.math.mul(usize, n, 2) catch return &.{};
        const capacity = std.math.ceilPowerOfTwo(usize, target) catch return &.{};
        const scratch = MemoryBudget.reclaimingAllocator(index.budget.allocator());
        const available = MemoryBudget.headroom(scratch).?;
        // Five bytes per bucket, with the geometric sequence of allocations
        // totaling less than twice the final capacity in a monotonic region.
        const table_bytes = std.math.mul(usize, capacity, 10) catch return &.{};
        if (table_bytes >= available) return &.{};
        // Use the affordable prefix, rather than dropping the entire memo
        // when a few bytes are missing from the preferred prefix allowance.
        const count: usize = @min(n, 8192, available / 16 / @sizeOf(u64), (available - table_bytes) / @sizeOf(u64));
        if (count < 32) return &.{};
        return scratch.alloc(u64, count) catch |err| return allocationError(index.budget, err);
    }

    fn populate(index: *Membership, work: *Budget) !void {
        switch (index.value.element_type) {
            .text, .jsonb => try index.populateWithHashes(work, true),
            else => try index.populateWithHashes(work, false),
        }
    }

    fn populateWithHashes(index: *Membership, work: *Budget, comptime memoize: bool) !void {
        var hashes: []u64 = &.{};
        defer if (memoize) MemoryBudget.reclaimingAllocator(index.budget.allocator()).free(hashes);
        var considered_hashes = false;
        const value = index.value;
        for (0..value.cardinality()) |ordinal| {
            const element = value.flatCell(ordinal);
            try work.consume(1);
            if (element.sql_null) {
                if (index.first_null == null) index.first_null = ordinal;
                continue;
            }
            const hash = try hashElement(value.element_type, element, work);
            const found = try index.searchSlot(element, hash, work);
            if (found.found) continue;
            // Only growth invalidates the vacant bucket from the equality
            // search. Distinct cardinality still controls table capacity.
            const bucket = if (index.count >= index.slots.len / 2) bucket: {
                var initialized = true;
                // Wait for distinct keys to require growth: a large source
                // containing a few repeated values never allocates a memo.
                if (memoize and !considered_hashes and index.count >= 8) {
                    considered_hashes = true;
                    hashes = try index.buildHashes();
                    initialized = false;
                }
                try index.grow(work, hashes, initialized);
                var at: usize = @intCast(hash & (index.slots.len - 1));
                while (index.slots[at] != empty) {
                    try work.consume(1);
                    at = (at + 1) & (index.slots.len - 1);
                }
                break :bucket at;
            } else found.bucket;
            if (ordinal < hashes.len) hashes[ordinal] = hash;
            index.slots[bucket] = @intCast(ordinal);
            index.tags[bucket] = @truncate(hash >> 56);
            index.count += 1;
        }
    }

    fn grow(self: *Membership, work: *Budget, hashes: []u64, initialized: bool) !void {
        const a = self.budget.allocator();
        const capacity = if (self.slots.len == 0) @as(usize, 16) else std.math.mul(usize, self.slots.len, 2) catch return error.SqlProgramLimitExceeded;
        const words = std.math.add(usize, capacity, capacity / 4) catch return error.SqlProgramLimitExceeded;
        const buffer = a.alloc(u32, words) catch |err| return allocationError(self.budget, err);
        errdefer a.free(buffer);
        const slots = buffer[0..capacity];
        const tags = std.mem.sliceAsBytes(buffer[capacity..]);
        @memset(slots, empty);
        for (self.slots) |ordinal| {
            try work.consume(1);
            if (ordinal == empty) continue;
            const hash = if (initialized and ordinal < hashes.len) cached: {
                try work.consume(1);
                break :cached hashes[ordinal];
            } else try hashElement(self.value.element_type, self.value.flatCell(ordinal), work);
            if (ordinal < hashes.len) hashes[ordinal] = hash;
            var bucket: usize = @intCast(hash & (slots.len - 1));
            while (slots[bucket] != empty) {
                try work.consume(1);
                bucket = (bucket + 1) & (slots.len - 1);
            }
            slots[bucket] = ordinal;
            tags[bucket] = @truncate(hash >> 56);
        }
        a.free(self.buffer);
        self.buffer = buffer;
        self.slots = slots;
        self.tags = tags;
    }

    pub fn deinit(self: *Membership) void {
        self.budget.allocator().free(self.buffer);
        const backing = self.budget.backing;
        std.debug.assert(self.budget.live == 0);
        backing.destroy(self.budget);
        self.* = undefined;
    }

    const Search = struct { bucket: usize = 0, found: bool = false };
    fn searchSlot(self: *const Membership, element: Element, hash: u64, work: *Budget) !Search {
        if (self.slots.len == 0) return .{};
        var bucket: usize = @intCast(hash & (self.slots.len - 1));
        while (self.slots[bucket] != empty) {
            if (self.tags[bucket] == @as(u8, @truncate(hash >> 56))) {
                // Equality already charges this occupied bucket's visit.
                if (try compareElement(self.value.element_type, self.value.flatCell(self.slots[bucket]), element, work) == .eq) return .{ .bucket = bucket, .found = true };
            } else try work.consume(1);
            bucket = (bucket + 1) & (self.slots.len - 1);
        }
        return .{ .bucket = bucket };
    }

    fn findSlot(self: *const Membership, element: Element, hash: u64, work: *Budget) !?usize {
        const result = try self.searchSlot(element, hash, work);
        return if (result.found) result.bucket else null;
    }

    fn find(self: *const Membership, element: Element, hash: u64, work: *Budget) !?usize {
        return if (try self.findSlot(element, hash, work)) |slot| self.slots[slot] else null;
    }

    pub fn contains(self: *const Membership, other: Value, work: *Budget) !bool {
        if (self.value.element_type != other.element_type) return error.SqlTypeMismatch;
        for (0..other.cardinality()) |array_cell_0| {
            const element = other.flatCell(array_cell_0);
            try work.consume(1);
            if (element.sql_null) return false;
            if (try self.find(element, try hashElement(other.element_type, element, work), work) == null) return false;
        }
        return true;
    }

    pub fn overlaps(self: *const Membership, other: Value, work: *Budget) !bool {
        if (self.value.element_type != other.element_type) return error.SqlTypeMismatch;
        for (0..other.cardinality()) |array_cell_0| {
            const element = other.flatCell(array_cell_0);
            try work.consume(1);
            if (element.sql_null) continue;
            if (try self.find(element, try hashElement(other.element_type, element, work), work) != null) return true;
        }
        return false;
    }

    /// This index describes the required probes. Scan the larger source once
    /// and track distinct entries, independently of dimensions/lower bounds.
    fn containedBy(self: *Membership, source: Value, work: *Budget) !bool {
        if (self.first_null != null) return false;
        var remaining = self.count;
        if (remaining == 0) return true;
        const a = self.budget.allocator();
        const matched = a.alloc(u8, (self.slots.len + 7) / 8) catch |err| return allocationError(self.budget, err);
        defer a.free(matched);
        @memset(matched, 0);
        for (0..source.cardinality()) |i| {
            const cell = source.flatCell(i);
            try work.consume(1);
            if (cell.sql_null) continue;
            const hash = try hashElement(source.element_type, cell, work);
            if (try self.findSlot(cell, hash, work)) |slot| {
                const mask = @as(u8, 1) << @intCast(slot % 8);
                if (matched[slot / 8] & mask == 0) {
                    matched[slot / 8] |= mask;
                    remaining -= 1;
                }
                if (remaining == 0) return true;
            }
        }
        return false;
    }

    /// Hashed equality ANY preserves SQL NULL provenance. The caller excludes
    /// mixed/exact numeric comparisons that require a different hash domain.
    pub fn equalAny(self: *const Membership, element: Element, work: *Budget) !?bool {
        const validated_hash = try validateElementHash(switch (self.value.element_type) {
            .int16, .int32 => .int64,
            .float32 => .float64,
            else => self.value.element_type,
        }, element, work);
        try work.consume(1);
        if (self.value.cardinality() == 0) return false;
        if (element.sql_null) return null;
        if (try self.find(element, validated_hash orelse try hashElement(self.value.element_type, element, work), work) != null) return true;
        return if (self.first_null != null) null else false;
    }

    pub fn firstPosition(self: *const Membership, element: Element, work: *Budget) !?i32 {
        const validated_hash = try validateElementHash(self.value.element_type, element, work);
        if (self.value.dimensions.len > 1) return error.UnsupportedSqlShape;
        if (self.value.dimensions.len == 0) return null;
        const ordinal = if (element.sql_null) self.first_null else try self.find(element, validated_hash orelse try hashElement(self.value.element_type, element, work), work);
        return if (ordinal) |at| @intCast(@as(i64, self.value.dimensions[0].lower) + @as(i64, @intCast(at))) else null;
    }
};

test "SQL prepared array footprint survives copies without bypassing validation quotas" {
    const a = std.testing.allocator;
    const cases = [_]Value{
        try Value.init(.int64, &.{.{ .length = 3, .lower = -7 }}, &.{ Element.json(.{ .integer = 7 }), .{}, Element.json(.{ .integer = 9 }) }, .{}),
        try Value.init(.text, &.{.{ .length = 2 }}, &.{ Element.json(.{ .string = "é" }), .{} }, .{}),
        try Value.init(.jsonb, &.{.{ .length = 2 }}, &.{ Element.json(.null), .{} }, .{}),
        try Value.init(.int64, &.{.{ .length = 0, .lower = -7 }}, &.{}, .{}),
    };
    for (cases) |value| {
        var uncached = value;
        uncached.prepared_footprint = null;
        try std.testing.expectEqual(try uncached.retainedBytes(), value.prepared_footprint.?);
        var owned = try Owned.init(a, value, .{});
        defer owned.deinit();
        try std.testing.expectEqual(value.prepared_footprint, owned.value.prepared_footprint);
        for ([_]Value{ value, uncached }) |source| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const copy = (try operators.cloneDatum(arena.allocator(), Element.typedArray(&source))).array.?;
            try std.testing.expect(copy.prepared_footprint != null);
            try std.testing.expectEqual(try value.retainedBytes(), try copy.retainedBytes());
            try std.testing.expectEqual(try operators.datumBytes(Element.typedArray(&value)), try operators.datumBytes(Element.typedArray(copy)));
        }
    }
    const value = cases[0];
    try std.testing.expectError(error.SqlProgramLimitExceeded, value.validate(.{ .elements = 2 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, value.validate(.{ .bytes = value.prepared_footprint.? - 1 }));
    try std.testing.expectError(error.SqlProgramLimitExceeded, value.validate(.{ .work = 0 }));
    // Sizing metadata does not authenticate a replacement shape or payload.
    var changed = value;
    changed.elements = &.{ Element.json(.{ .string = "invalid" }), .{}, .{} };
    try std.testing.expectError(error.SqlTypeMismatch, changed.validate(.{}));
    changed = value;
    changed.dimensions = &.{.{ .length = 4 }};
    try std.testing.expectError(error.InvalidSqlArrayShape, changed.validate(.{}));
}

test "SQL prepared array footprint page sizing measurement" {
    const a = std.testing.allocator;
    const elements = try a.alloc(Element, 65536);
    defer a.free(elements);
    for (elements, 0..) |*element, i| element.* = Element.json(.{ .integer = @intCast(i) });
    const cached = try Value.init(.int64, &.{.{ .length = 65536 }}, elements, .{});
    var uncached = cached;
    uncached.prepared_footprint = null;
    var totals: [2]usize = @splat(0);
    var elapsed: [2]i128 = undefined;
    for ([_]*const Value{ &uncached, &cached }, 0..) |value, mode| {
        const column: [64]Element = @splat(Element.typedArray(value));
        const columns = [_][]const Element{&column};
        const batch: @import("execution_batch.zig").Batch = .{ .vectors = .{ .values = &columns, .count = 64 } };
        const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
        for (0..20) |_| for (0..64) |row| {
            const size = try batch.cellBytes(a, row, 0);
            std.mem.doNotOptimizeAway(size);
            totals[mode] +|= size;
        };
        elapsed[mode] = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start;
    }
    try std.testing.expectEqual(totals[0], totals[1]);
    std.debug.print("SQL_PREPARED_ARRAY_SIZING elements=65536 calls=1280 uncached_ns={} cached_ns={} sizing_allocations=0\n", .{ elapsed[0], elapsed[1] });
}

test "SQL native transform plans retain sparse sizing no-op reuse and exact text ownership" {
    const a = std.testing.allocator;
    const compact = @import("array_compact.zig");
    const Fixture = struct {
        fn run(backing: Allocator) !void {
            var source_arena = std.heap.ArenaAllocator.init(backing);
            defer source_arena.deinit();
            var work: Budget = .{};
            var builder = try compact.ResultBuilder.initKnown(source_arena.allocator(), .int64, &.{.{ .length = 32768, .lower = -7 }}, 32768, 1, .{});
            for (0..32768) |i| try builder.append(if (i == 9) .json(.{ .integer = 42 }) else .{}, &work);
            const source = try builder.finish(source_arena.allocator(), .{}, &work);
            const same = try source.transform(std.testing.failing_allocator, .json(.{ .integer = 99 }), null, .{ .bytes = 64 * 1024 }, &work);
            try std.testing.expect(same.stored == source.stored);
            const same_replace = try source.transform(std.testing.failing_allocator, .json(.{ .integer = 99 }), .{}, .{ .bytes = 64 * 1024 }, &work);
            try std.testing.expect(same_replace.stored == source.stored);
            var counter: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = backing, .limit = 64 * 1024 };
            {
                var arena = std.heap.ArenaAllocator.init(counter.allocator());
                defer arena.deinit();
                var nr: @import("../storage/lite/test_allocator.zig").NoResizeAllocator = .{ .backing = arena.allocator() };
                const result = try source.transform(nr.allocator(), .json(.{ .integer = 42 }), .{}, .{ .bytes = 64 * 1024 }, &work);
                try std.testing.expectEqual(@as(?usize, 0), try result.knownNonNullCount());
                try std.testing.expect(result.flatCell(9).sql_null);
                try std.testing.expectEqual(@as(i32, -7), result.dimensions[0].lower);
                _ = try @import("../common/sql_array_layout.zig").View.open(.int64, result.stored.?.bytes, .{});
            }
            try std.testing.expectEqual(@as(usize, 0), counter.live);
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            var text_builder = try compact.CheckedBuilder.init(source_arena.allocator(), .text, &.{.{ .length = 129, .lower = -3 }}, 129, 1, 5, .{});
            for (0..129) |i| try text_builder.appendChecked(if (i == 7) .json(.{ .string = "é☺" }) else .{}, .text, .{}, &work);
            const text = try text_builder.finish(source_arena.allocator(), .{});
            const result = try text.transform(arena.allocator(), .{}, .json(.{ .string = "new" }), .{}, &work);
            try std.testing.expect(result.stored != null);
            try std.testing.expectEqual(@as(?usize, 129), try result.knownNonNullCount());
            _ = source_arena.reset(.free_all);
            try std.testing.expectEqualStrings("é☺", result.flatCell(7).value.string);
            try std.testing.expectEqualStrings("new", result.flatCell(8).value.string);
            var denied: Budget = .{ .remaining = 0 };
            try std.testing.expectError(error.SqlProgramLimitExceeded, result.transform(std.testing.failing_allocator, .json(.{ .string = "missing" }), null, .{}, &denied));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{});
}

test "SQL native transform custom larger inputs retain bounded scratch replay" {
    const a = std.testing.allocator;
    var source_arena = std.heap.ArenaAllocator.init(a);
    defer source_arena.deinit();
    const cells = try source_arena.allocator().alloc(Element, 65537);
    @memset(cells, .{});
    cells[65536] = .json(.{ .integer = 42 });
    var work: Budget = .{};
    const source = try Value.initWithBudget(.int64, &.{.{ .length = 65537, .lower = -7 }}, cells, .{ .elements = 65537 }, &work);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const result = try source.transform(arena.allocator(), .json(.{ .integer = 42 }), null, .{}, &work);
    try std.testing.expectEqual(@as(usize, 65536), result.cardinality());
    try std.testing.expectEqual(@as(?usize, 0), try result.knownNonNullCount());
    _ = try @import("../common/sql_array_layout.zig").View.open(.int64, result.stored.?.bytes, .{});
}

test "SQL native bitmap transforms match opaque scalar NULL NaN text and removal semantics" {
    const a = std.testing.allocator;
    const compact = @import("array_compact.zig");
    for ([_]ElementType{ .boolean, .int16, .int32, .int64, .float32, .float64, .text }) |kind| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const region = arena.allocator();
        var cells: [129]Element = undefined;
        var present: usize = 0;
        var payload: usize = 0;
        for (&cells, 0..) |*cell, i| {
            cell.* = if (i % 3 == 0) .{} else .json(switch (kind) {
                .boolean => .{ .bool = i % 2 == 0 },
                .float32, .float64 => .{ .float = if (i % 8 == 7) std.math.nan(f64) else if (i % 8 == 1) -0.0 else @floatFromInt(i % 5) },
                .text => .{ .string = if (i % 2 == 0) "é☺" else "other" },
                else => .{ .integer = @intCast(i % 5) },
            });
            if (!cell.sql_null) {
                present += 1;
                if (kind == .text) payload += cell.value.string.len;
            }
        }
        var builder = try compact.CheckedBuilder.init(region, kind, &.{.{ .length = 129, .lower = -7 }}, 129, present, payload, .{});
        var build_work: Budget = .{};
        for (cells) |cell| try builder.appendChecked(cell, kind, .{}, &build_work);
        const source = try builder.finish(region, .{});
        var view = source.stored.?.*;
        view.validation = null;
        var unproven = source;
        unproven.stored = &view;
        for ([_]Element{ .{}, cells[1], cells[7] }) |needle| for ([_]?Element{ null, Element{}, cells[2] }) |replacement| {
            var fast_work: Budget = .{};
            var slow_work: Budget = .{};
            const fast = try source.transform(region, needle, replacement, .{}, &fast_work);
            const slow = try unproven.transform(region, needle, replacement, .{}, &slow_work);
            try std.testing.expectEqual(slow_work.remaining, fast_work.remaining);
            const expected = try @import("array_storage.zig").encodeAlloc(region, slow, .{});
            const actual = try @import("array_storage.zig").encodeAlloc(region, fast, .{});
            try std.testing.expectEqualSlices(u8, expected, actual);
            _ = try @import("../common/sql_array_layout.zig").View.open(kind, actual, .{});
        };
    }
}

test "SQL native removal skips matched runs and empty outputs with exact work parity" {
    const a = std.testing.allocator;
    for (0..3) |pattern| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const region = arena.allocator();
        const count = 65536;
        const survivors: usize = if (pattern == 0) 0 else 17;
        var builder = try @import("array_compact.zig").CheckedBuilder.init(region, .int64, &.{.{ .length = count, .lower = -19 }}, count, if (pattern == 2) count else survivors, 0, .{});
        var build_work: Budget = .{};
        for (0..count) |i| try builder.appendChecked(if (pattern != 0 and (i % 4096 == 0 or i == count - 1)) .json(.{ .integer = 9 }) else if (pattern == 2) .json(.{ .integer = 7 }) else .{}, .int64, .{}, &build_work);
        const source = try builder.finish(region, .{});
        var view = source.stored.?.*;
        view.validation = null;
        var unproven = source;
        unproven.stored = &view;
        const needle: Element = if (pattern == 2) .json(.{ .integer = 7 }) else .{};
        var oracle: ?[]const u8 = null;
        var expected_work: ?usize = null;
        for (0..2) |mode| {
            var elapsed: i96 = 0;
            for (0..20) |_| {
                var output_arena = std.heap.ArenaAllocator.init(a);
                defer output_arena.deinit();
                var work: Budget = .{};
                const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
                const result = try (if (mode == 0) unproven else source).transform(output_arena.allocator(), needle, null, .{}, &work);
                elapsed += @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start);
                try std.testing.expectEqual(survivors, result.cardinality());
                try std.testing.expectEqual(@as(usize, if (survivors == 0) 0 else 1), result.dimensions.len);
                if (survivors != 0) try std.testing.expectEqual(@as(i32, -19), result.dimensions[0].lower);
                const bytes = try @import("array_storage.zig").encodeAlloc(region, result, .{});
                if (oracle) |expected| try std.testing.expectEqualSlices(u8, expected, bytes) else oracle = bytes;
                if (expected_work) |expected| try std.testing.expectEqual(expected, work.remaining) else expected_work = work.remaining;
            }
            std.debug.print("SQL_REMOVAL_RUNS pattern={} native={} repetitions=20 elapsed_ns={} identical_canonical_and_work=true\n", .{ pattern, mode == 1, elapsed });
        }
        var denied: Budget = .{ .remaining = 1 };
        try std.testing.expectError(error.SqlProgramLimitExceeded, source.transform(std.testing.failing_allocator, needle, null, .{}, &denied));
    }
}

test "SQL native matching cursors preserve dense packed tails text NaNs zeros and work" {
    const a = std.testing.allocator;
    for ([_]ElementType{ .boolean, .int16, .int32, .int64, .float32, .float64, .text }) |kind| for ([_]usize{ 1, 31, 32, 63, 64, 65, 129, 1024 }) |count| for (0..3) |pattern| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const region = arena.allocator();
        const cells = try region.alloc(Element, count);
        var present: usize = 0;
        var payload: usize = 0;
        for (cells, 0..) |*cell, i| {
            cell.* = if (pattern == 2 or (pattern == 1 and i % 17 != 0)) .{} else .json(switch (kind) {
                .boolean => .{ .bool = i % 2 == 0 },
                .int16, .int32, .int64 => .{ .integer = @intCast(i % 7) },
                .float32, .float64 => .{ .float = if (i % 7 == 1) std.math.nan(f64) else if (i % 7 == 0) 0.0 else if (i % 7 == 4) -0.0 else @as(f64, @floatFromInt(i % 7)) * 0.5 },
                .text => .{ .string = if (i % 3 == 0) "é☺" else "other" },
                else => unreachable,
            });
            if (!cell.sql_null) {
                present += 1;
                if (kind == .text) payload += cell.value.string.len;
            }
        }
        var builder = try @import("array_compact.zig").CheckedBuilder.init(region, kind, &.{.{ .length = @intCast(count), .lower = -7 }}, count, present, payload, .{});
        var build_work: Budget = .{};
        for (cells) |cell| try builder.appendChecked(cell, kind, .{}, &build_work);
        const source = try builder.finish(region, .{});
        var view = source.stored.?.*;
        view.validation = null;
        var opaque_source = source;
        opaque_source.stored = &view;
        const replacement: Element = .json(switch (kind) {
            .boolean => .{ .bool = false },
            .int16, .int32, .int64 => .{ .integer = 3 },
            .float32, .float64 => .{ .float = -0.0 },
            .text => .{ .string = "new" },
            else => unreachable,
        });
        for ([_]Element{ .{}, cells[0], cells[@min(1, count - 1)] }) |needle| for ([_]?Element{ null, Element{}, replacement }) |target| {
            var fast_work: Budget = .{};
            var slow_work: Budget = .{};
            const fast = try source.transform(region, needle, target, .{}, &fast_work);
            const slow = try opaque_source.transform(region, needle, target, .{}, &slow_work);
            try std.testing.expectEqual(slow_work.remaining, fast_work.remaining);
            try std.testing.expectEqualSlices(u8, try @import("array_storage.zig").encodeAlloc(region, slow, .{}), try @import("array_storage.zig").encodeAlloc(region, fast, .{}));
        };
    };
}

test "SQL identity replacements borrow only admitted representations and retain cancellation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var builder = try @import("array_compact.zig").ResultBuilder.initKnown(arena.allocator(), .int64, &.{.{ .length = 65536, .lower = -7 }}, 65536, 0, .{});
    var work: Budget = .{};
    for (0..65536) |_| try builder.append(.{}, &work);
    const source = try builder.finish(arena.allocator(), .{}, &work);
    const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..20) |_| {
        work = .{};
        const borrowed = try source.transform(std.testing.failing_allocator, .{}, Element{}, .{}, &work);
        try std.testing.expect(borrowed.stored == source.stored);
    }
    std.debug.print("SQL_IDENTITY_REPLACEMENT cells=65536 repetitions=20 allocations=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started});
    var denied: Budget = .{ .remaining = 2 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, source.transform(std.testing.failing_allocator, .{}, Element{}, .{}, &denied));
    var oversized = source;
    oversized.prepared_footprint = 1024 * 1024;
    work = .{};
    const rebuilt = try oversized.transform(arena.allocator(), .{}, Element{}, .{ .bytes = 64 * 1024 }, &work);
    try std.testing.expect(rebuilt.stored != source.stored);
    try std.testing.expect(try rebuilt.admissionBytes() <= 64 * 1024);
    const Cancel = struct {
        fn poll(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    var owner: @import("numeric_value.zig").Context = .{ .alloc = std.testing.failing_allocator, .checkpoint = Cancel.poll };
    var canceled: Budget = .{ .shared = &owner };
    try std.testing.expectError(error.Canceled, source.transform(std.testing.failing_allocator, .{}, Element{}, .{}, &canceled));
}

test "SQL native search preserves scalar results bounds and exact work" {
    const a = std.testing.allocator;
    for ([_]ElementType{ .boolean, .int16, .int32, .int64, .float32, .float64, .text }) |kind| {
        for ([_]usize{ 0, 1, 17, 65, 129 }) |count| for (0..3) |pattern| {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const region = arena.allocator();
            const cells = try region.alloc(Element, count);
            for (cells, 0..) |*cell, i| cell.* = if (pattern == 2 or (pattern == 1 and i % 7 != 0)) .{} else .json(switch (kind) {
                .boolean => .{ .bool = i % 2 == 0 },
                .float32, .float64 => .{ .float = if (i == 0) -0.0 else if (i % 11 == 0) std.math.nan(f64) else 42.0 },
                .text => .{ .string = if (i % 3 == 0) "é☺" else "other" },
                else => .{ .integer = if (i % 3 == 0) 42 else 7 },
            });
            const axes = [_]Dimension{.{ .length = @intCast(count), .lower = -7 }};
            const prepared = try Value.init(kind, axes[0..@intFromBool(count != 0)], cells, .{});
            const encoded = try @import("array_storage.zig").encodeAlloc(region, prepared, .{});
            const native = try @import("array_storage.zig").viewLeaky(region, kind, encoded, .{});
            var unproven_view = native.stored.?.*;
            unproven_view.validation = null;
            var oracle = native;
            oracle.stored = &unproven_view;
            for (0..4) |probe| {
                const needle: Element = if (probe == 0) .{} else .json(switch (kind) {
                    .boolean => .{ .bool = probe == 1 },
                    .float32, .float64 => .{ .float = if (probe == 1) -0.0 else if (probe == 2) std.math.nan(f64) else 999.0 },
                    .text => .{ .string = if (probe == 1) "é☺" else if (probe == 2) "other" else "missing" },
                    else => .{ .integer = if (probe == 1) 42 else if (probe == 2) 7 else 999 },
                });
                for ([_]?i32{ null, -99, -7, -1, 8, 56, 57, 58, 120, 500 }) |start| {
                    var fast: Budget = .{};
                    var slow: Budget = .{};
                    try std.testing.expectEqual(try oracle.position(needle, start, &slow), try native.position(needle, start, &fast));
                    try std.testing.expectEqual(slow.remaining, fast.remaining);
                }
                var fast: Budget = .{};
                var slow: Budget = .{};
                const expected = try oracle.positions(region, needle, .{}, &slow);
                const actual = try native.positions(region, needle, .{}, &fast);
                try std.testing.expectEqual(slow.remaining, fast.remaining);
                try std.testing.expectEqualSlices(u8, try @import("array_storage.zig").encodeAlloc(region, expected, .{}), try @import("array_storage.zig").encodeAlloc(region, actual, .{}));
                const used = (Budget{}).remaining - fast.remaining;
                if (used != 0) {
                    var denied: Budget = .{ .remaining = used - 1 };
                    try std.testing.expectError(error.SqlProgramLimitExceeded, native.positions(region, needle, .{}, &denied));
                }
            }
        };
    }
}

test "SQL native search measurements retain canonical output" {
    const a = std.testing.allocator;
    for ([_]ElementType{ .int64, .text }) |kind| {
        var source_arena = std.heap.ArenaAllocator.init(a);
        defer source_arena.deinit();
        const region = source_arena.allocator();
        const count: usize = 16384;
        var builder = try @import("array_compact.zig").ResultBuilder.initPlanned(region, kind, &.{.{ .length = count, .lower = -7 }}, count, count / 16, if (kind == .text) count / 16 * 5 else null, .{});
        var initial: Budget = .{};
        for (0..count) |i| try builder.append(if (i % 16 != 0) .{} else .json(if (kind == .text) .{ .string = "match" } else .{ .integer = 42 }), &initial);
        const source = try builder.finish(region, .{}, &initial);
        var unproven_view = source.stored.?.*;
        unproven_view.validation = null;
        var unproven = source;
        unproven.stored = &unproven_view;
        for (0..2) |mode| {
            var elapsed: i96 = 0;
            var counter: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 1024 * 1024 };
            for (0..20) |_| {
                var arena = std.heap.ArenaAllocator.init(counter.allocator());
                defer arena.deinit();
                const input = if (mode == 0) unproven else source;
                var work: Budget = .{};
                const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
                try std.testing.expectEqual(@as(?i32, null), try input.position(.json(if (kind == .text) .{ .string = "missing" } else .{ .integer = 99 }), null, &work));
                const result = try input.positions(arena.allocator(), .json(if (kind == .text) .{ .string = "match" } else .{ .integer = 42 }), .{}, &work);
                elapsed += @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start);
                try std.testing.expectEqual(count / 16, result.cardinality());
                for (0..result.cardinality()) |i| try std.testing.expectEqual(@as(i64, @intCast(i * 16)) - 7, result.flatCell(i).value.integer);
            }
            try std.testing.expectEqual(@as(usize, 0), counter.live);
            std.debug.print("SQL_NATIVE_SEARCH kind={s} native={} repetitions=20 peak={} elapsed_ns={}\n", .{ @tagName(kind), mode == 1, counter.peak, elapsed });
        }
    }
}

test "SQL native search unwinds allocation faults cancellation and oversized scratch fallback" {
    const a = std.testing.allocator;
    var source_arena = std.heap.ArenaAllocator.init(a);
    defer source_arena.deinit();
    const region = source_arena.allocator();
    var builder = try @import("array_compact.zig").ResultBuilder.initKnown(region, .int64, &.{.{ .length = 129 }}, 129, 1, .{});
    var initial: Budget = .{};
    for (0..129) |i| try builder.append(if (i == 128) .json(.{ .integer = 42 }) else .{}, &initial);
    const source = try builder.finish(region, .{}, &initial);
    const Fixture = struct {
        fn run(backing: Allocator, input: *const Value) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            var work: Budget = .{};
            const output = try input.positions(arena.allocator(), .json(.{ .integer = 42 }), .{}, &work);
            try std.testing.expectEqual(@as(i64, 129), output.flatCell(0).value.integer);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{&source});
    var exact: Budget = .{ .remaining = 2 };
    try std.testing.expectEqual(@as(?i32, 1), try source.position(.{}, null, &exact));
    try std.testing.expectEqual(@as(usize, 0), exact.remaining);
    const Cancel = struct {
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 2) return error.Canceled;
        }
    };
    var cancel: Cancel = .{};
    var context: @import("numeric_value.zig").Context = .{ .alloc = region, .checkpoint = Cancel.check, .ptr = &cancel };
    var work: Budget = .{ .shared = &context };
    try std.testing.expectError(error.Canceled, source.positions(region, .{}, .{}, &work));
    try std.testing.expectError(error.Canceled, work.consume(0));
    const count: usize = 70000;
    const limits: Limits = .{ .elements = count };
    var large_builder = try @import("array_compact.zig").ResultBuilder.initKnown(region, .int64, &.{.{ .length = count, .lower = -7 }}, count, 0, limits);
    var lw: Budget = .{};
    for (0..count) |_| try large_builder.append(.{}, &lw);
    const large = try large_builder.finish(region, limits, &lw);
    const output = try large.positions(region, .{}, limits, &lw);
    try std.testing.expectEqual(count, output.cardinality());
    try std.testing.expectEqual(@as(i64, -7), output.flatCell(0).value.integer);
    try std.testing.expectEqual(@as(i64, count - 8), output.flatCell(count - 1).value.integer);
}

test "SQL native dynamic membership matches indexes across representations and probe sizes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const region = arena.allocator();
    const number: @import("numeric_value.zig").Value = .{ .digits = &.{7}, .weight = 0 };
    const cases = [_]struct { kind: ElementType, cells: []const Element }{
        .{ .kind = .numeric, .cells = &.{ .{ .sql_null = false, .numeric = &number }, .{}, .{ .sql_null = false, .numeric = &number } } },
        .{ .kind = .uuid, .cells = &.{ .json(.{ .string = "00112233-4455-6677-8899-aabbccddeeff" }), .{}, .json(.{ .string = "ffeeddcc-bbaa-9988-7766-554433221100" }) } },
        .{ .kind = .boolean, .cells = &.{ .json(.{ .bool = true }), .{}, .json(.{ .bool = false }) } },
        .{ .kind = .int16, .cells = &.{ .json(.{ .integer = -7 }), .{}, .json(.{ .integer = 9 }) } },
        .{ .kind = .int32, .cells = &.{ .json(.{ .integer = -7 }), .{}, .json(.{ .integer = 9 }) } },
        .{ .kind = .int64, .cells = &.{ .json(.{ .integer = -7 }), .{}, .json(.{ .integer = 9 }) } },
        .{ .kind = .float32, .cells = &.{ .json(.{ .float = -0.0 }), .{}, .json(.{ .float = std.math.nan(f64) }) } },
        .{ .kind = .float64, .cells = &.{ .json(.{ .float = -0.0 }), .{}, .json(.{ .float = std.math.nan(f64) }) } },
        .{ .kind = .text, .cells = &.{ .json(.{ .string = "é" }), .{}, .json(.{ .string = "" }) } },
        .{ .kind = .jsonb, .cells = &.{ .json(.null), .{}, .json(.{ .string = "x" }) } },
    };
    for (cases) |case| {
        var cells: [96]Element = undefined;
        for (&cells, 0..) |*cell, i| cell.* = case.cells[i % 3];
        const prepared = try Value.init(case.kind, &.{ .{ .length = 3, .lower = -7 }, .{ .length = 32 } }, &cells, .{});
        var work: Budget = .{};
        var builder = try @import("array_compact.zig").ResultBuilder.initPlanned(region, case.kind, prepared.dimensions, 96, 64, if (case.kind == .text) 32 * (case.cells[0].value.string.len + case.cells[2].value.string.len) else null, .{});
        for (cells) |cell| try builder.append(cell, &work);
        const native = try builder.finish(region, .{}, &work);
        for ([_]Value{ prepared, native }) |source| {
            var index = try Membership.init(a, source, .{});
            defer index.deinit();
            for (case.cells) |probe| try std.testing.expectEqual(try source.quantified(probe, .eq, .any, &work), try index.equalAny(probe, &work));
            for ([_]usize{ 0, 1, 4, 5, 32, 64, 65, 96 }) |count| {
                const probes = try Value.init(case.kind, if (count == 0) &.{} else &.{.{ .length = @intCast(count), .lower = 4 }}, cells[0..count], .{});
                for ([_]bool{ false, true }) |overlap| {
                    const expected = if (overlap) try index.overlaps(probes, &work) else try index.contains(probes, &work);
                    const alloc = if (count <= 64) std.testing.failing_allocator else a;
                    try std.testing.expectEqual(expected, try dynamicMembership(alloc, source, probes, overlap, .{}, &work));
                    if (overlap) try std.testing.expectEqual(expected, try dynamicMembership(alloc, probes, source, true, .{}, &work));
                }
            }
            // Large duplicate probe sets exercise distinct-match accounting.
            var repeated: [65]Element = undefined;
            for (&repeated, 0..) |*cell, i| cell.* = case.cells[if (i % 2 == 0) 0 else 2];
            const duplicates = try Value.init(case.kind, &.{.{ .length = 65 }}, &repeated, .{});
            try std.testing.expect(try dynamicMembership(a, source, duplicates, false, .{}, &work));
            // Probe sets without NULLs test duplicate and NaN/zero matches.
            const probes = try Value.init(case.kind, &.{.{ .length = 3 }}, &.{ case.cells[2], case.cells[0], case.cells[0] }, .{});
            try std.testing.expect(try dynamicMembership(std.testing.failing_allocator, source, probes, false, .{}, &work));
            const missing: ?Element = switch (case.kind) {
                .int16, .int32, .int64 => .json(.{ .integer = 10000 }),
                .float32, .float64 => .json(.{ .float = 1.25 }),
                .text => .json(.{ .string = "missing" }),
                .uuid => .json(.{ .string = "11111111-1111-1111-1111-111111111111" }),
                .jsonb => .json(.{ .bool = false }),
                else => null,
            };
            if (missing) |cell| {
                const absent = try Value.init(case.kind, &.{.{ .length = 1 }}, &.{cell}, .{});
                for ([_]bool{ false, true }) |overlap| try std.testing.expect(!try dynamicMembership(std.testing.failing_allocator, source, absent, overlap, .{}, &work));
            }
            if (case.kind == .float32 or case.kind == .float64) {
                const equal = try Value.init(case.kind, &.{.{ .length = 2 }}, &.{ .json(.{ .float = 0.0 }), .json(.{ .float = -std.math.nan(f64) }) }, .{});
                try std.testing.expect(try dynamicMembership(std.testing.failing_allocator, source, equal, false, .{}, &work));
            }
        }
    }
}

test "SQL native dynamic membership reclaims indexed scratch and preserves admission failures" {
    const a = std.testing.allocator;
    var cells: [129]Element = undefined;
    for (&cells, 0..) |*cell, i| cell.* = .json(.{ .integer = @intCast(i) });
    const source = try Value.init(.int64, &.{.{ .length = cells.len }}, &cells, .{});
    const probes = try Value.init(.int64, &.{.{ .length = 65 }}, cells[64..], .{});
    const Fixture = struct {
        fn run(backing: Allocator, left: Value, right: Value) !void {
            var arena = std.heap.ArenaAllocator.init(backing);
            defer arena.deinit();
            var work: Budget = .{};
            try std.testing.expect(try dynamicMembership(arena.allocator(), left, right, false, .{}, &work));
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Fixture.run, .{ source, probes });
    var counter: @import("../storage/lite/test_allocator.zig").BudgetAllocator = .{ .backing = a, .limit = 1024 * 1024 };
    var arena = std.heap.ArenaAllocator.init(counter.allocator());
    defer arena.deinit();
    var work: Budget = .{};
    for (0..20) |_| {
        try std.testing.expect(try dynamicMembership(arena.allocator(), source, probes, false, .{}, &work));
        try std.testing.expectEqual(@as(usize, 0), counter.live);
    }
    var empty = try Value.init(.int64, &.{}, &.{}, .{});
    var invalid = source;
    invalid.element_type = .text;
    try std.testing.expectError(error.SqlTypeMismatch, dynamicMembership(a, invalid, empty, false, .{}, &work));
    invalid = source;
    invalid.dimensions = &.{.{ .length = 128 }};
    try std.testing.expectError(error.InvalidSqlArrayShape, dynamicMembership(a, invalid, empty, false, .{}, &work));
    try std.testing.expectError(error.SqlProgramLimitExceeded, dynamicMembership(a, source, empty, false, .{ .elements = 128 }, &work));
    try std.testing.expectError(error.SqlProgramLimitExceeded, dynamicMembership(a, source, probes, false, .{ .bytes = 4096 }, &work));
    var exhausted: Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, dynamicMembership(a, source, empty, false, .{}, &exhausted));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    var context: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Cancel.check };
    var canceled: Budget = .{ .shared = &context };
    try std.testing.expectError(error.Canceled, dynamicMembership(a, empty, empty, false, .{}, &canceled));
    empty.element_type = .text;
    try std.testing.expectError(error.SqlTypeMismatch, dynamicMembership(a, source, empty, true, .{}, &work));
}

test "SQL compact membership handles collisions duplicates growth and bounded memory" {
    const Harness = struct {
        fn run(a: Allocator) !void {
            var cells: [256]Element = undefined;
            // Deliberate shared low hash bits exercise collision equality and
            // insertion-order-independent probing across multiple table grows.
            var candidate: i64 = 0;
            var work: Budget = .{};
            for (&cells) |*cell| {
                while (true) : (candidate += 1) {
                    const element = Element.json(.{ .integer = candidate });
                    if ((try hashElement(.int64, element, &work)) & 31 == 0) {
                        cell.* = element;
                        candidate += 1;
                        break;
                    }
                }
            }
            const source = try Value.init(.int64, &.{.{ .length = cells.len, .lower = -7 }}, &cells, .{});
            var index = try Membership.init(a, source, .{});
            defer index.deinit();
            for (cells, 0..) |cell, i| try std.testing.expectEqual(@as(?i32, @as(i32, @intCast(i)) - 7), try index.firstPosition(cell, &work));
            try std.testing.expectEqual(@as(?bool, false), try index.equalAny(.json(.{ .integer = -1 }), &work));
            try std.testing.expectEqual(@as(usize, 256), index.count);
            try std.testing.expectEqual(index.slots.len * 5, index.budget.live);
        }
    };
    try Harness.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{});
    var repeated: [8192]Element = @splat(.json(.{ .integer = 42 }));
    repeated[31] = .{};
    const source = try Value.init(.int64, &.{.{ .length = repeated.len, .lower = -7 }}, &repeated, .{});
    var build_work: Budget = .{};
    var index = try Membership.initBorrowedWithBudget(std.testing.allocator, source, .{}, 256, &build_work);
    defer index.deinit();
    try std.testing.expectEqual(@as(usize, 1), index.count);
    try std.testing.expect(index.budget.peak <= 256);
    var work: Budget = .{};
    try std.testing.expectEqual(@as(?i32, -7), try index.firstPosition(.json(.{ .integer = 42 }), &work));
    try std.testing.expectEqual(@as(?i32, 24), try index.firstPosition(.{}, &work));
    try std.testing.expectEqual(@as(?bool, null), try index.equalAny(.json(.{ .integer = 41 }), &work));
}

test "SQL borrowed membership keeps source admission independent of index storage" {
    const Harness = struct {
        fn run(a: Allocator) !void {
            var cells: [128]Element = undefined;
            for (&cells, 0..) |*cell, i| cell.* = .json(.{ .integer = @intCast(i) });
            const source = try Value.init(.int64, &.{.{ .length = cells.len, .lower = -7 }}, &cells, .{});
            var work: Budget = .{};
            var index = try Membership.initBorrowedWithBudget(a, source, .{}, 2048, &work);
            defer index.deinit();
            try std.testing.expect(try source.retainedBytes() > 2048);
            try std.testing.expect(index.budget.peak <= 2048);
            try std.testing.expect(try index.contains(source, &work));
            try std.testing.expectEqual(@as(?i32, 120), try index.firstPosition(.json(.{ .integer = 127 }), &work));
        }
    };
    const a = std.testing.allocator;
    try Harness.run(a);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Harness.run, .{});
    var cells: [128]Element = @splat(.json(.{ .integer = 42 }));
    const source = try Value.init(.int64, &.{.{ .length = cells.len }}, &cells, .{});
    var work: Budget = .{};
    try std.testing.expectError(error.SqlProgramLimitExceeded, Membership.initBorrowedWithBudget(a, source, .{ .bytes = 2048 }, 65536, &work));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Membership.initBorrowedWithBudget(a, source, .{ .elements = 127 }, 65536, &work));
    try std.testing.expectError(error.SqlProgramLimitExceeded, Membership.initBorrowedWithBudget(a, source, .{}, 1, &work));
    // Cached sizing metadata does not authorize skipping typed validation.
    cells[127] = .json(.{ .string = "invalid bigint" });
    try std.testing.expectError(error.SqlTypeMismatch, Membership.initBorrowedWithBudget(a, source, .{}, 65536, &work));
    cells[127] = .json(.{ .integer = 42 });
    var denied: Budget = .{ .remaining = 0 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, Membership.initBorrowedWithBudget(a, source, .{}, 65536, &denied));
}

test "SQL membership construction hash scratch preserves quotas faults and first ordinals" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var payloads: [128][256]u8 = undefined;
            var cells: [128]Element = undefined;
            for (&payloads, &cells, 0..) |*payload, *cell, i| {
                @memset(payload, 'a');
                _ = try std.fmt.bufPrint(payload[0..8], "{d:0>8}", .{i % 31});
                cell.* = if (i % 17 == 0) .{} else .json(.{ .string = payload });
            }
            for ([_]ElementType{ .text, .jsonb }) |kind| {
                const source = try Value.init(kind, &.{.{ .length = cells.len, .lower = -7 }}, &cells, .{});
                var index = try Membership.init(a, source, .{});
                defer index.deinit();
                try std.testing.expectEqual(index.slots.len * 5, index.budget.live);
                var work: Budget = .{};
                try std.testing.expectEqual(@as(?i32, -6), try index.firstPosition(cells[1], &work));
                try std.testing.expectEqual(@as(?i32, -7), try index.firstPosition(.{}, &work));
                try std.testing.expectEqual(@as(?bool, true), try index.equalAny(cells[1], &work));
                try std.testing.expectEqual(@as(?bool, null), try index.equalAny(.json(.{ .string = "absent" }), &work));
            }
            const source = try Value.init(.text, &.{.{ .length = cells.len }}, &cells, .{});
            var tight = try Membership.initValidated(a, source, .{ .bytes = 512 });
            defer tight.deinit();
            const skipped = try tight.buildHashes();
            try std.testing.expectEqual(@as(usize, 0), skipped.len);
            var work: Budget = .{};
            try tight.populate(&work);
            try std.testing.expectEqual(@as(usize, 31), tight.count);
            try std.testing.expect(tight.budget.peak <= 512);
            const Cancel = struct {
                calls: usize = 0,
                fn check(raw: ?*anyopaque) !void {
                    const self: *@This() = @ptrCast(@alignCast(raw.?));
                    self.calls += 1;
                    if (self.calls == 24) return error.Canceled;
                }
            };
            var control: Cancel = .{};
            var canceled: @import("numeric_value.zig").Context = .{ .alloc = a, .checkpoint = Cancel.check, .ptr = &control };
            var interrupted: Budget = .{ .shared = &canceled };
            var partial = try Membership.initValidated(a, source, .{});
            defer partial.deinit();
            partial.populate(&interrupted) catch |err| {
                if (err != error.Canceled) return err;
                return;
            };
            return error.ExpectedCancellation;
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL membership construction compares legacy probes and bounded hash reuse" {
    const Reference = struct {
        fn populate(index: *Membership, work: *Budget) !void {
            for (0..index.value.cardinality()) |ordinal| {
                const element = index.value.flatCell(ordinal);
                try work.consume(1);
                if (element.sql_null) {
                    if (index.first_null == null) index.first_null = ordinal;
                    continue;
                }
                const hash = try hashElement(index.value.element_type, element, work);
                if (try index.findSlot(element, hash, work) != null) continue;
                if (index.count >= index.slots.len / 2) try index.grow(work, &.{}, true);
                var bucket: usize = @intCast(hash & (index.slots.len - 1));
                while (index.slots[bucket] != Membership.empty) {
                    try work.consume(1);
                    bucket = (bucket + 1) & (index.slots.len - 1);
                }
                index.slots[bucket] = @intCast(ordinal);
                index.tags[bucket] = @truncate(hash >> 56);
                index.count += 1;
            }
        }
    };
    const a = std.testing.allocator;
    for ([_]ElementType{ .int64, .text, .jsonb }) |kind| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const n: usize = if (kind == .int64) 8192 else 1024;
        const cells = try arena.allocator().alloc(Element, n);
        for (cells, 0..) |*cell, i| {
            if (kind == .int64) {
                cell.* = .json(.{ .integer = @intCast(i) });
            } else {
                const payload = try arena.allocator().alloc(u8, 256);
                @memset(payload, 'a');
                _ = try std.fmt.bufPrint(payload[0..8], "{d:0>8}", .{i});
                cell.* = .json(.{ .string = payload });
            }
        }
        const source = try Value.init(kind, &.{.{ .length = @intCast(n) }}, cells, .{ .work = 64 * 1024 * 1024 });
        var charged: [3]usize = undefined;
        for (0..3) |mode| {
            var elapsed: i96 = 0;
            var peak: usize = 0;
            for (0..20) |_| {
                var work: Budget = .{ .remaining = 64 * 1024 * 1024 };
                const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
                const admitted = try source.validateWithBudget(.{}, &work);
                var index = try Membership.initValidated(a, admitted, .{ .bytes = 1024 * 1024 });
                defer index.deinit();
                switch (mode) {
                    0 => try Reference.populate(&index, &work),
                    1 => try index.populateWithHashes(&work, false),
                    else => try index.populate(&work),
                }
                elapsed += @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start);
                charged[mode] = 64 * 1024 * 1024 - work.remaining;
                peak = @max(peak, index.budget.peak);
                try std.testing.expectEqual(n, index.count);
                try std.testing.expectEqual(index.slots.len * 5, index.budget.live);
                try std.testing.expectEqual(@as(?i32, @intCast(n)), try index.firstPosition(cells[n - 1], &work));
            }
            std.debug.print("SQL_MEMBERSHIP_BUILD kind={s} mode={} repetitions=20 cells={} elapsed_ns={} work_per_build={} peak_bytes={}\n", .{ @tagName(kind), mode, n, elapsed, charged[mode], peak });
        }
        try std.testing.expect(charged[1] < charged[0]);
        if (kind != .int64) try std.testing.expect(charged[2] < charged[1]);
        if (kind == .jsonb) {
            // Eliminating replay also makes this build fit the execution
            // cache's unchanged 1 Miwork allowance.
            try std.testing.expect(charged[0] > (Limits{}).work);
            var admitted = try Membership.init(a, source, .{});
            defer admitted.deinit();
            try std.testing.expectEqual(n, admitted.count);
        }
    }
}

test "SQL membership construction bounds native hash prefixes and region ownership" {
    const a = std.testing.allocator;
    var source_arena = std.heap.ArenaAllocator.init(a);
    defer source_arena.deinit();
    const region = source_arena.allocator();
    const n = 8193;
    const cells = try region.alloc(Element, n);
    for (cells, 0..) |*cell, i| {
        const payload = try region.alloc(u8, 256);
        @memset(payload, 'a');
        _ = try std.fmt.bufPrint(payload[0..8], "{d:0>8}", .{i});
        cell.* = .json(.{ .string = payload });
    }
    var creation: Budget = .{ .remaining = 64 * 1024 * 1024 };
    const raw = try Value.initWithBudget(.text, &.{.{ .length = n, .lower = -11 }}, cells, .{}, &creation);
    const storage = @import("array_storage.zig");
    const encoded = try storage.encodeAlloc(region, raw, .{ .values = .{ .work = 64 * 1024 * 1024 } });
    const native = try storage.borrowLeaky(region, .text, encoded, .{ .values = .{ .work = 64 * 1024 * 1024 } });
    var parent: MemoryBudget = .{ .backing = a, .limit = 4 * 1024 * 1024, .monotonic = true };
    {
        var owner = std.heap.ArenaAllocator.init(parent.allocator());
        defer owner.deinit();
        var index = try Membership.initValidated(owner.allocator(), native, .{ .bytes = 2 * 1024 * 1024 });
        defer index.deinit();
        var work: Budget = .{ .remaining = 64 * 1024 * 1024 };
        try index.populate(&work);
        try std.testing.expectEqual(@as(usize, n), index.count);
        try std.testing.expectEqual(index.slots.len * 5, index.budget.live);
        try std.testing.expectEqual(@as(usize, 311296), index.budget.peak);
        // Last source ordinal lies just outside the 64 KiB memo prefix.
        try std.testing.expectEqual(@as(?i32, n - 12), try index.firstPosition(cells[n - 1], &work));
        try std.testing.expectEqual(@as(?bool, false), try index.equalAny(.json(.{ .string = "missing" }), &work));
        try std.testing.expect(parent.footprint() < parent.limit);
    }
    try std.testing.expectEqual(@as(usize, 0), parent.live);
}

test "SQL membership construction adapts hash prefixes to tight headroom" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const n = 1024;
    const cells = try arena.allocator().alloc(Element, n);
    for (cells, 0..) |*cell, i| {
        const payload = try arena.allocator().alloc(u8, 256);
        @memset(payload, 'a');
        _ = try std.fmt.bufPrint(payload[0..8], "{d:0>8}", .{i});
        cell.* = .json(.{ .string = payload });
    }
    const source = try Value.init(.jsonb, &.{.{ .length = n }}, cells, .{ .work = 64 * 1024 * 1024 });
    var construction: [3]usize = undefined;
    var peaks: [3]usize = undefined;
    for ([_]usize{ 64 * 1024, 128 * 1024, 128 * 1024 + 80 }, 0..) |bytes, mode| {
        var work: Budget = .{ .remaining = 1024 * 1024 };
        var index = try Membership.initBorrowedWithBudget(a, source, .{}, bytes, &work);
        defer index.deinit();
        construction[mode] = 1024 * 1024 - work.remaining;
        peaks[mode] = index.budget.peak;
        try std.testing.expectEqual(@as(usize, n), index.count);
        try std.testing.expectEqual(@as(usize, 10240), index.budget.live);
        try std.testing.expect(index.budget.peak <= bytes);
        try std.testing.expectEqual(@as(?i32, n), try index.firstPosition(cells[n - 1], &work));
        std.debug.print("SQL_HASH_HEADROOM bytes={} work={} peak={} retained={}\n", .{ bytes, construction[mode], peaks[mode], index.budget.live });
    }
    // One byte less than the preferred allowance retains a smaller prefix;
    // the previous implementation rejected the complete memo at 128 KiB.
    try std.testing.expect(construction[0] > construction[1]);
    try std.testing.expectEqual(construction[1], construction[2]);
    try std.testing.expectEqual(@as(usize, 23544), peaks[1]);
    try std.testing.expectEqual(@as(usize, 23552), peaks[2]);
    // There is still a scratch-free path when conservative table headroom
    // cannot also admit a useful hash prefix.
    var work: Budget = .{ .remaining = 64 * 1024 * 1024 };
    var tight = try Membership.initBorrowedWithBudget(a, source, .{}, 20 * 1024, &work);
    defer tight.deinit();
    try std.testing.expectEqual(@as(usize, 15360), tight.budget.peak);
    try std.testing.expectEqual(@as(usize, n), tight.count);
}

test "SQL JSONB lookup reuses validation hashes with exact bounded work" {
    const a = std.testing.allocator;
    const source = try Value.init(.jsonb, &.{.{ .length = 1 }}, &.{Element.json(.{ .string = "source" })}, .{});
    var index = try Membership.init(a, source, .{});
    defer index.deinit();
    const payload: [4096]u8 = @splat('x');
    const probe: Element = .json(.{ .string = &payload });
    const allowance = 1024 * 1024;
    // Reproduce the old validation-plus-rehash lookup with the same bucket
    // checks so the saved work is isolated from table and collision behavior.
    var legacy: Budget = .{ .remaining = allowance };
    try validateElement(.jsonb, probe, &legacy);
    try legacy.consume(1);
    try std.testing.expect((try index.find(probe, try hashElement(.jsonb, probe, &legacy), &legacy)) == null);
    var current: Budget = .{ .remaining = allowance };
    try std.testing.expectEqual(@as(?bool, false), try index.equalAny(probe, &current));
    const before = allowance - legacy.remaining;
    const after = allowance - current.remaining;
    try std.testing.expectEqual(@as(usize, payload.len + 2), before - after);
    var exact: Budget = .{ .remaining = after };
    try std.testing.expectEqual(@as(?bool, false), try index.equalAny(probe, &exact));
    try std.testing.expectEqual(@as(usize, 0), exact.remaining);
    var short: Budget = .{ .remaining = after - 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, index.equalAny(probe, &short));
    var position: Budget = .{ .remaining = allowance };
    try std.testing.expectEqual(@as(?i32, null), try index.firstPosition(probe, &position));
    const position_work = allowance - position.remaining;
    try std.testing.expectEqual(after - 1, position_work);
    var exact_position: Budget = .{ .remaining = position_work };
    try std.testing.expectEqual(@as(?i32, null), try index.firstPosition(probe, &exact_position));
    try std.testing.expectEqual(@as(usize, 0), exact_position.remaining);
    var short_position: Budget = .{ .remaining = position_work - 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, index.firstPosition(probe, &short_position));
    // Request exhaustion and cancellation must remain sticky even when the
    // validation hash is available before the lookup starts.
    var exhausted: @import("numeric_value.zig").Context = .{ .alloc = std.testing.failing_allocator, .remaining = after - 1 };
    var shared: Budget = .{ .shared = &exhausted };
    try std.testing.expectError(error.SqlProgramLimitExceeded, index.equalAny(probe, &shared));
    try std.testing.expectError(error.SqlProgramLimitExceeded, shared.consume(0));
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Canceled;
        }
    };
    var canceled: @import("numeric_value.zig").Context = .{ .alloc = std.testing.failing_allocator, .checkpoint = Cancel.check };
    var interrupted: Budget = .{ .shared = &canceled };
    try std.testing.expectError(error.Canceled, index.firstPosition(probe, &interrupted));
    try std.testing.expectError(error.Canceled, interrupted.consume(0));
    std.debug.print("SQL_JSONB_PROBE_HASH bytes={} old_lookup_work={} reused_lookup_work={} reused_position_work={} retained_probe_bytes=0\n", .{ payload.len, before, after, position_work });
}

test "SQL JSONB lookup hash reuse preserves semantic equality nulls and validation errors" {
    const a = std.testing.allocator;
    var object = try std.json.parseFromSlice(std.json.Value, a, "{\"a\":1,\"b\":[true,null]}", .{});
    defer object.deinit();
    var reordered = try std.json.parseFromSlice(std.json.Value, a, "{\"b\":[true,null],\"a\":1.0}", .{});
    defer reordered.deinit();
    var array = try std.json.parseFromSlice(std.json.Value, a, "[1,\"alpha\",null]", .{});
    defer array.deinit();
    const source = try Value.init(.jsonb, &.{.{ .length = 7, .lower = -7 }}, &.{ .json(.{ .string = "alpha" }), .json(.{ .number_string = "1" }), .json(.{ .bool = true }), .json(.null), .json(object.value), .{}, .json(array.value) }, .{});
    var index = try Membership.init(a, source, .{});
    defer index.deinit();
    for ([_]Element{ .json(.{ .string = "alpha" }), .json(.{ .number_string = "1.000" }), .json(.{ .integer = 1 }), .json(.{ .float = 1 }), .json(.{ .bool = true }), .json(.null), .json(reordered.value), .json(array.value), .{}, .json(.{ .string = "missing" }) }) |probe| {
        var work: Budget = .{};
        try std.testing.expectEqual(try source.quantified(probe, .eq, .any, &work), try index.equalAny(probe, &work));
        try std.testing.expectEqual(try source.position(probe, null, &work), try index.firstPosition(probe, &work));
    }
    // Invalid probes still fail before any reusable hash can escape, including
    // invalid numeric tokens that text-domain validation alone cannot reject.
    var invalid_object = try std.json.parseFromSlice(std.json.Value, a, "{\"bad\\u0000key\":1}", .{});
    defer invalid_object.deinit();
    var nested: [132]u8 = undefined;
    @memset(nested[0..66], '[');
    @memset(nested[66..], ']');
    var deep = try std.json.parseFromSlice(std.json.Value, a, &nested, .{});
    defer deep.deinit();
    for ([_]Element{ .json(.{ .string = "bad\x00text" }), .json(.{ .string = "\xff" }), .json(.{ .number_string = "invalid" }), .json(invalid_object.value), .json(deep.value), .{ .value = .{ .string = "not SQL NULL" } }, .typedArray(&source) }) |probe| {
        var work: Budget = .{};
        validateElement(.jsonb, probe, &work) catch |expected| {
            var any_work: Budget = .{};
            try std.testing.expectError(expected, index.equalAny(probe, &any_work));
            var position_work: Budget = .{};
            try std.testing.expectError(expected, index.firstPosition(probe, &position_work));
            continue;
        };
        return error.TestExpectedError;
    }
}
