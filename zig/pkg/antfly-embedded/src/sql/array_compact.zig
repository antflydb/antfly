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
