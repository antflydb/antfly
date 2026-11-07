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

//! UUID SQL operations use the invocation's clock and secure entropy. The
//! current SQL temporal capability is UTC, never the process's ambient TZ.
const std = @import("std");
const uuid = @import("../common/uuid.zig");
const temporal = @import("temporal_value.zig");
const text = @import("temporal_text.zig");

/// Shared by SQL evaluation and owned native default preparation. A request
/// may inject its own clock for qualification; no schema epoch owns I/O state.
pub var process_clock: uuid.Version7Clock = .{};
pub fn generate4(io: std.Io) ![16]u8 {
    var bytes: [16]u8 = undefined;
    std.Io.randomSecure(io, &bytes) catch |err| return if (err == error.EntropyUnavailable) error.SqlEntropyUnavailable else err;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    return bytes;
}

/// Calendar months, then calendar days, then the independent time component.
/// Month-end clamping is PostgreSQL arithmetic, not a 30-day approximation.
pub fn shiftUtc(value: temporal.Timestamp, shift: temporal.Interval) !temporal.Timestamp {
    if (value.infinity() != 0 or shift.infinity() != 0) return error.TemporalOutOfRange;
    var fields = try text.Fields.fromTimestamp(value);
    if (shift.months != 0) {
        var civil = try text.Civil.fromDate(fields.date);
        const month = @as(i64, civil.year) * 12 + civil.month - 1 + shift.months;
        civil.year = std.math.cast(i32, @divFloor(month, 12)) orelse return error.TemporalOutOfRange;
        civil.month = @intCast(@mod(month, 12) + 1);
        while (true) {
            fields.date = civil.toDate() catch |err| switch (err) {
                error.InvalidCalendarDate => {
                    if (civil.day <= 28) return err;
                    civil.day -= 1;
                    continue;
                },
                else => return err,
            };
            break;
        }
    }
    fields.date = try temporal.Date.init(std.math.cast(i32, @as(i64, fields.date.days) + shift.days) orelse return error.TemporalOutOfRange);
    const base = try fields.toTimestamp();
    const result = try temporal.Timestamp.init(std.math.add(i64, base.micros, shift.micros) catch return error.TemporalOutOfRange);
    if (result.infinity() != 0) return error.TemporalOutOfRange;
    return result;
}

pub fn shiftedTicks(ticks: u60, shift: temporal.Interval) !u60 {
    // Smallest nanosecond instant represented by this reserved clock tick.
    // Preserve its sub-microsecond part across calendar arithmetic.
    const ns = @as(i128, ticks >> 12) * 1_000_000 + @divFloor(@as(i128, ticks & 0xfff) * 1_000_000 + 4095, 4096);
    const original = try temporal.Timestamp.fromUnixMicroseconds(@divFloor(ns, 1000));
    const shifted = try shiftUtc(original, shift);
    const shifted_us = @as(i128, shifted.micros) + temporal.unix_epoch_offset_micros;
    // PostgreSQL's interval overload validates the microsecond timestamp
    // against the final representable millisecond before restoring the
    // original sub-microsecond remainder. The unshifted codec can represent
    // the rest of that millisecond, but this overload must reject it.
    if (shifted_us < 0 or shifted_us > @as(i128, uuid.Version7Clock.max_millis) * 1000) return error.TemporalOutOfRange;
    return uuid.Version7Clock.ticksForUnixNs(shifted_us * 1000 + @mod(ns, 1000)) catch return error.TemporalOutOfRange;
}

pub fn generate(io: std.Io, clock: *uuid.Version7Clock, shift: ?temporal.Interval) ![16]u8 {
    if (shift) |value| if (value.infinity() != 0) return error.TemporalOutOfRange;
    var entropy: [8]u8 = undefined;
    std.Io.randomSecure(io, &entropy) catch |err| return if (err == error.EntropyUnavailable) error.SqlEntropyUnavailable else err;
    const ticks = clock.reserve(std.Io.Clock.now(.real, io).nanoseconds) catch return error.TemporalOutOfRange;
    return uuid.version7(if (shift) |value| try shiftedTicks(ticks, value) else ticks, entropy);
}

test "SQL UUIDv7 interval shifts preserve calendar month ends independent time and timestamp boundaries" {
    const start = try temporal.Timestamp.fromUnixMicroseconds(1_706_704_496_123_456); // 2024-01-31
    const shifted = try shiftUtc(start, .{ .months = 1 });
    const original_fields = try text.Fields.fromTimestamp(start);
    const fields = try text.Fields.fromTimestamp(shifted);
    const civil = try text.Civil.fromDate(fields.date);
    try std.testing.expectEqual(@as(i32, 2024), civil.year);
    try std.testing.expectEqual(@as(u8, 2), civil.month);
    try std.testing.expectEqual(@as(u8, 29), civil.day);
    try std.testing.expectEqual(original_fields.time.micros, fields.time.micros);
    const ticks = try uuid.Version7Clock.ticksForUnixNs(1_706_704_496_123_456_789);
    try std.testing.expectEqual(ticks, try shiftedTicks(ticks, .{}));
    try std.testing.expectEqual(ticks + 4096, try shiftedTicks(ticks, .{ .micros = 1000 }));
    const maximum: u60 = @as(u60, uuid.Version7Clock.max_millis) << 12;
    try std.testing.expectEqual(maximum, try shiftedTicks(maximum, .{}));
    try std.testing.expectEqual(maximum, try shiftedTicks(maximum - 4096, .{ .micros = 1000 }));
    // Sub-microsecond clock precision survives the interval arithmetic;
    // crossing the first full microsecond exceeds PostgreSQL's upper bound.
    try std.testing.expectEqual(maximum + 4, try shiftedTicks(maximum + 4, .{}));
    try std.testing.expectError(error.TemporalOutOfRange, shiftedTicks(maximum + 5, .{}));
    try std.testing.expectError(error.TemporalOutOfRange, shiftedTicks(std.math.maxInt(u60), .{}));
    try std.testing.expectError(error.TemporalOutOfRange, shiftedTicks(maximum, .{ .micros = 1 }));
    try std.testing.expectError(error.TemporalOutOfRange, shiftedTicks(0, .{ .micros = -1 }));
    try std.testing.expectError(error.TemporalOutOfRange, shiftedTicks(ticks, .{ .months = std.math.maxInt(i32) }));
    try std.testing.expectError(error.TemporalOutOfRange, shiftUtc(start, .{ .months = std.math.maxInt(i32), .days = std.math.maxInt(i32), .micros = std.math.maxInt(i64) }));
}
