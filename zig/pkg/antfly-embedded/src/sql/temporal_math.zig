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

//! Checked calendar arithmetic in the SQL execution boundary's UTC zone.
//! Months clamp the day before days and elapsed microseconds are applied.
//! No allocation, JSON conversion, floating point or epoch rescaling.
const std = @import("std");
const t = @import("temporal_value.zig");
const text = @import("temporal_text.zig");

pub fn timestamp(value: t.Timestamp, interval: t.Interval, subtract: bool) !t.Timestamp {
    _ = try t.Timestamp.init(value.micros);
    // PostgreSQL negates each finite interval component before applying a
    // subtraction, including when the timestamp itself is infinite.
    if (subtract and interval.infinity() == 0 and
        (interval.months == std.math.minInt(i32) or interval.days == std.math.minInt(i32) or interval.micros == std.math.minInt(i64))) return error.TemporalOutOfRange;
    const sign: i64 = if (subtract) -1 else 1;
    const infinity = interval.infinity() * @as(i8, @intCast(sign));
    if (infinity != 0) {
        if (value.infinity() != 0 and value.infinity() != infinity) return error.TemporalOutOfRange;
        return .{ .micros = if (infinity < 0) std.math.minInt(i64) else std.math.maxInt(i64) };
    }
    if (value.infinity() != 0) return value;
    var fields = try text.Fields.fromTimestamp(value);
    if (interval.months != 0) {
        var civil = try text.Civil.fromDate(fields.date);
        const ordinal = @as(i64, civil.year) * 12 + civil.month - 1 + @as(i64, interval.months) * sign;
        civil.year = std.math.cast(i32, @divFloor(ordinal, 12)) orelse return error.TemporalOutOfRange;
        civil.month = @intCast(@mod(ordinal, 12) + 1);
        const leap = @mod(civil.year, 4) == 0 and (@mod(civil.year, 100) != 0 or @mod(civil.year, 400) == 0);
        const last: u8 = switch (civil.month) {
            2 => if (leap) 29 else 28,
            4, 6, 9, 11 => 30,
            else => 31,
        };
        civil.day = @min(civil.day, last);
        fields.date = try civil.toDate();
    }
    fields.date = try t.Date.init(std.math.cast(i32, @as(i64, fields.date.days) + @as(i64, interval.days) * sign) orelse return error.TemporalOutOfRange);
    const shifted = try fields.toTimestamp();
    const micros = @as(i128, shifted.micros) + @as(i128, interval.micros) * sign;
    const result = try t.Timestamp.init(std.math.cast(i64, micros) orelse return error.TemporalOutOfRange);
    // A finite calculation must never manufacture an infinity sentinel.
    if (result.infinity() != 0) return error.TemporalOutOfRange;
    return result;
}
