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

//! Native temporal values and PostgreSQL binary boundaries. SQL NULL belongs
//! to the enclosing typed datum, not to a temporal infinity or integer sentinel.
//! No JSON, floating-point arithmetic, allocation or host timezone is involved.
//! Parser/binder/storage/session activation remains a separate required step.
const std = @import("std");
pub const micros_per_day: i64 = 86_400_000_000;
pub const unix_epoch_offset_micros: i64 = 946_684_800_000_000;
pub const min_timestamp: i64 = -211_813_488_000_000_000;
pub const end_timestamp: i64 = 9_223_371_331_200_000_000;
pub const min_date: i32 = -2_451_545;
pub const end_date: i32 = 2_145_031_949;

// Typed spill tags are append-only; do not reorder or recycle identities.
pub const Kind = enum(u8) {
    date = 0,
    time = 1,
    time_tz = 2,
    timestamp = 3,
    timestamp_tz = 4,
    interval = 5,
    pub fn oid(self: Kind) u32 {
        return switch (self) {
            .date => 1082,
            .time => 1083,
            .time_tz => 1266,
            .timestamp => 1114,
            .timestamp_tz => 1184,
            .interval => 1186,
        };
    }
    pub fn binarySize(self: Kind) usize {
        return switch (self) {
            .date => 4,
            .time_tz => 12,
            .interval => 16,
            else => 8,
        };
    }
};
pub const Error = error{ InvalidTemporalBinary, TemporalOutOfRange, TemporalPrecisionLoss, InvalidTemporalPrecision };

/// Precision coercion never passes through floating point or an epoch shift.
/// i128 intermediates preserve signed half ties and i64-min interval values.
fn roundedMicros(micros: i64, precision: u8) Error!i64 {
    if (precision > 6) return error.InvalidTemporalPrecision;
    if (precision == 6) return micros;
    const scales = [_]i64{ 1_000_000, 100_000, 10_000, 1000, 100, 10, 1 };
    const scale = scales[precision];
    const value: i128 = micros;
    const magnitude = if (value < 0) -value else value;
    const rounded = @divTrunc(magnitude + @divExact(scale, 2), scale) * scale;
    return std.math.cast(i64, if (value < 0) -rounded else rounded) orelse error.TemporalOutOfRange;
}

/// Distinct SQL timestamp kinds share this exact signed PostgreSQL-epoch unit.
/// +/-infinity are values; neither is SQL NULL.
pub const Timestamp = struct {
    micros: i64,
    pub fn init(micros: i64) Error!Timestamp {
        if (micros != std.math.minInt(i64) and micros != std.math.maxInt(i64) and (micros < min_timestamp or micros >= end_timestamp)) return error.TemporalOutOfRange;
        return .{ .micros = micros };
    }
    pub fn infinity(self: Timestamp) i8 {
        return if (self.micros == std.math.minInt(i64)) -1 else if (self.micros == std.math.maxInt(i64)) 1 else 0;
    }
    pub fn fromUnixNanoseconds(ns: i128) Error!Timestamp {
        if (@mod(ns, 1000) != 0) return error.TemporalPrecisionLoss;
        return fromUnixMicroseconds(@divExact(ns, 1000));
    }
    pub fn fromUnixMicroseconds(us: i128) Error!Timestamp {
        const relative = std.math.sub(i128, us, unix_epoch_offset_micros) catch return error.TemporalOutOfRange;
        const micros = std.math.cast(i64, relative) orelse return error.TemporalOutOfRange;
        const value = try init(micros);
        if (value.infinity() != 0) return error.TemporalOutOfRange;
        return value;
    }
    pub fn unixNanoseconds(self: Timestamp) ?i128 {
        return if (self.infinity() != 0) null else (@as(i128, self.micros) + unix_epoch_offset_micros) * 1000;
    }
    /// The caller owns SQL warning/clamping rules for syntactic precision >6.
    /// Rounding is around the PostgreSQL epoch, including negative half ties.
    pub fn withPrecision(self: Timestamp, precision: u8) Error!Timestamp {
        _ = try init(self.micros);
        if (precision > 6) return error.InvalidTemporalPrecision;
        if (self.infinity() != 0 or precision == 6) return self;
        return init(try roundedMicros(self.micros, precision));
    }
};
pub const Date = struct {
    days: i32,
    pub fn init(days: i32) Error!Date {
        if (days != std.math.minInt(i32) and days != std.math.maxInt(i32) and (days < min_date or days >= end_date)) return error.TemporalOutOfRange;
        return .{ .days = days };
    }
    pub fn infinity(self: Date) i8 {
        return if (self.days == std.math.minInt(i32)) -1 else if (self.days == std.math.maxInt(i32)) 1 else 0;
    }
};
pub const Time = struct {
    micros: i64,
    pub fn init(micros: i64) Error!Time {
        // PostgreSQL preserves 24:00:00 as distinct from 00:00:00.
        if (micros < 0 or micros > micros_per_day) return error.TemporalOutOfRange;
        return .{ .micros = micros };
    }
    pub fn withPrecision(self: Time, precision: u8) Error!Time {
        _ = try init(self.micros);
        return init(try roundedMicros(self.micros, precision));
    }
};
pub const TimeTz = struct {
    time: Time,
    seconds_west: i32,
    pub fn init(micros: i64, seconds_west: i32) Error!TimeTz {
        if (seconds_west <= -57_600 or seconds_west >= 57_600) return error.TemporalOutOfRange;
        return .{ .time = try Time.init(micros), .seconds_west = seconds_west };
    }
    pub fn withPrecision(self: TimeTz, precision: u8) Error!TimeTz {
        _ = try init(self.time.micros, self.seconds_west);
        return .{ .time = try self.time.withPrecision(precision), .seconds_west = self.seconds_west };
    }
};
pub const Interval = struct {
    /// These are independent calendar/time components, never one duration.
    months: i32 = 0,
    days: i32 = 0,
    micros: i64 = 0,
    pub fn infinity(self: Interval) i8 {
        if (self.months == std.math.minInt(i32) and self.days == std.math.minInt(i32) and self.micros == std.math.minInt(i64)) return -1;
        if (self.months == std.math.maxInt(i32) and self.days == std.math.maxInt(i32) and self.micros == std.math.maxInt(i64)) return 1;
        return 0;
    }
    pub fn withPrecision(self: Interval, precision: u8) Error!Interval {
        if (precision > 6) return error.InvalidTemporalPrecision;
        if (self.infinity() != 0) return self;
        return .{ .months = self.months, .days = self.days, .micros = try roundedMicros(self.micros, precision) };
    }
};
pub const Value = union(Kind) {
    date: Date,
    time: Time,
    time_tz: TimeTz,
    timestamp: Timestamp,
    timestamp_tz: Timestamp,
    interval: Interval,
    pub fn kind(self: Value) Kind {
        return std.meta.activeTag(self);
    }
    pub fn withPrecision(self: Value, precision: u8) Error!Value {
        return switch (self) {
            .date => error.InvalidTemporalPrecision,
            .time => |v| .{ .time = try v.withPrecision(precision) },
            .time_tz => |v| .{ .time_tz = try v.withPrecision(precision) },
            .timestamp => |v| .{ .timestamp = try v.withPrecision(precision) },
            .timestamp_tz => |v| .{ .timestamp_tz = try v.withPrecision(precision) },
            .interval => |v| .{ .interval = try v.withPrecision(precision) },
        };
    }
    pub fn infinity(self: Value) i8 {
        return switch (self) {
            .date => |v| v.infinity(),
            .timestamp, .timestamp_tz => |v| v.infinity(),
            .interval => |v| v.infinity(),
            else => 0,
        };
    }
    /// Exposed for typed consumers and independent field-level oracle checks.
    pub fn components(self: Value) struct { values: [3]i64, len: usize } {
        return switch (self) {
            .date => |v| .{ .values = .{ v.days, 0, 0 }, .len = 1 },
            .time => |v| .{ .values = .{ v.micros, 0, 0 }, .len = 1 },
            .timestamp, .timestamp_tz => |v| .{ .values = .{ v.micros, 0, 0 }, .len = 1 },
            .time_tz => |v| .{ .values = .{ v.time.micros, v.seconds_west, 0 }, .len = 2 },
            .interval => |v| .{ .values = .{ v.months, v.days, v.micros }, .len = 3 },
        };
    }
};

pub fn decode(kind: Kind, bytes: []const u8) Error!Value {
    if (bytes.len != kind.binarySize()) return error.InvalidTemporalBinary;
    return switch (kind) {
        .date => .{ .date = try Date.init(std.mem.readInt(i32, bytes[0..4], .big)) },
        .time => .{ .time = try Time.init(std.mem.readInt(i64, bytes[0..8], .big)) },
        .time_tz => .{ .time_tz = try TimeTz.init(std.mem.readInt(i64, bytes[0..8], .big), std.mem.readInt(i32, bytes[8..12], .big)) },
        .timestamp => .{ .timestamp = try Timestamp.init(std.mem.readInt(i64, bytes[0..8], .big)) },
        .timestamp_tz => .{ .timestamp_tz = try Timestamp.init(std.mem.readInt(i64, bytes[0..8], .big)) },
        .interval => .{ .interval = .{ .micros = std.mem.readInt(i64, bytes[0..8], .big), .days = std.mem.readInt(i32, bytes[8..12], .big), .months = std.mem.readInt(i32, bytes[12..16], .big) } },
    };
}

/// Canonical frame in caller-owned stack/batch memory. Revalidate public fields;
/// constructing a struct literal is not permission to emit an invalid value.
pub fn encode(value: Value, output: *[16]u8) Error![]const u8 {
    switch (value) {
        .date => |v| {
            _ = try Date.init(v.days);
            std.mem.writeInt(i32, output[0..4], v.days, .big);
        },
        .time => |v| {
            _ = try Time.init(v.micros);
            std.mem.writeInt(i64, output[0..8], v.micros, .big);
        },
        .timestamp, .timestamp_tz => |v| {
            _ = try Timestamp.init(v.micros);
            std.mem.writeInt(i64, output[0..8], v.micros, .big);
        },
        .time_tz => |v| {
            _ = try TimeTz.init(v.time.micros, v.seconds_west);
            std.mem.writeInt(i64, output[0..8], v.time.micros, .big);
            std.mem.writeInt(i32, output[8..12], v.seconds_west, .big);
        },
        .interval => |v| {
            std.mem.writeInt(i64, output[0..8], v.micros, .big);
            std.mem.writeInt(i32, output[8..12], v.days, .big);
            std.mem.writeInt(i32, output[12..16], v.months, .big);
        },
    }
    return output[0..value.kind().binarySize()];
}

test "SQL temporal binary boundary preserves domains infinities epoch and framing" {
    const t = std.testing;
    try t.expectEqual(@as(?i128, -1000), (try Timestamp.fromUnixNanoseconds(-1000)).unixNanoseconds());
    try t.expectError(error.TemporalPrecisionLoss, Timestamp.fromUnixNanoseconds(-1));
    try t.expectError(error.TemporalOutOfRange, Timestamp.init(min_timestamp - 1));
    try t.expectError(error.TemporalOutOfRange, Timestamp.init(end_timestamp));
    try t.expectError(error.TemporalOutOfRange, Date.init(min_date - 1));
    try t.expectError(error.TemporalOutOfRange, Date.init(end_date));
    try t.expectError(error.TemporalOutOfRange, Time.init(-1));
    try t.expectError(error.TemporalOutOfRange, Time.init(micros_per_day + 1));
    try t.expectError(error.TemporalOutOfRange, TimeTz.init(0, 57_600));
    try t.expectError(error.TemporalOutOfRange, TimeTz.init(0, -57_600));
    var output: [16]u8 = undefined;
    const long_frame: [17]u8 = @splat(0);
    for (std.enums.values(Kind)) |kind| {
        try t.expectError(error.InvalidTemporalBinary, decode(kind, output[0 .. kind.binarySize() - 1]));
        try t.expectError(error.InvalidTemporalBinary, decode(kind, &long_frame));
    }
    try t.expectError(error.TemporalOutOfRange, encode(.{ .time = .{ .micros = -1 } }, &output));
    const infinity = try Timestamp.init(std.math.maxInt(i64));
    try t.expectEqual(@as(?i128, null), infinity.unixNanoseconds());
    try t.expectEqual(@as(i8, 1), (try infinity.withPrecision(0)).infinity());
    try t.expectEqual(@as(i64, -1_000_000), (try (try Timestamp.init(-500_000)).withPrecision(0)).micros);
    try t.expectEqual(@as(i64, 1_000_000), (try (try Timestamp.init(500_000)).withPrecision(0)).micros);
    try t.expectError(error.InvalidTemporalPrecision, infinity.withPrecision(7));
}

test "SQL temporal PostgreSQL binary fields and precision preserve full domains" {
    const a = std.testing.allocator;
    const Entry = struct { kind: Kind, sql: []const u8, oid: u32, binary: []const u8, components: []const i64, infinity: i8, source_binary: ?[]const u8 = null, precision: ?u8 = null };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, a, @embedFile("fixtures/sql_temporal_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 174), fixture.value.entries.len);
    var covered: u8 = 0;
    for (fixture.value.entries) |entry| {
        errdefer std.debug.print("temporal oracle: {s}\n", .{entry.sql});
        covered |= @as(u8, 1) << @intCast(@backingInt(entry.kind));
        try std.testing.expectEqual(entry.oid, entry.kind.oid());
        var bytes: [16]u8 = undefined;
        const frame = try std.fmt.hexToBytes(&bytes, entry.binary);
        const value = try decode(entry.kind, frame);
        try std.testing.expectEqual(entry.kind, value.kind());
        try std.testing.expectEqual(entry.infinity, value.infinity());
        if (entry.infinity == 0) {
            const components = value.components();
            try std.testing.expectEqualSlices(i64, entry.components, components.values[0..components.len]);
        }
        var encoded: [16]u8 = undefined;
        try std.testing.expectEqualSlices(u8, frame, try encode(value, &encoded));
        if (entry.source_binary) |source_hex| {
            var source_bytes: [16]u8 = undefined;
            const source = try decode(entry.kind, try std.fmt.hexToBytes(&source_bytes, source_hex));
            const rounded = try source.withPrecision(entry.precision.?);
            try std.testing.expectEqualSlices(u8, frame, try encode(rounded, &encoded));
        }
    }
    try std.testing.expectEqual(@as(u8, 63), covered);
}

test "SQL temporal binary codec rejects malformed values before publishing bytes" {
    var frame: [16]u8 = @splat(0xaa);
    const before = frame;
    try std.testing.expectError(error.TemporalOutOfRange, encode(.{ .timestamp = .{ .micros = end_timestamp } }, &frame));
    try std.testing.expectEqualSlices(u8, &before, &frame);
    for ([_]i64{ min_timestamp - 1, end_timestamp, std.math.maxInt(i64) - 1 }) |raw| {
        std.mem.writeInt(i64, frame[0..8], raw, .big);
        try std.testing.expectError(error.TemporalOutOfRange, decode(.timestamp_tz, frame[0..8]));
    }
    for ([_]i32{ min_date - 1, end_date, std.math.maxInt(i32) - 1 }) |raw| {
        std.mem.writeInt(i32, frame[0..4], raw, .big);
        try std.testing.expectError(error.TemporalOutOfRange, decode(.date, frame[0..4]));
    }
    // Precision rounding may carry out of the valid timestamp domain.
    try std.testing.expectError(error.TemporalOutOfRange, (try Timestamp.init(end_timestamp - 1)).withPrecision(0));
}

test "SQL temporal precision preserves calendar components and detects carry overflow" {
    const t = std.testing;
    try t.expectEqual(micros_per_day, (try (Time{ .micros = micros_per_day - 1 }).withPrecision(0)).micros);
    const zoned = try (TimeTz{ .time = .{ .micros = micros_per_day - 1 }, .seconds_west = -20_700 }).withPrecision(0);
    try t.expectEqual(micros_per_day, zoned.time.micros);
    try t.expectEqual(@as(i32, -20_700), zoned.seconds_west);
    const interval = try (Interval{ .months = -14, .days = 3, .micros = -500_000 }).withPrecision(0);
    try t.expectEqual(@as(i32, -14), interval.months);
    try t.expectEqual(@as(i32, 3), interval.days);
    try t.expectEqual(@as(i64, -1_000_000), interval.micros);
    for ([_]i64{ std.math.minInt(i64), std.math.maxInt(i64) }) |micros| {
        const finite: Interval = .{ .micros = micros };
        try t.expectEqual(micros, (try finite.withPrecision(6)).micros);
        try t.expectError(error.TemporalOutOfRange, finite.withPrecision(0));
    }
    for ([_]Interval{
        .{ .months = std.math.minInt(i32), .days = std.math.minInt(i32), .micros = std.math.minInt(i64) },
        .{ .months = std.math.maxInt(i32), .days = std.math.maxInt(i32), .micros = std.math.maxInt(i64) },
    }) |infinity| {
        for (0..7) |precision| try t.expectEqual(infinity, try infinity.withPrecision(@intCast(precision)));
        try t.expectError(error.InvalidTemporalPrecision, infinity.withPrecision(7));
    }
    try t.expectError(error.InvalidTemporalPrecision, (Time{ .micros = 0 }).withPrecision(7));
    try t.expectError(error.TemporalOutOfRange, (TimeTz{ .time = .{ .micros = 0 }, .seconds_west = 57_600 }).withPrecision(0));
    try t.expectError(error.InvalidTemporalPrecision, (Value{ .date = .{ .days = 0 } }).withPrecision(0));
}

test "SQL temporal full-range epoch conversion and mixed codec roundtrips allocate nothing" {
    var random_state = std.Random.DefaultPrng.init(0xc104c2000);
    const random = random_state.random();
    const span: u64 = @intCast(@as(i128, end_timestamp) - min_timestamp);
    var frame: [16]u8 = undefined;
    for (0..20_000) |_| {
        const micros: i64 = @intCast(@as(i128, min_timestamp) + random.int(u64) % span);
        const timestamp = try Timestamp.init(micros);
        const recovered = try Timestamp.fromUnixNanoseconds(timestamp.unixNanoseconds().?);
        try std.testing.expectEqual(micros, recovered.micros);
        const value: Value = if (random.boolean()) .{ .timestamp = timestamp } else .{ .timestamp_tz = timestamp };
        try std.testing.expectEqualDeep(value, try decode(value.kind(), try encode(value, &frame)));
        const interval: Value = .{ .interval = .{ .months = random.int(i32), .days = random.int(i32), .micros = random.int(i64) } };
        try std.testing.expectEqualDeep(interval, try decode(.interval, try encode(interval, &frame)));
    }
}

test "SQL temporal mixed binary throughput uses bounded caller-owned memory" {
    const values = [_]Value{
        .{ .date = try Date.init(end_date - 1) },
        .{ .time = try Time.init(micros_per_day) },
        .{ .time_tz = try TimeTz.init(12_345_678, -57_599) },
        .{ .timestamp = try Timestamp.init(min_timestamp) },
        .{ .timestamp_tz = try Timestamp.init(end_timestamp - 1) },
        .{ .interval = .{ .months = -17, .days = 23, .micros = -123_456_789 } },
    };
    var inputs: [values.len][16]u8 = undefined;
    for (values, &inputs) |value, *frame| _ = try encode(value, frame);
    std.mem.doNotOptimizeAway(&inputs);
    var output: [16]u8 = undefined;
    var checksum: u64 = 0;
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..1_000_000) |i| {
        const kind = values[i % values.len].kind();
        const value = try decode(kind, inputs[i % values.len][0..kind.binarySize()]);
        const encoded = try encode(value, &output);
        checksum +%= encoded[0];
        std.mem.doNotOptimizeAway(encoded);
    }
    std.mem.doNotOptimizeAway(checksum);
    std.debug.print("SQL temporal mixed codec: rows=1000000 output_bytes=16 input_frame_bytes=96 heap_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
}
