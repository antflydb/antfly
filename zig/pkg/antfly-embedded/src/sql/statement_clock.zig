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

//! Execution-owned clock views. Never retain these in cached plans or durable
//! programs: the live source belongs to the statement/cursor owner. A durable
//! transaction start must come from its authorized owner, not request JSON.
const std = @import("std");
const temporal = @import("temporal_value.zig");
pub const Instant = temporal.Timestamp;
pub const Source = union(enum) {
    io: std.Io,
    callback: struct { ptr: ?*anyopaque, now: *const fn (?*anyopaque) anyerror!i128 },
    pub fn sample(self: Source) !Instant {
        const ns: i128 = switch (self) {
            .io => |io| std.Io.Clock.now(.real, io).nanoseconds,
            .callback => |callback| try callback.now(callback.ptr),
        };
        // Host sub-microsecond precision is quantized once, using floor for
        // negative instants too. No multiplication or intermediate i64 epoch.
        return Instant.fromUnixMicroseconds(@divFloor(ns, 1000));
    }
};
pub const Function = enum { transaction, statement, wall };
pub const StableFunction = enum { transaction, statement };

/// Stable-address execution storage, not a prepared-plan cache. Once copied
/// into its final owner, scalar results may borrow its immutable typed values
/// until that owner is released. Wall time deliberately has no cached slot.
pub const Frame = struct {
    view: View,
    values: [2][7]temporal.Value,
    transaction_date: temporal.Value,
    valid: [2]u7,

    pub fn init(view: View) !Frame {
        try view.validate();
        var frame: Frame = .{ .view = view, .values = undefined, .valid = .{ 0, 0 }, .transaction_date = .{ .date = try temporal.Date.init(@intCast(@divFloor(view.transaction.micros, temporal.micros_per_day))) } };
        for ([_]Instant{ view.transaction, view.statement }, 0..) |instant, index| {
            for (0..7) |precision| {
                // A valid finite instant can overflow when rounded at a
                // coarser precision. Record that error for demand time rather
                // than failing unrelated expressions during admission.
                const rounded = instant.withPrecision(@intCast(precision)) catch |err| switch (err) {
                    error.TemporalOutOfRange => {
                        frame.values[index][precision] = .{ .timestamp_tz = instant };
                        continue;
                    },
                    else => return err,
                };
                frame.values[index][precision] = .{ .timestamp_tz = rounded };
                frame.valid[index] |= @as(u7, 1) << @intCast(precision);
            }
        }
        return frame;
    }

    pub fn matches(self: *const Frame, view: View) bool {
        return self.view.transaction.micros == view.transaction.micros and
            self.view.statement.micros == view.statement.micros and
            std.meta.eql(self.view.source, view.source);
    }

    pub fn value(self: *const Frame, function: StableFunction, precision: u8) !*const temporal.Value {
        if (precision > 6) return error.InvalidTemporalPrecision;
        const index: usize = @backingInt(function);
        if (self.valid[index] & (@as(u7, 1) << @intCast(precision)) == 0) return error.TemporalOutOfRange;
        return &self.values[index][precision];
    }
};
pub const View = struct {
    source: Source,
    transaction: Instant,
    statement: Instant,
    /// Validate inherited views without sampling the source. A copied view
    /// may cross child-plan boundaries, never a new statement/transaction.
    pub fn validate(self: View) !void {
        for ([_]Instant{ self.transaction, self.statement }) |instant| {
            _ = try Instant.init(instant.micros);
            if (instant.infinity() != 0) return error.InvalidTransactionClock;
        }
    }
    pub fn capture(source: Source, transaction: ?Instant) !View {
        if (transaction) |start| {
            _ = try Instant.init(start.micros);
            if (start.infinity() != 0) return error.InvalidTransactionClock;
        }
        const statement = try source.sample();
        return .{ .source = source, .transaction = transaction orelse statement, .statement = statement };
    }
    pub fn value(self: View, function: Function, precision: u8) !Instant {
        if (precision > 6) return error.InvalidTemporalPrecision;
        return (switch (function) {
            .transaction => self.transaction,
            .statement => self.statement,
            .wall => try self.source.sample(),
        }).withPrecision(precision);
    }
};
const Fake = struct {
    values: []const i128,
    calls: usize = 0,
    fn now(raw: ?*anyopaque) !i128 {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        if (self.calls == self.values.len) return error.TestClockUnavailable;
        const value = self.values[self.calls];
        self.calls += 1;
        return value;
    }
    fn source(self: *@This()) Source {
        return .{ .callback = .{ .ptr = self, .now = now } };
    }
};

test "SQL statement clocks pin owner transaction time and preserve views across child pages" {
    var fake: Fake = .{ .values = &.{ 20_000_000_123, 21_000_000_000, 19_000_000_000 } };
    const transaction = try Instant.fromUnixMicroseconds(10_000_000);
    const view = try View.capture(fake.source(), transaction);
    // Multiple child plans/cursor pages borrow a value copy of one view.
    const child = view;
    for (0..10_000) |_| {
        try std.testing.expectEqual(transaction.micros, (try child.value(.transaction, 6)).micros);
        try std.testing.expectEqual(@as(?i128, 20_000_000_000), (try child.value(.statement, 6)).unixNanoseconds());
    }
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    const next_statement = try View.capture(fake.source(), transaction);
    try std.testing.expectEqual(transaction.micros, (try next_statement.value(.transaction, 6)).micros);
    try std.testing.expectEqual(@as(?i128, 21_000_000_000), (try next_statement.value(.statement, 6)).unixNanoseconds());
    // Realtime may move backwards. Do not manufacture monotonic wall time.
    try std.testing.expectEqual(@as(?i128, 19_000_000_000), (try view.value(.wall, 6)).unixNanoseconds());
    try std.testing.expectEqual(@as(?i128, 20_000_000_000), (try view.value(.statement, 6)).unixNanoseconds());
}

test "SQL statement clocks distinguish implicit snapshots negative epochs and source failures" {
    var fake: Fake = .{ .values = &.{ -1, 0 } };
    const view = try View.capture(fake.source(), null);
    try std.testing.expectEqual(view.statement.micros, view.transaction.micros);
    try std.testing.expectEqual(@as(?i128, -1000), view.statement.unixNanoseconds());
    try std.testing.expectEqual(@as(?i128, 0), (try view.value(.wall, 6)).unixNanoseconds());
    try std.testing.expectError(error.TestClockUnavailable, view.value(.wall, 6));
    const before = fake.calls;
    try std.testing.expectError(error.InvalidTemporalPrecision, view.value(.wall, 7));
    try std.testing.expectEqual(before, fake.calls);
    try std.testing.expectError(error.InvalidTransactionClock, View.capture(fake.source(), try Instant.init(std.math.maxInt(i64))));
    try std.testing.expectEqual(before, fake.calls);
}

test "SQL statement clocks use injected std Io realtime without a thread dependency" {
    const view = try View.capture(.{ .io = std.testing.io }, null);
    try std.testing.expectEqual(@as(i8, 0), view.statement.infinity());
    try std.testing.expectEqual(view.statement.micros, view.transaction.micros);
}

test "SQL statement clock frames retain typed precision values without sampling or allocation" {
    var fake: Fake = .{ .values = &.{ 20_123_456_789, 19_000_000_000 } };
    const view = try View.capture(fake.source(), try Instant.init(-500_000));
    const frame = try Frame.init(view);
    for ([_]StableFunction{ .transaction, .statement }) |function| {
        for (0..7) |precision| {
            const p: u8 = @intCast(precision);
            const expected = try view.value(if (function == .transaction) .transaction else .statement, p);
            const borrowed = try frame.value(function, p);
            try std.testing.expectEqual(expected.micros, borrowed.timestamp_tz.micros);
            for (0..10_000) |_| try std.testing.expect(borrowed == try frame.value(function, p));
        }
    }
    try std.testing.expect(frame.matches(view));
    var different = view;
    different.statement = try Instant.init(view.statement.micros + 1);
    try std.testing.expect(!frame.matches(different));
    different = view;
    different.source = .{ .io = std.testing.io };
    try std.testing.expect(!frame.matches(different));
    try std.testing.expectError(error.InvalidTemporalPrecision, frame.value(.transaction, 7));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    try std.testing.expectEqual(@as(?i128, 19_000_000_000), (try view.value(.wall, 6)).unixNanoseconds());
    try std.testing.expectEqual(@as(usize, 2), fake.calls);
    try std.testing.expectEqual(view.statement.micros, (try frame.value(.statement, 6)).timestamp_tz.micros);
}

test "SQL statement clock frames defer finite precision overflow without admitting infinity" {
    var fake: Fake = .{ .values = &.{} };
    const edge = try Instant.init(temporal.end_timestamp - 1);
    const view: View = .{ .source = fake.source(), .transaction = edge, .statement = try Instant.init(0) };
    const frame = try Frame.init(view);
    try std.testing.expectEqual(edge.micros, (try frame.value(.transaction, 6)).timestamp_tz.micros);
    for (0..6) |precision| try std.testing.expectError(error.TemporalOutOfRange, frame.value(.transaction, @intCast(precision)));
    for (0..7) |precision| try std.testing.expectEqual(@as(i64, 0), (try frame.value(.statement, @intCast(precision))).timestamp_tz.micros);
    var invalid = view;
    invalid.transaction = try Instant.init(std.math.maxInt(i64));
    try std.testing.expectError(error.InvalidTransactionClock, Frame.init(invalid));
    try std.testing.expectEqual(@as(usize, 0), fake.calls);
}
