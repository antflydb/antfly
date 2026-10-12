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
