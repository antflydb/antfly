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

//! Independent arithmetic oracle. Public SQL admission/ownership and backend
//! connection cache semantics require their own native lifecycle qualification.
const std = @import("std");
const sequence = @import("system_catalog/sequences.zig");
const Action = union(enum) {
    reserve,
    set: struct { value: i64, called: bool },
    alter: sequence.Options,
};
const Expected = struct {
    integer: sequence.Integer,
    increment: i64,
    minimum: i64,
    maximum: i64,
    start: i64,
    cache: u64,
    cycle: bool,
    last_value: i64,
    is_called: bool,

    fn check(self: @This(), actual: sequence.Record) !void {
        try actual.state.validate(actual.definition);
        inline for (.{ "integer", "increment", "minimum", "maximum", "start", "cache", "cycle" }) |field|
            try std.testing.expectEqual(@field(self, field), @field(actual.definition, field));
        try std.testing.expectEqual(self.last_value, actual.state.last_value);
        try std.testing.expectEqual(self.is_called, actual.state.is_called);
    }
};
const Profile = struct {
    name: []const u8,
    create: []const u8,
    options: sequence.Options,
    initial: Expected,
    steps: []const struct {
        sql: []const u8,
        action: Action,
        sqlstate: ?[]const u8,
        expected: Expected,
        lease: ?struct { first: i64, count: u64, last: i64 },
    },
};

fn apply(record: *sequence.Record, action: Action) !?sequence.Lease {
    switch (action) {
        .reserve => {
            const result = try record.state.reserve(record.definition);
            record.state = result.state;
            return result.lease;
        },
        .set => |value| record.state = try record.state.set(record.definition, value.value, value.called),
        .alter => |options| record.* = try record.alter(options),
    }
    return null;
}

fn sqlstate(err: anyerror) []const u8 {
    return switch (err) {
        error.SequenceGeneratorLimitExceeded => "2200H",
        error.InvalidSequenceDefinition => "22023",
        error.SequenceValueOutOfRange => "22003",
        else => "XX000",
    };
}

test "sequence catalog arithmetic matches independently captured PostgreSQL cache bounds and ALTER semantics" {
    const Fixture = struct { format: u8, profiles: []const Profile };
    var fixture = try std.json.parseFromSlice(Fixture, std.testing.allocator, @embedFile("sql/fixtures/sql_sequence_reference.json"), .{});
    defer fixture.deinit();
    try std.testing.expectEqual(@as(u8, 1), fixture.value.format);
    try std.testing.expectEqual(@as(usize, 7), fixture.value.profiles.len);
    for (fixture.value.profiles) |profile| {
        var record = try sequence.Record.create(profile.options);
        try profile.initial.check(record);
        for (profile.steps) |step| {
            const before = record;
            var observed_code: ?[]const u8 = null;
            const lease = apply(&record, step.action) catch |err| failed: {
                observed_code = sqlstate(err);
                try std.testing.expectEqualDeep(before, record);
                break :failed null;
            };
            if ((observed_code == null) != (step.sqlstate == null)) {
                std.debug.print("sequence oracle mismatch: {s}: {s}: expected {?s}, got {?s}\n", .{ profile.name, step.sql, step.sqlstate, observed_code });
                return error.TestUnexpectedResult;
            }
            if (observed_code) |code| try std.testing.expectEqualStrings(step.sqlstate.?, code);
            try step.expected.check(record);
            try std.testing.expectEqual(step.lease != null, lease != null);
            if (step.lease) |expected| {
                try std.testing.expectEqual(expected.first, lease.?.first);
                try std.testing.expectEqual(expected.count, lease.?.count);
                try std.testing.expectEqual(expected.last, try lease.?.value(lease.?.count - 1));
            }
        }
    }
}

test {
    _ = @import("system_catalog/sequence_journal.zig");
}
