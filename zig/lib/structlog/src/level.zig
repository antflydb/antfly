// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

/// Global runtime log level. Atomic so it can be changed at runtime.
/// Unlike std.log which only has compile-time levels, this enables
/// dynamic level control (e.g., via HTTP endpoint or signal).
var global_level: std.atomic.Value(i32) = std.atomic.Value(i32).init(@intFromEnum(std.log.Level.info));

pub fn setLevel(level: std.log.Level) void {
    global_level.store(@intFromEnum(level), .monotonic);
}

pub fn getLevel() std.log.Level {
    return @enumFromInt(global_level.load(.monotonic));
}

pub fn isEnabled(level: std.log.Level) bool {
    return @intFromEnum(level) <= global_level.load(.monotonic);
}

test "level: default is info" {
    try std.testing.expectEqual(std.log.Level.info, getLevel());
}

test "level: set and get" {
    const original = getLevel();
    defer setLevel(original);

    setLevel(.debug);
    try std.testing.expectEqual(std.log.Level.debug, getLevel());
    try std.testing.expect(isEnabled(.debug));
    try std.testing.expect(isEnabled(.info));
    try std.testing.expect(isEnabled(.warn));
    try std.testing.expect(isEnabled(.err));

    setLevel(.err);
    try std.testing.expectEqual(std.log.Level.err, getLevel());
    try std.testing.expect(isEnabled(.err));
    try std.testing.expect(!isEnabled(.warn));
    try std.testing.expect(!isEnabled(.info));
    try std.testing.expect(!isEnabled(.debug));
}
