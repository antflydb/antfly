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

//! Exact date/timestamp coercions under the invocation's UTC timezone.
const std = @import("std");
const temporal = @import("temporal_value.zig");

pub fn asTimestamp(value: temporal.Value) !temporal.Timestamp {
    return switch (value) {
        .timestamp, .timestamp_tz => |stamp| stamp,
        .date => |date| if (date.infinity() != 0)
            .{ .micros = if (date.infinity() < 0) std.math.minInt(i64) else std.math.maxInt(i64) }
        else
            temporal.Timestamp.init(std.math.mul(i64, date.days, temporal.micros_per_day) catch return error.TemporalOutOfRange),
        else => error.SqlUndefinedFunction,
    };
}

pub fn allowed(source: temporal.Kind, target: temporal.Kind, implicit: bool) bool {
    if (source == target) return true;
    if (source != .date and source != .timestamp and source != .timestamp_tz) return false;
    if (target != .date and target != .timestamp and target != .timestamp_tz) return false;
    return !implicit or source == .date or (source == .timestamp and target == .timestamp_tz);
}

/// The current invocation timezone is UTC. These conversions never consult
/// ambient process TZ and retain infinities separately from finite epoch data.
pub fn castUtc(value: temporal.Value, target: temporal.Kind) !temporal.Value {
    if (!allowed(value.kind(), target, false)) return error.SqlCannotCoerce;
    if (value.kind() == target) return value;
    const stamp = try asTimestamp(value);
    return switch (target) {
        .timestamp => .{ .timestamp = stamp },
        .timestamp_tz => .{ .timestamp_tz = stamp },
        .date => .{ .date = if (stamp.infinity() != 0) .{ .days = if (stamp.infinity() < 0) std.math.minInt(i32) else std.math.maxInt(i32) } else try temporal.Date.init(@intCast(@divFloor(stamp.micros, temporal.micros_per_day))) },
        else => error.SqlCannotCoerce,
    };
}
