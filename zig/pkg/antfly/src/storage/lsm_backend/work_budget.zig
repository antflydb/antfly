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
const time = @import("antfly_platform").time;

/// A CPU work slice keeps its deadline and clock together as it passes through
/// planning jobs. Integer deadlines retain the native-clock test interface.
pub const Deadline = struct {
    io: ?std.Io,
    ns: u64,
};

fn nowNs(io: ?std.Io) u64 {
    if (io) |runtime| {
        return @intCast(std.math.clamp(std.Io.Clock.awake.now(runtime).nanoseconds, 0, std.math.maxInt(u64)));
    }
    return time.monotonicNs();
}

pub fn after(io: ?std.Io, duration_ns: u64) Deadline {
    return .{ .io = io, .ns = nowNs(io) +| duration_ns };
}

pub fn before(deadline: anytype) bool {
    if (@TypeOf(deadline) == Deadline) return nowNs(deadline.io) < deadline.ns;
    return switch (@typeInfo(@TypeOf(deadline))) {
        .null => true,
        .optional => if (deadline) |value| before(value) else true,
        .int, .comptime_int => time.monotonicNs() < deadline,
        else => @compileError("unsupported work deadline"),
    };
}

pub fn capped(deadline: anytype, io: ?std.Io, duration_ns: u64) Deadline {
    var result = after(io, duration_ns);
    const limit = if (@TypeOf(deadline) == Deadline) deadline.ns else deadline;
    result.ns = @min(result.ns, limit);
    return result;
}
