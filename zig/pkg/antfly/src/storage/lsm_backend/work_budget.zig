// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

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
