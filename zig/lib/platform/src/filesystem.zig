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
const builtin = @import("builtin");

pub const capacity_supported = builtin.link_libc and switch (builtin.os.tag) {
    .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
    else => false,
};

const c = if (!capacity_supported)
    struct {}
else if (builtin.os.tag == .linux)
    @cImport({
        @cInclude("sys/statfs.h");
    })
else
    @cImport({
        @cInclude("sys/statvfs.h");
    });

pub const Capacity = struct {
    total_bytes: u64,
    available_bytes: u64,
};

/// Return capacity available to an unprivileged writer on the filesystem that
/// contains `path`. Saturating overflow handling keeps the observation safe on
/// unusual filesystems that report sentinel block counts.
pub fn capacity(path: []const u8) !Capacity {
    if (comptime !capacity_supported) return error.UnsupportedPlatform;
    if (path.len > std.fs.max_path_bytes) return error.NameTooLong;

    var path_buf: [std.fs.max_path_bytes + 1]u8 = undefined;
    @memcpy(path_buf[0..path.len], path);
    path_buf[path.len] = 0;

    if (comptime builtin.os.tag == .linux) {
        var stat: c.struct_statfs = undefined;
        if (c.statfs(@ptrCast(&path_buf), &stat) != 0) return error.CapacityProbeFailed;
        return capacityFromStat(stat);
    }

    var stat: c.struct_statvfs = undefined;
    if (c.statvfs(@ptrCast(&path_buf), &stat) != 0) return error.CapacityProbeFailed;
    return capacityFromStat(stat);
}

fn capacityFromStat(stat: anytype) Capacity {
    const fragment_bytes: u64 = @intCast(if (stat.f_frsize != 0) stat.f_frsize else stat.f_bsize);
    const blocks: u64 = @intCast(stat.f_blocks);
    const available_blocks: u64 = @intCast(stat.f_bavail);
    return .{
        .total_bytes = std.math.mul(u64, fragment_bytes, blocks) catch std.math.maxInt(u64),
        .available_bytes = std.math.mul(u64, fragment_bytes, available_blocks) catch std.math.maxInt(u64),
    };
}

test "filesystem capacity reports the test volume" {
    if (!capacity_supported) return error.SkipZigTest;
    const observation = try capacity(".");
    try std.testing.expect(observation.total_bytes > 0);
    try std.testing.expect(observation.available_bytes <= observation.total_bytes);
}
