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

extern fn antfly_platform_filesystem_capacity(
    path: [*:0]const u8,
    fragment_bytes: *u64,
    blocks: *u64,
    available_blocks: *u64,
) c_int;

pub const Capacity = struct {
    total_bytes: u64,
    available_bytes: u64,
};

/// Return capacity available to an unprivileged writer on the filesystem that
/// contains `path`. Saturating overflow handling keeps the observation safe on
/// unusual filesystems that report sentinel block counts.
pub fn capacity(path: []const u8) !Capacity {
    if (!capacity_supported) return error.UnsupportedPlatform;
    if (path.len > std.fs.max_path_bytes) return error.NameTooLong;

    var path_buf: [std.fs.max_path_bytes + 1]u8 = undefined;
    @memcpy(path_buf[0..path.len], path);
    path_buf[path.len] = 0;

    var fragment_bytes: u64 = undefined;
    var blocks: u64 = undefined;
    var available_blocks: u64 = undefined;
    if (antfly_platform_filesystem_capacity(
        @ptrCast(&path_buf),
        &fragment_bytes,
        &blocks,
        &available_blocks,
    ) != 0) return error.CapacityProbeFailed;

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
