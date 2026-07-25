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

fn hasPosixProcessApi() bool {
    return switch (builtin.os.tag) {
        .freestanding, .windows, .wasi => false,
        else => true,
    };
}

pub fn currentId() ?u32 {
    if (!hasPosixProcessApi()) return null;
    return @intCast(std.posix.system.getpid());
}

pub fn alive(pid: u32) bool {
    if (pid == 0) return false;
    if (!hasPosixProcessApi()) return true;
    switch (std.posix.errno(std.posix.system.kill(@intCast(pid), @enumFromInt(0)))) {
        .SUCCESS => return true,
        .SRCH => return false,
        .PERM => return true,
        else => return true,
    }
}

pub const FileDescriptorLimit = struct {
    initial_soft: u64,
    soft: u64,
    hard: u64,
};

/// Raises the process file-descriptor soft limit toward `target`, without
/// exceeding the hard limit. The caller owns the policy and any logging.
pub fn ensureFileDescriptorSoftLimitAtLeast(target: u64) !FileDescriptorLimit {
    if (comptime std.posix.rlimit_resource == void) return error.UnsupportedPlatform;

    const initial = try std.posix.getrlimit(.NOFILE);
    const initial_soft: u64 = @intCast(initial.cur);
    const hard: u64 = @intCast(initial.max);
    const desired = desiredFileDescriptorSoftLimit(initial_soft, hard, target);
    if (desired > initial_soft) {
        var raised = initial;
        raised.cur = @intCast(desired);
        try std.posix.setrlimit(.NOFILE, raised);
    }
    const final = try std.posix.getrlimit(.NOFILE);
    return .{
        .initial_soft = initial_soft,
        .soft = @intCast(final.cur),
        .hard = @intCast(final.max),
    };
}

fn desiredFileDescriptorSoftLimit(current: u64, hard: u64, target: u64) u64 {
    return @max(current, @min(hard, target));
}

test "file descriptor target is bounded by hard limit" {
    try std.testing.expectEqual(@as(u64, 4096), desiredFileDescriptorSoftLimit(256, 8192, 4096));
    try std.testing.expectEqual(@as(u64, 1024), desiredFileDescriptorSoftLimit(256, 1024, 4096));
    try std.testing.expectEqual(@as(u64, 8192), desiredFileDescriptorSoftLimit(8192, 8192, 4096));
}
