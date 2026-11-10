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

//! Native file-cache advice is an explicit capability supplied by the I/O
//! producer. No descriptor is retained, and no process-global registry exists.
const std = @import("std");
const builtin = @import("builtin");

pub const max_range_bytes: u64 = 8 * 1024 * 1024;

pub const Advice = extern struct {
    context: ?*anyopaque = null,
    discard_range: ?*const fn (?*anyopaque, i32, u64, u64) callconv(.c) void = null,

    /// Only the producer of an OS-backed I/O interface may grant this.
    pub fn native() Advice {
        return if (builtin.os.tag == .linux) .{ .discard_range = discardNative } else .{};
    }

    pub fn available(self: Advice) bool {
        return self.discard_range != null;
    }

    /// Used only by the file owner, after bulk output is synced or while its
    /// writer lease still excludes a successor. Never called by admission.
    /// Each syscall has a bounded range; yields allow other runtime work to
    /// progress between ranges. Hints may initiate writeback and are not credit.
    pub fn discard(self: Advice, io: std.Io, file: std.Io.File, bytes: u64) void {
        if (comptime builtin.os.tag != .linux) return;
        const callback = self.discard_range orelse return;
        var offset: u64 = 0;
        while (offset < bytes) {
            const len = @min(max_range_bytes, bytes - offset);
            callback(self.context, file.handle, offset, len);
            offset += len;
            if (offset < bytes) io.sleep(.zero, .awake) catch {};
        }
    }
};

fn discardNative(_: ?*anyopaque, fd: i32, offset: u64, bytes: u64) callconv(.c) void {
    if (comptime builtin.os.tag != .linux) return;
    std.debug.assert(bytes > 0 and bytes <= max_range_bytes);
    _ = std.os.linux.fadvise(fd, @intCast(offset), @intCast(bytes), std.os.linux.POSIX_FADV.DONTNEED);
}

test "file cache advice uses bounded non-overlapping ranges and retains no descriptors" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const Probe = struct {
        offset: u64 = 0,
        calls: usize = 0,
        fn discard(raw: ?*anyopaque, _: i32, offset: u64, len: u64) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            std.debug.assert(offset == self.offset and len > 0 and len <= max_range_bytes);
            self.offset += len;
            self.calls += 1;
        }
    };
    var probe = Probe{};
    const advice = Advice{ .context = &probe, .discard_range = Probe.discard };
    advice.discard(std.testing.io, .{ .handle = -1, .flags = .{ .nonblocking = false } }, max_range_bytes * 2 + 17);
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(max_range_bytes * 2 + 17, probe.offset);
}
