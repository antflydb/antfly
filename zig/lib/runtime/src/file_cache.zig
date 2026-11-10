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
/// Range-only producers bound their hint batches. Native terminal cleanup
/// uses the kernel cache index instead; bulk output drains incrementally.
pub const max_pass_bytes: u64 = 2 * 1024 * 1024 * 1024;
pub const bulk_window_bytes: u64 = 64 * 1024 * 1024;

/// Authority to advise a file does not imply permission to cool ordinary
/// reopen traffic. A physical-pressure epoch marks explicit retirement intent.
pub const Retirement = enum(u32) { on_pressure, retain, discard };

pub const Advice = extern struct {
    context: ?*anyopaque = null,
    /// Producer-owned terminal cleanup. Native implementations traverse the
    /// kernel cache index, not every logical offset (including sparse holes).
    discard_file: ?*const fn (?*anyopaque, i32) callconv(.c) void = null,
    discard_range: ?*const fn (?*anyopaque, i32, u64, u64) callconv(.c) void = null,

    /// Only the producer of an OS-backed I/O interface may grant this.
    pub fn native() Advice {
        return if (builtin.os.tag == .linux) .{ .discard_file = discardNativeFile, .discard_range = discardNative } else .{};
    }

    pub fn available(self: Advice) bool {
        return self.discard_file != null or self.discard_range != null;
    }

    /// Terminal owner cleanup, outside admission. The native operation visits
    /// cached folios at any offset, with work proportional to cache residency,
    /// not logical file size. No descriptor survives the operation. This is
    /// best-effort advice, not a hard wall-clock bound or allocation credit.
    pub fn discardFile(self: Advice, io: std.Io, file: std.Io.File) void {
        if (comptime builtin.os.tag != .linux) return;
        const callback = self.discard_file orelse return;
        io.checkCancel() catch return;
        callback(self.context, file.handle);
    }

    /// Bulk producers call this after syncing each output window. Offsets are
    /// file offsets, not a promise of physical bytes released by the kernel.
    pub fn discardRange(self: Advice, io: std.Io, file: std.Io.File, start: u64, bytes: u64) void {
        if (comptime builtin.os.tag != .linux) return;
        const callback = self.discard_range orelse return;
        const end = start +| @min(bytes, max_pass_bytes);
        var offset = start;
        var calls: usize = 0;
        while (offset < end) {
            io.checkCancel() catch return;
            const len = @min(max_range_bytes, end - offset);
            callback(self.context, file.handle, offset, len);
            offset += len;
            calls += 1;
            // Avoid one timer syscall per 8 MiB on sparse/uncached files.
            if (offset < end and calls % 32 == 0) io.sleep(.zero, .awake) catch return;
        }
    }
};

fn discardNativeFile(_: ?*anyopaque, fd: i32) callconv(.c) void {
    if (comptime builtin.os.tag != .linux) return;
    // len=0 covers EOF. Linux's invalidate_mapping_pages walks cached folios
    // in the mapping's xarray; it does not iterate over absent file pages.
    // See mm/fadvise.c and mm/truncate.c in the Linux source tree.
    _ = std.os.linux.fadvise(fd, 0, 0, std.os.linux.POSIX_FADV.DONTNEED);
}

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
    advice.discardRange(std.testing.io, .{ .handle = -1, .flags = .{ .nonblocking = false } }, 0, max_range_bytes * 2 + 17);
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
    try std.testing.expectEqual(max_range_bytes * 2 + 17, probe.offset);
}

test "file cache advice bounds total work for sparse files and honors cancellation" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const Probe = struct {
        bytes: u64 = 0,
        fn discard(raw: ?*anyopaque, _: i32, _: u64, len: u64) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.bytes += len;
        }
        fn cancel(_: ?*anyopaque) std.Io.Cancelable!void {
            return error.Canceled;
        }
    };
    var probe = Probe{};
    const advice = Advice{ .context = &probe, .discard_range = Probe.discard };
    const file: std.Io.File = .{ .handle = -1, .flags = .{ .nonblocking = false } };
    advice.discardRange(std.testing.io, file, 0, 1024 * 1024 * 1024 * 1024);
    try std.testing.expectEqual(max_pass_bytes, probe.bytes);
    var table = std.testing.io.vtable.*;
    table.checkCancel = Probe.cancel;
    const cancelled: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &table };
    advice.discardRange(cancelled, file, 0, max_pass_bytes);
    try std.testing.expectEqual(max_pass_bytes, probe.bytes);
}

test "file cache terminal capability covers arbitrary offsets without a logical range scan" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    const Probe = struct {
        calls: usize = 0,
        fn whole(raw: ?*anyopaque, _: i32) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
        }
        fn range(_: ?*anyopaque, _: i32, _: u64, _: u64) callconv(.c) void {
            @panic("terminal cleanup scanned logical ranges");
        }
        fn cancel(_: ?*anyopaque) std.Io.Cancelable!void {
            return error.Canceled;
        }
    };
    var probe = Probe{};
    const advice = Advice{ .context = &probe, .discard_file = Probe.whole, .discard_range = Probe.range };
    const file: std.Io.File = .{ .handle = -1, .flags = .{ .nonblocking = false } };
    advice.discardFile(std.testing.io, file);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    var table = std.testing.io.vtable.*;
    table.checkCancel = Probe.cancel;
    advice.discardFile(.{ .userdata = std.testing.io.userdata, .vtable = &table }, file);
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
}
