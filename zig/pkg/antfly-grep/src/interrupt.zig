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

//! CLI-owned cooperative cancellation. Signal handlers only publish atomics.
const std = @import("std");
const builtin = @import("builtin");
pub const Guard = if (builtin.os.tag == .windows) struct {
    pub fn init(_: *std.atomic.Value(bool)) @This() {
        return .{};
    }
    pub fn deinit(_: @This()) void {}
} else struct {
    old_int: std.posix.Sigaction,
    old_term: std.posix.Sigaction,
    var active: ?*std.atomic.Value(bool) = null;
    fn interrupted(_: std.posix.SIG) callconv(.c) void {
        if (active) |cancel| if (cancel.swap(true, .acq_rel)) std.c._exit(130);
    }
    pub fn init(cancel: *std.atomic.Value(bool)) @This() {
        active = cancel;
        const action = std.posix.Sigaction{ .handler = .{ .handler = interrupted }, .mask = std.posix.sigemptyset(), .flags = 0 };
        var self = @This(){ .old_int = undefined, .old_term = undefined };
        std.posix.sigaction(.INT, &action, &self.old_int);
        std.posix.sigaction(.TERM, &action, &self.old_term);
        return self;
    }
    pub fn deinit(self: @This()) void {
        std.posix.sigaction(.INT, &self.old_int, null);
        std.posix.sigaction(.TERM, &self.old_term, null);
        active = null;
    }
};

// Polling only affects stdin, where a producer may stay open indefinitely.
// Ordinary file readers keep their adaptive buffered/mapped paths.
pub const Stdin = if (builtin.os.tag == .windows) struct {
    pub fn init(io: std.Io, _: *const std.atomic.Value(bool), buffer: []u8) std.Io.File.Reader {
        return std.Io.File.stdin().readerStreaming(io, buffer);
    }
} else struct {
    interface: std.Io.Reader,
    cancel: *const std.atomic.Value(bool),
    pub fn init(_: std.Io, cancel: *const std.atomic.Value(bool), buffer: []u8) @This() {
        return .{ .cancel = cancel, .interface = .{ .vtable = &.{ .stream = stream }, .buffer = buffer, .seek = 0, .end = 0 } };
    }
    fn stream(reader: *std.Io.Reader, writer: *std.Io.Writer, limit: std.Io.Limit) std.Io.Reader.StreamError!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("interface", reader));
        var pollfd = [_]std.c.pollfd{.{ .fd = std.Io.File.stdin().handle, .events = std.c.POLL.IN, .revents = 0 }};
        while (!self.cancel.load(.acquire)) {
            const ready = std.c.poll(&pollfd, 1, 25);
            if (ready == 0) continue;
            if (ready < 0) {
                if (std.posix.errno(ready) == .INTR) continue;
                return error.ReadFailed;
            }
            var bytes: [16384]u8 = undefined;
            const capacity = limit.minInt(bytes.len);
            if (capacity == 0) return 0;
            const n = std.c.read(pollfd[0].fd, &bytes, capacity);
            if (n < 0) {
                if (std.posix.errno(n) == .INTR) continue;
                return error.ReadFailed;
            }
            if (n == 0) return error.EndOfStream;
            try writer.writeAll(bytes[0..@intCast(n)]);
            return @intCast(n);
        }
        return error.ReadFailed;
    }
};
