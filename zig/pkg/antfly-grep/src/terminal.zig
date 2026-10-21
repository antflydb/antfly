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

//! Local terminal input with scoped, restored echo settings.
const std = @import("std");
const windows = @import("builtin").os.tag == .windows;
const EchoGuard = if (windows) struct {
    original: u32,
    handle: std.os.windows.HANDLE,
    extern "kernel32" fn GetConsoleMode(std.os.windows.HANDLE, *u32) callconv(.winapi) i32;
    extern "kernel32" fn SetConsoleMode(std.os.windows.HANDLE, u32) callconv(.winapi) i32;
    fn init() !@This() {
        const handle = std.Io.File.stdin().handle;
        var original: u32 = undefined;
        if (GetConsoleMode(handle, &original) == 0 or SetConsoleMode(handle, original & ~@as(u32, 4)) == 0) return error.TerminalConfigurationFailed;
        return .{ .original = original, .handle = handle };
    }
    fn restore(self: @This()) void {
        _ = SetConsoleMode(self.handle, self.original);
    }
} else struct {
    original: std.c.termios,
    old_int: std.posix.Sigaction,
    old_term: std.posix.Sigaction,
    var active: ?std.c.termios = null;
    fn interrupted(signal: std.posix.SIG) callconv(.c) void {
        if (active) |*original| _ = std.c.tcsetattr(std.Io.File.stdin().handle, .NOW, original);
        std.c._exit(128 + @as(c_int, @intCast(@backingInt(signal))));
    }
    fn init() !@This() {
        var original: std.c.termios = undefined;
        if (std.c.tcgetattr(std.Io.File.stdin().handle, &original) != 0) return error.TerminalConfigurationFailed;
        var changed = original;
        changed.lflag.ECHO = false;
        changed.lflag.ECHONL = false;
        if (std.c.tcsetattr(std.Io.File.stdin().handle, .NOW, &changed) != 0) return error.TerminalConfigurationFailed;
        active = original;
        const action = std.posix.Sigaction{ .handler = .{ .handler = interrupted }, .mask = std.posix.sigemptyset(), .flags = 0 };
        var guard = @This(){ .original = original, .old_int = undefined, .old_term = undefined };
        std.posix.sigaction(.INT, &action, &guard.old_int);
        std.posix.sigaction(.TERM, &action, &guard.old_term);
        return guard;
    }
    fn restore(self: @This()) void {
        _ = std.c.tcsetattr(std.Io.File.stdin().handle, .NOW, &self.original);
        std.posix.sigaction(.INT, &self.old_int, null);
        std.posix.sigaction(.TERM, &self.old_term, null);
        active = null;
    }
};
pub fn prompt(a: std.mem.Allocator, io: std.Io, label: []const u8, secret: bool) ![]u8 {
    const input = std.Io.File.stdin();
    if (!try input.isTty(io)) return error.AuthRequiresTerminalOrStdin;
    const stderr = std.Io.File.stderr();
    const guard: ?EchoGuard = if (secret) try .init() else null;
    defer if (guard) |g| g.restore();
    try stderr.writeStreamingAll(io, label);
    defer if (secret) stderr.writeStreamingAll(io, "\n") catch {};
    var result: std.ArrayList(u8) = .empty;
    errdefer result.deinit(a);
    while (true) {
        var byte: [1]u8 = undefined;
        if (try input.readStreaming(io, &.{&byte}) == 0) return error.EndOfStream;
        if (byte[0] == '\n') break;
        if (result.items.len == 65536) return error.CredentialTooLong;
        try result.append(a, byte[0]);
    }
    if (std.mem.endsWith(u8, result.items, "\r")) result.items.len -= 1;
    return result.toOwnedSlice(a);
}
