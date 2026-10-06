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

/// Process-fatal watchdog and owner-loss paths must bypass C atexit handlers
/// and stdio flushing: either can wait on a lock held by the stalled thread.
/// Callers must explicitly publish and flush any durable state beforehand.
pub fn exitImmediately(status: u8) noreturn {
    if (builtin.link_libc) std.c._Exit(status);
    std.process.exit(status);
}

fn hasPosixProcessApi() bool {
    return switch (builtin.os.tag) {
        .freestanding, .windows, .wasi => false,
        else => true,
    };
}

pub fn currentId() ?u32 {
    if (comptime !hasPosixProcessApi()) return null;
    return @intCast(std.posix.system.getpid());
}

pub fn alive(pid: u32) bool {
    if (pid == 0) return false;
    if (comptime !hasPosixProcessApi()) return true;
    switch (std.posix.errno(std.posix.system.kill(@intCast(pid), @fromBackingInt(@intCast(0))))) {
        .SUCCESS => return true,
        .SRCH => return false,
        .PERM => return true,
        else => return true,
    }
}

/// Iterates an argv subset the way `Args.Iterator.init(.{ .vector = argv })`
/// does on POSIX. Windows `Args` holds a WTF-16 command line rather than argv,
/// so the subset is serialized and reparsed there. The Windows buffers are
/// process-lifetime (experimental Windows support; CLI argument scopes only).
pub fn argsIterator(argv: []const [*:0]const u8) std.process.Args.Iterator {
    if (comptime builtin.os.tag != .windows) return std.process.Args.Iterator.init(.{ .vector = argv });
    const gpa = std.heap.page_allocator;
    const command_line = windowsCommandLine(gpa, "antfly", argv) catch @panic("out of memory serializing arguments");
    var it = std.process.Args.Iterator.initAllocator(.{ .vector = command_line }, gpa) catch @panic("out of memory parsing arguments");
    // The leading synthetic program name absorbs CommandLineToArgvW's
    // special first-argument rules.
    _ = it.skip();
    return it;
}

/// Serializes `program_name` followed by `args` into a WTF-16 command line
/// using the `CommandLineToArgvW` quoting rules. `program_name` is written
/// verbatim, so it must be a plain token without spaces or quotes.
pub fn windowsCommandLine(gpa: std.mem.Allocator, program_name: []const u8, args: []const [*:0]const u8) ![:0]u16 {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(gpa);
    try buf.appendSlice(gpa, program_name);
    for (args) |arg| {
        try buf.append(gpa, ' ');
        try appendWindowsArg(gpa, &buf, std.mem.span(arg));
    }
    return try std.unicode.wtf8ToWtf16LeAllocZ(gpa, buf.items);
}

/// Appends one non-first argument, quoted the way std's private
/// `argvToCommandLineWindows` quotes arguments after the program name.
fn appendWindowsArg(gpa: std.mem.Allocator, buf: *std.ArrayList(u8), arg: []const u8) !void {
    const needs_quotes = for (arg) |c| {
        if (c <= ' ' or c == '"') break true;
    } else arg.len == 0;
    if (!needs_quotes) return buf.appendSlice(gpa, arg);
    try buf.append(gpa, '"');
    var backslashes: usize = 0;
    for (arg) |byte| switch (byte) {
        '\\' => backslashes += 1,
        '"' => {
            try buf.appendNTimes(gpa, '\\', backslashes * 2 + 1);
            try buf.append(gpa, '"');
            backslashes = 0;
        },
        else => {
            try buf.appendNTimes(gpa, '\\', backslashes);
            try buf.append(gpa, byte);
            backslashes = 0;
        },
    };
    try buf.appendNTimes(gpa, '\\', backslashes * 2);
    try buf.append(gpa, '"');
}
