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

//! Simple runtime-filtering test runner for debug tooling.
//!
//! Zig's default test runner does not accept test filters at runtime. The Metal
//! crash-debug script uses this runner to build one all-tests binary, then run
//! filtered chunks as separate processes with Metal validation enabled.

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");

var log_err_count: usize = 0;

pub const std_options: std.Options = .{
    .logFn = log,
};

pub fn main(init: std.process.Init.Minimal) !void {
    const allocator = std.heap.page_allocator;
    const args = try init.args.toSlice(allocator);
    defer allocator.free(args);

    var filters: std.ArrayList([]const u8) = .empty;
    defer filters.deinit(allocator);

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--listen=-")) {
            continue;
        } else if (std.mem.startsWith(u8, arg, "--seed=")) {
            std.testing.random_seed = std.fmt.parseUnsigned(u32, arg["--seed=".len..], 0) catch
                @panic("unable to parse --seed command line argument");
        } else if (std.mem.startsWith(u8, arg, "--cache-dir=")) {
            continue;
        } else if (std.mem.eql(u8, arg, "--test-filter")) {
            i += 1;
            if (i >= args.len) @panic("missing value after --test-filter");
            try filters.append(allocator, args[i]);
        } else {
            try filters.append(allocator, arg);
        }
    }

    const test_fn_list = builtin.test_functions;
    const list_tests_path = getenvSpan("TERMITE_TEST_LIST_FILE");
    if (list_tests_path) |path| {
        try writeSelectedRuntimeTests(path, test_fn_list, filters.items);
        return;
    }
    const runtime_offset = getenvUsize("TERMITE_TEST_RUNTIME_OFFSET") orelse 0;
    const runtime_limit = getenvUsize("TERMITE_TEST_RUNTIME_LIMIT") orelse std.math.maxInt(usize);

    var matched_count: usize = 0;
    var selected_count: usize = 0;
    var ok_count: usize = 0;
    var skip_count: usize = 0;
    var fail_count: usize = 0;
    var leak_count: usize = 0;

    for (test_fn_list) |test_fn| {
        if (!matchesAnyFilter(test_fn.name, filters.items)) continue;
        matched_count += 1;
        if (matched_count <= runtime_offset) continue;
        if (selected_count >= runtime_limit) break;
        selected_count += 1;

        std.testing.allocator_instance = .{};
        std.testing.io_instance = .init(std.testing.allocator, .{
            .argv0 = .init(init.args),
            .environ = init.environ,
        });
        std.testing.environ = init.environ;
        std.testing.log_level = .warn;
        log_err_count = 0;

        writeRuntimeTestProgress(selected_count, test_fn.name);
        std.debug.print("{d}/{d} {s}...", .{ selected_count, test_fn_list.len, test_fn.name });
        if (test_fn.func()) |_| {
            ok_count += 1;
            std.debug.print("OK\n", .{});
        } else |err| switch (err) {
            error.SkipZigTest => {
                skip_count += 1;
                std.debug.print("SKIP\n", .{});
            },
            else => {
                fail_count += 1;
                std.debug.print("FAIL ({t})\n", .{err});
                if (@errorReturnTrace()) |trace| std.debug.dumpErrorReturnTrace(trace);
            },
        }

        std.testing.io_instance.deinit();
        if (std.testing.allocator_instance.deinit() == .leak) leak_count += 1;
        if (log_err_count != 0) fail_count += 1;
    }

    if (selected_count == 0) {
        std.debug.print("No tests matched runtime filters.\n", .{});
        std.process.exit(1);
    }
    if (fail_count == 0 and leak_count == 0) {
        std.debug.print("{d} selected; {d} passed; {d} skipped.\n", .{ selected_count, ok_count, skip_count });
        return;
    }
    std.debug.print("{d} selected; {d} passed; {d} skipped; {d} failed; {d} leaked.\n", .{
        selected_count,
        ok_count,
        skip_count,
        fail_count,
        leak_count,
    });
    std.process.exit(1);
}

fn matchesAnyFilter(name: []const u8, filters: []const []const u8) bool {
    if (filters.len == 0) return true;
    for (filters) |filter| {
        if (filter.len == 0) continue;
        if (std.mem.indexOf(u8, name, filter) != null) return true;
    }
    return false;
}

fn getenvSpan(comptime name: [:0]const u8) ?[]const u8 {
    const value = platform.env.getenvSlice(name) orelse return null;
    if (value.len == 0) return null;
    return value;
}

fn getenvUsize(comptime name: [:0]const u8) ?usize {
    const value = getenvSpan(name) orelse return null;
    return std.fmt.parseUnsigned(usize, value, 10) catch null;
}

fn writeSelectedRuntimeTests(path: []const u8, test_fn_list: []const std.builtin.TestFn, filters: []const []const u8) !void {
    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{ .truncate = true });
    defer file.close(std.testing.io);

    var buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &buf);
    for (test_fn_list) |test_fn| {
        if (!matchesAnyFilter(test_fn.name, filters)) continue;
        try writer.interface.writeAll(test_fn.name);
        try writer.interface.writeAll("\n");
    }
    try writer.end();
    try fsyncFile(file.handle);
}

fn writeRuntimeTestProgress(index: usize, name: []const u8) void {
    if (getenvSpan("TERMITE_TEST_CURRENT_FILE")) |path| {
        writeRuntimeCurrentTestPath(path, name) catch {};
    }
    if (getenvSpan("TERMITE_TEST_TRACE_FILE")) |path| {
        appendRuntimeTracePath(path, index, name) catch {};
    }
}

fn writeRuntimeCurrentTestPath(path: []const u8, name: []const u8) !void {
    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{ .truncate = true });
    defer file.close(std.testing.io);

    var buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &buf);
    try writer.interface.writeAll(name);
    try writer.interface.writeAll("\n");
    try writer.end();

    try fsyncFile(file.handle);
}

fn appendRuntimeTracePath(path: []const u8, index: usize, name: []const u8) !void {
    var buf: [4096]u8 = undefined;
    const line = try std.fmt.bufPrint(&buf, "{d}\t{s}\n", .{ index, name });

    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .WRONLY,
        .CREAT = true,
        .APPEND = true,
        .CLOEXEC = true,
    }, 0o666);

    var file: std.Io.File = .{ .handle = fd, .flags = .{ .nonblocking = false } };
    defer file.close(std.testing.io);
    try writeAllFd(fd, line);
    try fsyncFile(fd);
}

fn writeAllFd(fd: std.posix.fd_t, bytes: []const u8) !void {
    var offset: usize = 0;
    while (offset < bytes.len) {
        const rc = std.c.write(fd, bytes[offset..].ptr, bytes.len - offset);
        if (rc < 0) return error.InputOutput;
        if (rc == 0) return error.InputOutput;
        offset += @intCast(rc);
    }
}

fn fsyncFile(handle: std.posix.fd_t) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return;
    while (true) switch (std.posix.errno(std.posix.system.fsync(handle))) {
        .SUCCESS => return,
        .INTR => continue,
        .INVAL => return,
        .BADF => unreachable,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        .DQUOT => return error.DiskQuota,
        else => |err| return std.posix.unexpectedErrno(err),
    };
}

pub fn log(
    comptime message_level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    if (@intFromEnum(message_level) <= @intFromEnum(std.log.Level.err)) {
        log_err_count +|= 1;
    }
    if (@intFromEnum(message_level) <= @intFromEnum(std.testing.log_level)) {
        std.debug.print(
            "[" ++ @tagName(scope) ++ "] (" ++ @tagName(message_level) ++ "): " ++ format ++ "\n",
            args,
        );
    }
}
