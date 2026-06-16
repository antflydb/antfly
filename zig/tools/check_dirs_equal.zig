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

pub fn main(init: std.process.Init) !void {
    const gpa = init.gpa;
    const io = init.io;

    var arena_impl = std.heap.ArenaAllocator.init(gpa);
    defer arena_impl.deinit();
    const arena = arena_impl.allocator();

    const argv = try init.minimal.args.toSlice(arena);
    if (argv.len < 3 or (argv.len - 1) % 2 != 0) {
        std.debug.print("usage: check-dirs-equal <actual-dir> <expected-dir> [<actual-dir> <expected-dir>...]\n", .{});
        std.process.exit(2);
    }

    var i: usize = 1;
    while (i < argv.len) : (i += 2) {
        try checkDirsEqual(gpa, io, argv[i], argv[i + 1]);
    }
}

fn checkDirsEqual(gpa: std.mem.Allocator, io: std.Io, actual_root: []const u8, expected_root: []const u8) !void {
    try compareExpectedFiles(gpa, io, actual_root, expected_root);
    try rejectUnexpectedActualFiles(gpa, io, actual_root, expected_root);
}

fn compareExpectedFiles(gpa: std.mem.Allocator, io: std.Io, actual_root: []const u8, expected_root: []const u8) !void {
    var expected_dir = std.Io.Dir.cwd().openDir(io, expected_root, .{ .iterate = true }) catch |err| {
        std.debug.print("check-dirs-equal: unable to open expected directory {s}: {}\n", .{ expected_root, err });
        std.process.exit(1);
    };
    defer expected_dir.close(io);

    var walker = try expected_dir.walk(gpa);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        const actual_path = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ actual_root, entry.path });
        defer gpa.free(actual_path);
        const expected_path = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ expected_root, entry.path });
        defer gpa.free(expected_path);
        try compareFiles(gpa, io, actual_path, expected_path);
    }
}

fn rejectUnexpectedActualFiles(gpa: std.mem.Allocator, io: std.Io, actual_root: []const u8, expected_root: []const u8) !void {
    var actual_dir = std.Io.Dir.cwd().openDir(io, actual_root, .{ .iterate = true }) catch |err| {
        std.debug.print("check-dirs-equal: unable to open actual directory {s}: {}\n", .{ actual_root, err });
        std.process.exit(1);
    };
    defer actual_dir.close(io);

    var walker = try actual_dir.walk(gpa);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        const expected_path = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ expected_root, entry.path });
        defer gpa.free(expected_path);
        std.Io.Dir.cwd().access(io, expected_path, .{}) catch |err| switch (err) {
            error.FileNotFound => {
                std.debug.print("check-dirs-equal: generated unexpected file {s}/{s}\n", .{ actual_root, entry.path });
                std.process.exit(1);
            },
            else => return err,
        };
    }
}

fn compareFiles(gpa: std.mem.Allocator, io: std.Io, actual_path: []const u8, expected_path: []const u8) !void {
    const actual = std.Io.Dir.cwd().readFileAlloc(io, actual_path, gpa, .limited(20 * 1024 * 1024)) catch |err| {
        std.debug.print("check-dirs-equal: unable to read actual {s}: {}\n", .{ actual_path, err });
        std.process.exit(1);
    };
    defer gpa.free(actual);

    const expected = std.Io.Dir.cwd().readFileAlloc(io, expected_path, gpa, .limited(20 * 1024 * 1024)) catch |err| {
        std.debug.print("check-dirs-equal: unable to read expected {s}: {}\n", .{ expected_path, err });
        std.process.exit(1);
    };
    defer gpa.free(expected);

    if (!std.mem.eql(u8, actual, expected)) {
        std.debug.print("check-dirs-equal: {s} differs from generated {s}\n", .{ expected_path, actual_path });
        std.process.exit(1);
    }
}
