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

const Violation = struct {
    path: []const u8,
    line: usize,
    text: []const u8,
};

const default_roots = [_][]const u8{
    "pkg/antfly/src/api",
    "pkg/antfly/src/graph",
    "pkg/antfly/src/storage/db",
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    const io = init.io;
    var violations = std.ArrayListUnmanaged(Violation).empty;
    defer {
        for (violations.items) |violation| {
            alloc.free(violation.path);
            alloc.free(violation.text);
        }
        violations.deinit(alloc);
    }

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    var scanned_roots: usize = 0;
    while (args.next()) |root| {
        try scanRoot(alloc, io, root, &violations);
        scanned_roots += 1;
    }
    if (scanned_roots == 0) {
        for (default_roots) |root| {
            try scanRoot(alloc, io, root, &violations);
            scanned_roots += 1;
        }
    } else {
        // Arguments are interpreted only as roots to keep this guardrail easy
        // to wire into build steps and CI jobs.
    }

    if (violations.items.len > 0) {
        for (violations.items) |violation| {
            std.debug.print("algebraic_planner_ownership_violation path={s} line={d} text={s}\n", .{
                violation.path,
                violation.line,
                violation.text,
            });
        }
        return error.AlgebraicPlannerOwnershipViolation;
    }

    std.debug.print("algebraic_planner_ownership_guardrail ok scanned_roots={d}\n", .{scanned_roots});
}

fn scanRoot(
    alloc: std.mem.Allocator,
    io: std.Io,
    root: []const u8,
    violations: *std.ArrayListUnmanaged(Violation),
) !void {
    var dir = try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
    defer dir.close(io);
    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".zig")) continue;
        const full_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, entry.path });
        defer alloc.free(full_path);
        try scanFile(alloc, io, full_path, violations);
    }
}

fn scanFile(
    alloc: std.mem.Allocator,
    io: std.Io,
    path: []const u8,
    violations: *std.ArrayListUnmanaged(Violation),
) !void {
    if (isPlannerOwnedSource(path)) return;
    const raw = try std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(64 * 1024 * 1024));
    defer alloc.free(raw);

    var iter = std.mem.splitScalar(u8, raw, '\n');
    var line_no: usize = 0;
    var in_test = false;
    var test_depth: isize = 0;
    while (iter.next()) |line| {
        line_no += 1;
        const code = stripLineComment(line);
        const trimmed = std.mem.trim(u8, code, " \t\r");
        const starts_test = startsTestBlock(trimmed);
        if (starts_test and !in_test) {
            in_test = true;
            test_depth = 0;
        }

        if (!in_test and constructsRawTensorProgram(trimmed)) {
            try violations.append(alloc, .{
                .path = try alloc.dupe(u8, path),
                .line = line_no,
                .text = try alloc.dupe(u8, trimmed),
            });
        }

        if (in_test) {
            test_depth += braceDelta(code);
            if (test_depth <= 0 and std.mem.indexOfScalar(u8, code, '{') != null) {
                in_test = false;
                test_depth = 0;
            }
        }
    }
}

fn isPlannerOwnedSource(path: []const u8) bool {
    return std.mem.endsWith(u8, path, "pkg/antfly/src/storage/db/algebraic/planner.zig") or
        std.mem.endsWith(u8, path, "pkg/antfly/src/storage/db/algebraic/ir.zig");
}

fn startsTestBlock(trimmed: []const u8) bool {
    return std.mem.startsWith(u8, trimmed, "test ") or
        std.mem.startsWith(u8, trimmed, "test\"");
}

fn stripLineComment(line: []const u8) []const u8 {
    if (std.mem.indexOf(u8, line, "//")) |idx| return line[0..idx];
    return line;
}

fn constructsRawTensorProgram(line: []const u8) bool {
    return std.mem.indexOf(u8, line, "TensorProgram{") != null or
        std.mem.indexOf(u8, line, "TensorProgram {") != null;
}

fn braceDelta(line: []const u8) isize {
    var delta: isize = 0;
    for (line) |c| {
        if (c == '{') delta += 1;
        if (c == '}') delta -= 1;
    }
    return delta;
}
