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
const toon = @import("antfly_toon");

const Allocator = std.mem.Allocator;

const default_root_dir = "/tmp/toon-format-spec";
const default_repo_url = "https://github.com/toon-format/spec";
const max_fixture_bytes = 2 * 1024 * 1024;

const Config = struct {
    root_dir: []const u8 = default_root_dir,
    allow_fetch: bool = true,
    print_successes: bool = false,
};

const Summary = struct {
    files: usize = 0,
    tests: usize = 0,
    passed: usize = 0,
    failed: usize = 0,
    expected_errors: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "lib-toon-conformance";
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return error.InvalidArguments;
    };

    if (std.mem.eql(u8, subcommand, "fetch")) {
        const root_dir = args.next() orelse default_root_dir;
        try ensureFixturesAvailable(alloc, root_dir, true);
        std.debug.print("toon-format/spec fixtures ready at {s}\n", .{root_dir});
        return;
    }

    if (std.mem.eql(u8, subcommand, "status")) {
        const root_dir = args.next() orelse default_root_dir;
        std.debug.print("toon conformance: root={s} fixtures={s}\n", .{ root_dir, if (fixturesPresent(alloc, root_dir)) "present" else "missing" });
        return;
    }

    if (std.mem.eql(u8, subcommand, "run")) {
        var config = Config{};
        if (args.next()) |root_dir| config.root_dir = root_dir;
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--no-fetch")) {
                config.allow_fetch = false;
            } else if (std.mem.eql(u8, arg, "--print-successes")) {
                config.print_successes = true;
            } else {
                printUsage(argv0);
                return error.InvalidArguments;
            }
        }

        try ensureFixturesAvailable(alloc, config.root_dir, config.allow_fetch);
        const summary = try runFixtures(alloc, config);
        std.debug.print(
            "toon conformance: files={d} tests={d} passed={d} expected_errors={d} failed={d}\n",
            .{ summary.files, summary.tests, summary.passed, summary.expected_errors, summary.failed },
        );
        if (summary.failed != 0) return error.ToonConformanceFailed;
        return;
    }

    printUsage(argv0);
    return error.InvalidArguments;
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage: {s} fetch [root_dir]
        \\       {s} run [root_dir] [--no-fetch] [--print-successes]
        \\       {s} status [root_dir]
        \\
    , .{ argv0, argv0, argv0 });
}

fn ensureFixturesAvailable(alloc: Allocator, root_dir: []const u8, allow_fetch: bool) !void {
    if (fixturesPresent(alloc, root_dir)) return;
    if (!allow_fetch) return error.ToonFixturesUnavailable;
    try runChild(&.{ "git", "clone", "--depth=1", default_repo_url, root_dir });
    if (!fixturesPresent(alloc, root_dir)) return error.ToonFixturesUnavailable;
}

fn fixturesPresent(alloc: Allocator, root_dir: []const u8) bool {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const fixtures_dir = std.fs.path.join(alloc, &.{ root_dir, "tests", "fixtures" }) catch return false;
    defer alloc.free(fixtures_dir);
    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), fixtures_dir, .{}) catch return false;
    dir.close(io_impl.io());
    return true;
}

fn runFixtures(alloc: Allocator, config: Config) !Summary {
    var summary = Summary{};
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const fixtures_dir = try std.fs.path.join(alloc, &.{ config.root_dir, "tests", "fixtures" });
    defer alloc.free(fixtures_dir);

    var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), fixtures_dir, .{ .iterate = true });
    defer dir.close(io_impl.io());
    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".json")) continue;
        const path = try std.fs.path.join(alloc, &.{ fixtures_dir, entry.path });
        defer alloc.free(path);
        const bytes = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_fixture_bytes));
        defer alloc.free(bytes);
        summary.files += 1;
        try runFixtureFile(alloc, entry.path, bytes, config, &summary);
    }

    return summary;
}

fn runFixtureFile(alloc: Allocator, path: []const u8, bytes: []const u8, config: Config, summary: *Summary) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, bytes, .{});
    defer parsed.deinit();
    const category = parsed.value.object.get("category").?.string;
    const tests = parsed.value.object.get("tests").?.array.items;

    for (tests) |case| {
        summary.tests += 1;
        const name = case.object.get("name").?.string;
        const should_error = if (case.object.get("shouldError")) |v| v == .bool and v.bool else false;
        const options_value = case.object.get("options");

        const passed = if (std.mem.eql(u8, category, "encode"))
            try runEncodeCase(alloc, case, options_value, should_error)
        else if (std.mem.eql(u8, category, "decode"))
            try runDecodeCase(alloc, case, options_value, should_error)
        else
            false;

        if (passed) {
            summary.passed += 1;
            if (should_error) summary.expected_errors += 1;
            if (config.print_successes) std.debug.print("PASS\t{s}\t{s}\n", .{ path, name });
        } else {
            summary.failed += 1;
            std.debug.print("FAIL\t{s}\t{s}\n", .{ path, name });
        }
    }
}

fn runEncodeCase(alloc: Allocator, case: std.json.Value, options_value: ?std.json.Value, should_error: bool) !bool {
    const options = parseEncodeOptions(options_value);
    const actual = toon.encodeValueAlloc(alloc, case.object.get("input").?, options) catch |err| {
        return should_error and err != error.OutOfMemory;
    };
    defer alloc.free(actual);
    if (should_error) return false;
    const expected = case.object.get("expected").?.string;
    if (!std.mem.eql(u8, actual, expected)) {
        std.debug.print("  expected: {s}\n  actual:   {s}\n", .{ expected, actual });
        return false;
    }
    return true;
}

fn runDecodeCase(alloc: Allocator, case: std.json.Value, options_value: ?std.json.Value, should_error: bool) !bool {
    const options = parseDecodeOptions(options_value);
    const input = case.object.get("input").?.string;
    var parsed = toon.decodeValueAlloc(alloc, input, options) catch |err| {
        return should_error and err != error.OutOfMemory;
    };
    defer parsed.deinit();
    if (should_error) return false;
    const expected = case.object.get("expected").?;
    if (!jsonEqual(parsed.value, expected)) {
        const actual_json = try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
        defer alloc.free(actual_json);
        const expected_json = try std.json.Stringify.valueAlloc(alloc, expected, .{});
        defer alloc.free(expected_json);
        std.debug.print("  expected: {s}\n  actual:   {s}\n", .{ expected_json, actual_json });
        return false;
    }
    return true;
}

fn parseEncodeOptions(value: ?std.json.Value) toon.EncodeOptions {
    var options = toon.EncodeOptions{};
    const object = if (value) |v| if (v == .object) v.object else return options else return options;
    if (object.get("indent")) |indent| {
        if (indent == .integer and indent.integer > 0) options.indent = @intCast(indent.integer);
    }
    if (object.get("delimiter")) |delimiter| {
        if (delimiter == .string) {
            if (std.mem.eql(u8, delimiter.string, "\t")) options.delimiter = .tab;
            if (std.mem.eql(u8, delimiter.string, "|")) options.delimiter = .pipe;
            if (std.mem.eql(u8, delimiter.string, ",")) options.delimiter = .comma;
        }
    }
    if (object.get("keyFolding")) |mode| {
        if (mode == .string and std.mem.eql(u8, mode.string, "safe")) options.key_folding = .safe;
    }
    if (object.get("flattenDepth")) |depth| {
        if (depth == .integer and depth.integer >= 0) options.flatten_depth = @intCast(depth.integer);
    }
    return options;
}

fn parseDecodeOptions(value: ?std.json.Value) toon.DecodeOptions {
    var options = toon.DecodeOptions{};
    const object = if (value) |v| if (v == .object) v.object else return options else return options;
    if (object.get("indent")) |indent| {
        if (indent == .integer and indent.integer > 0) options.indent = @intCast(indent.integer);
    }
    if (object.get("strict")) |strict| {
        if (strict == .bool) options.strict = strict.bool;
    }
    if (object.get("expandPaths")) |mode| {
        if (mode == .string and std.mem.eql(u8, mode.string, "safe")) options.expand_paths = .safe;
    }
    return options;
}

fn jsonEqual(a: std.json.Value, b: std.json.Value) bool {
    if (a == .integer and b == .float) return @as(f64, @floatFromInt(a.integer)) == b.float;
    if (a == .float and b == .integer) return a.float == @as(f64, @floatFromInt(b.integer));
    if (@intFromEnum(a) != @intFromEnum(b)) return false;
    return switch (a) {
        .null => true,
        .bool => a.bool == b.bool,
        .integer => a.integer == b.integer,
        .float => a.float == b.float,
        .number_string => std.mem.eql(u8, a.number_string, b.number_string),
        .string => std.mem.eql(u8, a.string, b.string),
        .array => blk: {
            if (a.array.items.len != b.array.items.len) break :blk false;
            for (a.array.items, b.array.items) |left, right| {
                if (!jsonEqual(left, right)) break :blk false;
            }
            break :blk true;
        },
        .object => blk: {
            if (a.object.count() != b.object.count()) break :blk false;
            var left_it = a.object.iterator();
            var right_it = b.object.iterator();
            while (left_it.next()) |left| {
                const right = right_it.next() orelse break :blk false;
                if (!std.mem.eql(u8, left.key_ptr.*, right.key_ptr.*)) break :blk false;
                if (!jsonEqual(left.value_ptr.*, right.value_ptr.*)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn runChild(argv: []const []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();

    var child = try std.process.spawn(io_impl.io(), .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    });
    const term = try child.wait(io_impl.io());
    switch (term) {
        .exited => |code| if (code != 0) return error.ChildProcessFailed,
        else => return error.ChildProcessFailed,
    }
}
