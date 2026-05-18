// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const builtin = @import("builtin");
const std = @import("std");
const structlog = @import("structlog");
const testing = std.testing;

pub const std_options: std.Options = .{
    .logFn = log,
};

var log_err_count: usize = 0;
var arg_buffer: [8192]u8 = undefined;
const max_filters = 64;
var test_filters: [max_filters][]const u8 = undefined;
var test_filter_count: usize = 0;
var skip_test_filters: [max_filters][]const u8 = undefined;
var skip_test_filter_count: usize = 0;

pub fn main(init: std.process.Init.Minimal) void {
    @disableInstrumentation();

    var fba = std.heap.FixedBufferAllocator.init(&arg_buffer);
    const args = init.args.toSlice(fba.allocator()) catch |err| {
        std.debug.panic("unable to parse command line args: {t}", .{err});
    };

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.startsWith(u8, arg, "--seed=")) {
            testing.random_seed = std.fmt.parseUnsigned(u32, arg["--seed=".len..], 0) catch
                @panic("unable to parse --seed command line argument");
        } else if (std.mem.startsWith(u8, arg, "--test-filter=")) {
            appendFilter("--test-filter", &test_filters, &test_filter_count, arg["--test-filter=".len..]);
        } else if (std.mem.eql(u8, arg, "--test-filter")) {
            i += 1;
            if (i >= args.len) @panic("missing value for --test-filter");
            appendFilter("--test-filter", &test_filters, &test_filter_count, args[i]);
        } else if (std.mem.startsWith(u8, arg, "--skip-test-filter=")) {
            appendFilter("--skip-test-filter", &skip_test_filters, &skip_test_filter_count, arg["--skip-test-filter=".len..]);
        } else if (std.mem.eql(u8, arg, "--skip-test-filter")) {
            i += 1;
            if (i >= args.len) @panic("missing value for --skip-test-filter");
            appendFilter("--skip-test-filter", &skip_test_filters, &skip_test_filter_count, args[i]);
        } else if (std.mem.startsWith(u8, arg, "--cache-dir=")) {
            // Accepted for compatibility with the default test runner.
        } else if (std.mem.eql(u8, arg, "--listen=-")) {
            // Accepted defensively; this runner does not implement the server protocol.
        } else {
            std.debug.panic("unrecognized command line argument: {s}", .{arg});
        }
    }

    const test_fns = builtin.test_functions;
    var ok_count: usize = 0;
    var skip_count: usize = 0;
    var fail_count: usize = 0;
    var leak_count: usize = 0;
    var total_count: usize = 0;

    for (test_fns) |test_fn| {
        if (matchesFilter(test_fn.name)) total_count += 1;
    }

    var current_count: usize = 0;
    for (test_fns) |test_fn| {
        if (!matchesFilter(test_fn.name)) continue;
        current_count += 1;
        testing.allocator_instance = .{};
        testing.io_instance = .init(testing.allocator, .{
            .argv0 = .init(init.args),
            .environ = init.environ,
        });
        testing.environ = init.environ;
        testing.log_level = .warn;

        std.debug.print("{d}/{d} {s}...", .{ current_count, total_count, test_fn.name });

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
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpErrorReturnTrace(trace);
                }
            },
        }

        testing.io_instance.deinit();
        if (testing.allocator_instance.deinit() == .leak) {
            leak_count += 1;
        }
    }

    std.debug.print(
        "{d} passed; {d} skipped; {d} failed; {d} leaked.\n",
        .{ ok_count, skip_count, fail_count, leak_count },
    );
    if (log_err_count != 0) {
        std.debug.print("{d} errors were logged.\n", .{log_err_count});
    }
    if (fail_count != 0 or leak_count != 0 or log_err_count != 0) {
        std.process.exit(1);
    }
}

fn matchesFilter(name: []const u8) bool {
    if (test_filter_count != 0) {
        var included = false;
        for (test_filters[0..test_filter_count]) |filter| {
            if (matchesSingleFilter(name, filter)) {
                included = true;
                break;
            }
        }
        if (!included) return false;
    }

    for (skip_test_filters[0..skip_test_filter_count]) |filter| {
        if (matchesSingleFilter(name, filter)) return false;
    }
    return true;
}

fn matchesSingleFilter(name: []const u8, filter: []const u8) bool {
    const target = if (std.mem.indexOfScalar(u8, filter, '.') != null)
        name
    else
        declaredTestName(name);
    return std.mem.indexOf(u8, target, filter) != null;
}

fn appendFilter(
    kind: []const u8,
    filters: *[max_filters][]const u8,
    count: *usize,
    filter: []const u8,
) void {
    if (count.* >= max_filters) {
        std.debug.panic("too many {s} arguments", .{kind});
    }
    filters[count.*] = filter;
    count.* += 1;
}

fn declaredTestName(name: []const u8) []const u8 {
    const marker = ".test.";
    if (std.mem.indexOf(u8, name, marker)) |idx| {
        return name[idx + marker.len ..];
    }
    return name;
}

pub fn log(
    comptime message_level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    @disableInstrumentation();
    if (@intFromEnum(message_level) <= @intFromEnum(std.log.Level.err)) {
        log_err_count +|= 1;
    }
    structlog.logFn(message_level, scope, format, args);
}
