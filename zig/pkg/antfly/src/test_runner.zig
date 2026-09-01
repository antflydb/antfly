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
const testing = std.testing;

pub const std_options: std.Options = .{
    .logFn = log,
};

var log_err_count: std.atomic.Value(usize) = .init(0);
var expected_error_log_count: std.atomic.Value(usize) = .init(0);
var test_filters: []const []const u8 = &.{};
var skip_test_filters: []const []const u8 = &.{};

pub fn main(init: std.process.Init.Minimal) void {
    @disableInstrumentation();

    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const args = init.args.toSlice(arena) catch |err| {
        std.debug.panic("unable to parse command line args: {t}", .{err});
    };
    var include_filters: std.ArrayList([]const u8) = .empty;
    var exclude_filters: std.ArrayList([]const u8) = .empty;
    var allow_empty_test_filter = false;
    var list_tests = false;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.startsWith(u8, arg, "--seed=")) {
            testing.random_seed = std.fmt.parseUnsigned(u32, arg["--seed=".len..], 0) catch
                @panic("unable to parse --seed command line argument");
        } else if (std.mem.startsWith(u8, arg, "--test-filter=")) {
            appendFilter(arena, "--test-filter", &include_filters, arg["--test-filter=".len..]);
        } else if (std.mem.eql(u8, arg, "--test-filter")) {
            i += 1;
            if (i >= args.len) @panic("missing value for --test-filter");
            appendFilter(arena, "--test-filter", &include_filters, args[i]);
        } else if (std.mem.startsWith(u8, arg, "--skip-test-filter=")) {
            appendFilter(arena, "--skip-test-filter", &exclude_filters, arg["--skip-test-filter=".len..]);
        } else if (std.mem.eql(u8, arg, "--skip-test-filter")) {
            i += 1;
            if (i >= args.len) @panic("missing value for --skip-test-filter");
            appendFilter(arena, "--skip-test-filter", &exclude_filters, args[i]);
        } else if (std.mem.eql(u8, arg, "--allow-empty-test-filter")) {
            allow_empty_test_filter = true;
        } else if (std.mem.eql(u8, arg, "--list-tests")) {
            list_tests = true;
        } else if (std.mem.startsWith(u8, arg, "--cache-dir=")) {
            // Accepted for compatibility with the default test runner.
        } else if (std.mem.eql(u8, arg, "--listen=-")) {
            // Accepted defensively; this runner does not implement the server protocol.
        } else {
            std.debug.panic("unrecognized command line argument: {s}", .{arg});
        }
    }
    test_filters = include_filters.items;
    skip_test_filters = exclude_filters.items;

    const test_fns = builtin.test_functions;
    var ok_count: usize = 0;
    var skip_count: usize = 0;
    var fail_count: usize = 0;
    var leak_count: usize = 0;
    var matched_error_log_count: usize = 0;
    var total_count: usize = 0;
    const matched_filter_counts = arena.alloc(usize, test_filters.len) catch
        @panic("out of memory while allocating test filter counters");
    @memset(matched_filter_counts, 0);

    for (test_fns) |test_fn| {
        recordMatchingIncludeFilters(test_fn.name, matched_filter_counts);
        if (matchesFilter(test_fn.name)) total_count += 1;
    }

    var missing_filter_count: usize = 0;
    for (test_filters, 0..) |filter, filter_index| {
        if (matched_filter_counts[filter_index] != 0) continue;
        missing_filter_count += 1;
        if (!allow_empty_test_filter) {
            std.debug.print("test filter matched no declared tests: {s}\n", .{filter});
        }
    }
    if (missing_filter_count != 0 and !allow_empty_test_filter) {
        std.process.exit(1);
    }
    if (total_count == 0) {
        if (allow_empty_test_filter) {
            return;
        }
        std.debug.print("test selection matched no runnable tests\n", .{});
        std.process.exit(1);
    }

    if (list_tests) {
        for (test_fns) |test_fn| {
            if (matchesFilter(test_fn.name)) {
                std.debug.print("TEST\t{s}\n", .{test_fn.name});
            }
        }
        return;
    }

    const trace_cleanup = getenvBool("ANTFLY_TEST_CLEANUP_TRACE");
    const fail_on_error_logs = getenvBool("ANTFLY_TEST_FAIL_ON_ERROR_LOGS");
    var current_count: usize = 0;
    for (test_fns) |test_fn| {
        if (!matchesFilter(test_fn.name)) continue;
        current_count += 1;
        // Print attribution before initializing per-test I/O. If platform I/O
        // setup itself terminates the process, CI still identifies the test
        // boundary instead of reporting an anonymous signal.
        std.debug.print("{d}/{d} {s}...", .{ current_count, total_count, test_fn.name });
        testing.allocator_instance = .init(std.heap.page_allocator, .{});
        testing.io_instance = .init(testing.allocator, .{
            .argv0 = .init(init.args),
            .environ = init.environ,
        });
        testing.environ = init.environ;
        testing.log_level = .warn;
        expected_error_log_count.store(0, .release);
        const error_logs_before = log_err_count.load(.acquire);

        const Outcome = enum { passed, skipped, failed };
        var outcome: Outcome = .passed;
        if (test_fn.func()) |_| {} else |err| switch (err) {
            error.SkipZigTest => {
                outcome = .skipped;
            },
            else => {
                outcome = .failed;
                // Logs emitted by a test can split the leading test name from
                // its result. Repeat it on failure so CI attribution survives
                // interleaved diagnostics and truncated log windows.
                std.debug.print("FAIL ({t}) {s}\n", .{ err, test_fn.name });
                if (@errorReturnTrace()) |trace| {
                    std.debug.dumpErrorReturnTrace(trace);
                }
            },
        }

        if (trace_cleanup) std.debug.print("CLEANUP io_deinit begin {s}\n", .{test_fn.name});
        testing.io_instance.deinit();
        if (trace_cleanup) std.debug.print("CLEANUP allocator_deinit begin {s}\n", .{test_fn.name});
        if (testing.allocator_instance.deinit() != 0) {
            leak_count += 1;
        }
        if (trace_cleanup) std.debug.print("CLEANUP done {s}\n", .{test_fn.name});

        const error_logs_after = log_err_count.load(.acquire);
        const actual_error_logs = error_logs_after -| error_logs_before;
        const expected_error_logs = expected_error_log_count.swap(0, .acq_rel);
        const declared_expectation_mismatch =
            expected_error_logs != 0 and actual_error_logs != expected_error_logs;
        const strict_unexpected_error_logs =
            fail_on_error_logs and expected_error_logs == 0 and actual_error_logs != 0;
        if (declared_expectation_mismatch or strict_unexpected_error_logs) {
            fail_count += 1;
            std.debug.print(
                "FAIL (expected {d} error logs, observed {d}) {s}\n",
                .{ expected_error_logs, actual_error_logs, test_fn.name },
            );
        } else {
            if (actual_error_logs == expected_error_logs) {
                matched_error_log_count +|= actual_error_logs;
            }
            switch (outcome) {
                .passed => {
                    ok_count += 1;
                    std.debug.print("OK\n", .{});
                },
                .skipped => {
                    skip_count += 1;
                    std.debug.print("SKIP\n", .{});
                },
                .failed => fail_count += 1,
            }
        }
    }

    std.debug.print(
        "{d} passed; {d} skipped; {d} failed; {d} leaked.\n",
        .{ ok_count, skip_count, fail_count, leak_count },
    );
    const total_error_log_count = log_err_count.load(.acquire);
    const unexpected_error_log_count = total_error_log_count -| matched_error_log_count;
    if (total_error_log_count != 0) {
        std.debug.print(
            "{d} errors were logged ({d} expected, {d} unexpected).\n",
            .{ total_error_log_count, matched_error_log_count, unexpected_error_log_count },
        );
    }
    if (fail_count != 0 or leak_count != 0 or
        (fail_on_error_logs and unexpected_error_log_count != 0))
    {
        std.process.exit(1);
    }
}

pub export fn antfly_test_expect_error_logs(count: usize) callconv(.c) void {
    _ = expected_error_log_count.fetchAdd(count, .monotonic);
}

fn matchesFilter(name: []const u8) bool {
    if (test_filters.len != 0) {
        var included = false;
        for (test_filters) |filter| {
            if (matchesSingleFilter(name, filter)) {
                included = true;
                break;
            }
        }
        if (!included) return false;
    }

    for (skip_test_filters) |filter| {
        if (matchesSingleFilter(name, filter)) return false;
    }
    return true;
}

fn recordMatchingIncludeFilters(name: []const u8, matched_filter_counts: []usize) void {
    for (test_filters, 0..) |filter, filter_index| {
        if (matchesSingleFilter(name, filter)) matched_filter_counts[filter_index] += 1;
    }
}

fn matchesSingleFilter(name: []const u8, filter: []const u8) bool {
    const target = if (std.mem.indexOfScalar(u8, filter, '.') != null)
        name
    else
        declaredTestName(name);
    return std.mem.indexOf(u8, target, filter) != null;
}

fn appendFilter(
    allocator: std.mem.Allocator,
    kind: []const u8,
    filters: *std.ArrayList([]const u8),
    filter: []const u8,
) void {
    if (filter.len == 0) {
        std.debug.panic("missing value for {s}", .{kind});
    }
    filters.append(allocator, filter) catch
        std.debug.panic("out of memory while appending {s}", .{kind});
}

fn declaredTestName(name: []const u8) []const u8 {
    const marker = ".test.";
    if (std.mem.indexOf(u8, name, marker)) |idx| {
        return name[idx + marker.len ..];
    }
    return name;
}

fn getenvBool(comptime name: [:0]const u8) bool {
    if (!builtin.link_libc) return false;
    const value = std.c.getenv(name) orelse return false;
    const span = std.mem.span(value);
    return span.len != 0 and
        !std.mem.eql(u8, span, "0") and
        !std.ascii.eqlIgnoreCase(span, "false") and
        !std.ascii.eqlIgnoreCase(span, "no");
}

pub fn log(
    comptime message_level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    @disableInstrumentation();
    if (@backingInt(message_level) <= @backingInt(std.log.Level.err)) {
        _ = log_err_count.fetchAdd(1, .monotonic);
    }
    std.debug.print("[{s}] ({s}): ", .{
        @tagName(message_level),
        @tagName(scope),
    });
    std.debug.print(format, args);
    std.debug.print("\n", .{});
}
