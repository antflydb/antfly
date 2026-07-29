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

pub fn select(
    alloc: std.mem.Allocator,
    args: []const []const u8,
    default_filters: []const []const u8,
) []const []const u8 {
    if (args.len == 0) return default_filters;

    var filters = std.ArrayListUnmanaged([]const u8).empty;
    filters.ensureTotalCapacity(alloc, args.len) catch @panic("OOM");

    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--test-filter")) {
            i += 1;
            if (i >= args.len or args[i].len == 0)
                @panic("missing value after --test-filter");
            filters.appendAssumeCapacity(args[i]);
        } else if (std.mem.startsWith(u8, arg, "--test-filter=")) {
            const filter = arg["--test-filter=".len..];
            if (filter.len == 0) @panic("missing value after --test-filter=");
            filters.appendAssumeCapacity(filter);
        } else if (std.mem.eql(u8, arg, "--skip-test-filter")) {
            i += 1;
            if (i >= args.len or args[i].len == 0)
                @panic("missing value after --skip-test-filter");
        } else if (std.mem.startsWith(u8, arg, "--skip-test-filter=") or
            std.mem.startsWith(u8, arg, "--seed=") or
            std.mem.startsWith(u8, arg, "--cache-dir=") or
            std.mem.eql(u8, arg, "--listen=-"))
        {
            // Runtime-only controls do not participate in compile reachability.
        } else if (std.mem.startsWith(u8, arg, "--")) {
            @panic("unsupported test runner argument");
        } else {
            filters.appendAssumeCapacity(arg);
        }
        i += 1;
    }

    if (filters.items.len == 0) {
        filters.deinit(alloc);
        return default_filters;
    }
    return filters.toOwnedSlice(alloc) catch @panic("OOM");
}

pub fn addRuntimeControls(
    run: *std.Build.Step.Run,
    args: []const []const u8,
) void {
    var i: usize = 0;
    while (i < args.len) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--test-filter")) {
            i += 1;
            if (i >= args.len or args[i].len == 0)
                @panic("missing value after --test-filter");
        } else if (std.mem.startsWith(u8, arg, "--test-filter=")) {
            if (arg.len == "--test-filter=".len)
                @panic("missing value after --test-filter=");
        } else if (std.mem.eql(u8, arg, "--skip-test-filter")) {
            i += 1;
            if (i >= args.len or args[i].len == 0)
                @panic("missing value after --skip-test-filter");
            run.addArgs(&.{ "--skip-test-filter", args[i] });
        } else if (std.mem.startsWith(u8, arg, "--skip-test-filter=") or
            std.mem.startsWith(u8, arg, "--seed=") or
            std.mem.startsWith(u8, arg, "--cache-dir=") or
            std.mem.eql(u8, arg, "--listen=-"))
        {
            run.addArg(arg);
        } else if (std.mem.startsWith(u8, arg, "--")) {
            @panic("unsupported test runner argument");
        }
        i += 1;
    }
}

test "select accepts repeated filters and ignores runtime controls" {
    const filters = select(
        std.testing.allocator,
        &.{
            "--test-filter",
            "metrics render",
            "--test-filter=singleflight basic",
            "--skip-test-filter",
            "metrics render",
            "--seed=0x1234",
        },
        &.{"default"},
    );
    defer std.testing.allocator.free(filters);

    try std.testing.expectEqual(@as(usize, 2), filters.len);
    try std.testing.expectEqualStrings("metrics render", filters[0]);
    try std.testing.expectEqualStrings("singleflight basic", filters[1]);
}
