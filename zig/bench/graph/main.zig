// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const command = args.next() orelse return usage();
    if (std.mem.eql(u8, command, "prepare")) return @import("metric_preparation_bench.zig").run(init, &args);
    if (std.mem.eql(u8, command, "pattern")) return @import("pattern_query_bench.zig").run(init, &args);
    return usage();
}

fn usage() void {
    std.debug.print("usage: antfly-graph-bench <prepare|pattern> [options]\n", .{});
    std.process.exit(2);
}
