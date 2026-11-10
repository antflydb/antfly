// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.next();
    const command = args.next() orelse return error.ExpectedHbcRead;
    if (!std.mem.eql(u8, command, "hbc-read")) return error.ExpectedHbcRead;
    try @import("hbc_read_bench.zig").run(init, &args);
}
