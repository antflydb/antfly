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
const trace = @import("testing/trace.zig");
const core = @import("core/mod.zig");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const alloc = init.arena.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    _ = args.next();
    const seed_arg = args.next() orelse {
        std.log.err("usage: emit_seeded_trace <seed> <steps> [check_quorum] [pre_vote] [profile]", .{});
        return error.InvalidArguments;
    };
    const steps_arg = args.next() orelse {
        std.log.err("usage: emit_seeded_trace <seed> <steps> [check_quorum] [pre_vote] [profile]", .{});
        return error.InvalidArguments;
    };
    const check_quorum_arg = args.next();
    const pre_vote_arg = args.next();
    const profile_arg = args.next();
    const async_arg = args.next();
    const read_only_arg = args.next();
    if (args.next() != null) {
        std.log.err("usage: emit_seeded_trace <seed> <steps> [check_quorum] [pre_vote] [profile] [async_storage_writes] [read_only_option]", .{});
        return error.InvalidArguments;
    }

    const seed = try std.fmt.parseInt(u64, seed_arg, 10);
    const steps = try std.fmt.parseInt(usize, steps_arg, 10);
    const check_quorum = if (check_quorum_arg) |arg| std.mem.eql(u8, arg, "true") else true;
    const pre_vote = if (pre_vote_arg) |arg| std.mem.eql(u8, arg, "true") else true;
    const profile: trace.SeededTraceOptions.Profile = if (profile_arg) |arg|
        if (std.mem.eql(u8, arg, "stress")) .stress else .stable
    else
        .stable;
    const async_storage_writes = if (async_arg) |arg| std.mem.eql(u8, arg, "true") else false;
    const read_only_option: core.ReadOnlyOption = if (read_only_arg) |arg|
        if (std.mem.eql(u8, arg, "lease_based")) .lease_based else .safe
    else
        .safe;

    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    const stdout = &stdout_writer.interface;

    var recorder = try trace.recordSeededDifferentialTrace(alloc, .{
        .seed = seed,
        .steps = steps,
        .check_quorum = check_quorum,
        .pre_vote = pre_vote,
        .async_storage_writes = async_storage_writes,
        .read_only_option = read_only_option,
        .profile = profile,
    });
    defer recorder.deinit();

    try recorder.writeJson(stdout);
    try stdout.flush();
}
