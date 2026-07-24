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

const std = @import("std");
const raft_engine = @import("raft_engine");
const runtime_loop = @import("raft/runtime_loop.zig");
const hosted_shard_ops = @import("raft/hosted_shard_ops.zig");
const transition_service = @import("raft/transition_service.zig");

test "raft scheduler ready priority cannot starve consensus ticks" {
    var scheduler = raft_engine.runtime.scheduler.Scheduler.init(std.testing.allocator, .{
        .max_tick_batch = 2,
        .priority_boost = 8,
    });
    defer scheduler.deinit();

    try scheduler.registerGroup(1);
    try scheduler.registerGroup(2);
    try scheduler.registerGroup(3);

    for (0..8) |_| scheduler.noteActivity(1);
    const first = try scheduler.tickBatch(std.testing.allocator);
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2 }, first);

    const second = try scheduler.tickBatch(std.testing.allocator);
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualSlices(u64, &.{ 3, 1 }, second);

    try std.testing.expectEqual(@as(?u64, 1), scheduler.nextReadyGroup());
    try std.testing.expectEqual(@as(?u64, 1), scheduler.nextReadyGroup());

    var reused = raft_engine.runtime.scheduler.Scheduler.init(std.testing.allocator, .{
        .max_tick_batch = 8,
        .priority_boost = 8,
    });
    defer reused.deinit();
    try reused.registerGroup(1);
    try reused.registerGroup(2);
    reused.noteReady(1);
    try std.testing.expect(reused.unregisterGroup(1));
    try reused.registerGroup(1);
    try std.testing.expectEqual(@as(?u64, 2), reused.nextReadyGroup());

    const Register = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var candidate = raft_engine.runtime.scheduler.Scheduler.init(alloc, .{});
            defer candidate.deinit();
            try candidate.registerGroup(17);
            try std.testing.expectError(error.GroupAlreadyRegistered, candidate.registerGroup(17));
            try std.testing.expect(candidate.unregisterGroup(17));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Register.run, .{});
}

test {
    _ = runtime_loop;
    _ = hosted_shard_ops;
    std.testing.refAllDecls(transition_service);
}
