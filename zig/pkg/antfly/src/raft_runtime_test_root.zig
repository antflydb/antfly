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

    var ready_pass = scheduler.beginReadyPass(.fair);
    try std.testing.expectEqual(@as(?u64, 1), scheduler.nextReadyGroup(&ready_pass));
    scheduler.completeReady(1, true);
    try std.testing.expectEqual(@as(?u64, 2), scheduler.nextReadyGroup(&ready_pass));
    try std.testing.expectEqual(@as(?u64, 3), scheduler.nextReadyGroup(&ready_pass));
    try std.testing.expectEqual(@as(?u64, null), scheduler.nextReadyGroup(&ready_pass));
    scheduler.finishReadyPass(&ready_pass);

    var requeued_pass = scheduler.beginReadyPass(.continuation);
    defer scheduler.finishReadyPass(&requeued_pass);
    try std.testing.expectEqual(@as(?u64, 1), scheduler.nextReadyGroup(&requeued_pass));

    var reused = raft_engine.runtime.scheduler.Scheduler.init(std.testing.allocator, .{
        .max_tick_batch = 8,
    });
    defer reused.deinit();
    try reused.registerGroup(1);
    try reused.registerGroup(2);
    reused.noteReady(1);
    try std.testing.expect(reused.unregisterGroup(1));
    try reused.registerGroup(1);
    var reused_pass = reused.beginReadyPass(.fair);
    defer reused.finishReadyPass(&reused_pass);
    try std.testing.expectEqual(@as(?u64, 2), reused.nextReadyGroup(&reused_pass));

    {
        var bounded = raft_engine.runtime.scheduler.Scheduler.init(std.testing.allocator, .{});
        defer bounded.deinit();
        const group_count = 32;
        for (0..group_count) |index| {
            const group_id: u64 = @intCast(index + 1);
            try bounded.registerGroup(group_id);
            bounded.completeReady(group_id, true);
        }

        // Requeue every group while consuming the original snapshot. Newly
        // produced continuation hints must remain behind that snapshot.
        var full_pass = bounded.beginReadyPass(.continuation);
        for (0..group_count) |index| {
            const group_id: u64 = @intCast(index + 1);
            try std.testing.expectEqual(@as(?u64, group_id), bounded.nextReadyGroup(&full_pass));
            bounded.completeReady(group_id, true);
        }
        try std.testing.expectEqual(@as(?u64, null), bounded.nextReadyGroup(&full_pass));
        bounded.finishReadyPass(&full_pass);
        try std.testing.expect(bounded.hasQueuedContinuation());

        // Stop partway through the next frontier and verify that unconsumed
        // and newly requeued hints retain deterministic FIFO order.
        const partial_count = 7;
        var partial_pass = bounded.beginReadyPass(.continuation);
        for (0..partial_count) |index| {
            const group_id: u64 = @intCast(index + 1);
            try std.testing.expectEqual(@as(?u64, group_id), bounded.nextReadyGroup(&partial_pass));
            bounded.completeReady(group_id, true);
        }
        bounded.finishReadyPass(&partial_pass);

        var compacted_pass = bounded.beginReadyPass(.continuation);
        for (partial_count..group_count) |index| {
            const group_id: u64 = @intCast(index + 1);
            try std.testing.expectEqual(@as(?u64, group_id), bounded.nextReadyGroup(&compacted_pass));
        }
        for (0..partial_count) |index| {
            const group_id: u64 = @intCast(index + 1);
            try std.testing.expectEqual(@as(?u64, group_id), bounded.nextReadyGroup(&compacted_pass));
        }
        try std.testing.expectEqual(@as(?u64, null), bounded.nextReadyGroup(&compacted_pass));
        bounded.finishReadyPass(&compacted_pass);
        try std.testing.expect(!bounded.hasQueuedContinuation());
    }

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
