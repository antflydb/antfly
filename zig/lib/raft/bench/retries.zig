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

// Node reconnect: drain the production bounded retry queue after an outage.
const std = @import("std");
const raft = @import("raft");
const Driver = struct {
    fail: bool = true,
    sent: usize = 0,
    fn send(ptr: *anyopaque, _: raft.runtime.frame_driver_iface.SendFrameRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.fail) return error.Disconnected;
        self.sent += 1;
    }
    fn iface(self: *@This()) raft.runtime.FrameDriver {
        return .{ .ptr = self, .vtable = &.{ .send_frame = send } };
    }
};
pub fn main() !void {
    const alloc = std.heap.c_allocator;
    var io_runtime = std.Io.Threaded.init(alloc, .{});
    defer io_runtime.deinit();
    for ([_]usize{ 100, 1000, 4096 }) |count| {
        const groups = try alloc.alloc(raft.runtime.transport_iface.GroupMessageBatch, count);
        defer alloc.free(groups);
        const messages = try alloc.alloc(raft.core.Message, count);
        defer alloc.free(messages);
        for (groups, messages, 0..) |*group, *message, i| {
            message.* = .{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7 };
            group.* = .{ .group_id = i + 100, .messages = messages[i..][0..1] };
        }
        var driver: Driver = .{};
        var host = raft.runtime.CodecTransportHost.init(alloc, raft.runtime.BinaryCodec.codec(), driver.iface(), .{});
        defer host.deinit();
        for (groups) |group| try host.transport().addPeer(group.group_id, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://node-2" }} });
        var samples: [9]i96 = undefined;
        for (&samples) |*elapsed| {
            driver = .{};
            try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = groups }});
            if (host.pending_retries.items.len != count) return error.WrongPendingCount;
            driver.fail = false;
            const start = std.Io.Clock.awake.now(io_runtime.io()).nanoseconds;
            try host.transport().advanceRound();
            elapsed.* = std.Io.Clock.awake.now(io_runtime.io()).nanoseconds - start;
            if (driver.sent != count or host.pending_retries.items.len != 0) return error.WrongSentCount;
        }
        std.mem.sort(i96, &samples, {}, std.sort.asc(i96));
        std.debug.print("RETRY_DRAIN groups={d} p50_ms={d:.3} frames={d}\n", .{ count, @as(f64, @floatFromInt(samples[4])) / 1e6, driver.sent });
    }
}
