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

// Idle tenant/range workloads: codec framing and the production transport
// routing/encoding path. The counting driver excludes HTTP/network latency.
const std = @import("std");
const raft = @import("raft");
const CountingDriver = struct {
    frames: usize = 0,
    bytes: usize = 0,
    fn send(ptr: *anyopaque, request: raft.runtime.frame_driver_iface.SendFrameRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.frames += 1;
        self.bytes += request.frame.bytes.len;
    }
    fn iface(self: *@This()) raft.runtime.FrameDriver {
        return .{ .ptr = self, .vtable = &.{ .send_frame = send } };
    }
};

pub fn main() !void {
    const alloc = std.heap.page_allocator;
    var io_runtime = std.Io.Threaded.init(alloc, .{});
    defer io_runtime.deinit();
    const io = io_runtime.io();
    const codec = raft.runtime.BinaryCodec.codec();
    for ([_]usize{ 100, 1000, 10000 }) |count| {
        const groups = try alloc.alloc(raft.runtime.transport_iface.GroupMessageBatch, count);
        defer alloc.free(groups);
        const messages = try alloc.alloc(raft.core.Message, count);
        defer alloc.free(messages);
        for (groups, messages, 0..) |*group, *message, i| {
            message.* = .{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7, .commit_index = i + 10 };
            group.* = .{ .group_id = i + 100, .messages = messages[i..][0..1] };
        }
        {
            var driver: CountingDriver = .{};
            var host = raft.runtime.CodecTransportHost.init(alloc, codec, driver.iface(), .{});
            defer host.deinit();
            for (groups) |group| try host.transport().addPeer(group.group_id, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://node-2" }} });
            var elapsed: [7]i96 = undefined;
            for (&elapsed) |*sample| {
                driver = .{};
                const start = std.Io.Clock.awake.now(io).nanoseconds;
                try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = groups }});
                sample.* = std.Io.Clock.awake.now(io).nanoseconds - start;
            }
            std.mem.sort(i96, &elapsed, {}, std.sort.asc(i96));
            std.debug.print("HEARTBEAT_HOST_BENCH groups={d} frames={d} encoded_bytes={d} send_p50_ms={d:.3}\n", .{ count, driver.frames, driver.bytes, @as(f64, @floatFromInt(elapsed[3])) / 1e6 });
        }
        for ([_]usize{ 1, 64, 256 }) |cap| {
            var elapsed: [7]i96 = undefined;
            var bytes: usize = 0;
            var frames: usize = 0;
            for (&elapsed) |*sample| {
                bytes = 0;
                frames = 0;
                const start = std.Io.Clock.awake.now(io).nanoseconds;
                var offset: usize = 0;
                while (offset < count) : (offset += cap) {
                    const frame = try codec.encodePeerBatch(alloc, .{ .peer_id = 2, .groups = groups[offset..@min(count, offset + cap)] });
                    bytes += frame.bytes.len;
                    frames += 1;
                    codec.freeFrame(alloc, frame);
                }
                sample.* = std.Io.Clock.awake.now(io).nanoseconds - start;
            }
            // Decode separately, outside timing, to ensure group-specific term
            // and commit information survives the unchanged wire format.
            const frame = try codec.encodePeerBatch(alloc, .{ .peer_id = 2, .groups = groups[0..@min(count, cap)] });
            defer codec.freeFrame(alloc, frame);
            const decoded = try codec.decodeFrame(alloc, frame);
            defer codec.freeDecoded(alloc, decoded);
            for (decoded.raft_peer_batch.groups, 0..) |group, i| {
                if (group.group_id != i + 100 or group.messages[0].commit_index != i + 10 or group.messages[0].term != 7) return error.HeartbeatChanged;
            }
            std.mem.sort(i96, &elapsed, {}, std.sort.asc(i96));
            std.debug.print("HEARTBEAT_BENCH groups={d} max_groups_per_frame={d} frames={d} encoded_bytes={d} encode_p50_ms={d:.3}\n", .{ count, cap, frames, bytes, @as(f64, @floatFromInt(elapsed[3])) / 1e6 });
        }
    }
}
