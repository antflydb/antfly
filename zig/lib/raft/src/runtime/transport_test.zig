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
const core = @import("../core/mod.zig");
const runtime = @import("mod.zig");

const RecordedFrame = struct {
    peer_id: core.types.NodeId,
    address: []u8,
    media_type: []u8,
    bytes: []u8,

    fn deinit(self: *RecordedFrame, alloc: std.mem.Allocator) void {
        alloc.free(self.address);
        alloc.free(self.media_type);
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

const RecordingFrameDriver = struct {
    alloc: std.mem.Allocator,
    failures_remaining: usize = 0,
    sent: std.ArrayListUnmanaged(RecordedFrame) = .empty,

    fn deinit(self: *RecordingFrameDriver) void {
        for (self.sent.items) |*frame| frame.deinit(self.alloc);
        self.sent.deinit(self.alloc);
        self.* = undefined;
    }

    fn driver(self: *RecordingFrameDriver) runtime.FrameDriver {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_frame = sendFrame,
            },
        };
    }

    fn sendFrame(ptr: *anyopaque, req: runtime.frame_driver_iface.SendFrameRequest) !void {
        const self: *RecordingFrameDriver = @ptrCast(@alignCast(ptr));
        if (self.failures_remaining > 0) {
            self.failures_remaining -= 1;
            return error.TransportUnavailable;
        }
        try self.sent.append(self.alloc, .{
            .peer_id = req.peer_id,
            .address = try self.alloc.dupe(u8, req.endpoint.address),
            .media_type = try self.alloc.dupe(u8, req.frame.media_type),
            .bytes = try self.alloc.dupe(u8, req.frame.bytes),
        });
    }
};

const Receiver = struct {
    seen: usize = 0,
    last_group_id: core.types.GroupId = 0,

    fn iface(self: *Receiver) runtime.transport_iface.TransportReceiver {
        return .{
            .ptr = self,
            .vtable = &.{
                .handle_message = handleMessage,
            },
        };
    }

    fn handleMessage(ptr: *anyopaque, group_id: core.types.GroupId, msg: core.Message) !void {
        _ = msg;
        const self: *Receiver = @ptrCast(@alignCast(ptr));
        self.seen += 1;
        self.last_group_id = group_id;
    }
};

test "codec transport host sends, decodes, and delivers peer batches" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator };
    defer driver.deinit();

    var host = runtime.CodecTransportHost.init(
        std.testing.allocator,
        runtime.BinaryCodec.codec(),
        driver.driver(),
        .{},
    );
    defer host.deinit();

    var receiver = Receiver{};
    try host.transport().serveGroup(11, receiver.iface());
    try host.transport().addPeer(11, .{
        .node_id = 2,
        .endpoints = &.{.{ .protocol = .http3, .address = "https://n2", .metadata = "az=a" }},
    });

    const msg = core.Message{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = 4,
    };
    try host.transport().sendMessages(11, (&[_]core.Message{msg})[0..]);

    try std.testing.expectEqual(@as(usize, 1), driver.sent.items.len);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().sent_frames);
    try std.testing.expectEqualStrings("https://n2", driver.sent.items[0].address);

    try host.receiveFrame(.{
        .bytes = driver.sent.items[0].bytes,
        .media_type = driver.sent.items[0].media_type,
    });
    try std.testing.expectEqual(@as(usize, 1), receiver.seen);
    try std.testing.expectEqual(@as(core.types.GroupId, 11), receiver.last_group_id);
}

test "codec transport host rejects frames for unserved groups so senders retry" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator };
    defer driver.deinit();

    var sender = runtime.CodecTransportHost.init(
        std.testing.allocator,
        runtime.BinaryCodec.codec(),
        driver.driver(),
        .{},
    );
    defer sender.deinit();
    var receiver = runtime.CodecTransportHost.init(
        std.testing.allocator,
        runtime.BinaryCodec.codec(),
        driver.driver(),
        .{},
    );
    defer receiver.deinit();

    try sender.transport().addPeer(12, .{
        .node_id = 2,
        .endpoints = &.{.{ .protocol = .http3, .address = "https://n2", .metadata = "" }},
    });

    const msg = core.Message{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = 4,
    };
    try sender.transport().sendMessages(12, (&[_]core.Message{msg})[0..]);

    try std.testing.expectEqual(@as(usize, 1), driver.sent.items.len);
    try std.testing.expectError(error.UnknownGroup, receiver.receiveFrame(.{
        .bytes = driver.sent.items[0].bytes,
        .media_type = driver.sent.items[0].media_type,
    }));
}

test "codec transport host sends multi-group peer batches as isolated frames" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator };
    defer driver.deinit();

    var host = runtime.CodecTransportHost.init(
        std.testing.allocator,
        runtime.BinaryCodec.codec(),
        driver.driver(),
        .{},
    );
    defer host.deinit();

    try host.transport().addPeer(41, .{
        .node_id = 2,
        .endpoints = &.{.{ .protocol = .http3, .address = "https://n2", .metadata = "" }},
    });
    try host.transport().addPeer(42, .{
        .node_id = 2,
        .endpoints = &.{.{ .protocol = .http3, .address = "https://n2", .metadata = "" }},
    });

    const msg_a = core.Message{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7 };
    const msg_b = core.Message{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7 };
    const group_a = runtime.transport_iface.GroupMessageBatch{
        .group_id = 41,
        .messages = (&[_]core.Message{msg_a})[0..],
    };
    const group_b = runtime.transport_iface.GroupMessageBatch{
        .group_id = 42,
        .messages = (&[_]core.Message{msg_b})[0..],
    };
    const batch = runtime.transport_iface.PeerBatch{
        .peer_id = 2,
        .groups = (&[_]runtime.transport_iface.GroupMessageBatch{ group_a, group_b })[0..],
    };
    try host.transport().sendPeerBatches((&[_]runtime.transport_iface.PeerBatch{batch})[0..]);

    try std.testing.expectEqual(@as(usize, 2), driver.sent.items.len);
    for (driver.sent.items, 0..) |frame, i| {
        const decoded = try runtime.BinaryCodec.codec().decodeFrame(std.testing.allocator, .{
            .bytes = frame.bytes,
            .media_type = frame.media_type,
        });
        defer runtime.BinaryCodec.codec().freeDecoded(std.testing.allocator, decoded);
        try std.testing.expectEqual(@as(usize, 1), decoded.raft_peer_batch.groups.len);
        try std.testing.expectEqual(@as(core.types.GroupId, if (i == 0) 41 else 42), decoded.raft_peer_batch.groups[0].group_id);
    }
}

test "codec transport host retries failed sends and refreshes peer endpoints" {
    var driver = RecordingFrameDriver{
        .alloc = std.testing.allocator,
        .failures_remaining = 1,
    };
    defer driver.deinit();

    var host = runtime.CodecTransportHost.init(
        std.testing.allocator,
        runtime.BinaryCodec.codec(),
        driver.driver(),
        .{
            .initial_backoff_rounds = 1,
            .max_backoff_rounds = 2,
            .max_attempts = 3,
        },
    );
    defer host.deinit();

    try host.transport().addPeer(21, .{
        .node_id = 2,
        .endpoints = &.{.{ .protocol = .http3, .address = "https://old", .metadata = "" }},
    });

    const msg = core.Message{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = 5,
    };
    try host.transport().sendMessages(21, (&[_]core.Message{msg})[0..]);
    try std.testing.expectEqual(@as(usize, 0), driver.sent.items.len);
    try std.testing.expectEqual(@as(usize, 1), host.pendingRetryCount());
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().send_failures);

    try host.transport().advanceRound();
    try std.testing.expectEqual(@as(usize, 1), driver.sent.items.len);
    try std.testing.expectEqualStrings("https://old", driver.sent.items[0].address);
    try std.testing.expectEqual(@as(usize, 0), host.pendingRetryCount());
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().retried_successes);

    try host.transport().upsertPeer(21, .{
        .node_id = 2,
        .endpoints = &.{.{ .protocol = .http3, .address = "https://new", .metadata = "v=2" }},
    });
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().peer_refreshes);

    try host.transport().sendMessages(21, (&[_]core.Message{msg})[0..]);
    try std.testing.expectEqual(@as(usize, 2), driver.sent.items.len);
    try std.testing.expectEqualStrings("https://new", driver.sent.items[1].address);
}

test "codec transport host treats missing peer route as non-fatal send failure" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator };
    defer driver.deinit();

    var host = runtime.CodecTransportHost.init(
        std.testing.allocator,
        runtime.BinaryCodec.codec(),
        driver.driver(),
        .{},
    );
    defer host.deinit();

    const msg = core.Message{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = 6,
    };
    try host.transport().sendMessages(31, (&[_]core.Message{msg})[0..]);

    try std.testing.expectEqual(@as(usize, 0), driver.sent.items.len);
    try std.testing.expectEqual(@as(usize, 0), host.pendingRetryCount());
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().send_failures);
}
