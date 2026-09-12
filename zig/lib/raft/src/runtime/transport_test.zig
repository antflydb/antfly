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

test "codec transport host bundles heartbeat groups sharing a route and source" {
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

    try std.testing.expectEqual(@as(usize, 1), driver.sent.items.len);
    for (driver.sent.items) |frame| {
        const decoded = try runtime.BinaryCodec.codec().decodeFrame(std.testing.allocator, .{
            .bytes = frame.bytes,
            .media_type = frame.media_type,
        });
        defer runtime.BinaryCodec.codec().freeDecoded(std.testing.allocator, decoded);
        try std.testing.expectEqual(@as(usize, 2), decoded.raft_peer_batch.groups.len);
        try std.testing.expectEqual(@as(core.types.GroupId, 41), decoded.raft_peer_batch.groups[0].group_id);
        try std.testing.expectEqual(@as(core.types.GroupId, 42), decoded.raft_peer_batch.groups[1].group_id);
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

test "codec transport heartbeat retries re-resolve each group and bound retained bytes" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator, .failures_remaining = 1 };
    defer driver.deinit();
    var host = runtime.CodecTransportHost.init(std.testing.allocator, runtime.BinaryCodec.codec(), driver.driver(), .{});
    defer host.deinit();
    for ([_]u64{ 41, 42, 43 }) |id| try host.transport().addPeer(id, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://old" }} });
    var context = "read-index-token".*;
    const messages = [_]core.Message{.{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7, .context = &context }};
    const groups = [_]runtime.transport_iface.GroupMessageBatch{
        .{ .group_id = 41, .messages = &messages }, .{ .group_id = 42, .messages = &messages }, .{ .group_id = 43, .messages = &messages },
    };
    try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = &groups }});
    try std.testing.expectEqual(@as(usize, 3), host.pendingRetryCount());
    try host.transport().upsertPeer(42, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://new" }} });
    try host.transport().removePeer(43, 2);
    try host.transport().advanceRound();
    try std.testing.expectEqual(@as(usize, 2), driver.sent.items.len);
    try std.testing.expectEqualStrings("http://old", driver.sent.items[0].address);
    try std.testing.expectEqualStrings("http://new", driver.sent.items[1].address);
    try std.testing.expectEqual(@as(usize, 0), host.pendingRetryCount());
    try std.testing.expectEqual(@as(usize, 0), host.pending_retry_bytes);
    for (driver.sent.items) |frame| {
        const decoded = try host.codec.decodeFrame(std.testing.allocator, .{ .bytes = frame.bytes, .media_type = frame.media_type });
        defer host.codec.freeDecoded(std.testing.allocator, decoded);
        try std.testing.expectEqualStrings("read-index-token", decoded.raft_peer_batch.groups[0].messages[0].context);
    }
    driver.failures_remaining = 1;
    host.retry_policy.max_pending_bytes = 1;
    try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = groups[0..1] }});
    try std.testing.expectEqual(@as(usize, 0), host.pendingRetryCount());
    try std.testing.expectEqual(@as(usize, 0), host.pending_retry_bytes);
}

test "codec transport heartbeat bundles bound groups and separate source identities" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator };
    defer driver.deinit();
    var host = runtime.CodecTransportHost.init(std.testing.allocator, runtime.BinaryCodec.codec(), driver.driver(), .{});
    defer host.deinit();
    var groups: [300]runtime.transport_iface.GroupMessageBatch = undefined;
    const message = [_]core.Message{.{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7 }};
    const other_source = [_]core.Message{.{ .msg_type = .heartbeat_response, .from = 3, .to = 2, .term = 7 }};
    for (&groups, 0..) |*group, i| {
        group.* = .{ .group_id = i + 1, .messages = if (i == 299) &other_source else &message };
        try host.transport().addPeer(i + 1, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://same" }} });
    }
    try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = &groups }});
    try std.testing.expectEqual(@as(usize, 3), driver.sent.items.len);
    var total: usize = 0;
    for (driver.sent.items) |frame| {
        const decoded = try host.codec.decodeFrame(std.testing.allocator, .{ .bytes = frame.bytes, .media_type = frame.media_type });
        defer host.codec.freeDecoded(std.testing.allocator, decoded);
        const batch = decoded.raft_peer_batch;
        try std.testing.expect(batch.groups.len <= 256);
        for (batch.groups) |group| try std.testing.expectEqual(batch.groups[0].messages[0].from, group.messages[0].from);
        total += batch.groups.len;
    }
    try std.testing.expectEqual(@as(usize, 300), total);
}

test "codec transport bundles separate endpoint metadata and preserve group message order" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator };
    defer driver.deinit();
    var host = runtime.CodecTransportHost.init(std.testing.allocator, runtime.BinaryCodec.codec(), driver.driver(), .{});
    defer host.deinit();
    for ([_]u64{ 41, 42 }) |id| try host.transport().addPeer(id, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://same", .metadata = if (id == 41) "tenant=a" else "tenant=b" }} });
    const heartbeat = [_]core.Message{.{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7 }};
    const vote = [_]core.Message{.{ .msg_type = .request_vote, .from = 1, .to = 2, .term = 8 }};
    try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = &.{
        .{ .group_id = 41, .messages = &heartbeat },
        .{ .group_id = 42, .messages = &heartbeat },
        .{ .group_id = 41, .messages = &vote },
    } }});
    try std.testing.expectEqual(@as(usize, 3), driver.sent.items.len);
    var seen_heartbeat = false;
    for (driver.sent.items) |frame| {
        const decoded = try host.codec.decodeFrame(std.testing.allocator, .{ .bytes = frame.bytes, .media_type = frame.media_type });
        defer host.codec.freeDecoded(std.testing.allocator, decoded);
        try std.testing.expectEqual(@as(usize, 1), decoded.raft_peer_batch.groups.len);
        const group = decoded.raft_peer_batch.groups[0];
        if (group.group_id == 41) {
            if (group.messages[0].msg_type == .request_vote) try std.testing.expect(seen_heartbeat) else seen_heartbeat = true;
        }
    }
}

test "codec transport bounds encoded heartbeat bundles and preserves read contexts" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator };
    defer driver.deinit();
    var host = runtime.CodecTransportHost.init(std.testing.allocator, runtime.BinaryCodec.codec(), driver.driver(), .{});
    defer host.deinit();
    const context = try std.testing.allocator.alloc(u8, 300 * 1024);
    defer std.testing.allocator.free(context);
    @memset(context, 7);
    const heartbeat = [_]core.Message{.{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7, .context = context }};
    var groups: [4]runtime.transport_iface.GroupMessageBatch = undefined;
    for (&groups, 0..) |*group, i| {
        group.* = .{ .group_id = i + 1, .messages = &heartbeat };
        try host.transport().addPeer(i + 1, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://same" }} });
    }
    try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = &groups }});
    try std.testing.expectEqual(@as(usize, 2), driver.sent.items.len);
    var total: usize = 0;
    for (driver.sent.items) |frame| {
        try std.testing.expect(frame.bytes.len <= 1024 * 1024);
        const decoded = try host.codec.decodeFrame(std.testing.allocator, .{ .bytes = frame.bytes, .media_type = frame.media_type });
        defer host.codec.freeDecoded(std.testing.allocator, decoded);
        for (decoded.raft_peer_batch.groups) |group| try std.testing.expectEqualSlices(u8, context, group.messages[0].context);
        total += decoded.raft_peer_batch.groups.len;
    }
    try std.testing.expectEqual(@as(usize, 4), total);
}

test "codec transport asynchronous failures re-resolve routes and exhaust actual attempts" {
    const AsyncDriver = struct {
        alloc: std.mem.Allocator,
        failed: std.ArrayListUnmanaged(runtime.frame_driver_iface.FailedFrame) = .empty,
        calls: usize = 0,
        fn send(ptr: *anyopaque, req: runtime.frame_driver_iface.SendFrameRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (req.attempt == 1) try std.testing.expectEqualStrings("http://old", req.endpoint.address) else {
                try std.testing.expectEqualStrings("http://new", req.endpoint.address);
                try std.testing.expectEqualSlices(u64, &.{42}, req.group_ids);
            }
            try std.testing.expectEqual(@as(?u64, 1), req.source_id);
            const decoded = try runtime.BinaryCodec.codec().decodeFrame(self.alloc, req.frame);
            defer runtime.BinaryCodec.codec().freeDecoded(self.alloc, decoded);
            for (decoded.raft_peer_batch.groups) |group| try std.testing.expectEqualStrings("read-token", group.messages[0].context);
            const bytes = try self.alloc.dupe(u8, req.frame.bytes);
            errdefer self.alloc.free(bytes);
            const media_type = try self.alloc.dupe(u8, req.frame.media_type);
            errdefer self.alloc.free(media_type);
            try self.failed.append(self.alloc, .{ .alloc = self.alloc, .source_id = req.source_id, .peer_id = req.peer_id, .frame = .{ .bytes = bytes, .media_type = media_type }, .attempt = req.attempt });
        }
        fn poll(ptr: *anyopaque) ?runtime.frame_driver_iface.FailedFrame {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return if (self.failed.items.len == 0) null else self.failed.orderedRemove(0);
        }
    };
    const alloc = std.testing.allocator;
    var driver: AsyncDriver = .{ .alloc = alloc };
    defer {
        for (driver.failed.items) |*failed| failed.deinit();
        driver.failed.deinit(alloc);
    }
    var host = runtime.CodecTransportHost.init(alloc, runtime.BinaryCodec.codec(), .{ .ptr = &driver, .vtable = &.{ .send_frame = AsyncDriver.send, .poll_failed_frame = AsyncDriver.poll } }, .{ .max_attempts = 3 });
    defer host.deinit();
    for ([_]u64{ 41, 42 }) |id| try host.transport().addPeer(id, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://old" }} });
    var context = "read-token".*;
    const messages = [_]core.Message{.{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7, .context = &context }};
    try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = &.{ .{ .group_id = 41, .messages = &messages }, .{ .group_id = 42, .messages = &messages } } }});
    try host.transport().removePeer(41, 2);
    try host.transport().upsertPeer(42, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://new" }} });
    for (0..16) |_| try host.transport().advanceRound();
    try std.testing.expectEqual(@as(usize, 3), driver.calls);
    try std.testing.expectEqual(@as(usize, 0), host.pending_retry_bytes);
    try std.testing.expectEqual(@as(usize, 0), host.pendingRetryCount());
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().retries_exhausted);
}

test "codec transport retry compaction preserves delayed and failed survivor order" {
    var driver = RecordingFrameDriver{ .alloc = std.testing.allocator, .failures_remaining = 1 };
    defer driver.deinit();
    var host = runtime.CodecTransportHost.init(std.testing.allocator, runtime.BinaryCodec.codec(), driver.driver(), .{});
    defer host.deinit();
    const message = [_]core.Message{.{ .msg_type = .heartbeat, .from = 1, .to = 2, .term = 7 }};
    var groups: [4]runtime.transport_iface.GroupMessageBatch = undefined;
    for (&groups, 41..) |*group, id| {
        group.* = .{ .group_id = id, .messages = &message };
        try host.transport().addPeer(id, .{ .node_id = 2, .endpoints = &.{.{ .protocol = .http1, .address = "http://peer" }} });
    }
    try host.transport().sendPeerBatches(&.{.{ .peer_id = 2, .groups = &groups }});
    host.pending_retries.items[1].retry_round = 10;
    host.pending_retries.items[3].retry_round = 10;
    driver.failures_remaining = 1;
    try host.transport().advanceRound();
    try std.testing.expectEqual(@as(usize, 3), host.pendingRetryCount());
    for (host.pending_retries.items, [_]u64{ 41, 42, 44 }) |pending, id| try std.testing.expectEqual(id, pending.group_id);
    var bytes: usize = 0;
    for (host.pending_retries.items) |pending| bytes += pending.frame.bytes.len;
    try std.testing.expectEqual(bytes, host.pending_retry_bytes);
    host.current_round = 10;
    try host.transport().advanceRound();
    try std.testing.expectEqual(@as(usize, 0), host.pendingRetryCount());
    try std.testing.expectEqual(@as(usize, 0), host.pending_retry_bytes);
    try std.testing.expectEqual(@as(usize, 4), driver.sent.items.len);
}
