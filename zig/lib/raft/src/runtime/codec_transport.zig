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
const codec_iface = @import("codec_iface.zig");
const frame_driver_iface = @import("frame_driver_iface.zig");
const transport_iface = @import("transport_iface.zig");

pub const RetryPolicy = struct {
    initial_backoff_rounds: u32 = 1,
    max_backoff_rounds: u32 = 8,
    max_attempts: u32 = 4,
};

pub const Metrics = struct {
    sent_frames: usize = 0,
    send_failures: usize = 0,
    retries_scheduled: usize = 0,
    retries_exhausted: usize = 0,
    retried_successes: usize = 0,
    peer_refreshes: usize = 0,
};

const PeerRouteKey = struct {
    group_id: core.types.GroupId,
    node_id: core.types.NodeId,
};

const OwnedEndpoint = struct {
    protocol: transport_iface.TransportProtocol,
    address: []u8,
    metadata: []u8,

    fn clone(alloc: std.mem.Allocator, peer_endpoint: transport_iface.PeerEndpoint) !OwnedEndpoint {
        return .{
            .protocol = peer_endpoint.protocol,
            .address = try alloc.dupe(u8, peer_endpoint.address),
            .metadata = try alloc.dupe(u8, peer_endpoint.metadata),
        };
    }

    fn deinit(self: *OwnedEndpoint, alloc: std.mem.Allocator) void {
        alloc.free(self.address);
        alloc.free(self.metadata);
        self.* = undefined;
    }

    fn endpoint(self: OwnedEndpoint) transport_iface.PeerEndpoint {
        return .{
            .protocol = self.protocol,
            .address = self.address,
            .metadata = self.metadata,
        };
    }

    fn eql(self: OwnedEndpoint, peer_endpoint: transport_iface.PeerEndpoint) bool {
        return self.protocol == peer_endpoint.protocol and
            std.mem.eql(u8, self.address, peer_endpoint.address) and
            std.mem.eql(u8, self.metadata, peer_endpoint.metadata);
    }
};

const PendingRetry = struct {
    group_id: core.types.GroupId,
    source_id: ?core.types.NodeId,
    peer_id: core.types.NodeId,
    frame: codec_iface.EncodedFrame,
    attempts: u32,
    retry_round: u64,

    fn deinit(self: *PendingRetry, alloc: std.mem.Allocator) void {
        alloc.free(self.frame.bytes);
        self.* = undefined;
    }
};

pub const CodecTransportHost = struct {
    alloc: std.mem.Allocator,
    codec: codec_iface.MessageCodec,
    driver: frame_driver_iface.FrameDriver,
    retry_policy: RetryPolicy,
    current_round: u64 = 0,
    current_time_ms: u64 = 0,
    served_groups: std.AutoHashMapUnmanaged(core.types.GroupId, transport_iface.TransportReceiver) = .empty,
    peer_routes: std.AutoHashMapUnmanaged(PeerRouteKey, OwnedEndpoint) = .empty,
    pending_retries: std.ArrayListUnmanaged(PendingRetry) = .empty,
    metrics: Metrics = .{},

    pub fn init(
        alloc: std.mem.Allocator,
        codec: codec_iface.MessageCodec,
        driver: frame_driver_iface.FrameDriver,
        retry_policy: RetryPolicy,
    ) CodecTransportHost {
        return .{
            .alloc = alloc,
            .codec = codec,
            .driver = driver,
            .retry_policy = retry_policy,
        };
    }

    pub fn deinit(self: *CodecTransportHost) void {
        self.served_groups.deinit(self.alloc);
        var route_it = self.peer_routes.valueIterator();
        while (route_it.next()) |endpoint| endpoint.deinit(self.alloc);
        self.peer_routes.deinit(self.alloc);
        for (self.pending_retries.items) |*pending| pending.deinit(self.alloc);
        self.pending_retries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn transport(self: *CodecTransportHost) transport_iface.Transport {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_messages = sendMessages,
                .send_peer_batches = sendPeerBatches,
                .serve_group = serveGroup,
                .unserve_group = unserveGroup,
                .add_peer = addPeer,
                .upsert_peer = upsertPeer,
                .remove_peer = removePeer,
                .advance_time_ms = advanceTimeMs,
                .advance_round = advanceRound,
            },
        };
    }

    pub fn metricsSnapshot(self: *const CodecTransportHost) Metrics {
        return self.metrics;
    }

    pub fn pendingRetryCount(self: *const CodecTransportHost) usize {
        return self.pending_retries.items.len;
    }

    pub fn receiveFrame(self: *CodecTransportHost, frame: codec_iface.EncodedFrame) !void {
        const decoded = try self.codec.decodeFrame(self.alloc, frame);
        defer self.codec.freeDecoded(self.alloc, decoded);

        switch (decoded) {
            .raft_peer_batch => |batch| {
                var missing_group = false;
                for (batch.groups) |group_batch| {
                    const receiver = self.served_groups.get(group_batch.group_id) orelse {
                        missing_group = true;
                        continue;
                    };
                    for (group_batch.messages) |msg| {
                        try receiver.handleMessage(group_batch.group_id, msg);
                    }
                }
                if (missing_group) return error.UnknownGroup;
            },
            .snapshot_manifest => {},
        }
    }

    fn sendMessages(ptr: *anyopaque, group_id: core.types.GroupId, messages: []const core.Message) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        if (messages.len == 0) return;

        var group_batch = transport_iface.GroupMessageBatch{
            .group_id = group_id,
            .messages = messages,
        };
        const peer_id = messages[0].to;
        try self.sendBatch(.{
            .peer_id = peer_id,
            .groups = (&group_batch)[0..1],
        });
    }

    fn sendPeerBatches(ptr: *anyopaque, batches: []const transport_iface.PeerBatch) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        for (batches) |batch| {
            if (batch.groups.len <= 1) {
                try self.sendBatch(batch);
                continue;
            }
            for (batch.groups) |group_batch| {
                var single_group = group_batch;
                try self.sendBatch(.{
                    .peer_id = batch.peer_id,
                    .groups = (&single_group)[0..1],
                });
            }
        }
    }

    fn sendBatch(self: *CodecTransportHost, batch: transport_iface.PeerBatch) !void {
        const endpoint = self.resolveEndpoint(batch) catch {
            self.metrics.send_failures += 1;
            return;
        };
        const frame = try self.codec.encodePeerBatch(self.alloc, batch);
        errdefer self.codec.freeFrame(self.alloc, frame);

        self.driver.sendFrame(.{
            .source_id = firstSourceNodeId(batch),
            .peer_id = batch.peer_id,
            .endpoint = endpoint,
            .frame = frame,
        }) catch {
            const group_id = firstGroupId(batch) orelse {
                self.metrics.send_failures += 1;
                self.codec.freeFrame(self.alloc, frame);
                return;
            };
            self.metrics.send_failures += 1;
            try self.scheduleRetry(group_id, firstSourceNodeId(batch), batch.peer_id, frame, 1);
            self.codec.freeFrame(self.alloc, frame);
            return;
        };
        self.metrics.sent_frames += 1;
        self.codec.freeFrame(self.alloc, frame);
    }

    fn scheduleRetry(
        self: *CodecTransportHost,
        group_id: core.types.GroupId,
        source_id: ?core.types.NodeId,
        peer_id: core.types.NodeId,
        frame: codec_iface.EncodedFrame,
        attempt: u32,
    ) !void {
        const bounded_delay = computeBackoffRounds(self.retry_policy, attempt);
        try self.pending_retries.append(self.alloc, .{
            .group_id = group_id,
            .source_id = source_id,
            .peer_id = peer_id,
            .frame = .{
                .bytes = try self.alloc.dupe(u8, frame.bytes),
                .media_type = frame.media_type,
            },
            .attempts = attempt,
            .retry_round = self.current_round + bounded_delay,
        });
        self.metrics.retries_scheduled += 1;
    }

    fn resolveEndpoint(self: *CodecTransportHost, batch: transport_iface.PeerBatch) !transport_iface.PeerEndpoint {
        for (batch.groups) |group_batch| {
            if (self.peer_routes.get(.{ .group_id = group_batch.group_id, .node_id = batch.peer_id })) |endpoint| {
                return endpoint.endpoint();
            }
        }
        return error.UnknownPeerRoute;
    }

    fn serveGroup(ptr: *anyopaque, group_id: core.types.GroupId, receiver: transport_iface.TransportReceiver) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        try self.served_groups.put(self.alloc, group_id, receiver);
    }

    fn unserveGroup(ptr: *anyopaque, group_id: core.types.GroupId) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        _ = self.served_groups.remove(group_id);
    }

    fn addPeer(ptr: *anyopaque, group_id: core.types.GroupId, peer: transport_iface.PeerDescriptor) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        const key: PeerRouteKey = .{ .group_id = group_id, .node_id = peer.node_id };
        if (self.peer_routes.contains(key)) return;
        try self.peer_routes.put(self.alloc, key, try OwnedEndpoint.clone(self.alloc, peer.endpoints[0]));
    }

    fn upsertPeer(ptr: *anyopaque, group_id: core.types.GroupId, peer: transport_iface.PeerDescriptor) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        const key: PeerRouteKey = .{ .group_id = group_id, .node_id = peer.node_id };
        const gop = try self.peer_routes.getOrPut(self.alloc, key);
        if (gop.found_existing) {
            if (gop.value_ptr.eql(peer.endpoints[0])) return;
            gop.value_ptr.deinit(self.alloc);
            self.metrics.peer_refreshes += 1;
        }
        gop.value_ptr.* = try OwnedEndpoint.clone(self.alloc, peer.endpoints[0]);
    }

    fn removePeer(ptr: *anyopaque, group_id: core.types.GroupId, node_id: core.types.NodeId) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        const removed = self.peer_routes.fetchRemove(.{ .group_id = group_id, .node_id = node_id }) orelse return;
        var endpoint = removed.value;
        endpoint.deinit(self.alloc);
    }

    fn advanceRound(ptr: *anyopaque) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        self.current_round += 1;
        return try self.drainRetries();
    }

    fn advanceTimeMs(ptr: *anyopaque, now_ms: u64) !void {
        const self: *CodecTransportHost = @ptrCast(@alignCast(ptr));
        self.current_round += 1;
        self.current_time_ms = now_ms;
        return try self.drainRetries();
    }

    fn drainRetries(self: *CodecTransportHost) !void {
        var i: usize = 0;
        while (i < self.pending_retries.items.len) {
            var pending = &self.pending_retries.items[i];
            if (pending.retry_round > self.current_round) {
                i += 1;
                continue;
            }

            const endpoint = self.peer_routes.get(.{
                .group_id = pending.group_id,
                .node_id = pending.peer_id,
            }) orelse {
                self.metrics.retries_exhausted += 1;
                pending.deinit(self.alloc);
                _ = self.pending_retries.orderedRemove(i);
                continue;
            };
            const req: frame_driver_iface.SendFrameRequest = .{
                .source_id = pending.source_id,
                .peer_id = pending.peer_id,
                .endpoint = endpoint.endpoint(),
                .frame = pending.frame,
            };
            self.driver.sendFrame(req) catch {
                if (pending.attempts >= self.retry_policy.max_attempts) {
                    self.metrics.retries_exhausted += 1;
                    pending.deinit(self.alloc);
                    _ = self.pending_retries.orderedRemove(i);
                    continue;
                }
                pending.attempts += 1;
                pending.retry_round = self.current_round + computeBackoffRounds(self.retry_policy, pending.attempts);
                i += 1;
                continue;
            };

            self.metrics.retried_successes += 1;
            self.metrics.sent_frames += 1;
            pending.deinit(self.alloc);
            _ = self.pending_retries.orderedRemove(i);
        }
    }
};

fn firstSourceNodeId(batch: transport_iface.PeerBatch) ?core.types.NodeId {
    for (batch.groups) |group| {
        if (group.messages.len > 0) return group.messages[0].from;
    }
    return null;
}

fn firstGroupId(batch: transport_iface.PeerBatch) ?core.types.GroupId {
    for (batch.groups) |group| return group.group_id;
    return null;
}

fn computeBackoffRounds(policy: RetryPolicy, attempt: u32) u32 {
    var delay = policy.initial_backoff_rounds;
    var shift = attempt - 1;
    while (shift > 0) : (shift -= 1) {
        delay = std.math.mul(u32, delay, 2) catch return policy.max_backoff_rounds;
        if (delay >= policy.max_backoff_rounds) return policy.max_backoff_rounds;
    }
    return @min(delay, policy.max_backoff_rounds);
}
