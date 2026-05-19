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
const transport_iface = @import("transport_iface.zig");

pub const InMemoryTransportHost = struct {
    alloc: std.mem.Allocator,
    served_groups: std.AutoHashMapUnmanaged(core.types.GroupId, transport_iface.TransportReceiver) = .empty,
    peer_counts: std.AutoHashMapUnmanaged(core.types.GroupId, usize) = .empty,
    peer_sets: std.AutoHashMapUnmanaged(u128, transport_iface.PeerDescriptor) = .empty,
    sent_messages: usize = 0,
    sent_peer_batches: usize = 0,

    pub fn init(alloc: std.mem.Allocator) InMemoryTransportHost {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *InMemoryTransportHost) void {
        self.served_groups.deinit(self.alloc);
        self.peer_counts.deinit(self.alloc);
        var it = self.peer_sets.valueIterator();
        while (it.next()) |peer| freePeerDescriptor(self.alloc, peer.*);
        self.peer_sets.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn transport(self: *InMemoryTransportHost) transport_iface.Transport {
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
            },
        };
    }

    pub fn isServing(self: *const InMemoryTransportHost, group_id: core.types.GroupId) bool {
        return self.served_groups.contains(group_id);
    }

    pub fn peerCount(self: *const InMemoryTransportHost, group_id: core.types.GroupId) usize {
        return self.peer_counts.get(group_id) orelse 0;
    }

    pub fn deliver(self: *InMemoryTransportHost, group_id: core.types.GroupId, msg: core.Message) !void {
        const receiver = self.served_groups.get(group_id) orelse return error.UnknownGroup;
        try receiver.handleMessage(group_id, msg);
    }

    fn sendMessages(ptr: *anyopaque, group_id: core.types.GroupId, messages: []const core.Message) !void {
        _ = group_id;
        const self: *InMemoryTransportHost = @ptrCast(@alignCast(ptr));
        self.sent_messages += messages.len;
    }

    fn sendPeerBatches(ptr: *anyopaque, batches: []const transport_iface.PeerBatch) !void {
        const self: *InMemoryTransportHost = @ptrCast(@alignCast(ptr));
        self.sent_peer_batches += batches.len;
        for (batches) |batch| {
            for (batch.groups) |group_batch| {
                self.sent_messages += group_batch.messages.len;
            }
        }
    }

    fn serveGroup(ptr: *anyopaque, group_id: core.types.GroupId, receiver: transport_iface.TransportReceiver) !void {
        const self: *InMemoryTransportHost = @ptrCast(@alignCast(ptr));
        try self.served_groups.put(self.alloc, group_id, receiver);
    }

    fn unserveGroup(ptr: *anyopaque, group_id: core.types.GroupId) !void {
        const self: *InMemoryTransportHost = @ptrCast(@alignCast(ptr));
        _ = self.served_groups.remove(group_id);
    }

    fn addPeer(ptr: *anyopaque, group_id: core.types.GroupId, peer: transport_iface.PeerDescriptor) !void {
        const self: *InMemoryTransportHost = @ptrCast(@alignCast(ptr));
        const key = peerKey(group_id, peer.node_id);
        if (self.peer_sets.contains(key)) return;
        try self.peer_sets.put(self.alloc, key, try clonePeerDescriptor(self.alloc, peer));
        const current = self.peer_counts.get(group_id) orelse 0;
        try self.peer_counts.put(self.alloc, group_id, current + 1);
    }

    fn upsertPeer(ptr: *anyopaque, group_id: core.types.GroupId, peer: transport_iface.PeerDescriptor) !void {
        const self: *InMemoryTransportHost = @ptrCast(@alignCast(ptr));
        const key = peerKey(group_id, peer.node_id);
        const gop = try self.peer_sets.getOrPut(self.alloc, key);
        if (gop.found_existing) {
            freePeerDescriptor(self.alloc, gop.value_ptr.*);
            gop.value_ptr.* = try clonePeerDescriptor(self.alloc, peer);
            return;
        }
        gop.value_ptr.* = try clonePeerDescriptor(self.alloc, peer);
        const current = self.peer_counts.get(group_id) orelse 0;
        try self.peer_counts.put(self.alloc, group_id, current + 1);
    }

    fn removePeer(ptr: *anyopaque, group_id: core.types.GroupId, node_id: core.types.NodeId) !void {
        const self: *InMemoryTransportHost = @ptrCast(@alignCast(ptr));
        const removed = self.peer_sets.fetchRemove(peerKey(group_id, node_id)) orelse return;
        freePeerDescriptor(self.alloc, removed.value);
        const current = self.peer_counts.get(group_id) orelse 0;
        if (current <= 1) {
            _ = self.peer_counts.remove(group_id);
        } else {
            try self.peer_counts.put(self.alloc, group_id, current - 1);
        }
    }

    fn peerKey(group_id: core.types.GroupId, node_id: core.types.NodeId) u128 {
        return (@as(u128, group_id) << 64) | @as(u128, node_id);
    }

    fn clonePeerDescriptor(alloc: std.mem.Allocator, peer: transport_iface.PeerDescriptor) !transport_iface.PeerDescriptor {
        const endpoints = try alloc.alloc(transport_iface.PeerEndpoint, peer.endpoints.len);
        var initialized: usize = 0;
        errdefer {
            for (endpoints[0..initialized]) |endpoint| {
                alloc.free(endpoint.address);
                alloc.free(endpoint.metadata);
            }
            alloc.free(endpoints);
        }
        for (peer.endpoints, 0..) |endpoint, i| {
            endpoints[i] = .{
                .protocol = endpoint.protocol,
                .address = try alloc.dupe(u8, endpoint.address),
                .metadata = try alloc.dupe(u8, endpoint.metadata),
            };
            initialized += 1;
        }
        return .{
            .node_id = peer.node_id,
            .endpoints = endpoints,
        };
    }

    fn freePeerDescriptor(alloc: std.mem.Allocator, peer: transport_iface.PeerDescriptor) void {
        for (peer.endpoints) |endpoint| {
            alloc.free(endpoint.address);
            alloc.free(endpoint.metadata);
        }
        alloc.free(peer.endpoints);
    }
};

test "in-memory transport host serves, counts peers, and delivers" {
    const Receiver = struct {
        seen: usize = 0,

        fn iface(self: *@This()) transport_iface.TransportReceiver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .handle_message = handleMessage,
                },
            };
        }

        fn handleMessage(ptr: *anyopaque, group_id: core.types.GroupId, msg: core.Message) !void {
            _ = group_id;
            _ = msg;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += 1;
        }
    };

    var host = InMemoryTransportHost.init(std.testing.allocator);
    defer host.deinit();

    var receiver = Receiver{};
    try host.transport().serveGroup(1, receiver.iface());
    try std.testing.expect(host.isServing(1));

    try host.transport().addPeer(1, .{
        .node_id = 2,
        .endpoints = &.{.{ .protocol = .http3, .address = "https://n2" }},
    });
    try std.testing.expectEqual(@as(usize, 1), host.peerCount(1));

    try host.deliver(1, .{
        .msg_type = .heartbeat,
        .from = 2,
        .to = 1,
        .term = 1,
    });
    try std.testing.expectEqual(@as(usize, 1), receiver.seen);

    try host.transport().removePeer(1, 2);
    try std.testing.expectEqual(@as(usize, 0), host.peerCount(1));
    try host.transport().unserveGroup(1);
    try std.testing.expect(!host.isServing(1));
}
