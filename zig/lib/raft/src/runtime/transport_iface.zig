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

const core = @import("../core/mod.zig");

pub const TransportProtocol = enum {
    http1,
    http2,
    http3,
    quic,
    grpc,
    custom,
};

pub const PeerEndpoint = struct {
    protocol: TransportProtocol,
    address: []const u8,
    metadata: []const u8 = &.{},
};

pub const PeerDescriptor = struct {
    node_id: core.types.NodeId,
    endpoints: []const PeerEndpoint,
};

pub const GroupMessageBatch = struct {
    group_id: core.types.GroupId,
    messages: []const core.Message,
};

pub const PeerBatch = struct {
    peer_id: core.types.NodeId,
    groups: []const GroupMessageBatch,
};

pub const SnapshotStatus = enum {
    finish,
    failure,
};

pub const TransportReceiver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        handle_message: *const fn (ptr: *anyopaque, group_id: core.types.GroupId, msg: core.Message) anyerror!void,
        report_unreachable: ?*const fn (ptr: *anyopaque, group_id: core.types.GroupId, node_id: core.types.NodeId) anyerror!void = null,
        report_snapshot: ?*const fn (
            ptr: *anyopaque,
            group_id: core.types.GroupId,
            node_id: core.types.NodeId,
            status: SnapshotStatus,
        ) anyerror!void = null,
    };

    pub fn handleMessage(self: TransportReceiver, group_id: core.types.GroupId, msg: core.Message) !void {
        return try self.vtable.handle_message(self.ptr, group_id, msg);
    }

    pub fn reportUnreachable(self: TransportReceiver, group_id: core.types.GroupId, node_id: core.types.NodeId) !void {
        if (self.vtable.report_unreachable) |report_unreachable| {
            return try report_unreachable(self.ptr, group_id, node_id);
        }
    }

    pub fn reportSnapshot(
        self: TransportReceiver,
        group_id: core.types.GroupId,
        node_id: core.types.NodeId,
        status: SnapshotStatus,
    ) !void {
        if (self.vtable.report_snapshot) |report_snapshot| {
            return try report_snapshot(self.ptr, group_id, node_id, status);
        }
    }
};

// Transport owns outbound raft delivery for hosted groups.
// Implementations must consume the passed message slice synchronously and must not retain it.
pub const Transport = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        send_messages: *const fn (ptr: *anyopaque, group_id: core.types.GroupId, messages: []const core.Message) anyerror!void,
        send_peer_batches: ?*const fn (ptr: *anyopaque, batches: []const PeerBatch) anyerror!void = null,
        serve_group: ?*const fn (ptr: *anyopaque, group_id: core.types.GroupId, receiver: TransportReceiver) anyerror!void = null,
        unserve_group: ?*const fn (ptr: *anyopaque, group_id: core.types.GroupId) anyerror!void = null,
        add_peer: ?*const fn (ptr: *anyopaque, group_id: core.types.GroupId, peer: PeerDescriptor) anyerror!void = null,
        upsert_peer: ?*const fn (ptr: *anyopaque, group_id: core.types.GroupId, peer: PeerDescriptor) anyerror!void = null,
        remove_peer: ?*const fn (ptr: *anyopaque, group_id: core.types.GroupId, node_id: core.types.NodeId) anyerror!void = null,
        advance_time_ms: ?*const fn (ptr: *anyopaque, now_ms: u64) anyerror!void = null,
        advance_round: ?*const fn (ptr: *anyopaque) anyerror!void = null,
    };

    pub fn sendMessages(self: Transport, group_id: core.types.GroupId, messages: []const core.Message) !void {
        return try self.vtable.send_messages(self.ptr, group_id, messages);
    }

    pub fn sendPeerBatches(self: Transport, batches: []const PeerBatch) !void {
        if (self.vtable.send_peer_batches) |send_peer_batches| {
            return try send_peer_batches(self.ptr, batches);
        }
        for (batches) |peer_batch| {
            _ = peer_batch.peer_id;
            for (peer_batch.groups) |group_batch| {
                try self.sendMessages(group_batch.group_id, group_batch.messages);
            }
        }
    }

    pub fn supportsPeerBatches(self: Transport) bool {
        return self.vtable.send_peer_batches != null;
    }

    pub fn serveGroup(self: Transport, group_id: core.types.GroupId, receiver: TransportReceiver) !void {
        if (self.vtable.serve_group) |serve_group| {
            return try serve_group(self.ptr, group_id, receiver);
        }
    }

    pub fn unserveGroup(self: Transport, group_id: core.types.GroupId) !void {
        if (self.vtable.unserve_group) |unserve_group| {
            return try unserve_group(self.ptr, group_id);
        }
    }

    pub fn addPeer(self: Transport, group_id: core.types.GroupId, peer: PeerDescriptor) !void {
        if (self.vtable.add_peer) |add_peer| {
            return try add_peer(self.ptr, group_id, peer);
        }
    }

    pub fn upsertPeer(self: Transport, group_id: core.types.GroupId, peer: PeerDescriptor) !void {
        if (self.vtable.upsert_peer) |upsert_peer| {
            return try upsert_peer(self.ptr, group_id, peer);
        }
        return try self.addPeer(group_id, peer);
    }

    pub fn removePeer(self: Transport, group_id: core.types.GroupId, node_id: core.types.NodeId) !void {
        if (self.vtable.remove_peer) |remove_peer| {
            return try remove_peer(self.ptr, group_id, node_id);
        }
    }

    pub fn advanceRound(self: Transport) !void {
        if (self.vtable.advance_round) |advance_round| {
            return try advance_round(self.ptr);
        }
    }

    pub fn advanceTimeMs(self: Transport, now_ms: u64) !void {
        if (self.vtable.advance_time_ms) |advance_time_ms| {
            return try advance_time_ms(self.ptr, now_ms);
        }
        return try self.advanceRound();
    }
};

test "transport iface compiles optional lifecycle and receiver hooks" {
    _ = TransportProtocol;
    _ = PeerEndpoint;
    _ = PeerDescriptor;
    _ = GroupMessageBatch;
    _ = PeerBatch;
    _ = SnapshotStatus;
    _ = TransportReceiver;
    _ = Transport;
}
