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
const replica = @import("replica.zig");
const replica_catalog_iface = @import("replica_catalog_iface.zig");
const multi_raft = @import("multi_raft.zig");

pub const PlacementIntent = struct {
    record: replica.ReplicaRecord,
    peers: []transport_iface.PeerDescriptor = &.{},

    pub fn clone(self: PlacementIntent, alloc: std.mem.Allocator) !PlacementIntent {
        const peers = try alloc.alloc(transport_iface.PeerDescriptor, self.peers.len);
        var initialized: usize = 0;
        errdefer {
            for (peers[0..initialized]) |peer| freePeerDescriptor(alloc, peer);
            alloc.free(peers);
        }
        for (self.peers, 0..) |peer, i| {
            peers[i] = try clonePeerDescriptor(alloc, peer);
            initialized += 1;
        }
        return .{
            .record = try self.record.clone(alloc),
            .peers = peers,
        };
    }

    pub fn deinit(self: *PlacementIntent, alloc: std.mem.Allocator) void {
        self.record.deinit(alloc);
        for (self.peers) |peer| freePeerDescriptor(alloc, peer);
        if (self.peers.len > 0) alloc.free(self.peers);
        self.* = undefined;
    }
};

pub const PlacementProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        list_desired: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, node_id: core.types.NodeId) anyerror![]PlacementIntent,
    };

    pub fn listDesired(self: PlacementProvider, alloc: std.mem.Allocator, node_id: core.types.NodeId) ![]PlacementIntent {
        return try self.vtable.list_desired(self.ptr, alloc, node_id);
    }
};

pub const MemoryPlacementProvider = struct {
    alloc: std.mem.Allocator,
    intents: std.ArrayListUnmanaged(PlacementIntent) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryPlacementProvider {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryPlacementProvider) void {
        for (self.intents.items) |*intent| intent.deinit(self.alloc);
        self.intents.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn provider(self: *MemoryPlacementProvider) PlacementProvider {
        return .{
            .ptr = self,
            .vtable = &.{
                .list_desired = listDesired,
            },
        };
    }

    pub fn replaceAll(self: *MemoryPlacementProvider, intents: []const PlacementIntent) !void {
        for (self.intents.items) |*intent| intent.deinit(self.alloc);
        self.intents.clearRetainingCapacity();
        try self.intents.ensureTotalCapacity(self.alloc, intents.len);
        for (intents) |intent| self.intents.appendAssumeCapacity(try intent.clone(self.alloc));
    }

    fn listDesired(ptr: *anyopaque, alloc: std.mem.Allocator, node_id: core.types.NodeId) ![]PlacementIntent {
        const self: *MemoryPlacementProvider = @ptrCast(@alignCast(ptr));
        var out = std.ArrayListUnmanaged(PlacementIntent).empty;
        errdefer {
            for (out.items) |*intent| intent.deinit(alloc);
            out.deinit(alloc);
        }
        for (self.intents.items) |intent| {
            if (intent.record.local_node_id != node_id) continue;
            try out.append(alloc, try intent.clone(alloc));
        }
        return try out.toOwnedSlice(alloc);
    }
};

pub const ReconcileResult = struct {
    ensured: usize = 0,
    removed: usize = 0,
    peer_updates: usize = 0,
};

pub const ReplicaReconciler = struct {
    alloc: std.mem.Allocator,
    host: *multi_raft.MultiRaft,
    provider: PlacementProvider,
    factory: replica_catalog_iface.ReplicaFactory,

    pub fn reconcile(self: *ReplicaReconciler, node_id: core.types.NodeId) !ReconcileResult {
        const desired = try self.provider.listDesired(self.alloc, node_id);
        defer {
            for (desired) |*intent| intent.deinit(self.alloc);
            self.alloc.free(desired);
        }

        const existing = try self.host.listGroupIds(self.alloc);
        defer self.alloc.free(existing);

        var result: ReconcileResult = .{};

        for (desired) |intent| {
            const desc = try self.factory.instantiateReplica(&intent.record);
            _ = try self.host.ensureReplica(desc);
            result.ensured += 1;
            for (intent.peers) |peer| {
                try self.host.upsertPeer(intent.record.group_id, peer);
                result.peer_updates += 1;
            }
        }

        for (existing) |group_id| {
            if (containsDesired(desired, group_id)) continue;
            try self.host.removeReplica(group_id);
            result.removed += 1;
        }

        return result;
    }
};

fn containsDesired(desired: []const PlacementIntent, group_id: core.types.GroupId) bool {
    for (desired) |intent| {
        if (intent.record.group_id == group_id) return true;
    }
    return false;
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
