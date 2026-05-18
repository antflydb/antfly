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

pub const Protocol = enum {
    http,
    https,
    http2,
    http3,
    quic,
};

pub const PeerEndpoint = struct {
    protocol: Protocol,
    address: []const u8,
    metadata: []const u8 = &.{},
};

pub const PeerResolver = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        resolve_group_peer: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            node_id: u64,
        ) anyerror![]PeerEndpoint,
    };

    pub fn resolveGroupPeer(
        self: PeerResolver,
        alloc: std.mem.Allocator,
        group_id: u64,
        node_id: u64,
    ) ![]PeerEndpoint {
        return try self.vtable.resolve_group_peer(self.ptr, alloc, group_id, node_id);
    }
};

pub const MemoryPeerResolver = struct {
    alloc: std.mem.Allocator,
    routes: std.AutoHashMapUnmanaged(u128, []PeerEndpoint) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryPeerResolver {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryPeerResolver) void {
        var it = self.routes.valueIterator();
        while (it.next()) |endpoints| {
            for (endpoints.*) |endpoint| {
                self.alloc.free(endpoint.address);
                self.alloc.free(endpoint.metadata);
            }
            self.alloc.free(endpoints.*);
        }
        self.routes.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn resolver(self: *MemoryPeerResolver) PeerResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .resolve_group_peer = resolveGroupPeer,
            },
        };
    }

    pub fn upsert(self: *MemoryPeerResolver, group_id: u64, node_id: u64, endpoints: []const PeerEndpoint) !void {
        const gop = try self.routes.getOrPut(self.alloc, key(group_id, node_id));
        if (gop.found_existing) {
            if (endpointsEqual(gop.value_ptr.*, endpoints)) return;
            const cloned = try self.cloneEndpoints(endpoints);
            freeEndpoints(self.alloc, gop.value_ptr.*);
            gop.value_ptr.* = cloned;
            return;
        }
        const cloned = try self.cloneEndpoints(endpoints);
        gop.value_ptr.* = cloned;
    }

    pub fn remove(self: *MemoryPeerResolver, group_id: u64, node_id: u64) bool {
        const removed = self.routes.fetchRemove(key(group_id, node_id));
        if (removed) |entry| {
            freeEndpoints(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    fn resolveGroupPeer(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64) ![]PeerEndpoint {
        const self: *MemoryPeerResolver = @ptrCast(@alignCast(ptr));
        const endpoints = self.routes.get(key(group_id, node_id)) orelse return error.UnknownPeer;
        var out = try alloc.alloc(PeerEndpoint, endpoints.len);
        errdefer alloc.free(out);
        for (endpoints, 0..) |endpoint, i| {
            out[i] = .{
                .protocol = endpoint.protocol,
                .address = try alloc.dupe(u8, endpoint.address),
                .metadata = try alloc.dupe(u8, endpoint.metadata),
            };
        }
        return out;
    }

    fn cloneEndpoints(self: *MemoryPeerResolver, endpoints: []const PeerEndpoint) ![]PeerEndpoint {
        var out = try self.alloc.alloc(PeerEndpoint, endpoints.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |endpoint| {
                self.alloc.free(endpoint.address);
                self.alloc.free(endpoint.metadata);
            }
            self.alloc.free(out);
        }
        for (endpoints, 0..) |endpoint, i| {
            out[i] = .{
                .protocol = endpoint.protocol,
                .address = try self.alloc.dupe(u8, endpoint.address),
                .metadata = try self.alloc.dupe(u8, endpoint.metadata),
            };
            initialized += 1;
        }
        return out;
    }

    fn endpointsEqual(existing: []const PeerEndpoint, incoming: []const PeerEndpoint) bool {
        if (existing.len != incoming.len) return false;
        for (existing, incoming) |lhs, rhs| {
            if (lhs.protocol != rhs.protocol) return false;
            if (!std.mem.eql(u8, lhs.address, rhs.address)) return false;
            if (!std.mem.eql(u8, lhs.metadata, rhs.metadata)) return false;
        }
        return true;
    }

    fn freeEndpoints(alloc: std.mem.Allocator, endpoints: []PeerEndpoint) void {
        for (endpoints) |endpoint| {
            alloc.free(endpoint.address);
            alloc.free(endpoint.metadata);
        }
        alloc.free(endpoints);
    }

    fn key(group_id: u64, node_id: u64) u128 {
        return (@as(u128, group_id) << 64) | @as(u128, node_id);
    }
};

test "peer resolver module compiles" {
    _ = Protocol;
    _ = PeerEndpoint;
    _ = PeerResolver;
    _ = MemoryPeerResolver;
}

test "memory peer resolver clones and resolves endpoints" {
    var resolver = MemoryPeerResolver.init(std.testing.allocator);
    defer resolver.deinit();

    try resolver.upsert(9, 2, &.{
        .{
            .protocol = .http,
            .address = "http://n2",
            .metadata = "zone=b",
        },
    });

    const endpoints = try resolver.resolver().resolveGroupPeer(std.testing.allocator, 9, 2);
    defer {
        for (endpoints) |endpoint| {
            std.testing.allocator.free(endpoint.address);
            std.testing.allocator.free(endpoint.metadata);
        }
        std.testing.allocator.free(endpoints);
    }
    try std.testing.expectEqual(@as(usize, 1), endpoints.len);
    try std.testing.expectEqualStrings("http://n2", endpoints[0].address);
}

test "memory peer resolver can remove routes" {
    var resolver = MemoryPeerResolver.init(std.testing.allocator);
    defer resolver.deinit();

    try resolver.upsert(9, 2, &.{
        .{
            .protocol = .http,
            .address = "http://n2",
            .metadata = "",
        },
    });
    try std.testing.expect(resolver.remove(9, 2));
    try std.testing.expectError(error.UnknownPeer, resolver.resolver().resolveGroupPeer(std.testing.allocator, 9, 2));
}
