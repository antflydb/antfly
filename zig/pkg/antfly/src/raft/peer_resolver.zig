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
const platform_sync = @import("antfly_platform").sync;

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

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
        /// Optional per-route generation used to fence a resolved snapshot
        /// across prepare/publish phases. Immutable resolvers may omit it.
        route_generation: ?*const fn (
            ptr: *anyopaque,
            group_id: u64,
            node_id: u64,
        ) u64 = null,
    };

    pub fn resolveGroupPeer(
        self: PeerResolver,
        alloc: std.mem.Allocator,
        group_id: u64,
        node_id: u64,
    ) ![]PeerEndpoint {
        return try self.vtable.resolve_group_peer(self.ptr, alloc, group_id, node_id);
    }

    pub fn routeGeneration(self: PeerResolver, group_id: u64, node_id: u64) ?u64 {
        const get_generation = self.vtable.route_generation orelse return null;
        return get_generation(self.ptr, group_id, node_id);
    }
};

pub const MemoryPeerResolver = struct {
    const Route = struct {
        endpoints: []PeerEndpoint,
        generation: u64,
    };

    alloc: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    routes: std.AutoHashMapUnmanaged(u128, Route) = .empty,
    next_generation: u64 = 1,

    pub fn init(alloc: std.mem.Allocator) MemoryPeerResolver {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryPeerResolver) void {
        var it = self.routes.valueIterator();
        while (it.next()) |route| {
            for (route.endpoints) |endpoint| {
                self.alloc.free(endpoint.address);
                self.alloc.free(endpoint.metadata);
            }
            self.alloc.free(route.endpoints);
        }
        self.routes.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn resolver(self: *MemoryPeerResolver) PeerResolver {
        return .{
            .ptr = self,
            .vtable = &.{
                .resolve_group_peer = resolveGroupPeer,
                .route_generation = routeGeneration,
            },
        };
    }

    pub fn upsert(self: *MemoryPeerResolver, group_id: u64, node_id: u64, endpoints: []const PeerEndpoint) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const route_key = key(group_id, node_id);
        if (self.routes.getPtr(route_key)) |route| {
            if (endpointsEqual(route.endpoints, endpoints)) return;
            const cloned = try self.cloneEndpoints(endpoints);
            freeEndpoints(self.alloc, route.endpoints);
            route.* = .{
                .endpoints = cloned,
                .generation = self.takeGeneration(),
            };
            return;
        }
        const cloned = try self.cloneEndpoints(endpoints);
        errdefer freeEndpoints(self.alloc, cloned);
        try self.routes.put(self.alloc, route_key, .{
            .endpoints = cloned,
            .generation = self.takeGeneration(),
        });
    }

    pub fn remove(self: *MemoryPeerResolver, group_id: u64, node_id: u64) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const removed = self.routes.fetchRemove(key(group_id, node_id));
        if (removed) |entry| {
            freeEndpoints(self.alloc, entry.value.endpoints);
            return true;
        }
        return false;
    }

    fn resolveGroupPeer(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64) ![]PeerEndpoint {
        const self: *MemoryPeerResolver = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const route = self.routes.get(key(group_id, node_id)) orelse return error.UnknownPeer;
        const endpoints = route.endpoints;
        var out = try alloc.alloc(PeerEndpoint, endpoints.len);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |endpoint| {
                alloc.free(endpoint.address);
                alloc.free(endpoint.metadata);
            }
            alloc.free(out);
        }
        for (endpoints, 0..) |endpoint, i| {
            const address = try alloc.dupe(u8, endpoint.address);
            const metadata = alloc.dupe(u8, endpoint.metadata) catch |err| {
                alloc.free(address);
                return err;
            };
            out[i] = .{ .protocol = endpoint.protocol, .address = address, .metadata = metadata };
            initialized += 1;
        }
        return out;
    }

    fn routeGeneration(ptr: *anyopaque, group_id: u64, node_id: u64) u64 {
        const self: *MemoryPeerResolver = @ptrCast(@alignCast(ptr));
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const route = self.routes.get(key(group_id, node_id)) orelse return 0;
        return route.generation;
    }

    fn takeGeneration(self: *MemoryPeerResolver) u64 {
        const generation = self.next_generation;
        self.next_generation +%= 1;
        if (self.next_generation == 0) self.next_generation = 1;
        return generation;
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
            out[i] = try cloneEndpoint(self.alloc, endpoint);
            initialized += 1;
        }
        return out;
    }

    fn cloneEndpoint(alloc: std.mem.Allocator, endpoint: PeerEndpoint) !PeerEndpoint {
        const address = try alloc.dupe(u8, endpoint.address);
        errdefer alloc.free(address);
        return .{
            .protocol = endpoint.protocol,
            .address = address,
            .metadata = try alloc.dupe(u8, endpoint.metadata),
        };
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
    const route_generation = resolver.resolver().routeGeneration(9, 2).?;
    try resolver.upsert(9, 2, &.{
        .{
            .protocol = .http,
            .address = "http://n2",
            .metadata = "zone=b",
        },
    });
    try std.testing.expectEqual(route_generation, resolver.resolver().routeGeneration(9, 2).?);

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

test "memory peer resolver endpoint ownership is failure atomic" {
    const Runner = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var resolver = MemoryPeerResolver.init(alloc);
            defer resolver.deinit();
            try resolver.upsert(9, 2, &.{
                .{ .protocol = .http, .address = "http://n2-a", .metadata = "zone=a" },
                .{ .protocol = .http, .address = "http://n2-b", .metadata = "zone=b" },
            });
            const endpoints = try resolver.resolver().resolveGroupPeer(alloc, 9, 2);
            defer MemoryPeerResolver.freeEndpoints(alloc, endpoints);
            try std.testing.expectEqual(@as(usize, 2), endpoints.len);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
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
    try std.testing.expectEqual(@as(?u64, 0), resolver.resolver().routeGeneration(9, 2));
    try std.testing.expectError(error.UnknownPeer, resolver.resolver().resolveGroupPeer(std.testing.allocator, 9, 2));
}
