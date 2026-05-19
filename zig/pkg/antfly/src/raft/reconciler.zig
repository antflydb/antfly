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
const raft_engine = @import("raft_engine");
const catalog = @import("catalog.zig");
const host_mod = @import("host.zig");
const peer_resolver = @import("peer_resolver.zig");

pub const PlacementIntent = struct {
    record: catalog.ReplicaRecord,
    store_id: u64 = 0,
    peer_node_ids: []const u64 = &.{},
};

pub const PlacementProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        list_local_intents: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, local_node_id: u64) anyerror![]PlacementIntent,
    };

    pub fn listLocalIntents(self: PlacementProvider, alloc: std.mem.Allocator, local_node_id: u64) ![]PlacementIntent {
        return try self.vtable.list_local_intents(self.ptr, alloc, local_node_id);
    }
};

pub const MemoryPlacementProvider = struct {
    alloc: std.mem.Allocator,
    intents: std.ArrayListUnmanaged(PlacementIntent) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryPlacementProvider {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryPlacementProvider) void {
        for (self.intents.items) |intent| freeIntent(self.alloc, intent);
        self.intents.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn provider(self: *MemoryPlacementProvider) PlacementProvider {
        return .{
            .ptr = self,
            .vtable = &.{
                .list_local_intents = listLocalIntents,
            },
        };
    }

    pub fn replaceAll(self: *MemoryPlacementProvider, intents: []const PlacementIntent) !void {
        var next = std.ArrayListUnmanaged(PlacementIntent).empty;
        errdefer {
            for (next.items) |intent| freeIntent(self.alloc, intent);
            next.deinit(self.alloc);
        }
        for (intents) |intent| try next.append(self.alloc, try cloneIntent(self.alloc, intent));

        for (self.intents.items) |intent| freeIntent(self.alloc, intent);
        self.intents.deinit(self.alloc);
        self.intents = next;
    }

    fn listLocalIntents(ptr: *anyopaque, alloc: std.mem.Allocator, local_node_id: u64) ![]PlacementIntent {
        const self: *MemoryPlacementProvider = @ptrCast(@alignCast(ptr));
        var out = std.ArrayListUnmanaged(PlacementIntent).empty;
        errdefer {
            for (out.items) |intent| freeIntent(alloc, intent);
            out.deinit(alloc);
        }
        for (self.intents.items) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            try out.append(alloc, try cloneIntent(alloc, intent));
        }
        return try out.toOwnedSlice(alloc);
    }
};

pub const MetadataPlacementUpdate = union(enum) {
    upsert_intent: PlacementIntent,
    remove_group: u64,
};

pub const MetadataPlacementState = struct {
    alloc: std.mem.Allocator,
    intents: std.AutoHashMapUnmanaged(u64, PlacementIntent) = .empty,

    pub fn init(alloc: std.mem.Allocator) MetadataPlacementState {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MetadataPlacementState) void {
        var it = self.intents.valueIterator();
        while (it.next()) |intent| freeIntent(self.alloc, intent.*);
        self.intents.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn provider(self: *MetadataPlacementState) PlacementProvider {
        return .{
            .ptr = self,
            .vtable = &.{
                .list_local_intents = listLocalIntents,
            },
        };
    }

    pub fn apply(self: *MetadataPlacementState, update: MetadataPlacementUpdate) !void {
        switch (update) {
            .upsert_intent => |intent| try self.upsertIntent(intent),
            .remove_group => |group_id| _ = try self.removeGroup(group_id),
        }
    }

    pub fn upsertIntent(self: *MetadataPlacementState, intent: PlacementIntent) !void {
        if (self.intents.getPtr(intent.record.group_id)) |existing| {
            freeIntent(self.alloc, existing.*);
            existing.* = try cloneIntent(self.alloc, intent);
            return;
        }
        try self.intents.put(self.alloc, intent.record.group_id, try cloneIntent(self.alloc, intent));
    }

    pub fn replaceAll(self: *MetadataPlacementState, intents: []const PlacementIntent) !void {
        var next = std.AutoHashMapUnmanaged(u64, PlacementIntent).empty;
        errdefer {
            var it = next.valueIterator();
            while (it.next()) |intent| freeIntent(self.alloc, intent.*);
            next.deinit(self.alloc);
        }

        for (intents) |intent| {
            try next.put(self.alloc, intent.record.group_id, try cloneIntent(self.alloc, intent));
        }

        var existing_it = self.intents.valueIterator();
        while (existing_it.next()) |intent| freeIntent(self.alloc, intent.*);
        self.intents.deinit(self.alloc);
        self.intents = next;
    }

    pub fn removeGroup(self: *MetadataPlacementState, group_id: u64) !bool {
        const removed = self.intents.fetchRemove(group_id);
        if (removed) |entry| {
            freeIntent(self.alloc, entry.value);
            return true;
        }
        return false;
    }

    fn listLocalIntents(ptr: *anyopaque, alloc: std.mem.Allocator, local_node_id: u64) ![]PlacementIntent {
        const self: *MetadataPlacementState = @ptrCast(@alignCast(ptr));
        var out = std.ArrayListUnmanaged(PlacementIntent).empty;
        errdefer {
            for (out.items) |intent| freeIntent(alloc, intent);
            out.deinit(alloc);
        }

        var it = self.intents.valueIterator();
        while (it.next()) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            try out.append(alloc, try cloneIntent(alloc, intent.*));
        }
        return try out.toOwnedSlice(alloc);
    }
};

pub const ReconcileResult = struct {
    ensured: usize = 0,
    removed: usize = 0,
    refreshed_peers: usize = 0,
};

pub const Reconciler = struct {
    alloc: std.mem.Allocator,
    host: *host_mod.Host,
    provider: PlacementProvider,
    last_intent_hashes: std.AutoHashMapUnmanaged(u64, u64) = .empty,

    pub fn deinit(self: *Reconciler) void {
        self.last_intent_hashes.deinit(self.alloc);
        self.last_intent_hashes = .empty;
    }

    pub fn reconcileOnce(self: *Reconciler) !ReconcileResult {
        const intents = try self.provider.listLocalIntents(self.alloc, self.host.cfg.local_node_id);
        defer freeIntentSlice(self.alloc, intents);
        const existing = try self.host.listGroupIds(self.alloc);
        defer self.alloc.free(existing);

        var desired_group_ids = std.AutoHashMapUnmanaged(u64, void).empty;
        defer desired_group_ids.deinit(self.alloc);

        var result: ReconcileResult = .{};
        for (intents) |intent| {
            try desired_group_ids.put(self.alloc, intent.record.group_id, {});

            const intent_hash = hashIntent(intent);
            const hosted_status = self.host.status(intent.record.group_id);
            const stored_hash = self.last_intent_hashes.get(intent.record.group_id);
            const should_apply =
                hosted_status != .active or
                stored_hash == null or
                stored_hash.? != intent_hash;

            if (should_apply) {
                _ = try self.host.ensureReplica(intent.record);
                result.ensured += 1;
                for (intent.peer_node_ids) |node_id| {
                    if (node_id == self.host.cfg.local_node_id) continue;
                    result.refreshed_peers += self.host.refreshPeerEndpoints(intent.record.group_id, node_id) catch |err| switch (err) {
                        error.UnknownPeer => 0,
                        else => return err,
                    };
                }
                try self.last_intent_hashes.put(self.alloc, intent.record.group_id, intent_hash);
            }
        }
        for (existing) |group_id| {
            if (desired_group_ids.contains(group_id)) continue;
            try self.host.removeReplica(group_id);
            _ = self.last_intent_hashes.remove(group_id);
            result.removed += 1;
        }
        self.host.metrics.reconcile_rounds += 1;
        return result;
    }
};

fn hashIntent(intent: PlacementIntent) u64 {
    var hasher = std.hash.Wyhash.init(0);
    hashU64(&hasher, intent.record.group_id);
    hashU64(&hasher, intent.record.replica_id);
    hashU64(&hasher, intent.record.local_node_id);
    hashU64(&hasher, @as(u64, @intFromEnum(intent.record.bootstrap_mode)));
    hashU64(&hasher, intent.record.metadata_version);
    hashU64(&hasher, intent.store_id);
    hashU64(&hasher, @intCast(intent.peer_node_ids.len));
    for (intent.peer_node_ids) |node_id| hashU64(&hasher, node_id);
    if (intent.record.snapshot_bootstrap) |snapshot| {
        hashU64(&hasher, 1);
        hashU64(&hasher, snapshot.from_node_id);
        hashU64(&hasher, snapshot.term);
        hasher.update(snapshot.snapshot_id);
        hasher.update(snapshot.uri);
    } else {
        hashU64(&hasher, 0);
    }
    if (intent.record.backup_restore_bootstrap) |backup| {
        hashU64(&hasher, 1);
        hasher.update(backup.backup_id);
        hasher.update(backup.location);
        hasher.update(backup.snapshot_path);
    } else {
        hashU64(&hasher, 0);
    }
    return hasher.final();
}

fn hashU64(hasher: *std.hash.Wyhash, value: u64) void {
    var numeric = value;
    hasher.update(std.mem.asBytes(&numeric));
}

fn cloneIntent(alloc: std.mem.Allocator, intent: PlacementIntent) !PlacementIntent {
    var cloned_record = try intent.record.clone(alloc);
    errdefer cloned_record.deinit(alloc);
    return .{
        .record = cloned_record,
        .store_id = intent.store_id,
        .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try alloc.dupe(u64, intent.peer_node_ids),
    };
}

pub fn cloneIntentOwned(alloc: std.mem.Allocator, intent: PlacementIntent) !PlacementIntent {
    return try cloneIntent(alloc, intent);
}

fn freeIntent(alloc: std.mem.Allocator, intent: PlacementIntent) void {
    var record = intent.record;
    record.deinit(alloc);
    if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
}

pub fn freeIntentOwned(alloc: std.mem.Allocator, intent: PlacementIntent) void {
    freeIntent(alloc, intent);
}

fn freeIntentSlice(alloc: std.mem.Allocator, intents: []PlacementIntent) void {
    for (intents) |intent| freeIntent(alloc, intent);
    alloc.free(intents);
}

test "reconciler can ensure desired replicas and remove stale ones" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        stores: [2]*raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const store = if (record.group_id == 301) self.stores[0] else self.stores[1];
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const Resolver = struct {
        fn iface(_: *@This()) peer_resolver.PeerResolver {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .resolve_group_peer = resolve,
                },
            };
        }

        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, node_id: u64) ![]peer_resolver.PeerEndpoint {
            _ = group_id;
            return try alloc.dupe(peer_resolver.PeerEndpoint, &.{
                .{
                    .protocol = .http,
                    .address = if (node_id == 2) try alloc.dupe(u8, "http://n2") else try alloc.dupe(u8, "http://n3"),
                    .metadata = try alloc.dupe(u8, ""),
                },
            });
        }
    };

    var store_a = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .stores = .{ &store_a, &store_b } };
    var resolver = Resolver{};
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .peer_resolver = resolver.iface(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group_id = 301,
        .replica_id = 1,
        .local_node_id = 1,
    });

    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{
        .{
            .record = .{
                .group_id = 302,
                .replica_id = 2,
                .local_node_id = 1,
            },
            .peer_node_ids = &.{2},
        },
    });

    var reconciler = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer reconciler.deinit();
    const result = try reconciler.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), result.ensured);
    try std.testing.expectEqual(@as(usize, 1), result.removed);
    try std.testing.expectEqual(@as(usize, 1), result.refreshed_peers);
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.absent, host.status(301));
    try std.testing.expectEqual(host_mod.HostedReplicaStatus.active, host.status(302));
}

test "reconciler skips unchanged intents after first apply" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const Resolver = struct {
        fn iface(_: *@This()) peer_resolver.PeerResolver {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .resolve_group_peer = resolve,
                },
            };
        }

        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: u64, _: u64) ![]peer_resolver.PeerEndpoint {
            return try alloc.dupe(peer_resolver.PeerEndpoint, &.{});
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var resolver = Resolver{};
    var host = host_mod.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
        .descriptor_factory = factory.iface(),
        .peer_resolver = resolver.iface(),
    });
    defer host.deinit();

    var provider = MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();
    try provider.replaceAll(&.{
        .{
            .record = .{
                .group_id = 401,
                .replica_id = 1,
                .local_node_id = 1,
                .metadata_version = 7,
            },
            .peer_node_ids = &.{ 2, 3 },
        },
    });

    var reconciler = Reconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
    };
    defer reconciler.deinit();

    const first = try reconciler.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 1), first.ensured);
    try std.testing.expectEqual(@as(usize, 0), first.removed);

    const ensure_calls_after_first = host.metrics.ensure_replica_calls;
    const rounds_after_first = host.metrics.reconcile_rounds;

    const second = try reconciler.reconcileOnce();
    try std.testing.expectEqual(@as(usize, 0), second.ensured);
    try std.testing.expectEqual(@as(usize, 0), second.removed);
    try std.testing.expectEqual(ensure_calls_after_first, host.metrics.ensure_replica_calls);
    try std.testing.expectEqual(rounds_after_first + 1, host.metrics.reconcile_rounds);
}

test "reconciler module compiles" {
    _ = PlacementIntent;
    _ = PlacementProvider;
    _ = MemoryPlacementProvider;
    _ = MetadataPlacementUpdate;
    _ = MetadataPlacementState;
    _ = ReconcileResult;
    _ = Reconciler;
    _ = peer_resolver;
}

test "metadata placement state applies incremental updates" {
    var state = MetadataPlacementState.init(std.testing.allocator);
    defer state.deinit();

    try state.apply(.{
        .upsert_intent = .{
            .record = .{
                .group_id = 41,
                .replica_id = 2,
                .local_node_id = 7,
                .metadata_version = 1,
            },
            .peer_node_ids = &.{ 7, 8 },
        },
    });
    try state.apply(.{
        .upsert_intent = .{
            .record = .{
                .group_id = 42,
                .replica_id = 3,
                .local_node_id = 9,
                .metadata_version = 1,
            },
            .peer_node_ids = &.{9},
        },
    });

    const intents = try state.provider().listLocalIntents(std.testing.allocator, 7);
    defer {
        for (intents) |intent| freeIntent(std.testing.allocator, intent);
        std.testing.allocator.free(intents);
    }
    try std.testing.expectEqual(@as(usize, 1), intents.len);
    try std.testing.expectEqual(@as(u64, 41), intents[0].record.group_id);
    try std.testing.expectEqual(@as(usize, 2), intents[0].peer_node_ids.len);

    try std.testing.expect(try state.removeGroup(41));
    const after = try state.provider().listLocalIntents(std.testing.allocator, 7);
    defer {
        for (after) |intent| freeIntent(std.testing.allocator, intent);
        std.testing.allocator.free(after);
    }
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "cloneIntentOwned deep clones backup restore metadata" {
    const original = PlacementIntent{
        .record = .{
            .group_id = 52,
            .replica_id = 4,
            .local_node_id = 9,
            .metadata_version = 11,
            .backup_restore_bootstrap = .{
                .backup_id = try std.testing.allocator.dupe(u8, "snap-52"),
                .location = try std.testing.allocator.dupe(u8, "file:///tmp/backups"),
                .snapshot_path = try std.testing.allocator.dupe(u8, "snap-52/groups/52"),
            },
        },
        .store_id = 21,
        .peer_node_ids = try std.testing.allocator.dupe(u64, &.{ 9, 10 }),
    };
    defer freeIntentOwned(std.testing.allocator, original);

    const cloned = try cloneIntentOwned(std.testing.allocator, original);
    defer freeIntentOwned(std.testing.allocator, cloned);

    try std.testing.expect(cloned.record.backup_restore_bootstrap != null);
    try std.testing.expect(cloned.record.backup_restore_bootstrap.?.backup_id.ptr != original.record.backup_restore_bootstrap.?.backup_id.ptr);
    try std.testing.expect(cloned.record.backup_restore_bootstrap.?.location.ptr != original.record.backup_restore_bootstrap.?.location.ptr);
    try std.testing.expect(cloned.record.backup_restore_bootstrap.?.snapshot_path.ptr != original.record.backup_restore_bootstrap.?.snapshot_path.ptr);
    try std.testing.expect(cloned.peer_node_ids.ptr != original.peer_node_ids.ptr);
}
