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
const metadata = @import("../metadata/mod.zig");
const peer_resolver = @import("peer_resolver.zig");
const reconciler = @import("reconciler.zig");

pub const ReplicaIntentUpdate = union(enum) {
    upsert: reconciler.PlacementIntent,
    remove_group: u64,
};

pub const PeerRouteRecord = struct {
    group_id: u64,
    node_id: u64,
    endpoints: []const peer_resolver.PeerEndpoint,

    pub fn clone(self: PeerRouteRecord, alloc: std.mem.Allocator) !PeerRouteRecord {
        const endpoints = try alloc.alloc(peer_resolver.PeerEndpoint, self.endpoints.len);
        var initialized: usize = 0;
        errdefer {
            for (endpoints[0..initialized]) |endpoint| {
                alloc.free(endpoint.address);
                alloc.free(endpoint.metadata);
            }
            alloc.free(endpoints);
        }
        for (self.endpoints, 0..) |endpoint, i| {
            endpoints[i] = .{
                .protocol = endpoint.protocol,
                .address = try alloc.dupe(u8, endpoint.address),
                .metadata = try alloc.dupe(u8, endpoint.metadata),
            };
            initialized += 1;
        }
        return .{
            .group_id = self.group_id,
            .node_id = self.node_id,
            .endpoints = endpoints,
        };
    }

    pub fn deinit(self: *PeerRouteRecord, alloc: std.mem.Allocator) void {
        for (self.endpoints) |endpoint| {
            alloc.free(endpoint.address);
            alloc.free(endpoint.metadata);
        }
        if (self.endpoints.len > 0) alloc.free(self.endpoints);
        self.* = undefined;
    }
};

pub const PeerRouteUpdate = union(enum) {
    upsert: PeerRouteRecord,
    remove: struct {
        group_id: u64,
        node_id: u64,
    },
};

pub const TransitionUpdate = union(enum) {
    upsert: metadata.TransitionRecord,
    remove: struct {
        kind: metadata.TransitionKind,
        transition_id: u64,
    },
};

pub const MetadataUpdate = union(enum) {
    replica_intent: ReplicaIntentUpdate,
    peer_route: PeerRouteUpdate,
    transition: TransitionUpdate,

    pub fn clone(self: MetadataUpdate, alloc: std.mem.Allocator) !MetadataUpdate {
        return switch (self) {
            .replica_intent => |intent| .{
                .replica_intent = switch (intent) {
                    .upsert => |value| .{
                        .upsert = .{
                            .record = value.record,
                            .store_id = value.store_id,
                            .peer_node_ids = if (value.peer_node_ids.len == 0) &.{} else try alloc.dupe(u64, value.peer_node_ids),
                        },
                    },
                    .remove_group => |group_id| .{ .remove_group = group_id },
                },
            },
            .peer_route => |route| .{
                .peer_route = switch (route) {
                    .upsert => |record| .{ .upsert = try record.clone(alloc) },
                    .remove => |record| .{ .remove = record },
                },
            },
            .transition => |transition| .{
                .transition = switch (transition) {
                    .upsert => |record| .{ .upsert = try cloneTransitionRecord(alloc, record) },
                    .remove => |record| .{ .remove = record },
                },
            },
        };
    }

    pub fn deinit(self: *MetadataUpdate, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .replica_intent => |*intent| switch (intent.*) {
                .upsert => |*value| {
                    if (value.peer_node_ids.len > 0) alloc.free(value.peer_node_ids);
                },
                .remove_group => {},
            },
            .peer_route => |*route| switch (route.*) {
                .upsert => |*record| record.deinit(alloc),
                .remove => {},
            },
            .transition => |*transition| switch (transition.*) {
                .upsert => |*record| deinitTransitionRecord(alloc, record),
                .remove => {},
            },
        }
        self.* = undefined;
    }
};

const TransitionState = struct {
    alloc: std.mem.Allocator,
    split: std.AutoHashMapUnmanaged(u64, metadata.SplitTransitionRecord) = .empty,
    merge: std.AutoHashMapUnmanaged(u64, metadata.MergeTransitionRecord) = .empty,

    fn init(alloc: std.mem.Allocator) TransitionState {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *TransitionState) void {
        var split_it = self.split.valueIterator();
        while (split_it.next()) |record| deinitSplitRecord(self.alloc, record);
        self.split.deinit(self.alloc);
        var merge_it = self.merge.valueIterator();
        while (merge_it.next()) |record| deinitMergeRecord(self.alloc, record);
        self.merge.deinit(self.alloc);
        self.* = undefined;
    }

    fn apply(self: *TransitionState, update: TransitionUpdate) !void {
        switch (update) {
            .upsert => |record| switch (record) {
                .split => |split| try self.upsertSplit(split),
                .merge => |merge| try self.upsertMerge(merge),
            },
            .remove => |record| switch (record.kind) {
                .split => _ = try self.removeSplit(record.transition_id),
                .merge => _ = try self.removeMerge(record.transition_id),
            },
        }
    }

    fn upsertSplit(self: *TransitionState, record: metadata.SplitTransitionRecord) !void {
        if (self.split.getPtr(record.transition_id)) |existing| {
            deinitSplitRecord(self.alloc, existing);
            existing.* = try cloneSplitRecord(self.alloc, record);
            return;
        }
        try self.split.put(self.alloc, record.transition_id, try cloneSplitRecord(self.alloc, record));
    }

    fn upsertMerge(self: *TransitionState, record: metadata.MergeTransitionRecord) !void {
        if (self.merge.getPtr(record.transition_id)) |existing| {
            deinitMergeRecord(self.alloc, existing);
            existing.* = try cloneMergeRecord(self.alloc, record);
            return;
        }
        try self.merge.put(self.alloc, record.transition_id, try cloneMergeRecord(self.alloc, record));
    }

    fn removeSplit(self: *TransitionState, transition_id: u64) !bool {
        const removed = self.split.fetchRemove(transition_id);
        if (removed) |entry| {
            var record = entry.value;
            deinitSplitRecord(self.alloc, &record);
            return true;
        }
        return false;
    }

    fn removeMerge(self: *TransitionState, transition_id: u64) !bool {
        const removed = self.merge.fetchRemove(transition_id);
        if (removed) |entry| {
            var record = entry.value;
            deinitMergeRecord(self.alloc, &record);
            return true;
        }
        return false;
    }
};

pub const MetadataView = struct {
    alloc: std.mem.Allocator,
    placements: reconciler.MetadataPlacementState,
    peers: peer_resolver.MemoryPeerResolver,
    transitions: TransitionState,

    pub fn init(alloc: std.mem.Allocator) MetadataView {
        return .{
            .alloc = alloc,
            .placements = reconciler.MetadataPlacementState.init(alloc),
            .peers = peer_resolver.MemoryPeerResolver.init(alloc),
            .transitions = TransitionState.init(alloc),
        };
    }

    pub fn deinit(self: *MetadataView) void {
        self.transitions.deinit();
        self.peers.deinit();
        self.placements.deinit();
        self.* = undefined;
    }

    pub fn apply(self: *MetadataView, update: MetadataUpdate) !void {
        switch (update) {
            .replica_intent => |intent| switch (intent) {
                .upsert => |value| try self.placements.apply(.{ .upsert_intent = value }),
                .remove_group => |group_id| try self.placements.apply(.{ .remove_group = group_id }),
            },
            .peer_route => |route| switch (route) {
                .upsert => |record| try self.peers.upsert(record.group_id, record.node_id, record.endpoints),
                .remove => |record| _ = self.peers.remove(record.group_id, record.node_id),
            },
            .transition => |transition| try self.transitions.apply(transition),
        }
    }

    pub fn placementProvider(self: *MetadataView) reconciler.PlacementProvider {
        return self.placements.provider();
    }

    pub fn replaceReplicaIntents(self: *MetadataView, intents: []const reconciler.PlacementIntent) !void {
        try self.placements.replaceAll(intents);
    }

    pub fn peerResolver(self: *MetadataView) peer_resolver.PeerResolver {
        return self.peers.resolver();
    }
};

fn cloneTransitionRecord(alloc: std.mem.Allocator, record: metadata.TransitionRecord) !metadata.TransitionRecord {
    return switch (record) {
        .split => |split| .{ .split = try cloneSplitRecord(alloc, split) },
        .merge => |merge| .{ .merge = try cloneMergeRecord(alloc, merge) },
    };
}

fn deinitTransitionRecord(alloc: std.mem.Allocator, record: *metadata.TransitionRecord) void {
    switch (record.*) {
        .split => |*split| deinitSplitRecord(alloc, split),
        .merge => |*merge| deinitMergeRecord(alloc, merge),
    }
    record.* = undefined;
}

fn cloneSplitRecord(alloc: std.mem.Allocator, record: metadata.SplitTransitionRecord) !metadata.SplitTransitionRecord {
    return .{
        .transition_id = record.transition_id,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = if (record.split_key) |split_key| try alloc.dupe(u8, split_key) else null,
        .source_range_end = if (record.source_range_end) |end| try alloc.dupe(u8, end) else null,
        .rollback_reason = if (record.rollback_reason) |reason| try alloc.dupe(u8, reason) else null,
    };
}

fn cloneMergeRecord(alloc: std.mem.Allocator, record: metadata.MergeTransitionRecord) !metadata.MergeTransitionRecord {
    return .{
        .transition_id = record.transition_id,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
        .phase = record.phase,
        .rollback_reason = if (record.rollback_reason) |reason| try alloc.dupe(u8, reason) else null,
        .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
    };
}

fn deinitSplitRecord(alloc: std.mem.Allocator, record: *metadata.SplitTransitionRecord) void {
    if (record.split_key) |split_key| alloc.free(split_key);
    if (record.source_range_end) |end| alloc.free(end);
    if (record.rollback_reason) |reason| alloc.free(reason);
    record.* = undefined;
}

fn deinitMergeRecord(alloc: std.mem.Allocator, record: *metadata.MergeTransitionRecord) void {
    if (record.rollback_reason) |reason| alloc.free(reason);
    record.* = undefined;
}

test "metadata view applies placement and peer updates" {
    var view = MetadataView.init(std.testing.allocator);
    defer view.deinit();

    try view.apply(.{
        .replica_intent = .{
            .upsert = .{
                .record = .{
                    .group_id = 101,
                    .replica_id = 2,
                    .local_node_id = 7,
                    .metadata_version = 3,
                },
                .peer_node_ids = &.{ 7, 8 },
            },
        },
    });
    try view.apply(.{
        .peer_route = .{
            .upsert = .{
                .group_id = 101,
                .node_id = 8,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = "http://n8:9000",
                        .metadata = "rack=r1",
                    },
                },
            },
        },
    });

    const intents = try view.placementProvider().listLocalIntents(std.testing.allocator, 7);
    defer {
        for (intents) |intent| {
            if (intent.peer_node_ids.len > 0) std.testing.allocator.free(intent.peer_node_ids);
        }
        std.testing.allocator.free(intents);
    }
    try std.testing.expectEqual(@as(usize, 1), intents.len);
    try std.testing.expectEqual(@as(u64, 101), intents[0].record.group_id);

    const endpoints = try view.peerResolver().resolveGroupPeer(std.testing.allocator, 101, 8);
    defer {
        for (endpoints) |endpoint| {
            std.testing.allocator.free(endpoint.address);
            std.testing.allocator.free(endpoint.metadata);
        }
        std.testing.allocator.free(endpoints);
    }
    try std.testing.expectEqual(@as(usize, 1), endpoints.len);
    try std.testing.expectEqualStrings("http://n8:9000", endpoints[0].address);

    try view.apply(.{
        .replica_intent = .{ .remove_group = 101 },
    });
    try view.apply(.{
        .peer_route = .{ .remove = .{ .group_id = 101, .node_id = 8 } },
    });
    try std.testing.expectError(error.UnknownPeer, view.peerResolver().resolveGroupPeer(std.testing.allocator, 101, 8));
}

test "metadata view module compiles" {
    _ = ReplicaIntentUpdate;
    _ = PeerRouteRecord;
    _ = PeerRouteUpdate;
    _ = MetadataUpdate;
    _ = MetadataView;
}

test "metadata update clone round-trips owned buffers" {
    var update = try (MetadataUpdate{
        .peer_route = .{
            .upsert = .{
                .group_id = 91,
                .node_id = 7,
                .endpoints = &.{
                    .{
                        .protocol = .http,
                        .address = "http://n7:9000",
                        .metadata = "rack=r1",
                    },
                },
            },
        },
    }).clone(std.testing.allocator);
    defer update.deinit(std.testing.allocator);

    switch (update) {
        .peer_route => |route| switch (route) {
            .upsert => |record| {
                try std.testing.expectEqual(@as(usize, 1), record.endpoints.len);
                try std.testing.expectEqualStrings("http://n7:9000", record.endpoints[0].address);
            },
            .remove => return error.TestUnexpectedResult,
        },
        else => return error.TestUnexpectedResult,
    }
}
