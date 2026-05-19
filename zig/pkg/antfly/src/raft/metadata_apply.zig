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
const catalog = @import("catalog.zig");
const metadata = @import("../metadata/mod.zig");
const metadata_view = @import("metadata_view.zig");
const peer_resolver = @import("peer_resolver.zig");
const runtime_loop = @import("runtime_loop.zig");

pub const AppliedMetadataChange = union(enum) {
    upsert_replica_intent: struct {
        record: catalog.ReplicaRecord,
        peer_node_ids: []const u64 = &.{},
    },
    remove_replica_intent: struct {
        group_id: u64,
    },
    upsert_peer_route: struct {
        group_id: u64,
        node_id: u64,
        endpoints: []const peer_resolver.PeerEndpoint,
    },
    remove_peer_route: struct {
        group_id: u64,
        node_id: u64,
    },
    upsert_split_transition: metadata.SplitTransitionRecord,
    upsert_merge_transition: metadata.MergeTransitionRecord,
    remove_split_transition: struct {
        transition_id: u64,
    },
    remove_merge_transition: struct {
        transition_id: u64,
    },
};

pub const MetadataApplier = struct {
    sink: runtime_loop.MetadataUpdateSink,

    pub fn init(sink: runtime_loop.MetadataUpdateSink) MetadataApplier {
        return .{ .sink = sink };
    }

    pub fn apply(self: MetadataApplier, change: AppliedMetadataChange) !void {
        try self.sink.submit(switch (change) {
            .upsert_replica_intent => |intent| .{
                .replica_intent = .{
                    .upsert = .{
                        .record = intent.record,
                        .peer_node_ids = intent.peer_node_ids,
                    },
                },
            },
            .remove_replica_intent => |intent| .{
                .replica_intent = .{ .remove_group = intent.group_id },
            },
            .upsert_peer_route => |route| .{
                .peer_route = .{
                    .upsert = .{
                        .group_id = route.group_id,
                        .node_id = route.node_id,
                        .endpoints = route.endpoints,
                    },
                },
            },
            .remove_peer_route => |route| .{
                .peer_route = .{
                    .remove = .{
                        .group_id = route.group_id,
                        .node_id = route.node_id,
                    },
                },
            },
            .upsert_split_transition => |record| .{
                .transition = .{
                    .upsert = .{ .split = record },
                },
            },
            .upsert_merge_transition => |record| .{
                .transition = .{
                    .upsert = .{ .merge = record },
                },
            },
            .remove_split_transition => |record| .{
                .transition = .{
                    .remove = .{
                        .kind = .split,
                        .transition_id = record.transition_id,
                    },
                },
            },
            .remove_merge_transition => |record| .{
                .transition = .{
                    .remove = .{
                        .kind = .merge,
                        .transition_id = record.transition_id,
                    },
                },
            },
        });
    }

    pub fn applyBatch(self: MetadataApplier, changes: []const AppliedMetadataChange) !void {
        var updates = try std.heap.page_allocator.alloc(metadata_view.MetadataUpdate, changes.len);
        defer std.heap.page_allocator.free(updates);

        for (changes, 0..) |change, i| {
            updates[i] = switch (change) {
                .upsert_replica_intent => |intent| .{
                    .replica_intent = .{
                        .upsert = .{
                            .record = intent.record,
                            .peer_node_ids = intent.peer_node_ids,
                        },
                    },
                },
                .remove_replica_intent => |intent| .{
                    .replica_intent = .{ .remove_group = intent.group_id },
                },
                .upsert_peer_route => |route| .{
                    .peer_route = .{
                        .upsert = .{
                            .group_id = route.group_id,
                            .node_id = route.node_id,
                            .endpoints = route.endpoints,
                        },
                    },
                },
                .remove_peer_route => |route| .{
                    .peer_route = .{
                        .remove = .{
                            .group_id = route.group_id,
                            .node_id = route.node_id,
                        },
                    },
                },
                .upsert_split_transition => |record| .{
                    .transition = .{
                        .upsert = .{ .split = record },
                    },
                },
                .upsert_merge_transition => |record| .{
                    .transition = .{
                        .upsert = .{ .merge = record },
                    },
                },
                .remove_split_transition => |record| .{
                    .transition = .{
                        .remove = .{
                            .kind = .split,
                            .transition_id = record.transition_id,
                        },
                    },
                },
                .remove_merge_transition => |record| .{
                    .transition = .{
                        .remove = .{
                            .kind = .merge,
                            .transition_id = record.transition_id,
                        },
                    },
                },
            };
        }

        try self.sink.submitBatch(updates);
    }
};

test "metadata applier submits replica and peer changes through sink" {
    var source = runtime_loop.MemoryUpdateSource.init(std.testing.allocator);
    defer source.deinit();

    const applier = MetadataApplier.init(source.sink());
    try applier.apply(.{
        .upsert_replica_intent = .{
            .record = .{
                .group_id = 1001,
                .replica_id = 2,
                .local_node_id = 7,
            },
            .peer_node_ids = &.{ 7, 8 },
        },
    });
    try applier.apply(.{
        .upsert_peer_route = .{
            .group_id = 1001,
            .node_id = 8,
            .endpoints = &.{
                .{
                    .protocol = .http,
                    .address = "http://n8:9000",
                    .metadata = "",
                },
            },
        },
    });

    const drained = try source.source().drainUpdates(std.testing.allocator, 16);
    defer source.source().freeUpdates(std.testing.allocator, drained);

    try std.testing.expectEqual(@as(usize, 2), drained.len);
    try std.testing.expectEqual(@as(u64, 1001), drained[0].replica_intent.upsert.record.group_id);
    try std.testing.expectEqual(@as(u64, 8), drained[1].peer_route.upsert.node_id);
}

test "metadata applier submits transition changes through sink" {
    var source = runtime_loop.MemoryUpdateSource.init(std.testing.allocator);
    defer source.deinit();

    const applier = MetadataApplier.init(source.sink());
    try applier.apply(.{
        .upsert_split_transition = .{
            .transition_id = 501,
            .source_group_id = 11,
            .destination_group_id = 12,
        },
    });
    try applier.apply(.{
        .remove_split_transition = .{
            .transition_id = 501,
        },
    });

    const drained = try source.source().drainUpdates(std.testing.allocator, 16);
    defer source.source().freeUpdates(std.testing.allocator, drained);

    try std.testing.expectEqual(@as(usize, 2), drained.len);
    try std.testing.expect(drained[0].transition == .upsert);
    try std.testing.expect(drained[1].transition == .remove);
}

test "metadata applier module compiles" {
    _ = AppliedMetadataChange;
    _ = MetadataApplier;
}
