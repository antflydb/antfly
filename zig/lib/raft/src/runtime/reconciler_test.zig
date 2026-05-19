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

fn addGroup(
    host: *runtime.MultiRaft,
    group_id: core.types.GroupId,
    local_node_id: core.types.NodeId,
    store: *core.MemoryStorage,
) !void {
    var peers = [_]core.types.NodeId{ local_node_id, 2 };
    try host.addGroup(.{
        .group_id = group_id,
        .local_node_id = local_node_id,
        .raft_config = .{
            .id = local_node_id,
            .group_id = group_id,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });
}

test "replica reconciler ensures desired replicas, removes stale ones, and refreshes peers" {
    var transport = runtime.InMemoryTransportHost.init(std.testing.allocator);
    defer transport.deinit();

    var factory = runtime.MemoryReplicaFactory.init(std.testing.allocator);
    defer factory.deinit();

    var provider = runtime.MemoryPlacementProvider.init(std.testing.allocator);
    defer provider.deinit();

    var store_old = core.MemoryStorage.init(std.testing.allocator);
    defer store_old.deinit();
    var store_new = core.MemoryStorage.init(std.testing.allocator);
    defer store_new.deinit();

    try factory.registerStore(301, &store_old);
    try factory.registerStore(302, &store_new);

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .transport = transport.transport(),
        .replica_factory = factory.factory(),
    });
    defer host.deinit();

    try addGroup(&host, 301, 1, &store_old);

    var peers = [_]runtime.transport_iface.PeerDescriptor{
        .{
            .node_id = 2,
            .endpoints = &.{.{ .protocol = .http3, .address = "https://n2", .metadata = "zone=b" }},
        },
    };
    var raft_peers = [_]core.types.NodeId{ 1, 2 };
    try provider.replaceAll(&.{
        .{
            .record = .{
                .group_id = 302,
                .local_node_id = 1,
                .raft = .{
                    .peers = raft_peers[0..],
                    .election_tick = 7,
                    .heartbeat_tick = 1,
                },
                .bootstrap = .persisted,
            },
            .peers = peers[0..],
        },
    });

    var reconciler = runtime.ReplicaReconciler{
        .alloc = std.testing.allocator,
        .host = &host,
        .provider = provider.provider(),
        .factory = factory.factory(),
    };

    const result = try reconciler.reconcile(1);
    try std.testing.expectEqual(@as(usize, 1), result.ensured);
    try std.testing.expectEqual(@as(usize, 1), result.removed);
    try std.testing.expectEqual(@as(usize, 1), result.peer_updates);
    try std.testing.expect(host.group(301) == null);
    try std.testing.expect(host.group(302) != null);
    try std.testing.expectEqual(@as(usize, 1), transport.peerCount(302));
}
