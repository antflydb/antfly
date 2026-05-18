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
const group = @import("group.zig");
const replica = @import("replica.zig");
const catalog_iface = @import("replica_catalog_iface.zig");

pub const MemoryReplicaFactory = struct {
    alloc: std.mem.Allocator,
    stores: std.AutoHashMapUnmanaged(core.types.GroupId, *core.MemoryStorage) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryReplicaFactory {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryReplicaFactory) void {
        self.stores.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn registerStore(self: *MemoryReplicaFactory, group_id: core.types.GroupId, store: *core.MemoryStorage) !void {
        try self.stores.put(self.alloc, group_id, store);
    }

    pub fn unregisterStore(self: *MemoryReplicaFactory, group_id: core.types.GroupId) bool {
        return self.stores.remove(group_id);
    }

    pub fn factory(self: *MemoryReplicaFactory) catalog_iface.ReplicaFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .instantiate_replica = instantiateReplica,
            },
        };
    }

    fn instantiateReplica(ptr: *anyopaque, record: *const replica.ReplicaRecord) !replica.ReplicaDescriptor {
        const self: *MemoryReplicaFactory = @ptrCast(@alignCast(ptr));
        const store = self.stores.get(record.group_id) orelse return error.UnknownGroup;
        return .{
            .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = record.raft.toConfig(record.group_id, record.local_node_id),
                .storage = store.storage(),
            },
            .bootstrap = record.bootstrap,
        };
    }
};

test "memory replica factory instantiates descriptor from record" {
    var factory = MemoryReplicaFactory.init(std.testing.allocator);
    defer factory.deinit();

    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    try factory.registerStore(7, &store);

    var peers = [_]core.types.NodeId{1};
    const record = replica.ReplicaRecord{
        .group_id = 7,
        .local_node_id = 1,
        .raft = .{ .peers = peers[0..] },
    };
    const desc = try factory.factory().instantiateReplica(&record);
    try std.testing.expectEqual(@as(core.types.GroupId, 7), desc.group.group_id);
    try std.testing.expectEqual(@as(core.types.NodeId, 1), desc.group.local_node_id);
    try std.testing.expectEqual(@as(usize, 1), desc.group.raft_config.peers.len);
}
