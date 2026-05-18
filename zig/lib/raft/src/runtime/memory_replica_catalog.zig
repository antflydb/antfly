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
const replica = @import("replica.zig");
const catalog_iface = @import("replica_catalog_iface.zig");

pub const MemoryReplicaCatalog = struct {
    alloc: std.mem.Allocator,
    records: std.AutoHashMapUnmanaged(core.types.GroupId, replica.ReplicaRecord) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryReplicaCatalog {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryReplicaCatalog) void {
        var it = self.records.valueIterator();
        while (it.next()) |record| record.deinit(self.alloc);
        self.records.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn catalog(self: *MemoryReplicaCatalog) catalog_iface.ReplicaCatalog {
        return .{
            .ptr = self,
            .vtable = &.{
                .upsert_replica = upsertReplica,
                .remove_replica = removeReplica,
                .list_replicas = listReplicas,
            },
        };
    }

    pub fn contains(self: *const MemoryReplicaCatalog, group_id: core.types.GroupId) bool {
        return self.records.contains(group_id);
    }

    fn upsertReplica(ptr: *anyopaque, record: replica.ReplicaRecord) !void {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        const cloned = try record.clone(self.alloc);
        errdefer {
            var tmp = cloned;
            tmp.deinit(self.alloc);
        }

        const gop = try self.records.getOrPut(self.alloc, record.group_id);
        if (gop.found_existing) {
            gop.value_ptr.deinit(self.alloc);
        }
        gop.value_ptr.* = cloned;
    }

    fn removeReplica(ptr: *anyopaque, group_id: core.types.GroupId) !bool {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        const removed = self.records.fetchRemove(group_id) orelse return false;
        var record = removed.value;
        record.deinit(self.alloc);
        return true;
    }

    fn listReplicas(ptr: *anyopaque, alloc: std.mem.Allocator) ![]replica.ReplicaRecord {
        const self: *MemoryReplicaCatalog = @ptrCast(@alignCast(ptr));
        var out = try alloc.alloc(replica.ReplicaRecord, self.records.count());
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |*record| record.deinit(alloc);
            alloc.free(out);
        }

        var i: usize = 0;
        var it = self.records.valueIterator();
        while (it.next()) |record| : (i += 1) {
            out[i] = try record.clone(alloc);
            initialized += 1;
        }

        std.sort.block(replica.ReplicaRecord, out, {}, struct {
            fn lessThan(_: void, lhs: replica.ReplicaRecord, rhs: replica.ReplicaRecord) bool {
                return lhs.group_id < rhs.group_id;
            }
        }.lessThan);
        return out;
    }
};

test "memory replica catalog stores cloned replica records" {
    var catalog = MemoryReplicaCatalog.init(std.testing.allocator);
    defer catalog.deinit();

    var peers = [_]core.types.NodeId{ 1, 2, 3 };
    const record = replica.ReplicaRecord{
        .group_id = 9,
        .local_node_id = 2,
        .raft = .{
            .peers = peers[0..],
            .pre_vote = false,
        },
    };

    try catalog.catalog().upsertReplica(record);
    try std.testing.expect(catalog.contains(9));

    const listed = try catalog.catalog().listReplicas(std.testing.allocator);
    defer {
        for (listed) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(listed);
    }
    try std.testing.expectEqual(@as(usize, 1), listed.len);
    try std.testing.expectEqual(@as(core.types.GroupId, 9), listed[0].group_id);
    try std.testing.expectEqual(@as(core.types.NodeId, 2), listed[0].local_node_id);
    try std.testing.expectEqual(false, listed[0].raft.pre_vote);
}
