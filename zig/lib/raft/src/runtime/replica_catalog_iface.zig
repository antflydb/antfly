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

pub const ReplicaCatalog = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        upsert_replica: *const fn (ptr: *anyopaque, record: replica.ReplicaRecord) anyerror!void,
        remove_replica: *const fn (ptr: *anyopaque, group_id: core.types.GroupId) anyerror!bool,
        list_replicas: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror![]replica.ReplicaRecord,
    };

    pub fn upsertReplica(self: ReplicaCatalog, record: replica.ReplicaRecord) !void {
        return try self.vtable.upsert_replica(self.ptr, record);
    }

    pub fn removeReplica(self: ReplicaCatalog, group_id: core.types.GroupId) !bool {
        return try self.vtable.remove_replica(self.ptr, group_id);
    }

    pub fn listReplicas(self: ReplicaCatalog, alloc: std.mem.Allocator) ![]replica.ReplicaRecord {
        return try self.vtable.list_replicas(self.ptr, alloc);
    }
};

pub const ReplicaFactory = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        instantiate_replica: *const fn (ptr: *anyopaque, record: *const replica.ReplicaRecord) anyerror!replica.ReplicaDescriptor,
    };

    pub fn instantiateReplica(self: ReplicaFactory, record: *const replica.ReplicaRecord) !replica.ReplicaDescriptor {
        return try self.vtable.instantiate_replica(self.ptr, record);
    }
};

test "replica catalog iface compiles" {
    _ = ReplicaCatalog;
    _ = ReplicaFactory;
}
