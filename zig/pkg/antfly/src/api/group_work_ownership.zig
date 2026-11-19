// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Volatile scheduling ownership for restartable group work. Durable effects
//! still require their own authority/receipt checks after a leadership change.
const std = @import("std");
const Persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
const RequestContext = @import("antfly_local_sources").api_operation.RequestContext;

/// Scheduling selection only. Captured operation authorities and native root
/// proofs still govern every effect; election never authorizes a row read.
pub const NativePurpose = enum { serving, retirement };
pub const NativeSelection = struct {
    table_id: u64,
    group_id: u64,
    descriptor: Persistence.Descriptor,
    purpose: NativePurpose,
};
pub const Source = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        is_local_leader: *const fn (ptr: *anyopaque, group_id: u64) bool,
        native_local: ?*const fn (*anyopaque, std.mem.Allocator, NativeSelection, RequestContext) anyerror!bool = null,
    };

    pub fn isLocalLeader(self: Source, group_id: u64) bool {
        return self.vtable.is_local_leader(self.ptr, group_id);
    }
};

/// Permanent groups retain their existing Raft scheduling election. Native
/// groups never fall through to Raft, including on mixed-mode data nodes.
pub fn mayCoordinate(source: ?Source, a: std.mem.Allocator, table_id: u64, group_id: u64, descriptor: ?Persistence.Descriptor, purpose: NativePurpose, context: RequestContext) !bool {
    if (table_id == 0 or group_id == 0) return error.InvalidCatalogRecord;
    if (descriptor) |native| {
        try native.validate();
        const selected = source orelse return false;
        const callback = selected.vtable.native_local orelse return false;
        return callback(selected.ptr, a, .{ .table_id = table_id, .group_id = group_id, .descriptor = native, .purpose = purpose }, context);
    }
    return if (source) |selected| selected.isLocalLeader(group_id) else true;
}

test "SQL relation lifetime maintenance selects captured native owner without a Raft fallback" {
    const Fake = struct {
        raft_calls: usize = 0,
        native_calls: usize = 0,
        fn raft(ptr: *anyopaque, _: u64) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.raft_calls += 1;
            return true;
        }
        fn native(ptr: *anyopaque, _: std.mem.Allocator, selection: NativeSelection, _: RequestContext) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.native_calls += 1;
            return selection.table_id == 11 and selection.group_id == 22 and selection.descriptor.owner_node_id == 33 and selection.purpose == .retirement;
        }
    };
    const a = std.testing.allocator;
    var fake: Fake = .{};
    const descriptor: Persistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 33, .owner_store_id = 44, .generation_instance = @splat(5), .data_generation = 1 };
    const raft_only: Source = .{ .ptr = &fake, .vtable = &.{ .is_local_leader = Fake.raft } };
    try std.testing.expect(!try mayCoordinate(null, a, 11, 22, descriptor, .retirement, .{}));
    try std.testing.expect(!try mayCoordinate(raft_only, a, 11, 22, descriptor, .retirement, .{}));
    try std.testing.expectEqual(@as(usize, 0), fake.raft_calls);
    const source: Source = .{ .ptr = &fake, .vtable = &.{ .is_local_leader = Fake.raft, .native_local = Fake.native } };
    try std.testing.expect(try mayCoordinate(source, a, 11, 22, descriptor, .retirement, .{}));
    try std.testing.expect(!try mayCoordinate(source, a, 12, 22, descriptor, .retirement, .{}));
    try std.testing.expect(!try mayCoordinate(source, a, 11, 22, descriptor, .serving, .{}));
    try std.testing.expectEqual(@as(usize, 0), fake.raft_calls);
    try std.testing.expect(try mayCoordinate(source, a, 11, 22, null, .serving, .{}));
    try std.testing.expectEqual(@as(usize, 1), fake.raft_calls);
}
