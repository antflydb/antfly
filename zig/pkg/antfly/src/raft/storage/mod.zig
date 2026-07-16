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
pub const catalog = @import("catalog.zig");
pub const ReplicaBootstrapMode = catalog.ReplicaBootstrapMode;
pub const ReplicaRecord = catalog.ReplicaRecord;
pub const ReplicaCatalog = catalog.ReplicaCatalog;
pub const MemoryReplicaCatalog = catalog.MemoryReplicaCatalog;
pub const FileReplicaCatalog = catalog.FileReplicaCatalog;
pub const file_snapshot_store = @import("file_snapshot_store.zig");
pub const FileSnapshotStore = file_snapshot_store.FileSnapshotStore;
pub const FileSnapshotStoreConfig = file_snapshot_store.FileSnapshotStoreConfig;
pub const wal_replica_state = @import("wal_replica_state.zig");
pub const WalReplicaState = wal_replica_state.WalReplicaState;
pub const WalReplicaStateConfig = wal_replica_state.WalReplicaStateConfig;
pub const replica_state = @import("replica_state.zig");
pub const PersistentReplicaState = replica_state.PersistentReplicaState;
pub const persistent_provider = @import("persistent_provider.zig");
pub const PersistentReplicaProvider = persistent_provider.PersistentReplicaProvider;
pub const PersistentReplicaProviderConfig = persistent_provider.PersistentReplicaProviderConfig;
pub const wal_provider = @import("wal_provider.zig");
pub const WalReplicaProvider = wal_provider.WalReplicaProvider;
pub const WalReplicaProviderConfig = wal_provider.WalReplicaProviderConfig;

pub const BootstrapSource = union(enum) {
    empty,
    local_catalog,
    snapshot_fetch: struct {
        from_node_id: u64,
        snapshot_id: []const u8,
        uri: []const u8,
    },
};

pub const ReplicaPathLayout = struct {
    root_dir: []const u8,
    log_dir: []const u8,
    snapshot_dir: []const u8,

    pub fn initForReplica(alloc: std.mem.Allocator, base_dir: []const u8, group_id: u64, replica_id: u64) !ReplicaPathLayout {
        const root_dir = try std.fmt.allocPrint(alloc, "{s}/group-{d}/replica-{d}", .{ base_dir, group_id, replica_id });
        errdefer alloc.free(root_dir);
        const log_dir = try std.fmt.allocPrint(alloc, "{s}/raft-log", .{root_dir});
        errdefer alloc.free(log_dir);
        const snapshot_dir = try std.fmt.allocPrint(alloc, "{s}/snapshots", .{root_dir});
        errdefer alloc.free(snapshot_dir);
        return .{
            .root_dir = root_dir,
            .log_dir = log_dir,
            .snapshot_dir = snapshot_dir,
        };
    }

    pub fn deinit(self: *ReplicaPathLayout, alloc: std.mem.Allocator) void {
        alloc.free(self.root_dir);
        alloc.free(self.log_dir);
        alloc.free(self.snapshot_dir);
        self.* = undefined;
    }
};

test "raft storage module compiles" {
    _ = BootstrapSource;
    _ = ReplicaPathLayout;
    _ = ReplicaBootstrapMode;
    _ = ReplicaRecord;
    _ = ReplicaCatalog;
    _ = MemoryReplicaCatalog;
    _ = FileReplicaCatalog;
    _ = FileSnapshotStore;
    _ = FileSnapshotStoreConfig;
    _ = WalReplicaState;
    _ = WalReplicaStateConfig;
    _ = PersistentReplicaState;
    _ = PersistentReplicaProvider;
    _ = PersistentReplicaProviderConfig;
    _ = WalReplicaProvider;
    _ = WalReplicaProviderConfig;
}

test "replica path layout derives stable directories" {
    var layout = try ReplicaPathLayout.initForReplica(std.testing.allocator, "/tmp/antfly-raft", 42, 7);
    defer layout.deinit(std.testing.allocator);

    try std.testing.expectEqualStrings("/tmp/antfly-raft/group-42/replica-7", layout.root_dir);
    try std.testing.expectEqualStrings("/tmp/antfly-raft/group-42/replica-7/raft-log", layout.log_dir);
    try std.testing.expectEqualStrings("/tmp/antfly-raft/group-42/replica-7/snapshots", layout.snapshot_dir);
}
