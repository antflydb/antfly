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
const fs_paths = @import("../../common/fs_paths.zig");
const group_ids = @import("../../common/group_ids.zig");
const docstore = @import("../../storage/docstore.zig");
const lsm_backend = @import("../../storage/lsm_backend.zig");
const metadata = @import("../mod.zig");
const metadata_table_manager = @import("../table_manager.zig");
const raft_catalog = @import("../../raft/catalog.zig");
const raft_reconciler = @import("../../raft/reconciler.zig");
const raft_storage_mod = @import("../../raft/storage/mod.zig");
const wal_replica_state_mod = @import("../../raft/storage/wal_replica_state.zig");
const raft_state_machine = @import("../../raft/state_machine/mod.zig");

pub const AppliedMetadataBatch = struct {
    commit_index: u64,
    entries_bytes: []const u8,
};

pub const TransitionCommand = union(enum) {
    upsert_node: metadata.NodeRecord,
    register_node: metadata.NodeRecord,
    remove_node: struct {
        node_id: u64,
    },
    request_node_shutdown: struct {
        node_id: u64,
    },
    cancel_node_shutdown: struct {
        node_id: u64,
    },
    finalize_node_shutdown: struct {
        node_id: u64,
    },
    upsert_store: metadata.StoreRecord,
    register_store: metadata.StoreRecord,
    remove_store: struct {
        store_id: u64,
    },
    upsert_replica_intent: raft_reconciler.PlacementIntent,
    remove_replica_intent: struct {
        group_id: u64,
        local_node_id: u64,
    },
    upsert_table: metadata.TableRecord,
    remove_table: struct {
        table_id: u64,
    },
    upsert_schema_progress: metadata.SchemaProgressRecord,
    remove_schema_progress: struct {
        table_id: u64,
        node_id: u64,
    },
    upsert_restore_progress: metadata.RestoreProgressRecord,
    remove_restore_progress: struct {
        table_id: u64,
        node_id: u64,
        group_id: u64,
    },
    upsert_replication_source_status: metadata.ReplicationSourceStatusRecord,
    remove_replication_source_status: struct {
        table_id: u64,
        source_ordinal: u32,
    },
    upsert_range: metadata.RangeRecord,
    remove_range: struct {
        group_id: u64,
    },
    upsert_split_transition: metadata.SplitTransitionRecord,
    remove_split_transition: struct {
        transition_id: u64,
    },
    upsert_merge_transition: metadata.MergeTransitionRecord,
    remove_merge_transition: struct {
        transition_id: u64,
    },
    upsert_reconcile_lease: metadata.ReconcileLeaseRecord,
    remove_reconcile_lease: struct {},
    upsert_shuffle_join_lease: metadata.ShuffleJoinLeaseRecord,
    remove_shuffle_join_lease: struct {
        job_id: u64,
    },
    upsert_reallocation_request: metadata.ReallocationRequestRecord,
    remove_reallocation_request: struct {},

    pub fn deinit(self: *TransitionCommand, alloc: std.mem.Allocator) void {
        switch (self.*) {
            .upsert_node, .register_node => |*record| {
                metadata_table_manager.freeNode(alloc, record.*);
            },
            .upsert_store, .register_store => |*record| {
                metadata_table_manager.freeStore(alloc, record.*);
            },
            .upsert_replica_intent => |*intent| {
                var record = intent.record;
                record.deinit(alloc);
                if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            },
            .upsert_table => |*record| {
                metadata_table_manager.freeTable(alloc, record.*);
            },
            .upsert_restore_progress => |*record| {
                metadata_table_manager.freeRestoreProgress(alloc, record.*);
            },
            .upsert_replication_source_status => |*record| {
                metadata_table_manager.freeReplicationSourceStatus(alloc, record.*);
            },
            .upsert_range => |*record| {
                metadata_table_manager.freeRange(alloc, record.*);
            },
            .upsert_split_transition => |*record| {
                if (record.split_key) |split_key| alloc.free(split_key);
                if (record.source_range_end) |end| alloc.free(end);
                if (record.rollback_reason) |reason| alloc.free(reason);
            },
            .upsert_merge_transition => |*record| {
                if (record.rollback_reason) |reason| alloc.free(reason);
            },
            else => {},
        }
        self.* = undefined;
    }
};

pub fn validateTransitionCommandDataGroupIds(command: TransitionCommand) !void {
    switch (command) {
        .upsert_replica_intent => |intent| try group_ids.requireDataGroupId(intent.record.group_id),
        .remove_replica_intent => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_restore_progress => |record| try group_ids.requireDataGroupId(record.group_id),
        .remove_restore_progress => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .remove_range => |record| try group_ids.requireDataGroupId(record.group_id),
        .upsert_split_transition => |record| {
            try group_ids.requireDataGroupId(record.source_group_id);
            try group_ids.requireDataGroupId(record.destination_group_id);
        },
        .upsert_merge_transition => |record| {
            try group_ids.requireDataGroupId(record.donor_group_id);
            try group_ids.requireDataGroupId(record.receiver_group_id);
        },
        .upsert_shuffle_join_lease => |record| try group_ids.requireDataGroupId(record.owner_group_id),
        else => {},
    }
}

test "transition command validation rejects metadata group ids in data group fields" {
    const metadata_group_id = group_ids.main_metadata_group_id;
    const commands = [_]TransitionCommand{
        .{ .upsert_replica_intent = .{ .record = .{ .group_id = metadata_group_id, .replica_id = 1, .local_node_id = 1 } } },
        .{ .remove_replica_intent = .{ .group_id = metadata_group_id, .local_node_id = 1 } },
        .{ .upsert_restore_progress = .{ .table_id = 1, .node_id = 1, .group_id = metadata_group_id, .backup_id = "backup" } },
        .{ .remove_restore_progress = .{ .table_id = 1, .node_id = 1, .group_id = metadata_group_id } },
        .{ .upsert_range = .{ .group_id = metadata_group_id, .table_id = 1, .start_key = "" } },
        .{ .remove_range = .{ .group_id = metadata_group_id } },
        .{ .upsert_split_transition = .{ .transition_id = 1, .source_group_id = metadata_group_id, .destination_group_id = 2 } },
        .{ .upsert_split_transition = .{ .transition_id = 1, .source_group_id = 2, .destination_group_id = metadata_group_id } },
        .{ .upsert_merge_transition = .{ .transition_id = 1, .donor_group_id = metadata_group_id, .receiver_group_id = 2 } },
        .{ .upsert_merge_transition = .{ .transition_id = 1, .donor_group_id = 2, .receiver_group_id = metadata_group_id } },
        .{ .upsert_shuffle_join_lease = .{ .job_id = 1, .owner_group_id = metadata_group_id, .expires_at_ms = 1 } },
    };
    for (commands) |command| {
        try std.testing.expectError(error.ReservedGroupId, validateTransitionCommandDataGroupIds(command));
    }
}

pub const RaftApplyStoreConfig = struct {
    root_dir: []const u8,
    map_size: usize = 16 * 1024 * 1024,
    no_sync: bool = false,
    // Metadata apply traffic is many tiny durable WAL-backed writes. Flushing
    // every commit amplifies manifest churn and makes simulation/runtime costs
    // pathological without improving durability.
    flush_threshold: usize = 64,
};

pub const ProjectionSignalKind = enum {
    table,
    range,
    store,
    placement_intent,
    reconcile_lease,
    shuffle_join_lease,
    split_transition,
    merge_transition,
    schema_progress,
    restore_progress,
    replication_source_status,
};

pub const ProjectionSignal = struct {
    kind: ProjectionSignalKind,
    metadata_group_id: u64,
    table_name: ?[]const u8 = null,
    table_id: u64 = 0,
    group_id: u64 = 0,
    store_id: u64 = 0,
    node_id: u64 = 0,
};

pub const ProjectionListener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        on_projection_signal: *const fn (ptr: *anyopaque, signal: ProjectionSignal) void,
    };

    pub fn onProjectionSignal(self: ProjectionListener, signal: ProjectionSignal) void {
        self.vtable.on_projection_signal(self.ptr, signal);
    }
};

pub const CommittedKeySignal = struct {
    metadata_group_id: u64,
    key: []const u8,
};

pub const CommittedKeyListener = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        matches_key: *const fn (ptr: *anyopaque, signal: CommittedKeySignal) bool,
        on_committed_key: *const fn (ptr: *anyopaque, signal: CommittedKeySignal) void,
    };

    pub fn onCommittedKey(self: CommittedKeyListener, signal: CommittedKeySignal) void {
        if (!self.vtable.matches_key(self.ptr, signal)) return;
        self.vtable.on_committed_key(self.ptr, signal);
    }
};

pub const RaftApplyStore = struct {
    alloc: std.mem.Allocator,
    io_impl: std.Io.Threaded,
    root_dir: []u8,
    path: []u8,
    backend: lsm_backend.BackendHandle,
    store: docstore.DocStore,
    batches: std.AutoHashMapUnmanaged(u64, OwnedBatch) = .empty,
    projected_placement_intents: std.ArrayListUnmanaged(ProjectedPlacementIntent) = .empty,
    loaded_placement_groups: std.AutoHashMapUnmanaged(u64, void) = .empty,
    projection_listeners: std.ArrayListUnmanaged(ProjectionListener) = .empty,
    committed_key_listeners: std.ArrayListUnmanaged(CommittedKeyListener) = .empty,

    const OwnedBatch = struct {
        commit_index: u64,
        entries_bytes: []u8,
    };

    const ProjectedPlacementIntent = struct {
        metadata_group_id: u64,
        intent: raft_reconciler.PlacementIntent,
    };

    pub fn init(alloc: std.mem.Allocator, cfg: RaftApplyStoreConfig) !RaftApplyStore {
        var io_impl = std.Io.Threaded.init(alloc, .{});
        errdefer io_impl.deinit();

        const root_dir = try alloc.dupe(u8, cfg.root_dir);
        errdefer alloc.free(root_dir);
        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);

        const path = try std.fmt.allocPrint(alloc, "{s}/metadata-apply-store", .{root_dir});
        errdefer alloc.free(path);
        try fs_paths.createDirPathPortable(io_impl.io(), path);

        var backend = try lsm_backend.BackendHandle.open(alloc, path, .{
            .backend = .{
                .durability = if (cfg.no_sync) .none else .full,
            },
            .flush_threshold = cfg.flush_threshold,
        });
        errdefer backend.close();

        var runtime_store = try backend.backend.runtimeStore(alloc, .{ .name = "metadata-apply" });
        errdefer runtime_store.deinit();

        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .root_dir = root_dir,
            .path = path,
            .backend = backend,
            .store = try docstore.DocStore.openRuntime(alloc, runtime_store),
        };
    }

    pub fn deinit(self: *RaftApplyStore) void {
        var it = self.batches.valueIterator();
        while (it.next()) |batch| self.alloc.free(batch.entries_bytes);
        self.batches.deinit(self.alloc);
        for (self.projected_placement_intents.items) |*entry| freePlacementIntent(self.alloc, entry.intent);
        self.projected_placement_intents.deinit(self.alloc);
        self.loaded_placement_groups.deinit(self.alloc);
        self.projection_listeners.deinit(self.alloc);
        self.committed_key_listeners.deinit(self.alloc);
        self.store.close();
        self.backend.close();
        self.alloc.free(self.path);
        self.alloc.free(self.root_dir);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn snapshotBuilder(self: *RaftApplyStore) raft_state_machine.SnapshotBuilder {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_snapshot = buildSnapshot,
                .apply_batch = applyBatch,
            },
        };
    }

    pub fn snapshotWriteStats(self: *const RaftApplyStore) lsm_backend.Backend.WriteStats {
        return self.backend.snapshotWriteStats();
    }

    pub fn latestBatch(self: *RaftApplyStore, group_id: u64) !?AppliedMetadataBatch {
        const batch = (try self.ensureLoaded(group_id)) orelse return null;
        return .{
            .commit_index = batch.commit_index,
            .entries_bytes = batch.entries_bytes,
        };
    }

    pub fn addProjectionListener(self: *RaftApplyStore, listener: ProjectionListener) !void {
        try self.projection_listeners.append(self.alloc, listener);
    }

    pub fn addCommittedKeyListener(self: *RaftApplyStore, listener: CommittedKeyListener) !void {
        try self.committed_key_listeners.append(self.alloc, listener);
    }

    pub fn listSplitTransitions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SplitTransitionRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try splitTransitionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.SplitTransitionRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |*record| {
                if (record.split_key) |split_key| alloc.free(split_key);
                if (record.source_range_end) |end| alloc.free(end);
                if (record.rollback_reason) |reason| alloc.free(reason);
            }
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeSplitTransitionRecord(alloc, kv.value);
        return out;
    }

    pub fn listPlacementIntents(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]raft_reconciler.PlacementIntent {
        try self.ensurePlacementIntentsLoaded(alloc, group_id);

        var count: usize = 0;
        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id == group_id) count += 1;
        }

        const out = try alloc.alloc(raft_reconciler.PlacementIntent, count);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |intent| {
                var record = intent.record;
                record.deinit(alloc);
                if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            }
            alloc.free(out);
        }

        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != group_id) continue;
            out[initialized] = try clonePlacementIntent(alloc, entry.intent);
            initialized += 1;
        }
        return out;
    }

    pub fn listLocalPlacementIntents(self: *RaftApplyStore, alloc: std.mem.Allocator, metadata_group_id: u64, local_node_id: u64) ![]raft_reconciler.PlacementIntent {
        try self.ensurePlacementIntentsLoaded(alloc, metadata_group_id);

        var count: usize = 0;
        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            count += 1;
        }

        const out = try alloc.alloc(raft_reconciler.PlacementIntent, count);
        var initialized: usize = 0;
        errdefer {
            for (out[0..initialized]) |intent| freePlacementIntent(alloc, intent);
            alloc.free(out);
        }

        for (self.projected_placement_intents.items) |entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            out[initialized] = try clonePlacementIntent(alloc, entry.intent);
            initialized += 1;
        }
        return out;
    }

    pub fn freePlacementIntents(_: *RaftApplyStore, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        for (intents) |intent| {
            freePlacementIntent(alloc, intent);
        }
        alloc.free(intents);
    }

    fn ensurePlacementIntentsLoaded(self: *RaftApplyStore, alloc: std.mem.Allocator, metadata_group_id: u64) !void {
        if (self.loaded_placement_groups.contains(metadata_group_id)) return;

        var prefix_buf: [128]u8 = undefined;
        const prefix = try placementPrefixForGroup(&prefix_buf, metadata_group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        for (kvs) |kv| {
            const intent = try decodePlacementIntent(alloc, kv.value);
            defer freePlacementIntent(alloc, intent);
            try self.upsertProjectedPlacementIntent(metadata_group_id, intent);
        }
        try self.loaded_placement_groups.put(self.alloc, metadata_group_id, {});
    }

    fn upsertProjectedPlacementIntent(self: *RaftApplyStore, metadata_group_id: u64, intent: raft_reconciler.PlacementIntent) !void {
        for (self.projected_placement_intents.items) |*entry| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.group_id != intent.record.group_id) continue;
            if (entry.intent.record.local_node_id != intent.record.local_node_id) continue;
            freePlacementIntent(self.alloc, entry.intent);
            entry.* = .{
                .metadata_group_id = metadata_group_id,
                .intent = try clonePlacementIntent(self.alloc, intent),
            };
            return;
        }
        try self.projected_placement_intents.append(self.alloc, .{
            .metadata_group_id = metadata_group_id,
            .intent = try clonePlacementIntent(self.alloc, intent),
        });
    }

    fn removeProjectedPlacementIntent(self: *RaftApplyStore, metadata_group_id: u64, range_group_id: u64, local_node_id: u64) void {
        for (self.projected_placement_intents.items, 0..) |entry, i| {
            if (entry.metadata_group_id != metadata_group_id) continue;
            if (entry.intent.record.group_id != range_group_id) continue;
            if (entry.intent.record.local_node_id != local_node_id) continue;
            freePlacementIntent(self.alloc, entry.intent);
            _ = self.projected_placement_intents.swapRemove(i);
            return;
        }
    }

    pub fn listNodes(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.NodeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try nodePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.NodeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeNode(alloc, record);
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeNodeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeNodes(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.NodeRecord) void {
        for (records) |record| metadata_table_manager.freeNode(alloc, record);
        alloc.free(records);
    }

    pub fn listStores(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.StoreRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.StoreRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeStore(alloc, record);
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeStoreRecord(alloc, kv.value);
        return out;
    }

    pub fn freeStores(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.StoreRecord) void {
        for (records) |record| metadata_table_manager.freeStore(alloc, record);
        alloc.free(records);
    }

    pub fn listMergeTransitions(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.MergeTransitionRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try mergeTransitionPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.MergeTransitionRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |*record| {
                if (record.rollback_reason) |reason| alloc.free(reason);
            }
            alloc.free(out);
        }

        for (kvs, 0..) |kv, i| out[i] = try decodeMergeTransitionRecord(alloc, kv.value);
        return out;
    }

    pub fn listTables(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.TableRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try tablePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.TableRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| {
                metadata_table_manager.freeTable(alloc, record);
            }
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeTableRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeTables(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.TableRecord) void {
        for (records) |record| {
            metadata_table_manager.freeTable(alloc, record);
        }
        alloc.free(records);
    }

    pub fn listSchemaProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.SchemaProgressRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try schemaProgressPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.SchemaProgressRecord, kvs.len);
        errdefer alloc.free(out);
        for (kvs, 0..) |kv, i| out[i] = try decodeSchemaProgressRecord(kv.value);
        return out;
    }

    pub fn freeSchemaProgress(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SchemaProgressRecord) void {
        alloc.free(records);
    }

    pub fn listRestoreProgress(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RestoreProgressRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try restoreProgressPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.RestoreProgressRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeRestoreProgressRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeRestoreProgress(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.RestoreProgressRecord) void {
        for (records) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
        alloc.free(records);
    }

    pub fn listReplicationSourceStatuses(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ReplicationSourceStatusRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try replicationSourceStatusPrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.ReplicationSourceStatusRecord, kvs.len);
        var filled: usize = 0;
        errdefer {
            for (out[0..filled]) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| {
            out[i] = try decodeReplicationSourceStatusRecord(alloc, kv.value);
            filled = i + 1;
        }
        return out;
    }

    pub fn freeReplicationSourceStatuses(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.ReplicationSourceStatusRecord) void {
        for (records) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
        alloc.free(records);
    }

    pub fn listShuffleJoinLeases(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.ShuffleJoinLeaseRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try shuffleJoinLeasePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }

        const out = try alloc.alloc(metadata.ShuffleJoinLeaseRecord, kvs.len);
        errdefer alloc.free(out);
        for (kvs, 0..) |kv, i| {
            var pos: usize = 0;
            out[i] = try readShuffleJoinLeaseRecord(kv.value, &pos);
        }
        return out;
    }

    pub fn freeShuffleJoinLeases(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.ShuffleJoinLeaseRecord) void {
        alloc.free(records);
    }

    pub fn listRanges(self: *RaftApplyStore, alloc: std.mem.Allocator, group_id: u64) ![]metadata.RangeRecord {
        var prefix_buf: [128]u8 = undefined;
        const prefix = try rangePrefixForGroup(&prefix_buf, group_id);
        const kvs = try self.store.scanPrefix(alloc, prefix);
        defer {
            for (kvs) |kv| {
                alloc.free(kv.key);
                alloc.free(kv.value);
            }
            alloc.free(kvs);
        }
        const out = try alloc.alloc(metadata.RangeRecord, kvs.len);
        errdefer {
            for (out[0..kvs.len]) |record| metadata_table_manager.freeRange(alloc, record);
            alloc.free(out);
        }
        for (kvs, 0..) |kv, i| out[i] = try decodeRangeRecord(alloc, kv.value);
        return out;
    }

    pub fn freeRanges(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.RangeRecord) void {
        for (records) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(records);
    }

    pub fn freeSplitTransitions(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.SplitTransitionRecord) void {
        for (records) |record| {
            if (record.split_key) |split_key| alloc.free(split_key);
            if (record.source_range_end) |end| alloc.free(end);
            if (record.rollback_reason) |reason| alloc.free(reason);
        }
        alloc.free(records);
    }

    pub fn freeMergeTransitions(_: *RaftApplyStore, alloc: std.mem.Allocator, records: []metadata.MergeTransitionRecord) void {
        for (records) |record| {
            if (record.rollback_reason) |reason| alloc.free(reason);
        }
        alloc.free(records);
    }

    pub fn getReconcileLease(self: *RaftApplyStore, group_id: u64) !?metadata.ReconcileLeaseRecord {
        var key_buf: [160]u8 = undefined;
        const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readReconcileLeaseRecord(encoded, &pos);
    }

    pub fn getReallocationRequest(self: *RaftApplyStore, group_id: u64) !?metadata.ReallocationRequestRecord {
        var key_buf: [160]u8 = undefined;
        const key = try reallocationRequestKeyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readReallocationRequestRecord(encoded, &pos);
    }

    pub fn getShuffleJoinLease(self: *RaftApplyStore, group_id: u64, job_id: u64) !?metadata.ShuffleJoinLeaseRecord {
        var key_buf: [192]u8 = undefined;
        const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, job_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        var pos: usize = 0;
        return try readShuffleJoinLeaseRecord(encoded, &pos);
    }

    fn buildSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) ![]u8 {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        const batch = try self.ensureLoaded(group_id) orelse return error.MissingAppliedBatch;
        return try alloc.dupe(u8, batch.entries_bytes);
    }

    fn applyBatch(ptr: *anyopaque, batch: raft_state_machine.ApplyBatch) !void {
        const self: *RaftApplyStore = @ptrCast(@alignCast(ptr));
        try self.writeBatch(batch.group_id, batch.commit_index, batch.entries_bytes);
    }

    fn writeBatch(self: *RaftApplyStore, group_id: u64, commit_index: u64, entries_bytes: []const u8) !void {
        var value = try self.alloc.alloc(u8, @sizeOf(u64) + entries_bytes.len);
        defer self.alloc.free(value);
        std.mem.writeInt(u64, value[0..8], commit_index, .little);
        @memcpy(value[8..], entries_bytes);

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        var txn = try self.store.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(key, value);
        try self.projectEntriesTxn(&txn, group_id, entries_bytes);
        try txn.commit();

        const owned_entries = try self.alloc.dupe(u8, entries_bytes);
        errdefer self.alloc.free(owned_entries);
        if (self.batches.getPtr(group_id)) |existing| {
            self.alloc.free(existing.entries_bytes);
            existing.* = .{
                .commit_index = commit_index,
                .entries_bytes = owned_entries,
            };
            return;
        }
        try self.batches.put(self.alloc, group_id, .{
            .commit_index = commit_index,
            .entries_bytes = owned_entries,
        });
    }

    fn ensureLoaded(self: *RaftApplyStore, group_id: u64) !?*OwnedBatch {
        if (self.batches.getPtr(group_id)) |batch| return batch;

        var key_buf: [128]u8 = undefined;
        const key = try keyForGroup(&key_buf, group_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        if (encoded.len < @sizeOf(u64)) return error.InvalidMetadataApplyBatch;

        const commit_index = std.mem.readInt(u64, encoded[0..8], .little);
        const owned_entries = try self.alloc.dupe(u8, encoded[8..]);
        errdefer self.alloc.free(owned_entries);
        try self.batches.put(self.alloc, group_id, .{
            .commit_index = commit_index,
            .entries_bytes = owned_entries,
        });
        return self.batches.getPtr(group_id);
    }

    fn keyForGroup(buf: []u8, group_id: u64) ![]const u8 {
        return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_raft_apply:{d}", .{group_id});
    }

    fn projectEntriesTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, entries_bytes: []const u8) !void {
        const decoded = raft_state_machine.decodeCommittedEntries(self.alloc, entries_bytes) catch |err| switch (err) {
            error.InvalidCommittedEntriesEncoding => return,
            else => return err,
        };
        defer self.alloc.free(decoded);

        for (decoded) |entry| {
            if (entry.entry_type != .normal) continue;
            var command = (try decodeTransitionCommand(self.alloc, entry.data)) orelse continue;
            defer command.deinit(self.alloc);
            try self.applyTransitionCommandTxn(txn, group_id, command);
        }
    }

    fn applyTransitionCommandTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, command: TransitionCommand) !void {
        try validateTransitionCommandDataGroupIds(command);
        switch (command) {
            .upsert_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                const value = try encodeNodeRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .register_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                const value = try self.encodeRegistrationNodeRecordTxn(txn, group_id, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_node => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try nodeKeyForGroup(&key_buf, group_id, record.node_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .request_node_shutdown => |record| {
                try self.applyNodeShutdownRequestTxn(txn, group_id, record.node_id);
            },
            .cancel_node_shutdown => |record| {
                try self.applyNodeShutdownCancelTxn(txn, group_id, record.node_id);
            },
            .finalize_node_shutdown => |record| {
                try self.applyNodeShutdownFinalizeTxn(txn, group_id, record.node_id);
            },
            .upsert_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                const value = try encodeStoreRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                    .node_id = record.node_id,
                });
            },
            .register_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                const applied = try self.normalizeStoreDrainIntentTxn(txn, group_id, record);
                const value = try encodeStoreRecord(self.alloc, applied);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                    .node_id = record.node_id,
                });
            },
            .remove_store => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .store,
                    .metadata_group_id = group_id,
                    .store_id = record.store_id,
                });
            },
            .upsert_replica_intent => |intent| {
                var key_buf: [192]u8 = undefined;
                const key = try placementKeyForGroup(&key_buf, group_id, intent.record.group_id, intent.record.local_node_id);
                const value = try encodePlacementIntent(self.alloc, intent);
                defer self.alloc.free(value);
                try txn.put(key, value);
                try self.upsertProjectedPlacementIntent(group_id, intent);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .placement_intent,
                    .metadata_group_id = group_id,
                    .group_id = intent.record.group_id,
                    .node_id = intent.record.local_node_id,
                });
            },
            .remove_replica_intent => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try placementKeyForGroup(&key_buf, group_id, record.group_id, record.local_node_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.removeProjectedPlacementIntent(group_id, record.group_id, record.local_node_id);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .placement_intent,
                    .metadata_group_id = group_id,
                    .group_id = record.group_id,
                    .node_id = record.local_node_id,
                });
            },
            .upsert_table => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try tableKeyForGroup(&key_buf, group_id, record.table_id);
                const value = try encodeTableRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .table,
                    .metadata_group_id = group_id,
                    .table_name = record.name,
                    .table_id = record.table_id,
                });
            },
            .remove_table => |record| {
                const existing_table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (existing_table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try tableKeyForGroup(&key_buf, group_id, record.table_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .table,
                    .metadata_group_id = group_id,
                    .table_name = existing_table_name,
                    .table_id = record.table_id,
                });
            },
            .upsert_schema_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [192]u8 = undefined;
                const key = try schemaProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id);
                const value = try encodeSchemaProgressRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .schema_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                });
            },
            .remove_schema_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [192]u8 = undefined;
                const key = try schemaProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .schema_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                });
            },
            .upsert_restore_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try restoreProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id, record.group_id);
                const value = try encodeRestoreProgressRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .restore_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                    .group_id = record.group_id,
                });
            },
            .remove_restore_progress => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try restoreProgressKeyForGroup(&key_buf, group_id, record.table_id, record.node_id, record.group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .restore_progress,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .node_id = record.node_id,
                    .group_id = record.group_id,
                });
            },
            .upsert_replication_source_status => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try replicationSourceStatusKeyForGroup(&key_buf, group_id, record.table_id, record.source_ordinal);
                const value = try encodeReplicationSourceStatusRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .replication_source_status,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                });
            },
            .remove_replication_source_status => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [224]u8 = undefined;
                const key = try replicationSourceStatusKeyForGroup(&key_buf, group_id, record.table_id, record.source_ordinal);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .replication_source_status,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                });
            },
            .upsert_range => |record| {
                const table_name = try self.lookupTableNameTxn(txn, group_id, record.table_id);
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                const value = try encodeRangeRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = record.table_id,
                    .group_id = record.group_id,
                });
            },
            .remove_range => |record| {
                const existing = blk: {
                    var key_buf: [160]u8 = undefined;
                    const existing_key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                    const encoded = txn.get(existing_key) catch |err| switch (err) {
                        error.NotFound => break :blk null,
                        else => return err,
                    };
                    const decoded = try decodeRangeRecord(self.alloc, encoded);
                    break :blk decoded;
                };
                defer if (existing) |record_existing| metadata_table_manager.freeRange(self.alloc, record_existing);
                const existing_table_id = if (existing) |record_existing| record_existing.table_id else 0;
                const table_name = if (existing) |record_existing|
                    try self.lookupTableNameTxn(txn, group_id, record_existing.table_id)
                else
                    null;
                defer if (table_name) |name| self.alloc.free(name);
                var key_buf: [160]u8 = undefined;
                const key = try rangeKeyForGroup(&key_buf, group_id, record.group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .range,
                    .metadata_group_id = group_id,
                    .table_name = table_name,
                    .table_id = existing_table_id,
                    .group_id = record.group_id,
                });
            },
            .upsert_split_transition => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try splitTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
                const value = try encodeSplitTransitionRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .split_transition,
                    .metadata_group_id = group_id,
                    .group_id = record.source_group_id,
                });
            },
            .remove_split_transition => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try splitTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .split_transition,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_merge_transition => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try mergeTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
                const value = try encodeMergeTransitionRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .merge_transition,
                    .metadata_group_id = group_id,
                    .group_id = record.receiver_group_id,
                });
            },
            .remove_merge_transition => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try mergeTransitionKeyForGroup(&key_buf, group_id, record.transition_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .merge_transition,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_reconcile_lease => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
                const value = try encodeReconcileLeaseRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .reconcile_lease,
                    .metadata_group_id = group_id,
                });
            },
            .remove_reconcile_lease => {
                var key_buf: [160]u8 = undefined;
                const key = try reconcileLeaseKeyForGroup(&key_buf, group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .reconcile_lease,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_shuffle_join_lease => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, record.job_id);
                const value = try encodeShuffleJoinLeaseRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .shuffle_join_lease,
                    .metadata_group_id = group_id,
                });
            },
            .remove_shuffle_join_lease => |record| {
                var key_buf: [192]u8 = undefined;
                const key = try shuffleJoinLeaseKeyForGroup(&key_buf, group_id, record.job_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
                self.notifyProjectionListeners(.{
                    .kind = .shuffle_join_lease,
                    .metadata_group_id = group_id,
                });
            },
            .upsert_reallocation_request => |record| {
                var key_buf: [160]u8 = undefined;
                const key = try reallocationRequestKeyForGroup(&key_buf, group_id);
                const value = try encodeReallocationRequestRecord(self.alloc, record);
                defer self.alloc.free(value);
                try txn.put(key, value);
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
            .remove_reallocation_request => {
                var key_buf: [160]u8 = undefined;
                const key = try reallocationRequestKeyForGroup(&key_buf, group_id);
                txn.delete(key) catch |err| switch (err) {
                    error.NotFound => {},
                    else => return err,
                };
                self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            },
        }
    }

    fn encodeRegistrationNodeRecordTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.NodeRecord,
    ) ![]u8 {
        var applied = record;
        const existing = try self.loadNodeRecordTxn(txn, group_id, record.node_id);
        defer if (existing) |existing_record| metadata_table_manager.freeNode(self.alloc, existing_record);
        if (existing) |existing_record| {
            applied.lifecycle = existing_record.lifecycle;
        }
        return try encodeNodeRecord(self.alloc, applied);
    }

    fn applyNodeShutdownRequestTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        try self.setNodeLifecycleTxn(txn, group_id, node_id, metadata_table_manager.node_lifecycle_draining, true);
        try self.setNodeStoresDrainRequestedTxn(txn, group_id, node_id, true);
    }

    fn applyNodeShutdownCancelTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        try self.setNodeLifecycleTxn(txn, group_id, node_id, metadata_table_manager.node_lifecycle_active, false);
        try self.setNodeStoresDrainRequestedTxn(txn, group_id, node_id, false);
    }

    fn applyNodeShutdownFinalizeTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !void {
        var node_key_buf: [160]u8 = undefined;
        const node_key = try nodeKeyForGroup(&node_key_buf, group_id, node_id);

        const existing_node = try self.loadNodeRecordTxn(txn, group_id, node_id);
        defer if (existing_node) |record| metadata_table_manager.freeNode(self.alloc, record);
        var draining_node = false;
        if (existing_node) |record| {
            if (metadata_table_manager.nodeLifecycleActive(record.lifecycle)) return error.ActiveNodeFinalizeRejected;
            draining_node = true;
        }

        const StoreRef = struct {
            store_id: u64,
            node_id: u64,
        };
        var stores_to_delete = std.ArrayListUnmanaged(StoreRef).empty;
        defer stores_to_delete.deinit(self.alloc);

        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        {
            var cur = try txn.openCursor();
            defer cur.close();
            var entry = try cur.seekAtOrAfter(prefix);
            while (entry) |kv| : (entry = try cur.next()) {
                if (!std.mem.startsWith(u8, kv.key, prefix)) break;
                const store = try decodeStoreRecord(self.alloc, kv.value);
                defer metadata_table_manager.freeStore(self.alloc, store);
                if (store.node_id == node_id) {
                    if (!draining_node and !store.drain_requested) return error.ActiveNodeFinalizeRejected;
                    try stores_to_delete.append(self.alloc, .{ .store_id = store.store_id, .node_id = store.node_id });
                }
            }
        }

        txn.delete(node_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = node_key });

        for (stores_to_delete.items) |store| {
            var key_buf: [160]u8 = undefined;
            const key = try storeKeyForGroup(&key_buf, group_id, store.store_id);
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            self.notifyProjectionListeners(.{
                .kind = .store,
                .metadata_group_id = group_id,
                .store_id = store.store_id,
                .node_id = store.node_id,
            });
        }
    }

    fn setNodeLifecycleTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        node_id: u64,
        lifecycle: []const u8,
        create_if_missing: bool,
    ) !void {
        var key_buf: [160]u8 = undefined;
        const key = try nodeKeyForGroup(&key_buf, group_id, node_id);

        const existing = try self.loadNodeRecordTxn(txn, group_id, node_id);
        defer if (existing) |existing_record| metadata_table_manager.freeNode(self.alloc, existing_record);

        if (existing) |existing_record| {
            if (std.mem.eql(u8, existing_record.lifecycle, lifecycle)) return;
            var applied = existing_record;
            applied.lifecycle = lifecycle;
            const value = try encodeNodeRecord(self.alloc, applied);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            return;
        }

        if (!create_if_missing) return;
        const record: metadata.NodeRecord = .{
            .node_id = node_id,
            .role = "data",
            .lifecycle = lifecycle,
        };
        const value = try encodeNodeRecord(self.alloc, record);
        defer self.alloc.free(value);
        try txn.put(key, value);
        self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
    }

    fn setNodeStoresDrainRequestedTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        node_id: u64,
        drain_requested: bool,
    ) !void {
        var updates = std.ArrayListUnmanaged(metadata.StoreRecord).empty;
        defer {
            for (updates.items) |record| metadata_table_manager.freeStore(self.alloc, record);
            updates.deinit(self.alloc);
        }

        var prefix_buf: [128]u8 = undefined;
        const prefix = try storePrefixForGroup(&prefix_buf, group_id);
        {
            var cur = try txn.openCursor();
            defer cur.close();
            var entry = try cur.seekAtOrAfter(prefix);
            while (entry) |kv| : (entry = try cur.next()) {
                if (!std.mem.startsWith(u8, kv.key, prefix)) break;
                var store = try decodeStoreRecord(self.alloc, kv.value);
                var store_owned = true;
                errdefer if (store_owned) metadata_table_manager.freeStore(self.alloc, store);
                if (store.node_id == node_id and store.drain_requested != drain_requested) {
                    store.drain_requested = drain_requested;
                    try updates.append(self.alloc, store);
                    store_owned = false;
                } else {
                    store_owned = false;
                    metadata_table_manager.freeStore(self.alloc, store);
                }
            }
        }

        for (updates.items) |record| {
            var key_buf: [160]u8 = undefined;
            const key = try storeKeyForGroup(&key_buf, group_id, record.store_id);
            const value = try encodeStoreRecord(self.alloc, record);
            defer self.alloc.free(value);
            try txn.put(key, value);
            self.notifyCommittedKeyListeners(.{ .metadata_group_id = group_id, .key = key });
            self.notifyProjectionListeners(.{
                .kind = .store,
                .metadata_group_id = group_id,
                .store_id = record.store_id,
                .node_id = record.node_id,
            });
        }
    }

    fn normalizeStoreDrainIntentTxn(
        self: *RaftApplyStore,
        txn: *docstore.DocStore.Txn,
        group_id: u64,
        record: metadata.StoreRecord,
    ) !metadata.StoreRecord {
        var applied = record;
        if (try self.nodeDrainRequestedTxn(txn, group_id, record.node_id)) {
            applied.drain_requested = true;
            return applied;
        }
        const existing = try self.loadStoreRecordTxn(txn, group_id, record.store_id);
        defer if (existing) |existing_record| metadata_table_manager.freeStore(self.alloc, existing_record);
        if (existing) |existing_record| {
            applied.drain_requested = existing_record.drain_requested;
        } else {
            applied.drain_requested = false;
        }
        return applied;
    }

    fn loadNodeRecordTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !?metadata.NodeRecord {
        var key_buf: [160]u8 = undefined;
        const key = try nodeKeyForGroup(&key_buf, group_id, node_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeNodeRecord(self.alloc, encoded);
    }

    fn nodeDrainRequestedTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, node_id: u64) !bool {
        const node = (try self.loadNodeRecordTxn(txn, group_id, node_id)) orelse return false;
        defer metadata_table_manager.freeNode(self.alloc, node);
        return !metadata_table_manager.nodeLifecycleActive(node.lifecycle);
    }

    fn loadStoreRecordTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, store_id: u64) !?metadata.StoreRecord {
        var key_buf: [160]u8 = undefined;
        const key = try storeKeyForGroup(&key_buf, group_id, store_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decodeStoreRecord(self.alloc, encoded);
    }

    fn notifyProjectionListeners(self: *RaftApplyStore, signal: ProjectionSignal) void {
        for (self.projection_listeners.items) |listener| listener.onProjectionSignal(signal);
    }

    fn notifyCommittedKeyListeners(self: *RaftApplyStore, signal: CommittedKeySignal) void {
        for (self.committed_key_listeners.items) |listener| listener.onCommittedKey(signal);
    }

    fn lookupTableName(self: *RaftApplyStore, group_id: u64, table_id: u64) !?[]u8 {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        const encoded = self.store.get(self.alloc, key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer self.alloc.free(encoded);
        const table = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, table);
        return try self.alloc.dupe(u8, table.name);
    }

    fn lookupTableNameTxn(self: *RaftApplyStore, txn: *docstore.DocStore.Txn, group_id: u64, table_id: u64) !?[]u8 {
        var key_buf: [160]u8 = undefined;
        const key = try tableKeyForGroup(&key_buf, group_id, table_id);
        const encoded = txn.get(key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const table = try decodeTableRecord(self.alloc, encoded);
        defer metadata_table_manager.freeTable(self.alloc, table);
        return try self.alloc.dupe(u8, table.name);
    }
};

const transition_magic = "afmd1";
const runtime_status_record_version: u16 = 4;

const TransitionTag = enum(u8) {
    upsert_node = 1,
    remove_node = 2,
    upsert_store = 3,
    remove_store = 4,
    upsert_replica_intent = 5,
    remove_replica_intent = 6,
    upsert_table = 7,
    remove_table = 8,
    upsert_schema_progress = 9,
    remove_schema_progress = 10,
    upsert_restore_progress = 11,
    remove_restore_progress = 12,
    upsert_replication_source_status = 13,
    remove_replication_source_status = 14,
    upsert_range = 15,
    remove_range = 16,
    upsert_split_transition = 17,
    remove_split_transition = 18,
    upsert_merge_transition = 19,
    remove_merge_transition = 20,
    upsert_reconcile_lease = 21,
    remove_reconcile_lease = 22,
    upsert_shuffle_join_lease = 23,
    remove_shuffle_join_lease = 24,
    upsert_reallocation_request = 25,
    remove_reallocation_request = 26,
    request_node_shutdown = 27,
    cancel_node_shutdown = 28,
    register_node = 29,
    register_store = 30,
    finalize_node_shutdown = 31,
};

pub fn encodeTransitionCommand(alloc: std.mem.Allocator, command: TransitionCommand) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);

    try out.appendSlice(alloc, transition_magic);
    switch (command) {
        .upsert_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_node));
            try appendNodeRecord(alloc, &out, record);
        },
        .register_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.register_node));
            try appendNodeRecord(alloc, &out, record);
        },
        .remove_node => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_node));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .request_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.request_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .cancel_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.cancel_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .finalize_node_shutdown => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.finalize_node_shutdown));
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .upsert_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_store));
            try appendStoreRecord(alloc, &out, record);
        },
        .register_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.register_store));
            try appendStoreRecord(alloc, &out, record);
        },
        .remove_store => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_store));
            try appendInt(alloc, &out, u64, record.store_id);
        },
        .upsert_replica_intent => |intent| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_replica_intent));
            try appendPlacementIntent(alloc, &out, intent);
        },
        .remove_replica_intent => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_replica_intent));
            try appendInt(alloc, &out, u64, record.group_id);
            try appendInt(alloc, &out, u64, record.local_node_id);
        },
        .upsert_table => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_table));
            try appendTableRecord(alloc, &out, record);
        },
        .remove_table => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_table));
            try appendInt(alloc, &out, u64, record.table_id);
        },
        .upsert_schema_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_schema_progress));
            try appendSchemaProgressRecord(alloc, &out, record);
        },
        .remove_schema_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_schema_progress));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u64, record.node_id);
        },
        .upsert_restore_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_restore_progress));
            try appendRestoreProgressRecord(alloc, &out, record);
        },
        .remove_restore_progress => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_restore_progress));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u64, record.node_id);
            try appendInt(alloc, &out, u64, record.group_id);
        },
        .upsert_replication_source_status => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_replication_source_status));
            try appendReplicationSourceStatusRecord(alloc, &out, record);
        },
        .remove_replication_source_status => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_replication_source_status));
            try appendInt(alloc, &out, u64, record.table_id);
            try appendInt(alloc, &out, u32, record.source_ordinal);
        },
        .upsert_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_range));
            try appendRangeRecord(alloc, &out, record);
        },
        .remove_range => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_range));
            try appendInt(alloc, &out, u64, record.group_id);
        },
        .upsert_split_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_split_transition));
            try appendSplitTransitionRecord(alloc, &out, record);
        },
        .remove_split_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_split_transition));
            try appendInt(alloc, &out, u64, record.transition_id);
        },
        .upsert_merge_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_merge_transition));
            try appendMergeTransitionRecord(alloc, &out, record);
        },
        .remove_merge_transition => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_merge_transition));
            try appendInt(alloc, &out, u64, record.transition_id);
        },
        .upsert_reconcile_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_reconcile_lease));
            try appendReconcileLeaseRecord(alloc, &out, record);
        },
        .remove_reconcile_lease => {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_reconcile_lease));
        },
        .upsert_shuffle_join_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_shuffle_join_lease));
            try appendShuffleJoinLeaseRecord(alloc, &out, record);
        },
        .remove_shuffle_join_lease => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_shuffle_join_lease));
            try appendInt(alloc, &out, u64, record.job_id);
        },
        .upsert_reallocation_request => |record| {
            try out.append(alloc, @intFromEnum(TransitionTag.upsert_reallocation_request));
            try appendReallocationRequestRecord(alloc, &out, record);
        },
        .remove_reallocation_request => {
            try out.append(alloc, @intFromEnum(TransitionTag.remove_reallocation_request));
        },
    }
    return try out.toOwnedSlice(alloc);
}

pub fn decodeTransitionCommand(alloc: std.mem.Allocator, encoded: []const u8) !?TransitionCommand {
    if (encoded.len < transition_magic.len + 1) return null;
    if (!std.mem.eql(u8, encoded[0..transition_magic.len], transition_magic)) return null;

    var pos: usize = transition_magic.len;
    const tag: TransitionTag = @enumFromInt(encoded[pos]);
    pos += 1;

    return switch (tag) {
        .upsert_node => .{
            .upsert_node = try readNodeRecord(alloc, encoded, &pos),
        },
        .register_node => .{
            .register_node = try readNodeRecord(alloc, encoded, &pos),
        },
        .remove_node => .{
            .remove_node = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .request_node_shutdown => .{
            .request_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .cancel_node_shutdown => .{
            .cancel_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .finalize_node_shutdown => .{
            .finalize_node_shutdown = .{ .node_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_store => .{
            .upsert_store = try readStoreRecord(alloc, encoded, &pos),
        },
        .register_store => .{
            .register_store = try readStoreRecord(alloc, encoded, &pos),
        },
        .remove_store => .{
            .remove_store = .{ .store_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_replica_intent => .{
            .upsert_replica_intent = try readPlacementIntent(alloc, encoded, &pos),
        },
        .remove_replica_intent => .{
            .remove_replica_intent = .{
                .group_id = try readInt(encoded, &pos, u64),
                .local_node_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_table => .{
            .upsert_table = try readTableRecord(alloc, encoded, &pos),
        },
        .remove_table => .{
            .remove_table = .{ .table_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_schema_progress => .{
            .upsert_schema_progress = try readSchemaProgressRecord(encoded, &pos),
        },
        .remove_schema_progress => .{
            .remove_schema_progress = .{
                .table_id = try readInt(encoded, &pos, u64),
                .node_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_restore_progress => .{
            .upsert_restore_progress = try readRestoreProgressRecord(alloc, encoded, &pos),
        },
        .remove_restore_progress => .{
            .remove_restore_progress = .{
                .table_id = try readInt(encoded, &pos, u64),
                .node_id = try readInt(encoded, &pos, u64),
                .group_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_replication_source_status => .{
            .upsert_replication_source_status = try readReplicationSourceStatusRecord(alloc, encoded, &pos),
        },
        .remove_replication_source_status => .{
            .remove_replication_source_status = .{
                .table_id = try readInt(encoded, &pos, u64),
                .source_ordinal = try readInt(encoded, &pos, u32),
            },
        },
        .upsert_range => .{
            .upsert_range = try readRangeRecord(alloc, encoded, &pos),
        },
        .remove_range => .{
            .remove_range = .{ .group_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_split_transition => .{
            .upsert_split_transition = try readSplitTransitionRecord(alloc, encoded, &pos),
        },
        .remove_split_transition => .{
            .remove_split_transition = .{ .transition_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_merge_transition => .{
            .upsert_merge_transition = try readMergeTransitionRecord(alloc, encoded, &pos),
        },
        .remove_merge_transition => .{
            .remove_merge_transition = .{ .transition_id = try readInt(encoded, &pos, u64) },
        },
        .upsert_reconcile_lease => .{
            .upsert_reconcile_lease = try readReconcileLeaseRecord(encoded, &pos),
        },
        .remove_reconcile_lease => .{
            .remove_reconcile_lease = .{},
        },
        .upsert_shuffle_join_lease => .{
            .upsert_shuffle_join_lease = try readShuffleJoinLeaseRecord(encoded, &pos),
        },
        .remove_shuffle_join_lease => .{
            .remove_shuffle_join_lease = .{
                .job_id = try readInt(encoded, &pos, u64),
            },
        },
        .upsert_reallocation_request => .{
            .upsert_reallocation_request = try readReallocationRequestRecord(encoded, &pos),
        },
        .remove_reallocation_request => .{
            .remove_reallocation_request = .{},
        },
    };
}

fn encodePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendPlacementIntent(alloc, &out, intent);
    return try out.toOwnedSlice(alloc);
}

fn encodeNodeRecord(alloc: std.mem.Allocator, record: metadata.NodeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendNodeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeStoreRecord(alloc: std.mem.Allocator, record: metadata.StoreRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendStoreRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeTableRecord(alloc: std.mem.Allocator, record: metadata.TableRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendTableRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeRangeRecord(alloc: std.mem.Allocator, record: metadata.RangeRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendRangeRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeSchemaProgressRecord(alloc: std.mem.Allocator, record: metadata.SchemaProgressRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSchemaProgressRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeRestoreProgressRecord(alloc: std.mem.Allocator, record: metadata.RestoreProgressRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendRestoreProgressRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReplicationSourceStatusRecord(alloc: std.mem.Allocator, record: metadata.ReplicationSourceStatusRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReplicationSourceStatusRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReconcileLeaseRecord(alloc: std.mem.Allocator, record: metadata.ReconcileLeaseRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReconcileLeaseRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeShuffleJoinLeaseRecord(alloc: std.mem.Allocator, record: metadata.ShuffleJoinLeaseRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendShuffleJoinLeaseRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeReallocationRequestRecord(alloc: std.mem.Allocator, record: metadata.ReallocationRequestRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendReallocationRequestRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeSplitTransitionRecord(alloc: std.mem.Allocator, record: metadata.SplitTransitionRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendSplitTransitionRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn encodeMergeTransitionRecord(alloc: std.mem.Allocator, record: metadata.MergeTransitionRecord) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try appendMergeTransitionRecord(alloc, &out, record);
    return try out.toOwnedSlice(alloc);
}

fn decodeSplitTransitionRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.SplitTransitionRecord {
    var pos: usize = 0;
    return try readSplitTransitionRecord(alloc, encoded, &pos);
}

fn decodeMergeTransitionRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.MergeTransitionRecord {
    var pos: usize = 0;
    return try readMergeTransitionRecord(alloc, encoded, &pos);
}

fn decodeTableRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.TableRecord {
    var pos: usize = 0;
    return try readTableRecord(alloc, encoded, &pos);
}

fn decodeRangeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.RangeRecord {
    var pos: usize = 0;
    return try readRangeRecord(alloc, encoded, &pos);
}

fn decodeSchemaProgressRecord(encoded: []const u8) !metadata.SchemaProgressRecord {
    var pos: usize = 0;
    return try readSchemaProgressRecord(encoded, &pos);
}

fn decodeRestoreProgressRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.RestoreProgressRecord {
    var pos: usize = 0;
    return try readRestoreProgressRecord(alloc, encoded, &pos);
}

fn decodeReplicationSourceStatusRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.ReplicationSourceStatusRecord {
    var pos: usize = 0;
    return try readReplicationSourceStatusRecord(alloc, encoded, &pos);
}

fn decodePlacementIntent(alloc: std.mem.Allocator, encoded: []const u8) !raft_reconciler.PlacementIntent {
    var pos: usize = 0;
    return try readPlacementIntent(alloc, encoded, &pos);
}

fn clonePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) !raft_reconciler.PlacementIntent {
    return .{
        .record = try intent.record.clone(alloc),
        .store_id = intent.store_id,
        .peer_node_ids = try alloc.dupe(u64, intent.peer_node_ids),
    };
}

fn freePlacementIntent(alloc: std.mem.Allocator, intent: raft_reconciler.PlacementIntent) void {
    var record = intent.record;
    record.deinit(alloc);
    if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
}

fn decodeNodeRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.NodeRecord {
    var pos: usize = 0;
    return try readNodeRecord(alloc, encoded, &pos);
}

fn decodeStoreRecord(alloc: std.mem.Allocator, encoded: []const u8) !metadata.StoreRecord {
    var pos: usize = 0;
    return try readStoreRecord(alloc, encoded, &pos);
}

fn appendNodeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.NodeRecord,
) !void {
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, @intCast(record.role.len));
    try out.appendSlice(alloc, record.role);
    try appendInt(alloc, out, u32, @intCast(record.lifecycle.len));
    try out.appendSlice(alloc, record.lifecycle);
}

fn readNodeRecord(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !metadata.NodeRecord {
    const node_id = try readInt(encoded, pos, u64);
    const role_len = try readInt(encoded, pos, u32);
    if (pos.* + role_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const role = try alloc.dupe(u8, encoded[pos.* .. pos.* + role_len]);
    errdefer alloc.free(role);
    pos.* += role_len;
    const lifecycle = if (pos.* < encoded.len) blk: {
        const lifecycle_len = try readInt(encoded, pos, u32);
        if (pos.* + lifecycle_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
        const value = try alloc.dupe(u8, encoded[pos.* .. pos.* + lifecycle_len]);
        pos.* += lifecycle_len;
        break :blk value;
    } else try alloc.dupe(u8, metadata_table_manager.node_lifecycle_active);
    return .{
        .node_id = node_id,
        .role = role,
        .lifecycle = lifecycle,
    };
}

fn appendStoreRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.StoreRecord,
) !void {
    try appendInt(alloc, out, u64, record.store_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, @intCast(record.api_url.len));
    try out.appendSlice(alloc, record.api_url);
    try appendInt(alloc, out, u32, @intCast(record.raft_url.len));
    try out.appendSlice(alloc, record.raft_url);
    try appendInt(alloc, out, u32, @intCast(record.role.len));
    try out.appendSlice(alloc, record.role);
    try appendInt(alloc, out, u32, @intCast(record.health_class.len));
    try out.appendSlice(alloc, record.health_class);
    try appendInt(alloc, out, u32, @intCast(record.failure_domain.len));
    try out.appendSlice(alloc, record.failure_domain);
    try out.append(alloc, if (record.live) 1 else 0);
    try appendInt(alloc, out, u64, record.capacity_bytes);
    try appendInt(alloc, out, u64, record.available_bytes);
    try appendInt(alloc, out, u32, record.lease_pressure);
    try appendInt(alloc, out, u32, record.read_load);
    try appendInt(alloc, out, u32, record.write_load);
    try appendInt(alloc, out, u32, record.active_backfills);
    try appendInt(alloc, out, u16, record.backfill_progress_millis);
    try appendInt(alloc, out, u32, @intCast(record.group_statuses.len));
    for (record.group_statuses) |group_status| try appendGroupStatusRecord(alloc, out, group_status);
    try appendInt(alloc, out, u32, @intCast(record.runtime_statuses.len));
    for (record.runtime_statuses) |runtime_status| try appendRuntimeGroupStatusRecord(alloc, out, runtime_status);
    try out.append(alloc, if (record.drain_requested) 1 else 0);
}

fn readStoreRecord(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !metadata.StoreRecord {
    const store_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const api_url_len = try readInt(encoded, pos, u32);
    if (pos.* + api_url_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const api_url = try alloc.dupe(u8, encoded[pos.* .. pos.* + api_url_len]);
    errdefer alloc.free(api_url);
    pos.* += api_url_len;
    const raft_url_len = try readInt(encoded, pos, u32);
    if (pos.* + raft_url_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const raft_url = try alloc.dupe(u8, encoded[pos.* .. pos.* + raft_url_len]);
    errdefer alloc.free(raft_url);
    pos.* += raft_url_len;
    const role_len = try readInt(encoded, pos, u32);
    if (pos.* + role_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const role = try alloc.dupe(u8, encoded[pos.* .. pos.* + role_len]);
    errdefer alloc.free(role);
    pos.* += role_len;
    const health_class_len = try readInt(encoded, pos, u32);
    if (pos.* + health_class_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const health_class = try alloc.dupe(u8, encoded[pos.* .. pos.* + health_class_len]);
    errdefer alloc.free(health_class);
    pos.* += health_class_len;
    const failure_domain_len = try readInt(encoded, pos, u32);
    if (pos.* + failure_domain_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const failure_domain = try alloc.dupe(u8, encoded[pos.* .. pos.* + failure_domain_len]);
    errdefer alloc.free(failure_domain);
    pos.* += failure_domain_len;
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const live = encoded[pos.*] != 0;
    pos.* += 1;
    const capacity_bytes = try readInt(encoded, pos, u64);
    const available_bytes = try readInt(encoded, pos, u64);
    const lease_pressure = try readInt(encoded, pos, u32);
    const read_load = try readInt(encoded, pos, u32);
    const write_load = try readInt(encoded, pos, u32);
    const active_backfills = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const backfill_progress_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u16) else 1000;
    const group_status_count = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const group_statuses = try alloc.alloc(metadata.GroupStatusReport, group_status_count);
    var initialized: usize = 0;
    errdefer {
        for (group_statuses[0..initialized]) |record| metadata_table_manager.freeGroupStatus(alloc, record);
        if (group_statuses.len > 0) alloc.free(group_statuses);
    }
    while (initialized < group_status_count) : (initialized += 1) {
        group_statuses[initialized] = try readGroupStatusRecord(alloc, encoded, pos);
    }
    const runtime_status_count = if (pos.* < encoded.len) try readInt(encoded, pos, u32) else 0;
    const runtime_statuses = try alloc.alloc(metadata.RuntimeGroupStatusReport, runtime_status_count);
    var initialized_runtime_statuses: usize = 0;
    errdefer {
        for (runtime_statuses[0..initialized_runtime_statuses]) |record| metadata_table_manager.freeRuntimeGroupStatusReport(alloc, record);
        if (runtime_statuses.len > 0) alloc.free(runtime_statuses);
    }
    while (initialized_runtime_statuses < runtime_status_count) : (initialized_runtime_statuses += 1) {
        runtime_statuses[initialized_runtime_statuses] = try readRuntimeGroupStatusRecord(alloc, encoded, pos);
    }
    const drain_requested = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    return .{
        .store_id = store_id,
        .node_id = node_id,
        .api_url = api_url,
        .raft_url = raft_url,
        .role = role,
        .health_class = health_class,
        .failure_domain = failure_domain,
        .live = live,
        .drain_requested = drain_requested,
        .capacity_bytes = capacity_bytes,
        .available_bytes = available_bytes,
        .lease_pressure = lease_pressure,
        .read_load = read_load,
        .write_load = write_load,
        .active_backfills = active_backfills,
        .backfill_progress_millis = backfill_progress_millis,
        .group_statuses = group_statuses,
        .runtime_statuses = runtime_statuses,
    };
}

fn appendRuntimeGroupStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeGroupStatusReport,
) !void {
    try appendInt(alloc, out, u16, runtime_status_record_version);
    try appendInt(alloc, out, u64, record.table_id);
    try appendRequiredString(alloc, out, record.table_name);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.store_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u64, record.updated_at_ns);
    try appendRequiredString(alloc, out, record.source);
    try appendRequiredString(alloc, out, record.freshness);
    try appendInt(alloc, out, u64, record.topology_generation);
    try appendInt(alloc, out, u64, record.lsm_root_generation);
    try appendInt(alloc, out, u64, record.status_generation);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.disk_bytes);
    try appendInt(alloc, out, u64, record.created_at_millis);
    try appendInt(alloc, out, u32, record.index_count);
    try out.append(alloc, if (record.enrichment_enabled) 1 else 0);
    try appendInt(alloc, out, u64, record.enrichment_target_sequence);
    try appendInt(alloc, out, u64, record.enrichment_applied_sequence);
    try out.append(alloc, if (record.enrichment_retrying) 1 else 0);
    try out.append(alloc, if (record.enrichment_worker_failed) 1 else 0);
    try out.append(alloc, if (record.async_indexing_active) 1 else 0);
    try out.append(alloc, if (record.async_startup_active) 1 else 0);
    try out.append(alloc, if (record.async_dense_catch_up_active) 1 else 0);
    try out.append(alloc, if (record.async_bulk_coalescing_active) 1 else 0);
    try appendRuntimeDocIdentityStatusRecord(alloc, out, record.doc_identity);
    try appendRuntimeDocSetPlanningStatusRecord(alloc, out, record.doc_set_planning);
    try appendInt(alloc, out, u32, @intCast(record.indexes.len));
    for (record.indexes) |index| try appendRuntimeIndexStatusRecord(alloc, out, index);
}

fn readRuntimeGroupStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RuntimeGroupStatusReport {
    const version = try readInt(encoded, pos, u16);
    if (version != 1 and version != 2 and version != 3 and version != runtime_status_record_version) return error.InvalidMetadataTransitionEncoding;
    const table_id = try readInt(encoded, pos, u64);
    const table_name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(table_name);
    const group_id = try readInt(encoded, pos, u64);
    const store_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const updated_at_ns = try readInt(encoded, pos, u64);
    const source = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(source);
    const freshness = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(freshness);
    const topology_generation = try readInt(encoded, pos, u64);
    const lsm_root_generation = try readInt(encoded, pos, u64);
    const status_generation = try readInt(encoded, pos, u64);
    const doc_count = try readInt(encoded, pos, u64);
    const disk_bytes = if (version >= 2) try readInt(encoded, pos, u64) else 0;
    const created_at_millis = if (version >= 2) try readInt(encoded, pos, u64) else 0;
    const index_count = try readInt(encoded, pos, u32);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const enrichment_enabled = encoded[pos.*] != 0;
    pos.* += 1;
    const enrichment_target_sequence = try readInt(encoded, pos, u64);
    const enrichment_applied_sequence = try readInt(encoded, pos, u64);
    if (pos.* + 3 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const enrichment_retrying = encoded[pos.*] != 0;
    pos.* += 1;
    const enrichment_worker_failed = encoded[pos.*] != 0;
    pos.* += 1;
    const async_indexing_active = encoded[pos.*] != 0;
    pos.* += 1;
    if (pos.* + 3 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const async_startup_active = encoded[pos.*] != 0;
    pos.* += 1;
    const async_dense_catch_up_active = encoded[pos.*] != 0;
    pos.* += 1;
    const async_bulk_coalescing_active = encoded[pos.*] != 0;
    pos.* += 1;
    const doc_identity = if (version >= 3) try readRuntimeDocIdentityStatusRecord(encoded, pos) else metadata.RuntimeDocIdentityStatusReport{};
    const doc_set_planning = if (version >= 3) try readRuntimeDocSetPlanningStatusRecord(encoded, pos, version) else metadata.RuntimeDocSetPlanningStatusReport{};
    const runtime_index_count = try readInt(encoded, pos, u32);
    const indexes = try alloc.alloc(metadata.RuntimeIndexStatusReport, runtime_index_count);
    var initialized: usize = 0;
    errdefer {
        for (indexes[0..initialized]) |record| metadata_table_manager.freeRuntimeIndexStatusReport(alloc, record);
        if (indexes.len > 0) alloc.free(indexes);
    }
    while (initialized < runtime_index_count) : (initialized += 1) {
        indexes[initialized] = try readRuntimeIndexStatusRecord(alloc, encoded, pos);
    }
    return .{
        .table_id = table_id,
        .table_name = table_name,
        .group_id = group_id,
        .store_id = store_id,
        .node_id = node_id,
        .updated_at_ns = updated_at_ns,
        .source = source,
        .freshness = freshness,
        .topology_generation = topology_generation,
        .lsm_root_generation = lsm_root_generation,
        .status_generation = status_generation,
        .doc_count = doc_count,
        .disk_bytes = disk_bytes,
        .created_at_millis = created_at_millis,
        .index_count = index_count,
        .enrichment_enabled = enrichment_enabled,
        .enrichment_target_sequence = enrichment_target_sequence,
        .enrichment_applied_sequence = enrichment_applied_sequence,
        .enrichment_retrying = enrichment_retrying,
        .enrichment_worker_failed = enrichment_worker_failed,
        .async_indexing_active = async_indexing_active,
        .async_startup_active = async_startup_active,
        .async_dense_catch_up_active = async_dense_catch_up_active,
        .async_bulk_coalescing_active = async_bulk_coalescing_active,
        .doc_identity = doc_identity,
        .doc_set_planning = doc_set_planning,
        .indexes = indexes,
    };
}

fn appendRuntimeDocIdentityStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeDocIdentityStatusReport,
) !void {
    try appendInt(alloc, out, u64, record.namespace_table_id);
    try appendInt(alloc, out, u64, record.namespace_shard_id);
    try appendInt(alloc, out, u64, record.namespace_range_id);
    try appendInt(alloc, out, u32, record.next_ordinal);
    try appendInt(alloc, out, u64, record.allocated_ordinals);
    try appendInt(alloc, out, u64, record.ordinal_capacity_remaining);
    try out.append(alloc, if (record.ordinal_capacity_exhausted) 1 else 0);
    try out.append(alloc, if (record.rebuild_required) 1 else 0);
    try appendInt(alloc, out, u64, record.state_rows);
    try appendInt(alloc, out, u64, record.live_ordinals);
    try appendInt(alloc, out, u64, record.tombstone_ordinals);
    try appendInt(alloc, out, u64, record.min_created_generation);
    try appendInt(alloc, out, u64, record.max_created_generation);
    try appendInt(alloc, out, u64, record.min_deleted_generation);
    try appendInt(alloc, out, u64, record.max_deleted_generation);
    try appendInt(alloc, out, u64, record.scanned_primary_docs);
    try appendInt(alloc, out, u64, record.primary_docs_missing_ordinals);
    try appendInt(alloc, out, u64, record.primary_docs_missing_identity_state);
    try appendInt(alloc, out, u64, record.primary_docs_with_tombstone_ordinals);
    try out.append(alloc, if (record.complete) 1 else 0);
}

fn readRuntimeDocIdentityStatusRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.RuntimeDocIdentityStatusReport {
    const namespace_table_id = try readInt(encoded, pos, u64);
    const namespace_shard_id = try readInt(encoded, pos, u64);
    const namespace_range_id = try readInt(encoded, pos, u64);
    const next_ordinal = try readInt(encoded, pos, u32);
    const allocated_ordinals = try readInt(encoded, pos, u64);
    const ordinal_capacity_remaining = try readInt(encoded, pos, u64);
    if (pos.* + 2 > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const ordinal_capacity_exhausted = encoded[pos.*] != 0;
    pos.* += 1;
    const rebuild_required = encoded[pos.*] != 0;
    pos.* += 1;
    const state_rows = try readInt(encoded, pos, u64);
    const live_ordinals = try readInt(encoded, pos, u64);
    const tombstone_ordinals = try readInt(encoded, pos, u64);
    const min_created_generation = try readInt(encoded, pos, u64);
    const max_created_generation = try readInt(encoded, pos, u64);
    const min_deleted_generation = try readInt(encoded, pos, u64);
    const max_deleted_generation = try readInt(encoded, pos, u64);
    const scanned_primary_docs = try readInt(encoded, pos, u64);
    const primary_docs_missing_ordinals = try readInt(encoded, pos, u64);
    const primary_docs_missing_identity_state = try readInt(encoded, pos, u64);
    const primary_docs_with_tombstone_ordinals = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const complete = encoded[pos.*] != 0;
    pos.* += 1;
    return .{
        .namespace_table_id = namespace_table_id,
        .namespace_shard_id = namespace_shard_id,
        .namespace_range_id = namespace_range_id,
        .next_ordinal = next_ordinal,
        .allocated_ordinals = allocated_ordinals,
        .ordinal_capacity_remaining = ordinal_capacity_remaining,
        .ordinal_capacity_exhausted = ordinal_capacity_exhausted,
        .rebuild_required = rebuild_required,
        .state_rows = state_rows,
        .live_ordinals = live_ordinals,
        .tombstone_ordinals = tombstone_ordinals,
        .min_created_generation = min_created_generation,
        .max_created_generation = max_created_generation,
        .min_deleted_generation = min_deleted_generation,
        .max_deleted_generation = max_deleted_generation,
        .scanned_primary_docs = scanned_primary_docs,
        .primary_docs_missing_ordinals = primary_docs_missing_ordinals,
        .primary_docs_missing_identity_state = primary_docs_missing_identity_state,
        .primary_docs_with_tombstone_ordinals = primary_docs_with_tombstone_ordinals,
        .complete = complete,
    };
}

fn appendRuntimeDocSetPlanningStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeDocSetPlanningStatusReport,
) !void {
    try appendInt(alloc, out, u64, record.resolved_set_count);
    try appendInt(alloc, out, u64, record.all_set_count);
    try appendInt(alloc, out, u64, record.none_set_count);
    try appendInt(alloc, out, u64, record.doc_key_list_count);
    try appendInt(alloc, out, u64, record.ordinal_list_count);
    try appendInt(alloc, out, u64, record.ordinal_bitmap_count);
    try appendInt(alloc, out, u64, record.doc_key_list_docs);
    try appendInt(alloc, out, u64, record.ordinal_list_docs);
    try appendInt(alloc, out, u64, record.ordinal_bitmap_docs);
    try appendInt(alloc, out, u64, record.missing_ordinal_coverage_count);
    try appendInt(alloc, out, u64, record.bitmap_promotion_count);
    try appendInt(alloc, out, u64, record.unsupported_filter_shape_count);
    try appendInt(alloc, out, u64, record.stale_identity_generation_rejection_count);
}

fn readRuntimeDocSetPlanningStatusRecord(
    encoded: []const u8,
    pos: *usize,
    version: u16,
) !metadata.RuntimeDocSetPlanningStatusReport {
    return .{
        .resolved_set_count = try readInt(encoded, pos, u64),
        .all_set_count = try readInt(encoded, pos, u64),
        .none_set_count = try readInt(encoded, pos, u64),
        .doc_key_list_count = try readInt(encoded, pos, u64),
        .ordinal_list_count = try readInt(encoded, pos, u64),
        .ordinal_bitmap_count = try readInt(encoded, pos, u64),
        .doc_key_list_docs = try readInt(encoded, pos, u64),
        .ordinal_list_docs = try readInt(encoded, pos, u64),
        .ordinal_bitmap_docs = try readInt(encoded, pos, u64),
        .missing_ordinal_coverage_count = try readInt(encoded, pos, u64),
        .bitmap_promotion_count = try readInt(encoded, pos, u64),
        .unsupported_filter_shape_count = try readInt(encoded, pos, u64),
        .stale_identity_generation_rejection_count = if (version >= 4) try readInt(encoded, pos, u64) else 0,
    };
}

fn appendRuntimeIndexStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RuntimeIndexStatusReport,
) !void {
    try appendRequiredString(alloc, out, record.name);
    try appendRequiredString(alloc, out, record.kind);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.term_count);
    try appendInt(alloc, out, u64, record.edge_count);
    try appendInt(alloc, out, u64, record.node_count);
    try appendInt(alloc, out, u64, record.root_node);
    try out.append(alloc, if (record.backfill_active) 1 else 0);
    try appendInt(alloc, out, u16, record.backfill_progress_millis);
    try appendInt(alloc, out, u64, record.replay_applied_sequence);
    try appendInt(alloc, out, u64, record.replay_target_sequence);
    try out.append(alloc, if (record.replay_catch_up_required) 1 else 0);
}

fn readRuntimeIndexStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RuntimeIndexStatusReport {
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const kind = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(kind);
    const doc_count = try readInt(encoded, pos, u64);
    const term_count = try readInt(encoded, pos, u64);
    const edge_count = try readInt(encoded, pos, u64);
    const node_count = try readInt(encoded, pos, u64);
    const root_node = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const backfill_active = encoded[pos.*] != 0;
    pos.* += 1;
    const backfill_progress_millis = try readInt(encoded, pos, u16);
    const replay_applied_sequence = try readInt(encoded, pos, u64);
    const replay_target_sequence = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const replay_catch_up_required = encoded[pos.*] != 0;
    pos.* += 1;
    return .{
        .name = name,
        .kind = kind,
        .doc_count = doc_count,
        .term_count = term_count,
        .edge_count = edge_count,
        .node_count = node_count,
        .root_node = root_node,
        .backfill_active = backfill_active,
        .backfill_progress_millis = backfill_progress_millis,
        .replay_applied_sequence = replay_applied_sequence,
        .replay_target_sequence = replay_target_sequence,
        .replay_catch_up_required = replay_catch_up_required,
    };
}

fn appendGroupStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.GroupStatusReport,
) !void {
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.doc_count);
    try appendInt(alloc, out, u64, record.disk_bytes);
    try out.append(alloc, if (record.empty) 1 else 0);
    try appendInt(alloc, out, u32, 0);
    try appendInt(alloc, out, u64, record.updated_at_millis);
    try out.append(alloc, if (record.local_leader) 1 else 0);
    try appendInt(alloc, out, u64, record.created_at_millis);
    try out.append(alloc, if (record.transition_pending) 1 else 0);
    try out.append(alloc, if (record.replay_required) 1 else 0);
    try out.append(alloc, if (record.replay_caught_up) 1 else 0);
    try out.append(alloc, if (record.cutover_ready) 1 else 0);
    try out.append(alloc, if (record.reads_ready_after_cutover) 1 else 0);
    try out.append(alloc, if (record.local_voter) 1 else 0);
    try appendInt(alloc, out, u16, record.voter_count);
    try out.append(alloc, if (record.joint_consensus) 1 else 0);
}

fn readGroupStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.GroupStatusReport {
    _ = alloc;
    const group_id = try readInt(encoded, pos, u64);
    const doc_count = try readInt(encoded, pos, u64);
    const disk_bytes = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const empty = encoded[pos.*] != 0;
    pos.* += 1;
    const median_key_len = try readInt(encoded, pos, u32);
    if (pos.* + median_key_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    pos.* += median_key_len;
    const updated_at_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    const local_leader = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const created_at_millis = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    const transition_pending = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const replay_required = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const replay_caught_up = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const cutover_ready = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const reads_ready_after_cutover = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const local_voter = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    const voter_count = if (pos.* + @sizeOf(u16) <= encoded.len)
        try readInt(encoded, pos, u16)
    else
        0;
    const joint_consensus = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    return .{
        .group_id = group_id,
        .doc_count = doc_count,
        .disk_bytes = disk_bytes,
        .empty = empty,
        .created_at_millis = created_at_millis,
        .updated_at_millis = updated_at_millis,
        .local_leader = local_leader,
        .local_voter = local_voter,
        .voter_count = voter_count,
        .joint_consensus = joint_consensus,
        .transition_pending = transition_pending,
        .replay_required = replay_required,
        .replay_caught_up = replay_caught_up,
        .cutover_ready = cutover_ready,
        .reads_ready_after_cutover = reads_ready_after_cutover,
    };
}

fn appendPlacementIntent(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    intent: raft_reconciler.PlacementIntent,
) !void {
    try appendInt(alloc, out, u64, intent.record.group_id);
    try appendInt(alloc, out, u64, intent.record.replica_id);
    try appendInt(alloc, out, u64, intent.record.local_node_id);
    try appendInt(alloc, out, u64, intent.record.metadata_version);
    try out.append(alloc, @intFromEnum(intent.record.bootstrap_mode));
    try appendInt(alloc, out, u32, @intCast(intent.peer_node_ids.len));
    for (intent.peer_node_ids) |node_id| try appendInt(alloc, out, u64, node_id);
    try appendInt(alloc, out, u64, intent.store_id);
    const source_tag: u8 = if (intent.record.snapshot_bootstrap != null)
        1
    else if (intent.record.backup_restore_bootstrap != null)
        2
    else
        0;
    try out.append(alloc, source_tag);
    switch (source_tag) {
        1 => {
            const snapshot = intent.record.snapshot_bootstrap.?;
            try appendInt(alloc, out, u64, snapshot.from_node_id);
            try appendInt(alloc, out, u64, snapshot.term);
            try appendInt(alloc, out, u32, @intCast(snapshot.snapshot_id.len));
            try out.appendSlice(alloc, snapshot.snapshot_id);
            try appendInt(alloc, out, u32, @intCast(snapshot.uri.len));
            try out.appendSlice(alloc, snapshot.uri);
        },
        2 => {
            const backup = intent.record.backup_restore_bootstrap.?;
            try appendInt(alloc, out, u32, @intCast(backup.backup_id.len));
            try out.appendSlice(alloc, backup.backup_id);
            try appendInt(alloc, out, u32, @intCast(backup.location.len));
            try out.appendSlice(alloc, backup.location);
            try appendInt(alloc, out, u32, @intCast(backup.snapshot_path.len));
            try out.appendSlice(alloc, backup.snapshot_path);
        },
        else => {},
    }
}

fn appendTableRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.TableRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u16, record.desired_replica_count);
    try appendInt(alloc, out, u32, record.min_ranges);
    try appendInt(alloc, out, u32, @intCast(record.name.len));
    try out.appendSlice(alloc, record.name);
    try appendInt(alloc, out, u32, @intCast(record.description.len));
    try out.appendSlice(alloc, record.description);
    try appendInt(alloc, out, u32, @intCast(record.schema_json.len));
    try out.appendSlice(alloc, record.schema_json);
    try appendInt(alloc, out, u32, @intCast(record.read_schema_json.len));
    try out.appendSlice(alloc, record.read_schema_json);
    try appendInt(alloc, out, u32, @intCast(record.indexes_json.len));
    try out.appendSlice(alloc, record.indexes_json);
    try appendInt(alloc, out, u32, @intCast(record.replication_sources_json.len));
    try out.appendSlice(alloc, record.replication_sources_json);
    try appendInt(alloc, out, u32, @intCast(record.placement_role.len));
    try out.appendSlice(alloc, record.placement_role);
    try appendInt(alloc, out, u32, @intCast(record.restore_backup_id.len));
    try out.appendSlice(alloc, record.restore_backup_id);
    try appendInt(alloc, out, u32, @intCast(record.restore_location.len));
    try out.appendSlice(alloc, record.restore_location);
}

fn appendSchemaProgressRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SchemaProgressRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u32, record.schema_version);
}

fn appendRestoreProgressRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RestoreProgressRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u64, record.node_id);
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u32, @intCast(record.backup_id.len));
    try out.appendSlice(alloc, record.backup_id);
    try appendInt(alloc, out, u32, @intCast(record.snapshot_path.len));
    try out.appendSlice(alloc, record.snapshot_path);
    try out.append(alloc, if (record.primary_restored) 1 else 0);
    try out.append(alloc, if (record.runtime_repair_complete) 1 else 0);
    try appendInt(alloc, out, u32, @intCast(record.phase.len));
    try out.appendSlice(alloc, record.phase);
    try appendInt(alloc, out, u32, @intCast(record.last_error.len));
    try out.appendSlice(alloc, record.last_error);
    try appendInt(alloc, out, u64, record.updated_at_ms);
}

fn appendReplicationSourceStatusRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReplicationSourceStatusRecord,
) !void {
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u32, record.source_ordinal);
    try appendInt(alloc, out, u32, @intCast(record.source_kind.len));
    try out.appendSlice(alloc, record.source_kind);
    try appendInt(alloc, out, u32, @intCast(record.external_table.len));
    try out.appendSlice(alloc, record.external_table);
    try appendInt(alloc, out, u32, @intCast(record.slot_name.len));
    try out.appendSlice(alloc, record.slot_name);
    try appendInt(alloc, out, u32, @intCast(record.publication_name.len));
    try out.appendSlice(alloc, record.publication_name);
    try appendInt(alloc, out, u32, @intCast(record.phase.len));
    try out.appendSlice(alloc, record.phase);
    try appendInt(alloc, out, u32, @intCast(record.checkpoint.len));
    try out.appendSlice(alloc, record.checkpoint);
    try appendInt(alloc, out, u64, record.snapshot_offset);
    try appendInt(alloc, out, u32, @intCast(record.stream_checkpoint.len));
    try out.appendSlice(alloc, record.stream_checkpoint);
    try appendInt(alloc, out, u32, @intCast(record.last_error.len));
    try out.appendSlice(alloc, record.last_error);
    try appendInt(alloc, out, u64, record.lag_records);
    try appendInt(alloc, out, u64, record.updated_at_ms);
    try appendInt(alloc, out, u32, @intCast(record.prepared_checkpoint.len));
    try out.appendSlice(alloc, record.prepared_checkpoint);
    try appendInt(alloc, out, u32, @intCast(record.cutover_mode.len));
    try out.appendSlice(alloc, record.cutover_mode);
    try appendInt(alloc, out, u64, record.consecutive_failures);
    try appendInt(alloc, out, u64, record.last_success_at_ms);
    try appendInt(alloc, out, u64, record.last_change_applied_at_ms);
    try appendInt(alloc, out, u32, @intCast(record.failure_class.len));
    try out.appendSlice(alloc, record.failure_class);
    try appendInt(alloc, out, u64, record.lag_millis);
    try appendInt(alloc, out, u64, record.last_source_commit_at_ms);
}

fn appendRangeRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.RangeRecord,
) !void {
    try appendInt(alloc, out, u64, record.group_id);
    try appendInt(alloc, out, u64, record.table_id);
    try appendInt(alloc, out, u32, @intCast(record.start_key.len));
    try out.appendSlice(alloc, record.start_key);
    if (record.end_key) |end| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(end.len));
        try out.appendSlice(alloc, end);
    } else {
        try out.append(alloc, 0);
    }
    try appendInt(alloc, out, u32, @intCast(record.restore_backup_id.len));
    try out.appendSlice(alloc, record.restore_backup_id);
    try appendInt(alloc, out, u32, @intCast(record.restore_location.len));
    try out.appendSlice(alloc, record.restore_location);
    try appendInt(alloc, out, u32, @intCast(record.restore_snapshot_path.len));
    try out.appendSlice(alloc, record.restore_snapshot_path);
    const range_id = if (record.range_id == 0) record.group_id else record.range_id;
    try appendInt(alloc, out, u64, range_id);
    try appendInt(alloc, out, u64, record.doc_identity_shard_id);
    try appendInt(alloc, out, u64, record.doc_identity_range_id);
}

fn appendSplitTransitionRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.SplitTransitionRecord,
) !void {
    try appendInt(alloc, out, u64, record.transition_id);
    try appendInt(alloc, out, u64, record.source_group_id);
    try appendInt(alloc, out, u64, record.destination_group_id);
    try out.append(alloc, @intFromEnum(record.phase));
    if (record.split_key) |split_key| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(split_key.len));
        try out.appendSlice(alloc, split_key);
    } else {
        try out.append(alloc, 0);
    }
    if (record.source_range_end) |end| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(end.len));
        try out.appendSlice(alloc, end);
    } else {
        try out.append(alloc, 0);
    }
    if (record.rollback_reason) |reason| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(reason.len));
        try out.appendSlice(alloc, reason);
    } else {
        try out.append(alloc, 0);
    }
}

fn appendMergeTransitionRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.MergeTransitionRecord,
) !void {
    try appendInt(alloc, out, u64, record.transition_id);
    try appendInt(alloc, out, u64, record.donor_group_id);
    try appendInt(alloc, out, u64, record.receiver_group_id);
    try out.append(alloc, @intFromEnum(record.phase));
    if (record.rollback_reason) |reason| {
        try out.append(alloc, 1);
        try appendInt(alloc, out, u32, @intCast(reason.len));
        try out.appendSlice(alloc, reason);
    } else {
        try out.append(alloc, 0);
    }
    try out.append(alloc, if (record.allow_doc_identity_reassignment) 1 else 0);
}

fn appendReconcileLeaseRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReconcileLeaseRecord,
) !void {
    try appendInt(alloc, out, u64, record.owner_node_id);
    try appendInt(alloc, out, u64, record.expires_at_ms);
}

fn appendShuffleJoinLeaseRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ShuffleJoinLeaseRecord,
) !void {
    try appendInt(alloc, out, u64, record.job_id);
    try appendInt(alloc, out, u64, record.owner_group_id);
    try appendInt(alloc, out, u64, record.expires_at_ms);
}

fn appendReallocationRequestRecord(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    record: metadata.ReallocationRequestRecord,
) !void {
    try appendInt(alloc, out, u64, record.requested_at_ms);
}

fn readTableRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const start = pos.*;
    const newest_record = readTableRecordWithRestoreIntent(alloc, encoded, pos) catch null;
    if (newest_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const old_record = readTableRecordLegacy(alloc, encoded, pos) catch null;
    if (old_record) |record| {
        if (pos.* == encoded.len) return record;
        metadata_table_manager.freeTable(alloc, record);
        pos.* = start;
    } else {
        pos.* = start;
    }

    const record = try readTableRecordWithReadSchema(alloc, encoded, pos);
    if (pos.* != encoded.len) {
        metadata_table_manager.freeTable(alloc, record);
        return error.InvalidMetadataTransitionEncoding;
    }
    return record;
}

fn readTableRecordLegacy(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    return .{
        .table_id = table_id,
        .name = name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = try alloc.dupe(u8, ""),
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = try alloc.dupe(u8, ""),
        .restore_location = try alloc.dupe(u8, ""),
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readTableRecordWithReadSchema(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const read_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(read_schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    return .{
        .table_id = table_id,
        .name = name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = try alloc.dupe(u8, ""),
        .restore_location = try alloc.dupe(u8, ""),
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readTableRecordWithRestoreIntent(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.TableRecord {
    const table_id = try readInt(encoded, pos, u64);
    const desired_replica_count = try readInt(encoded, pos, u16);
    const min_ranges = try readInt(encoded, pos, u32);
    const name = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(name);
    const description = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(description);
    const schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(schema_json);
    const read_schema_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(read_schema_json);
    const indexes_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(indexes_json);
    const replication_sources_json = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(replication_sources_json);
    const placement_role = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(placement_role);
    const restore_backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_backup_id);
    const restore_location = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(restore_location);
    return .{
        .table_id = table_id,
        .name = name,
        .description = description,
        .schema_json = schema_json,
        .read_schema_json = read_schema_json,
        .indexes_json = indexes_json,
        .replication_sources_json = replication_sources_json,
        .placement_role = placement_role,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .desired_replica_count = desired_replica_count,
        .min_ranges = min_ranges,
    };
}

fn readRequiredString(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) ![]u8 {
    const value_len = try readInt(encoded, pos, u32);
    if (pos.* + value_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const value = try alloc.dupe(u8, encoded[pos.* .. pos.* + value_len]);
    pos.* += value_len;
    return value;
}

fn appendRequiredString(
    alloc: std.mem.Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    try appendInt(alloc, out, u32, @intCast(value.len));
    try out.appendSlice(alloc, value);
}

fn readRangeRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RangeRecord {
    const group_id = try readInt(encoded, pos, u64);
    const table_id = try readInt(encoded, pos, u64);
    const start_len = try readInt(encoded, pos, u32);
    if (pos.* + start_len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const start_key = try alloc.dupe(u8, encoded[pos.* .. pos.* + start_len]);
    pos.* += start_len;
    const end_key = try readOptionalString(alloc, encoded, pos);
    const restore_backup_id = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_backup_id);
    const restore_location = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_location);
    const restore_snapshot_path = if (pos.* < encoded.len)
        try readRequiredString(alloc, encoded, pos)
    else
        try alloc.dupe(u8, "");
    errdefer alloc.free(restore_snapshot_path);
    const range_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        group_id;
    const doc_identity_shard_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    const doc_identity_range_id = if (pos.* < encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    return .{
        .group_id = group_id,
        .range_id = if (range_id == 0) group_id else range_id,
        .table_id = table_id,
        .start_key = start_key,
        .end_key = end_key,
        .doc_identity_shard_id = doc_identity_shard_id,
        .doc_identity_range_id = doc_identity_range_id,
        .restore_backup_id = restore_backup_id,
        .restore_location = restore_location,
        .restore_snapshot_path = restore_snapshot_path,
    };
}

fn readSchemaProgressRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.SchemaProgressRecord {
    return .{
        .table_id = try readInt(encoded, pos, u64),
        .node_id = try readInt(encoded, pos, u64),
        .schema_version = try readInt(encoded, pos, u32),
    };
}

fn readRestoreProgressRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.RestoreProgressRecord {
    const table_id = try readInt(encoded, pos, u64);
    const node_id = try readInt(encoded, pos, u64);
    const group_id = try readInt(encoded, pos, u64);
    const backup_id = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(backup_id);
    const snapshot_path = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(snapshot_path);
    if (pos.* >= encoded.len) return error.InvalidRestoreProgressRecord;
    const primary_restored = switch (encoded[pos.*]) {
        0 => false,
        1 => true,
        else => return error.InvalidRestoreProgressRecord,
    };
    pos.* += 1;
    if (pos.* >= encoded.len) return error.InvalidRestoreProgressRecord;
    const runtime_repair_complete = switch (encoded[pos.*]) {
        0 => false,
        1 => true,
        else => return error.InvalidRestoreProgressRecord,
    };
    pos.* += 1;
    const phase = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(phase);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const updated_at_ms = try readInt(encoded, pos, u64);
    return .{
        .table_id = table_id,
        .node_id = node_id,
        .group_id = group_id,
        .backup_id = backup_id,
        .snapshot_path = snapshot_path,
        .primary_restored = primary_restored,
        .runtime_repair_complete = runtime_repair_complete,
        .phase = phase,
        .last_error = last_error,
        .updated_at_ms = updated_at_ms,
    };
}

fn readReplicationSourceStatusRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.ReplicationSourceStatusRecord {
    const table_id = try readInt(encoded, pos, u64);
    const source_ordinal = try readInt(encoded, pos, u32);
    const source_kind = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(source_kind);
    const external_table = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(external_table);
    const slot_name = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(slot_name);
    const publication_name = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(publication_name);
    const phase = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(phase);
    const checkpoint = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(checkpoint);
    const snapshot_offset = if (pos.* + @sizeOf(u64) <= encoded.len)
        try readInt(encoded, pos, u64)
    else
        0;
    const stream_checkpoint = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(stream_checkpoint);
    const last_error = try readRequiredString(alloc, encoded, pos);
    errdefer alloc.free(last_error);
    const lag_records = try readInt(encoded, pos, u64);
    const updated_at_ms = try readInt(encoded, pos, u64);
    const prepared_checkpoint = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(prepared_checkpoint);
    const cutover_mode = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(cutover_mode);
    const consecutive_failures = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_success_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_change_applied_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const failure_class = if (pos.* < encoded.len) try readRequiredString(alloc, encoded, pos) else try alloc.dupe(u8, "");
    errdefer alloc.free(failure_class);
    const lag_millis = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    const last_source_commit_at_ms = if (pos.* + @sizeOf(u64) <= encoded.len) try readInt(encoded, pos, u64) else 0;
    return .{
        .table_id = table_id,
        .source_ordinal = source_ordinal,
        .source_kind = source_kind,
        .external_table = external_table,
        .cutover_mode = cutover_mode,
        .slot_name = slot_name,
        .publication_name = publication_name,
        .phase = phase,
        .checkpoint = checkpoint,
        .snapshot_offset = snapshot_offset,
        .prepared_checkpoint = prepared_checkpoint,
        .stream_checkpoint = stream_checkpoint,
        .last_error = last_error,
        .failure_class = failure_class,
        .lag_records = lag_records,
        .lag_millis = lag_millis,
        .consecutive_failures = consecutive_failures,
        .last_source_commit_at_ms = last_source_commit_at_ms,
        .last_success_at_ms = last_success_at_ms,
        .last_change_applied_at_ms = last_change_applied_at_ms,
        .updated_at_ms = updated_at_ms,
    };
}

fn readPlacementIntent(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !raft_reconciler.PlacementIntent {
    const group_id = try readInt(encoded, pos, u64);
    const replica_id = try readInt(encoded, pos, u64);
    const local_node_id = try readInt(encoded, pos, u64);
    const metadata_version = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const bootstrap_mode: raft_catalog.ReplicaBootstrapMode = @enumFromInt(encoded[pos.*]);
    pos.* += 1;
    const peer_count = try readInt(encoded, pos, u32);
    const peer_node_ids = try alloc.alloc(u64, peer_count);
    errdefer alloc.free(peer_node_ids);
    for (peer_node_ids) |*node_id| node_id.* = try readInt(encoded, pos, u64);
    const store_id = if (pos.* < encoded.len) try readInt(encoded, pos, u64) else 0;
    var snapshot_bootstrap: ?raft_catalog.SnapshotBootstrapRecord = null;
    var backup_restore_bootstrap: ?raft_catalog.BackupRestoreBootstrapRecord = null;
    if (pos.* < encoded.len) {
        const source_tag = encoded[pos.*];
        pos.* += 1;
        switch (source_tag) {
            0 => {},
            1 => {
                const from_node_id = try readInt(encoded, pos, u64);
                const term = try readInt(encoded, pos, u64);
                const snapshot_id = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(snapshot_id);
                const uri = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(uri);
                snapshot_bootstrap = .{
                    .from_node_id = from_node_id,
                    .term = term,
                    .snapshot_id = snapshot_id,
                    .uri = uri,
                };
            },
            2 => {
                const backup_id = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(backup_id);
                const location = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(location);
                const snapshot_path = try readRequiredString(alloc, encoded, pos);
                errdefer alloc.free(snapshot_path);
                backup_restore_bootstrap = .{
                    .backup_id = backup_id,
                    .location = location,
                    .snapshot_path = snapshot_path,
                };
            },
            else => return error.InvalidMetadataTransitionEncoding,
        }
    }
    return .{
        .record = .{
            .group_id = group_id,
            .replica_id = replica_id,
            .local_node_id = local_node_id,
            .metadata_version = metadata_version,
            .bootstrap_mode = bootstrap_mode,
            .snapshot_bootstrap = snapshot_bootstrap,
            .backup_restore_bootstrap = backup_restore_bootstrap,
        },
        .store_id = store_id,
        .peer_node_ids = peer_node_ids,
    };
}

fn readSplitTransitionRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.SplitTransitionRecord {
    const transition_id = try readInt(encoded, pos, u64);
    const source_group_id = try readInt(encoded, pos, u64);
    const destination_group_id = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const phase: metadata.TransitionPhase = @enumFromInt(encoded[pos.*]);
    pos.* += 1;
    const split_key = try readOptionalString(alloc, encoded, pos);
    const source_range_end = try readOptionalString(alloc, encoded, pos);
    const rollback_reason = try readOptionalString(alloc, encoded, pos);
    return .{
        .transition_id = transition_id,
        .source_group_id = source_group_id,
        .destination_group_id = destination_group_id,
        .phase = phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
    };
}

fn readMergeTransitionRecord(
    alloc: std.mem.Allocator,
    encoded: []const u8,
    pos: *usize,
) !metadata.MergeTransitionRecord {
    const transition_id = try readInt(encoded, pos, u64);
    const donor_group_id = try readInt(encoded, pos, u64);
    const receiver_group_id = try readInt(encoded, pos, u64);
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const phase: metadata.TransitionPhase = @enumFromInt(encoded[pos.*]);
    pos.* += 1;
    const rollback_reason = try readOptionalString(alloc, encoded, pos);
    const allow_doc_identity_reassignment = if (pos.* < encoded.len) blk: {
        const value = encoded[pos.*] != 0;
        pos.* += 1;
        break :blk value;
    } else false;
    return .{
        .transition_id = transition_id,
        .donor_group_id = donor_group_id,
        .receiver_group_id = receiver_group_id,
        .phase = phase,
        .rollback_reason = rollback_reason,
        .allow_doc_identity_reassignment = allow_doc_identity_reassignment,
    };
}

fn readReconcileLeaseRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ReconcileLeaseRecord {
    return .{
        .owner_node_id = try readInt(encoded, pos, u64),
        .expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readShuffleJoinLeaseRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ShuffleJoinLeaseRecord {
    return .{
        .job_id = try readInt(encoded, pos, u64),
        .owner_group_id = try readInt(encoded, pos, u64),
        .expires_at_ms = try readInt(encoded, pos, u64),
    };
}

fn readReallocationRequestRecord(
    encoded: []const u8,
    pos: *usize,
) !metadata.ReallocationRequestRecord {
    return .{
        .requested_at_ms = try readInt(encoded, pos, u64),
    };
}

fn appendInt(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(alloc, &bytes);
}

fn readInt(encoded: []const u8, pos: *usize, comptime T: type) !T {
    if (pos.* + @sizeOf(T) > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const bytes: *const [@sizeOf(T)]u8 = @ptrCast(encoded[pos.* .. pos.* + @sizeOf(T)]);
    const value = std.mem.readInt(T, bytes, .little);
    pos.* += @sizeOf(T);
    return value;
}

fn readOptionalString(alloc: std.mem.Allocator, encoded: []const u8, pos: *usize) !?[]u8 {
    if (pos.* >= encoded.len) return error.InvalidMetadataTransitionEncoding;
    const present = encoded[pos.*];
    pos.* += 1;
    if (present == 0) return null;
    const len = try readInt(encoded, pos, u32);
    if (pos.* + len > encoded.len) return error.InvalidMetadataTransitionEncoding;
    const reason = try alloc.dupe(u8, encoded[pos.* .. pos.* + len]);
    pos.* += len;
    return reason;
}

pub fn splitTransitionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:split:{d}:", .{group_id});
}

pub fn placementPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_placement:{d}:", .{group_id});
}

pub fn nodePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_node:{d}:", .{group_id});
}

pub fn storePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_store:{d}:", .{group_id});
}

pub fn tablePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table:{d}:", .{group_id});
}

pub fn schemaProgressPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_schema_progress:{d}:", .{group_id});
}

pub fn restoreProgressPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_restore_progress:{d}:", .{group_id});
}

pub fn replicationSourceStatusPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_replication_source_status:{d}:", .{group_id});
}

pub fn shuffleJoinLeasePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_shuffle_join_lease:{d}:", .{group_id});
}

pub fn rangePrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_range:{d}:", .{group_id});
}

pub fn mergeTransitionPrefixForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:merge:{d}:", .{group_id});
}

pub fn reconcileLeaseKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_reconcile_lease:{d}", .{group_id});
}

pub fn reallocationRequestKeyForGroup(buf: []u8, group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_reallocation_request:{d}", .{group_id});
}

pub fn shuffleJoinLeaseKeyForGroup(buf: []u8, group_id: u64, job_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_shuffle_join_lease:{d}:{d}", .{ group_id, job_id });
}

fn splitTransitionKeyForGroup(buf: []u8, group_id: u64, transition_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:split:{d}:{d}", .{ group_id, transition_id });
}

fn mergeTransitionKeyForGroup(buf: []u8, group_id: u64, transition_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_transition:merge:{d}:{d}", .{ group_id, transition_id });
}

fn tableKeyForGroup(buf: []u8, group_id: u64, table_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_table:{d}:{d}", .{ group_id, table_id });
}

fn schemaProgressKeyForGroup(buf: []u8, group_id: u64, table_id: u64, node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_schema_progress:{d}:{d}:{d}", .{ group_id, table_id, node_id });
}

fn restoreProgressKeyForGroup(buf: []u8, group_id: u64, table_id: u64, node_id: u64, range_group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_restore_progress:{d}:{d}:{d}:{d}", .{ group_id, table_id, node_id, range_group_id });
}

fn replicationSourceStatusKeyForGroup(buf: []u8, group_id: u64, table_id: u64, source_ordinal: u32) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_replication_source_status:{d}:{d}:{d}", .{ group_id, table_id, source_ordinal });
}

fn rangeKeyForGroup(buf: []u8, group_id: u64, range_group_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_range:{d}:{d}", .{ group_id, range_group_id });
}

fn placementKeyForGroup(buf: []u8, group_id: u64, range_group_id: u64, local_node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_placement:{d}:{d}:{d}", .{ group_id, range_group_id, local_node_id });
}

fn nodeKeyForGroup(buf: []u8, group_id: u64, node_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_node:{d}:{d}", .{ group_id, node_id });
}

fn storeKeyForGroup(buf: []u8, group_id: u64, store_id: u64) ![]const u8 {
    return try std.fmt.bufPrint(buf, "\x00\x00__metadata__:metadata_store:{d}:{d}", .{ group_id, store_id });
}

test "metadata raft apply store persists batches across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-apply-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 13,
            .entries_bytes = "metadata-batch",
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const batch = (try store.latestBatch(21)) orelse return error.MissingMetadataBatch;
        try std.testing.expectEqual(@as(u64, 13), batch.commit_index);
        try std.testing.expectEqualStrings("metadata-batch", batch.entries_bytes);
    }
}

test "metadata raft apply store projects transition records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-transition-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 501,
            .source_group_id = 21,
            .destination_group_id = 22,
            .phase = .bootstrap_peer,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
            .rollback_reason = "slow-peer",
        },
    });
    defer std.testing.allocator.free(split_cmd);
    const merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = 31,
            .receiver_group_id = 30,
            .phase = .replay_deltas,
            .allow_doc_identity_reassignment = true,
        },
    });
    defer std.testing.allocator.free(merge_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 7, .entry_type = .normal, .data = split_cmd },
        .{ .term = 1, .index = 8, .entry_type = .normal, .data = merge_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 8,
            .entries_bytes = encoded_entries,
        });

        const splits = try store.listSplitTransitions(std.testing.allocator, 21);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        const merges = try store.listMergeTransitions(std.testing.allocator, 21);
        defer store.freeMergeTransitions(std.testing.allocator, merges);

        try std.testing.expectEqual(@as(usize, 1), splits.len);
        try std.testing.expectEqual(@as(u64, 501), splits[0].transition_id);
        try std.testing.expectEqualStrings("doc:m", splits[0].split_key.?);
        try std.testing.expectEqualStrings("doc:z", splits[0].source_range_end.?);
        try std.testing.expectEqualStrings("slow-peer", splits[0].rollback_reason.?);
        try std.testing.expectEqual(@as(usize, 1), merges.len);
        try std.testing.expectEqual(@as(u64, 601), merges[0].transition_id);
        try std.testing.expect(merges[0].allow_doc_identity_reassignment);
    }

    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_split_transition = .{ .transition_id = 501 },
    });
    defer std.testing.allocator.free(remove_cmd);
    const encoded_remove = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 9, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(encoded_remove);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 21,
            .commit_index = 9,
            .entries_bytes = encoded_remove,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const splits = try store.listSplitTransitions(std.testing.allocator, 21);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        const merges = try store.listMergeTransitions(std.testing.allocator, 21);
        defer store.freeMergeTransitions(std.testing.allocator, merges);

        try std.testing.expectEqual(@as(usize, 0), splits.len);
        try std.testing.expectEqual(@as(usize, 1), merges.len);
        try std.testing.expectEqual(@as(u64, 601), merges[0].transition_id);
        try std.testing.expect(merges[0].allow_doc_identity_reassignment);
    }
}

test "metadata raft apply store resolves stale store drain intent at apply time" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 9 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const draining_registration_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(draining_registration_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 9 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 9, .node_id = 9, .role = "data", .health_class = "healthy", .live = true, .drain_requested = true },
    });
    defer std.testing.allocator.free(stale_draining_store_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = draining_registration_cmd },
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = stale_draining_store_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 21,
        .commit_index = 4,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 21);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 21);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expect(!stores[0].drain_requested);
}

test "metadata raft apply store ignores stale drained first store registration after cancellation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-first-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 10 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 10 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_store = .{ .store_id = 10, .node_id = 10, .role = "data", .health_class = "healthy", .live = true, .drain_requested = true },
    });
    defer std.testing.allocator.free(stale_draining_store_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_draining_store_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 22,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 22);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 22);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expect(!stores[0].drain_requested);
}

test "metadata raft apply store ignores stale draining node registration after cancellation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-stale-drain-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .request_node_shutdown = .{ .node_id = 11 } });
    defer std.testing.allocator.free(draining_node_cmd);
    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .cancel_node_shutdown = .{ .node_id = 11 } });
    defer std.testing.allocator.free(active_node_cmd);
    const stale_draining_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .register_node = .{ .node_id = 11, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_draining },
    });
    defer std.testing.allocator.free(stale_draining_node_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = draining_node_cmd },
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = stale_draining_node_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 23,
        .commit_index = 3,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 23);
    defer store.freeNodes(std.testing.allocator, nodes);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expect(metadata_table_manager.nodeLifecycleActive(nodes[0].lifecycle));
}

test "metadata raft apply store finalizes node shutdown by deleting node and stores" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-finalize-apply", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const target_statuses = [_]metadata.GroupStatusReport{.{
        .group_id = 41,
        .updated_at_millis = 10,
        .local_voter = true,
        .voter_count = 1,
    }};
    const target_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 12, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_draining },
    });
    defer std.testing.allocator.free(target_node_cmd);
    const other_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 13, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
    });
    defer std.testing.allocator.free(other_node_cmd);
    const target_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{
            .store_id = 12,
            .node_id = 12,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .drain_requested = true,
            .group_statuses = @constCast(target_statuses[0..]),
        },
    });
    defer std.testing.allocator.free(target_store_cmd);
    const other_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 13, .node_id = 13, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(other_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 12 } });
    defer std.testing.allocator.free(finalize_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = target_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = other_node_cmd },
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = target_store_cmd },
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = other_store_cmd },
        .{ .term = 2, .index = 5, .entry_type = .normal, .data = finalize_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 24,
        .commit_index = 5,
        .entries_bytes = encoded_entries,
    });

    const nodes = try store.listNodes(std.testing.allocator, 24);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 24);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expectEqual(@as(u64, 13), nodes[0].node_id);
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 13), stores[0].store_id);
}

test "metadata raft apply store rejects finalizing active node shutdown" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-node-finalize-active-reject", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const active_node_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_node = .{ .node_id = 14, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
    });
    defer std.testing.allocator.free(active_node_cmd);
    const active_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 14, .node_id = 14, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(active_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 14 } });
    defer std.testing.allocator.free(finalize_cmd);

    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = active_node_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = active_store_cmd },
    });
    defer std.testing.allocator.free(initial_entries);
    const finalize_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 3, .entry_type = .normal, .data = finalize_cmd },
    });
    defer std.testing.allocator.free(finalize_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 25,
        .commit_index = 2,
        .entries_bytes = initial_entries,
    });

    try std.testing.expectError(error.ActiveNodeFinalizeRejected, store.snapshotBuilder().applyBatch(.{
        .group_id = 25,
        .commit_index = 3,
        .entries_bytes = finalize_entries,
    }));

    const nodes = try store.listNodes(std.testing.allocator, 25);
    defer store.freeNodes(std.testing.allocator, nodes);
    const stores = try store.listStores(std.testing.allocator, 25);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), nodes.len);
    try std.testing.expectEqual(@as(u64, 14), nodes[0].node_id);
    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 14), stores[0].store_id);
}

test "metadata raft apply store rejects finalizing active store-only node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-only-finalize-active-reject", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const active_store_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_store = .{ .store_id = 15, .node_id = 15, .role = "data", .health_class = "healthy", .live = true },
    });
    defer std.testing.allocator.free(active_store_cmd);
    const finalize_cmd = try encodeTransitionCommand(std.testing.allocator, .{ .finalize_node_shutdown = .{ .node_id = 15 } });
    defer std.testing.allocator.free(finalize_cmd);

    const initial_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = active_store_cmd },
    });
    defer std.testing.allocator.free(initial_entries);
    const finalize_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 2, .entry_type = .normal, .data = finalize_cmd },
    });
    defer std.testing.allocator.free(finalize_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 26,
        .commit_index = 1,
        .entries_bytes = initial_entries,
    });

    try std.testing.expectError(error.ActiveNodeFinalizeRejected, store.snapshotBuilder().applyBatch(.{
        .group_id = 26,
        .commit_index = 2,
        .entries_bytes = finalize_entries,
    }));

    const stores = try store.listStores(std.testing.allocator, 26);
    defer store.freeStores(std.testing.allocator, stores);

    try std.testing.expectEqual(@as(usize, 1), stores.len);
    try std.testing.expectEqual(@as(u64, 15), stores[0].store_id);
}

test "metadata raft apply store projects table and range records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "docs",
            .description = "docs table",
            .schema_json = "{\"kind\":\"demo\"}",
            .indexes_json = "{\"default\":{}}",
            .replication_sources_json = "[\"seed\"]",
            .desired_replica_count = 5,
            .min_ranges = 2,
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 4101,
            .range_id = 4101,
            .table_id = 41,
            .start_key = "doc:a",
            .end_key = "doc:z",
            .doc_identity_shard_id = 4001,
            .doc_identity_range_id = 9001,
        },
    });
    defer std.testing.allocator.free(range_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();
    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    const tables = try store.listTables(std.testing.allocator, 41);
    defer store.freeTables(std.testing.allocator, tables);
    const ranges = try store.listRanges(std.testing.allocator, 41);
    defer store.freeRanges(std.testing.allocator, ranges);

    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 41), tables[0].table_id);
    try std.testing.expectEqualStrings("docs", tables[0].name);
    try std.testing.expectEqualStrings("docs table", tables[0].description);
    try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", tables[0].schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", tables[0].indexes_json);
    try std.testing.expectEqualStrings("[\"seed\"]", tables[0].replication_sources_json);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 4101), ranges[0].group_id);
    try std.testing.expectEqual(@as(u64, 4001), ranges[0].doc_identity_shard_id);
    try std.testing.expectEqual(@as(u64, 9001), ranges[0].doc_identity_range_id);
    try std.testing.expectEqualStrings("doc:a", ranges[0].start_key);
}

test "metadata raft apply store rejects reserved data group ids in transition records" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-invalid-data-group-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = group_ids.main_metadata_group_id,
            .table_id = 41,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const range_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(range_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 1,
        .entries_bytes = range_entries,
    }));

    const split_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 501,
            .source_group_id = 21,
            .destination_group_id = group_ids.main_metadata_group_id,
            .phase = .prepare,
        },
    });
    defer std.testing.allocator.free(split_cmd);
    const split_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = split_cmd },
    });
    defer std.testing.allocator.free(split_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 2,
        .entries_bytes = split_entries,
    }));

    const merge_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_merge_transition = .{
            .transition_id = 601,
            .donor_group_id = group_ids.main_metadata_group_id,
            .receiver_group_id = 30,
            .phase = .prepare,
        },
    });
    defer std.testing.allocator.free(merge_cmd);
    const merge_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = merge_cmd },
    });
    defer std.testing.allocator.free(merge_entries);
    try std.testing.expectError(error.ReservedGroupId, store.snapshotBuilder().applyBatch(.{
        .group_id = group_ids.main_metadata_group_id,
        .commit_index = 3,
        .entries_bytes = merge_entries,
    }));
}

test "metadata raft apply store notifies projection listeners for committed table and range changes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        table_signals: usize = 0,
        range_signals: usize = 0,
        last_table_id: u64 = 0,
        last_range_group_id: u64 = 0,

        fn onSignal(ptr: *anyopaque, signal: ProjectionSignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            switch (signal.kind) {
                .table => {
                    self.table_signals += 1;
                    self.last_table_id = signal.table_id;
                },
                .range => {
                    self.range_signals += 1;
                    self.last_table_id = signal.table_id;
                    self.last_range_group_id = signal.group_id;
                },
                else => {},
            }
        }
    };

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 77,
            .name = "docs",
            .schema_json = "{}",
            .indexes_json = "{}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 1001,
            .table_id = 77,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addProjectionListener(.{
        .ptr = &capture,
        .vtable = &.{
            .on_projection_signal = Capture.onSignal,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 1), capture.table_signals);
    try std.testing.expectEqual(@as(usize, 1), capture.range_signals);
    try std.testing.expectEqual(@as(u64, 77), capture.last_table_id);
    try std.testing.expectEqual(@as(u64, 1001), capture.last_range_group_id);
}

test "metadata raft apply store notifies projection listeners for shuffle join lease changes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-shuffle-lease-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        shuffle_join_lease_signals: usize = 0,

        fn onSignal(ptr: *anyopaque, signal: ProjectionSignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (signal.kind == .shuffle_join_lease) self.shuffle_join_lease_signals += 1;
        }
    };

    const lease_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    });
    defer std.testing.allocator.free(lease_cmd);
    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_shuffle_join_lease = .{
            .job_id = 77,
        },
    });
    defer std.testing.allocator.free(remove_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = lease_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addProjectionListener(.{
        .ptr = &capture,
        .vtable = &.{
            .on_projection_signal = Capture.onSignal,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 2), capture.shuffle_join_lease_signals);
}

test "metadata raft apply store preserves projected tables and ranges across reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-projection-reopen-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 61,
            .name = "docs",
            .description = "docs table",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 6101,
            .table_id = 61,
            .start_key = "doc:a",
            .end_key = "doc:z",
        },
    });
    defer std.testing.allocator.free(range_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 61,
            .commit_index = 2,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        const tables = try store.listTables(std.testing.allocator, 61);
        defer store.freeTables(std.testing.allocator, tables);
        const ranges = try store.listRanges(std.testing.allocator, 61);
        defer store.freeRanges(std.testing.allocator, ranges);

        try std.testing.expectEqual(@as(usize, 1), tables.len);
        try std.testing.expectEqualStrings("docs", tables[0].name);
        try std.testing.expectEqual(@as(usize, 1), ranges.len);
        try std.testing.expectEqual(@as(u64, 6101), ranges[0].group_id);
    }
}

test "metadata raft apply store notifies committed key listeners for matched metadata prefixes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-key-listener-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const Capture = struct {
        matched: usize = 0,
        saw_table: bool = false,
        saw_range: bool = false,

        fn matches(ptr: *anyopaque, signal: CommittedKeySignal) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self;
            var table_prefix_buf: [96]u8 = undefined;
            const table_prefix = tablePrefixForGroup(&table_prefix_buf, signal.metadata_group_id) catch return false;
            if (std.mem.startsWith(u8, signal.key, table_prefix)) return true;

            var range_prefix_buf: [96]u8 = undefined;
            const range_prefix = rangePrefixForGroup(&range_prefix_buf, signal.metadata_group_id) catch return false;
            return std.mem.startsWith(u8, signal.key, range_prefix);
        }

        fn onKey(ptr: *anyopaque, signal: CommittedKeySignal) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.matched += 1;

            var table_prefix_buf: [96]u8 = undefined;
            const table_prefix = tablePrefixForGroup(&table_prefix_buf, signal.metadata_group_id) catch return;
            if (std.mem.startsWith(u8, signal.key, table_prefix)) {
                self.saw_table = true;
                return;
            }

            var range_prefix_buf: [96]u8 = undefined;
            const range_prefix = rangePrefixForGroup(&range_prefix_buf, signal.metadata_group_id) catch return;
            if (std.mem.startsWith(u8, signal.key, range_prefix)) self.saw_range = true;
        }
    };

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 88,
            .name = "docs",
            .schema_json = "{}",
            .indexes_json = "{}",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const range_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_range = .{
            .group_id = 2001,
            .table_id = 88,
            .start_key = "",
            .end_key = null,
        },
    });
    defer std.testing.allocator.free(range_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = range_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    var capture = Capture{};
    try store.addCommittedKeyListener(.{
        .ptr = &capture,
        .vtable = &.{
            .matches_key = Capture.matches,
            .on_committed_key = Capture.onKey,
        },
    });

    try store.snapshotBuilder().applyBatch(.{
        .group_id = 41,
        .commit_index = 2,
        .entries_bytes = encoded_entries,
    });

    try std.testing.expectEqual(@as(usize, 2), capture.matched);
    try std.testing.expect(capture.saw_table);
    try std.testing.expect(capture.saw_range);
}

test "metadata.table record decoder accepts legacy table metadata encoding" {
    var encoded = std.ArrayListUnmanaged(u8).empty;
    defer encoded.deinit(std.testing.allocator);

    try appendInt(std.testing.allocator, &encoded, u64, 41);
    try appendInt(std.testing.allocator, &encoded, u16, 5);
    try appendInt(std.testing.allocator, &encoded, u32, 2);
    try appendInt(std.testing.allocator, &encoded, u32, 4);
    try encoded.appendSlice(std.testing.allocator, "docs");
    try appendInt(std.testing.allocator, &encoded, u32, 10);
    try encoded.appendSlice(std.testing.allocator, "docs table");
    try appendInt(std.testing.allocator, &encoded, u32, 15);
    try encoded.appendSlice(std.testing.allocator, "{\"kind\":\"demo\"}");
    try appendInt(std.testing.allocator, &encoded, u32, 14);
    try encoded.appendSlice(std.testing.allocator, "{\"default\":{}}");
    try appendInt(std.testing.allocator, &encoded, u32, 8);
    try encoded.appendSlice(std.testing.allocator, "[\"seed\"]");
    try appendInt(std.testing.allocator, &encoded, u32, 4);
    try encoded.appendSlice(std.testing.allocator, "data");

    const decoded = try decodeTableRecord(std.testing.allocator, encoded.items);
    defer metadata_table_manager.freeTable(std.testing.allocator, decoded);

    try std.testing.expectEqual(@as(u64, 41), decoded.table_id);
    try std.testing.expectEqualStrings("docs", decoded.name);
    try std.testing.expectEqualStrings("docs table", decoded.description);
    try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", decoded.schema_json);
    try std.testing.expectEqualStrings("", decoded.read_schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", decoded.indexes_json);
    try std.testing.expectEqualStrings("[\"seed\"]", decoded.replication_sources_json);
    try std.testing.expectEqualStrings("data", decoded.placement_role);
}

test "metadata.table record decoder round-trips read schema metadata" {
    const encoded = try encodeTableRecord(std.testing.allocator, .{
        .table_id = 41,
        .name = "docs",
        .description = "docs table",
        .schema_json = "{\"version\":1}",
        .read_schema_json = "{\"version\":0}",
        .indexes_json = "{\"default\":{}}",
        .replication_sources_json = "[\"seed\"]",
        .placement_role = "data",
        .desired_replica_count = 5,
        .min_ranges = 2,
    });
    defer std.testing.allocator.free(encoded);

    const decoded = try decodeTableRecord(std.testing.allocator, encoded);
    defer metadata_table_manager.freeTable(std.testing.allocator, decoded);

    try std.testing.expectEqualStrings("{\"version\":1}", decoded.schema_json);
    try std.testing.expectEqualStrings("{\"version\":0}", decoded.read_schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", decoded.indexes_json);
}

test "metadata schema progress transition command round-trips" {
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
            .schema_version = 3,
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)) orelse return error.InvalidMetadataTransitionEncoding;
    defer decoded.deinit(std.testing.allocator);

    switch (decoded) {
        .upsert_schema_progress => |record| {
            try std.testing.expectEqual(@as(u64, 41), record.table_id);
            try std.testing.expectEqual(@as(u64, 7), record.node_id);
            try std.testing.expectEqual(@as(u32, 3), record.schema_version);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }
}

test "metadata reallocation request transition command round-trips" {
    const encoded = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_reallocation_request = .{
            .requested_at_ms = 42_000,
        },
    });
    defer std.testing.allocator.free(encoded);

    var decoded = (try decodeTransitionCommand(std.testing.allocator, encoded)) orelse return error.InvalidMetadataTransitionEncoding;
    defer decoded.deinit(std.testing.allocator);

    switch (decoded) {
        .upsert_reallocation_request => |record| {
            try std.testing.expectEqual(@as(u64, 42_000), record.requested_at_ms);
        },
        else => return error.InvalidMetadataTransitionEncoding,
    }
}

test "metadata raft apply store projects schema progress records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-schema-progress-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const progress_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
            .schema_version = 3,
        },
    });
    defer std.testing.allocator.free(progress_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = progress_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 4,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listSchemaProgress(std.testing.allocator, 41);
        defer store.freeSchemaProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 1), progress.len);
        try std.testing.expectEqual(@as(u64, 41), progress[0].table_id);
        try std.testing.expectEqual(@as(u64, 7), progress[0].node_id);
        try std.testing.expectEqual(@as(u32, 3), progress[0].schema_version);
    }

    const remove_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .remove_schema_progress = .{
            .table_id = 41,
            .node_id = 7,
        },
    });
    defer std.testing.allocator.free(remove_cmd);
    const encoded_remove = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 2, .index = 5, .entry_type = .normal, .data = remove_cmd },
    });
    defer std.testing.allocator.free(encoded_remove);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 5,
            .entries_bytes = encoded_remove,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listSchemaProgress(std.testing.allocator, 41);
        defer store.freeSchemaProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 0), progress.len);
    }
}

test "metadata restore progress transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_restore_progress = .{
            .table_id = 41,
            .node_id = 7,
            .group_id = 4101,
            .backup_id = "snap1",
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_restore_progress);
    try std.testing.expectEqual(@as(u64, 41), decoded.?.upsert_restore_progress.table_id);
    try std.testing.expectEqual(@as(u64, 7), decoded.?.upsert_restore_progress.node_id);
    try std.testing.expectEqual(@as(u64, 4101), decoded.?.upsert_restore_progress.group_id);
    try std.testing.expectEqualStrings("snap1", decoded.?.upsert_restore_progress.backup_id);
}

test "metadata replication source status transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_replication_source_status = .{
            .table_id = 41,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "users",
            .cutover_mode = "exported_snapshot",
            .phase = "snapshot",
            .checkpoint = "lsn:0/16B6A50",
            .prepared_checkpoint = "lsn:0/16B6A50",
            .last_error = "",
            .failure_class = "terminal",
            .lag_records = 12,
            .lag_millis = 34,
            .consecutive_failures = 4,
            .last_source_commit_at_ms = 120,
            .last_success_at_ms = 123,
            .last_change_applied_at_ms = 124,
            .updated_at_ms = 555,
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_replication_source_status);
    try std.testing.expectEqual(@as(u64, 41), decoded.?.upsert_replication_source_status.table_id);
    try std.testing.expectEqual(@as(u32, 0), decoded.?.upsert_replication_source_status.source_ordinal);
    try std.testing.expectEqualStrings("postgres", decoded.?.upsert_replication_source_status.source_kind);
    try std.testing.expectEqualStrings("users", decoded.?.upsert_replication_source_status.external_table);
    try std.testing.expectEqualStrings("exported_snapshot", decoded.?.upsert_replication_source_status.cutover_mode);
    try std.testing.expectEqualStrings("snapshot", decoded.?.upsert_replication_source_status.phase);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", decoded.?.upsert_replication_source_status.prepared_checkpoint);
    try std.testing.expectEqualStrings("terminal", decoded.?.upsert_replication_source_status.failure_class);
    try std.testing.expectEqual(@as(u64, 34), decoded.?.upsert_replication_source_status.lag_millis);
    try std.testing.expectEqual(@as(u64, 4), decoded.?.upsert_replication_source_status.consecutive_failures);
    try std.testing.expectEqual(@as(u64, 120), decoded.?.upsert_replication_source_status.last_source_commit_at_ms);
    try std.testing.expectEqual(@as(u64, 123), decoded.?.upsert_replication_source_status.last_success_at_ms);
    try std.testing.expectEqual(@as(u64, 124), decoded.?.upsert_replication_source_status.last_change_applied_at_ms);
}

test "metadata shuffle join lease transition command round-trips" {
    const command: TransitionCommand = .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    };

    const encoded = try encodeTransitionCommand(std.testing.allocator, command);
    defer std.testing.allocator.free(encoded);

    var decoded = try decodeTransitionCommand(std.testing.allocator, encoded);
    defer if (decoded) |*d| d.deinit(std.testing.allocator);

    try std.testing.expect(decoded != null);
    try std.testing.expect(decoded.? == .upsert_shuffle_join_lease);
    try std.testing.expectEqual(@as(u64, 77), decoded.?.upsert_shuffle_join_lease.job_id);
    try std.testing.expectEqual(@as(u64, 202), decoded.?.upsert_shuffle_join_lease.owner_group_id);
    try std.testing.expectEqual(@as(u64, 9_999), decoded.?.upsert_shuffle_join_lease.expires_at_ms);
}

test "metadata raft apply store projects shuffle join lease records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-shuffle-lease-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const lease_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_shuffle_join_lease = .{
            .job_id = 77,
            .owner_group_id = 202,
            .expires_at_ms = 9_999,
        },
    });
    defer std.testing.allocator.free(lease_cmd);
    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 4, .entry_type = .normal, .data = lease_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 4,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const lease = (try store.getShuffleJoinLease(41, 77)).?;
        try std.testing.expectEqual(@as(u64, 77), lease.job_id);
        try std.testing.expectEqual(@as(u64, 202), lease.owner_group_id);
        try std.testing.expectEqual(@as(u64, 9_999), lease.expires_at_ms);

        const leases = try store.listShuffleJoinLeases(std.testing.allocator, 41);
        defer store.freeShuffleJoinLeases(std.testing.allocator, leases);
        try std.testing.expectEqual(@as(usize, 1), leases.len);
    }
}

test "metadata raft apply store projects restore progress records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-restore-progress-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const progress_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_restore_progress = .{
            .table_id = 41,
            .node_id = 7,
            .group_id = 4101,
            .backup_id = "snap1",
        },
    });
    defer std.testing.allocator.free(progress_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = progress_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const progress = try store.listRestoreProgress(std.testing.allocator, 41);
        defer store.freeRestoreProgress(std.testing.allocator, progress);
        try std.testing.expectEqual(@as(usize, 1), progress.len);
        try std.testing.expectEqual(@as(u64, 41), progress[0].table_id);
        try std.testing.expectEqual(@as(u64, 7), progress[0].node_id);
        try std.testing.expectEqual(@as(u64, 4101), progress[0].group_id);
        try std.testing.expectEqualStrings("snap1", progress[0].backup_id);
    }
}

test "metadata raft apply store projects replication source status records from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-replication-source-status-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const table_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_table = .{
            .table_id = 41,
            .name = "docs",
            .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]",
        },
    });
    defer std.testing.allocator.free(table_cmd);
    const status_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replication_source_status = .{
            .table_id = 41,
            .source_ordinal = 0,
            .source_kind = "postgres",
            .external_table = "users",
            .phase = "streaming",
            .checkpoint = "lsn:0/16B6B10",
            .prepared_checkpoint = "lsn:0/16B6A50",
            .last_error = "",
            .failure_class = "retryable",
            .lag_records = 3,
            .lag_millis = 21,
            .consecutive_failures = 2,
            .last_source_commit_at_ms = 333,
            .last_success_at_ms = 444,
            .last_change_applied_at_ms = 555,
            .updated_at_ms = 777,
        },
    });
    defer std.testing.allocator.free(status_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 1, .entry_type = .normal, .data = table_cmd },
        .{ .term = 1, .index = 2, .entry_type = .normal, .data = status_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 41,
            .commit_index = 2,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const statuses = try store.listReplicationSourceStatuses(std.testing.allocator, 41);
        defer store.freeReplicationSourceStatuses(std.testing.allocator, statuses);
        try std.testing.expectEqual(@as(usize, 1), statuses.len);
        try std.testing.expectEqual(@as(u64, 41), statuses[0].table_id);
        try std.testing.expectEqual(@as(u32, 0), statuses[0].source_ordinal);
        try std.testing.expectEqualStrings("postgres", statuses[0].source_kind);
        try std.testing.expectEqualStrings("users", statuses[0].external_table);
        try std.testing.expectEqualStrings("streaming", statuses[0].phase);
        try std.testing.expectEqualStrings("lsn:0/16B6A50", statuses[0].prepared_checkpoint);
        try std.testing.expectEqualStrings("retryable", statuses[0].failure_class);
        try std.testing.expectEqual(@as(u64, 3), statuses[0].lag_records);
        try std.testing.expectEqual(@as(u64, 21), statuses[0].lag_millis);
        try std.testing.expectEqual(@as(u64, 2), statuses[0].consecutive_failures);
        try std.testing.expectEqual(@as(u64, 333), statuses[0].last_source_commit_at_ms);
        try std.testing.expectEqual(@as(u64, 444), statuses[0].last_success_at_ms);
        try std.testing.expectEqual(@as(u64, 555), statuses[0].last_change_applied_at_ms);
    }
}

test "metadata raft apply store projects placement intents from committed entries" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const intent_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replica_intent = .{
            .record = .{
                .group_id = 5101,
                .replica_id = 2,
                .local_node_id = 7,
                .bootstrap_mode = .fetch_snapshot,
                .metadata_version = 3,
                .snapshot_bootstrap = .{
                    .from_node_id = 3,
                    .term = 8,
                    .snapshot_id = "snap-5101",
                    .uri = "http://127.0.0.1:7777/raft/v1/snapshot/fetch/snap-5101",
                },
            },
            .store_id = 44,
            .peer_node_ids = &.{ 7, 8, 9 },
        },
    });
    defer std.testing.allocator.free(intent_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = intent_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 51,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const intents = try store.listPlacementIntents(std.testing.allocator, 51);
        defer store.freePlacementIntents(std.testing.allocator, intents);
        try std.testing.expectEqual(@as(usize, 1), intents.len);
        try std.testing.expectEqual(@as(u64, 5101), intents[0].record.group_id);
        try std.testing.expectEqual(@as(u64, 7), intents[0].record.local_node_id);
        try std.testing.expectEqual(@as(u64, 44), intents[0].store_id);
        try std.testing.expectEqual(@as(usize, 3), intents[0].peer_node_ids.len);
        try std.testing.expectEqual(raft_catalog.ReplicaBootstrapMode.fetch_snapshot, intents[0].record.bootstrap_mode);
        try std.testing.expect(intents[0].record.snapshot_bootstrap != null);
        try std.testing.expectEqual(@as(u64, 3), intents[0].record.snapshot_bootstrap.?.from_node_id);
        try std.testing.expectEqual(@as(u64, 8), intents[0].record.snapshot_bootstrap.?.term);
        try std.testing.expectEqualStrings("snap-5101", intents[0].record.snapshot_bootstrap.?.snapshot_id);
    }
}

test "metadata raft apply store projects backup restore bootstrap source in placement intents" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-backup-source-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    const intent_cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_replica_intent = .{
            .record = .{
                .group_id = 5201,
                .replica_id = 2,
                .local_node_id = 7,
                .bootstrap_mode = .fetch_snapshot,
                .metadata_version = 4,
                .backup_restore_bootstrap = .{
                    .backup_id = "snap-5201",
                    .location = "file:///tmp/backups",
                    .snapshot_path = "snap-5201/groups/5201",
                },
            },
            .store_id = 45,
            .peer_node_ids = &.{ 7, 8, 9 },
        },
    });
    defer std.testing.allocator.free(intent_cmd);

    const encoded_entries = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
        .{ .term = 1, .index = 3, .entry_type = .normal, .data = intent_cmd },
    });
    defer std.testing.allocator.free(encoded_entries);

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        try store.snapshotBuilder().applyBatch(.{
            .group_id = 52,
            .commit_index = 3,
            .entries_bytes = encoded_entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();
        const intents = try store.listPlacementIntents(std.testing.allocator, 52);
        defer store.freePlacementIntents(std.testing.allocator, intents);
        try std.testing.expectEqual(@as(usize, 1), intents.len);
        try std.testing.expect(intents[0].record.backup_restore_bootstrap != null);
        try std.testing.expectEqualStrings("snap-5201", intents[0].record.backup_restore_bootstrap.?.backup_id);
        try std.testing.expectEqualStrings("file:///tmp/backups", intents[0].record.backup_restore_bootstrap.?.location);
        try std.testing.expectEqualStrings("snap-5201/groups/5201", intents[0].record.backup_restore_bootstrap.?.snapshot_path);
    }
}

test "metadata state machine projects transitions through metadata apply store" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-sm-store", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
    defer store.deinit();

    const SinkRecorder = struct {
        last_index: u64 = 0,

        fn sink(self: *@This()) raft_state_machine.AppliedIndexSink {
            return .{
                .ptr = self,
                .vtable = &.{
                    .set_applied_index = setAppliedIndex,
                },
            };
        }

        fn setAppliedIndex(ptr: *anyopaque, _: u64, index: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.last_index = index;
        }
    };

    var sink = SinkRecorder{};
    var sm = raft_state_machine.MetadataStateMachine{
        .alloc = std.testing.allocator,
        .applied_sink = sink.sink(),
        .snapshot_builder = store.snapshotBuilder(),
    };

    const cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 701,
            .source_group_id = 41,
            .destination_group_id = 42,
            .phase = .bootstrap_peer,
        },
    });
    defer std.testing.allocator.free(cmd);

    try sm.stateMachine().applyReady(41, &.{
        .{ .term = 3, .index = 12, .entry_type = .normal, .data = cmd },
    }, &.{});

    const splits = try store.listSplitTransitions(std.testing.allocator, 41);
    defer store.freeSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 701), splits[0].transition_id);
    try std.testing.expectEqual(@as(u64, 12), sink.last_index);
}

test "metadata raft apply store runtime status codec preserves document identity telemetry" {
    const alloc = std.testing.allocator;

    var runtime_statuses = [_]metadata.RuntimeGroupStatusReport{.{
        .table_id = 1,
        .table_name = "docs",
        .group_id = 10,
        .store_id = 20,
        .node_id = 30,
        .source = "background_refresh",
        .freshness = "fresh",
        .doc_identity = .{
            .namespace_table_id = 1,
            .namespace_shard_id = 10,
            .namespace_range_id = 1001,
            .next_ordinal = 44,
            .allocated_ordinals = 43,
            .ordinal_capacity_remaining = 123,
            .rebuild_required = true,
            .state_rows = 42,
            .live_ordinals = 40,
            .tombstone_ordinals = 2,
            .min_created_generation = 11,
            .max_created_generation = 17,
            .min_deleted_generation = 15,
            .max_deleted_generation = 18,
            .scanned_primary_docs = 41,
            .primary_docs_missing_ordinals = 1,
            .primary_docs_with_tombstone_ordinals = 1,
            .complete = true,
        },
        .doc_set_planning = .{
            .resolved_set_count = 9,
            .ordinal_list_count = 8,
            .ordinal_list_docs = 7,
            .missing_ordinal_coverage_count = 6,
            .stale_identity_generation_rejection_count = 5,
        },
    }};

    const encoded = try encodeStoreRecord(alloc, .{
        .store_id = 20,
        .node_id = 30,
        .runtime_statuses = runtime_statuses[0..],
    });
    defer alloc.free(encoded);

    const decoded = try decodeStoreRecord(alloc, encoded);
    defer metadata_table_manager.freeStore(alloc, decoded);

    try std.testing.expectEqual(@as(usize, 1), decoded.runtime_statuses.len);
    const status = decoded.runtime_statuses[0];
    try std.testing.expectEqual(@as(u64, 1), status.doc_identity.namespace_table_id);
    try std.testing.expectEqual(@as(u64, 10), status.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(@as(u64, 1001), status.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u32, 44), status.doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 43), status.doc_identity.allocated_ordinals);
    try std.testing.expect(status.doc_identity.rebuild_required);
    try std.testing.expect(status.doc_identity.complete);
    try std.testing.expectEqual(@as(u64, 9), status.doc_set_planning.resolved_set_count);
    try std.testing.expectEqual(@as(u64, 8), status.doc_set_planning.ordinal_list_count);
    try std.testing.expectEqual(@as(u64, 7), status.doc_set_planning.ordinal_list_docs);
    try std.testing.expectEqual(@as(u64, 6), status.doc_set_planning.missing_ordinal_coverage_count);
    try std.testing.expectEqual(@as(u64, 5), status.doc_set_planning.stale_identity_generation_rejection_count);
}

test "metadata apply store replay is idempotent when applied watermark lags WAL state" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-apply-replay-idempotent", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var layout = try raft_storage_mod.ReplicaPathLayout.initForReplica(std.testing.allocator, root, 202, 1);
    defer layout.deinit(std.testing.allocator);

    const cmd = try encodeTransitionCommand(std.testing.allocator, .{
        .upsert_split_transition = .{
            .transition_id = 902,
            .source_group_id = 51,
            .destination_group_id = 52,
            .phase = .bootstrap_peer,
        },
    });
    defer std.testing.allocator.free(cmd);

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();

        const entries = try std.testing.allocator.dupe(raft_engine.core.Entry, &[_]raft_engine.core.Entry{
            .{ .term = 5, .index = 1, .entry_type = .normal, .data = cmd },
        });
        defer std.testing.allocator.free(entries);

        try wal_state.groupStorage().persistReady(202, .{
            .hard_state = .{ .current_term = 5, .voted_for = 1, .commit_index = 1 },
            .entries = entries,
        });
    }

    {
        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.MetadataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };
        try sm.stateMachine().applyReady(202, &.{
            .{ .term = 5, .index = 1, .entry_type = .normal, .data = cmd },
        }, &.{});
    }

    {
        var wal_state = try wal_replica_state_mod.WalReplicaState.init(std.testing.allocator, layout, .{});
        defer wal_state.deinit();
        try std.testing.expectEqual(@as(u64, 0), wal_state.appliedIndex());

        var raw = try raft_engine.core.RawNode.init(std.testing.allocator, .{
            .id = 1,
            .group_id = 202,
            .peers = &.{1},
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .check_quorum = true,
            .applied = wal_state.appliedIndex(),
        }, wal_state.storage());
        defer raw.deinit();

        try std.testing.expect(raw.hasReady());
        const rd = raw.ready();
        try std.testing.expectEqual(@as(usize, 1), rd.committed_entries.len);

        var store = try RaftApplyStore.init(std.testing.allocator, .{ .root_dir = root });
        defer store.deinit();

        var sm = raft_state_machine.MetadataStateMachine{
            .alloc = std.testing.allocator,
            .applied_sink = raft_state_machine.noopAppliedIndexSink(),
            .snapshot_builder = store.snapshotBuilder(),
        };
        try sm.stateMachine().applyReady(202, rd.committed_entries, &.{});

        const batch = (try store.latestBatch(202)) orelse return error.MissingMetadataBatch;
        try std.testing.expectEqual(@as(u64, 1), batch.commit_index);

        const splits = try store.listSplitTransitions(std.testing.allocator, 202);
        defer store.freeSplitTransitions(std.testing.allocator, splits);
        try std.testing.expectEqual(@as(usize, 1), splits.len);
        try std.testing.expectEqual(@as(u64, 902), splits[0].transition_id);
        try std.testing.expectEqual(@as(u64, 51), splits[0].source_group_id);
        try std.testing.expectEqual(@as(u64, 52), splits[0].destination_group_id);
    }
}
