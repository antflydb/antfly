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
const clock = @import("clock.zig");

fn sleepOneMillisecond() void {
    std.testing.io.sleep(.fromNanoseconds(std.time.ns_per_ms), .awake) catch {};
}

const StorageRecorder = struct {
    alloc: std.mem.Allocator,
    stores: std.AutoHashMapUnmanaged(core.types.GroupId, *core.MemoryStorage) = .empty,
    persist_calls: usize = 0,
    persisted_entries: usize = 0,
    persisted_snapshots: usize = 0,
    compact_failures_remaining: usize = 0,
    compact_failure_group: ?core.types.GroupId = null,
    compact_successes: usize = 0,
    compacted_groups: [8]core.types.GroupId = [_]core.types.GroupId{0} ** 8,
    retired_groups: usize = 0,

    fn deinit(self: *StorageRecorder) void {
        self.stores.deinit(self.alloc);
        self.* = undefined;
    }

    fn registerStore(self: *StorageRecorder, group_id: core.types.GroupId, store: *core.MemoryStorage) !void {
        try self.stores.put(self.alloc, group_id, store);
    }

    fn iface(self: *StorageRecorder) runtime.storage_iface.GroupStorage {
        return .{
            .ptr = self,
            .vtable = &.{
                .persist_ready = persistReady,
                .compact_snapshot = compactSnapshot,
                .retire_group = retireGroup,
            },
        };
    }

    fn persistReady(ptr: *anyopaque, group_id: core.types.GroupId, ready: core.Ready) !void {
        const self: *StorageRecorder = @ptrCast(@alignCast(ptr));
        const store = self.stores.get(group_id) orelse return error.UnknownGroup;
        self.persist_calls += 1;
        self.persisted_entries += ready.entries.len;
        if (ready.snapshot != null) self.persisted_snapshots += 1;

        if (ready.snapshot) |snapshot| {
            try store.applySnapshot(snapshot);
        }
        if (ready.hard_state) |hard_state| {
            store.setHardState(hard_state);
        }
        if (ready.conf_state) |conf_state| try store.setConfState(conf_state);
        if (ready.entries.len > 0) {
            try store.append(ready.entries);
        }
    }

    fn compactSnapshot(ptr: *anyopaque, group_id: core.types.GroupId, snapshot: core.types.Snapshot, compact_index: core.types.Index) !void {
        const self: *StorageRecorder = @ptrCast(@alignCast(ptr));
        const store = self.stores.get(group_id) orelse return error.UnknownGroup;
        self.persist_calls += 1;
        self.persisted_snapshots += 1;
        if (self.compact_failures_remaining > 0 and
            (self.compact_failure_group == null or self.compact_failure_group.? == group_id))
        {
            self.compact_failures_remaining -= 1;
            return error.InjectedSnapshotPublishFailure;
        }
        try store.compactToSnapshot(snapshot, compact_index);
        if (self.compact_successes < self.compacted_groups.len) {
            self.compacted_groups[self.compact_successes] = group_id;
        }
        self.compact_successes += 1;
    }

    fn retireGroup(ptr: *anyopaque, group_id: core.types.GroupId) void {
        const self: *StorageRecorder = @ptrCast(@alignCast(ptr));
        self.retired_groups += 1;
        _ = self.stores.remove(group_id);
    }
};

const FailingRemovalCatalog = struct {
    fail_remove: bool = true,
    remove_calls: usize = 0,

    fn iface(self: *@This()) runtime.replica_catalog_iface.ReplicaCatalog {
        return .{
            .ptr = self,
            .vtable = &.{
                .upsert_replica = upsertReplica,
                .remove_replica = removeReplica,
                .list_replicas = listReplicas,
            },
        };
    }

    fn upsertReplica(_: *anyopaque, _: runtime.ReplicaRecord) !void {}

    fn removeReplica(ptr: *anyopaque, _: core.types.GroupId) !bool {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.remove_calls += 1;
        if (self.fail_remove) return error.InjectedCatalogRemovalFailure;
        return true;
    }

    fn listReplicas(_: *anyopaque, alloc: std.mem.Allocator) ![]runtime.ReplicaRecord {
        return try alloc.alloc(runtime.ReplicaRecord, 0);
    }
};

const DiskBatcherRecorder = struct {
    alloc: std.mem.Allocator,
    stores: std.AutoHashMapUnmanaged(core.types.GroupId, *core.MemoryStorage) = .empty,
    begin_calls: usize = 0,
    finish_calls: usize = 0,
    persist_calls: usize = 0,
    persisted_entries: usize = 0,

    fn deinit(self: *DiskBatcherRecorder) void {
        self.stores.deinit(self.alloc);
        self.* = undefined;
    }

    fn registerStore(self: *DiskBatcherRecorder, group_id: core.types.GroupId, store: *core.MemoryStorage) !void {
        try self.stores.put(self.alloc, group_id, store);
    }

    fn iface(self: *DiskBatcherRecorder) runtime.storage_iface.DiskBatcher {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_batch = beginBatch,
            },
        };
    }

    fn beginBatch(ptr: *anyopaque) !runtime.storage_iface.PersistBatch {
        const self: *DiskBatcherRecorder = @ptrCast(@alignCast(ptr));
        self.begin_calls += 1;
        return .{
            .ptr = self,
            .vtable = &.{
                .persist_ready = persistReady,
                .finish = finish,
            },
        };
    }

    fn persistReady(ptr: *anyopaque, group_id: core.types.GroupId, ready: core.Ready) !void {
        const self: *DiskBatcherRecorder = @ptrCast(@alignCast(ptr));
        const store = self.stores.get(group_id) orelse return error.UnknownGroup;
        self.persist_calls += 1;
        self.persisted_entries += ready.entries.len;
        if (ready.snapshot) |snapshot| try store.applySnapshot(snapshot);
        if (ready.hard_state) |hard_state| store.setHardState(hard_state);
        if (ready.conf_state) |conf_state| try store.setConfState(conf_state);
        if (ready.entries.len > 0) try store.append(ready.entries);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *DiskBatcherRecorder = @ptrCast(@alignCast(ptr));
        self.finish_calls += 1;
    }
};

const TestSnapshotArtifact = struct {
    alloc: std.mem.Allocator,
    bytes: []u8,

    fn create(alloc: std.mem.Allocator, bytes: []u8) !runtime.storage_iface.SnapshotArtifact {
        const self = try alloc.create(TestSnapshotArtifact);
        self.* = .{ .alloc = alloc, .bytes = bytes };
        return .{
            .ptr = self,
            .vtable = &.{
                .len = len,
                .write_to = writeTo,
                .read_all = readAll,
                .deinit = deinit,
            },
        };
    }

    fn len(ptr: *anyopaque) u64 {
        const self: *TestSnapshotArtifact = @ptrCast(@alignCast(ptr));
        return self.bytes.len;
    }

    fn writeTo(ptr: *anyopaque, writer: *std.Io.Writer) !void {
        const self: *TestSnapshotArtifact = @ptrCast(@alignCast(ptr));
        try writer.writeAll(self.bytes);
    }

    fn readAll(ptr: *anyopaque, alloc: std.mem.Allocator) ![]u8 {
        const self: *TestSnapshotArtifact = @ptrCast(@alignCast(ptr));
        return try alloc.dupe(u8, self.bytes);
    }

    fn deinit(ptr: *anyopaque) void {
        const self: *TestSnapshotArtifact = @ptrCast(@alignCast(ptr));
        const alloc = self.alloc;
        alloc.free(self.bytes);
        alloc.destroy(self);
    }
};

const ApplyRecorder = struct {
    alloc: std.mem.Allocator,
    apply_calls: usize = 0,
    applied_entries: usize = 0,
    applied_read_states: usize = 0,
    last_applied_index: core.types.Index = 0,
    last_applied_by_group: [128]core.types.Index = [_]core.types.Index{0} ** 128,
    last_read_index: core.types.Index = 0,
    snapshot_materializations: std.atomic.Value(usize) = .init(0),
    snapshot_failures_remaining: std.atomic.Value(usize) = .init(0),
    snapshot_prepare_failures_remaining: usize = 0,
    materialized_groups: [8]std.atomic.Value(core.types.GroupId) = [_]std.atomic.Value(core.types.GroupId){.init(0)} ** 8,
    block_snapshot_materialization: bool = false,
    snapshot_materialization_started: std.atomic.Value(bool) = .init(false),
    release_snapshot_materialization: std.atomic.Value(bool) = .init(false),
    materialize_artifact: bool = false,

    const PreparedSnapshot = struct {
        recorder: *ApplyRecorder,
        group_id: core.types.GroupId,
        applied_index: core.types.Index,

        fn source(self: *@This()) runtime.storage_iface.SnapshotSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .materialize = materialize,
                    .cancel = cancel,
                    .deinit = deinit,
                },
            };
        }

        fn materialize(ptr: *anyopaque, alloc: std.mem.Allocator) !runtime.storage_iface.SnapshotMaterialization {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const ordinal = self.recorder.snapshot_materializations.fetchAdd(1, .monotonic);
            if (ordinal < self.recorder.materialized_groups.len) {
                self.recorder.materialized_groups[ordinal].store(self.group_id, .release);
            }
            const failures = self.recorder.snapshot_failures_remaining.load(.acquire);
            if (failures > 0 and self.recorder.snapshot_failures_remaining.cmpxchgStrong(failures, failures - 1, .acq_rel, .acquire) == null) {
                return error.InjectedSnapshotBuildFailure;
            }
            if (self.recorder.block_snapshot_materialization) {
                self.recorder.snapshot_materialization_started.store(true, .release);
                while (!self.recorder.release_snapshot_materialization.load(.acquire)) {
                    std.Thread.yield() catch {};
                }
            }
            const bytes = try std.fmt.allocPrint(alloc, "applied-state-{d}", .{self.applied_index});
            if (!self.recorder.materialize_artifact) return .{ .bytes = bytes };
            return .{ .artifact = TestSnapshotArtifact.create(alloc, bytes) catch |err| {
                alloc.free(bytes);
                return err;
            } };
        }

        fn deinit(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.heap.page_allocator.destroy(self);
        }

        fn cancel(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.recorder.release_snapshot_materialization.store(true, .release);
        }
    };

    fn iface(self: *ApplyRecorder) runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .prepare_snapshot = prepareSnapshot,
                .build_snapshot = buildSnapshot,
                .apply_ready = applyReady,
            },
        };
    }

    fn prepareSnapshot(ptr: *anyopaque, group_id: core.types.GroupId, applied_index: core.types.Index) !?runtime.storage_iface.SnapshotSource {
        const self: *ApplyRecorder = @ptrCast(@alignCast(ptr));
        if (self.snapshot_prepare_failures_remaining > 0) {
            self.snapshot_prepare_failures_remaining -= 1;
            return error.InjectedSnapshotPrepareFailure;
        }
        const group_applied = if (group_id < self.last_applied_by_group.len)
            self.last_applied_by_group[@intCast(group_id)]
        else
            self.last_applied_index;
        if (applied_index != group_applied) return error.AppliedSnapshotIndexMismatch;
        const prepared = try std.heap.page_allocator.create(PreparedSnapshot);
        prepared.* = .{ .recorder = self, .group_id = group_id, .applied_index = applied_index };
        return prepared.source();
    }

    fn buildSnapshot(_: *anyopaque, alloc: std.mem.Allocator, _: core.types.GroupId) !?[]u8 {
        return try alloc.dupe(u8, "applied-state");
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: core.types.GroupId,
        snapshot: ?core.types.Snapshot,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        _ = snapshot;
        const self: *ApplyRecorder = @ptrCast(@alignCast(ptr));
        self.apply_calls += 1;
        self.applied_entries += committed_entries.len;
        self.applied_read_states += read_states.len;
        if (committed_entries.len > 0) {
            self.last_applied_index = committed_entries[committed_entries.len - 1].index;
            if (group_id < self.last_applied_by_group.len) {
                self.last_applied_by_group[@intCast(group_id)] = self.last_applied_index;
            }
        }
        if (read_states.len > 0) {
            self.last_read_index = read_states[read_states.len - 1].index;
        }
    }
};

const ApplyQueueRecorder = struct {
    enqueue_calls: usize = 0,
    drain_calls: usize = 0,
    queued_tasks: usize = 0,
    applied_entries: usize = 0,
    applied_read_states: usize = 0,

    fn iface(self: *ApplyQueueRecorder) runtime.storage_iface.ApplyQueue {
        return .{
            .ptr = self,
            .vtable = &.{
                .enqueue_apply = enqueueApply,
                .drain = drain,
                .abort = abort,
            },
        };
    }

    fn enqueueApply(
        ptr: *anyopaque,
        group_id: core.types.GroupId,
        _: ?core.types.Snapshot,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        _ = group_id;
        const self: *ApplyQueueRecorder = @ptrCast(@alignCast(ptr));
        self.enqueue_calls += 1;
        self.queued_tasks += 1;
        self.applied_entries += committed_entries.len;
        self.applied_read_states += read_states.len;
    }

    fn drain(ptr: *anyopaque) runtime.storage_iface.ApplyDrainResult {
        const self: *ApplyQueueRecorder = @ptrCast(@alignCast(ptr));
        self.drain_calls += 1;
        const completed = self.queued_tasks;
        self.queued_tasks = 0;
        return .{ .completed = completed };
    }

    fn abort(ptr: *anyopaque) void {
        const self: *ApplyQueueRecorder = @ptrCast(@alignCast(ptr));
        self.queued_tasks = 0;
    }
};

const PartialApplyRecorder = struct {
    attempts: usize = 0,
    failed_once: bool = false,
    successful_by_group: [256]usize = [_]usize{0} ** 256,

    fn iface(self: *@This()) runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{ .apply_ready = applyReady },
        };
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: core.types.GroupId,
        _: ?core.types.Snapshot,
        _: []const core.Entry,
        _: []const core.ReadState,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        self.attempts += 1;
        if (!self.failed_once and self.attempts == 2) {
            self.failed_once = true;
            return error.InjectedApplyFailure;
        }
        self.successful_by_group[@intCast(group_id)] += 1;
    }
};

const TransportRecorder = struct {
    alloc: std.mem.Allocator,
    send_calls: usize = 0,
    peer_batch_calls: usize = 0,
    sent_messages: usize = 0,
    batched_peer_count: usize = 0,
    batched_group_count: usize = 0,

    fn iface(self: *TransportRecorder) runtime.transport_iface.Transport {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_messages = sendMessages,
                .send_peer_batches = sendPeerBatches,
            },
        };
    }

    fn sendMessages(ptr: *anyopaque, group_id: core.types.GroupId, messages: []const core.Message) !void {
        _ = group_id;
        const self: *TransportRecorder = @ptrCast(@alignCast(ptr));
        self.send_calls += 1;
        self.sent_messages += messages.len;
    }

    fn sendPeerBatches(ptr: *anyopaque, batches: []const runtime.transport_iface.PeerBatch) !void {
        const self: *TransportRecorder = @ptrCast(@alignCast(ptr));
        self.peer_batch_calls += 1;
        self.batched_peer_count += batches.len;
        for (batches) |peer_batch| {
            for (peer_batch.groups) |group_batch| {
                self.batched_group_count += 1;
                self.sent_messages += group_batch.messages.len;
            }
        }
    }
};

const SnapshotTransportRecorder = struct {
    send_calls: usize = 0,
    sent_bytes: usize = 0,
    last_group_id: core.types.GroupId = 0,
    last_to: core.types.NodeId = 0,

    fn iface(self: *SnapshotTransportRecorder) runtime.snapshot_transport_iface.SnapshotTransport {
        return .{
            .ptr = self,
            .vtable = &.{
                .send_snapshot = sendSnapshot,
            },
        };
    }

    fn sendSnapshot(ptr: *anyopaque, req: runtime.snapshot_transport_iface.SnapshotSendRequest) !void {
        const self: *SnapshotTransportRecorder = @ptrCast(@alignCast(ptr));
        self.send_calls += 1;
        self.sent_bytes += req.snapshot.data.len;
        self.last_group_id = req.group_id;
        self.last_to = req.to;
    }
};

const SnapshotThrottleRecorder = struct {
    allow: bool = true,
    begin_calls: usize = 0,
    end_calls: usize = 0,

    fn iface(self: *SnapshotThrottleRecorder) runtime.snapshot_iface.SnapshotThrottle {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_snapshot = beginSnapshot,
                .end_snapshot = endSnapshot,
            },
        };
    }

    fn beginSnapshot(ptr: *anyopaque, group_id: core.types.GroupId) bool {
        _ = group_id;
        const self: *SnapshotThrottleRecorder = @ptrCast(@alignCast(ptr));
        self.begin_calls += 1;
        return self.allow;
    }

    fn endSnapshot(ptr: *anyopaque, group_id: core.types.GroupId) void {
        _ = group_id;
        const self: *SnapshotThrottleRecorder = @ptrCast(@alignCast(ptr));
        self.end_calls += 1;
    }
};

const BackpressureRecorder = struct {
    allow: bool = true,
    denied_group: ?core.types.GroupId = null,
    tracked_group: ?core.types.GroupId = null,
    calls: usize = 0,
    tracked_calls: usize = 0,
    last_pressure: runtime.ReadyPressure = .{ .group_id = 0 },

    fn iface(self: *BackpressureRecorder) runtime.Backpressure {
        return .{
            .ptr = self,
            .vtable = &.{
                .allow_ready = allowReady,
            },
        };
    }

    fn allowReady(ptr: *anyopaque, pressure: runtime.ReadyPressure) bool {
        const self: *BackpressureRecorder = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        if (self.tracked_group == pressure.group_id) self.tracked_calls += 1;
        self.last_pressure = pressure;
        return self.allow and self.denied_group != pressure.group_id;
    }
};

fn addSingleNodeGroup(
    host: *runtime.MultiRaft,
    group_id: core.types.GroupId,
    store: *core.MemoryStorage,
    async_storage_writes: bool,
) !void {
    var peers = [_]core.types.NodeId{1};
    try host.addGroup(.{
        .group_id = group_id,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = group_id,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
            .async_storage_writes = async_storage_writes,
        },
        .storage = store.storage(),
    });
}

fn drainGroup(host: *runtime.MultiRaft, group_id: core.types.GroupId) !usize {
    var passes: usize = 0;
    while (passes < 16) : (passes += 1) {
        const processed = try host.processReady(group_id);
        if (!processed) break;
    }
    return passes;
}

test "multi raft processReady drains a synchronous single-node group" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(11, &store);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 11, &store, false);
    try host.group(11).?.campaign();

    const passes = try drainGroup(&host, 11);
    try std.testing.expect(passes > 0);
    try std.testing.expectEqual(@as(usize, 0), (try host.drainReady(8)).processed_ready_steps);

    const grp = host.group(11).?;
    try std.testing.expect(!grp.hasReady());
    try std.testing.expectEqual(core.types.StateRole.leader, grp.status().soft.role);
    try std.testing.expectEqual(@as(core.types.Index, 1), grp.status().hard.commit_index);

    try std.testing.expect(storage_recorder.persist_calls > 0);
    try std.testing.expectEqual(@as(usize, 1), storage_recorder.persisted_entries);
    try std.testing.expect(apply_recorder.apply_calls > 0);
    try std.testing.expectEqual(@as(usize, 1), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(core.types.Index, 1), apply_recorder.last_applied_index);
    try std.testing.expectEqual(@as(usize, 0), transport_recorder.sent_messages);
    try std.testing.expectEqual(@as(core.types.Index, 1), store.hard_state.commit_index);
}

test "multi raft drainReady continues async pipeline without starving peer" {
    var async_store = core.MemoryStorage.init(std.testing.allocator);
    defer async_store.deinit();
    var peer_store = core.MemoryStorage.init(std.testing.allocator);
    defer peer_store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(21, &async_store);
    try storage_recorder.registerStore(22, &peer_store);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 21, &async_store, true);
    try addSingleNodeGroup(&host, 22, &peer_store, false);
    try host.group(21).?.campaign();
    try host.group(22).?.campaign();

    const processed = try host.drainReady(16);
    try std.testing.expectEqual(@as(usize, 2), processed.processed_groups);
    try std.testing.expect(processed.processed_ready_steps > processed.processed_groups);
    try std.testing.expectEqual(@as(usize, 0), (try host.drainReady(8)).processed_ready_steps);

    const async_group = host.group(21).?;
    try std.testing.expect(!async_group.hasReady());
    try std.testing.expectEqual(core.types.StateRole.leader, async_group.status().soft.role);
    try std.testing.expectEqual(@as(core.types.Index, 1), async_group.status().hard.commit_index);
    const peer_group = host.group(22).?;
    try std.testing.expect(!peer_group.hasReady());
    try std.testing.expectEqual(core.types.StateRole.leader, peer_group.status().soft.role);
    try std.testing.expectEqual(@as(core.types.Index, 1), peer_group.status().hard.commit_index);

    try std.testing.expect(storage_recorder.persist_calls > 0);
    try std.testing.expectEqual(@as(usize, 2), storage_recorder.persisted_entries);
    try std.testing.expect(apply_recorder.apply_calls > 0);
    try std.testing.expectEqual(@as(usize, 2), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(core.types.Index, 1), apply_recorder.last_applied_index);
    try std.testing.expectEqual(@as(usize, 0), transport_recorder.sent_messages);
    try std.testing.expectEqual(@as(core.types.Index, 1), async_store.hard_state.commit_index);
    try std.testing.expectEqual(@as(core.types.Index, 1), peer_store.hard_state.commit_index);
}

test "multi raft drainReady does not retry a no-progress frontier" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(23, &store);

    var backpressure = BackpressureRecorder{ .allow = false };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .backpressure = backpressure.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 23, &store, false);
    try host.campaignGroup(23);

    try std.testing.expectEqual(@as(usize, 0), (try host.drainReady(64)).processed_ready_steps);
    try std.testing.expectEqual(@as(usize, 1), backpressure.calls);
    try std.testing.expect(host.group(23).?.hasReady());

    backpressure.allow = true;
    try std.testing.expect((try host.drainReady(64)).processed_ready_steps > 0);
    try std.testing.expect(!host.group(23).?.hasReady());
}

test "multi raft drainReady reserves continuations for productive groups" {
    var async_store = core.MemoryStorage.init(std.testing.allocator);
    defer async_store.deinit();
    var denied_store = core.MemoryStorage.init(std.testing.allocator);
    defer denied_store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(24, &async_store);
    try storage_recorder.registerStore(25, &denied_store);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var backpressure = BackpressureRecorder{
        .denied_group = 25,
        .tracked_group = 25,
    };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .backpressure = backpressure.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 24, &async_store, true);
    try addSingleNodeGroup(&host, 25, &denied_store, false);
    try host.campaignGroup(24);
    try host.campaignGroup(25);

    const first = try host.drainReady(16);
    try std.testing.expectEqual(@as(usize, 1), first.processed_groups);
    try std.testing.expect(first.processed_ready_steps > first.processed_groups);
    try std.testing.expectEqual(@as(usize, 1), backpressure.tracked_calls);
    try std.testing.expect(!host.group(24).?.hasReady());
    try std.testing.expect(host.group(25).?.hasReady());

    backpressure.denied_group = null;
    const second = try host.drainReady(16);
    try std.testing.expectEqual(@as(usize, 1), second.processed_groups);
    try std.testing.expect(second.processed_ready_steps > 0);
    try std.testing.expect(!host.group(25).?.hasReady());
}

test "multi raft drainReady processes multiple hosted groups" {
    var store_a = core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(31, &store_a);
    try storage_recorder.registerStore(32, &store_b);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 31, &store_a, false);
    try addSingleNodeGroup(&host, 32, &store_b, false);
    try host.group(31).?.campaign();
    try host.group(32).?.campaign();

    {
        const ready_ids = try host.readyGroupIds(std.testing.allocator, 8);
        defer std.testing.allocator.free(ready_ids);
        try std.testing.expectEqual(@as(usize, 2), ready_ids.len);
    }

    try std.testing.expectEqual(@as(usize, 1), (try host.drainReady(1)).processed_ready_steps);

    {
        const ready_ids = try host.readyGroupIds(std.testing.allocator, 8);
        defer std.testing.allocator.free(ready_ids);
        try std.testing.expectEqual(@as(usize, 1), ready_ids.len);
    }

    try std.testing.expectEqual(@as(usize, 1), (try host.drainReady(8)).processed_ready_steps);
    try std.testing.expectEqual(@as(usize, 0), (try host.drainReady(8)).processed_ready_steps);

    try std.testing.expectEqual(core.types.StateRole.leader, host.group(31).?.status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.leader, host.group(32).?.status().soft.role);
    try std.testing.expectEqual(@as(core.types.Index, 1), host.group(31).?.status().hard.commit_index);
    try std.testing.expectEqual(@as(core.types.Index, 1), host.group(32).?.status().hard.commit_index);

    try std.testing.expectEqual(@as(usize, 2), storage_recorder.persisted_entries);
    try std.testing.expectEqual(@as(usize, 2), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(usize, 0), transport_recorder.sent_messages);
    try std.testing.expectEqual(@as(core.types.Index, 1), store_a.hard_state.commit_index);
    try std.testing.expectEqual(@as(core.types.Index, 1), store_b.hard_state.commit_index);
}

test "multi raft snapshot throttle gates snapshot ready processing" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(41, &store);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };
    var throttle = SnapshotThrottleRecorder{ .allow = false };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .transport = transport_recorder.iface(),
        .snapshot_throttle = throttle.iface(),
    });
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    try host.addGroup(.{
        .group_id = 41,
        .local_node_id = 2,
        .raft_config = .{
            .id = 2,
            .group_id = 41,
            .peers = peers[0..],
            .election_tick = 10,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });

    try host.step(41, .{
        .msg_type = .snapshot,
        .from = 1,
        .to = 2,
        .term = 2,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = peers[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expect(host.group(41).?.hasReady());
    try std.testing.expectEqual(false, try host.processReady(41));
    try std.testing.expect(host.group(41).?.hasReady());
    try std.testing.expectEqual(@as(usize, 1), throttle.begin_calls);
    try std.testing.expectEqual(@as(usize, 0), throttle.end_calls);
    try std.testing.expectEqual(@as(usize, 0), storage_recorder.persisted_snapshots);

    throttle.allow = true;
    try std.testing.expectEqual(true, try host.processReady(41));
    try std.testing.expectEqual(@as(usize, 2), throttle.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), throttle.end_calls);
    try std.testing.expectEqual(@as(usize, 1), storage_recorder.persisted_snapshots);
    try std.testing.expectEqual(@as(usize, 1), transport_recorder.sent_messages);
    try std.testing.expectEqual(@as(core.types.Index, 11), store.snapshot_state.metadata.index);
}

test "multi raft drainReady batches outbound transport by peer" {
    var store_a = core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(51, &store_a);
    try storage_recorder.registerStore(52, &store_b);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    try host.addGroup(.{
        .group_id = 51,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 51,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store_a.storage(),
    });
    try host.addGroup(.{
        .group_id = 52,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 52,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store_b.storage(),
    });

    try host.group(51).?.campaign();
    try host.group(52).?.campaign();

    try std.testing.expectEqual(@as(usize, 2), (try host.drainReady(8)).processed_ready_steps);
    try std.testing.expectEqual(@as(usize, 1), transport_recorder.peer_batch_calls);
    try std.testing.expectEqual(@as(usize, 1), transport_recorder.batched_peer_count);
    try std.testing.expectEqual(@as(usize, 2), transport_recorder.batched_group_count);
    try std.testing.expectEqual(@as(usize, 2), transport_recorder.sent_messages);
}

test "multi raft runRound ticks and drains work" {
    var store_a = core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(61, &store_a);
    try storage_recorder.registerStore(62, &store_b);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 61, &store_a, false);
    try addSingleNodeGroup(&host, 62, &store_b, false);
    try host.group(61).?.campaign();
    try host.group(62).?.campaign();

    const round = try host.runRound(2, 8);
    try std.testing.expectEqual(@as(usize, 2), round.ticked_groups);
    try std.testing.expectEqual(@as(usize, 2), round.processed_groups);
    try std.testing.expectEqual(@as(u64, 1), round.virtual_round);
    try std.testing.expectEqual(@as(u64, 100), round.virtual_time_ms);
    try std.testing.expectEqual(@as(u64, 1), host.virtualRound());
    try std.testing.expectEqual(@as(u64, 100), host.virtualTimeMs());
    try std.testing.expectEqual(core.types.StateRole.leader, host.group(61).?.status().soft.role);
    try std.testing.expectEqual(core.types.StateRole.leader, host.group(62).?.status().soft.role);
    try std.testing.expectEqual(@as(usize, 2), apply_recorder.applied_entries);
}

test "multi raft compacts active applied log after retained entry window" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(63, &store);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator, .materialize_artifact = true };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .applied_log_retained_entries = 2,
        .applied_log_compaction_min_interval_entries = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 63, &store, false);
    try host.group(63).?.campaign();
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 63));

    var proposal_index: usize = 0;
    while (proposal_index < 8) : (proposal_index += 1) {
        const payload = try std.fmt.allocPrint(std.testing.allocator, "payload-{d}", .{proposal_index});
        defer std.testing.allocator.free(payload);
        try host.propose(63, payload);
        try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 63));
    }

    const completion_deadline = clock.monotonicNs() +| 5 * std.time.ns_per_s;
    while (host.group(63).?.raw_node.raft.log.firstIndex() < 8 and clock.monotonicNs() < completion_deadline) {
        _ = try host.drainReady(0);
        sleepOneMillisecond();
    }

    const grp = host.group(63) orelse return error.UnknownGroup;
    try std.testing.expectEqual(@as(core.types.Index, 9), apply_recorder.last_applied_index);
    try std.testing.expect(host.metricsSnapshot().snapshot_compaction_completions > 0);
    try std.testing.expect(apply_recorder.snapshot_materializations.load(.monotonic) > 0);
    try std.testing.expectEqual(@as(core.types.Index, 8), grp.raw_node.raft.log.firstIndex());
    try std.testing.expectEqual(@as(core.types.Index, 8), try store.storage().firstIndex());
    try std.testing.expectEqual(@as(core.types.Term, 1), try store.storage().term(7));
    try std.testing.expectEqual(@as(core.types.Index, 9), grp.raw_node.raft.log.lastIndex());
}

test "multi raft applies an incoming snapshot before stale compaction maintenance" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(66, &store);
    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .applied_log_retained_entries = 1,
        .applied_log_compaction_min_interval_entries = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    try host.addGroup(.{
        .group_id = 66,
        .local_node_id = 2,
        .raft_config = .{
            .id = 2,
            .group_id = 66,
            .peers = peers[0..],
            .election_tick = 10,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });

    const incarnation = host.group_incarnations.get(66).?;
    try host.snapshot_candidates.put(std.testing.allocator, 66, .{
        .applied_index = 2,
        .incarnation = incarnation,
        .enqueue_sequence = 1,
        .conf_state = try (core.types.ConfState{ .voters = peers[0..] }).clone(std.testing.allocator),
    });
    try host.step(66, .{
        .msg_type = .snapshot,
        .from = 1,
        .to = 2,
        .term = 2,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 2,
                .conf_state = .{ .voters = peers[0..] },
            },
            .data = @constCast("incoming-state"),
        },
    });

    try std.testing.expect(try host.processReady(66));
    try std.testing.expectEqual(@as(usize, 1), apply_recorder.apply_calls);
    try std.testing.expectEqual(@as(usize, 0), host.snapshot_candidates.count());
    try std.testing.expectEqual(@as(core.types.Index, 11), store.snapshot_state.metadata.index);
    try std.testing.expectEqualStrings("incoming-state", store.snapshot_state.data);
}

test "multi raft cancels and drops a snapshot from a retired group incarnation" {
    var old_store = core.MemoryStorage.init(std.testing.allocator);
    defer old_store.deinit();
    var replacement_store = core.MemoryStorage.init(std.testing.allocator);
    defer replacement_store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(64, &old_store);
    var apply_recorder = ApplyRecorder{
        .alloc = std.testing.allocator,
        .block_snapshot_materialization = true,
    };
    defer apply_recorder.release_snapshot_materialization.store(true, .release);

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .applied_log_retained_entries = 1,
        .applied_log_compaction_min_interval_entries = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 64, &old_store, false);
    try host.group(64).?.campaign();
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 64));
    try host.propose(64, "old-generation");
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 64));

    const start_deadline = clock.monotonicNs() +| 5 * std.time.ns_per_s;
    while (!apply_recorder.snapshot_materialization_started.load(.acquire) and clock.monotonicNs() < start_deadline) {
        sleepOneMillisecond();
    }
    try std.testing.expect(apply_recorder.snapshot_materialization_started.load(.acquire));
    try std.testing.expect(host.removeGroup(64));
    try std.testing.expect(apply_recorder.release_snapshot_materialization.load(.acquire));
    try storage_recorder.registerStore(64, &replacement_store);
    try addSingleNodeGroup(&host, 64, &replacement_store, false);

    const stale_deadline = clock.monotonicNs() +| 5 * std.time.ns_per_s;
    while (host.metricsSnapshot().snapshot_compaction_stale_drops == 0 and clock.monotonicNs() < stale_deadline) {
        _ = try host.drainReady(0);
        sleepOneMillisecond();
    }
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().snapshot_compaction_stale_drops);
    try std.testing.expectEqual(@as(usize, 0), storage_recorder.persisted_snapshots);
}

test "multi raft retries a failed snapshot build without requiring another write" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(65, &store);
    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    apply_recorder.snapshot_failures_remaining.store(1, .release);

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .applied_log_retained_entries = 1,
        .applied_log_compaction_min_interval_entries = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 65, &store, false);
    try host.group(65).?.campaign();
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 65));
    try host.propose(65, "compact-after-failure");
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 65));

    var attempts: usize = 0;
    while (host.metricsSnapshot().snapshot_compaction_completions == 0 and attempts < 5_000) : (attempts += 1) {
        _ = try host.drainReady(0);
        sleepOneMillisecond();
    }
    const metrics = host.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), metrics.snapshot_compaction_failures);
    try std.testing.expect(metrics.snapshot_compaction_retries >= 1);
    try std.testing.expectEqual(@as(usize, 1), metrics.snapshot_compaction_completions);
}

test "multi raft does not replay apply when snapshot preparation is deferred" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(67, &store);
    var apply_recorder = ApplyRecorder{
        .alloc = std.testing.allocator,
        .snapshot_prepare_failures_remaining = 1,
    };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .applied_log_retained_entries = 1,
        .applied_log_compaction_min_interval_entries = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 67, &store, false);
    try host.group(67).?.campaign();
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 67));
    try host.propose(67, "apply-once");
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 67));

    try std.testing.expectEqual(@as(usize, 2), apply_recorder.apply_calls);
    try std.testing.expectEqual(@as(usize, 2), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(core.types.Index, 2), apply_recorder.last_applied_index);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().snapshot_compaction_failures);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().snapshot_compaction_retries);

    _ = try host.drainReady(0);
    try std.testing.expectEqual(@as(usize, 2), apply_recorder.apply_calls);
}

test "multi raft yields a repeatedly failing snapshot publish to queued groups" {
    var failing_store = core.MemoryStorage.init(std.testing.allocator);
    defer failing_store.deinit();
    var waiting_store = core.MemoryStorage.init(std.testing.allocator);
    defer waiting_store.deinit();
    var storage_recorder = StorageRecorder{
        .alloc = std.testing.allocator,
        .compact_failures_remaining = 5,
        .compact_failure_group = 68,
    };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(68, &failing_store);
    try storage_recorder.registerStore(69, &waiting_store);
    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .applied_log_retained_entries = 1,
        .applied_log_compaction_min_interval_entries = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 68, &failing_store, false);
    try addSingleNodeGroup(&host, 69, &waiting_store, false);
    try host.group(68).?.campaign();
    try host.group(69).?.campaign();
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 68));
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 69));
    try host.propose(68, "publish-fails");
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 68));
    try host.propose(69, "must-not-starve");
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 69));

    var attempts: usize = 0;
    while (host.metricsSnapshot().snapshot_compaction_completions < 2 and attempts < 5_000) : (attempts += 1) {
        _ = try host.drainReady(0);
        sleepOneMillisecond();
    }
    try std.testing.expectEqual(@as(usize, 2), host.metricsSnapshot().snapshot_compaction_completions);
    try std.testing.expectEqual(@as(core.types.GroupId, 69), storage_recorder.compacted_groups[0]);
    try std.testing.expectEqual(@as(core.types.GroupId, 68), storage_recorder.compacted_groups[1]);
}

test "multi raft shutdown cancels a blocked snapshot materialization" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(70, &store);
    var apply_recorder = ApplyRecorder{
        .alloc = std.testing.allocator,
        .block_snapshot_materialization = true,
    };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .applied_log_retained_entries = 1,
        .applied_log_compaction_min_interval_entries = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    var host_live = true;
    defer if (host_live) host.deinit();

    try addSingleNodeGroup(&host, 70, &store, false);
    try host.group(70).?.campaign();
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 70));
    try host.propose(70, "blocked-build");
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 70));

    const start_deadline = clock.monotonicNs() +| 5 * std.time.ns_per_s;
    while (!apply_recorder.snapshot_materialization_started.load(.acquire) and clock.monotonicNs() < start_deadline) {
        sleepOneMillisecond();
    }
    try std.testing.expect(apply_recorder.snapshot_materialization_started.load(.acquire));

    host.deinit();
    host_live = false;
    try std.testing.expect(apply_recorder.release_snapshot_materialization.load(.acquire));
}

test "multi raft snapshot scheduling is fair when a hot group requeues" {
    var hot_store = core.MemoryStorage.init(std.testing.allocator);
    defer hot_store.deinit();
    var cold_store = core.MemoryStorage.init(std.testing.allocator);
    defer cold_store.deinit();
    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(66, &hot_store);
    try storage_recorder.registerStore(67, &cold_store);
    var apply_recorder = ApplyRecorder{
        .alloc = std.testing.allocator,
        .block_snapshot_materialization = true,
    };
    defer apply_recorder.release_snapshot_materialization.store(true, .release);

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .applied_log_retained_entries = 1,
        .applied_log_compaction_min_interval_entries = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 66, &hot_store, false);
    try addSingleNodeGroup(&host, 67, &cold_store, false);
    try host.group(66).?.campaign();
    try host.group(67).?.campaign();
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 66));
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 67));
    try host.propose(66, "hot-0");
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 66));

    const start_deadline = clock.monotonicNs() +| 5 * std.time.ns_per_s;
    while (!apply_recorder.snapshot_materialization_started.load(.acquire) and clock.monotonicNs() < start_deadline) {
        sleepOneMillisecond();
    }
    try std.testing.expect(apply_recorder.snapshot_materialization_started.load(.acquire));
    try host.propose(67, "cold");
    try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 67));
    for (1..5) |i| {
        var payload: [16]u8 = undefined;
        try host.propose(66, try std.fmt.bufPrint(&payload, "hot-{d}", .{i}));
        try std.testing.expectEqual(@as(usize, 1), try drainGroup(&host, 66));
    }

    apply_recorder.release_snapshot_materialization.store(true, .release);
    const completion_deadline = clock.monotonicNs() +| 5 * std.time.ns_per_s;
    while (apply_recorder.snapshot_materializations.load(.acquire) < 3 and clock.monotonicNs() < completion_deadline) {
        _ = try host.drainReady(0);
        sleepOneMillisecond();
    }
    try std.testing.expect(apply_recorder.snapshot_materializations.load(.acquire) >= 3);
    try std.testing.expectEqual(@as(core.types.GroupId, 66), apply_recorder.materialized_groups[0].load(.acquire));
    try std.testing.expectEqual(@as(core.types.GroupId, 67), apply_recorder.materialized_groups[1].load(.acquire));
}

test "multi raft quiescing skips host round and drain fairness" {
    var store_a = core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(71, &store_a);
    try storage_recorder.registerStore(72, &store_b);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 71, &store_a, false);
    try addSingleNodeGroup(&host, 72, &store_b, false);
    try host.group(71).?.campaign();
    try host.group(72).?.campaign();
    try host.quiesceGroup(71);

    const round = try host.runRound(8, 8);
    try std.testing.expectEqual(@as(usize, 1), round.ticked_groups);
    try std.testing.expectEqual(@as(usize, 1), round.processed_groups);
    try std.testing.expect(host.group(71).?.hasReady());
    try std.testing.expect(!host.group(72).?.hasReady());

    try host.resumeGroup(71);
    try std.testing.expectEqual(@as(usize, 1), (try host.drainReady(8)).processed_ready_steps);
    try std.testing.expect(!host.group(71).?.hasReady());
}

test "runtime control plane commands drive lifecycle" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{});
    defer host.deinit();

    var peers = [_]core.types.NodeId{1};
    try runtime.control_plane.apply(&host, .{
        .add_group = .{
            .group_id = 81,
            .local_node_id = 1,
            .raft_config = .{
                .id = 1,
                .group_id = 81,
                .peers = peers[0..],
                .election_tick = 5,
                .heartbeat_tick = 1,
                .pre_vote = false,
            },
            .storage = store.storage(),
        },
    });

    try std.testing.expect(host.group(81) != null);
    try runtime.control_plane.apply(&host, .{ .quiesce_group = 81 });
    try std.testing.expect(host.isGroupQuiesced(81));
    try runtime.control_plane.apply(&host, .{ .resume_group = 81 });
    try std.testing.expect(!host.isGroupQuiesced(81));
    try runtime.control_plane.apply(&host, .{ .campaign_group = 81 });
    try std.testing.expect(host.group(81).?.hasReady());
    try runtime.control_plane.apply(&host, .{ .remove_group = 81 });
    try std.testing.expect(host.group(81) == null);
}

test "multi raft uses disk batcher and apply queue across a host round" {
    var store_a = core.MemoryStorage.init(std.testing.allocator);
    defer store_a.deinit();
    var store_b = core.MemoryStorage.init(std.testing.allocator);
    defer store_b.deinit();

    var disk_batcher = DiskBatcherRecorder{ .alloc = std.testing.allocator };
    defer disk_batcher.deinit();
    try disk_batcher.registerStore(91, &store_a);
    try disk_batcher.registerStore(92, &store_b);

    var apply_queue = ApplyQueueRecorder{};
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .disk_batcher = disk_batcher.iface(),
        .apply_queue = apply_queue.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 91, &store_a, false);
    try addSingleNodeGroup(&host, 92, &store_b, false);
    try host.group(91).?.campaign();
    try host.group(92).?.campaign();

    const round = try host.runRound(2, 8);
    try std.testing.expectEqual(@as(usize, 2), round.processed_groups);
    try std.testing.expectEqual(@as(usize, 1), disk_batcher.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), disk_batcher.finish_calls);
    try std.testing.expectEqual(@as(usize, 2), disk_batcher.persist_calls);
    try std.testing.expectEqual(@as(usize, 2), disk_batcher.persisted_entries);
    try std.testing.expectEqual(@as(usize, 2), apply_queue.enqueue_calls);
    try std.testing.expectEqual(@as(usize, 1), apply_queue.drain_calls);
    try std.testing.expectEqual(@as(usize, 2), apply_queue.applied_entries);

    const metrics = host.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 2), metrics.group_count);
    try std.testing.expectEqual(@as(usize, 0), metrics.quiesced_group_count);
    try std.testing.expectEqual(@as(usize, 1), metrics.rounds);
    try std.testing.expectEqual(@as(u64, 1), metrics.virtual_round);
    try std.testing.expectEqual(@as(u64, 100), metrics.virtual_time_ms);
    try std.testing.expectEqual(@as(usize, 2), metrics.ticked_groups);
    try std.testing.expectEqual(@as(usize, 2), metrics.processed_groups);
    try std.testing.expectEqual(@as(usize, 1), metrics.persist_batches);
    try std.testing.expectEqual(@as(usize, 1), metrics.apply_queue_drains);
}

test "multi raft retires a successful apply prefix before retrying a failed suffix" {
    for ([_]bool{ false, true }) |use_apply_queue| {
        var store_a = core.MemoryStorage.init(std.testing.allocator);
        defer store_a.deinit();
        var store_b = core.MemoryStorage.init(std.testing.allocator);
        defer store_b.deinit();

        var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
        defer storage_recorder.deinit();
        try storage_recorder.registerStore(171, &store_a);
        try storage_recorder.registerStore(172, &store_b);

        var apply_recorder = PartialApplyRecorder{};
        var apply_worker = runtime.QueuedApplyWorker.init(std.testing.allocator, apply_recorder.iface());
        defer apply_worker.deinit();
        var hooks = runtime.RuntimeHooks{
            .group_storage = storage_recorder.iface(),
            .state_machine = apply_recorder.iface(),
        };
        if (use_apply_queue) hooks.apply_queue = apply_worker.queue();

        var host = runtime.MultiRaft.init(std.testing.allocator, .{}, hooks);
        defer host.deinit();
        try addSingleNodeGroup(&host, 171, &store_a, false);
        try addSingleNodeGroup(&host, 172, &store_b, false);
        try host.group(171).?.campaign();
        try host.group(172).?.campaign();

        try std.testing.expectError(error.InjectedApplyFailure, host.drainReady(8));
        try std.testing.expectEqual(@as(usize, 1), apply_recorder.successful_by_group[171] + apply_recorder.successful_by_group[172]);

        try std.testing.expectEqual(@as(usize, 0), (try host.drainReady(0)).processed_ready_steps);
        try std.testing.expectEqual(@as(usize, 3), apply_recorder.attempts);
        try std.testing.expectEqual(@as(usize, 1), apply_recorder.successful_by_group[171]);
        try std.testing.expectEqual(@as(usize, 1), apply_recorder.successful_by_group[172]);
    }
}

test "multi raft skips persistence for message-only ready" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(155, &store);

    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    try host.addGroup(.{
        .group_id = 155,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 155,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });

    try host.group(155).?.campaign();
    _ = try drainGroup(&host, 155);
    try host.step(155, .{
        .msg_type = .request_vote_response,
        .from = 2,
        .to = 1,
        .term = 1,
    });
    _ = try drainGroup(&host, 155);
    try std.testing.expectEqual(core.types.StateRole.leader, host.group(155).?.status().soft.role);

    const persisted_before_heartbeat = storage_recorder.persist_calls;
    transport_recorder.send_calls = 0;
    transport_recorder.peer_batch_calls = 0;
    transport_recorder.sent_messages = 0;
    transport_recorder.batched_peer_count = 0;
    transport_recorder.batched_group_count = 0;

    const round = try host.runRound(1, 1);
    try std.testing.expectEqual(@as(usize, 1), round.processed_groups);
    try std.testing.expect(transport_recorder.sent_messages > 0);
    try std.testing.expectEqual(persisted_before_heartbeat, storage_recorder.persist_calls);
    try std.testing.expect(round.slowest_ready_group.persist_ready_detail.skipped_no_durable_state);
    try std.testing.expect(!round.slowest_ready_group.persist_ready_detail.used_group_storage);
    try std.testing.expect(!round.slowest_ready_group.persist_ready_detail.used_batch);
}

test "multi raft metrics track quiesced groups and throttle denials" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(93, &store);

    var throttle = SnapshotThrottleRecorder{ .allow = false };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .snapshot_throttle = throttle.iface(),
    });
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    try host.addGroup(.{
        .group_id = 93,
        .local_node_id = 2,
        .raft_config = .{
            .id = 2,
            .group_id = 93,
            .peers = peers[0..],
            .election_tick = 10,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });
    try host.quiesceGroup(93);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().quiesced_group_count);
    try host.resumeGroup(93);

    try host.step(93, .{
        .msg_type = .snapshot,
        .from = 1,
        .to = 2,
        .term = 2,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = peers[0..],
                },
            },
            .data = &.{},
        },
    });

    try std.testing.expectEqual(false, try host.processReady(93));
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().snapshot_throttle_denials);
}

test "multi raft routes outbound snapshots through snapshot transport" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var voters = [_]core.types.NodeId{ 1, 2 };
    try store.applySnapshot(.{
        .metadata = .{
            .index = 11,
            .term = 11,
            .conf_state = .{
                .voters = voters[0..],
            },
        },
        .data = &.{},
    });
    store.setHardState(.{
        .current_term = 11,
        .commit_index = 11,
    });

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(94, &store);

    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };
    var snapshot_transport = SnapshotTransportRecorder{};

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .transport = transport_recorder.iface(),
        .snapshot_transport = snapshot_transport.iface(),
    });
    defer host.deinit();

    try host.addGroup(.{
        .group_id = 94,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 94,
            .peers = voters[0..],
            .election_tick = 10,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });

    try host.campaignGroup(94);
    try host.step(94, .{
        .msg_type = .request_vote_response,
        .from = 2,
        .to = 1,
        .term = 12,
    });
    _ = try host.processReady(94);
    transport_recorder.sent_messages = 0;
    transport_recorder.send_calls = 0;
    transport_recorder.peer_batch_calls = 0;
    transport_recorder.batched_peer_count = 0;
    transport_recorder.batched_group_count = 0;

    const grp = host.group(94).?;
    grp.raw_node.raft.progress[1] = .{
        .match_index = 0,
        .next_index = 1,
        .state = .probe,
        .probe_sent = false,
        .recent_active = true,
    };

    try host.step(94, .{
        .msg_type = .append_entries_response,
        .from = 2,
        .to = 1,
        .term = 12,
        .log_index = 0,
        .reject = true,
        .reject_hint = 0,
    });

    try std.testing.expectEqual(true, try host.processReady(94));
    try std.testing.expectEqual(@as(usize, 1), snapshot_transport.send_calls);
    try std.testing.expectEqual(@as(core.types.GroupId, 94), snapshot_transport.last_group_id);
    try std.testing.expectEqual(@as(core.types.NodeId, 2), snapshot_transport.last_to);
    try std.testing.expectEqual(@as(usize, 0), transport_recorder.sent_messages);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().transport_snapshot_sends);
}

test "multi raft fetches snapshot through snapshot transport and steps it into the group" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(95, &store);

    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    const root_dir = "/tmp/antflydb-raft-runtime-fetch-snapshot";
    var snapshot_transport = try runtime.LocalSnapshotTransport.init(std.testing.allocator, root_dir);
    defer snapshot_transport.deinit();

    var voters = [_]core.types.NodeId{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.dupe(u8, "runtime-snapshot");
    defer std.testing.allocator.free(snapshot_bytes);
    try snapshot_transport.transport().sendSnapshot(.{
        .group_id = 95,
        .to = 2,
        .term = 4,
        .snapshot = .{
            .metadata = .{
                .index = 11,
                .term = 11,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "runtime-fetch" },
    });

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .transport = transport_recorder.iface(),
        .snapshot_transport = snapshot_transport.transport(),
    });
    defer host.deinit();

    try host.addGroup(.{
        .group_id = 95,
        .local_node_id = 2,
        .raft_config = .{
            .id = 2,
            .group_id = 95,
            .peers = voters[0..],
            .election_tick = 10,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });

    try runtime.control_plane.apply(&host, .{
        .fetch_snapshot = .{
            .group_id = 95,
            .from = 1,
            .term = 4,
            .locator = .{ .snapshot_id = "runtime-fetch" },
        },
    });

    try std.testing.expect(host.group(95).?.hasReady());
    try std.testing.expectEqual(true, try host.processReady(95));
    try std.testing.expectEqual(@as(core.types.Index, 11), store.snapshot_state.metadata.index);
    try std.testing.expectEqualStrings("runtime-snapshot", store.snapshot_state.data);
    try std.testing.expectEqual(@as(usize, 1), transport_recorder.sent_messages);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().transport_message_sends);
}

test "multi raft ensureReplica creates persisted replica and can remove it" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    store.setHardState(.{
        .current_term = 3,
        .commit_index = 2,
    });

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{});
    defer host.deinit();

    var peers = [_]core.types.NodeId{1};
    const result = try host.ensureReplica(.{
        .group = .{
            .group_id = 131,
            .local_node_id = 1,
            .raft_config = .{
                .id = 1,
                .group_id = 131,
                .peers = peers[0..],
                .election_tick = 5,
                .heartbeat_tick = 1,
                .pre_vote = false,
            },
            .storage = store.storage(),
        },
        .bootstrap = .persisted,
    });
    try std.testing.expect(result.created);
    try std.testing.expectEqual(@as(core.types.Index, 2), host.group(131).?.status().hard.commit_index);

    try host.removeReplica(131);
    try std.testing.expect(host.group(131) == null);
}

test "multi raft ensureReplica can fetch snapshot bootstrap" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    const root_dir = "/tmp/antflydb-raft-runtime-ensure-fetch-snapshot";
    var snapshot_transport = try runtime.LocalSnapshotTransport.init(std.testing.allocator, root_dir);
    defer snapshot_transport.deinit();

    var voters = [_]core.types.NodeId{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.dupe(u8, "ensure-snapshot");
    defer std.testing.allocator.free(snapshot_bytes);
    try snapshot_transport.transport().sendSnapshot(.{
        .group_id = 132,
        .to = 2,
        .term = 6,
        .snapshot = .{
            .metadata = .{
                .index = 15,
                .term = 6,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "ensure-rejoin" },
    });

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(132, &store);

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .snapshot_transport = snapshot_transport.transport(),
    });
    defer host.deinit();

    const result = try host.ensureReplica(.{
        .group = .{
            .group_id = 132,
            .local_node_id = 2,
            .raft_config = .{
                .id = 2,
                .group_id = 132,
                .peers = voters[0..],
                .election_tick = 10,
                .heartbeat_tick = 1,
                .pre_vote = false,
            },
            .storage = store.storage(),
        },
        .bootstrap = .{
            .fetch_snapshot = .{
                .from = 1,
                .term = 6,
                .locator = .{ .snapshot_id = "ensure-rejoin" },
            },
        },
    });
    try std.testing.expect(result.created);
    try std.testing.expect(result.fetched_snapshot);
    try std.testing.expect(host.group(132).?.hasReady());
    try std.testing.expectEqual(true, try host.processReady(132));
    try std.testing.expectEqual(@as(core.types.Index, 15), store.snapshot_state.metadata.index);
}

test "multi raft backpressure can defer ready processing" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(133, &store);

    var backpressure = BackpressureRecorder{ .allow = false };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .backpressure = backpressure.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 133, &store, false);
    try host.campaignGroup(133);

    try std.testing.expectEqual(false, try host.processReady(133));
    try std.testing.expect(host.group(133).?.hasReady());
    try std.testing.expectEqual(@as(usize, 1), backpressure.calls);
    try std.testing.expect(backpressure.last_pressure.unstable_entries > 0);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().backpressure_denials);

    backpressure.allow = true;
    try std.testing.expectEqual(true, try host.processReady(133));
    try std.testing.expectEqual(@as(core.types.Index, 1), store.hard_state.commit_index);
}

test "multi raft backpressure rejects async ready before cloning messages" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(135, &store);

    var backpressure = BackpressureRecorder{ .allow = false };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .backpressure = backpressure.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 135, &store, true);
    try host.campaignGroup(135);
    try std.testing.expect(host.group(135).?.hasReady());

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const original_alloc = host.alloc;
    const process_result = blk: {
        host.alloc = failing.allocator();
        defer host.alloc = original_alloc;
        break :blk host.processReady(135);
    };
    try std.testing.expectEqual(false, try process_result);
    try std.testing.expectEqual(@as(usize, 0), failing.alloc_index);
}

test "multi raft limit backpressure denies oversized snapshot ready" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(134, &store);

    const root_dir = "/tmp/antflydb-raft-runtime-limit-backpressure";
    var snapshot_transport = try runtime.LocalSnapshotTransport.init(std.testing.allocator, root_dir);
    defer snapshot_transport.deinit();

    var voters = [_]core.types.NodeId{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.dupe(u8, "oversized-snapshot");
    defer std.testing.allocator.free(snapshot_bytes);
    try snapshot_transport.transport().sendSnapshot(.{
        .group_id = 134,
        .to = 2,
        .term = 6,
        .snapshot = .{
            .metadata = .{
                .index = 9,
                .term = 6,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "oversized" },
    });

    var backpressure = runtime.LimitBackpressure.init(.{
        .max_snapshot_bytes = 4,
    });
    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .snapshot_transport = snapshot_transport.transport(),
        .backpressure = backpressure.policy(),
    });
    defer host.deinit();

    _ = try host.ensureReplica(.{
        .group = .{
            .group_id = 134,
            .local_node_id = 2,
            .raft_config = .{
                .id = 2,
                .group_id = 134,
                .peers = voters[0..],
                .election_tick = 10,
                .heartbeat_tick = 1,
                .pre_vote = false,
            },
            .storage = store.storage(),
        },
        .bootstrap = .{
            .fetch_snapshot = .{
                .from = 1,
                .term = 6,
                .locator = .{ .snapshot_id = "oversized" },
            },
        },
    });

    try std.testing.expect(host.group(134).?.hasReady());
    try std.testing.expectEqual(false, try host.processReady(134));
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().backpressure_denials);
    try std.testing.expectEqual(@as(usize, 1), backpressure.denials);
}

test "multi raft transport queue defers outbound messages across rounds" {
    var store1 = core.MemoryStorage.init(std.testing.allocator);
    defer store1.deinit();
    var store2 = core.MemoryStorage.init(std.testing.allocator);
    defer store2.deinit();

    var transport = TransportRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_transport_messages_per_round = 1,
    }, .{
        .transport = transport.iface(),
    });
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    try host.addGroup(.{
        .group_id = 151,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 151,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store1.storage(),
    });
    try host.addGroup(.{
        .group_id = 152,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 152,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store2.storage(),
    });

    try host.campaignGroup(151);
    try host.campaignGroup(152);

    try std.testing.expectEqual(@as(usize, 2), (try host.drainReady(8)).processed_ready_steps);
    try std.testing.expectEqual(@as(usize, 1), transport.sent_messages);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().pending_outbound_messages);

    try std.testing.expectEqual(@as(usize, 0), (try host.drainReady(0)).processed_ready_steps);
    try std.testing.expectEqual(@as(usize, 2), transport.sent_messages);
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().pending_outbound_messages);
}

test "multi raft transport queue denial leaves ready pending" {
    var store1 = core.MemoryStorage.init(std.testing.allocator);
    defer store1.deinit();
    var store2 = core.MemoryStorage.init(std.testing.allocator);
    defer store2.deinit();

    var transport = TransportRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_pending_outbound_messages = 1,
        .max_transport_messages_per_round = 0,
    }, .{
        .transport = transport.iface(),
    });
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    try host.addGroup(.{
        .group_id = 153,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 153,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store1.storage(),
    });
    try host.addGroup(.{
        .group_id = 154,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 154,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store2.storage(),
    });

    try host.campaignGroup(153);
    try host.campaignGroup(154);

    try std.testing.expectEqual(@as(usize, 1), (try host.drainReady(8)).processed_ready_steps);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().pending_outbound_messages);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().transport_queue_denials);
    try std.testing.expect(host.group(154).?.hasReady());
}

test "multi raft drainReady admits one oversized outbound ready only into an empty queue" {
    var store1 = core.MemoryStorage.init(std.testing.allocator);
    defer store1.deinit();
    var store2 = core.MemoryStorage.init(std.testing.allocator);
    defer store2.deinit();

    var transport = TransportRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_pending_outbound_messages = 8,
        .max_pending_outbound_bytes = 1,
        .max_single_outbound_ready_bytes = 4096,
        .max_transport_messages_per_round = 0,
    }, .{ .transport = transport.iface() });
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    for ([_]struct { id: u64, store: *core.MemoryStorage }{
        .{ .id = 253, .store = &store1 },
        .{ .id = 254, .store = &store2 },
    }) |item| {
        try host.addGroup(.{
            .group_id = item.id,
            .local_node_id = 1,
            .raft_config = .{
                .id = 1,
                .group_id = item.id,
                .peers = peers[0..],
                .election_tick = 5,
                .heartbeat_tick = 1,
                .pre_vote = false,
            },
            .storage = item.store.storage(),
        });
        try host.campaignGroup(item.id);
    }

    try std.testing.expectEqual(@as(usize, 1), (try host.drainReady(8)).processed_ready_steps);
    try std.testing.expect(host.metricsSnapshot().pending_outbound_bytes > 1);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().transport_queue_denials);
    try std.testing.expect(host.group(254).?.hasReady());
}

test "multi raft apply queue drains with per-round budget" {
    var store1 = core.MemoryStorage.init(std.testing.allocator);
    defer store1.deinit();
    var store2 = core.MemoryStorage.init(std.testing.allocator);
    defer store2.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(155, &store1);
    try storage_recorder.registerStore(156, &store2);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_apply_tasks_per_round = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 155, &store1, false);
    try addSingleNodeGroup(&host, 156, &store2, false);
    try host.campaignGroup(155);
    try host.campaignGroup(156);

    try std.testing.expectEqual(@as(usize, 2), (try host.drainReady(8)).processed_ready_steps);
    try std.testing.expectEqual(@as(usize, 1), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().pending_apply_tasks);

    try std.testing.expectEqual(@as(usize, 0), (try host.drainReady(0)).processed_ready_steps);
    try std.testing.expectEqual(@as(usize, 2), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().pending_apply_tasks);
}

test "multi raft drainReady admits one oversized apply ready only into an empty queue" {
    var store1 = core.MemoryStorage.init(std.testing.allocator);
    defer store1.deinit();
    var store2 = core.MemoryStorage.init(std.testing.allocator);
    defer store2.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(255, &store1);
    try storage_recorder.registerStore(256, &store2);
    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_pending_apply_tasks = 8,
        .max_pending_apply_bytes = 1,
        .max_single_apply_ready_bytes = 4096,
        .max_apply_tasks_per_round = 0,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 255, &store1, false);
    try addSingleNodeGroup(&host, 256, &store2, false);
    try host.campaignGroup(255);
    try host.campaignGroup(256);

    try std.testing.expectEqual(@as(usize, 1), (try host.drainReady(8)).processed_ready_steps);
    try std.testing.expect(host.metricsSnapshot().pending_apply_bytes > 1);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().apply_queue_denials);
    try std.testing.expect(host.group(256).?.hasReady());
}

test "multi raft drainReady always makes progress on one oversized Ready" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(257, &store);
    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_pending_outbound_bytes = 1,
        .max_pending_apply_bytes = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 257, &store, false);
    try host.campaignGroup(257);

    try std.testing.expectEqual(@as(usize, 1), (try host.drainReady(1)).processed_ready_steps);
    try std.testing.expect(!host.group(257).?.hasReady());
}

test "multi raft drainReady surfaces an impossible outbound Ready hard ceiling" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_pending_outbound_bytes = 1,
        .max_single_outbound_ready_bytes = 1,
    }, .{});
    defer host.deinit();

    var peers = [_]core.types.NodeId{ 1, 2 };
    try host.addGroup(.{
        .group_id = 258,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 258,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });
    try host.campaignGroup(258);

    try std.testing.expectError(error.OutboundReadyTooLarge, host.drainReady(1));
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().pending_outbound_bytes);
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().transport_queue_denials);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().oversized_outbound_ready_rejections);
    try std.testing.expect(host.group(258).?.hasReady());
}

test "multi raft drainReady surfaces an impossible apply Ready hard ceiling" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(259, &store);
    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_pending_apply_bytes = 1,
        .max_single_apply_ready_bytes = 1,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 259, &store, false);
    try host.campaignGroup(259);

    try std.testing.expectError(error.ApplyReadyTooLarge, host.drainReady(1));
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().pending_apply_bytes);
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().apply_queue_denials);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().oversized_apply_ready_rejections);
    try std.testing.expect(host.group(259).?.hasReady());
}

test "multi raft removal drops pending applies and retires replica storage" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(157, &store);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var host = runtime.MultiRaft.init(std.testing.allocator, .{
        .max_apply_tasks_per_round = 0,
    }, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 157, &store, false);
    try host.campaignGroup(157);
    try std.testing.expect(try host.processReady(157));
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().pending_apply_tasks);

    try host.removeReplica(157);
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().pending_apply_tasks);
    try std.testing.expectEqual(@as(usize, 0), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(usize, 1), storage_recorder.retired_groups);
}

test "multi raft catalog failure leaves replica hosted for reconciliation retry" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(158, &store);
    var replica_catalog = FailingRemovalCatalog{};

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .replica_catalog = replica_catalog.iface(),
    });
    defer host.deinit();
    try addSingleNodeGroup(&host, 158, &store, false);

    try std.testing.expectError(error.InjectedCatalogRemovalFailure, host.removeReplica(158));
    try std.testing.expect(host.group(158) != null);
    try std.testing.expectEqual(@as(usize, 0), storage_recorder.retired_groups);

    replica_catalog.fail_remove = false;
    try host.removeReplica(158);
    try std.testing.expect(host.group(158) == null);
    try std.testing.expectEqual(@as(usize, 1), storage_recorder.retired_groups);
    try std.testing.expectEqual(@as(usize, 2), replica_catalog.remove_calls);
}

test "in-memory disk batcher and queued apply worker integrate with host" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var disk_batcher = runtime.InMemoryDiskBatcher.init(std.testing.allocator);
    defer disk_batcher.deinit();
    try disk_batcher.registerStore(101, &store);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var apply_worker = runtime.QueuedApplyWorker.init(std.testing.allocator, apply_recorder.iface());
    defer apply_worker.deinit();

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .disk_batcher = disk_batcher.batcher(),
        .apply_queue = apply_worker.queue(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 101, &store, false);
    try host.campaignGroup(101);
    const passes = try drainGroup(&host, 101);
    try std.testing.expect(passes > 0);
    try std.testing.expectEqual(@as(core.types.Index, 1), store.hard_state.commit_index);
    try std.testing.expectEqual(@as(usize, 1), apply_recorder.applied_entries);
}

test "multi raft resumes quiesced group on inbound and local activity" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{});
    defer host.deinit();

    var peers = [_]core.types.NodeId{1};
    try host.addGroup(.{
        .group_id = 111,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 111,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });

    try host.quiesceGroup(111);
    try std.testing.expect(host.isGroupQuiesced(111));

    try host.step(111, .{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 1,
        .term = 1,
    });
    try std.testing.expect(!host.isGroupQuiesced(111));

    try host.quiesceGroup(111);
    try host.propose(111, "x");
    try std.testing.expect(!host.isGroupQuiesced(111));
}

test "multi raft serves groups and routes peer lifecycle through transport" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var transport = runtime.InMemoryTransportHost.init(std.testing.allocator);
    defer transport.deinit();

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .transport = transport.transport(),
    });
    defer host.deinit();

    var peers = [_]core.types.NodeId{1};
    try host.addGroup(.{
        .group_id = 121,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 121,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = store.storage(),
    });

    try std.testing.expect(transport.isServing(121));
    try runtime.control_plane.apply(&host, .{
        .add_peer = .{
            .group_id = 121,
            .peer = .{
                .node_id = 2,
                .endpoints = &.{.{ .protocol = .http3, .address = "https://node2" }},
            },
        },
    });
    try std.testing.expectEqual(@as(usize, 1), transport.peerCount(121));

    try runtime.control_plane.apply(&host, .{
        .remove_peer = .{
            .group_id = 121,
            .node_id = 2,
        },
    });
    try std.testing.expectEqual(@as(usize, 0), transport.peerCount(121));

    try runtime.control_plane.apply(&host, .{ .remove_group = 121 });
    try std.testing.expect(!transport.isServing(121));
    const metrics = host.metricsSnapshot();
    try std.testing.expectEqual(@as(usize, 1), metrics.transport_group_serves);
    try std.testing.expectEqual(@as(usize, 1), metrics.transport_group_unserves);
    try std.testing.expectEqual(@as(usize, 1), metrics.transport_peer_adds);
    try std.testing.expectEqual(@as(usize, 1), metrics.transport_peer_removes);
}

test "multi raft restoreReplicasFromCatalog reconstructs persisted replica" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    store.setHardState(.{
        .current_term = 4,
        .commit_index = 7,
    });

    var catalog = runtime.MemoryReplicaCatalog.init(std.testing.allocator);
    defer catalog.deinit();

    var factory = runtime.MemoryReplicaFactory.init(std.testing.allocator);
    defer factory.deinit();
    try factory.registerStore(141, &store);

    var peers = [_]core.types.NodeId{1};
    try catalog.catalog().upsertReplica(.{
        .group_id = 141,
        .local_node_id = 1,
        .raft = .{
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .bootstrap = .persisted,
    });

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .replica_catalog = catalog.catalog(),
        .replica_factory = factory.factory(),
    });
    defer host.deinit();

    try std.testing.expectEqual(@as(usize, 1), try host.restoreReplicasFromCatalog(std.testing.allocator));
    try std.testing.expect(host.group(141) != null);
    try std.testing.expectEqual(@as(core.types.Index, 7), host.group(141).?.status().hard.commit_index);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().restored_replicas);
}

test "runtime control plane restore_replicas can rejoin via snapshot bootstrap" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(142, &store);

    var catalog = runtime.MemoryReplicaCatalog.init(std.testing.allocator);
    defer catalog.deinit();

    var factory = runtime.MemoryReplicaFactory.init(std.testing.allocator);
    defer factory.deinit();
    try factory.registerStore(142, &store);

    const root_dir = "/tmp/antflydb-raft-runtime-catalog-rejoin";
    var snapshot_transport = try runtime.LocalSnapshotTransport.init(std.testing.allocator, root_dir);
    defer snapshot_transport.deinit();

    var voters = [_]core.types.NodeId{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.dupe(u8, "catalog-rejoin");
    defer std.testing.allocator.free(snapshot_bytes);
    try snapshot_transport.transport().sendSnapshot(.{
        .group_id = 142,
        .to = 2,
        .term = 6,
        .snapshot = .{
            .metadata = .{
                .index = 20,
                .term = 6,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "catalog-rejoin" },
    });

    try catalog.catalog().upsertReplica(.{
        .group_id = 142,
        .local_node_id = 2,
        .raft = .{
            .peers = voters[0..],
            .election_tick = 10,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .bootstrap = .{
            .fetch_snapshot = .{
                .from = 1,
                .term = 6,
                .locator = .{ .snapshot_id = "catalog-rejoin" },
            },
        },
    });

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .snapshot_transport = snapshot_transport.transport(),
        .replica_catalog = catalog.catalog(),
        .replica_factory = factory.factory(),
    });
    defer host.deinit();

    try runtime.control_plane.apply(&host, .restore_replicas);
    try std.testing.expect(host.group(142) != null);
    try std.testing.expect(host.group(142).?.hasReady());
    try std.testing.expectEqual(true, try host.processReady(142));
    try std.testing.expectEqual(@as(core.types.Index, 20), store.snapshot_state.metadata.index);
    try std.testing.expectEqualStrings("catalog-rejoin", store.snapshot_state.data);
}

test "multi raft restoreReplicasFromCatalog works with file replica catalog" {
    const path = "/tmp/antflydb-raft-runtime-file-catalog.bin";
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    store.setHardState(.{
        .current_term = 5,
        .commit_index = 3,
    });

    {
        var catalog = try runtime.FileReplicaCatalog.init(std.testing.allocator, path);
        defer catalog.deinit();

        var peers = [_]core.types.NodeId{1};
        try catalog.catalog().upsertReplica(.{
            .group_id = 143,
            .local_node_id = 1,
            .raft = .{
                .peers = peers[0..],
                .election_tick = 5,
                .heartbeat_tick = 1,
                .pre_vote = false,
            },
            .bootstrap = .persisted,
        });
    }

    var catalog = try runtime.FileReplicaCatalog.init(std.testing.allocator, path);
    defer catalog.deinit();

    var factory = runtime.MemoryReplicaFactory.init(std.testing.allocator);
    defer factory.deinit();
    try factory.registerStore(143, &store);

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .replica_catalog = catalog.catalog(),
        .replica_factory = factory.factory(),
    });
    defer host.deinit();

    try std.testing.expectEqual(@as(usize, 1), try host.restoreReplicasFromCatalog(std.testing.allocator));
    try std.testing.expectEqual(@as(core.types.Index, 3), host.group(143).?.status().hard.commit_index);
}
