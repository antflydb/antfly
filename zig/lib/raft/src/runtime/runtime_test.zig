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

const StorageRecorder = struct {
    alloc: std.mem.Allocator,
    stores: std.AutoHashMapUnmanaged(core.types.GroupId, *core.MemoryStorage) = .empty,
    persist_calls: usize = 0,
    persisted_entries: usize = 0,
    persisted_snapshots: usize = 0,

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
        if (ready.entries.len > 0) {
            try store.append(ready.entries);
        }
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
        if (ready.entries.len > 0) try store.append(ready.entries);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *DiskBatcherRecorder = @ptrCast(@alignCast(ptr));
        self.finish_calls += 1;
    }
};

const ApplyRecorder = struct {
    alloc: std.mem.Allocator,
    apply_calls: usize = 0,
    applied_entries: usize = 0,
    applied_read_states: usize = 0,
    last_applied_index: core.types.Index = 0,
    last_read_index: core.types.Index = 0,

    fn iface(self: *ApplyRecorder) runtime.storage_iface.StateMachine {
        return .{
            .ptr = self,
            .vtable = &.{
                .apply_ready = applyReady,
            },
        };
    }

    fn applyReady(
        ptr: *anyopaque,
        group_id: core.types.GroupId,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        _ = group_id;
        const self: *ApplyRecorder = @ptrCast(@alignCast(ptr));
        self.apply_calls += 1;
        self.applied_entries += committed_entries.len;
        self.applied_read_states += read_states.len;
        if (committed_entries.len > 0) {
            self.last_applied_index = committed_entries[committed_entries.len - 1].index;
        }
        if (read_states.len > 0) {
            self.last_read_index = read_states[read_states.len - 1].index;
        }
    }
};

const ApplyQueueRecorder = struct {
    enqueue_calls: usize = 0,
    drain_calls: usize = 0,
    applied_entries: usize = 0,
    applied_read_states: usize = 0,

    fn iface(self: *ApplyQueueRecorder) runtime.storage_iface.ApplyQueue {
        return .{
            .ptr = self,
            .vtable = &.{
                .enqueue_apply = enqueueApply,
                .drain = drain,
            },
        };
    }

    fn enqueueApply(
        ptr: *anyopaque,
        group_id: core.types.GroupId,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        _ = group_id;
        const self: *ApplyQueueRecorder = @ptrCast(@alignCast(ptr));
        self.enqueue_calls += 1;
        self.applied_entries += committed_entries.len;
        self.applied_read_states += read_states.len;
    }

    fn drain(ptr: *anyopaque) !void {
        const self: *ApplyQueueRecorder = @ptrCast(@alignCast(ptr));
        self.drain_calls += 1;
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
    calls: usize = 0,
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
        self.last_pressure = pressure;
        return self.allow;
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
    try std.testing.expectEqual(@as(usize, 0), try host.drainReady(8));

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

test "multi raft processReady executes async local storage pipeline" {
    var store = core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();

    var storage_recorder = StorageRecorder{ .alloc = std.testing.allocator };
    defer storage_recorder.deinit();
    try storage_recorder.registerStore(21, &store);

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };
    var transport_recorder = TransportRecorder{ .alloc = std.testing.allocator };

    var host = runtime.MultiRaft.init(std.testing.allocator, .{}, .{
        .group_storage = storage_recorder.iface(),
        .state_machine = apply_recorder.iface(),
        .transport = transport_recorder.iface(),
    });
    defer host.deinit();

    try addSingleNodeGroup(&host, 21, &store, true);
    try host.group(21).?.campaign();

    const passes = try drainGroup(&host, 21);
    try std.testing.expect(passes > 1);
    try std.testing.expectEqual(@as(usize, 0), try host.drainReady(8));

    const grp = host.group(21).?;
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

    try std.testing.expectEqual(@as(usize, 1), try host.drainReady(1));

    {
        const ready_ids = try host.readyGroupIds(std.testing.allocator, 8);
        defer std.testing.allocator.free(ready_ids);
        try std.testing.expectEqual(@as(usize, 1), ready_ids.len);
    }

    try std.testing.expectEqual(@as(usize, 1), try host.drainReady(8));
    try std.testing.expectEqual(@as(usize, 0), try host.drainReady(8));

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

    try std.testing.expectEqual(@as(usize, 2), try host.drainReady(8));
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

    var apply_recorder = ApplyRecorder{ .alloc = std.testing.allocator };

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

    const grp = host.group(63) orelse return error.UnknownGroup;
    try std.testing.expectEqual(@as(core.types.Index, 9), apply_recorder.last_applied_index);
    try std.testing.expectEqual(@as(core.types.Index, 8), grp.raw_node.raft.log.firstIndex());
    try std.testing.expectEqual(@as(core.types.Index, 9), grp.raw_node.raft.log.lastIndex());
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
    try std.testing.expectEqual(@as(usize, 1), try host.drainReady(8));
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

    try std.testing.expectEqual(@as(usize, 2), try host.drainReady(8));
    try std.testing.expectEqual(@as(usize, 1), transport.sent_messages);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().pending_outbound_messages);

    try std.testing.expectEqual(@as(usize, 0), try host.drainReady(0));
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

    try std.testing.expectEqual(@as(usize, 1), try host.drainReady(8));
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().pending_outbound_messages);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().transport_queue_denials);
    try std.testing.expect(host.group(154).?.hasReady());
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

    try std.testing.expectEqual(@as(usize, 2), try host.drainReady(8));
    try std.testing.expectEqual(@as(usize, 1), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(usize, 1), host.metricsSnapshot().pending_apply_tasks);

    try std.testing.expectEqual(@as(usize, 0), try host.drainReady(0));
    try std.testing.expectEqual(@as(usize, 2), apply_recorder.applied_entries);
    try std.testing.expectEqual(@as(usize, 0), host.metricsSnapshot().pending_apply_tasks);
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
