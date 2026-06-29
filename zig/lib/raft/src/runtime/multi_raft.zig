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
const clock = @import("clock.zig");
const group_mod = @import("group.zig");
const scheduler_mod = @import("scheduler.zig");
const transport_iface = @import("transport_iface.zig");
const snapshot_transport_iface = @import("snapshot_transport_iface.zig");
const storage_iface = @import("storage_iface.zig");
const snapshot_iface = @import("snapshot_iface.zig");
const backpressure_iface = @import("backpressure_iface.zig");
const replica_mod = @import("replica.zig");
const replica_catalog_iface = @import("replica_catalog_iface.zig");

pub const RuntimeConfig = struct {
    tick_interval_ms: u32 = 100,
    max_groups: u32 = 1024,
    max_tick_batch: usize = 128,
    max_pending_outbound_messages: usize = std.math.maxInt(usize),
    max_pending_outbound_bytes: usize = std.math.maxInt(usize),
    max_transport_messages_per_round: usize = std.math.maxInt(usize),
    max_transport_bytes_per_round: usize = std.math.maxInt(usize),
    max_pending_apply_tasks: usize = std.math.maxInt(usize),
    max_pending_apply_bytes: usize = std.math.maxInt(usize),
    max_apply_tasks_per_round: usize = std.math.maxInt(usize),
    applied_log_retained_entries: u64 = 4096,
    applied_log_compaction_min_interval_entries: u64 = 4096,
    applied_log_compaction_single_node_only: bool = true,
};

pub const RuntimeHooks = struct {
    transport: ?transport_iface.Transport = null,
    snapshot_transport: ?snapshot_transport_iface.SnapshotTransport = null,
    group_storage: ?storage_iface.GroupStorage = null,
    disk_batcher: ?storage_iface.DiskBatcher = null,
    state_machine: ?storage_iface.StateMachine = null,
    apply_queue: ?storage_iface.ApplyQueue = null,
    snapshot_throttle: ?snapshot_iface.SnapshotThrottle = null,
    backpressure: ?backpressure_iface.Backpressure = null,
    replica_catalog: ?replica_catalog_iface.ReplicaCatalog = null,
    replica_factory: ?replica_catalog_iface.ReplicaFactory = null,
};

pub const ReadyGroupDiagnostics = struct {
    group_id: core.types.GroupId = 0,
    elapsed_ns: u64 = 0,
    ready_build_elapsed_ns: u64 = 0,
    backpressure_elapsed_ns: u64 = 0,
    capacity_check_elapsed_ns: u64 = 0,
    snapshot_throttle_elapsed_ns: u64 = 0,
    persist_ready_elapsed_ns: u64 = 0,
    async_ready_elapsed_ns: u64 = 0,
    clone_messages_elapsed_ns: u64 = 0,
    enqueue_apply_elapsed_ns: u64 = 0,
    async_message_loop_elapsed_ns: u64 = 0,
    outbox_append_elapsed_ns: u64 = 0,
    raft_advance_elapsed_ns: u64 = 0,
    inline_apply_flush_elapsed_ns: u64 = 0,
    inline_outbox_drain_elapsed_ns: u64 = 0,
    inline_transport_flush_elapsed_ns: u64 = 0,
    message_count: usize = 0,
    message_bytes: usize = 0,
    committed_entries: usize = 0,
    committed_entry_bytes: usize = 0,
    unstable_entries: usize = 0,
    unstable_entry_bytes: usize = 0,
    read_states: usize = 0,
    has_snapshot: bool = false,
    snapshot_bytes: usize = 0,
    async_storage_writes: bool = false,
    processed: bool = false,
    denied_by_backpressure: bool = false,
    denied_by_transport_capacity: bool = false,
    denied_by_apply_capacity: bool = false,
    denied_by_snapshot_throttle: bool = false,
    has_more_ready: bool = false,
};

pub const HostRound = struct {
    ticked_groups: usize = 0,
    processed_groups: usize = 0,
    virtual_round: u64 = 0,
    virtual_time_ms: u64 = 0,
    elapsed_ns: u64 = 0,
    inbound_drain_elapsed_ns: u64 = 0,
    tick_elapsed_ns: u64 = 0,
    drain_ready_elapsed_ns: u64 = 0,
    drain_ready_scan_elapsed_ns: u64 = 0,
    persist_batch_begin_elapsed_ns: u64 = 0,
    persist_batch_finish_elapsed_ns: u64 = 0,
    outbox_drain_elapsed_ns: u64 = 0,
    apply_flush_elapsed_ns: u64 = 0,
    transport_flush_elapsed_ns: u64 = 0,
    transport_advance_elapsed_ns: u64 = 0,
    slowest_ready_group: ReadyGroupDiagnostics = .{},
};

pub const DrainReadyDiagnostics = struct {
    scan_elapsed_ns: u64 = 0,
    persist_batch_begin_elapsed_ns: u64 = 0,
    persist_batch_finish_elapsed_ns: u64 = 0,
    outbox_drain_elapsed_ns: u64 = 0,
    apply_flush_elapsed_ns: u64 = 0,
    transport_flush_elapsed_ns: u64 = 0,
    slowest_ready_group: ReadyGroupDiagnostics = .{},
};

pub const HostMetrics = struct {
    group_count: usize = 0,
    quiesced_group_count: usize = 0,
    rounds: usize = 0,
    virtual_round: u64 = 0,
    virtual_time_ms: u64 = 0,
    ticked_groups: usize = 0,
    processed_groups: usize = 0,
    persist_batches: usize = 0,
    apply_queue_drains: usize = 0,
    snapshot_throttle_denials: usize = 0,
    backpressure_denials: usize = 0,
    transport_group_serves: usize = 0,
    transport_group_unserves: usize = 0,
    transport_peer_adds: usize = 0,
    transport_peer_removes: usize = 0,
    transport_message_sends: usize = 0,
    transport_peer_batch_flushes: usize = 0,
    transport_snapshot_sends: usize = 0,
    restored_replicas: usize = 0,
    pending_outbound_messages: usize = 0,
    pending_outbound_bytes: usize = 0,
    pending_apply_tasks: usize = 0,
    pending_apply_bytes: usize = 0,
    transport_queue_denials: usize = 0,
    apply_queue_denials: usize = 0,
};

const PendingApplyTask = struct {
    group_id: core.types.GroupId,
    entries: []core.Entry,
    read_states: []core.ReadState,
    approx_bytes: usize,

    fn deinit(self: *PendingApplyTask, alloc: std.mem.Allocator) void {
        core.types.freeEntries(alloc, self.entries);
        for (self.read_states) |*read_state| read_state.deinit(alloc);
        if (self.read_states.len > 0) alloc.free(self.read_states);
        self.* = undefined;
    }
};

pub const MultiRaft = struct {
    alloc: std.mem.Allocator,
    cfg: RuntimeConfig,
    hooks: RuntimeHooks,
    scheduler: scheduler_mod.Scheduler,
    groups: std.AutoHashMapUnmanaged(core.types.GroupId, group_mod.Group) = .empty,
    pending_outbox: TransportOutbox = .{},
    pending_apply: std.ArrayListUnmanaged(PendingApplyTask) = .empty,
    metrics: HostMetrics = .{},

    pub fn init(alloc: std.mem.Allocator, cfg: RuntimeConfig, hooks: RuntimeHooks) MultiRaft {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .hooks = hooks,
            .scheduler = scheduler_mod.Scheduler.init(alloc, .{
                .tick_interval_ms = cfg.tick_interval_ms,
                .max_tick_batch = cfg.max_tick_batch,
            }),
        };
    }

    pub fn deinit(self: *MultiRaft) void {
        var it = self.groups.valueIterator();
        while (it.next()) |grp| grp.deinit();
        self.groups.deinit(self.alloc);
        self.pending_outbox.deinit(self.alloc);
        for (self.pending_apply.items) |*task| task.deinit(self.alloc);
        self.pending_apply.deinit(self.alloc);
        self.scheduler.deinit();
        self.* = undefined;
    }

    pub fn addGroup(self: *MultiRaft, cfg: group_mod.GroupConfig) !void {
        if (self.groups.count() >= self.cfg.max_groups) return error.MaxGroupsExceeded;
        if (self.groups.contains(cfg.group_id)) return error.GroupAlreadyExists;

        var grp = try group_mod.Group.init(self.alloc, cfg);
        errdefer grp.deinit();

        try self.groups.put(self.alloc, cfg.group_id, grp);
        try self.scheduler.registerGroup(cfg.group_id);
        if (self.hooks.transport) |transport| {
            try transport.serveGroup(cfg.group_id, self.transportReceiver());
            self.metrics.transport_group_serves += 1;
        }
        self.refreshMetricsTopology();
    }

    pub fn ensureReplica(self: *MultiRaft, desc: replica_mod.ReplicaDescriptor) !replica_mod.EnsureReplicaResult {
        var result: replica_mod.EnsureReplicaResult = .{};

        if (self.group(desc.group.group_id)) |existing| {
            if (existing.localNodeId() != desc.group.local_node_id) return error.LocalNodeIdMismatch;
            if (self.isGroupQuiesced(desc.group.group_id)) {
                try self.resumeGroup(desc.group.group_id);
                result.resumed = true;
            }
        } else {
            try self.addGroup(desc.group);
            result.created = true;
        }

        switch (desc.bootstrap) {
            .empty, .persisted => {},
            .fetch_snapshot => |bootstrap| {
                if (bootstrap.fetch_immediately) {
                    try self.fetchSnapshot(.{
                        .group_id = desc.group.group_id,
                        .from = bootstrap.from,
                        .term = bootstrap.term,
                        .locator = bootstrap.locator,
                    });
                    result.fetched_snapshot = true;
                }
            },
        }

        try self.persistReplicaRecord(desc);

        return result;
    }

    pub fn removeReplica(self: *MultiRaft, group_id: core.types.GroupId) !void {
        if (!self.removeGroup(group_id)) return error.UnknownGroup;
        if (self.hooks.replica_catalog) |catalog| _ = try catalog.removeReplica(group_id);
    }

    pub fn restoreReplicasFromCatalog(self: *MultiRaft, alloc: std.mem.Allocator) !usize {
        const catalog = self.hooks.replica_catalog orelse return error.MissingReplicaCatalog;
        const factory = self.hooks.replica_factory orelse return error.MissingReplicaFactory;

        const records = try catalog.listReplicas(alloc);
        defer {
            for (records) |*record| record.deinit(alloc);
            alloc.free(records);
        }

        var restored: usize = 0;
        for (records) |*record| {
            if (self.groups.contains(record.group_id)) continue;
            const desc = try factory.instantiateReplica(record);
            const result = try self.ensureReplica(desc);
            if (result.created or result.resumed or result.fetched_snapshot) restored += 1;
        }
        self.metrics.restored_replicas += restored;
        return restored;
    }

    pub fn removeGroup(self: *MultiRaft, group_id: core.types.GroupId) bool {
        const removed = self.groups.fetchRemove(group_id) orelse return false;
        var grp = removed.value;
        grp.deinit();
        _ = self.scheduler.unregisterGroup(group_id);
        if (self.hooks.transport) |transport| {
            transport.unserveGroup(group_id) catch unreachable;
            self.metrics.transport_group_unserves += 1;
        }
        self.refreshMetricsTopology();
        return true;
    }

    pub fn quiesceGroup(self: *MultiRaft, group_id: core.types.GroupId) !void {
        if (!self.groups.contains(group_id)) return error.UnknownGroup;
        try self.scheduler.quiesceGroup(group_id);
        self.refreshMetricsTopology();
    }

    pub fn resumeGroup(self: *MultiRaft, group_id: core.types.GroupId) !void {
        if (!self.groups.contains(group_id)) return error.UnknownGroup;
        _ = self.scheduler.resumeGroup(group_id);
        self.refreshMetricsTopology();
    }

    pub fn isGroupQuiesced(self: *const MultiRaft, group_id: core.types.GroupId) bool {
        return self.scheduler.isQuiesced(group_id);
    }

    pub fn addPeer(self: *MultiRaft, group_id: core.types.GroupId, peer: transport_iface.PeerDescriptor) !void {
        if (!self.groups.contains(group_id)) return error.UnknownGroup;
        if (self.hooks.transport) |transport| {
            try transport.addPeer(group_id, peer);
            self.metrics.transport_peer_adds += 1;
        }
    }

    pub fn upsertPeer(self: *MultiRaft, group_id: core.types.GroupId, peer: transport_iface.PeerDescriptor) !void {
        if (!self.groups.contains(group_id)) return error.UnknownGroup;
        if (self.hooks.transport) |transport| {
            try transport.upsertPeer(group_id, peer);
            self.metrics.transport_peer_adds += 1;
        }
    }

    pub fn removePeer(self: *MultiRaft, group_id: core.types.GroupId, node_id: core.types.NodeId) !void {
        if (!self.groups.contains(group_id)) return error.UnknownGroup;
        if (self.hooks.transport) |transport| {
            try transport.removePeer(group_id, node_id);
            self.metrics.transport_peer_removes += 1;
        }
    }

    pub fn group(self: *MultiRaft, group_id: core.types.GroupId) ?*group_mod.Group {
        return self.groups.getPtr(group_id);
    }

    pub fn listGroupIds(self: *const MultiRaft, alloc: std.mem.Allocator) ![]core.types.GroupId {
        var out = try alloc.alloc(core.types.GroupId, self.groups.count());
        var i: usize = 0;
        var it = self.groups.keyIterator();
        while (it.next()) |group_id| : (i += 1) out[i] = group_id.*;
        std.sort.block(core.types.GroupId, out, {}, std.sort.asc(core.types.GroupId));
        return out;
    }

    pub fn tickGroup(self: *MultiRaft, group_id: core.types.GroupId) !void {
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        grp.tick();
    }

    pub fn tickBatch(self: *MultiRaft, alloc: std.mem.Allocator) ![]core.types.GroupId {
        return try self.scheduler.tickBatch(alloc);
    }

    pub fn virtualTimeMs(self: *const MultiRaft) u64 {
        return self.scheduler.nowMs();
    }

    pub fn virtualRound(self: *const MultiRaft) u64 {
        return self.scheduler.round();
    }

    pub fn tickAll(self: *MultiRaft) void {
        const count = self.groups.count();
        var i: usize = 0;
        while (i < count) : (i += 1) {
            const group_id = self.scheduler.nextTickGroup() orelse break;
            self.tickGroup(group_id) catch unreachable;
        }
    }

    pub fn runRound(self: *MultiRaft, max_tick_groups: usize, max_ready_groups: usize) !HostRound {
        const round_start_ns = clock.monotonicNs();
        const virtual_time = self.scheduler.advanceVirtualTime();
        var ticked_groups: usize = 0;
        const tick_limit = @min(max_tick_groups, self.scheduler.activeGroupCount());

        const tick_start_ns = clock.monotonicNs();
        while (ticked_groups < tick_limit) : (ticked_groups += 1) {
            const group_id = self.scheduler.nextTickGroup() orelse break;
            try self.tickGroup(group_id);
        }
        const tick_elapsed_ns = clock.elapsedSinceNs(tick_start_ns);

        var drain_diag = DrainReadyDiagnostics{};
        const drain_ready_start_ns = clock.monotonicNs();
        const processed_groups = try self.drainReadyWithDiagnostics(max_ready_groups, &drain_diag);
        const drain_ready_elapsed_ns = clock.elapsedSinceNs(drain_ready_start_ns);

        var round: HostRound = .{
            .ticked_groups = ticked_groups,
            .processed_groups = processed_groups,
            .virtual_round = virtual_time.round,
            .virtual_time_ms = virtual_time.now_ms,
            .tick_elapsed_ns = tick_elapsed_ns,
            .drain_ready_elapsed_ns = drain_ready_elapsed_ns,
            .drain_ready_scan_elapsed_ns = drain_diag.scan_elapsed_ns,
            .persist_batch_begin_elapsed_ns = drain_diag.persist_batch_begin_elapsed_ns,
            .persist_batch_finish_elapsed_ns = drain_diag.persist_batch_finish_elapsed_ns,
            .outbox_drain_elapsed_ns = drain_diag.outbox_drain_elapsed_ns,
            .apply_flush_elapsed_ns = drain_diag.apply_flush_elapsed_ns,
            .transport_flush_elapsed_ns = drain_diag.transport_flush_elapsed_ns,
            .slowest_ready_group = drain_diag.slowest_ready_group,
        };
        self.metrics.rounds += 1;
        self.metrics.virtual_round = virtual_time.round;
        self.metrics.virtual_time_ms = virtual_time.now_ms;
        self.metrics.ticked_groups += round.ticked_groups;
        self.metrics.processed_groups += round.processed_groups;
        const transport_advance_start_ns = clock.monotonicNs();
        if (self.hooks.transport) |transport| try transport.advanceTimeMs(virtual_time.now_ms);
        round.transport_advance_elapsed_ns = clock.elapsedSinceNs(transport_advance_start_ns);
        round.elapsed_ns = clock.elapsedSinceNs(round_start_ns);
        return round;
    }

    pub fn metricsSnapshot(self: *const MultiRaft) HostMetrics {
        return self.metrics;
    }

    pub fn step(self: *MultiRaft, group_id: core.types.GroupId, msg: core.Message) !void {
        try self.resumeOnActivity(group_id);
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        try grp.step(msg);
    }

    pub fn campaignGroup(self: *MultiRaft, group_id: core.types.GroupId) !void {
        try self.resumeOnActivity(group_id);
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        try grp.campaign();
    }

    pub fn transferLeader(self: *MultiRaft, group_id: core.types.GroupId, transferee: core.types.NodeId) !void {
        try self.resumeOnActivity(group_id);
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        try grp.transferLeader(transferee);
    }

    pub fn forgetLeader(self: *MultiRaft, group_id: core.types.GroupId) !void {
        try self.resumeOnActivity(group_id);
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        try grp.forgetLeader();
    }

    pub fn propose(self: *MultiRaft, group_id: core.types.GroupId, data: []const u8) !void {
        try self.resumeOnActivity(group_id);
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        try grp.propose(data);
    }

    pub fn readIndex(self: *MultiRaft, group_id: core.types.GroupId, request_ctx: []const u8) !void {
        try self.resumeOnActivity(group_id);
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        try grp.readIndex(request_ctx);
    }

    pub fn fetchSnapshot(self: *MultiRaft, req: snapshot_transport_iface.SnapshotFetchRequest) !void {
        try self.resumeOnActivity(req.group_id);
        const snapshot_transport = self.hooks.snapshot_transport orelse return error.MissingSnapshotTransport;
        try snapshot_transport.fetchSnapshot(req, self.snapshotReceiver());
    }

    pub fn proposeConfChange(self: *MultiRaft, group_id: core.types.GroupId, conf_change: core.ConfChange) !void {
        try self.resumeOnActivity(group_id);
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        try grp.proposeConfChange(conf_change);
    }

    pub fn proposeConfChangeV2(self: *MultiRaft, group_id: core.types.GroupId, conf_change: core.ConfChangeV2) !void {
        try self.resumeOnActivity(group_id);
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        try grp.proposeConfChangeV2(conf_change);
    }

    pub fn readyGroupIds(self: *MultiRaft, alloc: std.mem.Allocator, max_groups: usize) ![]core.types.GroupId {
        var out = std.ArrayListUnmanaged(core.types.GroupId).empty;
        errdefer out.deinit(alloc);

        var it = self.groups.iterator();
        while (it.next()) |entry| {
            if (out.items.len >= max_groups) break;
            if (self.scheduler.isQuiesced(entry.key_ptr.*)) continue;
            if (!entry.value_ptr.hasReady()) continue;
            try out.append(alloc, entry.key_ptr.*);
        }

        return try out.toOwnedSlice(alloc);
    }

    pub fn processReady(self: *MultiRaft, group_id: core.types.GroupId) !bool {
        var outbox = TransportOutbox{};
        defer outbox.deinit(self.alloc);
        const batch = if (self.hooks.disk_batcher) |disk_batcher| try disk_batcher.beginBatch() else null;
        if (batch != null) self.metrics.persist_batches += 1;
        defer if (batch) |persist_batch| persist_batch.finish() catch unreachable;
        const processed = try self.processReadyIntoOutbox(group_id, &outbox, batch, false, false, null);
        try outbox.drainInto(self.alloc, &self.pending_outbox);
        try self.flushPendingApply();
        try self.flushPendingTransport();
        self.refreshQueueMetrics();
        return processed;
    }

    pub fn drainReady(self: *MultiRaft, max_groups: usize) !usize {
        return try self.drainReadyWithDiagnostics(max_groups, null);
    }

    fn drainReadyWithDiagnostics(self: *MultiRaft, max_groups: usize, diagnostics: ?*DrainReadyDiagnostics) !usize {
        var processed: usize = 0;
        var outbox = TransportOutbox{};
        defer outbox.deinit(self.alloc);
        const persist_batch_begin_start_ns = clock.monotonicNs();
        const batch = if (self.hooks.disk_batcher) |disk_batcher| try disk_batcher.beginBatch() else null;
        if (diagnostics) |diag| diag.persist_batch_begin_elapsed_ns = clock.elapsedSinceNs(persist_batch_begin_start_ns);
        if (batch != null) self.metrics.persist_batches += 1;
        errdefer if (batch) |persist_batch| persist_batch.finish() catch unreachable;

        var scanned: usize = 0;
        const scan_limit = self.groups.count();
        const scan_start_ns = clock.monotonicNs();
        while (scanned < scan_limit) : (scanned += 1) {
            if (processed >= max_groups) break;
            const group_id = self.scheduler.nextReadyGroup() orelse break;
            var ready_diag = ReadyGroupDiagnostics{ .group_id = group_id };
            const ready_start_ns = clock.monotonicNs();
            const ready_processed = try self.processReadyIntoOutbox(group_id, &outbox, batch, false, false, &ready_diag);
            ready_diag.elapsed_ns = clock.elapsedSinceNs(ready_start_ns);
            ready_diag.processed = ready_processed;
            if (diagnostics) |diag| {
                if (ready_diag.elapsed_ns > diag.slowest_ready_group.elapsed_ns) diag.slowest_ready_group = ready_diag;
            }
            if (ready_processed) processed += 1;
        }
        if (diagnostics) |diag| diag.scan_elapsed_ns = clock.elapsedSinceNs(scan_start_ns);

        const outbox_drain_start_ns = clock.monotonicNs();
        try outbox.drainInto(self.alloc, &self.pending_outbox);
        if (diagnostics) |diag| diag.outbox_drain_elapsed_ns = clock.elapsedSinceNs(outbox_drain_start_ns);
        const apply_flush_start_ns = clock.monotonicNs();
        try self.flushPendingApply();
        if (diagnostics) |diag| diag.apply_flush_elapsed_ns = clock.elapsedSinceNs(apply_flush_start_ns);
        const transport_flush_start_ns = clock.monotonicNs();
        try self.flushPendingTransport();
        if (diagnostics) |diag| diag.transport_flush_elapsed_ns = clock.elapsedSinceNs(transport_flush_start_ns);
        if (batch) |persist_batch| {
            const persist_batch_finish_start_ns = clock.monotonicNs();
            try persist_batch.finish();
            if (diagnostics) |diag| diag.persist_batch_finish_elapsed_ns = clock.elapsedSinceNs(persist_batch_finish_start_ns);
        }
        self.refreshQueueMetrics();
        return processed;
    }

    fn processReadyIntoOutbox(
        self: *MultiRaft,
        group_id: core.types.GroupId,
        outbox: *TransportOutbox,
        persist_batch: ?storage_iface.PersistBatch,
        flush_transport: bool,
        flush_apply_queue: bool,
        diagnostics: ?*ReadyGroupDiagnostics,
    ) !bool {
        const grp = self.group(group_id) orelse return error.UnknownGroup;
        if (!grp.hasReady()) return false;

        const ready_build_start_ns = clock.monotonicNs();
        const ready = grp.ready();
        if (diagnostics) |diag| diag.ready_build_elapsed_ns = clock.elapsedSinceNs(ready_build_start_ns);
        if (ready.isEmpty()) return false;

        const ready_pressure = summarizeReady(group_id, ready);
        const async_storage_writes = grp.asyncStorageWrites();
        if (diagnostics) |diag| {
            diag.message_count = ready_pressure.message_count;
            diag.message_bytes = ready_pressure.message_bytes;
            diag.committed_entries = ready_pressure.committed_entries;
            diag.committed_entry_bytes = ready_pressure.committed_entry_bytes;
            diag.unstable_entries = ready_pressure.unstable_entries;
            diag.unstable_entry_bytes = ready_pressure.unstable_entry_bytes;
            diag.read_states = ready.read_states.len;
            diag.has_snapshot = ready_pressure.has_snapshot;
            diag.snapshot_bytes = ready_pressure.snapshot_bytes;
            diag.async_storage_writes = async_storage_writes;
        }

        if (self.hooks.backpressure) |backpressure| {
            const backpressure_start_ns = clock.monotonicNs();
            const allowed = backpressure.allowReady(ready_pressure);
            if (diagnostics) |diag| diag.backpressure_elapsed_ns = clock.elapsedSinceNs(backpressure_start_ns);
            if (!allowed) {
                if (diagnostics) |diag| diag.denied_by_backpressure = true;
                self.metrics.backpressure_denials += 1;
                self.scheduler.noteReady(group_id);
                return false;
            }
        }

        const capacity_check_start_ns = clock.monotonicNs();
        if (!self.hasOutboundCapacity(
            outbox.items.items.len + ready_pressure.message_count,
            outbox.approxBytes() + ready_pressure.message_bytes,
        )) {
            if (diagnostics) |diag| {
                diag.capacity_check_elapsed_ns = clock.elapsedSinceNs(capacity_check_start_ns);
                diag.denied_by_transport_capacity = true;
            }
            self.metrics.transport_queue_denials += 1;
            self.scheduler.noteReady(group_id);
            return false;
        }
        if (!self.hasApplyCapacity(
            if (ready.committed_entries.len > 0 or ready.read_states.len > 0) 1 else 0,
            ready_pressure.committed_entry_bytes + approxReadStatesSize(ready.read_states),
        )) {
            if (diagnostics) |diag| {
                diag.capacity_check_elapsed_ns = clock.elapsedSinceNs(capacity_check_start_ns);
                diag.denied_by_apply_capacity = true;
            }
            self.metrics.apply_queue_denials += 1;
            self.scheduler.noteReady(group_id);
            return false;
        }
        if (diagnostics) |diag| diag.capacity_check_elapsed_ns = clock.elapsedSinceNs(capacity_check_start_ns);

        const snapshot_throttle_start_ns = clock.monotonicNs();
        const snapshot_started = blk: {
            if (ready.snapshot == null) break :blk false;
            if (self.hooks.snapshot_throttle) |throttle| {
                if (!throttle.beginSnapshot(group_id)) {
                    if (diagnostics) |diag| {
                        diag.snapshot_throttle_elapsed_ns = clock.elapsedSinceNs(snapshot_throttle_start_ns);
                        diag.denied_by_snapshot_throttle = true;
                    }
                    self.metrics.snapshot_throttle_denials += 1;
                    self.scheduler.noteReady(group_id);
                    return false;
                }
                break :blk true;
            }
            break :blk false;
        };
        if (diagnostics) |diag| diag.snapshot_throttle_elapsed_ns = clock.elapsedSinceNs(snapshot_throttle_start_ns);
        defer if (snapshot_started) {
            self.hooks.snapshot_throttle.?.endSnapshot(group_id);
        };

        const persist_ready_start_ns = clock.monotonicNs();
        if (persist_batch) |batch| {
            try batch.persistReady(group_id, ready);
        } else if (self.hooks.group_storage) |storage| {
            try storage.persistReady(group_id, ready);
        }
        if (diagnostics) |diag| diag.persist_ready_elapsed_ns = clock.elapsedSinceNs(persist_ready_start_ns);

        if (async_storage_writes) {
            const async_ready_start_ns = clock.monotonicNs();
            try self.handleAsyncReady(group_id, grp, ready, outbox, flush_apply_queue, diagnostics);
            if (diagnostics) |diag| diag.async_ready_elapsed_ns = clock.elapsedSinceNs(async_ready_start_ns);
        } else {
            const enqueue_apply_start_ns = clock.monotonicNs();
            try self.enqueueApply(group_id, ready.committed_entries, ready.read_states);
            if (diagnostics) |diag| diag.enqueue_apply_elapsed_ns = clock.elapsedSinceNs(enqueue_apply_start_ns);
            const outbox_append_start_ns = clock.monotonicNs();
            try outbox.appendMessages(self.alloc, group_id, ready.messages);
            if (diagnostics) |diag| diag.outbox_append_elapsed_ns = clock.elapsedSinceNs(outbox_append_start_ns);
            const advance_start_ns = clock.monotonicNs();
            grp.advance(ready);
            if (diagnostics) |diag| diag.raft_advance_elapsed_ns = clock.elapsedSinceNs(advance_start_ns);
        }

        if (flush_apply_queue) {
            const inline_apply_flush_start_ns = clock.monotonicNs();
            try self.flushPendingApply();
            if (diagnostics) |diag| diag.inline_apply_flush_elapsed_ns = clock.elapsedSinceNs(inline_apply_flush_start_ns);
        }
        if (flush_transport) {
            const inline_outbox_drain_start_ns = clock.monotonicNs();
            try outbox.drainInto(self.alloc, &self.pending_outbox);
            if (diagnostics) |diag| diag.inline_outbox_drain_elapsed_ns = clock.elapsedSinceNs(inline_outbox_drain_start_ns);
            const inline_transport_flush_start_ns = clock.monotonicNs();
            try self.flushPendingTransport();
            if (diagnostics) |diag| diag.inline_transport_flush_elapsed_ns = clock.elapsedSinceNs(inline_transport_flush_start_ns);
        }
        const has_more_ready = grp.hasReady();
        if (diagnostics) |diag| diag.has_more_ready = has_more_ready;
        if (has_more_ready) self.scheduler.noteReady(group_id);
        return true;
    }

    fn handleAsyncReady(
        self: *MultiRaft,
        group_id: core.types.GroupId,
        grp: *group_mod.Group,
        ready: core.Ready,
        outbox: *TransportOutbox,
        flush_apply_queue: bool,
        diagnostics: ?*ReadyGroupDiagnostics,
    ) !void {
        const clone_messages_start_ns = clock.monotonicNs();
        const messages = try core.message.cloneMessages(self.alloc, ready.messages);
        defer core.message.freeMessages(self.alloc, messages);
        if (diagnostics) |diag| diag.clone_messages_elapsed_ns = clock.elapsedSinceNs(clone_messages_start_ns);

        const enqueue_apply_start_ns = clock.monotonicNs();
        try self.enqueueApply(group_id, ready.committed_entries, ready.read_states);
        if (diagnostics) |diag| diag.enqueue_apply_elapsed_ns = clock.elapsedSinceNs(enqueue_apply_start_ns);
        if (flush_apply_queue) {
            const inline_apply_flush_start_ns = clock.monotonicNs();
            try self.flushPendingApply();
            if (diagnostics) |diag| diag.inline_apply_flush_elapsed_ns = clock.elapsedSinceNs(inline_apply_flush_start_ns);
        }

        const async_message_loop_start_ns = clock.monotonicNs();
        for (messages) |msg| {
            switch (msg.msg_type) {
                .storage_append => try self.handleLocalStorageAppend(group_id, grp, msg, outbox),
                .storage_apply => try self.handleLocalStorageApply(group_id, grp, msg, outbox),
                else => try outbox.appendMessage(self.alloc, group_id, msg),
            }
        }
        if (diagnostics) |diag| diag.async_message_loop_elapsed_ns = clock.elapsedSinceNs(async_message_loop_start_ns);
    }

    fn enqueueApply(
        self: *MultiRaft,
        group_id: core.types.GroupId,
        committed_entries: []const core.Entry,
        read_states: []const core.ReadState,
    ) !void {
        if (committed_entries.len == 0 and read_states.len == 0) return;

        var cloned_read_states = try self.alloc.alloc(core.ReadState, read_states.len);
        errdefer self.alloc.free(cloned_read_states);
        for (read_states, 0..) |read_state, i| cloned_read_states[i] = try read_state.clone(self.alloc);

        try self.pending_apply.append(self.alloc, .{
            .group_id = group_id,
            .entries = try core.types.cloneEntries(self.alloc, committed_entries),
            .read_states = cloned_read_states,
            .approx_bytes = core.types.entriesApproxEncodedSize(committed_entries) + approxReadStatesSize(read_states),
        });
    }

    fn flushPendingApply(self: *MultiRaft) !void {
        if (self.pending_apply.items.len == 0) return;

        const drain_count = @min(self.cfg.max_apply_tasks_per_round, self.pending_apply.items.len);
        if (drain_count == 0) return;

        if (self.hooks.apply_queue) |apply_queue| {
            for (self.pending_apply.items[0..drain_count]) |task| {
                try apply_queue.enqueueApply(task.group_id, task.entries, task.read_states);
            }
            try apply_queue.drain();
            self.metrics.apply_queue_drains += 1;
        } else if (self.hooks.state_machine) |state_machine| {
            for (self.pending_apply.items[0..drain_count]) |task| {
                try state_machine.applyReady(task.group_id, task.entries, task.read_states);
            }
        }
        try self.compactAppliedLogs(self.pending_apply.items[0..drain_count]);

        self.consumePendingApplyPrefix(drain_count);
    }

    fn compactAppliedLogs(self: *MultiRaft, tasks: []const PendingApplyTask) !void {
        if (self.cfg.applied_log_retained_entries == 0) return;
        for (tasks) |task| {
            if (task.entries.len == 0) continue;
            const last_applied = task.entries[task.entries.len - 1].index;
            if (last_applied <= self.cfg.applied_log_retained_entries) continue;
            const compact_index = last_applied - self.cfg.applied_log_retained_entries;
            const grp = self.groups.getPtr(task.group_id) orelse continue;
            if (self.cfg.applied_log_compaction_single_node_only and grp.status().conf_state.voters.len != 1) continue;
            const first_index = grp.raw_node.raft.log.firstIndex();
            if (compact_index < first_index) continue;
            const snapshot_index = first_index - 1;
            if (self.cfg.applied_log_compaction_min_interval_entries > 0 and
                compact_index - snapshot_index < self.cfg.applied_log_compaction_min_interval_entries)
            {
                continue;
            }
            try grp.compactAppliedLogTo(compact_index);
        }
    }

    fn refreshMetricsTopology(self: *MultiRaft) void {
        self.metrics.group_count = self.groups.count();
        self.metrics.quiesced_group_count = self.groups.count() - self.scheduler.activeGroupCount();
    }

    fn refreshQueueMetrics(self: *MultiRaft) void {
        self.metrics.pending_outbound_messages = self.pending_outbox.items.items.len;
        self.metrics.pending_outbound_bytes = self.pending_outbox.approxBytes();

        var pending_apply_bytes: usize = 0;
        for (self.pending_apply.items) |task| pending_apply_bytes += task.approx_bytes;
        self.metrics.pending_apply_tasks = self.pending_apply.items.len;
        self.metrics.pending_apply_bytes = pending_apply_bytes;
    }

    fn persistReplicaRecord(self: *MultiRaft, desc: replica_mod.ReplicaDescriptor) !void {
        const catalog = self.hooks.replica_catalog orelse return;
        var record = try replica_mod.ReplicaRecord.fromDescriptor(self.alloc, desc);
        defer record.deinit(self.alloc);
        try catalog.upsertReplica(record);
    }

    fn recordTransportFlush(self: *MultiRaft, stats: TransportFlushStats) void {
        self.metrics.transport_message_sends += stats.message_sends;
        self.metrics.transport_peer_batch_flushes += stats.peer_batch_flushes;
        self.metrics.transport_snapshot_sends += stats.snapshot_sends;
    }

    fn hasOutboundCapacity(self: *const MultiRaft, total_messages: usize, total_bytes: usize) bool {
        return self.pending_outbox.items.items.len + total_messages <= self.cfg.max_pending_outbound_messages and
            self.pending_outbox.approxBytes() + total_bytes <= self.cfg.max_pending_outbound_bytes;
    }

    fn hasApplyCapacity(self: *const MultiRaft, new_tasks: usize, new_bytes: usize) bool {
        var pending_bytes: usize = 0;
        for (self.pending_apply.items) |task| pending_bytes += task.approx_bytes;
        return self.pending_apply.items.len + new_tasks <= self.cfg.max_pending_apply_tasks and
            pending_bytes + new_bytes <= self.cfg.max_pending_apply_bytes;
    }

    fn flushPendingTransport(self: *MultiRaft) !void {
        if (self.pending_outbox.items.items.len == 0) return;
        if (self.hooks.transport == null and self.hooks.snapshot_transport == null) return;

        const stats = try self.pending_outbox.flushBudgeted(
            self.alloc,
            self.hooks,
            self.cfg.max_transport_messages_per_round,
            self.cfg.max_transport_bytes_per_round,
        );
        self.recordTransportFlush(stats);
    }

    fn consumePendingApplyPrefix(self: *MultiRaft, count: usize) void {
        if (count == 0) return;

        for (self.pending_apply.items[0..count]) |*task| task.deinit(self.alloc);

        const remaining = self.pending_apply.items.len - count;
        std.mem.copyForwards(PendingApplyTask, self.pending_apply.items[0..remaining], self.pending_apply.items[count..]);
        self.pending_apply.items.len = remaining;
    }

    fn resumeOnActivity(self: *MultiRaft, group_id: core.types.GroupId) !void {
        if (!self.groups.contains(group_id)) return error.UnknownGroup;
        self.scheduler.noteActivity(group_id);
        if (self.isGroupQuiesced(group_id)) try self.resumeGroup(group_id);
    }

    fn transportReceiver(self: *MultiRaft) transport_iface.TransportReceiver {
        return .{
            .ptr = self,
            .vtable = &.{
                .handle_message = transportHandleMessage,
            },
        };
    }

    fn transportHandleMessage(ptr: *anyopaque, group_id: core.types.GroupId, msg: core.Message) !void {
        const self: *MultiRaft = @ptrCast(@alignCast(ptr));
        try self.step(group_id, msg);
    }

    fn snapshotReceiver(self: *MultiRaft) snapshot_transport_iface.SnapshotReceiver {
        return .{
            .ptr = self,
            .vtable = &.{
                .receive_snapshot = snapshotHandleReceive,
            },
        };
    }

    fn snapshotHandleReceive(
        ptr: *anyopaque,
        req: snapshot_transport_iface.SnapshotFetchRequest,
        snapshot: core.types.Snapshot,
    ) !void {
        const self: *MultiRaft = @ptrCast(@alignCast(ptr));
        const grp = self.group(req.group_id) orelse return error.UnknownGroup;
        var msg: core.Message = .{
            .msg_type = .snapshot,
            .from = req.from,
            .to = grp.localNodeId(),
            .term = req.term,
            .snapshot = snapshot,
        };
        defer msg.deinit(self.alloc);
        try self.step(req.group_id, msg);
    }

    fn handleLocalStorageAppend(
        self: *MultiRaft,
        group_id: core.types.GroupId,
        grp: *group_mod.Group,
        msg: core.Message,
        outbox: *TransportOutbox,
    ) !void {
        for (msg.responses) |response| {
            if (core.message.isLocalStorageThread(response.to) or response.to == grp.localNodeId()) {
                try grp.step(response);
            } else {
                try outbox.appendMessage(self.alloc, group_id, response);
            }
        }
    }

    fn handleLocalStorageApply(
        self: *MultiRaft,
        group_id: core.types.GroupId,
        grp: *group_mod.Group,
        msg: core.Message,
        outbox: *TransportOutbox,
    ) !void {
        for (msg.responses) |response| {
            if (core.message.isLocalStorageThread(response.to) or response.to == grp.localNodeId()) {
                try grp.step(response);
            } else {
                try outbox.appendMessage(self.alloc, group_id, response);
            }
        }
    }
};

const OutboundMessage = struct {
    group_id: core.types.GroupId,
    message: core.Message,
};

const TransportFlushStats = struct {
    message_sends: usize = 0,
    peer_batch_flushes: usize = 0,
    snapshot_sends: usize = 0,
};

fn summarizeReady(group_id: core.types.GroupId, ready: core.Ready) backpressure_iface.ReadyPressure {
    return .{
        .group_id = group_id,
        .message_count = ready.messages.len,
        .message_bytes = approxMessagesSize(ready.messages),
        .committed_entries = ready.committed_entries.len,
        .committed_entry_bytes = core.types.entriesApproxEncodedSize(ready.committed_entries),
        .unstable_entries = ready.entries.len,
        .unstable_entry_bytes = core.types.entriesApproxEncodedSize(ready.entries),
        .has_snapshot = ready.snapshot != null,
        .snapshot_bytes = if (ready.snapshot) |snapshot| snapshot.data.len else 0,
    };
}

fn approxMessagesSize(messages: []const core.Message) usize {
    var total: usize = 0;
    for (messages) |msg| {
        total += 64;
        total += msg.context.len;
        total += core.types.entriesApproxEncodedSize(msg.entries);
        if (msg.snapshot) |snapshot| total += snapshot.data.len;
    }
    return total;
}

fn approxReadStatesSize(read_states: []const core.ReadState) usize {
    var total: usize = 0;
    for (read_states) |read_state| total += 16 + read_state.request_ctx.len;
    return total;
}

const GroupBatchBuilder = struct {
    group_id: core.types.GroupId,
    messages: std.ArrayListUnmanaged(core.Message) = .empty,

    fn deinit(self: *GroupBatchBuilder, alloc: std.mem.Allocator) void {
        self.messages.deinit(alloc);
        self.* = undefined;
    }
};

const PeerBatchBuilder = struct {
    peer_id: core.types.NodeId,
    groups: std.ArrayListUnmanaged(GroupBatchBuilder) = .empty,

    fn deinit(self: *PeerBatchBuilder, alloc: std.mem.Allocator) void {
        for (self.groups.items) |*group| group.deinit(alloc);
        self.groups.deinit(alloc);
        self.* = undefined;
    }
};

const TransportOutbox = struct {
    items: std.ArrayListUnmanaged(OutboundMessage) = .empty,
    approx_bytes: usize = 0,

    fn deinit(self: *TransportOutbox, alloc: std.mem.Allocator) void {
        for (self.items.items) |*item| item.message.deinit(alloc);
        self.items.deinit(alloc);
        self.* = undefined;
    }

    fn appendMessage(
        self: *TransportOutbox,
        alloc: std.mem.Allocator,
        group_id: core.types.GroupId,
        msg: core.Message,
    ) !void {
        const msg_bytes = approxMessagesSize(&.{msg});
        try self.items.append(alloc, .{
            .group_id = group_id,
            .message = try msg.clone(alloc),
        });
        self.approx_bytes += msg_bytes;
    }

    fn appendMessages(
        self: *TransportOutbox,
        alloc: std.mem.Allocator,
        group_id: core.types.GroupId,
        messages: []const core.Message,
    ) !void {
        try self.items.ensureUnusedCapacity(alloc, messages.len);
        for (messages) |msg| {
            const msg_bytes = approxMessagesSize(&.{msg});
            self.items.appendAssumeCapacity(.{
                .group_id = group_id,
                .message = try msg.clone(alloc),
            });
            self.approx_bytes += msg_bytes;
        }
    }

    fn drainInto(self: *TransportOutbox, alloc: std.mem.Allocator, dst: *TransportOutbox) !void {
        if (self.items.items.len == 0) return;
        try dst.items.ensureUnusedCapacity(alloc, self.items.items.len);
        for (self.items.items) |item| dst.items.appendAssumeCapacity(item);
        dst.approx_bytes += self.approx_bytes;
        self.items.clearRetainingCapacity();
        self.approx_bytes = 0;
    }

    fn approxBytes(self: *const TransportOutbox) usize {
        return self.approx_bytes;
    }

    fn flush(self: *TransportOutbox, alloc: std.mem.Allocator, hooks: RuntimeHooks) !TransportFlushStats {
        if (self.items.items.len == 0) return .{};
        defer self.clear(alloc);

        var stats: TransportFlushStats = .{};

        if (hooks.snapshot_transport) |snapshot_transport| {
            for (self.items.items) |item| {
                if (item.message.msg_type != .snapshot) continue;
                const snapshot = item.message.snapshot orelse return error.MissingSnapshot;
                try snapshot_transport.sendSnapshot(.{
                    .group_id = item.group_id,
                    .from = item.message.from,
                    .to = item.message.to,
                    .term = item.message.term,
                    .snapshot = snapshot,
                });
                stats.snapshot_sends += 1;
            }
        }

        const transport = hooks.transport orelse return stats;

        if (!transport.supportsPeerBatches()) {
            for (self.items.items) |item| {
                if (item.message.msg_type == .snapshot) continue;
                try transport.sendMessages(item.group_id, &.{item.message});
                stats.message_sends += 1;
            }
            return stats;
        }

        var peer_builders = std.ArrayListUnmanaged(PeerBatchBuilder).empty;
        defer {
            for (peer_builders.items) |*peer| peer.deinit(alloc);
            peer_builders.deinit(alloc);
        }

        for (self.items.items) |item| {
            if (item.message.msg_type == .snapshot) continue;
            const peer_idx = blk: {
                for (peer_builders.items, 0..) |peer, i| {
                    if (peer.peer_id == item.message.to) break :blk i;
                }
                try peer_builders.append(alloc, .{ .peer_id = item.message.to });
                break :blk peer_builders.items.len - 1;
            };

            const peer = &peer_builders.items[peer_idx];
            const group_idx = blk: {
                for (peer.groups.items, 0..) |group, i| {
                    if (group.group_id == item.group_id) break :blk i;
                }
                try peer.groups.append(alloc, .{ .group_id = item.group_id });
                break :blk peer.groups.items.len - 1;
            };

            try peer.groups.items[group_idx].messages.append(alloc, item.message);
        }

        if (peer_builders.items.len == 0) return stats;

        var peer_batches = try alloc.alloc(transport_iface.PeerBatch, peer_builders.items.len);
        defer alloc.free(peer_batches);

        var total_group_count: usize = 0;
        var total_message_count: usize = 0;
        for (peer_builders.items) |peer| {
            total_group_count += peer.groups.items.len;
            for (peer.groups.items) |group| total_message_count += group.messages.items.len;
        }

        const group_batches = try alloc.alloc(transport_iface.GroupMessageBatch, total_group_count);
        defer alloc.free(group_batches);
        const message_buffer = try alloc.alloc(core.Message, total_message_count);
        defer alloc.free(message_buffer);

        var next_group_index: usize = 0;
        var next_message_index: usize = 0;
        for (peer_builders.items, 0..) |*peer, i| {
            const peer_group_start = next_group_index;

            for (peer.groups.items, 0..) |*group, j| {
                const message_start = next_message_index;
                const message_end = message_start + group.messages.items.len;
                std.mem.copyForwards(core.Message, message_buffer[message_start..message_end], group.messages.items);
                group_batches[next_group_index] = .{
                    .group_id = group.group_id,
                    .messages = message_buffer[message_start..message_end],
                };
                _ = j;
                next_group_index += 1;
                next_message_index = message_end;
            }

            peer_batches[i] = .{
                .peer_id = peer.peer_id,
                .groups = group_batches[peer_group_start..next_group_index],
            };
        }

        try transport.sendPeerBatches(peer_batches);
        stats.peer_batch_flushes += 1;
        for (peer_batches) |peer_batch| {
            for (peer_batch.groups) |group_batch| {
                stats.message_sends += group_batch.messages.len;
            }
        }
        return stats;
    }

    fn flushBudgeted(
        self: *TransportOutbox,
        alloc: std.mem.Allocator,
        hooks: RuntimeHooks,
        max_messages: usize,
        max_bytes: usize,
    ) !TransportFlushStats {
        if (self.items.items.len == 0) return .{};

        const take_count = self.countWithinBudget(max_messages, max_bytes);
        if (take_count == 0) return .{};

        var prefix = try self.takePrefix(alloc, take_count);
        defer prefix.deinit(alloc);
        return try prefix.flush(alloc, hooks);
    }

    fn countWithinBudget(self: *const TransportOutbox, max_messages: usize, max_bytes: usize) usize {
        var count: usize = 0;
        var used_bytes: usize = 0;

        for (self.items.items) |item| {
            if (count >= max_messages) break;
            const msg_bytes = approxMessagesSize(&.{item.message});
            if (count > 0 and used_bytes + msg_bytes > max_bytes) break;
            used_bytes += msg_bytes;
            count += 1;
        }
        return count;
    }

    fn takePrefix(self: *TransportOutbox, alloc: std.mem.Allocator, count: usize) !TransportOutbox {
        var out = TransportOutbox{};
        errdefer out.deinit(alloc);

        try out.items.ensureTotalCapacity(alloc, count);
        var taken_bytes: usize = 0;
        for (self.items.items[0..count]) |item| {
            out.items.appendAssumeCapacity(item);
            taken_bytes += approxMessagesSize(&.{item.message});
        }
        out.approx_bytes = taken_bytes;

        const remaining = self.items.items.len - count;
        std.mem.copyForwards(OutboundMessage, self.items.items[0..remaining], self.items.items[count..]);
        self.items.items.len = remaining;
        self.approx_bytes -= taken_bytes;

        return out;
    }

    fn clear(self: *TransportOutbox, alloc: std.mem.Allocator) void {
        for (self.items.items) |*item| item.message.deinit(alloc);
        self.items.clearRetainingCapacity();
        self.approx_bytes = 0;
    }
};

test "multi raft owns real groups" {
    var runtime = MultiRaft.init(std.testing.allocator, .{}, .{});
    defer runtime.deinit();

    var storage = core.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    var peers = [_]core.types.NodeId{1};
    try runtime.addGroup(.{
        .group_id = 11,
        .local_node_id = 1,
        .raft_config = .{
            .id = 1,
            .group_id = 11,
            .peers = peers[0..],
            .election_tick = 5,
            .heartbeat_tick = 1,
            .pre_vote = false,
        },
        .storage = storage.storage(),
    });

    try std.testing.expect(runtime.group(11) != null);
    runtime.tickAll();

    const ready_ids = try runtime.readyGroupIds(std.testing.allocator, 16);
    defer std.testing.allocator.free(ready_ids);
    try std.testing.expectEqual(@as(usize, 0), ready_ids.len);
    try std.testing.expect(runtime.removeGroup(11));
}
