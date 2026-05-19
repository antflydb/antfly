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
const data = @import("../data/mod.zig");
const metadata = @import("../metadata/mod.zig");
const db_types = @import("../storage/db/types.zig");
const catalog = @import("catalog.zig");
const feature_reads = @import("feature_reads.zig");
const host_mod = @import("host.zig");
const managed_host = @import("managed_host.zig");
const metadata_view = @import("metadata_view.zig");
const read_gate = @import("read_gate.zig");
const raft_state_machine = @import("state_machine/mod.zig");
const shard_ops = @import("shard_ops.zig");
const transition_runtime = @import("transition_runtime.zig");
const transition_service = @import("transition_service.zig");

pub const ManagedServiceConfig = struct {
    max_tick_groups: usize = 64,
    max_ready_groups: usize = 64,
};

pub const ManagedServiceDeps = struct {
    transition_runtime: ?transition_runtime.TransitionRuntime = null,
    transition_ops: ?shard_ops.ShardOperationAdapter = null,
};

pub const ManagedServiceMetrics = struct {
    queued_updates: usize = 0,
    applied_updates: usize = 0,
    sync_rounds: usize = 0,
    read_lease_requests: usize = 0,
    queued_split_transitions: usize = 0,
    queued_merge_transitions: usize = 0,
    stepped_split_transitions: usize = 0,
    stepped_merge_transitions: usize = 0,
    completed_split_transitions: usize = 0,
    completed_merge_transitions: usize = 0,
    awaiting_split_source_start: usize = 0,
    bootstrapping_split_destination: usize = 0,
    split_replay_blocked: usize = 0,
    split_ready_to_finalize: usize = 0,
    awaiting_merge_receiver_acceptance: usize = 0,
    bootstrapping_merge_receiver: usize = 0,
    merge_replay_blocked: usize = 0,
    merge_ready_to_finalize: usize = 0,
};

const TestSingleNodeFactory = struct {
    alloc: std.mem.Allocator,
    store: *raft_engine.core.MemoryStorage,

    fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
            },
        };
    }

    fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
        return .{
            .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = .{
                    .id = record.local_node_id,
                    .group_id = record.group_id,
                    .peers = peers[0..],
                    .election_tick = 5,
                    .heartbeat_tick = 1,
                    .pre_vote = false,
                    .check_quorum = true,
                },
                .storage = self.store.storage(),
            },
            .bootstrap = .persisted,
        };
    }

    fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = alloc;
        self.alloc.free(desc.group.raft_config.peers);
    }
};

pub const ManagedHostService = struct {
    alloc: std.mem.Allocator,
    cfg: ManagedServiceConfig,
    host: managed_host.ManagedHost,
    local_transition_runtime: ?transition_runtime.TransitionRuntime = null,
    pending_updates: std.ArrayListUnmanaged(metadata_view.MetadataUpdate) = .empty,
    transition_svc: ?transition_service.TransitionService = null,
    metrics: ManagedServiceMetrics = .{},

    pub fn init(
        alloc: std.mem.Allocator,
        host_cfg: managed_host.ManagedHostConfig,
        host_deps: managed_host.ManagedHostDeps,
        cfg: ManagedServiceConfig,
        deps: ManagedServiceDeps,
    ) !ManagedHostService {
        var svc = ManagedHostService{
            .alloc = alloc,
            .cfg = cfg,
            .host = try managed_host.ManagedHost.init(alloc, host_cfg, host_deps),
            .local_transition_runtime = deps.transition_runtime,
            .transition_svc = if (deps.transition_ops) |ops|
                transition_service.TransitionService.init(alloc, ops)
            else if (deps.transition_runtime) |runtime|
                transition_service.TransitionService.init(alloc, runtime)
            else
                null,
        };
        errdefer svc.deinit();
        try svc.seedTransitionsFromMetadataStore(host_cfg.host.metadata_group_id);
        return svc;
    }

    pub fn deinit(self: *ManagedHostService) void {
        self.clearPending();
        self.pending_updates.deinit(self.alloc);
        if (self.transition_svc) |*transition_svc| transition_svc.deinit();
        self.host.deinit();
        self.* = undefined;
    }

    pub fn submit(self: *ManagedHostService, update: metadata_view.MetadataUpdate) !void {
        try self.pending_updates.append(self.alloc, try update.clone(self.alloc));
        self.metrics.queued_updates = self.pending_updates.items.len;
    }

    pub fn submitBatch(self: *ManagedHostService, updates: []const metadata_view.MetadataUpdate) !void {
        for (updates) |update| try self.submit(update);
    }

    pub fn submitSplitTransition(self: *ManagedHostService, record: metadata.SplitTransitionRecord) !void {
        if (self.transition_svc) |*transition_svc| {
            try transition_svc.submitSplit(record);
            self.syncTransitionMetrics();
        }
    }

    pub fn submitMergeTransition(self: *ManagedHostService, record: metadata.MergeTransitionRecord) !void {
        if (self.transition_svc) |*transition_svc| {
            try transition_svc.submitMerge(record);
            self.syncTransitionMetrics();
        }
    }

    pub fn requestReadableLease(self: *ManagedHostService, group_id: u64, request_ctx: []const u8) !void {
        try self.host.host.readIndex(group_id, request_ctx);
        self.metrics.read_lease_requests += 1;
    }

    pub fn readableLeaseRequester(self: *ManagedHostService) read_gate.ReadableLeaseRequester {
        return .{
            .ptr = self,
            .vtable = &.{
                .request_readable_lease = requestReadableLeaseViaRequester,
            },
        };
    }

    pub fn featureReads(self: *ManagedHostService) feature_reads.FeatureReads {
        return feature_reads.FeatureReads.init(self.readableLeaseRequester());
    }

    pub fn shardOperationAdapter(self: *ManagedHostService) ?shard_ops.ShardOperationAdapter {
        if (self.transition_svc) |*transition_svc| return transition_svc.shardOperationAdapter();
        if (self.local_transition_runtime) |*runtime| return runtime.shardOperationAdapter();
        return null;
    }

    pub fn prepareEnrichmentRead(
        self: *ManagedHostService,
        group_id: u64,
        kind: read_gate.EnrichmentReadKind,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const gate = read_gate.EnrichmentReadGate.init(self.readableLeaseRequester());
        try gate.prepare(group_id, kind, consistency);
    }

    pub fn prepareSearchRead(self: *ManagedHostService, group_id: u64) !void {
        try self.prepareEnrichmentRead(group_id, .search, .read_index);
    }

    pub fn prepareSearchRequestWithConsistency(
        self: *ManagedHostService,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.featureReads().prepareSearchWithConsistency(group_id, req, consistency);
    }

    pub fn prepareSearchRequest(self: *ManagedHostService, group_id: u64, req: db_types.SearchRequest) !void {
        try self.prepareSearchRequestWithConsistency(group_id, req, .read_index);
    }

    pub fn prepareLookupRead(self: *ManagedHostService, group_id: u64) !void {
        try self.prepareEnrichmentRead(group_id, .lookup, .read_index);
    }

    pub fn prepareLookupRequestWithConsistency(
        self: *ManagedHostService,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.featureReads().prepareLookupWithConsistency(group_id, key, opts, consistency);
    }

    pub fn prepareLookupRequest(self: *ManagedHostService, group_id: u64, key: []const u8, opts: db_types.LookupOptions) !void {
        try self.prepareLookupRequestWithConsistency(group_id, key, opts, .read_index);
    }

    pub fn prepareScanRead(self: *ManagedHostService, group_id: u64) !void {
        try self.prepareEnrichmentRead(group_id, .scan, .read_index);
    }

    pub fn prepareScanRequestWithConsistency(
        self: *ManagedHostService,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.featureReads().prepareScanWithConsistency(group_id, from_key, to_key, opts, consistency);
    }

    pub fn prepareScanRequest(
        self: *ManagedHostService,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
    ) !void {
        try self.prepareScanRequestWithConsistency(group_id, from_key, to_key, opts, .read_index);
    }

    pub fn syncPending(self: *ManagedHostService) !managed_host.ManagedSyncResult {
        const result = try self.host.syncOnce(
            self.pending_updates.items,
            self.cfg.max_tick_groups,
            self.cfg.max_ready_groups,
        );
        try self.enqueueTransitionUpdates(self.pending_updates.items);
        self.metrics.applied_updates += self.pending_updates.items.len;
        self.metrics.sync_rounds += 1;
        self.clearPending();
        self.metrics.queued_updates = 0;
        _ = try self.stepTransitions();
        return result;
    }

    pub fn runRound(self: *ManagedHostService) !void {
        _ = try self.host.runRound(self.cfg.max_tick_groups, self.cfg.max_ready_groups);
        self.metrics.sync_rounds += 1;
        _ = try self.stepTransitions();
    }

    pub fn stepTransitions(self: *ManagedHostService) !transition_service.TransitionStepResult {
        if (self.transition_svc) |*transition_svc| {
            const result = try transition_svc.stepPending();
            self.syncTransitionMetrics();
            return result;
        }
        return .{};
    }

    pub fn observeSplitTransition(self: *ManagedHostService, transition_id: u64) !?metadata.SplitObservation {
        if (self.transition_svc) |*transition_svc| {
            var observation = (try transition_svc.observeSplit(transition_id)) orelse return null;
            if (transition_svc.splitRecord(transition_id)) |record| {
                observation.source_local_leader = observation.source_local_leader or self.host.host.isLocalLeader(record.source_group_id);
                observation.destination_local_leader = observation.destination_local_leader or self.host.host.isLocalLeader(record.destination_group_id);
            }
            return observation;
        }
        return null;
    }

    pub fn describeSplitTransition(self: *ManagedHostService, transition_id: u64) !?metadata.SplitExecutionState {
        if (self.transition_svc) |*transition_svc| {
            return try transition_svc.describeSplit(transition_id);
        }
        return null;
    }

    pub fn observeMergeTransition(self: *ManagedHostService, transition_id: u64) !?metadata.MergeObservation {
        if (self.transition_svc) |*transition_svc| {
            var observation = (try transition_svc.observeMerge(transition_id)) orelse return null;
            if (transition_svc.mergeRecord(transition_id)) |record| {
                observation.donor_local_leader = observation.donor_local_leader or self.host.host.isLocalLeader(record.donor_group_id);
                observation.receiver_local_leader = observation.receiver_local_leader or self.host.host.isLocalLeader(record.receiver_group_id);
            }
            return observation;
        }
        return null;
    }

    pub fn describeMergeTransition(self: *ManagedHostService, transition_id: u64) !?metadata.MergeExecutionState {
        if (self.transition_svc) |*transition_svc| {
            return try transition_svc.describeMerge(transition_id);
        }
        return null;
    }

    fn clearPending(self: *ManagedHostService) void {
        for (self.pending_updates.items) |*update| update.deinit(self.alloc);
        self.pending_updates.clearRetainingCapacity();
    }

    fn requestReadableLeaseViaRequester(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *ManagedHostService = @ptrCast(@alignCast(ptr));
        try self.requestReadableLease(group_id, request_ctx);
    }

    fn enqueueTransitionUpdates(self: *ManagedHostService, updates: []const metadata_view.MetadataUpdate) !void {
        if (self.transition_svc == null) return;
        for (updates) |update| {
            switch (update) {
                .transition => |transition| switch (transition) {
                    .upsert => |record| switch (record) {
                        .split => |split| try self.submitSplitTransition(split),
                        .merge => |merge| try self.submitMergeTransition(merge),
                    },
                    .remove => |record| switch (record.kind) {
                        .split => {
                            _ = self.transition_svc.?.removeSplit(record.transition_id);
                            self.syncTransitionMetrics();
                        },
                        .merge => {
                            _ = self.transition_svc.?.removeMerge(record.transition_id);
                            self.syncTransitionMetrics();
                        },
                    },
                },
                else => {},
            }
        }
    }

    fn syncTransitionMetrics(self: *ManagedHostService) void {
        if (self.transition_svc) |*transition_svc| {
            self.metrics.queued_split_transitions = transition_svc.metrics.queued_split_transitions;
            self.metrics.queued_merge_transitions = transition_svc.metrics.queued_merge_transitions;
            self.metrics.stepped_split_transitions = transition_svc.metrics.stepped_split_transitions;
            self.metrics.stepped_merge_transitions = transition_svc.metrics.stepped_merge_transitions;
            self.metrics.completed_split_transitions = transition_svc.metrics.completed_split_transitions;
            self.metrics.completed_merge_transitions = transition_svc.metrics.completed_merge_transitions;
            self.metrics.awaiting_split_source_start = transition_svc.metrics.awaiting_split_source_start;
            self.metrics.bootstrapping_split_destination = transition_svc.metrics.bootstrapping_split_destination;
            self.metrics.split_replay_blocked = transition_svc.metrics.split_replay_blocked;
            self.metrics.split_ready_to_finalize = transition_svc.metrics.split_ready_to_finalize;
            self.metrics.awaiting_merge_receiver_acceptance = transition_svc.metrics.awaiting_merge_receiver_acceptance;
            self.metrics.bootstrapping_merge_receiver = transition_svc.metrics.bootstrapping_merge_receiver;
            self.metrics.merge_replay_blocked = transition_svc.metrics.merge_replay_blocked;
            self.metrics.merge_ready_to_finalize = transition_svc.metrics.merge_ready_to_finalize;
        }
    }

    fn seedTransitionsFromMetadataStore(self: *ManagedHostService, metadata_group_id: ?u64) !void {
        if (self.transition_svc == null) return;
        const group_id = metadata_group_id orelse return;
        const store = self.host.owned_metadata_store orelse return;

        const split_records = try store.listSplitTransitions(self.alloc, group_id);
        defer store.freeSplitTransitions(self.alloc, split_records);
        for (split_records) |record| try self.submitSplitTransition(record);

        const merge_records = try store.listMergeTransitions(self.alloc, group_id);
        defer store.freeMergeTransitions(self.alloc, merge_records);
        for (merge_records) |record| try self.submitMergeTransition(record);
    }
};

pub const ManagedHttpHostService = struct {
    alloc: std.mem.Allocator,
    cfg: ManagedServiceConfig,
    host: managed_host.ManagedHttpHost,
    local_transition_runtime: ?transition_runtime.TransitionRuntime = null,
    pending_updates: std.ArrayListUnmanaged(metadata_view.MetadataUpdate) = .empty,
    transition_svc: ?transition_service.TransitionService = null,
    metrics: ManagedServiceMetrics = .{},

    pub fn init(
        alloc: std.mem.Allocator,
        host_cfg: managed_host.ManagedHttpHostConfig,
        host_deps: managed_host.ManagedHttpHostDeps,
        cfg: ManagedServiceConfig,
        deps: ManagedServiceDeps,
    ) !ManagedHttpHostService {
        var svc = ManagedHttpHostService{
            .alloc = alloc,
            .cfg = cfg,
            .host = try managed_host.ManagedHttpHost.init(alloc, host_cfg, host_deps),
            .local_transition_runtime = deps.transition_runtime,
            .transition_svc = if (deps.transition_ops) |ops|
                transition_service.TransitionService.init(alloc, ops)
            else if (deps.transition_runtime) |runtime|
                transition_service.TransitionService.init(alloc, runtime)
            else
                null,
        };
        errdefer svc.deinit();
        try svc.seedTransitionsFromMetadataStore(host_cfg.http.host.metadata_group_id);
        return svc;
    }

    pub fn deinit(self: *ManagedHttpHostService) void {
        self.clearPending();
        self.pending_updates.deinit(self.alloc);
        if (self.transition_svc) |*transition_svc| transition_svc.deinit();
        self.host.deinit();
        self.* = undefined;
    }

    pub fn start(self: *ManagedHttpHostService) !void {
        try self.host.start();
    }

    pub fn stop(self: *ManagedHttpHostService) void {
        self.host.stop();
    }

    pub fn baseUri(self: *ManagedHttpHostService, alloc: std.mem.Allocator) ![]u8 {
        return try self.host.baseUri(alloc);
    }

    pub fn replaceTransitionOps(self: *ManagedHttpHostService, ops: shard_ops.ShardOperationAdapter) !void {
        if (self.transition_svc) |*transition_svc| transition_svc.deinit();
        self.transition_svc = null;
        self.transition_svc = transition_service.TransitionService.init(self.alloc, ops);
        errdefer {
            if (self.transition_svc) |*transition_svc| transition_svc.deinit();
            self.transition_svc = null;
        }
        try self.seedTransitionsFromMetadataStore(self.host.http_host.host.cfg.metadata_group_id);
        self.syncTransitionMetrics();
    }

    pub fn submit(self: *ManagedHttpHostService, update: metadata_view.MetadataUpdate) !void {
        try self.pending_updates.append(self.alloc, try update.clone(self.alloc));
        self.metrics.queued_updates = self.pending_updates.items.len;
    }

    pub fn submitBatch(self: *ManagedHttpHostService, updates: []const metadata_view.MetadataUpdate) !void {
        for (updates) |update| try self.submit(update);
    }

    pub fn submitSplitTransition(self: *ManagedHttpHostService, record: metadata.SplitTransitionRecord) !void {
        if (self.transition_svc) |*transition_svc| {
            try transition_svc.submitSplit(record);
            self.syncTransitionMetrics();
        }
    }

    pub fn submitMergeTransition(self: *ManagedHttpHostService, record: metadata.MergeTransitionRecord) !void {
        if (self.transition_svc) |*transition_svc| {
            try transition_svc.submitMerge(record);
            self.syncTransitionMetrics();
        }
    }

    pub fn requestReadableLease(self: *ManagedHttpHostService, group_id: u64, request_ctx: []const u8) !void {
        try self.host.http_host.readIndex(group_id, request_ctx);
        self.metrics.read_lease_requests += 1;
    }

    pub fn readableLeaseRequester(self: *ManagedHttpHostService) read_gate.ReadableLeaseRequester {
        return .{
            .ptr = self,
            .vtable = &.{
                .request_readable_lease = requestReadableLeaseViaRequester,
            },
        };
    }

    pub fn featureReads(self: *ManagedHttpHostService) feature_reads.FeatureReads {
        return feature_reads.FeatureReads.init(self.readableLeaseRequester());
    }

    pub fn shardOperationAdapter(self: *ManagedHttpHostService) ?shard_ops.ShardOperationAdapter {
        if (self.transition_svc) |*transition_svc| return transition_svc.shardOperationAdapter();
        if (self.local_transition_runtime) |*runtime| return runtime.shardOperationAdapter();
        return null;
    }

    pub fn prepareEnrichmentRead(
        self: *ManagedHttpHostService,
        group_id: u64,
        kind: read_gate.EnrichmentReadKind,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const gate = read_gate.EnrichmentReadGate.init(self.readableLeaseRequester());
        try gate.prepare(group_id, kind, consistency);
    }

    pub fn prepareSearchRead(self: *ManagedHttpHostService, group_id: u64) !void {
        try self.prepareEnrichmentRead(group_id, .search, .read_index);
    }

    pub fn prepareSearchRequestWithConsistency(
        self: *ManagedHttpHostService,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.featureReads().prepareSearchWithConsistency(group_id, req, consistency);
    }

    pub fn prepareSearchRequest(self: *ManagedHttpHostService, group_id: u64, req: db_types.SearchRequest) !void {
        try self.prepareSearchRequestWithConsistency(group_id, req, .read_index);
    }

    pub fn prepareLookupRead(self: *ManagedHttpHostService, group_id: u64) !void {
        try self.prepareEnrichmentRead(group_id, .lookup, .read_index);
    }

    pub fn prepareLookupRequestWithConsistency(
        self: *ManagedHttpHostService,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.featureReads().prepareLookupWithConsistency(group_id, key, opts, consistency);
    }

    pub fn prepareLookupRequest(self: *ManagedHttpHostService, group_id: u64, key: []const u8, opts: db_types.LookupOptions) !void {
        try self.prepareLookupRequestWithConsistency(group_id, key, opts, .read_index);
    }

    pub fn prepareScanRead(self: *ManagedHttpHostService, group_id: u64) !void {
        try self.prepareEnrichmentRead(group_id, .scan, .read_index);
    }

    pub fn prepareScanRequestWithConsistency(
        self: *ManagedHttpHostService,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        try self.featureReads().prepareScanWithConsistency(group_id, from_key, to_key, opts, consistency);
    }

    pub fn prepareScanRequest(
        self: *ManagedHttpHostService,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
    ) !void {
        try self.prepareScanRequestWithConsistency(group_id, from_key, to_key, opts, .read_index);
    }

    pub fn syncPending(self: *ManagedHttpHostService) !managed_host.ManagedSyncResult {
        const result = try self.host.syncOnce(
            self.pending_updates.items,
            self.cfg.max_tick_groups,
            self.cfg.max_ready_groups,
        );
        try self.enqueueTransitionUpdates(self.pending_updates.items);
        self.metrics.applied_updates += self.pending_updates.items.len;
        self.metrics.sync_rounds += 1;
        self.clearPending();
        self.metrics.queued_updates = 0;
        _ = try self.stepTransitions();
        return result;
    }

    pub fn syncPendingRaftOnly(self: *ManagedHttpHostService) !managed_host.ManagedSyncResult {
        const result = try self.host.syncOnce(
            self.pending_updates.items,
            self.cfg.max_tick_groups,
            self.cfg.max_ready_groups,
        );
        try self.enqueueTransitionUpdates(self.pending_updates.items);
        self.metrics.applied_updates += self.pending_updates.items.len;
        self.metrics.sync_rounds += 1;
        self.clearPending();
        self.metrics.queued_updates = 0;
        return result;
    }

    pub fn runRound(self: *ManagedHttpHostService) !void {
        _ = try self.host.runRound(self.cfg.max_tick_groups, self.cfg.max_ready_groups);
        self.metrics.sync_rounds += 1;
        _ = try self.stepTransitions();
    }

    pub fn runRaftRoundOnly(self: *ManagedHttpHostService) !void {
        _ = try self.host.runRound(self.cfg.max_tick_groups, self.cfg.max_ready_groups);
        self.metrics.sync_rounds += 1;
    }

    pub fn stepTransitions(self: *ManagedHttpHostService) !transition_service.TransitionStepResult {
        if (self.transition_svc) |*transition_svc| {
            const result = try transition_svc.stepPending();
            self.syncTransitionMetrics();
            return result;
        }
        return .{};
    }

    pub fn observeSplitTransition(self: *ManagedHttpHostService, transition_id: u64) !?metadata.SplitObservation {
        if (self.transition_svc) |*transition_svc| {
            var observation = (try transition_svc.observeSplit(transition_id)) orelse return null;
            if (transition_svc.splitRecord(transition_id)) |record| {
                observation.source_local_leader = observation.source_local_leader or self.host.http_host.host.isLocalLeader(record.source_group_id);
                observation.destination_local_leader = observation.destination_local_leader or self.host.http_host.host.isLocalLeader(record.destination_group_id);
            }
            return observation;
        }
        return null;
    }

    pub fn describeSplitTransition(self: *ManagedHttpHostService, transition_id: u64) !?metadata.SplitExecutionState {
        if (self.transition_svc) |*transition_svc| {
            return try transition_svc.describeSplit(transition_id);
        }
        return null;
    }

    pub fn observeMergeTransition(self: *ManagedHttpHostService, transition_id: u64) !?metadata.MergeObservation {
        if (self.transition_svc) |*transition_svc| {
            var observation = (try transition_svc.observeMerge(transition_id)) orelse return null;
            if (transition_svc.mergeRecord(transition_id)) |record| {
                observation.donor_local_leader = observation.donor_local_leader or self.host.http_host.host.isLocalLeader(record.donor_group_id);
                observation.receiver_local_leader = observation.receiver_local_leader or self.host.http_host.host.isLocalLeader(record.receiver_group_id);
            }
            return observation;
        }
        return null;
    }

    pub fn describeMergeTransition(self: *ManagedHttpHostService, transition_id: u64) !?metadata.MergeExecutionState {
        if (self.transition_svc) |*transition_svc| {
            return try transition_svc.describeMerge(transition_id);
        }
        return null;
    }

    fn clearPending(self: *ManagedHttpHostService) void {
        for (self.pending_updates.items) |*update| update.deinit(self.alloc);
        self.pending_updates.clearRetainingCapacity();
    }

    fn requestReadableLeaseViaRequester(ptr: *anyopaque, group_id: u64, request_ctx: []const u8) !void {
        const self: *ManagedHttpHostService = @ptrCast(@alignCast(ptr));
        try self.requestReadableLease(group_id, request_ctx);
    }

    fn enqueueTransitionUpdates(self: *ManagedHttpHostService, updates: []const metadata_view.MetadataUpdate) !void {
        if (self.transition_svc == null) return;
        for (updates) |update| {
            switch (update) {
                .transition => |transition| switch (transition) {
                    .upsert => |record| switch (record) {
                        .split => |split| try self.submitSplitTransition(split),
                        .merge => |merge| try self.submitMergeTransition(merge),
                    },
                    .remove => |record| switch (record.kind) {
                        .split => {
                            _ = self.transition_svc.?.removeSplit(record.transition_id);
                            self.syncTransitionMetrics();
                        },
                        .merge => {
                            _ = self.transition_svc.?.removeMerge(record.transition_id);
                            self.syncTransitionMetrics();
                        },
                    },
                },
                else => {},
            }
        }
    }

    fn syncTransitionMetrics(self: *ManagedHttpHostService) void {
        if (self.transition_svc) |*transition_svc| {
            self.metrics.queued_split_transitions = transition_svc.metrics.queued_split_transitions;
            self.metrics.queued_merge_transitions = transition_svc.metrics.queued_merge_transitions;
            self.metrics.stepped_split_transitions = transition_svc.metrics.stepped_split_transitions;
            self.metrics.stepped_merge_transitions = transition_svc.metrics.stepped_merge_transitions;
            self.metrics.completed_split_transitions = transition_svc.metrics.completed_split_transitions;
            self.metrics.completed_merge_transitions = transition_svc.metrics.completed_merge_transitions;
            self.metrics.awaiting_split_source_start = transition_svc.metrics.awaiting_split_source_start;
            self.metrics.bootstrapping_split_destination = transition_svc.metrics.bootstrapping_split_destination;
            self.metrics.split_replay_blocked = transition_svc.metrics.split_replay_blocked;
            self.metrics.split_ready_to_finalize = transition_svc.metrics.split_ready_to_finalize;
            self.metrics.awaiting_merge_receiver_acceptance = transition_svc.metrics.awaiting_merge_receiver_acceptance;
            self.metrics.bootstrapping_merge_receiver = transition_svc.metrics.bootstrapping_merge_receiver;
            self.metrics.merge_replay_blocked = transition_svc.metrics.merge_replay_blocked;
            self.metrics.merge_ready_to_finalize = transition_svc.metrics.merge_ready_to_finalize;
        }
    }

    fn seedTransitionsFromMetadataStore(self: *ManagedHttpHostService, metadata_group_id: ?u64) !void {
        if (self.transition_svc == null) return;
        const group_id = metadata_group_id orelse return;
        const store = self.host.owned_metadata_store orelse return;

        const split_records = try store.listSplitTransitions(self.alloc, group_id);
        defer store.freeSplitTransitions(self.alloc, split_records);
        for (split_records) |record| try self.submitSplitTransition(record);

        const merge_records = try store.listMergeTransitions(self.alloc, group_id);
        defer store.freeMergeTransitions(self.alloc, merge_records);
        for (merge_records) |record| try self.submitMergeTransition(record);
    }
};

test "managed host service syncs queued metadata updates" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var svc = try ManagedHostService.init(std.testing.allocator, .{
        .host = .{ .local_node_id = 1 },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    }, .{}, .{});
    defer svc.deinit();

    try svc.submit(.{
        .replica_intent = .{
            .upsert = .{
                .record = .{
                    .group_id = 801,
                    .replica_id = 1,
                    .local_node_id = 1,
                },
                .peer_node_ids = &.{},
            },
        },
    });

    const result = try svc.syncPending();
    try std.testing.expectEqual(@as(usize, 1), result.reconcile.ensured);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.applied_updates);
    try std.testing.expectEqual(@as(usize, 0), svc.metrics.queued_updates);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.sync_rounds);
}

test "managed host service steps queued transitions during runtime rounds" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const FakeSplit = struct {
        status: @import("../data/mod.zig").SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },

        fn iface(self: *@This()) transition_runtime.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !@import("../data/mod.zig").SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .replay_deltas;
            self.status.bootstrapped = true;
            self.status.source_delta_sequence = 1;
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .cutover_ready;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.destination_ready_for_reads = true;
            self.status.dest_delta_sequence = 1;
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .finalized;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var fake_split = FakeSplit{};
    var svc = try ManagedHostService.init(std.testing.allocator, .{
        .host = .{ .local_node_id = 1 },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    }, .{}, .{
        .transition_runtime = .{ .split = fake_split.iface() },
    });
    defer svc.deinit();

    try svc.submitSplitTransition(.{
        .transition_id = 77,
        .source_group_id = 7,
        .destination_group_id = 8,
    });

    try svc.runRound();
    try svc.runRound();
    try svc.runRound();
    try svc.runRound();
    try svc.runRound();

    try std.testing.expectEqual(@as(usize, 0), svc.metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.completed_split_transitions);
}

test "managed host service queues transition metadata updates after sync" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const FakeSplit = struct {
        status: @import("../data/mod.zig").SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },

        fn iface(self: *@This()) transition_runtime.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !@import("../data/mod.zig").SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .replay_deltas;
            self.status.bootstrapped = true;
            self.status.source_delta_sequence = 1;
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .cutover_ready;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.destination_ready_for_reads = true;
            self.status.dest_delta_sequence = 1;
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .finalized;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var fake_split = FakeSplit{};
    var svc = try ManagedHostService.init(std.testing.allocator, .{
        .host = .{ .local_node_id = 1 },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    }, .{}, .{
        .transition_runtime = .{ .split = fake_split.iface() },
    });
    defer svc.deinit();

    try svc.submit(.{
        .transition = .{
            .upsert = .{
                .split = .{
                    .transition_id = 88,
                    .source_group_id = 8,
                    .destination_group_id = 9,
                },
            },
        },
    });

    _ = try svc.syncPending();
    try std.testing.expectEqual(@as(usize, 1), svc.metrics.queued_split_transitions);

    try svc.runRound();
    try svc.runRound();
    try svc.runRound();
    try svc.runRound();

    try std.testing.expectEqual(@as(usize, 1), svc.metrics.completed_split_transitions);
    try std.testing.expectEqual(@as(usize, 0), svc.metrics.queued_split_transitions);
}

test "managed host service seeds queued transitions from projected metadata store on restart" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) host_mod.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers[0..],
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const FakeSplit = struct {
        status: @import("../data/mod.zig").SplitTransitionStatus = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },

        fn iface(self: *@This()) transition_runtime.SplitRuntime {
            return .{
                .ptr = self,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(ptr: *anyopaque, _: u64, _: u64) !@import("../data/mod.zig").SplitTransitionStatus {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.status;
        }

        fn prepareSource(ptr: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .prepare;
            self.status.source_split_phase = .prepare;
            return true;
        }

        fn startSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .bootstrap_peer;
            self.status.source_split_phase = .splitting;
            self.status.replay_required = true;
            return true;
        }

        fn bootstrapDestination(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .replay_deltas;
            self.status.bootstrapped = true;
            self.status.source_delta_sequence = 1;
            return true;
        }

        fn catchUpDestination(ptr: *anyopaque, _: u64, _: u64) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .cutover_ready;
            self.status.replay_caught_up = true;
            self.status.cutover_ready = true;
            self.status.destination_ready_for_reads = true;
            self.status.dest_delta_sequence = 1;
            return 1;
        }

        fn finalizeSource(ptr: *anyopaque, _: u64, _: u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.status.phase = .finalized;
            self.status.source_split_phase = .none;
            self.status.replay_required = false;
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-transition-seed", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-transition-seed/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    {
        var svc = try ManagedHostService.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1300,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        }, .{}, .{});
        defer svc.deinit();

        _ = try svc.host.host.ensureReplica(.{
            .group_id = 1300,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        try svc.host.host.campaignGroup(1300);
        _ = try svc.host.host.runRound(1, 1);

        const cmd = try metadata.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 901,
                .source_group_id = 1300,
                .destination_group_id = 1301,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);

        try svc.host.host.propose(1300, cmd);
        _ = try svc.host.host.runRound(1, 1);

        const metadata_store = svc.host.owned_metadata_store orelse return error.MissingMetadataStore;
        const projected = try metadata_store.listSplitTransitions(std.testing.allocator, 1300);
        defer metadata_store.freeSplitTransitions(std.testing.allocator, projected);
        try std.testing.expectEqual(@as(usize, 1), projected.len);
        try std.testing.expectEqual(@as(u64, 901), projected[0].transition_id);
    }

    var fake_split = FakeSplit{};
    var restarted = try ManagedHostService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1300,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    }, .{}, .{
        .transition_runtime = .{ .split = fake_split.iface() },
    });
    defer restarted.deinit();

    try std.testing.expectEqual(@as(usize, 1), restarted.metrics.queued_split_transitions);

    try restarted.runRound();
    try restarted.runRound();
    try restarted.runRound();
    try restarted.runRound();
    try restarted.runRound();

    try std.testing.expectEqual(@as(usize, 0), restarted.metrics.queued_split_transitions);
    try std.testing.expectEqual(@as(usize, 1), restarted.metrics.completed_split_transitions);
}

test "managed host service resumes real split transition after restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-split-restart", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-split-restart/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-split-restart-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-split-restart-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();

        const prepare = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:b={\"v\":\"left-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
        });
        defer std.testing.allocator.free(prepare);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 1701,
            .commit_index = 4,
            .entries_bytes = prepare,
        });
    }

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = TestSingleNodeFactory{ .alloc = std.testing.allocator, .store = &store };

    {
        var svc = try ManagedHostService.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1300,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        }, .{}, .{});
        defer svc.deinit();

        _ = try svc.host.host.ensureReplica(.{
            .group_id = 1300,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        try svc.host.host.campaignGroup(1300);
        _ = try svc.host.host.runRound(1, 1);

        const cmd = try metadata.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_split_transition = .{
                .transition_id = 1001,
                .source_group_id = 1701,
                .destination_group_id = 1702,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        try svc.host.host.propose(1300, cmd);
        _ = try svc.host.host.runRound(1, 1);
    }

    {
        var split = try transition_runtime.SplitCoordinatorRuntime.init(std.testing.allocator, .{
            .source_root_dir = src_root,
            .dest_root_dir = dst_root,
            .source_group_id = 1701,
            .dest_group_id = 1702,
        });
        defer split.deinit();

        var svc = try ManagedHostService.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1300,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        }, .{}, .{
            .transition_runtime = .{ .split = split.runtime() },
        });
        defer svc.deinit();

        try std.testing.expectEqual(@as(usize, 1), svc.metrics.queued_split_transitions);
        try svc.runRound();
        const state = (try svc.describeSplitTransition(1001)) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(metadata.SplitExecutionStateTag.bootstrapping_destination, state.tag);
    }

    {
        var split = try transition_runtime.SplitCoordinatorRuntime.init(std.testing.allocator, .{
            .source_root_dir = src_root,
            .dest_root_dir = dst_root,
            .source_group_id = 1701,
            .dest_group_id = 1702,
        });
        defer split.deinit();

        var restarted = try ManagedHostService.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1300,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        }, .{}, .{
            .transition_runtime = .{ .split = split.runtime() },
        });
        defer restarted.deinit();

        try std.testing.expectEqual(@as(usize, 1), restarted.metrics.queued_split_transitions);
        var rounds: usize = 0;
        while (rounds < 8 and restarted.metrics.completed_split_transitions == 0) : (rounds += 1) {
            try restarted.runRound();
        }

        try std.testing.expectEqual(@as(usize, 0), restarted.metrics.queued_split_transitions);
        try std.testing.expectEqual(@as(usize, 1), restarted.metrics.completed_split_transitions);

        var dest = try data.SplitDestination.init(std.testing.allocator, .{ .root_dir = dst_root });
        defer dest.deinit();
        const range = dest.getRange();
        try std.testing.expectEqualStrings("doc:m", range.start);
        try std.testing.expectEqualStrings("doc:z", range.end);
        const right = (try dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(right);
        try std.testing.expectEqualStrings("{\"v\":\"right-0\"}", right);
    }
}

test "managed host service resumes real merge transition after restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-merge-restart", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-merge-restart/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-merge-restart-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-real-merge-restart-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();

        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 1801,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    {
        var receiver = try data.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = TestSingleNodeFactory{ .alloc = std.testing.allocator, .store = &store };

    {
        var svc = try ManagedHostService.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1300,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        }, .{}, .{});
        defer svc.deinit();

        _ = try svc.host.host.ensureReplica(.{
            .group_id = 1300,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        try svc.host.host.campaignGroup(1300);
        _ = try svc.host.host.runRound(1, 1);

        const cmd = try metadata.encodeTransitionCommand(std.testing.allocator, .{
            .upsert_merge_transition = .{
                .transition_id = 1002,
                .donor_group_id = 1801,
                .receiver_group_id = 1802,
                .phase = .prepare,
            },
        });
        defer std.testing.allocator.free(cmd);
        try svc.host.host.propose(1300, cmd);
        _ = try svc.host.host.runRound(1, 1);
    }

    {
        var merge = try transition_runtime.MergeCoordinatorRuntime.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 1801,
            .receiver_group_id = 1802,
        });
        defer merge.deinit();

        var svc = try ManagedHostService.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1300,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        }, .{}, .{
            .transition_runtime = .{ .merge = merge.runtime() },
        });
        defer svc.deinit();

        try std.testing.expectEqual(@as(usize, 1), svc.metrics.queued_merge_transitions);
        try svc.runRound();
        const state = (try svc.describeMergeTransition(1002)) orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(metadata.MergeExecutionStateTag.bootstrapping_receiver, state.tag);
    }

    {
        var merge = try transition_runtime.MergeCoordinatorRuntime.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 1801,
            .receiver_group_id = 1802,
        });
        defer merge.deinit();

        var restarted = try ManagedHostService.init(std.testing.allocator, .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 1300,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
        }, .{
            .host = .{ .descriptor_factory = factory.iface() },
        }, .{}, .{
            .transition_runtime = .{ .merge = merge.runtime() },
        });
        defer restarted.deinit();

        try std.testing.expectEqual(@as(usize, 1), restarted.metrics.queued_merge_transitions);
        var rounds: usize = 0;
        while (rounds < 8 and restarted.metrics.completed_merge_transitions == 0) : (rounds += 1) {
            try restarted.runRound();
        }

        try std.testing.expectEqual(@as(usize, 0), restarted.metrics.queued_merge_transitions);
        try std.testing.expectEqual(@as(usize, 1), restarted.metrics.completed_merge_transitions);

        var receiver = try data.SplitDestination.init(std.testing.allocator, .{ .root_dir = receiver_root });
        defer receiver.deinit();
        const range = receiver.getRange();
        try std.testing.expectEqualStrings("doc:a", range.start);
        try std.testing.expectEqualStrings("doc:z", range.end);
        const donor_doc = (try receiver.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
        defer std.testing.allocator.free(donor_doc);
        try std.testing.expectEqualStrings("{\"v\":\"donor\"}", donor_doc);
    }
}

test "managed host service reports local leader roles in split observations" {
    const FakeSplit = struct {
        fn iface() transition_runtime.SplitRuntime {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .prepare_source = prepareSource,
                    .start_source = startSource,
                    .bootstrap_destination = bootstrapDestination,
                    .catch_up_destination = catchUpDestination,
                    .finalize_source = finalizeSource,
                    .rollback_source = rollbackSource,
                },
            };
        }

        fn observeStatus(_: *anyopaque, _: u64, _: u64) !data.SplitTransitionStatus {
            return .{
                .phase = .bootstrap_peer,
                .source_split_phase = .splitting,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = false,
                .cutover_ready = false,
                .destination_ready_for_reads = false,
                .source_delta_sequence = 1,
                .dest_delta_sequence = 0,
            };
        }

        fn prepareSource(_: *anyopaque, _: u64, _: u64, _: []const u8, _: ?[]const u8) !bool {
            return true;
        }

        fn startSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn bootstrapDestination(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn catchUpDestination(_: *anyopaque, _: u64, _: u64) !usize {
            return 0;
        }

        fn finalizeSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn rollbackSource(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-split-leader-observation", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-split-leader-observation/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = TestSingleNodeFactory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try ManagedHostService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1300,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    }, .{}, .{
        .transition_runtime = .{ .split = FakeSplit.iface() },
    });
    defer svc.deinit();

    _ = try svc.host.host.ensureReplica(.{ .group_id = 1701, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted });
    _ = try svc.host.host.ensureReplica(.{ .group_id = 1702, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted });
    try svc.host.host.campaignGroup(1701);
    try svc.host.host.campaignGroup(1702);

    var rounds: usize = 0;
    while (rounds < 8 and (!svc.host.host.isLocalLeader(1701) or !svc.host.host.isLocalLeader(1702))) : (rounds += 1) {
        _ = try svc.host.host.runRound(8, 8);
    }

    try std.testing.expect(svc.host.host.isLocalLeader(1701));
    try std.testing.expect(svc.host.host.isLocalLeader(1702));

    try svc.submitSplitTransition(.{
        .transition_id = 2001,
        .source_group_id = 1701,
        .destination_group_id = 1702,
        .phase = .bootstrap_peer,
    });

    const observation = (try svc.observeSplitTransition(2001)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(data.RangeTransitionPhase, .bootstrap_peer), observation.status.phase);
    try std.testing.expect(observation.source_local_leader);
    try std.testing.expect(observation.destination_local_leader);
}

test "managed host service reports local leader roles in merge observations" {
    const FakeMerge = struct {
        fn iface() transition_runtime.MergeRuntime {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .observe_status = observeStatus,
                    .accept_receiver = acceptReceiver,
                    .catch_up_receiver = catchUpReceiver,
                    .finalize_merge = finalizeMerge,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeStatus(_: *anyopaque, donor_group_id: u64, receiver_group_id: u64) !data.MergeTransitionStatus {
            return .{
                .phase = .bootstrap_peer,
                .donor_group_id = donor_group_id,
                .receiver_group_id = receiver_group_id,
                .receiver_accepts_donor_range = true,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 1,
                .receiver_delta_sequence = 0,
            };
        }

        fn acceptReceiver(_: *anyopaque, _: u64, _: u64) !void {}

        fn catchUpReceiver(_: *anyopaque, _: u64, _: u64) !usize {
            return 0;
        }

        fn finalizeMerge(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }

        fn rollbackMerge(_: *anyopaque, _: u64, _: u64) !bool {
            return true;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-merge-leader-observation", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-merge-leader-observation/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = TestSingleNodeFactory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try ManagedHostService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1300,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    }, .{}, .{
        .transition_runtime = .{ .merge = FakeMerge.iface() },
    });
    defer svc.deinit();

    _ = try svc.host.host.ensureReplica(.{ .group_id = 1801, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted });
    _ = try svc.host.host.ensureReplica(.{ .group_id = 1802, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted });
    try svc.host.host.campaignGroup(1801);
    try svc.host.host.campaignGroup(1802);

    var rounds: usize = 0;
    while (rounds < 8 and (!svc.host.host.isLocalLeader(1801) or !svc.host.host.isLocalLeader(1802))) : (rounds += 1) {
        _ = try svc.host.host.runRound(8, 8);
    }

    try std.testing.expect(svc.host.host.isLocalLeader(1801));
    try std.testing.expect(svc.host.host.isLocalLeader(1802));

    try svc.submitMergeTransition(.{
        .transition_id = 2002,
        .donor_group_id = 1801,
        .receiver_group_id = 1802,
        .phase = .bootstrap_peer,
    });

    const observation = (try svc.observeMergeTransition(2002)) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(data.RangeTransitionPhase, .bootstrap_peer), observation.receiver.phase);
    try std.testing.expect(observation.donor_local_leader);
    try std.testing.expect(observation.receiver_local_leader);
}

test "managed host service preserves leader-routed observation roles from transition ops" {
    const FakeShardOps = struct {
        fn adapter() shard_ops.ShardOperationAdapter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .observe_split = observeSplit,
                    .observe_merge = observeMerge,
                    .prepare_split_source = noopPrepareSplitSource,
                    .start_split_source = noopStartSplitSource,
                    .bootstrap_split_destination = noopBootstrapSplitDestination,
                    .catch_up_split_destination = noopCatchUpSplitDestination,
                    .finalize_split_source = noopFinalizeSplitSource,
                    .rollback_split = noopRollbackSplit,
                    .accept_merge_receiver = noopAcceptMergeReceiver,
                    .catch_up_merge_receiver = noopCatchUpMergeReceiver,
                    .finalize_merge = noopFinalizeMerge,
                    .rollback_merge = noopRollbackMerge,
                },
            };
        }

        fn observeSplit(_: *anyopaque, _: metadata.SplitTransitionRecord) !metadata.SplitObservation {
            return .{
                .status = .{
                    .phase = .cutover_ready,
                    .source_split_phase = .splitting,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 9,
                    .dest_delta_sequence = 9,
                },
                .source_local_leader = true,
                .destination_local_leader = false,
            };
        }

        fn observeMerge(_: *anyopaque, record: metadata.MergeTransitionRecord) !metadata.MergeObservation {
            const status = data.MergeTransitionStatus{
                .phase = .replay_deltas,
                .donor_group_id = record.donor_group_id,
                .receiver_group_id = record.receiver_group_id,
                .receiver_accepts_donor_range = true,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = false,
                .cutover_ready = false,
                .receiver_ready_for_reads = false,
                .donor_delta_sequence = 5,
                .receiver_delta_sequence = 4,
            };
            return .{
                .donor = status,
                .receiver = status,
                .donor_local_leader = false,
                .receiver_local_leader = true,
            };
        }

        fn noopPrepareSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .prepare_split_source).type) !void {}
        fn noopStartSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .start_split_source).type) !void {}
        fn noopBootstrapSplitDestination(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .bootstrap_split_destination).type) !void {}
        fn noopCatchUpSplitDestination(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .catch_up_split_destination).type) !void {}
        fn noopFinalizeSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .finalize_split_source).type) !void {}
        fn noopRollbackSplit(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .rollback_split).type) !void {}
        fn noopAcceptMergeReceiver(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .accept_merge_receiver).type) !void {}
        fn noopCatchUpMergeReceiver(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .catch_up_merge_receiver).type) !void {}
        fn noopFinalizeMerge(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .finalize_merge).type) !void {}
        fn noopRollbackMerge(_: *anyopaque, _: std.meta.fieldInfo(metadata.TransitionAction, .rollback_merge).type) !void {}
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-preserve-routed-observation", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/svc-preserve-routed-observation/catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = TestSingleNodeFactory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try ManagedHostService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1300,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{ .descriptor_factory = factory.iface() },
    }, .{}, .{
        .transition_ops = FakeShardOps.adapter(),
    });
    defer svc.deinit();

    try svc.submitSplitTransition(.{
        .transition_id = 3001,
        .source_group_id = 1901,
        .destination_group_id = 1902,
        .phase = .bootstrap_peer,
    });
    try svc.submitMergeTransition(.{
        .transition_id = 3002,
        .donor_group_id = 2901,
        .receiver_group_id = 2902,
        .phase = .bootstrap_peer,
    });

    const split = (try svc.observeSplitTransition(3001)) orelse return error.TestExpectedEqual;
    try std.testing.expect(split.source_local_leader);
    try std.testing.expect(!split.destination_local_leader);

    const merge = (try svc.observeMergeTransition(3002)) orelse return error.TestExpectedEqual;
    try std.testing.expect(!merge.donor_local_leader);
    try std.testing.expect(merge.receiver_local_leader);
}

test "managed service module compiles" {
    _ = ManagedServiceConfig;
    _ = ManagedServiceDeps;
    _ = ManagedServiceMetrics;
    _ = ManagedHostService;
    _ = ManagedHttpHostService;
}
