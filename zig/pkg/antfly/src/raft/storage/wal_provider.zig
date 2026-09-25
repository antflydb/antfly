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
const catalog = @import("../catalog.zig");
const host = @import("../host.zig");
const state_machine = @import("../state_machine/mod.zig");
const storage_mod = @import("mod.zig");
const wal_replica_state = @import("wal_replica_state.zig");

pub const WalReplicaProviderConfig = struct {
    root_dir: []const u8,
    state: wal_replica_state.WalReplicaStateConfig = .{},
    flush_on_deinit: bool = true,
    completion_provider: ?raft_engine.runtime.completion_admission_iface.Provider = null,
};

pub const WalReplicaProvider = struct {
    pub const Diagnostics = struct {
        groups: usize = 0,
        entries: usize = 0,
        entry_capacity: usize = 0,
        entry_payload_bytes: usize = 0,
        estimated_bytes: usize = 0,
        max_entries_per_group: usize = 0,
        min_first_index: raft_engine.core.types.Index = 0,
        max_last_index: raft_engine.core.types.Index = 0,
        max_snapshot_index: raft_engine.core.types.Index = 0,
        storage_compactions: u64 = 0,
    };

    alloc: std.mem.Allocator,
    cfg: WalReplicaProviderConfig,
    root_dir: []u8,
    base_factory: host.ReplicaDescriptorFactory,
    states: std.AutoHashMapUnmanaged(u64, *wal_replica_state.WalReplicaState) = .empty,
    node_ids: std.AutoHashMapUnmanaged(u64, u64) = .empty,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: WalReplicaProviderConfig,
        base_factory: host.ReplicaDescriptorFactory,
    ) !WalReplicaProvider {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .root_dir = try alloc.dupe(u8, cfg.root_dir),
            .base_factory = base_factory,
        };
    }

    pub fn deinit(self: *WalReplicaProvider) void {
        var it = self.states.valueIterator();
        while (it.next()) |state| {
            if (self.cfg.flush_on_deinit) state.*.flushForShutdown() catch {};
            state.*.deinit();
            self.alloc.destroy(state.*);
        }
        self.states.deinit(self.alloc);
        self.node_ids.deinit(self.alloc);
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn flushForShutdown(self: *WalReplicaProvider) !void {
        var it = self.states.valueIterator();
        while (it.next()) |state| try state.*.flushForShutdown();
    }

    pub fn descriptorFactory(self: *WalReplicaProvider) host.ReplicaDescriptorFactory {
        return .{
            .ptr = self,
            .vtable = &.{
                .build_descriptor = buildDescriptor,
                .free_descriptor = freeDescriptor,
                .accepts_record = acceptsRecord,
            },
        };
    }

    pub fn runtimeHooks(self: *WalReplicaProvider) host.RuntimeHooks {
        return .{
            .group_storage = .{
                .ptr = self,
                .vtable = &.{
                    .persist_ready = persistReady,
                    .persist_ready_diagnostics = persistReadyWithDiagnostics,
                    .compact_snapshot = compactSnapshot,
                    .compact_snapshot_artifact = compactSnapshotArtifact,
                    .retire_group = retireGroup,
                },
            },
        };
    }

    fn appliedSinkVTable(comptime durable: bool) *const state_machine.AppliedIndexSink.VTable {
        return &.{ .set_applied_index = setAppliedIndex, .set_native_applied = if (durable) setNativeApplied else null, .set_durable_noop = if (durable) setDurableNoop else null };
    }

    pub fn appliedIndexSink(self: *WalReplicaProvider) state_machine.AppliedIndexSink {
        return .{ .ptr = self, .vtable = if (self.cfg.state.wal.no_sync) appliedSinkVTable(false) else appliedSinkVTable(true) };
    }

    pub fn stateForGroup(self: *WalReplicaProvider, group_id: u64) ?*wal_replica_state.WalReplicaState {
        return self.states.get(group_id);
    }

    pub fn diagnostics(self: *const WalReplicaProvider) Diagnostics {
        var out = Diagnostics{ .groups = self.states.count() };
        var it = self.states.valueIterator();
        while (it.next()) |state_ptr| {
            const state = state_ptr.*;
            const storage = state.store.diagnostics();
            out.entries += storage.entries;
            out.entry_capacity += storage.entry_capacity;
            out.entry_payload_bytes += storage.entry_payload_bytes;
            out.estimated_bytes += storage.estimated_bytes;
            out.max_entries_per_group = @max(out.max_entries_per_group, storage.entries);
            out.max_last_index = @max(out.max_last_index, storage.last_index);
            out.max_snapshot_index = @max(out.max_snapshot_index, storage.snapshot_index);
            if (out.min_first_index == 0 or storage.first_index < out.min_first_index) out.min_first_index = storage.first_index;
            out.storage_compactions += state.statsSnapshot().storage_compactions;
        }
        return out;
    }

    fn buildDescriptor(ptr: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = try self.ensureState(record);
        // A fetched snapshot becomes the local restart authority after its WAL
        // state is durable. Do not re-consume an intentionally short-lived
        // bootstrap artifact on every process restart.
        var effective_record = record;
        if (try state.storage().lastIndex() > 0) {
            effective_record.bootstrap_mode = .persisted;
            effective_record.snapshot_bootstrap = null;
        }
        var desc = try self.base_factory.buildDescriptor(effective_record);
        errdefer self.base_factory.freeDescriptor(self.alloc, &desc);
        // The base factory installs/restores native ownership. Query its
        // durable evidence only afterward, before publishing this descriptor.
        if (self.cfg.completion_provider) |provider| {
            if (!state.native_startup_reconciled) {
                try provider.reconcileDurable(record.group_id, record.local_node_id, state.completionDurableLog(true, null));
                state.native_startup_reconciled = true;
            }
            if (try provider.restoredProgress(record.group_id, record.local_node_id)) |progress|
                try state.restoreNativeProgress(progress);
        }
        try state.seedConfStateIfEmpty(desc.initial_voters orelse desc.group.raft_config.peers);
        desc.group.storage = state.storage();
        desc.group.raft_config.applied = state.completedAppliedIndex();
        desc.recover_persisted_snapshot = state.completedAppliedIndex() < state.store.snapshot_state.metadata.index;
        return desc;
    }

    fn acceptsRecord(ptr: *anyopaque, record: catalog.ReplicaRecord) bool {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        return self.base_factory.acceptsRecord(record);
    }

    fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        self.base_factory.freeDescriptor(alloc, desc);
    }

    fn persistReady(ptr: *anyopaque, group_id: u64, ready: raft_engine.core.Ready) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = self.states.get(group_id) orelse return error.UnknownGroup;
        try state.groupStorage().persistReady(group_id, ready);
        try self.reconcilePersisted(group_id, state, ready);
    }

    fn reconcilePersisted(self: *WalReplicaProvider, group_id: u64, state: *wal_replica_state.WalReplicaState, ready: raft_engine.core.Ready) !void {
        const provider = self.cfg.completion_provider orelse return;
        const node_id = self.node_ids.get(group_id) orelse return error.UnknownGroup;
        provider.reconcileDurable(group_id, node_id, state.completionDurableLog(false, ready)) catch |err| {
            state.native_progress_blocked = true;
            return err;
        };
    }

    fn persistReadyWithDiagnostics(
        ptr: *anyopaque,
        group_id: u64,
        ready: raft_engine.core.Ready,
        diag: *raft_engine.runtime.storage_iface.ReadyPersistenceDiagnostics,
    ) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = self.states.get(group_id) orelse return error.UnknownGroup;
        try state.groupStorage().persistReadyWithDiagnostics(group_id, ready, diag);
        try self.reconcilePersisted(group_id, state, ready);
    }

    fn compactSnapshot(ptr: *anyopaque, group_id: u64, snapshot: raft_engine.core.types.Snapshot, compact_index: u64) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = self.states.get(group_id) orelse return error.UnknownGroup;
        try state.groupStorage().compactSnapshot(group_id, snapshot, compact_index);
    }

    fn compactSnapshotArtifact(
        ptr: *anyopaque,
        group_id: u64,
        metadata: raft_engine.core.types.SnapshotMetadata,
        artifact: raft_engine.runtime.storage_iface.SnapshotArtifact,
        compact_index: u64,
    ) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = self.states.get(group_id) orelse return error.UnknownGroup;
        try state.groupStorage().compactSnapshotArtifact(self.alloc, group_id, metadata, artifact, compact_index);
    }

    fn setAppliedIndex(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        index: raft_engine.core.types.Index,
    ) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = self.states.get(group_id) orelse return error.UnknownGroup;
        try state.setAppliedIndex(index);
    }

    fn setDurableNoop(ptr: *anyopaque, group_id: u64, entry: raft_engine.core.Entry) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const replica = self.states.get(group_id) orelse return error.UnknownGroup;
        try replica.setDurableNoop(entry);
    }

    fn setNativeApplied(ptr: *anyopaque, group_id: u64, progress: state_machine.applied_sink.NativeProgress, entry: raft_engine.core.Entry) !void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const replica = self.states.get(group_id) orelse return error.UnknownGroup;
        try replica.setNativeApplied(progress, entry);
    }

    fn retireGroup(ptr: *anyopaque, group_id: u64) void {
        const self: *WalReplicaProvider = @ptrCast(@alignCast(ptr));
        const state = self.states.get(group_id) orelse {
            return;
        };
        if (self.cfg.flush_on_deinit) state.flushForShutdown() catch |err| {
            std.log.err("raft WAL replica retirement flush failed group_id={d} error={s}", .{ group_id, @errorName(err) });
        };
        const removed = self.states.fetchRemove(group_id) orelse unreachable;
        _ = self.node_ids.remove(group_id);
        removed.value.deinit();
        self.alloc.destroy(removed.value);
    }

    fn ensureState(self: *WalReplicaProvider, record: catalog.ReplicaRecord) !*wal_replica_state.WalReplicaState {
        if (self.states.get(record.group_id)) |state| return state;

        var layout = try storage_mod.ReplicaPathLayout.initForLocalNode(self.alloc, self.root_dir, record.group_id, record.local_node_id);
        defer layout.deinit(self.alloc);

        const state = try self.alloc.create(wal_replica_state.WalReplicaState);
        errdefer self.alloc.destroy(state);
        state.* = try wal_replica_state.WalReplicaState.init(self.alloc, layout, self.cfg.state);
        errdefer state.deinit();

        try self.node_ids.put(self.alloc, record.group_id, record.local_node_id);
        errdefer _ = self.node_ids.remove(record.group_id);
        try self.states.put(self.alloc, record.group_id, state);
        return state;
    }
};

test "workload admission native installation precedes durable reconciliation and descriptor publication" {
    const completion = raft_engine.runtime.completion_admission_iface;
    const Fixture = struct {
        store: *raft_engine.core.MemoryStorage,
        installed: bool = false,
        reject_reconciliation: bool = true,
        reconciliations: usize = 0,
        progress_queries: usize = 0,
        frees: usize = 0,
        proof: completion.Progress,

        fn build(raw: *anyopaque, record: catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const peers = try std.testing.allocator.dupe(u64, &.{record.local_node_id});
            // Production installs the native owner inside this base factory.
            self.installed = true;
            return .{ .group = .{
                .group_id = record.group_id,
                .local_node_id = record.local_node_id,
                .raft_config = .{ .id = record.local_node_id, .group_id = record.group_id, .peers = peers },
                .storage = self.store.storage(),
            }, .bootstrap = .persisted };
        }
        fn free(raw: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frees += 1;
            alloc.free(desc.group.raft_config.peers);
        }
        fn attach(_: *anyopaque, _: u64, _: u64, _: raft_engine.core.Storage) !completion.Guard {
            return error.UnexpectedGuardAttachment;
        }
        fn reconcile(raw: *anyopaque, _: u64, _: u64, log: completion.DurableLog) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.installed);
            try std.testing.expect(log.mode == .startup_complete and log.durability_confirmed);
            try std.testing.expectEqual(@as(u64, 1), log.commit_index);
            self.reconciliations += 1;
            if (self.reject_reconciliation) return error.CompletionAdmissionUnavailable;
        }
        fn progress(raw: *anyopaque, _: u64, _: u64) !?completion.Progress {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.installed and self.reconciliations != 0 and !self.reject_reconciliation);
            self.progress_queries += 1;
            return self.proof;
        }
    };
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/completion-order", .{tmp.sub_path});
    defer alloc.free(root);
    var store = raft_engine.core.MemoryStorage.init(alloc);
    defer store.deinit();
    const payload = "accepted-native-entry";
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &digest, .{});
    var fixture: Fixture = .{ .store = &store, .proof = .{ .term = 2, .index = 1, .payload_digest = digest } };
    var provider = try WalReplicaProvider.init(alloc, .{ .root_dir = root, .completion_provider = .{
        .ptr = &fixture,
        .vtable = &.{ .attach = Fixture.attach, .reconcile_durable = Fixture.reconcile, .restored_progress = Fixture.progress },
    } }, .{ .ptr = &fixture, .vtable = &.{ .build_descriptor = Fixture.build, .free_descriptor = Fixture.free } });
    defer provider.deinit();
    const record: catalog.ReplicaRecord = .{ .group_id = 19, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted };
    const state = try provider.ensureState(record);
    try state.groupStorage().persistReady(19, .{
        .hard_state = .{ .current_term = 2, .commit_index = 1 },
        .entries = &.{.{ .term = 2, .index = 1, .data = @constCast(payload) }},
    });
    try std.testing.expectError(error.CompletionAdmissionUnavailable, provider.descriptorFactory().buildDescriptor(record));
    try std.testing.expect(!state.native_startup_reconciled);
    try std.testing.expectEqual(@as(usize, 1), fixture.frees);
    try std.testing.expectEqual(@as(usize, 0), fixture.progress_queries);
    fixture.reject_reconciliation = false;
    {
        var desc = try provider.descriptorFactory().buildDescriptor(record);
        defer provider.descriptorFactory().freeDescriptor(alloc, &desc);
        // Native proof advances the physical apply watermark, but Raft must
        // replay the entry through its state machine before reporting completion.
        try std.testing.expectEqual(@as(u64, 1), state.appliedIndex());
        try std.testing.expectEqual(@as(u64, 0), desc.group.raft_config.applied);
    }
    try state.setAppliedIndex(1);
    var desc = try provider.descriptorFactory().buildDescriptor(record);
    defer provider.descriptorFactory().freeDescriptor(alloc, &desc);
    try std.testing.expectEqual(@as(u64, 1), desc.group.raft_config.applied);
    try std.testing.expect(state.native_startup_reconciled);
    try std.testing.expectEqual(@as(usize, 2), fixture.reconciliations);
    try std.testing.expectEqual(@as(usize, 2), fixture.progress_queries);
}

test "wal replica provider wires host through WAL-backed local state" {
    const BaseFactory = struct {
        alloc: std.mem.Allocator,
        dummy_store: *raft_engine.core.MemoryStorage,
        transport_peers: []const u64 = &.{},
        initial_voters: ?[]const u64 = null,

        fn iface(self: *@This()) host.ReplicaDescriptorFactory {
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
            const peer_source = if (self.transport_peers.len > 0) self.transport_peers else &.{record.local_node_id};
            const peers = try self.alloc.dupe(u64, peer_source);
            errdefer self.alloc.free(peers);
            const initial_voters = if (self.initial_voters) |voters| try self.alloc.dupe(u64, voters) else null;
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.dummy_store.storage(),
                },
                .initial_voters = initial_voters,
                .bootstrap = .persisted,
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            if (desc.initial_voters) |voters| self.alloc.free(voters);
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-provider", .{tmp.sub_path});
    defer std.testing.allocator.free(root);

    var dummy_store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer dummy_store.deinit();
    var base_factory = BaseFactory{ .alloc = std.testing.allocator, .dummy_store = &dummy_store };
    {
        var provider = try WalReplicaProvider.init(std.testing.allocator, .{ .root_dir = root }, base_factory.iface());
        defer provider.deinit();
        const runtime_hooks = provider.runtimeHooks();
        try std.testing.expect(runtime_hooks.group_storage.?.vtable.persist_ready_diagnostics != null);

        var local_host = host.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
            .descriptor_factory = provider.descriptorFactory(),
            .runtime_hooks = runtime_hooks,
        });
        defer local_host.deinit();

        _ = try local_host.ensureReplica(.{
            .group_id = 501,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        try local_host.campaignGroup(501);
        _ = try local_host.runRound(1, 1);
        try local_host.propose(501, "wal-backed");
        const persisted_round = try local_host.runRound(1, 1);
        try std.testing.expect(persisted_round.slowest_ready_group.persist_ready_detail.encoded_bytes > 0);
        try std.testing.expect(persisted_round.slowest_ready_group.persist_ready_detail.wal_append_elapsed_ns > 0);
        try std.testing.expect(persisted_round.slowest_ready_group.persist_ready_detail.wal_commit_elapsed_ns > 0);
        try std.testing.expect(persisted_round.slowest_ready_group.persist_ready_detail.wal_physical_commits > 0);
        try std.testing.expect(persisted_round.slowest_ready_group.persist_ready_detail.wal_inner_segment_syncs > 0);
        // The WAL index is synced only when its segment metadata changes.
        // Campaign/bootstrap may initialize it in an earlier Ready; a
        // steady-state proposal correctly persists with only a segment sync.
        try std.testing.expectEqual(@as(u64, 0), persisted_round.slowest_ready_group.persist_ready_detail.wal_post_commit_segment_syncs);
        try std.testing.expectEqual(@as(u64, 0), persisted_round.slowest_ready_group.persist_ready_detail.wal_post_commit_index_syncs);

        const state = provider.stateForGroup(501) orelse return error.MissingState;
        try std.testing.expect((try state.storage().lastIndex()) >= 1);
        var initial_state = try state.storage().initialState(std.testing.allocator);
        defer initial_state.deinit(std.testing.allocator);
        try std.testing.expectEqualSlices(u64, &.{1}, initial_state.conf_state.voters);

        _ = try local_host.ensureReplica(.{
            .group_id = 501,
            .replica_id = 2,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        try local_host.removeReplica(501);
        try std.testing.expect(provider.stateForGroup(501) == null);
        _ = try local_host.ensureReplica(.{
            .group_id = 501,
            .replica_id = 2,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });
        const replacement = provider.stateForGroup(501) orelse return error.MissingState;
        try std.testing.expect((try replacement.storage().lastIndex()) >= 1);
    }

    {
        var provider = try WalReplicaProvider.init(std.testing.allocator, .{ .root_dir = root }, base_factory.iface());
        defer provider.deinit();

        var local_host = host.Host.init(std.testing.allocator, .{ .local_node_id = 1 }, .{
            .descriptor_factory = provider.descriptorFactory(),
            .runtime_hooks = provider.runtimeHooks(),
        });
        defer local_host.deinit();

        _ = try local_host.ensureReplica(.{
            .group_id = 501,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .persisted,
        });

        const state = provider.stateForGroup(501) orelse return error.MissingState;
        try std.testing.expect((try state.storage().lastIndex()) >= 1);
    }

    {
        var relocation_factory = BaseFactory{
            .alloc = std.testing.allocator,
            .dummy_store = &dummy_store,
            .transport_peers = &.{ 1, 2 },
            .initial_voters = &.{1},
        };
        var provider = try WalReplicaProvider.init(std.testing.allocator, .{ .root_dir = root }, relocation_factory.iface());
        defer provider.deinit();
        var local_host = host.Host.init(std.testing.allocator, .{ .local_node_id = 2 }, .{
            .descriptor_factory = provider.descriptorFactory(),
            .runtime_hooks = provider.runtimeHooks(),
        });
        defer local_host.deinit();

        _ = try local_host.ensureReplica(.{
            .group_id = 502,
            .replica_id = 2,
            .local_node_id = 2,
            .bootstrap_mode = .persisted,
        });
        const state = provider.stateForGroup(502) orelse return error.MissingState;
        var initial_state = try state.storage().initialState(std.testing.allocator);
        defer initial_state.deinit(std.testing.allocator);
        try std.testing.expectEqualSlices(u64, &.{1}, initial_state.conf_state.voters);
    }
}
